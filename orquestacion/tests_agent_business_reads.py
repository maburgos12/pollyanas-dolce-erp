"""Live PostgreSQL reads with a simulated native Responses discovery loop."""
import copy
import json
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from api import ai_gateway_services as gateway
from core.models import AuditLog, Sucursal, UserProfile, UserModuleAccess
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from compras.models import SolicitudCompra
from pos_bridge.models import PointBranch, PointProduct, PointSyncJob, PointInventorySnapshot
from orquestacion.services.chat_service import create_chat_conversation, create_user_turn, execute_chat_turn, serialize_message


def call(name, arguments, number=1):
    return {'type':'function_call', 'call_id':f'b{number}', 'name':name, 'arguments':json.dumps(arguments)}


def response(*calls, text=''):
    return SimpleNamespace(output=list(calls), output_text=text, usage=None)


class FallbackClosureTests(SimpleTestCase):
    def event(self, name, status='ok'):
        return {'tool_name':name, 'tool_display_name':'Consulta', 'payload':{
            'result':{'status':status,'sources':['Fuente ERP'],'as_of':'2026-10-12',
                      'payload':{'items':[{'nombre':'Primera opción'},{'nombre':'Segunda opción'}]}}}}

    def test_business_ambiguity_uses_generic_choice_without_maintenance_note(self):
        from orquestacion.services.agent_read_runtime import _closure
        for name in ('erp_get_current_input_cost','erp_get_sales_summary'):
            with self.subTest(name=name):
                text=_closure([self.event(name,'ambiguous')])
                self.assertIn('seleccionar una opción',text)
                self.assertNotIn('equipo',text)
                self.assertNotIn('historial de mantenimiento',text)

    def test_business_only_fallback_does_not_infer_maintenance_history(self):
        from orquestacion.services.agent_read_runtime import _closure
        for pending in (None,{'items':[],'truncated':False}):
            with self.subTest(pending=pending):
                text=_closure([self.event('erp_get_inventory_low_stock'),self.event('erp_get_purchase_orders')],pending_workflows=pending)
                self.assertNotIn('historial de mantenimiento',text)

    def test_asset_and_workflow_fallbacks_retain_partial_history_note(self):
        from orquestacion.services.agent_read_runtime import _closure
        for name in ('erp_search_assets','erp_get_asset_context','erp_get_pending_maintenance',
                     'erp_prepare_asset_maintenance','erp_list_pending_workflows','erp_resume_asset_maintenance'):
            with self.subTest(name=name):
                self.assertIn('El historial de mantenimiento es parcial.',_closure([self.event(name)]))

    def test_mixed_results_and_pending_workflow_context_retain_history_note(self):
        from orquestacion.services.agent_read_runtime import _closure
        self.assertIn('El historial de mantenimiento es parcial.',_closure([
            self.event('erp_get_purchase_orders'),self.event('erp_get_asset_context')]))
        self.assertIn('El historial de mantenimiento es parcial.',_closure([],
            pending_workflows={'items':[],'truncated':False}))


@override_settings(AI_AGENT_READ_ENABLED=True, AI_GATEWAY_ASSETS_ENABLED=True, AI_AGENT_READ_MODEL='gpt-6.1-sol', OPENAI_API_KEY='fake')
class BusinessReadTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.branch = Sucursal.objects.create(codigo='BK-C', nombre='Centro')
        cls.other = Sucursal.objects.create(codigo='BK-C2', nombre='Centro norte')
        cls.actor = get_user_model().objects.create_superuser(username='bk-admin', password='test')
        UserProfile.objects.create(user=cls.actor, sucursal=cls.branch)
        cls.unit = UnidadMedida.objects.create(codigo='bk-kg', nombre='Kilo')
        cls.input = Insumo.objects.create(nombre='Fresa prueba BK', unidad_base=cls.unit)

    def setUp(self):
        pilot = override_settings(AI_AGENT_PILOT_USER_ID=self.actor.pk)
        pilot.enable(); self.addCleanup(pilot.disable)

    def bridge(self):
        from api import ai_gateway_business_reads
        return ai_gateway_business_reads

    def invoke(self, key, args):
        return self.bridge().invoke(user=self.actor, tool_key=key, arguments=args)

    def test_catalog_contains_only_approved_keys_and_safe_versioned_projection(self):
        knowledge = self.bridge().knowledge(self.actor)
        self.assertEqual(len(knowledge['capabilities']), 5)
        self.assertEqual(len(knowledge['version']), 64)
        self.assertEqual(knowledge, self.bridge().knowledge(self.actor))
        serialized = json.dumps(knowledge)
        for private in ('source_hash', '/Users/', 'handler', 'OPENAI_API_KEY', 'salary'):
            self.assertNotIn(private, serialized)
        self.assertEqual(knowledge['branches'][0]['name'], 'Centro')

    def test_strict_reader_arguments_and_sales_period(self):
        for key, args in [
            ('erp.get_sales_summary', {'actor_id':self.actor.pk}),
            ('erp.get_sales_summary', {'branch_ids':[self.branch.pk]}),
            ('erp.get_sales_summary', {'start_date':'2022-01-01', 'end_date':'2026-01-01'}),
            ('erp.get_sales_summary', {'start_date':'2026-05-01', 'end_date':'2026-01-01'}),
            ('erp.get_sales_summary', {'start_date':'2026-02-30'}),
            ('erp.get_inventory_low_stock', {'limit':True}),
            ('erp.get_inventory_low_stock', {'limit':'20'}),
            ('erp.get_purchase_orders', {'limit':201}),
            ('erp.get_current_input_cost', {'q':'x'*181}),
            ('erp.get_purchase_requests', {'sql':'SELECT * FROM rrhh_empleado'}),
        ]:
            with self.subTest(key=key, args=args), self.assertRaises(ValidationError):
                self.invoke(key,args)
        self.assertEqual(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ', payload__result_status='invalid_arguments').count(),10)

    def test_exact_branch_never_fuzzy_or_global_on_unknown(self):
        empty = {'source':'none','monto':0,'cantidad':0,'coverage_days':0,'coverage_accepted':False,'reason':'no_source'}
        with patch.object(gateway,'get_sales_range',return_value=empty) as read, patch.object(gateway,'get_sales_range_grouped',return_value={'rows':[]}):
            result = self.invoke('erp.get_sales_summary',{'branch':'Centro'})['result']
        self.assertEqual(read.call_args.kwargs['sucursales'],[self.branch.pk])
        self.assertLessEqual((read.call_args.kwargs['end_date']-read.call_args.kwargs['start_date']).days,30)
        self.assertEqual(result['status'],'no_data')
        self.assertIsNone(result['payload']['total_sales'])
        self.assertFalse(result['payload']['coverage_accepted'])
        with patch.object(gateway,'get_sales_range') as read, self.assertRaises(ValidationError):
            self.invoke('erp.get_sales_summary',{'branch':'does not exist'})
        read.assert_not_called()

    def test_duplicate_exact_names_require_clarification(self):
        Sucursal.objects.create(codigo='BK-DUP',nombre='Centro')
        with patch.object(gateway,'get_sales_range') as read:
            result=self.invoke('erp.get_sales_summary',{'branch':'Centro'})['result']
        self.assertEqual(result['status'],'ambiguous')
        read.assert_not_called()

    def test_capture_scope_without_assigned_branch_fails_closed(self):
        UserProfile.objects.filter(user=self.actor).update(modo_captura_sucursal=True,sucursal=None)
        with self.assertRaises(PermissionDenied):
            self.invoke('erp.get_inventory_low_stock',{})

    def test_capture_scope_exact_name_and_outside_denial(self):
        UserProfile.objects.filter(user=self.actor).update(modo_captura_sucursal=True)
        with patch.object(gateway,'_handle_inventory_low_stock',return_value={'status':'ok','sources':['snapshot'],'payload':{'items':[],'returned':0}}) as read:
            self.invoke('erp.get_inventory_low_stock',{'branch':'Centro'})
        self.assertEqual(read.call_args.kwargs['branch_ids'],[self.branch.pk])
        with self.assertRaises(PermissionDenied):
            self.invoke('erp.get_inventory_low_stock',{'branch':'Centro norte'})

    def test_missing_and_future_costs_are_not_zero_or_current(self):
        result=self.invoke('erp.get_current_input_cost',{'q':self.input.nombre})['result']
        self.assertEqual(result['status'],'no_data')
        self.assertIsNone(result['payload']['costo_unitario'])
        CostoInsumo.objects.create(insumo=self.input,fecha=timezone.localdate()+timedelta(days=1),costo_unitario=999,source_hash='future-bk')
        result=self.invoke('erp.get_current_input_cost',{'q':self.input.nombre})['result']
        self.assertEqual(result['status'],'no_data')
        self.assertIsNone(result['payload']['costo_unitario'])

    def test_cost_ambiguity_never_picks_first(self):
        Insumo.objects.create(nombre='Fresa prueba BK adicional',unidad_base=self.unit)
        result=self.invoke('erp.get_current_input_cost',{'q':'Fresa prueba BK'})['result']
        self.assertEqual(result['status'],'ambiguous')
        self.assertEqual(len(result['payload']['candidates']),2)
        self.assertNotIn('costo_unitario',result['payload'])

    def test_audit_failure_blocks_return_and_denials_are_audited(self):
        with patch('api.ai_gateway_business_reads.log_event',side_effect=RuntimeError('audit unavailable')), self.assertRaises(RuntimeError):
            self.invoke('erp.get_purchase_orders',{})
        with self.assertRaises(PermissionDenied):
            self.invoke('erp.get_dashboard',{})
        self.assertTrue(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ',payload__result_status='denied').exists())

    def run_turn(self, question, responses, hook=None):
        self.conversation=create_chat_conversation(user=self.actor)
        self.messages=create_user_turn(user=self.actor,conversation=self.conversation,content=question)
        self.requests=[]
        provider=Mock(); provider.with_options.return_value=provider
        queue=list(responses)
        def create(**kwargs):
            self.requests.append(copy.deepcopy(kwargs))
            if hook: hook(len(self.requests))
            item=queue.pop(0)
            if item == 'natural':
                evidence=json.loads(kwargs['input'][-1]['output'])['tool_call_id']
                return response(text=json.dumps({'answer':'Consulta verificada.','evidence_ids':[evidence],'action_claim':'none','incident_ids':[]}))
            return item
        provider.responses.create.side_effect=create
        with patch('openai.OpenAI',return_value=provider), patch('orquestacion.services.agent_pilot.reserve_request',return_value={}):
            return execute_chat_turn(user=self.actor,conversation=self.conversation,user_message=self.messages[0],assistant_message=self.messages[1])

    def test_native_discovery_enables_five_business_readers_on_next_round(self):
        questions={
            'erp.get_sales_summary':'¿Cuánto vendimos ayer?',
            'erp.get_inventory_low_stock':'¿Qué productos faltan en Centro?',
            'erp.get_current_input_cost':'¿Cuánto cuesta Fresa prueba BK?',
            'erp.get_purchase_requests':'¿Qué solicitudes hay pendientes?',
            'erp.get_purchase_orders':'¿Cuáles órdenes siguen abiertas?',
        }
        for key,question in questions.items():
            args={'q':self.input.nombre} if 'input_cost' in key else {}
            with self.subTest(key=key):
                result=self.run_turn(question,[response(call('erp_discover_capabilities',{'keys':[key]})),response(call(key.replace('.','_'),args,2)),'natural'])
                self.assertEqual(result.assistant_text,'Consulta verificada.')
                self.assertNotIn(key.replace('.','_'),[tool['name'] for tool in self.requests[0]['tools']])
                self.assertIn(key.replace('.','_'),[tool['name'] for tool in self.requests[1]['tools']])
                self.messages[1].refresh_from_db()
                self.assertEqual(self.messages[1].status,'complete')
                self.assertEqual(self.messages[1].tool_calls.count(),2)

    def test_undiscovered_reader_and_disallowed_discovery_never_execute(self):
        self.run_turn('Ventas',[response(call('erp_get_sales_summary',{})),response()])
        self.assertFalse(self.messages[1].tool_calls.filter(tool_key='erp.get_sales_summary').exists())
        self.assertTrue(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ',payload__tool_key='erp.get_sales_summary',payload__result_status='denied').exists())
        self.run_turn('Nómina',[response(call('erp_discover_capabilities',{'keys':['erp.get_dashboard']})),response()])
        self.assertTrue(self.messages[1].tool_calls.filter(status='error').exists())

    def test_permission_revoke_in_flight_and_history_blocks_all_evidence(self):
        key='erp.get_purchase_orders'
        original=gateway.TOOLS[key]
        def revoke(round):
            if round==2: gateway.TOOLS[key]=replace(original,access_check=lambda actor:False)
        try:
            self.run_turn('Órdenes',[response(call('erp_discover_capabilities',{'keys':[key]})),response(call('erp_get_purchase_orders',{},2))],revoke)
            self.messages[1].refresh_from_db()
            self.assertEqual(self.messages[1].status,'error')
            self.assertFalse(self.messages[1].tool_calls.filter(tool_key=key).exists())
        finally:
            gateway.TOOLS[key]=original
        self.run_turn('Órdenes privadas',[response(call('erp_discover_capabilities',{'keys':[key]})),response(call('erp_get_purchase_orders',{},2)),'natural'])
        self.messages[1].refresh_from_db()
        with patch.dict(gateway.TOOLS,{key:replace(original,access_check=lambda actor:False)}):
            from orquestacion.services.agent_read_context import history_context
            self.assertEqual(history_context(self.conversation,self.messages[1].sequence+1),([],set()))
            self.assertNotEqual(serialize_message(self.messages[1])['content'],'Consulta verificada.')

    def test_canonical_cost_candidate_can_be_selected_by_strict_id(self):
        CostoInsumo.objects.create(insumo=self.input,fecha=timezone.localdate(),costo_unitario='42.125',source_hash='current-bk')
        result=self.invoke('erp.get_current_input_cost',{'insumo_id':self.input.pk})['result']
        self.assertEqual(result['payload']['costo_unitario'],'42.125000')
        for args in ({'insumo_id':True},{'insumo_id':str(self.input.pk)},{'q':'Fresa','insumo_id':self.input.pk},{}):
            with self.subTest(args=args), self.assertRaises(ValidationError):
                self.invoke('erp.get_current_input_cost',args)

    def test_truncated_canonical_candidates_cannot_claim_one_cost(self):
        with patch.object(gateway,'_resolve_input_cost_candidates',return_value=([self.input,self.input],False)):
            result=self.invoke('erp.get_current_input_cost',{'q':'Fresa','limit':1})['result']
        self.assertEqual(result['status'],'ambiguous')
        self.assertTrue(result['payload']['truncated'])
        self.assertNotIn('costo_unitario',result['payload'])

    def test_raw_cost_search_cap_cannot_claim_one_cost_after_ranking(self):
        for number in range(41):
            Insumo.objects.create(nombre=f'Fresa búsqueda acotada {number}',unidad_base=self.unit)
        CostoInsumo.objects.create(insumo=self.input,fecha=timezone.localdate(),costo_unitario=42,source_hash='capped-cost')
        # Only one candidate survives scoring; the other raw matches still hide unseen options.
        with patch.object(gateway,'_score_input_candidate',side_effect=lambda **kw:100 if kw['insumo'].pk==self.input.pk else 0):
            result=self.invoke('erp.get_current_input_cost',{'q':'Fresa','limit':1})['result']
        self.assertEqual(result['status'],'ambiguous')
        self.assertTrue(result['payload']['truncated'])
        self.assertNotIn('costo_unitario',result['payload'])

    def test_non_dg_purchase_view_acl_lock_and_revocation_suppress_runtime_history(self):
        get_user_model().objects.filter(pk=self.actor.pk).update(is_superuser=False,is_staff=False)
        self.actor.refresh_from_db()
        self.assertFalse(self.actor.groups.filter(name='DG').exists())
        UserProfile.objects.filter(user=self.actor).update(modo_captura_sucursal=True,sucursal=self.branch)
        acl=UserModuleAccess.objects.create(user=self.actor,module='compras',access='VIEW')
        key='erp.get_purchase_orders'
        from orquestacion.services.agent_read_context import history_context
        for kind in ('lock','revoke_acl'):
            with self.subTest(kind=kind):
                UserProfile.objects.filter(user=self.actor).update(lock_compras=False)
                UserModuleAccess.objects.filter(pk=acl.pk).update(access='VIEW')
                self.assertIn(key,self.bridge().allowed_keys(self.actor))
                def revoke(round):
                    if round==2:
                        if kind=='lock': UserProfile.objects.filter(user=self.actor).update(lock_compras=True)
                        else: UserModuleAccess.objects.filter(pk=acl.pk).update(access=UserModuleAccess.ACCESS_NONE)
                self.run_turn('Órdenes restringidas',[response(call('erp_discover_capabilities',{'keys':[key]})),response(call('erp_get_purchase_orders',{},2))],revoke)
                self.messages[1].refresh_from_db()
                self.assertEqual(self.messages[1].status,'error')
                self.assertFalse(self.messages[1].tool_calls.filter(tool_key=key).exists())
                self.assertTrue(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ',payload__result_status='denied').exists())
                UserProfile.objects.filter(user=self.actor).update(lock_compras=False)
                UserModuleAccess.objects.filter(pk=acl.pk).update(access='VIEW')
                self.run_turn('Órdenes verificadas',[response(call('erp_discover_capabilities',{'keys':[key]})),response(call('erp_get_purchase_orders',{},2)),'natural'])
                self.messages[1].refresh_from_db()
                self.assertEqual(self.messages[1].status,'complete')
                revoke(2)
                self.assertEqual(history_context(self.conversation,self.messages[1].sequence+1),([],set()))
                projected=serialize_message(self.messages[1])
                self.assertNotEqual(projected['content'],'Consulta verificada.')
                self.assertTrue(projected['tool_calls'])
                self.assertTrue(all(row['tool_key']=='read_unavailable' and row['result']=={} for row in projected['tool_calls']))

    def test_scope_change_during_direct_reader_blocks_result(self):
        key='erp.get_purchase_orders'; original=gateway.TOOLS[key]
        def move(actor,arguments):
            UserProfile.objects.filter(user=actor).update(modo_captura_sucursal=True,sucursal=self.other)
            return original.handler(actor,arguments)
        with patch.dict(gateway.TOOLS,{key:replace(original,handler=move)}), self.assertRaises(PermissionDenied):
            self.invoke(key,{})

    def test_business_proof_is_required_but_old_asset_proofs_remain_compatible(self):
        from orquestacion.services.agent_read_runtime import can_project_read_message
        self.run_turn('Órdenes',[response(call('erp_discover_capabilities',{'keys':['erp.get_purchase_orders']})),response(call('erp_get_purchase_orders',{},2)),'natural'])
        answer=self.messages[1]; answer.refresh_from_db()
        proof=answer.metadata_json['agent_read']; proof.pop('business_fingerprint')
        answer.save(update_fields=['metadata_json'])
        self.assertFalse(can_project_read_message(answer))

    def test_real_role_candado_revoked_during_provider_and_history(self):
        get_user_model().objects.filter(pk=self.actor.pk).update(is_superuser=False,is_staff=False)
        self.actor.refresh_from_db()
        self.actor.groups.add(Group.objects.get_or_create(name='DG')[0])
        key='erp.get_purchase_orders'
        def revoke(round):
            if round==2: UserProfile.objects.filter(user=self.actor).update(lock_compras=True)
        self.run_turn('Órdenes',[response(call('erp_discover_capabilities',{'keys':[key]})),response(call('erp_get_purchase_orders',{},2))],revoke)
        self.messages[1].refresh_from_db()
        self.assertEqual(self.messages[1].status,'error')
        self.assertFalse(self.messages[1].tool_calls.filter(tool_key=key).exists())
        self.assertTrue(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ',payload__result_status='denied').exists())
        UserProfile.objects.filter(user=self.actor).update(lock_compras=False)
        self.run_turn('Órdenes privadas',[response(call('erp_discover_capabilities',{'keys':[key]})),response(call('erp_get_purchase_orders',{},2)),'natural'])
        UserProfile.objects.filter(user=self.actor).update(lock_compras=True)
        from orquestacion.services.agent_read_context import history_context
        self.assertEqual(history_context(self.conversation,self.messages[1].sequence+1),([],set()))

    def test_inventory_exact_scope_truncation_and_capture_freshness(self):
        point=PointBranch.objects.create(external_id='bk-a',name='Centro',erp_branch=self.branch)
        other=PointBranch.objects.create(external_id='bk-b',name='Centro norte',erp_branch=self.other)
        job=PointSyncJob.objects.create()
        captured=timezone.now()-timedelta(days=3)
        for number,branch in enumerate([point,point,other]):
            product=PointProduct.objects.create(external_id=f'bk-p{number}',name=f'Producto {number}')
            PointInventorySnapshot.objects.create(branch=branch,product=product,stock=1,min_stock=4,captured_at=captured,sync_job=job)
        result=self.invoke('erp.get_inventory_low_stock',{'branch':'Centro','limit':1})['result']
        self.assertEqual(result['payload']['returned'],1)
        self.assertTrue(result['payload']['truncated'])
        self.assertEqual(result['payload']['items'][0]['erp_branch_code'],self.branch.codigo)
        self.assertEqual(result['payload']['freshness']['captured_at'],[captured.isoformat()])
        self.assertNotEqual(result['as_of'],captured.isoformat())

    def test_purchase_source_is_explicit_and_live_bounded(self):
        self.assertEqual(self.invoke('erp.get_purchase_requests',{})['result']['status'],'no_data')
        for number in range(3):
            SolicitudCompra.objects.create(folio=f'BK-{number}',area='Producción',solicitante='Prueba',insumo=self.input,cantidad=1)
        result=self.invoke('erp.get_purchase_requests',{'limit':1})['result']
        self.assertEqual(result['status'],'ok')
        self.assertTrue(result['payload']['truncated'])
        self.assertIn('departamentales',result['payload']['limitations'][0])
        self.assertTrue(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ',payload__data_domain='purchasing').exists())

    def test_real_sales_coverage_reason_survives_business_bridge(self):
        selected={'source':'authoritative','source_detail':'venta_autoritativa_point','monto':0,'cantidad':0,'coverage_days':1,'coverage_accepted':True,'coverage_reason':'prefer_complete'}
        with patch.object(gateway,'get_sales_range',return_value=selected),patch.object(gateway,'get_sales_range_grouped',return_value={'rows':[]}):
            result=self.invoke('erp.get_sales_summary',{})['result']
        self.assertEqual(result['payload']['coverage_reason'],'prefer_complete')
        self.assertEqual(result['payload']['total_sales'],0)

    def test_multiple_domains_and_fresh_followup_use_discovery_without_routing(self):
        keys=['erp.get_inventory_low_stock','erp.get_purchase_orders']
        self.run_turn('Faltantes y órdenes',[response(call('erp_discover_capabilities',{'keys':keys})),response(call('erp_get_inventory_low_stock',{'branch':'Centro'},2)),response(call('erp_get_purchase_orders',{},3)),'natural'])
        self.messages=create_user_turn(user=self.actor,conversation=self.conversation,content='Ahora en Centro norte')
        self.requests=[]
        queue=[response(call('erp_discover_capabilities',{'keys':[keys[0]]})),response(call('erp_get_inventory_low_stock',{'branch':'Centro norte'},2)),response()]
        provider=Mock(); provider.with_options.return_value=provider
        def create(**kwargs):
            self.requests.append(copy.deepcopy(kwargs)); return queue.pop(0)
        provider.responses.create.side_effect=create
        with patch('openai.OpenAI',return_value=provider),patch('orquestacion.services.agent_pilot.reserve_request',return_value={}):
            execute_chat_turn(user=self.actor,conversation=self.conversation,user_message=self.messages[0],assistant_message=self.messages[1])
        self.assertNotIn('erp_get_inventory_low_stock',[tool['name'] for tool in self.requests[0]['tools']])
        self.assertIn('Faltantes y órdenes',json.dumps(self.requests[0]['input'],ensure_ascii=False))
        self.messages[1].refresh_from_db()
        self.assertEqual(self.messages[1].tool_calls.get(tool_key=keys[0]).result.result_json['scope']['resolved_branch_ids'],[self.other.pk])

    def test_aggregate_sales_ticket_placeholder_is_not_a_real_zero(self):
        selected={'source':'authoritative','source_detail':'venta_autoritativa_point','monto':123,'cantidad':3,'coverage_days':1,'coverage_accepted':True,'coverage_reason':'prefer_complete'}
        with patch.object(gateway,'get_sales_range',return_value=selected),patch.object(gateway,'get_sales_range_grouped',return_value={'rows':[{'total_tickets':0}]}):
            result=self.invoke('erp.get_sales_summary',{})['result']
        self.assertEqual(result['payload']['total_sales'],123)
        self.assertIsNone(result['payload']['total_tickets'])
        self.assertFalse(result['payload']['limitations']['ticket_count_available'])

    def test_fresh_inventory_source_capture_is_reported_even_without_alerts(self):
        point=PointBranch.objects.create(external_id='bk-full',name='Centro',erp_branch=self.branch)
        product=PointProduct.objects.create(external_id='bk-full-product',name='Con stock')
        captured=timezone.now()-timedelta(days=2)
        PointInventorySnapshot.objects.create(branch=point,product=product,stock=5,min_stock=2,captured_at=captured,sync_job=PointSyncJob.objects.create())
        result=self.invoke('erp.get_inventory_low_stock',{'branch':'Centro'})['result']
        self.assertEqual(result['payload']['freshness']['source_latest_capture'],captured.isoformat())
        self.assertTrue(result['payload']['freshness']['source_snapshots_available'])

    def test_inventory_freshness_is_scoped_and_uses_bounded_index_lookup(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext
        point=PointBranch.objects.create(external_id='bk-fast',name='Centro',erp_branch=self.branch)
        other=PointBranch.objects.create(external_id='bk-fast-other',name='Centro norte',erp_branch=self.other)
        product=PointProduct.objects.create(external_id='bk-fast-product',name='Con stock')
        job=PointSyncJob.objects.create()
        captured=timezone.now()-timedelta(days=2)
        for branch,stamp in [(point,captured-timedelta(days=1)),(point,captured),(other,timezone.now())]:
            PointInventorySnapshot.objects.create(branch=branch,product=product,stock=5,min_stock=2,captured_at=stamp,sync_job=job)
        with CaptureQueriesContext(connection) as queries:
            result=self.invoke('erp.get_inventory_low_stock',{'branch':'Centro'})['result']
        self.assertEqual(result['payload']['freshness']['source_latest_capture'],captured.isoformat())
        snapshot_queries=[q['sql'] for q in queries if 'pos_bridge_inventory_snapshots' in q['sql']]
        freshness=snapshot_queries[-1]
        self.assertIn('LIMIT 1',freshness)
        self.assertNotIn('MAX(',freshness)
        self.assertEqual(freshness.count('SELECT'),1)

    def test_limit_explanation_reports_permitted_business_capabilities(self):
        result=self.run_turn('¿Qué puedes consultar?',[response(call('erp_explain_read_limit',{'operation':'UNKNOWN'}))])
        self.assertIn('ventas',result.assistant_text.lower())
        self.assertIn('stock',result.assistant_text.lower())

    def test_discovery_schema_contains_only_actor_permitted_keys(self):
        get_user_model().objects.filter(pk=self.actor.pk).update(is_superuser=False,is_staff=False)
        self.actor.refresh_from_db()
        UserModuleAccess.objects.create(user=self.actor,module='inventario',access='VIEW')
        self.run_turn('Faltantes',[response()])
        discovery=next(tool for tool in self.requests[0]['tools'] if tool['name']=='erp_discover_capabilities')
        self.assertEqual(discovery['parameters']['properties']['keys']['items']['enum'],['erp.get_inventory_low_stock'])

    def test_identical_business_read_reuses_evidence_after_fresh_checks(self):
        key='erp.get_purchase_orders'
        self.run_turn('Órdenes',[response(call('erp_discover_capabilities',{'keys':[key]})),response(call('erp_get_purchase_orders',{},2)),response(call('erp_get_purchase_orders',{'q':'','limit':20},3)),'natural'])
        self.assertEqual(self.messages[1].tool_calls.count(),2)
        self.assertEqual(AuditLog.objects.filter(action='AI_AGENT_BUSINESS_READ',payload__tool_key=key).count(),1)
        self.assertTrue(json.loads(self.requests[-1]['input'][-1]['output'])['already_consulted'])

    def test_repeated_business_read_cannot_reuse_revoked_evidence(self):
        key='erp.get_purchase_orders'; original=gateway.TOOLS[key]
        def revoke(round):
            if round==3: gateway.TOOLS[key]=replace(original,access_check=lambda actor:False)
        try:
            self.run_turn('Órdenes',[response(call('erp_discover_capabilities',{'keys':[key]})),response(call('erp_get_purchase_orders',{},2)),response(call('erp_get_purchase_orders',{},3))],revoke)
            self.messages[1].refresh_from_db()
            self.assertEqual(self.messages[1].status,'error')
            self.assertEqual(self.messages[1].tool_calls.filter(tool_key=key).count(),1)
            self.assertNotIn('Órdenes de compra',self.messages[1].content)
        finally:
            gateway.TOOLS[key]=original

    def test_unknown_branch_in_runtime_can_be_clarified_without_global_read(self):
        self.run_turn('Faltantes en una sucursal',[response(call('erp_discover_capabilities',{'keys':['erp.get_inventory_low_stock']})),response(call('erp_get_inventory_low_stock',{'branch':'Unknown'},2)),response(call('erp_get_inventory_low_stock',{'branch':'Centro'},3)),'natural'])
        self.messages[1].refresh_from_db()
        self.assertEqual(self.messages[1].status,'complete')
        reads=list(self.messages[1].tool_calls.filter(tool_key='erp.get_inventory_low_stock').order_by('pk'))
        self.assertEqual(reads[0].result.result_json['error']['code'],'invalid_arguments')
        self.assertEqual(reads[1].result.result_json['scope']['resolved_branch_ids'],[self.branch.pk])
