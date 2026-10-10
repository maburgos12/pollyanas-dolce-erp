"""Real PostgreSQL services, synthetic business records and mocked provider."""
import copy
import json
from datetime import timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from PIL import Image
from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, TransactionTestCase, override_settings, Client
from django.urls import reverse
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from compras.models import SolicitudCompraDepartamental, ItemCompraDepartamental, CotizacionCompraDepartamental, CompraRealizadaDepartamental, RecepcionItemDepartamental, EventoCompraDepartamental
from core.models import AuditLog, Sucursal, UserProfile
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable
from orquestacion.models import ChatToolCall
from orquestacion.services import agent_purchases as purchases
from orquestacion.services.agent_workflows import WorkflowError
from orquestacion.services.chat_service import create_chat_conversation, create_user_turn, execute_chat_turn, serialize_conversation_detail


class PurchaseAgentFixture:
    def setUp(self):
        self.media=TemporaryDirectory();self.addCleanup(self.media.cleanup)
        self.user=get_user_model().objects.create_superuser('purchase-dg','dg@example.test','test-password')
        self.requester=get_user_model().objects.create_user('carolina.test',first_name='Carolina',last_name='Producción')
        self.other=get_user_model().objects.create_user('other.test')
        self.area=AreaPresupuesto.objects.create(codigo='purchase-prod',nombre='Producción')
        AreaPresupuestoResponsable.objects.create(area=self.area,usuario=self.requester)
        self.override=override_settings(MEDIA_ROOT=self.media.name,AI_AGENT_PILOT_USER_ID=self.user.pk)
        self.override.enable();self.addCleanup(self.override.disable)
        self.conversation=create_chat_conversation(user=self.user)
        self.messages=create_user_turn(user=self.user,conversation=self.conversation,content='Carolina necesita glicerina y azúcar para pruebas; ya compré por Mercado Libre.')
        self.args={'area_id':self.area.pk,'solicitante_id':self.requester.pk,'motivo':'Pruebas en Producción',
            'justificacion_extraordinaria':'Insumos para pruebas fuera del ciclo mensual.',
            'items':[{'descripcion':'Glicerina comestible Dayman 120 ml','cantidad':'2','unidad':'frasco','precio_total':'80.84','vendedor_reportado':'REGGIZ'},
                     {'descripcion':'Azúcar invertido Trimoline 500 g','cantidad':'1','unidad':'envase','precio_total':'133.00','vendedor_reportado':'AChocolart'}],
            'compra_reportada':True,'plataforma_reportada':'Mercado Libre','envio_global':'129.00','total_reportado':'342.84',
            'entrega_estimada':(timezone.localdate()+timedelta(days=5)).isoformat()}

    def invoke(self,args=None,key=purchases.KEY,call='first'):
        return purchases.invoke(user=self.user,tool_key=key,arguments=self.args if args is None else args,
            conversation=self.conversation,user_message=self.messages[0],assistant_message=self.messages[1],call_id=call)

    def prepare(self,args=None,call='first'):
        return self.invoke(args,call=call)['result']['payload']['incident']

    def command(self,dto):
        return {'confirm':True,'expected_version':dto['version'],'payload_hash':dto['payload_hash']}

    def confirm(self,dto):
        return purchases.confirm(user=self.user,draft_id=dto['draft_id'],arguments=self.command(dto))['result']['payload']['incident']

    def photo(self):
        image=BytesIO();Image.new('RGB',(10,10),'red').save(image,'PNG')
        return SimpleUploadedFile('compra.png',image.getvalue(),content_type='image/png')



@override_settings(AI_AGENT_READ_ENABLED=True, AI_GATEWAY_ASSETS_ENABLED=True, AI_AGENT_INCIDENTS_ENABLED=True, AI_AGENT_READ_MODEL='gpt-6.1-sol', OPENAI_API_KEY='fake-test-key')
class PurchaseAgentTests(PurchaseAgentFixture, TestCase):
    def test_calculation_optional_evidence_and_confirm_receipt_are_separate_from_payment(self):
        dto=self.prepare()
        self.assertEqual(dto['items'][0]['costo_unitario_estimado'],'40.42')
        self.assertEqual(dto['fields']['envio_global'],'129.00')
        self.assertEqual(dto['status'],'AWAITING_CONFIRMATION')
        self.assertFalse(SolicitudCompraDepartamental.objects.exists())
        result=self.confirm(dto)
        self.assertEqual(result['status'],'EXECUTED')
        self.assertEqual(self.confirm(dto),result)
        row=SolicitudCompraDepartamental.objects.get()
        self.assertEqual(row.solicitante,self.requester)
        self.assertEqual(row.estado,'ENVIADA')
        self.assertEqual(row.items.count(),2)
        self.assertEqual(row.items.get(descripcion__startswith='Glicerina').costo_unitario_estimado,Decimal('40.42'))
        self.assertTrue(all(i.monto_gastado==0 and i.estado=='POR_REVISAR' for i in row.items.all()))
        self.assertFalse(CotizacionCompraDepartamental.objects.exists())
        self.assertFalse(CompraRealizadaDepartamental.objects.exists())
        self.assertFalse(RecepcionItemDepartamental.objects.exists())
        self.assertEqual(EventoCompraDepartamental.objects.get().tipo,'COMPRA_REPORTADA_IA')
        self.assertTrue(AuditLog.objects.filter(user=self.user,action='AI_PURCHASE_REQUEST_CREATE',payload__solicitante_id=self.requester.pk).exists())

    def test_requirements_find_real_requester_without_contacts(self):
        result=self.invoke({'q':'Carolina'},key='purchase.requirements')['result']['payload']
        self.assertEqual(result['solicitantes'],[{'id':self.requester.pk,'nombre':'Carolina Producción'}])
        self.assertFalse(result['presupuesto_requerido']);self.assertFalse(result['evidencia_requerida'])
        self.assertNotIn('email',json.dumps(result))

    def test_purchase_detail_reuses_prefetched_items(self):
        self.confirm(self.prepare())
        rows=list(purchases.requests(self.user))
        with self.assertNumQueries(0):
            result=[purchases.request_dto(row) for row in rows]
        self.assertEqual(len(result[0]['items']),2)

    def test_missing_information_resume_and_unknown_estimate_stays_null(self):
        args=copy.deepcopy(self.args);args.pop('motivo');args['items'][0].pop('precio_total')
        dto=self.prepare(args)
        self.assertIn('motivo',dto['missing_fields'])
        self.assertIsNone(dto['items'][0]['costo_unitario_estimado'])
        with self.assertRaises(WorkflowError):self.confirm(dto)
        self.messages=create_user_turn(user=self.user,conversation=self.conversation,content='Es para pruebas de producción.')
        dto=self.prepare({'draft_id':dto['draft_id'],'expected_version':dto['version'],'motivo':'Pruebas de producción'},call='resume')
        self.confirm(dto)
        self.assertIsNone(ItemCompraDepartamental.objects.get(descripcion__startswith='Glicerina').costo_unitario_estimado)

    def test_repeated_prepare_and_conflicting_idempotency(self):
        dto=self.prepare()
        self.assertEqual(self.prepare(),dto)
        with self.assertRaises(WorkflowError):self.prepare({**self.args,'motivo':'Otro uso'})
        self.assertEqual(ChatToolCall.objects.filter(tool_key=purchases.KEY).count(),1)

    def test_duplicate_request_is_blocked_across_conversations(self):
        dto=self.prepare();self.confirm(dto)
        self.conversation=create_chat_conversation(user=self.user)
        self.messages=create_user_turn(user=self.user,conversation=self.conversation,content='Repite la solicitud')
        dto=self.prepare()
        self.assertEqual(dto['status'],'REVIEW_REQUIRED')
        with self.assertRaises(WorkflowError):self.confirm(dto)
        self.assertEqual(SolicitudCompraDepartamental.objects.count(),1)

    def test_wrong_total_precision_float_unknown_keys_and_future_purchase_rejected(self):
        variants=[{'total_reportado':'343.84'},{'fecha_compra_reportada':(timezone.localdate()+timedelta(days=1)).isoformat()},
                  {'total_reportado':342.84},{'payment_authorized':True},
                  {'items':[{'descripcion':'Prueba','cantidad':'3','unidad':'pieza','precio_total':'1.00'}]}]
        for changes in variants:
            with self.subTest(changes=changes),self.assertRaises((ValidationError,WorkflowError)):
                self.prepare({**self.args,**changes})
        self.assertFalse(SolicitudCompraDepartamental.objects.exists())

    def test_wrong_owner_participant_and_disabled_flags(self):
        dto=self.prepare()
        with self.assertRaises(WorkflowError):purchases.confirm(user=self.other,draft_id=dto['draft_id'],arguments=self.command(dto))
        with override_settings(AI_AGENT_PILOT_USER_ID=self.other.pk),self.assertRaises(WorkflowError):self.confirm(dto)
        with override_settings(AI_AGENT_INCIDENTS_ENABLED=False),self.assertRaises(WorkflowError):self.confirm(dto)
        self.requester.is_active=False;self.requester.save(update_fields=['is_active'])
        with self.assertRaises(WorkflowError):self.confirm(dto)

    def test_self_request_scope_and_no_impersonation(self):
        branch=Sucursal.objects.create(codigo='PURCHASE-A',nombre='A')
        UserProfile.objects.create(user=self.requester,sucursal=branch)
        with override_settings(AI_AGENT_PILOT_USER_ID=self.requester.pk):
            self.assertTrue(purchases.enabled(self.requester))
            with self.assertRaises(WorkflowError):purchases.identities(self.requester,{'area_id':self.area.pk,'solicitante_id':self.user.pk})
            another=AreaPresupuesto.objects.create(codigo='purchase-other',nombre='Otra')
            with self.assertRaises(WorkflowError):purchases.identities(self.requester,{'area_id':another.pk})
            row=SolicitudCompraDepartamental.objects.create(area=another,solicitante=self.user,periodo=timezone.localdate().replace(day=1))
            self.assertFalse(purchases.requests(self.requester).filter(pk=row.pk).exists())

    def test_file_upload_replay_stale_confirmation_private_preview_and_requester_visibility(self):
        dto=self.prepare()
        attached=purchases.attach(user=self.user,draft_id=dto['draft_id'],arguments=self.command(dto),files=[self.photo()])['result']['payload']['incident']
        replay=purchases.attach(user=self.user,draft_id=dto['draft_id'],arguments=self.command(dto),files=[self.photo()])['result']['payload']['incident']
        self.assertEqual(attached,replay)
        with self.assertRaises(WorkflowError):self.confirm(dto)
        url=attached['files'][0]['url']
        self.assertEqual(self.client.get(url).status_code,403)
        self.client.force_login(self.requester);self.assertEqual(self.client.get(url).status_code,404)
        self.client.force_login(self.user);res=self.client.get(url);self.assertEqual(res.status_code,200);self.assertTrue(b''.join(res.streaming_content))
        draft=ChatToolCall.objects.get(public_id=dto['draft_id'])
        self.assertEqual(self.client.get('/media/'+draft.metadata_json['files'][0]['path']).status_code,404)
        result=self.confirm(attached)
        self.client.force_login(self.requester)
        res=self.client.get(url);self.assertEqual(res.status_code,200);self.assertTrue(b''.join(res.streaming_content))
        self.assertContains(self.client.get(result['url']),'Ver evidencia: compra.png')
        self.assertContains(self.client.get(result['url']),'Pendiente de regularización')
        self.client.force_login(self.other);self.assertEqual(self.client.get(url).status_code,404)
        self.assertEqual(len(list(Path(self.media.name).rglob('*.png'))),1)

    def test_confirmation_requires_csrf_and_is_not_a_model_tool(self):
        dto=self.prepare();url=reverse('api_ai_purchase_confirm',args=[dto['draft_id']])
        client=Client(enforce_csrf_checks=True);client.force_login(self.user)
        self.assertEqual(client.post(url,self.command(dto),content_type='application/json').status_code,403)
        self.assertTrue(all('confirm' not in t['name'] for t in purchases.catalog(self.user)))
        with self.assertRaises(ValidationError):purchases.confirm(user=self.user,draft_id=dto['draft_id'],arguments={**self.command(dto),'confirm':'true'})

    def test_purchase_upload_counts_pending_followup_evidence(self):
        dto=self.prepare()
        pending=ChatToolCall.objects.create(conversation=self.conversation,request_message=self.messages[0],
            assistant_message=self.messages[1],tool_key='incident.followup',
            metadata_json={'incident_status':'AWAITING_CONFIRMATION','files':[{'bytes':100*1024*1024}]})
        with self.assertRaises(WorkflowError) as error:
            purchases.attach(user=self.user,draft_id=dto['draft_id'],arguments=self.command(dto),files=[self.photo()])
        self.assertEqual(error.exception.code,'purchase_staging_limit')
        self.assertFalse(list(Path(self.media.name).rglob('*.png')))
        pending.metadata_json['incident_status']='EXECUTED';pending.save()
        result=purchases.attach(user=self.user,draft_id=dto['draft_id'],arguments=self.command(dto),files=[self.photo()])
        self.assertEqual(len(result['result']['payload']['incident']['files']),1)

    def test_expired_and_altered_evidence_cannot_be_confirmed(self):
        dto=self.prepare()
        attached=purchases.attach(user=self.user,draft_id=dto['draft_id'],arguments=self.command(dto),files=[self.photo()])['result']['payload']['incident']
        draft=ChatToolCall.objects.get(public_id=dto['draft_id'])
        Path(self.media.name,draft.metadata_json['files'][0]['path']).write_bytes(b'changed')
        with self.assertRaises(WorkflowError):self.confirm(attached)
        draft.metadata_json['expires_at']=(timezone.now()-timedelta(seconds=1)).isoformat();draft.save(update_fields=['metadata_json'])
        self.assertEqual(purchases.project(draft,self.user)['status'],'EXPIRED')
        self.assertFalse(SolicitudCompraDepartamental.objects.exists())

    def test_runtime_uses_same_core_and_receipt_in_fresh_history(self):
        provider=Mock();provider.with_options.return_value=provider
        requests=[]
        def respond(**kwargs):
            requests.append(kwargs)
            if len(requests)==1:
                return SimpleNamespace(output=[{'type':'function_call','name':'erp_prepare_special_purchase','call_id':'purchase-c1','arguments':json.dumps(self.args)}],output_text='')
            dto=purchases.project(ChatToolCall.objects.get(tool_key=purchases.KEY),self.user)
            return SimpleNamespace(output=[],output_text=json.dumps({'answer':'Preparé la solicitud de Carolina; falta tu confirmación.',
                'evidence_ids':[dto['draft_id']],'incident_ids':[dto['draft_id']],'action_claim':'proposal'}))
        provider.responses.create.side_effect=respond
        with patch('openai.OpenAI',return_value=provider),patch('orquestacion.services.agent_pilot.reserve_request',return_value={}):
            result=execute_chat_turn(user=self.user,conversation=self.conversation,user_message=self.messages[0],assistant_message=self.messages[1])
        self.assertIn('Carolina',result.assistant_text)
        self.assertFalse(SolicitudCompraDepartamental.objects.exists())
        self.assertEqual(ChatToolCall.objects.filter(tool_key=purchases.KEY).count(),1)
        dto=purchases.project(ChatToolCall.objects.get(tool_key=purchases.KEY),self.user)
        self.confirm(dto)
        detail=serialize_conversation_detail(self.conversation)
        self.assertEqual(detail['messages'][-1]['tool_calls'][0]['result']['result']['payload']['incident']['status'],'EXECUTED')
        self.assertTrue(any(t['name']=='erp_prepare_special_purchase' for t in requests[0]['tools']))
        self.user.is_active=False;self.user.save(update_fields=['is_active'])
        detail=serialize_conversation_detail(self.conversation)
        self.assertEqual(detail['messages'][-1]['tool_calls'][0]['tool_key'],'read_unavailable')


@override_settings(AI_AGENT_READ_ENABLED=True, AI_GATEWAY_ASSETS_ENABLED=True, AI_AGENT_INCIDENTS_ENABLED=True, AI_AGENT_READ_MODEL='gpt-6.1-sol')
class PurchaseConfirmationConcurrencyTests(PurchaseAgentFixture, TransactionTestCase):
    def test_parallel_confirmation_creates_one_request(self):
        from concurrent.futures import ThreadPoolExecutor
        from django.db import close_old_connections
        dto=self.prepare()
        def execute():
            close_old_connections()
            try:
                return self.confirm(dto)
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(execute);second=pool.submit(execute)
            self.assertEqual(first.result(timeout=15),second.result(timeout=15))
        self.assertEqual(SolicitudCompraDepartamental.objects.count(),1)
        self.assertEqual(ItemCompraDepartamental.objects.count(),2)
        self.assertEqual(AuditLog.objects.filter(action='AI_PURCHASE_REQUEST_CREATE').count(),1)

    def test_parallel_different_drafts_do_not_duplicate_business_request(self):
        from concurrent.futures import ThreadPoolExecutor
        from django.db import close_old_connections
        first_dto=self.prepare()
        self.conversation=create_chat_conversation(user=self.user)
        self.messages=create_user_turn(user=self.user,conversation=self.conversation,content='Misma solicitud, otra conversación')
        second_dto=self.prepare()
        def execute(dto):
            close_old_connections()
            try:
                try:return self.confirm(dto)['status']
                except WorkflowError as exc:return exc.code
            finally:close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(execute,first_dto);second=pool.submit(execute,second_dto)
            results=[first.result(timeout=15),second.result(timeout=15)]
        self.assertEqual(results.count('EXECUTED'),1)
        self.assertEqual(SolicitudCompraDepartamental.objects.count(),1)
        self.assertEqual(ItemCompraDepartamental.objects.count(),2)
