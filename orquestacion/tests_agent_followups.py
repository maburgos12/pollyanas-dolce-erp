from datetime import timedelta
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from PIL import Image
from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, TransactionTestCase, override_settings, Client
from django.urls import reverse
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from core.models import Sucursal, UserProfile, AuditLog, UserModuleAccess
from fallas.models import ReporteFalla, BitacoraFalla, EvidenciaSeguimientoFalla, CategoriaFalla
from mantenimiento.evidence_validation import EvidenceValidationError
from orquestacion.services import agent_followups as followups, agent_incidents as incidents
from orquestacion.services.agent_workflows import WorkflowError
from orquestacion.services.chat_service import create_chat_conversation, create_user_turn


@override_settings(AI_AGENT_READ_ENABLED=True, AI_GATEWAY_ASSETS_ENABLED=True, AI_AGENT_INCIDENTS_ENABLED=True)
class FollowupTests(TestCase):
    def setUp(self):
        self.media = TemporaryDirectory(); self.addCleanup(self.media.cleanup)
        settings = override_settings(MEDIA_ROOT=self.media.name)
        settings.enable(); self.addCleanup(settings.disable)
        self.user = get_user_model().objects.create_superuser('followup-dg', 'dg@example.test', 'test-password')
        settings = override_settings(AI_AGENT_PILOT_USER_ID=self.user.pk)
        settings.enable(); self.addCleanup(settings.disable)
        self.other = get_user_model().objects.create_superuser('followup-other', 'other@example.test', 'test-password')
        self.branch = Sucursal.objects.create(codigo='FOLLOW-A', nombre='Sucursal El Tunel')
        self.category = CategoriaFalla.objects.create(nombre='Instalación', tipo='instalacion')
        self.report = ReporteFalla.objects.create(sucursal=self.branch, categoria=self.category,
            tipo_objetivo='INSTALACION', titulo='Filtración debajo del mostrador', descripcion='Sellar las juntas.',
            estatus='en_proceso', proveedor_servicio='Pedro Navarez', costo_estimado='2500', reportado_por=self.user,
            fecha_reporte=timezone.now() - timedelta(days=5))
        self.conversation = create_chat_conversation(user=self.user)
        self.messages = create_user_turn(user=self.user, conversation=self.conversation, content='Pedro terminó ayer; pago pendiente y misma cotización.')
        self.arguments = {'report_id': self.report.pk, 'estatus': 'resuelto',
            'fecha_trabajo_finalizado': (timezone.localdate() - timedelta(days=1)).isoformat(),
            'comentario': 'Trabajo terminado ayer por Pedro. Pago pendiente; cotización sin cambios.'}

    def prepare(self, arguments=None, call='first'):
        return followups.invoke(user=self.user, tool_key=followups.KEY, arguments=self.arguments if arguments is None else arguments,
            conversation=self.conversation, user_message=self.messages[0], assistant_message=self.messages[1], call_id=call)['result']['payload']['incident']

    def command(self, dto):
        return {'confirm': True, 'expected_version': dto['version'], 'payload_hash': dto['payload_hash']}

    def photo(self, name='photo.jpg', color='red'):
        out=BytesIO(); Image.new('RGB', (10, 10), color).save(out, 'JPEG')
        return SimpleUploadedFile(name, out.getvalue(), content_type='image/jpeg')

    def attach(self, dto, files=None):
        return followups.attach(user=self.user, draft_id=dto['draft_id'], arguments=self.command(dto),
            files=files or [self.photo()])['result']['payload']['incident']

    def confirm(self, dto, user=None):
        return incidents.confirm(user=user or self.user, draft_id=dto['draft_id'], arguments=self.command(dto))['result']['payload']['incident']

    def test_find_existing_installation_without_asset_and_read_quote(self):
        result = followups.invoke(user=self.user, tool_key='incident.search_reports', arguments={'q':'Tunel'},
            conversation=self.conversation, user_message=self.messages[0], assistant_message=self.messages[1], call_id='search')
        self.assertEqual(result['result']['payload']['reports'][0]['id'], self.report.pk)
        result = followups.invoke(user=self.user, tool_key='incident.get_report', arguments={'report_id':self.report.pk},
            conversation=self.conversation, user_message=self.messages[0], assistant_message=self.messages[1], call_id='read')
        self.assertEqual(result['result']['payload']['reports'][0]['costo_estimado'], '2500.00')

    def test_prepare_does_not_write_report_then_confirm_replay_and_audit(self):
        dto = self.prepare()
        self.report.refresh_from_db(); self.assertEqual(self.report.estatus, 'en_proceso')
        self.assertEqual(BitacoraFalla.objects.count(), 0)
        dto = self.attach(dto, [self.photo('one.jpg'), self.photo('two.jpg','blue'), self.photo('three.jpg','green')])
        result = self.confirm(dto)
        self.assertEqual(result['status'], 'EXECUTED')
        self.assertEqual(self.confirm(dto), result)
        self.report.refresh_from_db()
        self.assertEqual(ReporteFalla.objects.count(), 1)
        self.assertEqual(self.report.estatus, 'resuelto')
        self.assertEqual(self.report.fecha_trabajo_finalizado.isoformat(), self.arguments['fecha_trabajo_finalizado'])
        from mantenimiento.services_history import item_detail
        self.assertEqual(item_detail(self.user, 'falla', self.report.pk)['fechas']['trabajo_finalizado'], self.arguments['fecha_trabajo_finalizado'])
        self.assertEqual(timezone.localtime(self.report.fecha_resolucion).date(), timezone.localdate())
        self.assertIsNone(self.report.tiempo_resolucion_horas)
        self.assertEqual(str(self.report.costo_estimado), '2500.00'); self.assertIsNone(self.report.costo_real)
        self.assertEqual(self.report.proveedor_servicio, 'Pedro Navarez')
        self.assertEqual(BitacoraFalla.objects.count(), 1); self.assertEqual(EvidenciaSeguimientoFalla.objects.count(), 3)
        self.assertTrue(AuditLog.objects.filter(action='AI_FOLLOWUP_UPDATE', payload__confirmation=True).exists())
        self.assertEqual(len(list(Path(self.media.name).rglob('*.jpg'))), 3)
        self.report.costo_estimado='3000';self.report.save(update_fields=['costo_estimado'])
        self.assertEqual(self.confirm(dto)['report']['costo_estimado'],'2500.00')
        EvidenciaSeguimientoFalla.objects.first().delete()
        with self.assertRaises(WorkflowError): self.confirm(dto)

    def test_missing_date_multiturn_and_photos_survive_continuation(self):
        dto = self.prepare({'report_id':self.report.pk, 'estatus':'resuelto', 'comentario':'Trabajo terminado.'})
        self.assertIn('fecha_trabajo_finalizado', dto['missing_fields'])
        with self.assertRaises(WorkflowError): self.confirm(dto)
        dto = self.prepare({**self.arguments,'draft_id':dto['draft_id'],'expected_version':dto['version']}, 'continue')
        attached = self.attach(dto)
        continued = self.prepare({'draft_id':attached['draft_id'],'expected_version':attached['version'], 'comentario':'Pago pendiente.'}, 'comment')
        self.assertEqual(continued['files'], attached['files'])
        self.assertNotEqual(continued['payload_hash'], attached['payload_hash'])
        with self.assertRaises(WorkflowError): self.confirm(attached)
        self.assertEqual(self.confirm(continued)['status'], 'EXECUTED')

    def test_permissions_participant_ownership_and_scope_are_fresh(self):
        dto=self.prepare()
        with self.assertRaises(WorkflowError): self.confirm(dto, self.other)
        with override_settings(AI_AGENT_INCIDENTS_ENABLED=False):
            with self.assertRaises(WorkflowError): self.confirm(dto)
        with override_settings(AI_AGENT_PILOT_USER_ID=self.other.pk):
            with self.assertRaises(WorkflowError): self.confirm(dto)
        self.user.is_superuser=False; self.user.is_staff=False; self.user.save()
        with self.assertRaises(WorkflowError): self.confirm(dto)
        self.report.refresh_from_db(); self.assertEqual(self.report.estatus, 'en_proceso')

    def test_scoped_manager_cannot_reach_other_branch(self):
        self.user.is_superuser=False; self.user.is_staff=False; self.user.save()
        UserProfile.objects.create(user=self.user, sucursal=self.branch)
        UserModuleAccess.objects.create(user=self.user, module='fallas.gestion', access='manage')
        UserModuleAccess.objects.create(user=self.user, module='mantenimiento.app', access='view')
        with self.assertRaises(WorkflowError): self.prepare()

    def test_sensitive_fields_or_model_confirmation_and_bad_dates_rejected(self):
        for key in ('costo_real','costo_estimado','proveedor_servicio','confirm','user_id','files'):
            with self.subTest(key=key), self.assertRaises(ValidationError): self.prepare({**self.arguments,key:True})
        for value in (timezone.localdate()+timedelta(days=1), timezone.localdate()-timedelta(days=10)):
            with self.assertRaises(WorkflowError): self.prepare({**self.arguments,'fecha_trabajo_finalizado':value.isoformat()})
        with self.assertRaises(ValidationError): self.prepare({**self.arguments,'fecha_trabajo_finalizado':'ayer'})

    def test_changed_report_duplicate_or_final_state_requires_review(self):
        dto=self.prepare()
        self.report.costo_estimado='2600'; self.report.save()
        with self.assertRaises(WorkflowError): self.confirm(dto)
        self.assertEqual(BitacoraFalla.objects.count(), 0)
        self.report.estatus='resuelto'; self.report.save()
        with self.assertRaises(WorkflowError): self.prepare(call='another')

    def test_linked_duplicate_is_not_silently_finalized(self):
        ReporteFalla.objects.create(sucursal=self.branch, categoria=self.category, titulo='Repetido', reportado_por=self.user, duplicado_de=self.report)
        with self.assertRaises(WorkflowError): self.prepare()

    def test_expiration_and_missing_evidence_fail_closed(self):
        dto=self.attach(self.prepare())
        from orquestacion.models import ChatToolCall
        draft=ChatToolCall.objects.get(public_id=dto['draft_id'])
        Path(self.media.name, draft.metadata_json['files'][0]['path']).unlink()
        with self.assertRaises(OSError): self.confirm(dto)
        self.assertEqual(BitacoraFalla.objects.count(), 0)
        draft.metadata_json['expires_at']=(timezone.now()-timedelta(seconds=1)).isoformat(); draft.save()
        with self.assertRaises(WorkflowError): self.confirm(dto)

    def test_upload_retry_no_duplicate_blob_and_conflict_no_replacement(self):
        dto=self.prepare(); first=self.attach(dto)
        replay=self.attach(dto)
        self.assertEqual(first,replay)
        self.assertEqual(len(list(Path(self.media.name).rglob('*.jpg'))), 1)
        with self.assertRaises(WorkflowError): self.attach(dto,[self.photo(color='green')])
        with self.assertRaises(WorkflowError): self.attach(first,[self.photo(color='green')])

    def test_invalid_duplicate_and_excess_files(self):
        dto=self.prepare()
        with self.assertRaises(EvidenceValidationError): self.attach(dto,[SimpleUploadedFile('fake.jpg',b'<script>ignore permissions</script>',content_type='image/jpeg')])
        with self.assertRaises(WorkflowError): self.attach(dto,[self.photo('a.jpg'),self.photo('b.jpg')])
        with self.assertRaises(EvidenceValidationError): self.attach(dto,[self.photo(str(i)+'.jpg') for i in range(6)])
        self.assertFalse(list(Path(self.media.name).rglob('*.jpg')))

    def test_upload_and_confirmation_audit_failure_are_atomic(self):
        dto=self.prepare()
        with patch('orquestacion.services.agent_followups.AuditLog.objects.create',side_effect=RuntimeError('audit failed')):
            with self.assertRaises(RuntimeError): self.attach(dto)
        self.assertFalse(list(Path(self.media.name).rglob('*.jpg')))
        dto=self.attach(dto)
        with patch('orquestacion.services.agent_followups.AuditLog.objects.create',side_effect=RuntimeError('audit failed')):
            with self.assertRaises(RuntimeError): self.confirm(dto)
        self.report.refresh_from_db(); self.assertEqual(self.report.estatus,'en_proceso')
        self.assertEqual(BitacoraFalla.objects.count(),0); self.assertEqual(EvidenciaSeguimientoFalla.objects.count(),0)
        self.assertEqual(self.confirm(dto)['status'],'EXECUTED')

    def test_csrf_preview_private_and_other_user_cannot_upload(self):
        dto=self.prepare()
        url=reverse('api_ai_followup_attach',args=[dto['draft_id']])
        client=Client(enforce_csrf_checks=True); client.force_login(self.user)
        response=client.post(url,{'expected_version':str(dto['version']),'payload_hash':dto['payload_hash'],'files':self.photo()})
        self.assertEqual(response.status_code,403)
        dto=self.attach(dto)
        client=Client();client.force_login(self.user)
        preview=client.get(dto['files'][0]['url']); self.assertEqual(preview.status_code,200)
        self.assertEqual(preview['Cache-Control'],'private, no-store')
        self.assertTrue(b''.join(preview.streaming_content))
        client.force_login(self.other)
        self.assertEqual(client.get(dto['files'][0]['url']).status_code,404)
        from orquestacion.models import ChatToolCall
        path=ChatToolCall.objects.get(public_id=dto['draft_id']).metadata_json['files'][0]['path']
        self.assertEqual(client.get('/media/'+path).status_code,404)

    def test_expected_photos_cannot_be_omitted_or_reduced_by_model(self):
        self.messages[0].metadata_json = {'expected_photo_count':3}; self.messages[0].save()
        dto=self.prepare()
        self.assertIn('fotografías',dto['missing_fields'])
        with self.assertRaises(WorkflowError): self.confirm(dto)
        with self.assertRaises(WorkflowError): self.attach(dto,[self.photo()])
        self.assertFalse(list(Path(self.media.name).rglob('*.jpg')))
        dto=self.prepare({'draft_id':dto['draft_id'],'expected_version':dto['version'],'evidence_count':0},'reduce')
        self.assertIn('fotografías',dto['missing_fields'])

    def test_evidence_tampering_and_staging_quota_fail_before_operational_write(self):
        from orquestacion.models import ChatToolCall
        dto=self.attach(self.prepare())
        draft=ChatToolCall.objects.get(public_id=dto['draft_id'])
        path=Path(self.media.name, draft.metadata_json['files'][0]['path'])
        path.write_bytes(b'changed')
        with self.assertRaises(WorkflowError): self.confirm(dto)
        self.assertFalse(BitacoraFalla.objects.exists())
        draft.metadata_json['files'][0]['bytes']=100*1024*1024; draft.save()
        second=self.prepare(call='second')
        with self.assertRaises(WorkflowError) as error: self.attach(second,[self.photo(color='blue')])
        self.assertEqual(error.exception.code,'followup_staging_limit')
        self.assertEqual(len(list(Path(self.media.name).rglob('*.jpg'))),1)

    @override_settings(AI_AGENT_READ_MODEL='gpt-6.1-sol', OPENAI_API_KEY='fake-test-key')
    def test_followup_receipt_is_reprojected_in_natural_runtime_history(self):
        import json
        from types import SimpleNamespace
        from unittest.mock import Mock
        from orquestacion.services.chat_service import execute_chat_turn, serialize_message
        dto=self.confirm(self.attach(self.prepare()))
        messages=create_user_turn(user=self.user,conversation=self.conversation,content='¿Quedó actualizado?')
        provider=Mock();provider.with_options.return_value=provider
        provider.responses.create.return_value=SimpleNamespace(output=[],usage=None,output_text=json.dumps({
            'answer':f"Reporte #{self.report.pk} actualizado; pago pendiente.",
            'evidence_ids':[dto['draft_id']], 'action_claim':'receipt','incident_ids':[dto['draft_id']]}))
        with patch('openai.OpenAI',return_value=provider), patch('orquestacion.services.agent_pilot.reserve_request',return_value={}):
            result=execute_chat_turn(user=self.user,conversation=self.conversation,user_message=messages[0],assistant_message=messages[1])
        self.assertIn('actualizado',result.assistant_text)
        messages[1].refresh_from_db()
        projected=serialize_message(messages[1])
        self.assertEqual(projected['presentation'],'natural')
        self.assertEqual(projected['receipts'][0]['kind'],'followup')
        self.assertEqual(projected['receipts'][0]['report_id'],self.report.pk)
        self.assertEqual(messages[1].metadata_json['agent_read']['report_ids'],[self.report.pk])
        with override_settings(AI_AGENT_INCIDENTS_ENABLED=False):
            self.assertEqual(serialize_message(messages[1])['receipts'],[])
        self.assertEqual(BitacoraFalla.objects.count(),1)

    @override_settings(AI_AGENT_READ_MODEL='gpt-6.1-sol', OPENAI_API_KEY='fake-test-key')
    def test_real_runtime_dispatches_followup_and_keeps_false_execution_claim_out(self):
        import json
        from types import SimpleNamespace
        from unittest.mock import Mock
        from orquestacion.services.chat_service import execute_chat_turn
        provider=Mock();provider.with_options.return_value=provider
        provider.responses.create.side_effect=[
            SimpleNamespace(output=[{'type':'function_call','id':'fc1','call_id':'read1','name':'erp_get_failure_report','arguments':json.dumps({'report_id':self.report.pk})}],output_text='',usage=None),
            SimpleNamespace(output=[{'type':'function_call','id':'fc2','call_id':'prepare2','name':'erp_prepare_failure_followup','arguments':json.dumps(self.arguments)}],output_text='',usage=None),
            SimpleNamespace(output=[],output_text='Ya pagué y cerré el reporte.',usage=None)]
        with patch('openai.OpenAI',return_value=provider),patch('orquestacion.services.agent_pilot.reserve_request',return_value={}):
            result=execute_chat_turn(user=self.user,conversation=self.conversation,user_message=self.messages[0],assistant_message=self.messages[1])
        self.assertEqual(len(result.tool_events),2)
        self.assertEqual(result.tool_events[1]['payload']['result']['status'],'AWAITING_CONFIRMATION')
        self.assertNotIn('Ya pagué',result.assistant_text)
        self.report.refresh_from_db();self.assertEqual(self.report.estatus,'en_proceso')
        self.assertFalse(BitacoraFalla.objects.exists())
        from orquestacion.models import ChatToolCall
        self.assertEqual(ChatToolCall.objects.filter(tool_key=followups.KEY).count(),1)
        tools=provider.responses.create.call_args_list[0].kwargs['tools']
        self.assertFalse(any('confirm' in row['name'] for row in tools))


@override_settings(AI_AGENT_READ_ENABLED=True,AI_GATEWAY_ASSETS_ENABLED=True,AI_AGENT_INCIDENTS_ENABLED=True)
class FollowupConcurrencyTests(TransactionTestCase):
    setUp=FollowupTests.setUp
    prepare=FollowupTests.prepare
    command=FollowupTests.command
    photo=FollowupTests.photo
    attach=FollowupTests.attach
    confirm=FollowupTests.confirm

    def test_two_confirmations_produce_one_bitacora_and_one_evidence(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections
        dto=self.attach(self.prepare())
        barrier=Barrier(2)
        def worker(_):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                return self.confirm(dto)['report_id']
            finally:close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as executor:
            ids=list(executor.map(worker,range(2)))
        self.assertEqual(ids,[self.report.pk,self.report.pk])
        self.assertEqual(BitacoraFalla.objects.count(),1)
        self.assertEqual(EvidenciaSeguimientoFalla.objects.count(),1)
        self.assertEqual(AuditLog.objects.filter(action='AI_FOLLOWUP_UPDATE').count(),1)
