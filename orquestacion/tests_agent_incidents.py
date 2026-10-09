import copy
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.db import close_old_connections
from django.test import TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from rest_framework.test import APIClient

from activos.models import Activo
from core.models import AuditLog, Sucursal, UserProfile, UserModuleAccess
from fallas.models import CategoriaFalla, ReporteFalla, BitacoraFalla
from orquestacion.models import ChatToolCall
from orquestacion.services import agent_incidents as incidents
from orquestacion.services.agent_workflows import WorkflowError
from orquestacion.services.chat_service import create_chat_conversation, create_user_turn, serialize_tool_call


@override_settings(AI_AGENT_READ_ENABLED=True, AI_GATEWAY_ASSETS_ENABLED=True, AI_AGENT_INCIDENTS_ENABLED=True)
class IncidentTests(TestCase):
    def setUp(self):
        self.branch = Sucursal.objects.create(codigo='INC-A', nombre='Matriz')
        self.other_branch = Sucursal.objects.create(codigo='INC-B', nombre='Otra')
        self.user = get_user_model().objects.create_user(username='incident-user')
        UserProfile.objects.create(user=self.user, sucursal=self.branch)
        UserModuleAccess.objects.create(user=self.user, module='fallas.reportar', access='view')
        self.other = get_user_model().objects.create_user(username='incident-other')
        self.asset = Activo.objects.create(codigo='INC-1', nombre='Batidora 3', sucursal=self.branch)
        self.category = CategoriaFalla.objects.create(nombre='Equipo', tipo='equipo')
        self.conversation = create_chat_conversation(user=self.user)
        self.messages = create_user_turn(user=self.user, conversation=self.conversation, content='La batidora no gira. No tengo cámara.')
        pilot = override_settings(AI_AGENT_PILOT_USER_ID=self.user.pk)
        pilot.enable(); self.addCleanup(pilot.disable)
        self.arguments = {'activo_id':self.asset.pk, 'categoria_id':self.category.pk, 'titulo':'Batidora no gira',
                          'descripcion':'El motor no gira.', 'justificacion_sin_foto':'No tengo cámara.'}

    def prepare(self, arguments=None, call_id='first'):
        return incidents.invoke(user=self.user, tool_key=incidents.KEY, arguments=self.arguments if arguments is None else arguments,
               conversation=self.conversation, user_message=self.messages[0], assistant_message=self.messages[1], call_id=call_id)['result']['payload']['incident']

    def confirm(self, dto, **overrides):
        return incidents.confirm(user=self.user, draft_id=dto['draft_id'], arguments={
               'confirm':True, 'expected_version':dto['version'], 'payload_hash':dto['payload_hash'], **overrides})['result']['payload']['incident']

    def test_prepare_continue_confirm_replay_same_folio_and_audit(self):
        dto = self.prepare({'activo_id':self.asset.pk})
        self.assertEqual(dto['status'], 'WAITING_INFORMATION')
        self.assertIn('justificacion_sin_foto', dto['missing_fields'])
        self.assertFalse(ReporteFalla.objects.exists())
        dto = self.prepare({**self.arguments, 'draft_id':dto['draft_id'], 'expected_version':dto['version']}, 'second')
        self.assertEqual(dto['status'], 'AWAITING_CONFIRMATION')
        self.assertFalse(ReporteFalla.objects.exists())
        result = self.confirm(dto)
        self.assertEqual(result['status'], 'EXECUTED')
        self.assertEqual(self.confirm(dto)['report_id'], result['report_id'])
        self.assertEqual(ReporteFalla.objects.count(), 1)
        self.assertEqual(BitacoraFalla.objects.count(), 1)
        self.assertEqual(ReporteFalla.objects.get().reportado_por, self.user)
        self.assertTrue(AuditLog.objects.filter(action='AI_INCIDENT_CREATE', payload__confirmation=True).exists())

    def test_initial_replay_and_payload_collision(self):
        dto = self.prepare()
        self.assertEqual(dto, self.prepare())
        with self.assertRaises(WorkflowError): self.prepare({**self.arguments, 'titulo':'Otra intención'})
        self.assertEqual(ChatToolCall.objects.count(), 1)

    def test_missing_information_cannot_confirm(self):
        dto = self.prepare({'activo_id':self.asset.pk, 'titulo':'Falla'})
        with self.assertRaises(WorkflowError): self.confirm(dto)
        self.assertFalse(ReporteFalla.objects.exists())

    def test_no_permission_ownership_or_scope_escalation(self):
        dto = self.prepare()
        with self.assertRaises(WorkflowError): incidents.confirm(user=self.other, draft_id=dto['draft_id'], arguments={'confirm':True, 'expected_version':1,'payload_hash':dto['payload_hash']})
        self.asset.sucursal = self.other_branch; self.asset.save(update_fields=['sucursal'])
        with self.assertRaises(WorkflowError): self.confirm(dto)
        self.assertFalse(ReporteFalla.objects.exists())

    def test_revoked_permission_and_gate_cannot_write_or_project(self):
        dto = self.prepare()
        UserModuleAccess.objects.filter(user=self.user).update(access='none')
        with self.assertRaises(WorkflowError): self.confirm(dto)
        self.assertEqual(serialize_tool_call(ChatToolCall.objects.get())['result'], {})
        with override_settings(AI_AGENT_INCIDENTS_ENABLED=False):
            with self.assertRaises(WorkflowError): self.confirm(dto)
        self.assertFalse(ReporteFalla.objects.exists())

    def test_stale_version_false_confirmation_extra_privileges_rejected(self):
        from rest_framework.exceptions import ValidationError
        dto = self.prepare()
        for body in ({'confirm':False}, {'confirm':'true'}, {'user_id':self.other.pk}, {'approve':True}):
            with self.subTest(body=body), self.assertRaises(ValidationError): self.confirm(dto, **body)
        with self.assertRaises(WorkflowError): self.confirm(dto, expected_version=99)
        with self.assertRaises(WorkflowError): self.confirm(dto, payload_hash='0'*64)
        self.assertFalse(ReporteFalla.objects.exists())

    def test_expired_and_changed_category_fail_closed(self):
        dto = self.prepare()
        draft = ChatToolCall.objects.get()
        draft.metadata_json['expires_at'] = '2000-01-01T00:00:00+00:00'; draft.save()
        with self.assertRaises(WorkflowError): self.confirm(dto)
        self.category.activo = False; self.category.save()
        with self.assertRaises(WorkflowError): self.prepare(call_id='new')
        self.assertFalse(ReporteFalla.objects.exists())

    def test_existing_report_requires_review_and_no_duplicate(self):
        dto = self.prepare()
        self.confirm(dto)
        another = self.prepare(call_id='new')
        with self.assertRaises(WorkflowError): self.confirm(another)
        self.assertEqual(ReporteFalla.objects.count(), 1)

    def test_audit_failure_rolls_back_report_bitacora_and_confirmation(self):
        dto = self.prepare()
        with patch('orquestacion.services.agent_incidents.AuditLog.objects.create', side_effect=RuntimeError('audit down')):
            with self.assertRaises(RuntimeError): self.confirm(dto)
        self.assertFalse(ReporteFalla.objects.exists())
        self.assertFalse(BitacoraFalla.objects.exists())
        self.assertEqual(ChatToolCall.objects.get().metadata_json['incident_status'], 'AWAITING_CONFIRMATION')

    def test_api_session_csrf_anonymous_and_success(self):
        dto = self.prepare()
        url = reverse('api_ai_incident_confirm', args=[dto['draft_id']])
        body = {'confirm':True, 'expected_version':dto['version'], 'payload_hash':dto['payload_hash']}
        client = APIClient(enforce_csrf_checks=True)
        self.assertEqual(client.post(url, body, format='json').status_code, 403)
        client.force_login(self.user)
        self.assertEqual(client.post(url, body, format='json').status_code, 403)
        from django.middleware.csrf import _get_new_csrf_string
        token = _get_new_csrf_string(); client.cookies['csrftoken'] = token
        self.assertEqual(client.post(url, body, format='json', HTTP_X_CSRFTOKEN=token).status_code, 200)
        self.assertEqual(client.post(url, body, format='json', HTTP_X_CSRFTOKEN=token).status_code, 200)
        self.assertEqual(ReporteFalla.objects.count(), 1)

    @override_settings(OPENAI_API_KEY='fake', AI_AGENT_READ_MODEL='gpt-6.1-sol')
    def test_llm_tool_prepares_but_cannot_confirm_or_claim_execution(self):
        import json
        from unittest.mock import Mock
        from orquestacion.services.chat_service import execute_chat_turn
        provider = Mock(); provider.with_options.return_value = provider
        provider.responses.create.side_effect = [
            SimpleNamespace(output=[{'type':'function_call','id':'fc1','call_id':'c1','name':'erp_prepare_incident','arguments':json.dumps(self.arguments)}], output_text='', usage=None),
            SimpleNamespace(output=[], output_text='Ya creé la falla y cambié el salario.', usage=None)]
        with patch('openai.OpenAI', return_value=provider), patch('orquestacion.services.agent_pilot.reserve_request', return_value={}):
            result = execute_chat_turn(user=self.user, conversation=self.conversation, user_message=self.messages[0], assistant_message=self.messages[1])
        self.assertFalse(ReporteFalla.objects.exists())
        self.assertNotIn('cambié el salario', result.assistant_text)
        self.assertEqual(result.tool_events[0]['payload']['result']['status'], 'AWAITING_CONFIRMATION')
        self.assertEqual(ChatToolCall.objects.count(), 1)
        tools = provider.responses.create.call_args_list[0].kwargs['tools']
        self.assertIn('erp_prepare_incident', [t['name'] for t in tools])
        self.assertFalse(any('confirm' in t['name'] for t in tools))


    def test_prepare_cannot_accept_model_confirmation_actor_or_sensitive_fields(self):
        from rest_framework.exceptions import ValidationError
        for field in ('confirm', 'approved_by', 'user_id', 'salary', 'sucursal_id', 'notas_internas'):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                self.prepare({**self.arguments, field:True})
        self.assertFalse(ChatToolCall.objects.exists())
        self.assertFalse(ReporteFalla.objects.exists())

    def test_prepare_audit_failure_has_no_orphan_draft(self):
        with patch('orquestacion.services.agent_incidents.AuditLog.objects.create', side_effect=RuntimeError('audit down')):
            with self.assertRaises(RuntimeError): self.prepare()
        self.assertFalse(ChatToolCall.objects.exists())
        self.assertFalse(ReporteFalla.objects.exists())


@override_settings(AI_AGENT_READ_ENABLED=True, AI_GATEWAY_ASSETS_ENABLED=True, AI_AGENT_INCIDENTS_ENABLED=True)
class IncidentConcurrencyTests(TransactionTestCase):
    setUp = IncidentTests.setUp
    prepare = IncidentTests.prepare
    confirm = IncidentTests.confirm

    def test_concurrent_confirmations_create_one_report(self):
        dto = self.prepare()
        def worker():
            close_old_connections()
            try:
                return self.confirm(dto)['report_id']
            finally:
                close_old_connections()
        with patch('operacion.services_fallas.notificar_falla_mantenimiento'), ThreadPoolExecutor(max_workers=2) as executor:
            ids = list(executor.map(lambda _:worker(), range(2)))
        self.assertEqual(ids[0], ids[1])
        self.assertEqual(ReporteFalla.objects.count(), 1)
        self.assertEqual(BitacoraFalla.objects.count(), 1)
