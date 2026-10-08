"""Real PostgreSQL quota and admission; OpenAI is always a provider double."""
import json
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.db import close_old_connections, connection, transaction
from django.test import TransactionTestCase, override_settings

from activos.models import Activo
from api.ai_gateway_services import list_read_shadow_tools, invoke_read_shadow_tool
from core.models import AuditLog, Sucursal, UserProfile
from orquestacion.models import ChatConversation, ChatMessage
from orquestacion.services import agent_pilot as pilot
from orquestacion.services.chat_service import create_chat_conversation, create_user_turn, execute_chat_turn, get_chat_runtime_status


@override_settings(AI_AGENT_READ_ENABLED=True, AI_GATEWAY_ASSETS_ENABLED=True,
                   AI_AGENT_READ_MODEL=pilot.MODEL, OPENAI_API_KEY='fake-test-key')
class AgentPilotTests(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser(username='pilot', password='test')
        self.other = get_user_model().objects.create_superuser(username='outside', password='test')
        self.branch = Sucursal.objects.create(codigo='PILOT', nombre='Pilot')
        UserProfile.objects.create(user=self.user, sucursal=self.branch)
        self.asset = Activo.objects.create(codigo='P-1', nombre='Horno piloto', sucursal=self.branch)
        override = override_settings(AI_AGENT_PILOT_USER_ID=self.user.pk)
        override.enable(); self.addCleanup(override.disable)
        self.chat = create_chat_conversation(user=self.user)
        self.messages = create_user_turn(user=self.user, conversation=self.chat, content='Busca hornos')
        self.payload = dict(model=pilot.MODEL, service_tier='default', store=False,
                            max_output_tokens=pilot.OUTPUT_TOKENS, input=[{'role':'user','content':'Hola'}], tools=[])

    def reserve(self, turn=None, cycle=1, payload=None):
        return pilot.reserve_request(user=self.user, turn_id=turn or uuid4(), cycle=cycle,
                                     payload=payload or self.payload)

    def receipt(self, amount='0.01', turn=None, cycle=1):
        return AuditLog.objects.create(user=self.user, action=pilot.ACTION, model=pilot.LEDGER_MODEL,
            object_id=pilot.PILOT_ID, payload={'turn_id':str(turn or uuid4()), 'cycle':cycle,
                                             'model':pilot.MODEL, 'reserved_usd':amount})

    def run_provider(self, failure=None):
        provider = Mock()
        provider.with_options.return_value = provider
        provider.responses.create.return_value = SimpleNamespace(output=[], output_text='Hola', status='completed', model=pilot.MODEL, service_tier='default')
        if failure: provider.responses.create.side_effect = failure
        with patch('openai.OpenAI', return_value=provider), patch('orquestacion.services.chat_service._model_client', side_effect=AssertionError('legacy fallback forbidden')):
            result = execute_chat_turn(user=self.user, conversation=self.chat, user_message=self.messages[0], assistant_message=self.messages[1])
        return provider, result

    def test_only_configured_active_user_even_other_superuser_denied(self):
        self.assertTrue(pilot.is_pilot_participant(self.user))
        self.assertFalse(pilot.is_pilot_participant(self.other))
        self.assertEqual(list_read_shadow_tools(user=self.other), [])
        with self.assertRaises(PermissionDenied):
            invoke_read_shadow_tool(user=self.other, tool_key='erp.search_assets', arguments={})
        get_user_model().objects.filter(pk=self.user.pk).update(is_active=False)
        self.assertFalse(pilot.is_pilot_participant(self.user))
        self.assertEqual(list_read_shadow_tools(user=self.user), [])

    def test_invalid_participant_configuration_fails_closed(self):
        for value in (0, None, True, str(self.user.pk), [self.user.pk]):
            with self.subTest(value=value), override_settings(AI_AGENT_PILOT_USER_ID=value):
                self.assertFalse(pilot.is_pilot_participant(self.user))
                self.assertEqual(list_read_shadow_tools(user=self.user), [])

    def test_reserve_commits_before_network_and_timeout_is_not_refunded(self):
        def request(**kwargs):
            self.assertFalse(connection.in_atomic_block)
            self.assertEqual(AuditLog.objects.filter(action=pilot.ACTION).count(), 1)
            self.assertEqual(kwargs['service_tier'], 'default')
            self.assertEqual(kwargs['model'], pilot.MODEL)
            raise TimeoutError('private provider error')
        provider = Mock(); provider.with_options.return_value=provider
        provider.responses.create.side_effect=request
        with patch('openai.OpenAI', return_value=provider):
            execute_chat_turn(user=self.user, conversation=self.chat, user_message=self.messages[0], assistant_message=self.messages[1])
        self.assertEqual(pilot.pilot_status(self.user)['turns'], 1)
        self.messages[1].refresh_from_db()
        self.assertEqual(self.messages[1].metadata_json['agent_read']['error_code'], 'provider_failed')
        self.assertNotIn('private provider error', self.messages[1].content)

    def test_twenty_turns_across_chats_then_no_provider_or_auto_renewal(self):
        for _ in range(20): self.reserve()
        provider, result = self.run_provider()
        provider.responses.create.assert_not_called()
        self.assertIn('20 turnos', result.assistant_text)
        self.assertFalse(get_chat_runtime_status(self.user)['ready'])
        with self.assertRaises(pilot.PilotStopped) as caught: self.reserve()
        self.assertEqual(caught.exception.code, 'pilot_turn_limit')
        self.assertEqual(pilot.pilot_status(self.user)['turns'], 20)

    def test_last_admitted_turn_can_finish_its_cycles(self):
        for _ in range(19): self.reserve()
        turn=uuid4(); self.reserve(turn); self.reserve(turn, cycle=2)
        self.assertEqual(pilot.pilot_status(self.user)['turns'], 20)

    def test_dollar_limit_blocks_before_provider(self):
        self.receipt('0.99')
        provider, result=self.run_provider()
        provider.responses.create.assert_not_called()
        self.assertIn('saldo reservado', result.assistant_text)
        self.assertEqual(pilot.pilot_status(self.user)['reserved_usd'], '0.99')

    def test_same_cycle_never_dispatched_twice_and_sequential_rounds_required(self):
        turn=uuid4(); self.reserve(turn)
        for cycle in (1, 3):
            with self.assertRaises(pilot.PilotStopped) as caught:self.reserve(turn, cycle=cycle)
            self.assertEqual(caught.exception.code, 'pilot_duplicate_request')
        self.reserve(turn, cycle=2)
        self.assertEqual(pilot.pilot_status(self.user)['turns'], 1)

    def test_two_connections_compete_for_last_turn_only_one_wins(self):
        for _ in range(19): self.reserve()
        def attempt():
            close_old_connections()
            try:
                self.reserve(); return 'reserved'
            except pilot.PilotStopped as exc:return exc.code
            finally:connection.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:attempt(), range(2)))
        self.assertCountEqual(results, ['reserved','pilot_turn_limit'])
        self.assertEqual(pilot.pilot_status(self.user)['turns'], 20)

    def test_two_connections_compete_for_last_dollars_only_one_wins(self):
        amount=Decimal(self.reserve()['reserved_usd'])
        AuditLog.objects.filter(model=pilot.LEDGER_MODEL).delete()
        self.receipt(str(pilot.MAX_USD - amount))
        def attempt():
            close_old_connections()
            try:self.reserve();return 'reserved'
            except pilot.PilotStopped as exc:return exc.code
            finally:connection.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:attempt(), range(2)))
        self.assertCountEqual(results, ['reserved','pilot_spend_limit'])
        self.assertEqual(Decimal(pilot.pilot_status(self.user)['reserved_usd']), pilot.MAX_USD)

    def test_ledger_survives_chat_deletion_reload_and_participant_change(self):
        self.reserve(self.messages[1].public_id)
        self.chat.delete()
        connection.close()
        self.assertEqual(pilot.pilot_status(self.user)['turns'], 1)
        with override_settings(AI_AGENT_PILOT_USER_ID=self.other.pk):
            self.assertEqual(pilot.pilot_status(self.other)['turns'], 1)

    def test_corrupt_ledger_and_wrong_model_fail_closed_before_provider(self):
        self.receipt('NaN')
        provider,_=self.run_provider();provider.responses.create.assert_not_called()
        self.messages[1].refresh_from_db()
        self.assertEqual(self.messages[1].metadata_json['agent_read']['error_code'], 'pilot_ledger_invalid')
        AuditLog.objects.filter(model=pilot.LEDGER_MODEL).delete()
        self.messages=create_user_turn(user=self.user,conversation=self.chat,content='Otra consulta')
        with override_settings(AI_AGENT_READ_MODEL='other-model'):
            provider,_=self.run_provider();provider.responses.create.assert_not_called()

    def test_request_cap_untrusted_tier_and_outer_transaction_rejected(self):
        for change, code in (({'service_tier':'auto'},'pilot_configuration_invalid'),
                             ({'model':'other'},'pilot_configuration_invalid'),
                             ({'max_output_tokens':1401},'pilot_configuration_invalid'),
                             ({'input':'x'*60001},'pilot_request_limit')):
            with self.subTest(change=change), self.assertRaises(pilot.PilotStopped) as caught:
                self.reserve(payload={**self.payload,**change})
            self.assertEqual(caught.exception.code,code)
        with transaction.atomic(), self.assertRaises(pilot.PilotStopped) as caught:self.reserve()
        self.assertEqual(caught.exception.code,'pilot_transaction_boundary')
        self.assertEqual(AuditLog.objects.filter(action=pilot.ACTION).count(),0)

    def test_replay_has_no_new_charge_but_revocation_never_falls_back(self):
        self.run_provider()
        before=pilot.pilot_status(self.user)
        provider,_=self.run_provider();provider.responses.create.assert_not_called()
        self.assertEqual(pilot.pilot_status(self.user),before)
        with override_settings(AI_AGENT_PILOT_USER_ID=0):
            provider,_=self.run_provider();provider.responses.create.assert_not_called()
        self.messages=create_user_turn(user=self.user,conversation=self.chat,content='Consulta otra vez')
        with override_settings(AI_AGENT_PILOT_USER_ID=0):
            provider,_=self.run_provider();provider.responses.create.assert_not_called()

    def test_legacy_model_configuration_does_not_change_read_model(self):
        with override_settings(PRIVATE_AI_CHAT_MODEL='legacy-only'):
            provider,_=self.run_provider()
        self.assertEqual(provider.responses.create.call_args.kwargs['model'],pilot.MODEL)
        self.messages[1].refresh_from_db()
        self.assertEqual(self.messages[1].metadata_json['agent_read']['provider'], [{'model':pilot.MODEL,'service_tier':'default'}])

    def test_first_inflight_read_marks_chat_before_revocation_or_crash(self):
        def request(**kwargs):
            stored = ChatMessage.objects.get(pk=self.messages[1].pk)
            self.assertTrue(stored.metadata_json['agent_read'].get('execution_pending'))
            self.assertEqual(stored.status, 'streaming')
            following = create_user_turn(user=self.user, conversation=self.chat, content='Siguiente consulta')
            with override_settings(AI_AGENT_PILOT_USER_ID=0), patch('orquestacion.services.chat_service._model_client') as legacy:
                execute_chat_turn(user=self.user, conversation=self.chat, user_message=following[0], assistant_message=following[1])
            legacy.assert_not_called()
            following[1].refresh_from_db(); self.assertEqual(following[1].status, 'error')
            raise TimeoutError('simulated interrupted first turn')
        provider=Mock();provider.with_options.return_value=provider;provider.responses.create.side_effect=request
        with patch('openai.OpenAI',return_value=provider):
            execute_chat_turn(user=self.user,conversation=self.chat,user_message=self.messages[0],assistant_message=self.messages[1])
        self.assertEqual(provider.responses.create.call_count,1)
        self.assertEqual(pilot.pilot_status(self.user)['turns'],1)

    def test_deadline_and_permissions_rechecked_after_reservation(self):
        for expired in (False, True):
            with self.subTest(expired=expired):
                self.messages=create_user_turn(user=self.user,conversation=self.chat,content='Consulta')
                clock=[0]
                original=pilot.reserve_request
                def reserve(**kwargs):
                    result=original(**kwargs)
                    if expired:clock[0]=61
                    else:get_user_model().objects.filter(pk=self.user.pk).update(is_active=False)
                    return result
                provider=Mock();provider.with_options.return_value=provider
                with patch('openai.OpenAI',return_value=provider), patch('orquestacion.services.agent_pilot.reserve_request',side_effect=reserve), patch('orquestacion.services.agent_read_runtime.monotonic',side_effect=lambda:clock[0]):
                    execute_chat_turn(user=self.user,conversation=self.chat,user_message=self.messages[0],assistant_message=self.messages[1])
                provider.responses.create.assert_not_called()
                self.messages[1].refresh_from_db()
                self.assertEqual(self.messages[1].metadata_json['agent_read']['error_code'],'time_limit' if expired else 'runtime_failed')
                get_user_model().objects.filter(pk=self.user.pk).update(is_active=True)
        self.assertEqual(pilot.pilot_status(self.user)['turns'],2)

    def test_lock_contention_is_bounded_and_reserves_nothing(self):
        from threading import Event
        from django.db import OperationalError
        locked,release=Event(),Event()
        def holder():
            close_old_connections()
            try:
                with transaction.atomic():
                    with connection.cursor() as cursor:cursor.execute('SELECT pg_advisory_xact_lock(%s)',[pilot.LOCK_ID])
                    locked.set();release.wait(10)
            finally:connection.close()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(holder)
            self.assertTrue(locked.wait(5))
            try:
                with self.assertRaises(OperationalError):self.reserve()
            finally:release.set();future.result(10)
        self.assertEqual(AuditLog.objects.filter(action=pilot.ACTION).count(),0)

    @override_settings(AI_AGENT_WORKFLOWS_ENABLED=True)
    def test_http_gateway_and_workflows_reject_nonparticipant(self):
        from rest_framework.test import APIClient
        from orquestacion.services.agent_workflows import create_workflow
        workflow=create_workflow(user=self.user,kind='CONSULT_ASSET_MAINTENANCE',origin_request_id=uuid4(),
                                 conversation_id=self.chat.public_id,payload={'query':'Horno'})
        client=APIClient();client.force_authenticate(self.other)
        response=client.post('/api/ai-gateway/tools/erp.search_assets/invoke/',{'arguments':{}},format='json')
        self.assertEqual(response.status_code,403)
        response=client.get('/api/ai-gateway/workflows/')
        self.assertEqual(response.status_code,403)
        response=client.get('/api/ai-gateway/workflows/'+workflow['public_id']+'/')
        self.assertEqual(response.status_code,403)
        self.assertNotIn('Horno piloto',response.content.decode())
        self.assertEqual(AuditLog.objects.filter(action=pilot.ACTION).count(),0)

    def test_quota_does_not_override_branch_scope(self):
        outsider=get_user_model().objects.create_user(username='no-branch-pilot')
        with override_settings(AI_AGENT_PILOT_USER_ID=outsider.pk):
            self.assertTrue(pilot.is_pilot_participant(outsider))
            self.assertEqual(list_read_shadow_tools(user=outsider),[])
            with self.assertRaises(PermissionDenied):
                invoke_read_shadow_tool(user=outsider,tool_key='erp.search_assets',arguments={})
