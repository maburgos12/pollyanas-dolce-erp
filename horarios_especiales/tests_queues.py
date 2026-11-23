from datetime import timedelta
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth.models import User
from django.core.management import call_command
from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from django_celery_beat.models import PeriodicTask

from horarios_especiales.models import SolicitudHorarioEspecial
from horarios_especiales.tasks import reconcile_approved_hours_task


class HoursQueueTests(TestCase):
    def setUp(self):
        self.actor = User.objects.create_user('queue-owner', is_superuser=True)
        broker = patch('celery.current_app.connection_for_write')
        self.broker = broker.start().return_value.__enter__.return_value
        self.broker.default_channel.queue_declare.return_value.message_count = 0
        self.addCleanup(broker.stop)

    def request(self, **overrides):
        return SolicitudHorarioEspecial.objects.create(**{
            'raw_command': 'test', 'canonical_payload': {'schema_version': 2},
            'status': 'APROBADO', 'approved_at': timezone.now(), 'approved_by': self.actor,
            **overrides,
        })

    def test_routes_separate_control_publication_and_general_work(self):
        self.assertEqual(settings.CELERY_TASK_ROUTES['horarios_especiales.telegram_poll']['queue'], 'hours_control')
        self.assertEqual(settings.CELERY_TASK_ROUTES['horarios_especiales.reconcile_approved']['queue'], 'hours_control')
        self.assertEqual(settings.CELERY_TASK_ROUTES['horarios_especiales.execute_request']['queue'], 'hours_publish')

    def test_configuration_replaces_legacy_delivery_overrides(self):
        config = dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=self.actor.pk)
        with patch('horarios_especiales.management.commands.configure_hours_telegram.load_config', return_value=config):
            call_command('configure_hours_telegram')
            PeriodicTask.objects.filter(name__startswith='hours-').update(
                queue='celery', exchange='legacy', routing_key='legacy', priority=9,
                expires=timezone.now()+timedelta(days=1))
            call_command('configure_hours_telegram', enable=True)
        for task in PeriodicTask.objects.filter(name__startswith='hours-'):
            self.assertEqual(task.queue, 'hours_control')
            self.assertEqual((task.exchange, task.routing_key), (None, None))
            self.assertIsNone(task.priority)
            self.assertIsNone(task.expires)
            self.assertEqual(task.expire_seconds, 30 if task.task.endswith('telegram_poll') else 840)
            self.assertTrue(task.enabled)

    @patch('horarios_especiales.tasks.execute_special_hours_request_task.apply_async')
    def test_dispatch_only_due_approved_v2_and_reserve_until_retry(self, enqueue):
        obj = self.request()
        retry = self.request(status='FALLIDO')
        weekly = self.request(status='EJECUTADO', execution_summary_json={'next_reconcile_at': (timezone.now()-timedelta(days=1)).isoformat()})
        self.request(approved_at=None)
        self.request(cancelled_at=timezone.now())
        self.request(status='BORRADOR')
        self.request(canonical_payload={'schema_version': 1})
        self.request(execution_summary_json={'projection_expired': True})
        self.request(execution_summary_json={'next_reconcile_at': (timezone.now()+timedelta(days=1)).isoformat()})
        with patch('horarios_especiales.tasks.execute_request') as execute:
            reconcile_approved_hours_task()
            reconcile_approved_hours_task()
            execute.assert_not_called()
        self.assertEqual({call.kwargs['kwargs']['request_id'] for call in enqueue.call_args_list}, {obj.pk, retry.pk, weekly.pk})
        for call in enqueue.call_args_list:
            self.assertEqual(call.kwargs['kwargs']['actor_id'], self.actor.pk)
            self.assertEqual(call.kwargs['expires'], 840)
            self.assertFalse(call.kwargs['retry'])
        obj.refresh_from_db()
        self.assertIn('next_reconcile_at', obj.execution_summary_json)

    @patch('horarios_especiales.tasks.execute_special_hours_request_task.apply_async', side_effect=RuntimeError('broker unavailable'))
    def test_broker_failure_does_not_lose_due_intent(self, enqueue):
        obj = self.request(execution_summary_json={'consumer_readback': {'kept': True}})
        reconcile_approved_hours_task()
        obj.refresh_from_db()
        self.assertEqual(obj.execution_summary_json, {'consumer_readback': {'kept': True}})

    @patch('horarios_especiales.tasks.execute_special_hours_request_task.apply_async')
    def test_filters_before_cap_and_eventually_dispatches_beyond_first_batch(self, enqueue):
        for _ in range(101):
            self.request(execution_summary_json={'projection_expired': True})
        due = [self.request() for _ in range(101)]
        reconcile_approved_hours_task()
        self.assertEqual(enqueue.call_count, 100)
        reconcile_approved_hours_task()
        self.assertEqual(enqueue.call_count, 101)
        self.assertEqual(enqueue.call_args.kwargs['kwargs']['request_id'], due[-1].pk)

    @patch('horarios_especiales.tasks.execute_special_hours_request_task.apply_async')
    def test_pending_queue_backpressure_preserves_due_intent(self, enqueue):
        obj = self.request()
        self.broker.default_channel.queue_declare.return_value.message_count = 100
        reconcile_approved_hours_task()
        enqueue.assert_not_called()
        obj.refresh_from_db()
        self.assertEqual(obj.execution_summary_json, {})

    @patch('horarios_especiales.tasks.execute_special_hours_request_task.apply_async')
    def test_only_available_queue_capacity_is_filled(self, enqueue):
        for _ in range(3):
            self.request()
        self.broker.default_channel.queue_declare.return_value.message_count = 98
        reconcile_approved_hours_task()
        self.assertEqual(enqueue.call_count, 2)

    @patch('horarios_especiales.tasks.execute_special_hours_request_task.apply_async')
    def test_expired_reservation_recovers_without_losing_receipts(self, enqueue):
        now = timezone.now()
        obj = self.request(execution_summary_json={'consumer_readback': {'kept': True}})
        reconcile_approved_hours_task()
        with patch('django.utils.timezone.now', return_value=now + timedelta(seconds=901)):
            reconcile_approved_hours_task()
        self.assertEqual(enqueue.call_count, 2)
        obj.refresh_from_db()
        self.assertEqual(obj.execution_summary_json['consumer_readback'], {'kept': True})

    @patch('horarios_especiales.tasks.execute_special_hours_request_task.apply_async')
    def test_broker_depth_failure_leaves_due_request_unreserved(self, enqueue):
        obj = self.request()
        self.broker.ensure_connection.side_effect = RuntimeError('offline')
        result = reconcile_approved_hours_task()
        self.assertEqual(result, {'queued': 0, 'failed': 1})
        enqueue.assert_not_called()
        obj.refresh_from_db()
        self.assertEqual(obj.execution_summary_json, {})

    def test_compose_workers_are_exclusive_low_concurrency_and_private_mount_scoped(self):
        from pathlib import Path
        import re
        compose = (Path(settings.BASE_DIR) / 'docker-compose.yml').read_text()
        for name, queue in [('worker_hours_control', 'hours_control'), ('worker_hours_publish', 'hours_publish')]:
            block = re.search(r'^  ' + name + r':\n(.*?)(?=^  \w+:|\Z)', compose, re.M | re.S).group(1)
            self.assertIn('    build: .', block)
            self.assertIn('--pool=prefork --concurrency=1 --prefetch-multiplier=1', block)
            self.assertIn('-Q ' + queue + ' ', block)
            self.assertIn('${HOURS_TELEGRAM_CONFIG_DIR_HOST:-./.hours-agent}:/etc/pollyanas-hours:ro', block)


class HoursPublicationQueueTests(TransactionTestCase):
    def setUp(self):
        self.actor = User.objects.create_user('queue-owner', is_superuser=True)
        runtime = patch('horarios_especiales.services.telegram_bot.load_config', return_value=dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=self.actor.pk))
        runtime.start()
        self.addCleanup(runtime.stop)

    def test_publication_task_keeps_owner_fence_and_weekly_readback_idempotency(self):
        from django.core.exceptions import PermissionDenied
        from core.models import Sucursal
        from horarios_especiales.models import SucursalPlataformaExterna, TelegramHoursUpdate
        from horarios_especiales.services.requests import create_request_from_rules, approve_request
        from horarios_especiales.tasks import execute_special_hours_request_task
        from horarios_especiales.tests_google_projection import MemoryGoogle, make_rule
        from integraciones.services.google_business_profile.publisher import GoogleBusinessProfilePublisher
        branch = Sucursal.objects.create(codigo='QUEUE', nombre='Queue', activa=True)
        SucursalPlataformaExterna.objects.create(sucursal=branch, external_location_name='locations/queue')
        obj, payload = create_request_from_rules(raw_text='weekly', actor=self.actor,
            branch_ids=[branch.pk], rules=[make_rule('weekly', start_date=timezone.localdate().isoformat())])
        approve_request(request_obj=obj, actor=self.actor, expected_revision=payload['revision'])
        TelegramHoursUpdate.objects.create(bot_id=12345, update_id=1, request=obj,
            telegram_user_id=777, chat_id=777, state='QUEUED')
        other = User.objects.create_user('other-approver', is_superuser=True)
        client = MemoryGoogle()
        with patch('horarios_especiales.services.google_projection.GoogleBusinessProfilePublisher',
                   return_value=GoogleBusinessProfilePublisher(client=client)):
            with self.assertRaises(PermissionDenied):
                execute_special_hours_request_task(request_id=obj.pk, actor_id=other.pk)
            execute_special_hours_request_task(request_id=obj.pk, actor_id=self.actor.pk)
            execute_special_hours_request_task(request_id=obj.pk, actor_id=self.actor.pk)
        obj.refresh_from_db()
        self.assertEqual(len(client.writes), 1, obj.execution_summary_json)
        self.assertTrue(obj.execution_summary_json['ongoing_projection'])
        self.assertTrue(obj.execution_summary_json['next_reconcile_at'])
        self.assertEqual(obj.executed_by_id, self.actor.pk)
