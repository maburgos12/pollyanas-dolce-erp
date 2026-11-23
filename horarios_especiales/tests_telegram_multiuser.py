"""Explicit people, own requests, one bot budget; all transports are synthetic."""
import copy
import json
import tempfile
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth.models import User
from django.core.exceptions import PermissionDenied
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from horarios_especiales.models import TelegramHoursUpdate, TelegramHoursCursor
from horarios_especiales.services import telegram_bot as bot, telegram_audio as audio
from horarios_especiales import tests_telegram as fixtures
from horarios_especiales.tests_telegram import FakeTelegram
from horarios_especiales.tests_telegram_audio import pcm


class MultiuserTests(TestCase):
    update = fixtures.TelegramHoursTests.update

    def setUp(self):
        fixtures.TelegramHoursTests.setUp(self)
        self.member = User.objects.create_user('hours_member')
        self.config['users'] = [dict(telegram_user_id=888, telegram_chat_id=888,
                                    erp_user_id=self.member.pk, approve_own=True)]
        self.runtime = patch.object(bot, 'load_config', side_effect=lambda *a: copy.deepcopy(self.config))
        self.runtime.start()
        self.addCleanup(self.runtime.stop)

    def incoming(self, text='Matriz hoy cerrada', id=1, who=888, voice=False):
        payload = self.update(text, id)
        payload['message']['from']['id'] = who
        payload['message']['chat']['id'] = who
        if voice:
            del payload['message']['text']
            payload['message']['voice'] = {'file_id': 'fixture', 'duration': 1, 'file_size': 100}
        return payload

    def poll(self, *payloads):
        client = FakeTelegram(payloads)
        with patch('horarios_especiales.tasks.execute_special_hours_request_task.delay'):
            bot.poll_once(self.config, client)
        return client

    def approve(self, old, id, who=None):
        who = who or old.telegram_user_id
        payload = {'update_id': id, 'callback_query': {'id': str(id), 'from': {'id': who},
            'message': {'message_id': old.preview_message_id, 'chat': {'id': who, 'type': 'private'}},
            'data': f'approve:{old.pk}:{old.nonce}'}}
        return self.poll(payload)

    def test_two_people_own_previews_cancel_status_and_audit(self):
        self.poll(self.incoming(id=1, who=777), self.incoming('Payan hoy cerrada', id=2))
        self.assertEqual(TelegramHoursUpdate.objects.filter(state='PREVIEW').count(), 2)
        first, second = TelegramHoursUpdate.objects.order_by('pk')
        self.assertEqual(second.request.requested_by_id, self.member.pk)
        self.approve(first, 3, who=888)
        first.request.refresh_from_db()
        self.assertIsNone(first.request.approved_at)
        self.approve(second, 4)
        second.request.refresh_from_db()
        self.assertEqual(second.request.approved_by_id, self.member.pk)
        self.assertEqual(second.request.audit_entries.get(action='APPROVED').actor_user_id, self.member.pk)
        self.poll(self.incoming('/cancel', id=5))
        first.refresh_from_db()
        self.assertEqual(first.state, 'PREVIEW')
        status = self.poll(self.incoming('/status', id=6))
        self.assertIn(second.request.request_code, status.sent[0]['text'])
        self.assertNotIn(first.request.request_code, status.sent[0]['text'])
        self.assertFalse(self.member.is_superuser)
        self.assertEqual(self.member.groups.count(), 0)

    def test_inactive_member_does_not_stop_owner_poll(self):
        self.member.is_active = False
        self.member.save()
        client = self.poll(self.incoming('/start', id=1), self.incoming('/help', id=2, who=777))
        self.assertEqual([row['chat_id'] for row in client.sent], [777])
        self.assertFalse(TelegramHoursUpdate.objects.filter(telegram_user_id=888).exists())

    def test_member_revoke_does_not_stop_owner_or_allow_pending_execution(self):
        self.poll(self.incoming())
        row = TelegramHoursUpdate.objects.get(update_id=1)
        self.approve(row, 2)
        self.config['users'] = []
        client = self.poll(self.incoming(id=3), self.incoming('/help', id=4, who=777))
        self.assertEqual([m['chat_id'] for m in client.sent], [777])
        from horarios_especiales.tasks import execute_special_hours_request_task
        with self.assertRaises(PermissionDenied), patch('horarios_especiales.tasks.execute_request') as execute:
            execute_special_hours_request_task(request_id=row.request_id, actor_id=self.member.pk)
        execute.assert_not_called()

    def test_same_priority_change_requires_fresh_preview_with_warning(self):
        self.poll(self.incoming(id=1, who=777), self.incoming(id=2))
        self.assertEqual(TelegramHoursUpdate.objects.filter(state='PREVIEW').count(), 2)
        first, second = TelegramHoursUpdate.objects.order_by('pk')
        self.approve(first, 3)
        result = self.approve(second, 4)
        second.request.refresh_from_db()
        self.assertIsNone(second.request.approved_at)
        self.assertIn('cambió', result.sent[0]['text'])
        fresh = self.poll(self.incoming(id=5))
        self.assertIn(first.request.request_code, fresh.sent[0]['text'])
        self.assertIn('reemplazar', fresh.sent[0]['text'])
        self.approve(TelegramHoursUpdate.objects.get(update_id=5), 6)
        self.assertEqual(TelegramHoursUpdate.objects.get(update_id=5).request.approved_by_id, self.member.pk)

    def test_nonoverlapping_branch_or_date_does_not_block(self):
        self.poll(self.incoming('Matriz hoy cerrada', id=1, who=777), self.incoming('Matriz mañana cerrada', id=2))
        self.assertEqual(TelegramHoursUpdate.objects.filter(state='PREVIEW').count(), 2)
        first, second = TelegramHoursUpdate.objects.order_by('pk')
        self.approve(first, 3)
        self.approve(second, 4)
        second.request.refresh_from_db()
        self.assertIsNotNone(second.request.approved_at)

    def test_audio_claim_finish_and_corrections_are_per_person(self):
        self.poll(self.incoming(id=1, voice=True), self.incoming(id=2, who=777))
        with patch.object(audio, 'download_pcm', return_value=pcm()):
            job = audio.claim(self.config)
        self.assertIn('id', job)
        result = audio.finish(self.config, {**{k: job[k] for k in ('id','lease','sha256')}, 'text': 'Matriz hoy cerrada'})
        self.assertTrue(result['accepted'])
        row = TelegramHoursUpdate.objects.get(pk=job['id'])
        self.assertEqual(row.request.requested_by_id, self.member.pk)
        self.assertEqual(TelegramHoursUpdate.objects.get(update_id=2).state, 'PREVIEW')

    def test_audio_revocation_or_relink_rejects_existing_lease(self):
        self.poll(self.incoming(id=1, voice=True))
        with patch.object(audio, 'download_pcm', return_value=pcm()):
            job = audio.claim(self.config)
        self.assertIn('id', job)
        self.config['users'][0]['erp_user_id'] = User.objects.create_user('different').pk
        result = audio.finish(self.config, {**{k: job[k] for k in ('id','lease','sha256')}, 'text': 'Matriz hoy cerrada'})
        self.assertFalse(result['accepted'])
        self.assertIsNone(TelegramHoursUpdate.objects.get(pk=job['id']).request_id)

    def test_audio_budget_is_global_and_member_spend_not_reset(self):
        self.poll(self.incoming(id=1, voice=True), self.incoming(id=2, who=777, voice=True))
        self.assertEqual(TelegramHoursUpdate.objects.filter(state='AUDIO_WAIT').count(), 2)
        TelegramHoursCursor.objects.update(audio_reserved_cents=496)
        TelegramHoursUpdate.objects.update(expires_at=timezone.now()-timedelta(seconds=1))
        with patch.object(audio, 'download_pcm', return_value=pcm()), patch.object(audio, 'paid_transcribe', return_value='Matriz hoy cerrada') as paid:
            audio.advance(self.config)
            audio.advance(self.config)
        self.assertEqual(paid.call_count, 1)
        self.assertEqual(TelegramHoursCursor.objects.get().audio_reserved_cents, 499)


class ConfigTests(TestCase):
    def test_explicit_members_and_ambiguous_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = dict(telegram_user_id=777, telegram_chat_id=777, erp_user_id=1,
                         users=[dict(telegram_user_id=888, telegram_chat_id=888, erp_user_id=2, approve_own=True)])
            token = root/'telegram-token'
            token.write_text('12345:'+'a'*32); token.chmod(0o600)
            path = root/'owner.json'
            path.write_text(json.dumps(owner)); path.chmod(0o600)
            self.assertEqual(bot.load_config(directory)['users'], owner['users'])
            for key in ('telegram_user_id','telegram_chat_id','erp_user_id'):
                invalid = copy.deepcopy(owner); invalid['users'][0][key] = invalid[key]
                path.write_text(json.dumps(invalid))
                with self.assertRaises(ValueError):
                    bot.load_config(directory)
            invalid = copy.deepcopy(owner); invalid['users'][0]['approve_own'] = 'true'
            path.write_text(json.dumps(invalid))
            with self.assertRaises(ValueError):
                bot.load_config(directory)


class RevocationTests(TransactionTestCase):
    setUp = MultiuserTests.setUp
    update = MultiuserTests.update
    incoming = MultiuserTests.incoming
    poll = MultiuserTests.poll
    approve = MultiuserTests.approve
    def test_simultaneous_overlapping_approvals_cannot_both_win(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections
        self.poll(self.incoming(id=1, who=777), self.incoming(id=2))
        old_rows = list(TelegramHoursUpdate.objects.order_by('pk'))
        for update_id, old in enumerate(old_rows, 3):
            who = old.telegram_user_id
            bot.persist_updates(self.config, [{'update_id': update_id, 'callback_query': {
                'id': str(update_id), 'from': {'id': who}, 'message': {'message_id': old.preview_message_id,
                'chat': {'id': who, 'type': 'private'}}, 'data': f'approve:{old.pk}:{old.nonce}'}}])
        barrier = Barrier(2)
        def approve(update_id):
            close_old_connections()
            try:
                row = TelegramHoursUpdate.objects.get(update_id=update_id)
                barrier.wait(timeout=5)
                bot.process_callback(row, self.config, None)
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(approve, [3, 4]))
        self.assertEqual(TelegramHoursUpdate.objects.filter(state='APPROVED').count(), 1)
        self.assertEqual(TelegramHoursUpdate.objects.filter(state='PREVIEW').count(), 1)

    def test_revocation_during_google_preview_cannot_approve(self):
        self.poll(self.incoming())
        row = TelegramHoursUpdate.objects.get(update_id=1)
        def revoke(*args):
            self.config['users'] = []
            return 'Google pendiente'
        with patch('horarios_especiales.services.google_projection.preview_google_conflicts', side_effect=revoke):
            with self.assertRaises(PermissionDenied):
                self.approve(row, 2)
        row.request.refresh_from_db()
        self.assertIsNone(row.request.approved_at)

    def test_revocation_during_paid_audio_never_creates_preview(self):
        self.poll(self.incoming(voice=True))
        TelegramHoursUpdate.objects.update(expires_at=timezone.now()-timedelta(seconds=1))
        def revoke(data):
            self.config['users'] = []
            return 'Matriz hoy cerrada'
        with patch.object(audio, 'download_pcm', return_value=pcm()), patch.object(audio, 'paid_transcribe', side_effect=revoke):
            with self.assertRaises(PermissionDenied):
                audio.advance(self.config)
        row = TelegramHoursUpdate.objects.get(update_id=1)
        self.assertIsNone(row.request_id)
        self.assertEqual(TelegramHoursCursor.objects.get().audio_reserved_cents, 3)

    def test_revoked_audio_is_purged_and_cannot_resume_after_regrant(self):
        self.poll(self.incoming(voice=True))
        with patch.object(audio, 'download_pcm', return_value=pcm()):
            job = audio.claim(self.config)
        identities = self.config.pop('users')
        self.poll(self.incoming('/help', id=2, who=777))
        row = TelegramHoursUpdate.objects.get(update_id=1)
        self.assertEqual(row.state, 'CANCELLED')
        self.assertNotIn('pcm', row.receipt)
        self.config['users'] = identities
        result = audio.finish(self.config, {**{k: job[k] for k in ('id','lease','sha256')}, 'text':'Matriz hoy cerrada'})
        self.assertFalse(result['accepted'])

    def test_projection_and_cli_revalidate_member_and_audit_real_actor(self):
        import io
        from django.core.management import call_command
        from horarios_especiales.tests_google_projection import MemoryGoogle, TODAY
        from integraciones.services.google_business_profile.publisher import GoogleBusinessProfilePublisher
        self.poll(self.incoming())
        row = TelegramHoursUpdate.objects.get(update_id=1)
        self.approve(row, 2)
        google = MemoryGoogle()
        with patch('integraciones.services.google_business_profile.publisher.GoogleBusinessProfileClient', return_value=google):
            call_command('reconcile_special_hours', request_id=row.request_id, stdout=io.StringIO())
        row.request.refresh_from_db()
        self.assertEqual(row.request.executed_by_id, self.member.pk)
        self.assertTrue(row.request.audit_entries.filter(actor_user=self.member, action='EXECUTION_SUCCESS').exists())
        self.assertFalse(row.request.audit_entries.exclude(actor_user=self.member).exists())
        self.config['users'][0]['approve_own'] = False
        from horarios_especiales.services.google_projection import execute_rules
        with self.assertRaises(PermissionDenied):
            execute_rules(request_obj=row.request, today=TODAY, publisher=GoogleBusinessProfilePublisher(client=google))

    def test_shared_publisher_blocks_revoked_winning_contributor(self):
        from horarios_especiales.tests_google_projection import MemoryGoogle, TODAY, make_rule
        from horarios_especiales.services.requests import create_request_from_rules, approve_request
        from horarios_especiales.services.google_projection import execute_rules
        from integraciones.services.google_business_profile.publisher import GoogleBusinessProfilePublisher
        self.poll(self.incoming())
        row = TelegramHoursUpdate.objects.get(update_id=1)
        self.approve(row, 2)
        other, payload = create_request_from_rules(raw_text='another day', branch_ids=[self.branches['MATRIZ'].pk], actor=self.user,
            rules=[make_rule(start_date='2026-11-23')])
        approve_request(request_obj=other, actor=self.user, expected_revision=payload['revision'])
        self.config['users'] = []
        google = MemoryGoogle()
        summary = execute_rules(request_obj=other, today=TODAY, publisher=GoogleBusinessProfilePublisher(client=google))
        self.assertEqual(summary['failed'], 1)
        self.assertFalse(google.writes)

    def test_pending_identity_cannot_transfer_to_new_erp_account(self):
        self.poll(self.incoming())
        row = TelegramHoursUpdate.objects.get(update_id=1)
        self.config['users'][0]['erp_user_id'] = User.objects.create_user('replacement').pk
        self.approve(row, 2)
        row.request.refresh_from_db()
        self.assertIsNone(row.request.approved_at)
        result = self.poll(self.incoming('/status', id=3))
        self.assertIn('Sin solicitudes', result.sent[0]['text'])



class RuntimeConfigTests(TestCase):
    def test_private_file_reload_revokes_loaded_config(self):
        owner_user = User.objects.create_user('owner', is_superuser=True)
        member = User.objects.create_user('member')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = dict(telegram_user_id=777, telegram_chat_id=777, erp_user_id=owner_user.pk,
                         users=[dict(telegram_user_id=888, telegram_chat_id=888, erp_user_id=member.pk, approve_own=True)])
            token = root/'telegram-token'
            token.write_text('12345:'+'a'*32); token.chmod(0o600)
            path = root/'owner.json'
            path.write_text(json.dumps(owner)); path.chmod(0o600)
            loaded = bot.load_config(directory)
            scoped = bot.scoped_config(loaded, owner['users'][0])
            self.assertEqual(bot.actor_for(scoped), member)
            owner['users'] = []
            path.write_text(json.dumps(owner))
            with self.assertRaises(PermissionDenied):
                bot.actor_for(scoped)
            self.assertEqual(bot.actor_for(loaded), owner_user)


class ConfigurationCommandTests(TestCase):
    def test_validate_only_checks_members_without_reconfiguring_beat(self):
        from django.core.management import call_command
        from django.core.management.base import CommandError
        from django_celery_beat.models import PeriodicTask
        import io
        owner = User.objects.create_user('owner', is_superuser=True)
        member = User.objects.create_user('member')
        config = dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=owner.pk,
                      users=[dict(telegram_user_id=888, telegram_chat_id=888, erp_user_id=member.pk, approve_own=True)])
        with patch('horarios_especiales.management.commands.configure_hours_telegram.load_config', return_value=config):
            output = io.StringIO()
            call_command('configure_hours_telegram', validate_only=True, stdout=output)
            self.assertFalse(PeriodicTask.objects.filter(name__startswith='hours-').exists())
            self.assertIn('2 usuarios', output.getvalue())
            member.is_active = False; member.save()
            with self.assertRaises(CommandError):
                call_command('configure_hours_telegram', validate_only=True, stdout=io.StringIO())

    def test_legacy_migration_preserves_approved_and_budget_but_purges_unbound_audio(self):
        from importlib import import_module
        from django.apps import apps
        from django.db import connection
        from horarios_especiales.models import SolicitudHorarioEspecial
        owner = User.objects.create_user('legacy-owner', is_superuser=True)
        request = SolicitudHorarioEspecial.objects.create(raw_command='legacy approved', requested_by=owner,
            approved_by=owner, approved_at=timezone.now(), status='APROBADO')
        linked = TelegramHoursUpdate.objects.create(bot_id=12345, update_id=1, request=request, state='QUEUED')
        orphan = TelegramHoursUpdate.objects.create(bot_id=12345, update_id=2, state='AUDIO_LOCAL',
            nonce='a'*32, receipt={'pcm':'private', 'sha256':'hash', 'reserved_cents':3})
        TelegramHoursCursor.objects.create(bot_id=12345, next_update_id=200, audio_reserved_cents=499)
        with connection.schema_editor() as editor:
            import_module('horarios_especiales.migrations.0004_telegram_identity').bind_existing_requests(apps, editor)
        linked.refresh_from_db(); orphan.refresh_from_db(); request.refresh_from_db()
        self.assertEqual(linked.erp_user_id, owner.pk)
        self.assertEqual(linked.state, 'QUEUED')
        self.assertEqual(request.status, 'APROBADO')
        self.assertEqual(orphan.state, 'CANCELLED')
        self.assertNotIn('pcm', orphan.receipt)
        self.assertEqual(TelegramHoursCursor.objects.get().audio_reserved_cents, 499)
        self.assertEqual(TelegramHoursCursor.objects.get().next_update_id, 200)


class SharedBudgetConcurrencyTests(TransactionTestCase):
    def test_two_people_compete_for_single_remaining_reservation(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections
        owner = User.objects.create_user('owner', is_superuser=True)
        member = User.objects.create_user('member')
        config = dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=owner.pk,
                      users=[dict(telegram_user_id=888, telegram_chat_id=888, erp_user_id=member.pk, approve_own=True)])
        TelegramHoursCursor.objects.create(bot_id=12345, audio_reserved_cents=496)
        rows = [TelegramHoursUpdate.objects.create(bot_id=12345, update_id=i+1,
            telegram_user_id=tid, chat_id=tid, erp_user=user, state='AUDIO_WAIT')
            for i, (tid, user) in enumerate(((777, owner), (888, member)))]
        barrier = Barrier(2)
        def reserve(pk):
            close_old_connections()
            try:
                row = TelegramHoursUpdate.objects.get(pk=pk)
                barrier.wait(timeout=5)
                return audio.reserve(row, config)
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            result = list(pool.map(reserve, [row.pk for row in rows]))
        self.assertEqual(sorted(result), [False, True])
        self.assertEqual(TelegramHoursCursor.objects.get().audio_reserved_cents, 499)
