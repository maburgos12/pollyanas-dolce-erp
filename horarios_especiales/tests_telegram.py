import copy
from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from core.models import Sucursal
from horarios_especiales.models import SolicitudHorarioEspecial, SucursalPlataformaExterna, TelegramHoursCursor, TelegramHoursUpdate
from horarios_especiales.services.telegram_bot import (TelegramUnavailable, consumer_lock, persist_updates, poll_once,
    prepare_text, process_callback, recover_approved, status_text)
from horarios_especiales.services.telegram_parser import parse_instruction
from horarios_especiales.services.requests import approve_request, create_request_from_rules

STAMP = int(datetime(2026, 11, 22, 23, 30, tzinfo=ZoneInfo('America/Mazatlan')).timestamp())
CODES = ['MATRIZ', 'PAYAN', 'LAS_GLORIAS', 'PLAZA_NIO', 'COLOSIO', 'LEYVA', 'EL_TUNEL', 'BAMOA', 'GUAMUCHIL', 'SINALOA_LEYVA']


class FakeTelegram:
    def __init__(self, updates=()):
        self.updates = list(updates)
        self.sent = []
        self.fail_send = False

    def call(self, method, **kwargs):
        if method == 'getUpdates':
            return [row for row in self.updates if row['update_id'] >= kwargs['offset']]
        if self.fail_send:
            raise TelegramUnavailable('test outage')
        self.sent.append(kwargs)
        return {'message_id': 500 + len(self.sent), 'chat': {'id': kwargs['chat_id'], 'type': 'private'}, 'from': {'id': 12345, 'is_bot': True}}


class TelegramHoursTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='hours_owner', is_superuser=True)
        self.config = dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=self.user.pk)
        self.branches = {}
        for code in CODES:
            branch = Sucursal.objects.create(codigo=code, nombre=code.replace('_', ' '), activa=True)
            self.branches[code] = branch
            SucursalPlataformaExterna.objects.create(sucursal=branch, external_location_name=f'locations/{branch.pk}')
        Sucursal.objects.create(codigo='CRUCERO', nombre='Crucero', activa=True)

    def update(self, text='Todas hoy cerradas', id=1):
        return {'update_id': id, 'message': {'message_id': id + 10, 'date': STAMP, 'from': {'id': 777},
            'chat': {'id': 777, 'type': 'private'}, 'text': text}}

    def preview(self):
        client = FakeTelegram([self.update()])
        poll_once(self.config, client)
        return TelegramHoursUpdate.objects.get(update_id=1), client

    def callback(self, old, id=2):
        payload = {'update_id': id, 'callback_query': {'id': str(id), 'from': {'id': 777},
            'message': {'message_id': old.preview_message_id, 'chat': {'id': 777, 'type': 'private'}},
            'data': f'approve:{old.pk}:{old.nonce}'}}
        persist_updates(self.config, [payload])
        return TelegramHoursUpdate.objects.get(update_id=id)

    def test_parser_relative_date_frozen_in_mazatlan_and_all_ten(self):
        ids, rules = parse_instruction('Todas hoy cerradas', STAMP)
        self.assertEqual(set(ids), {b.pk for b in self.branches.values()})
        self.assertEqual(rules[0]['start_date'], '2026-11-22')
        ids, rules = parse_instruction('Plaza Nio mañana de 1 pm a 9 pm', STAMP)
        self.assertEqual(ids, [self.branches['PLAZA_NIO'].pk])
        self.assertEqual(rules[0]['start_date'], '2026-11-23')
        self.assertEqual(rules[0]['time_windows'], [{'open': '13:00', 'close': '21:00'}])

    def test_parser_date_range_weekly_multiple_branches_and_weekdays(self):
        for text, kind, start, end in [
            ('Matriz y Payan el 24 de noviembre de 2026 de 09:00 a 18:00', 'date', '2026-11-24', None),
            ('Matriz del 2026-12-01 al 2026-12-03 cerrado', 'range', '2026-12-01', '2026-12-03'),
            ('Tunel todos los miercoles cerrado', 'weekly', '2026-11-22', None),
            ('Tunel todos los domingos cerrado', 'weekly', '2026-11-22', None),
            ('Tunel cada lunes y miercoles desde 2026-12-01 hasta 2026-12-31 cerrado', 'weekly', '2026-12-01', '2026-12-31'),
        ]:
            with self.subTest(text=text):
                ids, rules = parse_instruction(text, STAMP)
                self.assertEqual((rules[0]['kind'], rules[0]['start_date'], rules[0].get('end_date')), (kind, start, end))
        self.assertEqual(rules[0]['weekdays'], [0, 2])

    def test_parser_never_accepts_partial_instruction(self):
        for text in ['Matriz hoy de 1 a 9', 'Matriz hoy cerrado excepto de 1 pm a 9 pm',
                     'Matriz hoy cerrado y Payan mañana abierto', 'Glorias y desconocida hoy cerrado',
                     'Matriz lunes 2026-11-22 cerrado', 'Crucero hoy cerrado',
                     'Matriz hoy cerrado hasta nuevo aviso', 'Todas cerrado', 'Matriz 2026-02-30 cerrado',
                     'Matriz hoy de 20:00 a 09:00', 'Matriz del 2026-12-03 al 2026-12-01 cerrado']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_instruction(text, STAMP)

    def test_inbox_duplicate_cursor_and_restart_no_duplicate_preview(self):
        update = self.update()
        persist_updates(self.config, [update, update])
        self.assertEqual(TelegramHoursCursor.objects.get().next_update_id, 2)
        client = FakeTelegram([update])
        poll_once(self.config, client)
        poll_once(self.config, client)
        self.assertEqual(TelegramHoursUpdate.objects.count(), 1)
        self.assertEqual(SolicitudHorarioEspecial.objects.count(), 1)
        self.assertEqual(len(client.sent), 1)
        self.assertIn('NO aplicada', client.sent[0]['text'])
        self.assertFalse(SolicitudHorarioEspecial.objects.filter(approved_at__isnull=False).exists())

    def test_unknown_actor_chat_group_cannot_enter_inbox(self):
        for id, field, value in [(1, 'from', {'id': 999}), (2, 'chat', {'id': 999, 'type': 'private'}),
                                 (3, 'chat', {'id': 777, 'type': 'group'})]:
            payload = self.update(id=id)
            payload['message'][field] = value
            persist_updates(self.config, [payload])
        self.assertFalse(TelegramHoursUpdate.objects.exists())
        self.assertEqual(TelegramHoursCursor.objects.get().next_update_id, 4)

    def test_ambiguous_correction_invalidates_old_buttons(self):
        old, client = self.preview()
        client.updates.append(self.update('cierra más temprano', id=2))
        poll_once(self.config, client)
        old.refresh_from_db()
        self.assertEqual(old.state, 'CANCELLED')
        self.assertEqual(old.request.status, 'CANCELADO')
        self.assertEqual(TelegramHoursUpdate.objects.get(update_id=2).state, 'DONE')
        callback = self.callback(old, id=3)
        process_callback(callback, self.config, self.user)
        self.assertFalse(SolicitudHorarioEspecial.objects.filter(approved_at__isnull=False).exists())

    def test_callback_checks_binding_expiry_revision_and_single_approval(self):
        old, _ = self.preview()
        original = copy.deepcopy(old.__dict__)
        for index, (field, value) in enumerate([('telegram_user_id', 888), ('chat_id', 888),
                ('preview_message_id', 999), ('nonce', 'b' * 32), ('expires_at', timezone.now() - timedelta(seconds=1)),
                ('revision', 'c' * 64)], start=2):
            callback = self.callback(old, id=index)
            setattr(old, field, value)
            old.save()
            process_callback(callback, self.config, self.user)
            old.request.refresh_from_db()
            self.assertIsNone(old.request.approved_at, field)
            setattr(old, field, original[field])
            old.save()
        callback = self.callback(old, id=10)
        process_callback(callback, self.config, self.user)
        again = self.callback(old, id=11)
        process_callback(again, self.config, self.user)
        self.assertEqual(old.request.audit_entries.filter(action='APPROVED').count(), 1)
        self.assertIn('ya procesado', again.receipt['text'])

    def test_approval_survives_send_and_broker_failure(self):
        old, client = self.preview()
        callback = self.callback(old)
        client.fail_send = True
        with self.assertRaises(TelegramUnavailable):
            poll_once(self.config, client)
        old.refresh_from_db()
        self.assertEqual(old.state, 'APPROVED')
        with patch('horarios_especiales.tasks.execute_special_hours_request_task.delay', side_effect=RuntimeError('broker offline')):
            recover_approved(self.config)
        old.refresh_from_db()
        self.assertEqual(old.state, 'APPROVED')
        client.fail_send = False
        with patch('horarios_especiales.tasks.execute_special_hours_request_task.delay') as queued:
            poll_once(self.config, client)
            poll_once(self.config, client)
            queued.assert_called_once_with(request_id=old.request_id, actor_id=self.user.pk)
        old.refresh_from_db()
        self.assertEqual(old.state, 'QUEUED')
        self.assertEqual(old.request.audit_entries.filter(action='APPROVED').count(), 1)

    def test_failed_preview_delivery_preserves_same_revision(self):
        client = FakeTelegram([self.update()])
        client.fail_send = True
        with self.assertRaises(TelegramUnavailable):
            poll_once(self.config, client)
        old = TelegramHoursUpdate.objects.get()
        self.assertEqual(old.state, 'PREPARED')
        revision = old.revision
        client.fail_send = False
        poll_once(self.config, client)
        old.refresh_from_db()
        self.assertEqual(old.revision, revision)
        self.assertEqual(SolicitudHorarioEspecial.objects.count(), 1)

    def test_conflict_does_not_offer_weekly_approval(self):
        obj, payload = create_request_from_rules(raw_text='exception', branch_ids=[self.branches['EL_TUNEL'].pk], actor=self.user,
            rules=[{'kind': 'date', 'start_date': '2026-11-25', 'closed_all_day': False, 'time_windows': [{'open': '10:00', 'close': '20:00'}]}])
        approve_request(request_obj=obj, actor=self.user, expected_revision=payload['revision'])
        client = FakeTelegram([self.update('Tunel todos los miercoles cerrado')])
        poll_once(self.config, client)
        self.assertNotIn('reply_markup', client.sent[0])
        self.assertIn('mayor prioridad', client.sent[0]['text'])

    def test_receipts_do_not_equate_erp_approval_with_consumers(self):
        old, _ = self.preview()
        old.request.execution_summary_json = {'details': [{'branch_code': 'MATRIZ', 'status': 'GOOGLE_CONFIRMED'},
            {'branch_code': 'PAYAN', 'status': 'FAILED'}, {'branch_code': 'LEYVA', 'status': 'PROGRAMMED'}]}
        text = status_text(old.request)
        for phrase in ('confirmado por lectura Google', 'falló', 'programado', 'Tienda: pendiente', 'Maya: pendiente'):
            self.assertIn(phrase, text)
        self.assertNotIn('finalizado', text.lower())

    def test_voice_is_preserved_without_execution(self):
        update = self.update()
        del update['message']['text']
        update['message']['voice'] = {'file_id': 'fixture', 'file_unique_id': 'unique', 'duration': 12, 'file_size': 1000}
        client = FakeTelegram([update])
        poll_once(self.config, client)
        row = TelegramHoursUpdate.objects.get()
        self.assertEqual(row.payload['message']['voice']['file_id'], 'fixture')
        self.assertIn('transcripción está en preparación', client.sent[0]['text'])
        self.assertFalse(SolicitudHorarioEspecial.objects.exists())

    def test_revoked_permission_stops_poll_approval_enqueue_and_execution(self):
        from django.core.exceptions import PermissionDenied
        from horarios_especiales.tasks import execute_special_hours_request_task
        from horarios_especiales.services.google_projection import execute_rules
        old, client = self.preview()
        callback = self.callback(old)
        process_callback(callback, self.config, self.user)
        self.user.is_superuser = False
        self.user.save()
        for operation in (lambda: poll_once(self.config, client), lambda: recover_approved(self.config),
                          lambda: process_callback(callback, self.config, self.user),
                          lambda: execute_special_hours_request_task(request_id=old.request_id, actor_id=self.user.pk),
                          lambda: execute_rules(request_obj=old.request)):
            with self.assertRaises(PermissionDenied):
                operation()

    def test_owner_rotation_does_not_send_old_receipts_or_enqueue(self):
        old, client = self.preview()
        callback = self.callback(old)
        process_callback(callback, self.config, self.user)
        rotated = {**self.config, 'telegram_user_id': 888, 'telegram_chat_id': 888}
        sent = len(client.sent)
        with patch('horarios_especiales.tasks.execute_special_hours_request_task.delay') as queued:
            poll_once(rotated, client)
            queued.assert_not_called()
        self.assertEqual(len(client.sent), sent)

    def test_invalid_send_receipt_never_activates_buttons(self):
        client = FakeTelegram([self.update()])
        original = client.call
        def invalid(method, **kwargs):
            result = original(method, **kwargs)
            if method == 'sendMessage':
                result['chat']['id'] = 999
            return result
        client.call = invalid
        with self.assertRaises(TelegramUnavailable):
            poll_once(self.config, client)
        self.assertEqual(TelegramHoursUpdate.objects.get().state, 'PREPARED')

    def test_malformed_updates_are_skipped_without_inbox_or_cursor_poisoning(self):
        for id, field, value in [(1, 'date', 'bad'), (2, 'message_id', True), (3, 'chat', []), (4, 'text', {})]:
            update = self.update(id=id)
            update['message'][field] = value
            persist_updates(self.config, [update])
        persist_updates(self.config, [None, {'update_id': 2**100}])
        self.assertFalse(TelegramHoursUpdate.objects.exists())
        self.assertEqual(TelegramHoursCursor.objects.get().next_update_id, 5)

    def test_all_never_silently_selects_only_nine(self):
        SucursalPlataformaExterna.objects.filter(sucursal=self.branches['MATRIZ']).update(is_active=False)
        with self.assertRaises(ValueError):
            parse_instruction('Todas hoy cerradas', STAMP)

    def test_schedule_configuration_is_disabled_by_default_and_private(self):
        import json
        import tempfile
        from pathlib import Path
        from django.core.management import call_command
        from django_celery_beat.models import PeriodicTask
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'owner.json').write_text(json.dumps({k: v for k, v in self.config.items() if k != 'bot_id'}))
            (root / 'telegram-token').write_text('12345:' + 'a' * 32)
            for path in root.iterdir():
                path.chmod(0o600)
            call_command('configure_hours_telegram', config=folder)
            tasks = PeriodicTask.objects.filter(name__startswith='hours-')
            self.assertEqual(tasks.count(), 2)
            self.assertFalse(tasks.filter(enabled=True).exists())
            self.assertFalse(any('aaaa' in row.kwargs for row in tasks))



class TelegramConsumerLockTests(TransactionTestCase):
    def test_session_lock_excludes_another_connection(self):
        from concurrent.futures import ThreadPoolExecutor
        from django.db import close_old_connections, connections
        def other():
            close_old_connections()
            try:
                with consumer_lock(12345) as locked:
                    return locked
            finally:
                connections.close_all()
        with consumer_lock(12345) as locked:
            self.assertTrue(locked)
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertFalse(pool.submit(other).result(timeout=5))
        with consumer_lock(12345) as locked:
            self.assertTrue(locked)


class TelegramTransportTests(TestCase):
    def test_transport_fixed_origin_no_proxy_redirect_and_bounded_response(self):
        from horarios_especiales.services.telegram_bot import TelegramClient
        token = '12345:' + 'secret' * 6
        with patch('horarios_especiales.services.telegram_bot.requests.Session') as factory:
            session = factory.return_value.__enter__.return_value
            response = session.post.return_value.__enter__.return_value
            response.status_code = 200
            response.iter_content.return_value = [b'{"ok":true,"result":[]}']
            client = TelegramClient({'token': token})
            self.assertEqual(client.call('getUpdates', offset=0), [])
            self.assertFalse(session.trust_env)
            self.assertEqual(session.post.call_args.args[0], 'https://api.telegram.org/bot' + token + '/getUpdates')
            self.assertFalse(session.post.call_args.kwargs['allow_redirects'])
            self.assertEqual(session.post.call_args.kwargs['timeout'], (3, 5))
            response.status_code = 302
            with self.assertRaises(TelegramUnavailable):
                client.call('getUpdates')
            response.status_code = 200
            response.iter_content.return_value = [b'x' * 262145]
            with self.assertRaises(TelegramUnavailable):
                client.call('getUpdates')
            import requests
            session.post.side_effect = requests.ConnectionError('https://api.telegram.org/bot' + token)
            with self.assertRaises(TelegramUnavailable) as error:
                client.call('getUpdates')
            self.assertNotIn(token, str(error.exception))


class TelegramProjectionReceiptTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='hours_owner', is_superuser=True)
        branch = Sucursal.objects.create(codigo='MATRIZ', nombre='Matriz', activa=True)
        self.branches = {'MATRIZ': branch}
        SucursalPlataformaExterna.objects.create(sucursal=branch, external_location_name='locations/qa')

    def test_revoked_contributing_request_blocks_another_request_before_google_write(self):
        from horarios_especiales.tests_google_projection import MemoryGoogle, TODAY, make_rule
        from integraciones.services.google_business_profile.publisher import GoogleBusinessProfilePublisher
        from horarios_especiales.services.google_projection import execute_rules
        original, payload = create_request_from_rules(raw_text='original', branch_ids=[self.branches['MATRIZ'].pk], actor=self.user, rules=[make_rule()])
        approve_request(request_obj=original, actor=self.user, expected_revision=payload['revision'])
        TelegramHoursUpdate.objects.create(bot_id=12345, update_id=1, request=original,
            telegram_user_id=777, chat_id=777, state='QUEUED')
        other = User.objects.create_user(username='replacement_owner', is_superuser=True)
        branch = self.branches['MATRIZ']
        obj, payload = create_request_from_rules(raw_text='next day', branch_ids=[branch.pk], actor=other,
            rules=[make_rule(start_date='2026-11-23')])
        approve_request(request_obj=obj, actor=other, expected_revision=payload['revision'])
        self.user.is_superuser = False
        self.user.save()
        client = MemoryGoogle()
        summary = execute_rules(request_obj=obj, today=TODAY, publisher=GoogleBusinessProfilePublisher(client=client))
        self.assertEqual(summary['failed'], 1)
        self.assertEqual(client.writes, [])
        # A replacement on the same date supersedes the revoked rule and may publish.
        replacement, payload = create_request_from_rules(raw_text='replace', branch_ids=[branch.pk], actor=other,
            rules=[make_rule()])
        approve_request(request_obj=replacement, actor=other, expected_revision=payload['revision'])
        summary = execute_rules(request_obj=replacement, today=TODAY, publisher=GoogleBusinessProfilePublisher(client=client))
        self.assertEqual(summary['details'][0]['status'], 'GOOGLE_CONFIRMED')
        self.assertEqual(len(client.writes), 1)

    def test_superseded_and_partially_superseded_requests_never_claim_full_confirmation(self):
        from horarios_especiales.tests_google_projection import MemoryGoogle, TODAY, make_rule
        from integraciones.services.google_business_profile.publisher import GoogleBusinessProfilePublisher
        from horarios_especiales.services.google_projection import execute_rules
        branch = self.branches['MATRIZ']
        def approved(*rules):
            obj, payload = create_request_from_rules(raw_text='fixture', branch_ids=[branch.pk], actor=self.user, rules=list(rules))
            approve_request(request_obj=obj, actor=self.user, expected_revision=payload['revision'])
            return obj
        original = approved(make_rule())
        replacement = approved(make_rule(time_windows=[{'open': '11:00', 'close': '17:00'}]))
        client = MemoryGoogle()
        publisher = GoogleBusinessProfilePublisher(client=client)
        summary = execute_rules(request_obj=original, today=TODAY, publisher=publisher)
        self.assertEqual(summary['details'][0]['status'], 'SUPERSEDED')
        self.assertFalse(summary['public_confirmed'])
        mixed = approved(make_rule(), make_rule(start_date='2026-11-23'))
        approved(make_rule(time_windows=[{'open': '12:00', 'close': '16:00'}]))
        summary = execute_rules(request_obj=mixed, today=TODAY, publisher=publisher)
        self.assertEqual(summary['details'][0]['status'], 'PARTIALLY_SUPERSEDED')
        self.assertFalse(summary['public_confirmed'])
        self.assertIn('no se confirmó toda', status_text(mixed))
