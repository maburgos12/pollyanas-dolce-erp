import copy
from datetime import date, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from core.models import Sucursal
from horarios_especiales.models import SolicitudHorarioEspecial, SucursalAlias, SucursalPlataformaExterna, TelegramHoursCursor, TelegramHoursUpdate
from horarios_especiales.services.consumer_receipts import FINALIZED_LABEL
from horarios_especiales.services.telegram_bot import (TelegramUnavailable, consumer_lock, persist_updates, poll_once,
    prepare_text, process_callback, recover_approved, status_text)
from horarios_especiales.services.telegram_parser import parse_grouped_instruction, parse_instruction
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
        self.google_preflight = patch('horarios_especiales.services.google_projection.preview_google_conflicts', return_value='Google: comprobación previa pendiente.')
        self.google_preflight.start()
        self.addCleanup(self.google_preflight.stop)
        self.consumer_mock = patch('horarios_especiales.services.consumer_receipts.check_pair', return_value={})
        self.consumer_mock.start()
        self.addCleanup(self.consumer_mock.stop)
        self.user = User.objects.create_user(username='hours_owner', is_superuser=True)
        self.config = dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=self.user.pk)
        runtime = patch('horarios_especiales.services.telegram_bot.load_config', side_effect=lambda *a: self.config)
        runtime.start()
        self.addCleanup(runtime.stop)
        clock = patch('horarios_especiales.services.telegram_bot.timezone.localdate', return_value=datetime.fromtimestamp(STAMP, ZoneInfo('America/Mazatlan')).date())
        clock.start()
        self.addCleanup(clock.stop)
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

    def test_status_rotation_keeps_old_future_requests_and_reaches_all_pending(self):
        day = timezone.localdate()
        obj, payload = create_request_from_rules(raw_text='future', actor=self.user,
            branch_ids=[self.branches['MATRIZ'].pk], rules=[{'kind':'date',
            'start_date':(day + timedelta(days=20)).isoformat(), 'closed_all_day':True, 'time_windows':[]}])
        approve_request(request_obj=obj, actor=self.user, expected_revision=payload['revision'])
        rows = [TelegramHoursUpdate.objects.create(bot_id=12345, update_id=100+i,
            telegram_user_id=777, chat_id=777, request=obj, state='QUEUED',
            created_at=timezone.now()-timedelta(days=30)) for i in range(23)]
        with patch('horarios_especiales.services.telegram_bot.status_text', return_value='Programado, pendiente') as status:
            poll_once(self.config, FakeTelegram())
            self.assertEqual(status.call_count, 20)
            self.assertEqual(TelegramHoursUpdate.objects.filter(receipt__has_key='status_checked_at').count(),20)
            poll_once(self.config, FakeTelegram())
        self.assertEqual(TelegramHoursUpdate.objects.filter(receipt__has_key='status_checked_at').count(),23)
        self.assertEqual(TelegramHoursUpdate.objects.filter(pk__in=[row.pk for row in rows], state='QUEUED').count(),23)
        with patch('horarios_especiales.services.telegram_bot.status_text', return_value='Vigencia concluida'), \
                patch('horarios_especiales.services.telegram_bot.timezone.localdate', return_value=day+timedelta(days=21)):
            poll_once(self.config, FakeTelegram())
            poll_once(self.config, FakeTelegram())
        self.assertFalse(TelegramHoursUpdate.objects.filter(pk__in=[row.pk for row in rows],state='QUEUED').exists())

    def test_automatic_status_sends_one_silent_close_and_ignores_pending_flaps(self):
        for uncertain in (False, True):
            with self.subTest(uncertain_delivery=uncertain):
                obj, payload = create_request_from_rules(raw_text='fixture', actor=self.user,
                    branch_ids=[self.branches['BAMOA'].pk], rules=[{'kind': 'date',
                    'start_date': timezone.localdate().isoformat(), 'closed_all_day': True, 'time_windows': []}])
                approve_request(request_obj=obj, actor=self.user, expected_revision=payload['revision'])
                row = TelegramHoursUpdate.objects.create(bot_id=12345, update_id=200 + uncertain,
                    telegram_user_id=777, chat_id=777, request=obj, state='QUEUED')
                client = FakeTelegram()
                statuses = [
                    'Google BAMOA: pendiente\nTienda: pendiente de verificación.',
                    'Google BAMOA: pendiente\nTienda: confirmado por lectura.',
                    'Google BAMOA: pendiente\nTienda: pendiente de verificación.',
                    f'{obj.request_code}\n{FINALIZED_LABEL}',
                ]
                with patch('horarios_especiales.services.telegram_bot.status_text', side_effect=statuses) as status:
                    for _ in range(3):
                        poll_once(self.config, client)
                    self.assertEqual(client.sent, [])
                    row.refresh_from_db()
                    self.assertEqual(row.state, 'QUEUED')
                    if uncertain:
                        client.fail_send = True
                        with self.assertRaises(TelegramUnavailable):
                            poll_once(self.config, client)
                        client.fail_send = False
                    else:
                        poll_once(self.config, client)
                        self.assertEqual(len(client.sent), 1)
                        self.assertTrue(client.sent[0]['disable_notification'])
                        self.assertIn(FINALIZED_LABEL, client.sent[0]['text'])
                    poll_once(self.config, client)
                    self.assertEqual(len(client.sent), 0 if uncertain else 1)
                    self.assertEqual(status.call_count, 4)
                row.refresh_from_db()
                self.assertEqual(row.state, 'DONE')
                self.assertIn('terminal_notice_attempted_at', row.receipt)
                self.assertIsNotNone(row.request.approved_at)

    def test_previously_reported_final_status_retires_legacy_tracking(self):
        obj, payload = create_request_from_rules(raw_text='fixture', actor=self.user,
            branch_ids=[self.branches['BAMOA'].pk], rules=[{'kind': 'date',
            'start_date': timezone.localdate().isoformat(), 'closed_all_day': True, 'time_windows': []}])
        approve_request(request_obj=obj, actor=self.user, expected_revision=payload['revision'])
        row = TelegramHoursUpdate.objects.create(bot_id=12345, update_id=203,
            telegram_user_id=777, chat_id=777, request=obj, state='QUEUED',
            receipt={'last_status': f'{obj.request_code}\n{FINALIZED_LABEL}'})
        client = FakeTelegram()
        with patch('horarios_especiales.services.telegram_bot.status_text') as status:
            poll_once(self.config, client)
            status.assert_not_called()
        row.refresh_from_db()
        self.assertEqual(row.state, 'DONE')
        self.assertEqual(client.sent, [])

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


    def test_one_preview_and_approval_for_notice_with_exceptions(self):
        from horarios_especiales.services.consumer_receipts import consumer_receipts
        from horarios_especiales.services.google_projection import approved_rules, projection_fields
        from horarios_especiales.services.rules import resolve_approved_rule
        from horarios_especiales.tests_google_projection import REGULAR

        for label, code in [('Sinaloa de Leyva', 'SINALOA_LEYVA'), ('Estación Bamoa', 'BAMOA')]:
            SucursalAlias.objects.create(alias=label, sucursal=self.branches[code])
        matrix = self.branches['MATRIZ']
        matrix.nombre = 'Matriz & <Anexo>'
        matrix.save(update_fields=['nombre'])
        tunnel = self.branches['EL_TUNEL']
        prior, payload = create_request_from_rules(raw_text='Horario previo', actor=self.user,
            branch_ids=[tunnel.pk], rules=[
                {'kind': 'date', 'start_date': f'2026-11-{day}', 'closed_all_day': False,
                 'time_windows': [{'open': '14:00', 'close': '22:00'}]} for day in (23, 24)
            ] + [{'kind': 'weekly', 'start_date': '2026-11-22', 'weekdays': [2],
                  'closed_all_day': True, 'time_windows': []}])
        approve_request(request_obj=prior, actor=self.user, expected_revision=payload['revision'])
        notice = """AVISO IMPORTANTE

Priorizando la integridad de nuestro equipo de trabajo y de quienes nos visitan, les informamos
que durante esta semana contaremos con un horario temporal de atención

Todas las sucursales de 9:00AM – 7:00PM

En excepción de Sinaloa de Leyva y Estacion Bamoa de 9:00AM A 6:00PM

Seguimos atendiendo sus pedidos con el gusto de siempre dentro de este horario. ¡Gracias por su comprensión!"""
        update = self.update(notice)
        monday = datetime(2026, 11, 23, 16, tzinfo=ZoneInfo('America/Mazatlan'))
        update['message']['date'] = int(monday.timestamp())
        client = FakeTelegram([update])
        poll_once(self.config, client)
        row = TelegramHoursUpdate.objects.get(update_id=1)
        self.assertEqual(row.state, 'PREVIEW', row.receipt)
        self.assertEqual(len(client.sent), 1)
        self.assertEqual(len(client.sent[0]['reply_markup']['inline_keyboard']), 1)
        self.assertEqual(client.sent[0]['parse_mode'], 'HTML')
        self.assertIn('<b>Horarios propuestos</b>', client.sent[0]['text'])
        self.assertIn('Matriz &amp; &lt;Anexo&gt;', client.sent[0]['text'])
        self.assertIn('09:00–19:00', client.sent[0]['text'])
        self.assertIn('09:00–18:00', client.sent[0]['text'])
        self.assertIn('23 nov 2026', client.sent[0]['text'])
        self.assertIn('26 nov 2026 al 29 nov 2026\nEL TUNEL', client.sent[0]['text'])
        self.assertIn('miércoles 25 nov 2026 · Cerrado', client.sent[0]['text'])
        self.assertIsNone(row.request.approved_at)
        callback = self.callback(row)
        process_callback(callback, self.config, self.user)
        row.request.refresh_from_db()
        self.assertEqual(row.request.approved_by, self.user)

        for code, close_hour in [('MATRIZ', 19), ('SINALOA_LEYVA', 18), ('BAMOA', 18), ('EL_TUNEL', 19)]:
            with self.subTest(branch=code):
                branch = self.branches[code]
                target = date(2026, 11, 26)
                effective = resolve_approved_rule(branch, target)
                self.assertEqual(effective['request_id'], row.request.pk)
                self.assertEqual(effective['windows'], [{'open': '09:00', 'close': f'{close_hour}:00'}])
                _, owned, _, _, _ = projection_fields(current={'regularHours': REGULAR},
                    rows=approved_rules(branch.pk), today=monday.date(), owned={})
                self.assertEqual(owned[target.isoformat()][0]['closeTime']['hours'], close_hour)
        self.assertEqual(resolve_approved_rule(tunnel, date(2026, 11, 23))['request_id'], prior.pk)
        self.assertTrue(resolve_approved_rule(tunnel, date(2026, 11, 25))['closed_all_day'])
        with patch('horarios_especiales.services.consumer_receipts.timezone.now', return_value=monday):
            consumer_receipts(row.request)
        checked = row.request.execution_summary_json['consumer_readback']['pairs']
        self.assertEqual(len(checked), 67)
        self.assertNotIn('EL_TUNEL:2026-11-25', checked)

    def test_local_ai_keeps_wednesday_and_replaces_only_the_approval_button(self):
        from django.core.management import call_command
        from orquestacion.services import local_router

        local_router.configure(enabled=True)
        call_command('configure_hours_reasoning', 'enable')
        tunnel = self.branches['EL_TUNEL']
        SucursalAlias.objects.create(sucursal=tunnel, alias='Túnel')
        prior, payload = create_request_from_rules(raw_text='Cierre semanal existente', actor=self.user,
            branch_ids=[tunnel.pk], rules=[{'kind': 'weekly', 'start_date': '2026-11-22',
                                           'weekdays': [2], 'closed_all_day': True, 'time_windows': []}])
        approve_request(request_obj=prior, actor=self.user, expected_revision=payload['revision'])
        client = FakeTelegram([self.update(
            'Todas de 9 am a 7 pm excepto BAMOA de 9 am a 6 pm del 23 al 29 de noviembre',
        )])
        poll_once(self.config, client)
        old = TelegramHoursUpdate.objects.get(update_id=1)
        self.assertEqual(old.state, 'PREVIEW')
        nonce = old.nonce
        correction = 'Túnel conserva cerrado días miércoles con el ajuste que ya tenía'
        client.updates.append(self.update(correction, id=2))
        poll_once(self.config, client)
        new = TelegramHoursUpdate.objects.get(update_id=2)
        self.assertEqual(new.state, 'IA_WAIT')
        old.refresh_from_db()
        self.assertEqual(old.nonce, '')
        self.assertEqual(old.request.status, 'BORRADOR')
        job = local_router.claim('mac_ia_02')
        self.assertEqual(job['kind'], 'hours_draft_edit')
        self.assertEqual(job['data']['text'], correction)
        receipt = local_router.finish(job['id'], job['lease'], result={
            'action': 'keep_closed', 'branch': 'Túnel', 'detail': 'miércoles',
        })
        from orquestacion.models import AgentTask
        finished_at = datetime.fromisoformat(AgentTask.objects.get(pk=receipt['id']).output_payload['finished_at'])
        new.expires_at = finished_at
        new.save(update_fields=['expires_at'])
        # The result arrived on time, even if Telegram's next poll is after the deadline.
        poll_once(self.config, client)
        new.refresh_from_db()
        self.assertEqual(new.state, 'PREVIEW', new.receipt)
        self.assertNotEqual(new.nonce, nonce)
        self.assertIsNone(new.request.approved_at)
        self.assertIn('miércoles 25 nov 2026 · Cerrado', new.receipt['text'])
        self.assertIn('09:00–19:00', new.receipt['text'])
        self.assertIn('09:00–18:00', new.receipt['text'])
        old.refresh_from_db()
        self.assertEqual(old.request.status, 'CANCELADO')
        process_callback(self.callback(new, id=3), self.config, self.user)
        new.request.refresh_from_db()
        self.assertEqual(new.request.approved_by, self.user)

    def test_replaces_tunnel_dates_and_keeps_only_wednesday_in_one_preview(self):
        from django.core.management import call_command
        from horarios_especiales.services.google_projection import approved_rules, projection_fields
        from horarios_especiales.services.rules import resolve_approved_rule
        from horarios_especiales.tests_google_projection import REGULAR
        from orquestacion.models import AgentTask
        from orquestacion.services import local_router

        local_router.configure(enabled=True)
        call_command('configure_hours_reasoning', 'enable')
        tunnel = self.branches['EL_TUNEL']
        SucursalAlias.objects.create(sucursal=tunnel, alias='Túnel')
        prior, payload = create_request_from_rules(raw_text='Horarios previos de El Túnel', actor=self.user,
            branch_ids=[tunnel.pk], rules=[
                {'kind': 'date', 'start_date': f'2026-11-{day}', 'closed_all_day': False,
                 'time_windows': [{'open': '14:00', 'close': '22:00'}]} for day in (23, 24)
            ] + [{'kind': 'weekly', 'start_date': '2026-11-22', 'weekdays': [2], 'closed_all_day': True, 'time_windows': []}])
        approve_request(request_obj=prior, actor=self.user, expected_revision=payload['revision'])
        notice = 'Todas de 9 am a 7 pm excepto BAMOA de 9 am a 6 pm del 23 al 29 de noviembre'
        first = 'Los 23 y 24, en el túnel, deben de reemplazarse con el nuevo horario que se propone y el día miércoles se mantiene igual'
        second = 'El único horario que se conserva de el túnel es el miércoles que es cerrado'
        with_date = ('Sucursal el túnel solo debe conservar miércoles 25 de nov 2026 como cerrado. '
                     'Los días 23 y 24 deben ajustas al nuevo horario')
        for index, correction in ((1, first), (3, second), (5, with_date)):
            client = FakeTelegram([self.update(notice, id=index), self.update(correction, id=index+1)])
            poll_once(self.config, client)
            row = TelegramHoursUpdate.objects.get(update_id=index+1)
            self.assertEqual(row.state, 'PREVIEW', row.receipt)
            self.assertFalse(AgentTask.objects.filter(input_payload__kind='hours_draft_edit').exists())
            self.assertIn('miércoles 25 nov 2026 · Cerrado', row.receipt['text'])
            self.assertNotIn('14:00–22:00', row.receipt['text'])
            self.assertIn('23 nov 2026 al 24 nov 2026', row.receipt['text'])
            self.assertIsNone(row.request.approved_at)

        quoted = (
            '• EL TUNEL · lunes 23 nov 2026 · 14:00–22:00\n'
            '• EL TUNEL · martes 24 nov 2026 · 14:00–22:00'
        )
        natural_edits = [
            quoted + '\nNo, esas también de modifican de **09:00–19:00**',
            'te falta modificar\nHorarios que se conservan\n' + quoted,
            'El Túnel sigue cerrado los miércoles y los días 23 y 24 de nov de 2026 deben corregirse con el nuevo horario',
            'Que el Túnel use la franja del aviso lunes y martes; respeten el descanso del miércoles.',
        ]
        for offset, correction in enumerate(natural_edits):
            update_id = 7 + offset * 2
            client = FakeTelegram([self.update(notice, id=update_id), self.update(correction, id=update_id+1)])
            poll_once(self.config, client)
            row = TelegramHoursUpdate.objects.get(update_id=update_id+1)
            self.assertEqual(row.state, 'IA_WAIT', row.receipt)
            job = local_router.claim('mac_ia_02')
            if offset == 1:
                self.assertIsNone(local_router.claim('mac_ia'))
                local_router.finish(job['id'], job['lease'], error='offline')
                poll_once(self.config, client)
                row.refresh_from_db()
                self.assertEqual(row.state, 'IA_WAIT', row.receipt)
                self.assertEqual(local_router.panel()['ready_by_node'], {'mac_ia': 1})
                self.assertIsNone(local_router.claim('mac_ia_02'))
                alternate = local_router.claim('mac_ia')
                self.assertEqual(alternate['id'], job['id'])
                self.assertEqual(alternate['model'], 'qwen4b')
                job = alternate
            self.assertEqual(job['data']['text'], correction)
            self.assertIn('14:00–22:00', job['data']['draft'])
            self.assertIn('2026-11-25', job['data']['draft'])
            self.assertIn('· Cerrado', job['data']['draft'])
            local_router.finish(job['id'], job['lease'], result={
                'action': 'replace_draft_dates', 'branch': 'EL TUNEL', 'detail': '2026-11-23,2026-11-24',
            })
            poll_once(self.config, client)
            row.refresh_from_db()
            self.assertEqual(row.state, 'PREVIEW', row.receipt)
            self.assertIn('23 nov 2026 al 24 nov 2026', row.receipt['text'])
            self.assertNotIn('14:00–22:00', row.receipt['text'])
            self.assertIn('miércoles 25 nov 2026 · Cerrado', row.receipt['text'])
            self.assertIsNone(row.request.approved_at)

        from horarios_especiales.services.telegram_reasoning import revise
        draft = TelegramHoursUpdate.objects.get(update_id=update_id).request.canonical_payload
        with self.assertRaisesRegex(ValueError, 'Mantengo el cierre'):
            revise(draft, job['data'], {
                'action': 'replace_draft_dates', 'branch': 'EL TUNEL', 'detail': '2026-11-25',
            })
        with self.assertRaisesRegex(ValueError, 'no son excepciones'):
            revise(draft, job['data'], {
                'action': 'replace_draft_dates', 'branch': 'EL TUNEL', 'detail': '2026-12-01',
            })
        _, approved_exceptions, _ = revise(draft, job['data'], {
            'action': 'replace_draft_dates', 'branch': 'EL TUNEL', 'detail': '2026-11-23,2026-11-24,2026-11-25',
        })
        tunnel_rule = next(item for item in approved_exceptions if tunnel.pk in item.get('branch_ids', []))
        self.assertEqual(tunnel_rule['excluded_dates'], ['2026-11-25'])
        self.assertEqual(tunnel_rule['override_dates'], ['2026-11-23', '2026-11-24'])
        self.assertEqual(resolve_approved_rule(tunnel, date(2026, 11, 23))['request_id'], prior.pk)
        process_callback(self.callback(row, id=15), self.config, self.user)
        row.request.refresh_from_db()
        self.assertEqual(row.request.approved_by, self.user)
        for day in (23, 24, 26, 27, 28, 29):
            effective = resolve_approved_rule(tunnel, date(2026, 11, day))
            self.assertEqual(effective['windows'], [{'open': '09:00', 'close': '19:00'}])
            self.assertEqual(effective['request_id'], row.request.pk)
        self.assertTrue(resolve_approved_rule(tunnel, date(2026, 11, 25))['closed_all_day'])
        self.assertTrue(resolve_approved_rule(tunnel, date(2026, 12, 2))['closed_all_day'])
        _, owned, _, _, _ = projection_fields(current={'regularHours': REGULAR},
            rows=approved_rules(tunnel.pk), today=date(2026, 11, 23), owned={})
        self.assertEqual(owned['2026-11-23'][0]['closeTime']['hours'], 19)
        self.assertEqual(owned['2026-11-24'][0]['closeTime']['hours'], 19)
        self.assertNotIn('2026-11-25', owned)

    def test_ambiguous_replacement_uses_ai_and_does_not_reissue_wrong_card(self):
        from django.core.management import call_command
        from orquestacion.services import local_router

        local_router.configure(enabled=True)
        call_command('configure_hours_reasoning', 'enable')
        tunnel = self.branches['EL_TUNEL']
        SucursalAlias.objects.create(sucursal=tunnel, alias='Túnel')
        prior, payload = create_request_from_rules(raw_text='Miércoles cerrado', actor=self.user,
            branch_ids=[tunnel.pk], rules=[{'kind': 'weekly', 'start_date': '2026-11-22',
                                          'weekdays': [2], 'closed_all_day': True, 'time_windows': []}])
        approve_request(request_obj=prior, actor=self.user, expected_revision=payload['revision'])
        notice = 'Todas de 9 am a 7 pm excepto BAMOA de 9 am a 6 pm del 23 al 29 de noviembre'
        client = FakeTelegram([self.update(notice)])
        poll_once(self.config, client)
        draft = TelegramHoursUpdate.objects.get(update_id=1)
        wrong_date = 'El Túnel solo debe conservar miércoles 26 de nov 2026 como cerrado.'
        client.updates.append(self.update(wrong_date, id=2))
        poll_once(self.config, client)
        rejected = TelegramHoursUpdate.objects.get(update_id=2)
        self.assertIsNone(rejected.request_id)
        self.assertIn('no coincide con la fecha', rejected.receipt['text'])
        self.assertIsNone(local_router.claim('mac_ia_02'))

        ambiguous = 'El Túnel solo debe conservar miércoles 25 de nov 2026 como cerrado. Cambia el 99.'
        client.updates.append(self.update(ambiguous, id=3))
        poll_once(self.config, client)
        waiting = TelegramHoursUpdate.objects.get(update_id=3)
        self.assertEqual(waiting.state, 'IA_WAIT')
        self.assertIn('La IA local', client.sent[-1]['text'])
        job = local_router.claim('mac_ia_02')
        self.assertEqual(job['data']['text'], ambiguous)
        local_router.finish(job['id'], job['lease'], result={
            'action': 'keep_closed', 'branch': 'Túnel', 'detail': 'miércoles',
        })
        poll_once(self.config, client)
        waiting.refresh_from_db()
        draft.refresh_from_db()
        self.assertIsNone(waiting.request_id)
        self.assertNotIn('reply_markup', client.sent[-1])
        self.assertEqual(draft.request.status, 'BORRADOR')
        self.assertEqual(draft.nonce, '')

    def test_complex_first_notice_is_interpreted_with_dated_exceptions(self):
        from django.core.management import call_command
        from orquestacion.services import local_router

        local_router.configure(enabled=True)
        call_command('configure_hours_reasoning', 'enable')
        tunnel = self.branches['EL_TUNEL']
        prior, payload = create_request_from_rules(raw_text='Horarios previos de El Túnel', actor=self.user,
            branch_ids=[tunnel.pk], rules=[
                {'kind': 'date', 'start_date': f'2026-11-{day}', 'closed_all_day': False,
                 'time_windows': [{'open': '14:00', 'close': '22:00'}]} for day in (23, 24)
            ] + [{'kind': 'weekly', 'start_date': '2026-11-22', 'weekdays': [2], 'closed_all_day': True, 'time_windows': []}])
        approve_request(request_obj=prior, actor=self.user, expected_revision=payload['revision'])
        notice = ('TODAS las sucursales de 9 am a 7 pm, excepto Sinaloa Leyva y Bamoa de 9 am a 6 pm, '
                  'del 23 al 29 de noviembre. El Tunel abre del 23 al 24 de 9 am a 7 pm. '
                  'El Tunel cerrado el miércoles 25 de novimebre.')
        client = FakeTelegram([self.update(notice)])
        poll_once(self.config, client)
        row = TelegramHoursUpdate.objects.get(update_id=1)
        self.assertEqual(row.state, 'IA_WAIT', row.receipt)
        job = local_router.claim('mac_ia')
        self.assertEqual(job['data']['mode'], 'initial')
        self.assertEqual(job['data']['text'], notice)
        local_router.finish(job['id'], job['lease'], result={
            'start': '2026-11-23', 'end': '2026-11-29', 'general': '09:00-19:00',
            'groups': [
                {'branches': ['Sinaloa Leyva', 'Bamoa'], 'start': None, 'end': None, 'hours': '09:00-18:00'},
                {'branches': ['El Tunel'], 'start': '2026-11-23', 'end': '2026-11-24', 'hours': '09:00-19:00'},
                {'branches': ['El Tunel'], 'start': '2026-11-25', 'end': '2026-11-25', 'hours': 'closed'},
            ],
        })
        poll_once(self.config, client)
        row.refresh_from_db()
        self.assertEqual(row.state, 'PREVIEW', row.receipt)
        self.assertIn('23 nov 2026 al 24 nov 2026', row.receipt['text'])
        self.assertIn('miércoles 25 nov 2026 · Cerrado', row.receipt['text'])
        self.assertNotIn('14:00–22:00', row.receipt['text'])
        self.assertIn('09:00–18:00', row.receipt['text'])
        self.assertIsNone(row.request.approved_at)

    def test_local_ai_ambiguous_hour_pauses_button_but_keeps_draft_for_next_edit(self):
        from django.core.management import call_command
        from orquestacion.services import local_router

        local_router.configure(enabled=True)
        call_command('configure_hours_reasoning', 'enable')
        client = FakeTelegram([self.update('BAMOA del 23 de noviembre al 29 de noviembre de 9 am a 6 pm')])
        poll_once(self.config, client)
        old = TelegramHoursUpdate.objects.get(update_id=1)
        nonce = old.nonce
        for index, text, detail in [(2, 'BAMOA mejor hasta las 7', '7'), (3, 'BAMOA hasta 5 pm', '5 pm')]:
            client.updates.append(self.update(text, id=index))
            poll_once(self.config, client)
            job = local_router.claim('mac_ia_02')
            local_router.finish(job['id'], job['lease'], result={
                'action': 'change_close', 'branch': 'BAMOA', 'detail': detail,
            })
            poll_once(self.config, client)
            attempted = TelegramHoursUpdate.objects.get(update_id=index)
            if index == 2:
                self.assertIsNone(attempted.request_id)
                self.assertIn('Aclara la hora', attempted.receipt['text'])
                old.refresh_from_db()
                self.assertEqual(old.request.status, 'BORRADOR')
                self.assertEqual(old.nonce, '')
                self.assertNotIn('reply_markup', client.sent[-1])
            else:
                self.assertEqual(attempted.state, 'PREVIEW')
                self.assertIsNone(attempted.request.approved_at)
                self.assertNotEqual(attempted.nonce, nonce)
                self.assertIn('09:00–17:00', attempted.receipt['text'])

    def test_notice_requests_clarification_for_ambiguous_or_unparsed_qualifiers(self):
        cases = (
            'Todas las sucursales de 9 am a 7 pm excepto BAMOA de 9 am a 6 pm por una semana',
            'Todas de 9 am a 7 pm excepto BAMOA de 9 am a 6 pm excepto BAMOA de 9 am a 5 pm durante esta semana',
            'Todas de 9 am a 7 pm excepto BAMOA de 9 am a 6 pm y el martes cerrado durante esta semana',
        )
        for message in cases:
            with self.subTest(message=message), self.assertRaises(ValueError):
                parse_grouped_instruction(message, STAMP)
        ids, rules = parse_grouped_instruction(
            'Todas de 9 am a 7 pm excepto BAMOA de 9 am a 6 pm por esta semana del día 23 al 29 de noviembre',
            STAMP,
        )
        self.assertEqual(len(ids), 10)
        self.assertEqual(rules[1]['branch_ids'], [self.branches['BAMOA'].pk])
        self.assertEqual(rules[1]['start_date'], '2026-11-23')
        self.assertEqual(rules[1]['end_date'], '2026-11-29')

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
        read = patch('horarios_especiales.services.consumer_receipts.check_pair', return_value={})
        read.start()
        self.addCleanup(read.stop)
        self.user = User.objects.create_user(username='hours_owner', is_superuser=True)
        runtime = patch('horarios_especiales.services.telegram_bot.load_config', return_value=dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=self.user.pk))
        runtime.start()
        self.addCleanup(runtime.stop)
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
