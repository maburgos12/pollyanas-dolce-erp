"""Audio contract: no paid calls and no real Telegram traffic."""
import base64
import hashlib
import io
import shutil
import wave
from datetime import timedelta
from unittest.mock import patch

from django.core.exceptions import PermissionDenied
from django.test import TestCase, SimpleTestCase
from django.utils import timezone

from horarios_especiales.models import TelegramHoursCursor, TelegramHoursUpdate
from horarios_especiales.services import telegram_bot as bot
from horarios_especiales import tests_telegram as fixtures


def pcm(seconds=1):
    return b'\x00\x00' * (16000 * seconds)


class AudioTests(TestCase):
    setUp = fixtures.TelegramHoursTests.setUp
    update = fixtures.TelegramHoursTests.update
    preview = fixtures.TelegramHoursTests.preview

    def voice(self, id=1, **metadata):
        payload = self.update(id=id)
        del payload['message']['text']
        payload['message']['voice'] = {'file_id': 'file_1', 'duration': 1, 'file_size': 100, **metadata}
        bot.persist_updates(self.config, [payload])
        row = TelegramHoursUpdate.objects.get(update_id=id)
        bot.process_received(row, self.config, self.user)
        return row

    def test_voice_invalidates_preview_immediately(self):
        old, _ = self.preview()
        row = self.voice(id=2)
        old.refresh_from_db()
        self.assertEqual(old.state, 'CANCELLED')
        self.assertEqual(row.state, 'AUDIO_WAIT')
        self.assertIsNone(row.request_id)

    def test_oversize_invalidates_without_download(self):
        old, _ = self.preview()
        row = self.voice(id=2, file_size=8*1024*1024+1)
        old.refresh_from_db()
        self.assertEqual(old.state, 'CANCELLED')
        self.assertEqual(row.state, 'REPLY')

    def test_local_finish_preview_only_and_heard_text(self):
        from horarios_especiales.services import telegram_audio as audio
        row = self.voice()
        with patch.object(audio, 'download_pcm', return_value=pcm()):
            job = audio.claim(self.config)
        self.assertEqual(job['sha256'], hashlib.sha256(pcm()).hexdigest())
        self.assertNotIn('token', job)
        result = audio.finish(self.config, {'id': row.pk, 'lease':job['lease'], 'sha256':job['sha256'], 'text':'Todas hoy cerradas'})
        self.assertTrue(result['accepted'])
        row.refresh_from_db()
        self.assertEqual(row.state, 'PREPARED')
        self.assertIsNone(row.request.approved_at)
        self.assertEqual(row.receipt['parse_mode'], 'HTML')
        self.assertIn('<b>Escuché</b>\nTodas hoy cerradas', row.receipt['text'])
        self.assertNotIn('pcm', row.receipt)
        self.assertFalse(audio.finish(self.config, {'id':row.pk, 'lease':job['lease'], 'sha256':job['sha256'], 'text':'Todas hoy cerradas'})['accepted'])

    def test_correction_rejects_late_worker_even_before_processing(self):
        from horarios_especiales.services import telegram_audio as audio
        row = self.voice()
        with patch.object(audio, 'download_pcm', return_value=pcm()):
            job = audio.claim(self.config)
        bot.persist_updates(self.config, [self.update('Matriz mañana cerrada', id=2)])
        self.assertFalse(audio.finish(self.config, {'id':row.pk, 'lease':job['lease'], 'sha256':job['sha256'], 'text':'Todas hoy cerradas'})['accepted'])
        row.refresh_from_db()
        self.assertIsNone(row.request_id)

    def test_owner_permissions_checked_claim_finish_fallback(self):
        from horarios_especiales.services import telegram_audio as audio
        row = self.voice()
        with patch.object(audio, 'download_pcm', return_value=pcm()):
            job = audio.claim(self.config)
        self.user.is_active = False
        self.user.save()
        for operation in (lambda: audio.claim(self.config), lambda: audio.advance(self.config),
                lambda: audio.finish(self.config, {'id': row.pk, 'lease':job['lease'], 'sha256':job['sha256'], 'text':'Todas hoy cerradas'})):
            with self.assertRaises(PermissionDenied):
                operation()

    def test_offline_fallback_reserves_once_and_timeout_never_refunded(self):
        from horarios_especiales.services import telegram_audio as audio
        row = self.voice()
        row.expires_at = timezone.now() - timedelta(seconds=1)
        row.save()
        with patch.object(audio, 'download_pcm', return_value=pcm()), patch.object(audio, 'paid_transcribe', side_effect=TimeoutError('SECRET')) as paid:
            audio.advance(self.config)
            audio.advance(self.config)
        self.assertEqual(paid.call_count, 1)
        self.assertEqual(TelegramHoursCursor.objects.get().audio_reserved_cents, 3)
        row.refresh_from_db()
        self.assertNotIn('SECRET', str(row.receipt))
        self.assertEqual(row.state, 'REPLY')

    def test_cap_does_not_block_text(self):
        from horarios_especiales.services import telegram_audio as audio
        row = self.voice()
        TelegramHoursCursor.objects.update(audio_reserved_cents=498)
        row.expires_at = timezone.now() - timedelta(seconds=1)
        row.save()
        with patch.object(audio, 'download_pcm', return_value=pcm()), patch.object(audio, 'paid_transcribe') as paid:
            audio.advance(self.config)
        paid.assert_not_called()
        bot.persist_updates(self.config, [self.update(id=2)])
        newer = TelegramHoursUpdate.objects.get(update_id=2)
        bot.process_received(newer, self.config, self.user)
        self.assertEqual(newer.state, 'PREPARED')

    def test_crash_reserved_never_retries(self):
        from horarios_especiales.services import telegram_audio as audio
        row = self.voice()
        row.state = 'AUDIO_PAID'
        row.expires_at = timezone.now() - timedelta(seconds=1)
        row.receipt = {'reserved_cents':3}
        row.save()
        with patch.object(audio, 'paid_transcribe') as paid:
            audio.advance(self.config)
        paid.assert_not_called()
        row.refresh_from_db()
        self.assertEqual(row.state, 'REPLY')

    def test_audio_never_guesses_partial_branch_names(self):
        from horarios_especiales.services import telegram_audio as audio
        row = self.voice()
        with patch.object(audio, 'download_pcm', return_value=pcm()):
            job = audio.claim(self.config)
        audio.finish(self.config, {'id':row.pk, 'lease':job['lease'], 'sha256':job['sha256'], 'text':'Matr hoy cerrada'})
        row.refresh_from_db()
        self.assertEqual(row.state, 'REPLY')
        self.assertIsNone(row.request_id)

    def test_non_owner_never_queues(self):
        payload = self.update()
        payload['message']['from']['id'] = 888
        payload['message']['voice'] = {'file_id':'secret', 'duration':1}
        bot.persist_updates(self.config, [payload])
        self.assertFalse(TelegramHoursUpdate.objects.exists())


class AudioDecodeTests(SimpleTestCase):
    def test_decoder_has_explicit_secret_free_environment(self):
        import os
        from types import SimpleNamespace
        from horarios_especiales.services import telegram_audio as audio
        with patch.dict(os.environ, {'ERP_AUDIO_SECRET_SENTINEL':'must-not-leak'}), patch.object(audio.shutil, 'which', return_value='/usr/bin/ffmpeg'), patch.object(audio.subprocess, 'run', return_value=SimpleNamespace(stdout=pcm())) as run:
            audio.decode_pcm(b'fixture')
        self.assertEqual(run.call_args.kwargs.get('env'), {'PATH':'/usr/bin:/bin', 'LANG':'C'})

    def test_real_duration_not_metadata(self):
        from horarios_especiales.services import telegram_audio as audio
        if not shutil.which('ffmpeg'):
            self.skipTest('ffmpeg unavailable')
        for seconds in (1, 181):
            data = io.BytesIO()
            with wave.open(data, 'wb') as wav:
                wav.setnchannels(1); wav.setsampwidth(2); wav.setframerate(16000)
                wav.writeframes(pcm(seconds))
            if seconds == 1:
                self.assertEqual(len(audio.decode_pcm(data.getvalue())), 32000)
            else:
                with self.assertRaises(ValueError):
                    audio.decode_pcm(data.getvalue())

    def test_spoken_hours_conservative(self):
        from horarios_especiales.services.telegram_audio import normalize_spoken
        self.assertEqual(normalize_spoken('Plaza Nio mañana de una de la tarde a nueve de la noche.'), 'Plaza Nio mañana de 1 pm a 9 pm.')
        self.assertEqual(normalize_spoken('Neo mañana de una a nueve'), 'Neo mañana de una a nueve')
        self.assertEqual(normalize_spoken('doce de la noche'), '12 am')
        self.assertEqual(normalize_spoken('doce de la mañana'), 'doce de la mañana')

    def test_download_rejects_paths_before_fetch(self):
        from horarios_especiales.services import telegram_audio as audio
        from types import SimpleNamespace
        row = SimpleNamespace(payload={'message': {'voice': {'file_id':'a'}}})
        for path in ('../../etc/passwd', 'https://attacker/audio.ogg', 'voice/x.ogg?token=x', 'voice/x.m3u8'):
            with patch.object(audio, 'actor_for'), patch.object(bot, 'authorized', return_value=True), patch.object(bot.TelegramClient, 'call', return_value={'file_path':path}), patch.object(audio.requests, 'Session') as session:
                with self.assertRaises(ValueError):
                    audio.download_pcm(row, {'token':'SECRET'})
                session.assert_not_called()

    def test_rpc_rejects_unknown_action_without_outputting_secrets(self):
        from django.core.management import call_command
        from django.core.management.base import CommandError
        from unittest.mock import patch
        with patch('sys.stdin', io.StringIO('{"action":"shell","command":"SECRET"}')):
            with self.assertRaisesMessage(CommandError, 'Solicitud de audio inválida'):
                call_command('hours_audio_rpc')


from django.test import TransactionTestCase


class AudioBudgetConcurrencyTests(TransactionTestCase):
    def test_two_reservations_for_same_job_charge_once(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from django.db import close_old_connections
        from django.contrib.auth.models import User
        from horarios_especiales.services import telegram_audio as audio
        user = User.objects.create_user(username='audio_budget_owner', is_superuser=True)
        config = dict(bot_id=45678, telegram_user_id=777, telegram_chat_id=777, erp_user_id=user.pk)
        TelegramHoursCursor.objects.create(bot_id=config['bot_id'], audio_reserved_cents=496)
        row = TelegramHoursUpdate.objects.create(bot_id=config['bot_id'], update_id=1,
            telegram_user_id=777, chat_id=777, erp_user=user, state='AUDIO_WAIT')
        barrier = Barrier(2)
        def reserve():
            close_old_connections()
            try:
                copy = TelegramHoursUpdate.objects.get(pk=row.pk)
                barrier.wait(timeout=5)
                return audio.reserve(copy, config)
            finally:
                close_old_connections()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _:reserve(), range(2)))
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(TelegramHoursCursor.objects.get().audio_reserved_cents, 499)
        row.refresh_from_db()
        self.assertEqual(row.state, 'AUDIO_PAID')
