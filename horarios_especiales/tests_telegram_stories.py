from datetime import datetime
import copy
import hashlib
from io import BytesIO
from datetime import timedelta
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.core.exceptions import PermissionDenied
from django.test import SimpleTestCase, TestCase
from django.utils import timezone
from PIL import Image

from horarios_especiales.services.story_art import build_schedule, render_notice


class StoryArtTests(SimpleTestCase):
    def setUp(self):
        self.zone = ZoneInfo('America/Mazatlan')
        self.now = datetime(2026, 12, 1, 8, 5, tzinfo=self.zone)

    def test_daily_windows_stay_inside_approved_dates_and_disclose_last_day(self):
        rules = [{'kind': 'range', 'start_date': '2026-12-01', 'end_date': '2026-12-03'}]
        result, note = build_schedule(rules, self.now)
        self.assertEqual(result['valid_until'], '2026-12-04T00:00:00-07:00')
        starts = [row['scheduled_at'] for row in result['schedule_windows']]
        self.assertEqual(starts, ['2026-12-01T09:00:00-07:00', '2026-12-02T09:00:00-07:00'])
        self.assertIn('03/12', note)

    def test_cannot_offer_story_for_one_day_without_confirmed_early_removal(self):
        rules = [{'kind': 'date', 'start_date': '2026-12-01'}]
        with self.assertRaisesRegex(ValueError, '24 horas'):
            build_schedule(rules, self.now)

    def test_image_fits_required_format(self):
        raw = render_notice('1 al 3 de diciembre\n• 09:00–19:00 · Sucursal Bamoa\nMiércoles · El Túnel cerrado')
        with Image.open(BytesIO(raw)) as image:
            self.assertEqual((image.format, image.size), ('JPEG', (1080, 1920)))

    def test_spanish_accents_and_schedule_symbols_have_real_glyphs(self):
        missing_glyph = render_notice('\U0010ffff')
        for character in ('é', 'ú', 'ñ', '•', '–'):
            with self.subTest(character=character):
                self.assertNotEqual(render_notice(character), missing_glyph)


from horarios_especiales.models import SolicitudHorarioEspecial, TelegramHoursStory, TelegramHoursUpdate
from horarios_especiales.services import telegram_stories as stories
from horarios_especiales.services.story_planner import MarketingClient, MarketingError
from horarios_especiales.services.telegram_bot import poll_once


class StoryLifecycleTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='story-owner', is_superuser=True)
        self.config = dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=self.user.pk)
        self.request = SolicitudHorarioEspecial.objects.create(
            raw_command='Synthetic hours fixture', requested_by=self.user, approved_by=self.user,
            approved_at=timezone.now(), status='APROBADO',
            canonical_payload={'schema_version': 2, 'revision': 'a' * 64})
        self.source = TelegramHoursUpdate.objects.create(
            bot_id=12345, update_id=1, telegram_user_id=777, chat_id=777,
            erp_user=self.user, request=self.request, state='DONE')
        self.story = TelegramHoursStory.objects.create(
            source=self.source, request=self.request, revision='a' * 64, art_revision=1,
            nonce='b' * 32, snapshot=['original'], state='SCHEDULED',
            draft={'review_expires_at': (timezone.now() + timedelta(hours=1)).isoformat()},
            remote={'draft_id': f"hours:{self.request.pk}:{'a' * 64}:1", 'draft_digest': 'c' * 64},
            approval_body='{"immutable":"receipt"}', preview_message_id=500)
        self.telegram, self.marketing = Mock(), Mock()
        self.marketing.request.return_value = dict(
            request_id=self.request.pk, erp_revision_digest='a' * 64, art_revision=1,
            invalidated_draft_ids=[self.story.remote['draft_id']])
        for target, value in (
            ('telegram_bot.load_config', self.config),
            ('telegram_bot.config_for_row', self.config),
            ('telegram_bot.actor_for', self.user),
            ('telegram_bot.approval_snapshot', ['original']),
            ('telegram_stories.approved', True),
        ):
            mock = patch('horarios_especiales.services.' + target, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)

    def test_changed_schedule_revokes_already_scheduled_story(self):
        with patch('horarios_especiales.services.telegram_bot.approval_snapshot', return_value=['changed']):
            stories.advance(self.config, self.telegram, self.marketing)
        self.assertTrue(self.marketing.request.call_args.args[1].endswith('/invalidate'))
        self.story.refresh_from_db()
        self.assertEqual(self.story.state, 'STOPPED')

    def test_changed_schedule_revokes_accepted_story_before_notice(self):
        self.story.state = 'NOTICE'
        self.story.remote.update(post_ids=['post1'], notice='Scheduled')
        self.story.save()
        stories.process(self.story, self.telegram, self.marketing, False, timezone.now())
        self.assertTrue(self.marketing.request.call_args.args[1].endswith('/invalidate'))
        self.telegram.call.assert_not_called()

    def test_withdrawn_identity_revokes_notice_without_notifying(self):
        self.story.state = 'NOTICE'
        self.story.remote.update(post_ids=['post1'], notice='Scheduled')
        self.story.save()
        with patch('horarios_especiales.services.telegram_bot.config_for_row', side_effect=PermissionDenied):
            stories.advance(self.config, self.telegram, self.marketing)
        self.assertTrue(self.marketing.request.call_args.args[1].endswith('/invalidate'))
        self.telegram.call.assert_not_called()

    def test_ambiguous_approval_survives_terminal_transport_error(self):
        self.story.state = 'APPROVE'
        self.story.save()
        self.marketing.request.side_effect = MarketingError('hours_publishing_disabled')
        stories.process(self.story, self.telegram, self.marketing, True, timezone.now())
        self.story.refresh_from_db()
        self.assertEqual(self.story.state, 'APPROVE')
        self.assertEqual(self.story.approval_body, '{"immutable":"receipt"}')
        self.assertNotIn('post_ids', self.story.remote)

    def test_revocation_survives_terminal_error_without_notification(self):
        self.story.state = 'INVALIDATE'
        self.marketing.request.side_effect = MarketingError('http_401')
        stories.process(self.story, self.telegram, self.marketing, False, timezone.now(), notify=False)
        self.story.refresh_from_db()
        self.assertEqual(self.story.state, 'INVALIDATE')
        self.assertEqual(self.story.error_code, 'http_401')

    def test_revocation_requires_matching_acknowledgement(self):
        self.story.state = 'INVALIDATE'
        self.marketing.request.return_value = {'request_id': self.request.pk + 1}
        stories.process(self.story, self.telegram, self.marketing, False, timezone.now(), notify=False)
        self.story.refresh_from_db()
        self.assertEqual(self.story.state, 'INVALIDATE')
        self.assertEqual(self.story.error_code, 'invalid_revocation_receipt')

    def test_cleanup_only_mode_never_delivers_pending_approval(self):
        self.story.state = 'APPROVE'
        self.story.save()
        stories.advance(self.config, None, self.marketing, revocations_only=True)
        self.marketing.request.assert_not_called()

    def test_lost_approval_response_reuses_exact_receipt_and_recovers_ids(self):
        self.story.state = 'APPROVE'
        accepted = dict(draft_id=self.story.remote['draft_id'], draft_digest='c' * 64,
                        erp_revision_digest='a' * 64, art_revision=1, status='scheduled', post_ids=['ig', 'fb'])
        self.marketing.request.side_effect = [MarketingError('transport_unavailable', retryable=True), accepted]
        for _ in range(2):
            stories.process(self.story, self.telegram, self.marketing, True, timezone.now())
            self.story.refresh_from_db()
        self.assertEqual(self.story.state, 'NOTICE')
        self.assertEqual(self.story.remote['post_ids'], ['ig', 'fb'])
        bodies = [call.args[2] for call in self.marketing.request.call_args_list]
        self.assertEqual(bodies, [self.story.approval_body.encode()] * 2)
        self.telegram.call.assert_not_called()

    def test_last_identity_withdrawal_still_drains_revocations(self):
        with patch('horarios_especiales.services.telegram_bot.active_configs', return_value=[]), \
                patch('horarios_especiales.services.telegram_bot.actor_for', side_effect=PermissionDenied), \
                patch('horarios_especiales.services.story_planner.MarketingClient.configured', return_value=self.marketing), \
                patch('horarios_especiales.services.telegram_stories.advance') as advance:
            with self.assertRaises(PermissionDenied):
                poll_once(self.config, self.telegram)
        advance.assert_called_once_with(self.config, None, self.marketing, revocations_only=True)
        self.telegram.call.assert_not_called()

    def test_cleanup_transport_available_when_publishing_feature_is_disabled(self):
        with patch.dict('os.environ', {'HOURS_STORIES_ENABLED': 'false', 'HOURS_BOT_SIGNING_SECRET': 'fictional-service-secret'}):
            self.assertIsNone(MarketingClient.configured())
            self.assertIsNotNone(MarketingClient.configured(for_revocation=True))

    def callback(self, **overrides):
        payload = {'callback_query': {'from': {'id': 777}, 'message': {
            'message_id': 500, 'chat': {'id': 777, 'type': 'private'}},
            'data': f'storyapprove:{self.story.pk}:{self.story.nonce}'}}
        values = dict(bot_id=12345, update_id=20, telegram_user_id=777, chat_id=777,
                      erp_user=self.user, message_id=500, payload=payload)
        values.update(overrides)
        return TelegramHoursUpdate.objects.create(**values)

    def test_second_approval_is_bound_to_identity_message_nonce_and_expiry(self):
        self.story.state = 'REVIEW'
        self.story.save()
        row = self.callback()
        with patch('horarios_especiales.services.story_planner.MarketingClient.configured', return_value=self.marketing):
            for key, value in (('chat_id', 888), ('message_id', 501), ('telegram_user_id', 888), ('bot_id', 54321)):
                candidate = copy.copy(row)
                setattr(candidate, key, value)
                self.assertIn('no es válido', stories.handle_callback(candidate, self.config, self.user))
            wrong_nonce = copy.copy(row)
            wrong_nonce.payload = copy.deepcopy(row.payload)
            wrong_nonce.payload['callback_query']['data'] = f'storyapprove:{self.story.pk}:' + 'd' * 32
            self.assertIn('no es válido', stories.handle_callback(wrong_nonce, self.config, self.user))
            self.story.draft['review_expires_at'] = (timezone.now() - timedelta(seconds=1)).isoformat()
            self.story.save()
            self.assertIn('venció', stories.handle_callback(row, self.config, self.user))
        self.story.refresh_from_db()
        self.assertEqual(self.story.state, 'REVIEW')
        self.assertEqual(self.story.approval_body, '{"immutable":"receipt"}')

    def test_second_approval_rejects_changed_snapshot_and_preserves_receipt_on_replay(self):
        self.story.state = 'REVIEW'
        self.story.save()
        row = self.callback()
        with patch('horarios_especiales.services.story_planner.MarketingClient.configured', return_value=self.marketing):
            with patch('horarios_especiales.services.telegram_bot.approval_snapshot', return_value=['changed']):
                self.assertIn('Hubo un cambio', stories.handle_callback(row, self.config, self.user))
            self.assertIn('registrada', stories.handle_callback(row, self.config, self.user))
            self.story.refresh_from_db()
            receipt = self.story.approval_body
            row.update_id += 1
            self.assertIn('ya tiene una aprobación', stories.handle_callback(row, self.config, self.user))
            self.story.refresh_from_db()
            self.assertEqual(self.story.approval_body, receipt)
            self.assertEqual(self.story.state, 'APPROVE')
        self.marketing.request.assert_not_called()


class StoryRoundTripTests(TestCase):
    def test_real_first_approval_stage_art_callback_and_invalidation(self):
        from core.models import Sucursal
        from horarios_especiales.models import SucursalPlataformaExterna
        from horarios_especiales.services.requests import approve_request, create_request_from_rules
        from horarios_especiales.services.telegram_bot import approval_snapshot, persist_updates

        user = User.objects.create_user(username='story-roundtrip', is_superuser=True)
        config = dict(bot_id=12345, telegram_user_id=777, telegram_chat_id=777, erp_user_id=user.pk)
        branch = Sucursal.objects.create(codigo='BAMOA', nombre='Bamoa', activa=True)
        SucursalPlataformaExterna.objects.create(sucursal=branch, external_location_name=f'locations/{branch.pk}')
        first = timezone.now().astimezone(ZoneInfo('America/Mazatlan')).date() + timedelta(days=2)
        with patch('horarios_especiales.services.google_projection.preview_google_conflicts', return_value='Pendiente'), \
                patch('horarios_especiales.services.consumer_receipts.check_pair', return_value={}), \
                patch('horarios_especiales.services.telegram_bot.load_config', return_value=config), \
                patch('horarios_especiales.services.story_planner.MarketingClient.configured', return_value=Mock()):
            obj, payload = create_request_from_rules(raw_text='Isolated approved notice', actor=user,
                branch_ids=[branch.pk], rules=[dict(kind='range', start_date=first.isoformat(),
                    end_date=(first + timedelta(days=3)).isoformat(), closed_all_day=False,
                    time_windows=[dict(open='09:00', close='19:00')])])
            approve_request(request_obj=obj, actor=user, expected_revision=payload['revision'])
            obj.refresh_from_db()
            source = TelegramHoursUpdate.objects.create(bot_id=12345, update_id=1, telegram_user_id=777,
                chat_id=777, erp_user=user, request=obj, state='DONE')
            stories.stage(source, user)
            story = TelegramHoursStory.objects.get(request=obj)
            self.assertEqual(story.state, 'ASSET')
            self.assertEqual(story.revision, payload['revision'])
            self.assertEqual(story.draft['placement'], 'story')
            self.assertEqual(story.draft['platforms'], ['instagram', 'facebook'])
            self.assertIn((first + timedelta(days=3)).strftime('%d/%m'), story.remote['schedule_note'])
            with self.assertRaisesRegex(ValueError, 'ya tiene una historia'):
                stories.stage(source, user)
            image = render_notice(story.draft['caption'])
            remote = dict(draft_id=f'hours:{obj.pk}:{story.revision}:1', draft_digest='c' * 64,
                          erp_revision_digest=story.revision, art_revision=1)
            marketing, telegram = Mock(), Mock()
            telegram.call.return_value = dict(message_id=500, chat=dict(id=777, type='private'),
                                              **{'from': dict(id=12345, is_bot=True)})
            marketing.request.side_effect = [dict(asset_id='d' * 64, image_sha256=hashlib.sha256(image).hexdigest()),
                                             dict(remote, status='awaiting_approval'), image]
            for _ in range(3):
                stories.process(story, telegram, marketing, True, timezone.now())
                story.refresh_from_db()
            self.assertEqual(story.state, 'REVIEW')
            self.assertEqual(telegram.call.call_args.kwargs['photo'], image)
            self.assertEqual(story.snapshot, approval_snapshot(obj))
            incoming = dict(update_id=2, callback_query=dict(id='2', **{'from': dict(id=777)},
                message=dict(message_id=500, chat=dict(id=777, type='private')),
                data=f'storyapprove:{story.pk}:{story.nonce}'))
            persist_updates(config, [incoming])
            callback = TelegramHoursUpdate.objects.get(update_id=2)
            self.assertIn('registrada', stories.handle_callback(callback, config, user))
            story.refresh_from_db()
            marketing.request.side_effect = None
            marketing.request.return_value = dict(remote, status='scheduled', post_ids=['ig', 'fb'])
            stories.process(story, telegram, marketing, True, timezone.now())
            story.refresh_from_db()
            stories.process(story, telegram, marketing, True, timezone.now())
            story.refresh_from_db()
            self.assertEqual(story.state, 'SCHEDULED')
            obj.cancelled_at = timezone.now()
            obj.save(update_fields=['cancelled_at'])
            story.next_attempt_at = timezone.now()
            story.save(update_fields=['next_attempt_at'])
            marketing.request.return_value = dict(request_id=obj.pk, erp_revision_digest=story.revision,
                art_revision=1, invalidated_draft_ids=[remote['draft_id']])
            stories.advance(config, telegram, marketing)
            story.refresh_from_db()
            self.assertEqual(story.state, 'STOPPED')
            self.assertTrue(marketing.request.call_args.args[1].endswith('/invalidate'))
