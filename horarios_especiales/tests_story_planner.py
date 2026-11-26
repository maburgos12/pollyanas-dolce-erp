import hashlib
import hmac
from unittest.mock import patch

from django.test import SimpleTestCase

from horarios_especiales.services.story_planner import MarketingClient, MarketingError


class MarketingTransportTests(SimpleTestCase):
    @patch('horarios_especiales.services.story_planner.requests.Session')
    @patch('horarios_especiales.services.story_planner.time.time', return_value=1760000000)
    def test_approval_signs_exact_persisted_body_without_frontend_prefix(self, _clock, session):
        response = session.return_value.__enter__.return_value.request.return_value.__enter__.return_value
        response.status_code = 201
        response.iter_content.return_value = [b'{"status":"scheduled"}']
        client = MarketingClient('fictional-shared-secret')
        draft = f"hours:11:{'a' * 64}:1"
        path = f'/social-scheduler/hours-story-drafts/{draft}/approve'
        body = b'{"nonce":"abc","request_id":11}'
        self.assertEqual(client.request('POST', path, body), {'status': 'scheduled'})
        method, url = session.return_value.__enter__.return_value.request.call_args.args
        kwargs = session.return_value.__enter__.return_value.request.call_args.kwargs
        self.assertEqual((method, url), ('POST', 'https://ads.pollyanasdolce.com/api' + path))
        self.assertEqual(kwargs['data'], body)
        self.assertFalse(kwargs['allow_redirects'])
        signature = hmac.new(b'fictional-shared-secret',
                             b'1760000000\nPOST\n' + path.encode() + b'\n' + body, hashlib.sha256).hexdigest()
        self.assertEqual(kwargs['headers']['X-Hours-Signature'], signature)

    @patch('horarios_especiales.services.story_planner.requests.Session')
    def test_retries_only_known_conflict_and_never_follows_redirects(self, session):
        response = session.return_value.__enter__.return_value.request.return_value.__enter__.return_value
        response.status_code = 409
        response.iter_content.return_value = [b'{"detail":{"code":"previous_story_retirement_pending"}}']
        client = MarketingClient('fictional-shared-secret')
        with self.assertRaises(MarketingError) as caught:
            client.request('POST', '/social-scheduler/hours-story-drafts/invalidate', b'{}')
        self.assertTrue(caught.exception.retryable)
        response.status_code = 302
        with self.assertRaises(MarketingError) as caught:
            client.request('GET', '/social-scheduler/hours-story-drafts/assets/' + ('a' * 64) + '/preview', image=True)
        self.assertFalse(caught.exception.retryable)
