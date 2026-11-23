from datetime import timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone
from rest_framework.test import APIClient

from core.models import Sucursal
from horarios_especiales.services.requests import create_request_from_rules, approve_request


class PublicBranchHoursTests(TestCase):
    def setUp(self):
        self.branch = Sucursal.objects.create(codigo="EL_TUNEL", nombre="Sucursal El Túnel", activa=True)
        self.user = User.objects.create_user(username="hours_owner")
        self.day = timezone.now().astimezone(ZoneInfo("America/Mazatlan")).date()
        self.client = APIClient()

    def read(self, **kwargs):
        return self.client.get('/api/public/branch-hours/', {'branch_codes': 'EL_TUNEL', 'target_date': self.day.isoformat(), **kwargs})

    @patch('horarios_especiales.services.effective_hours._read_regular', side_effect=AssertionError('No recursive HTTP'))
    def test_public_projection_is_approved_only_sanitized_and_does_not_fetch_website(self, read_regular):
        rule = {'kind': 'weekly', 'start_date': self.day.isoformat(), 'weekdays': [self.day.weekday()],
                'closed_all_day': True, 'time_windows': []}
        obj, _ = create_request_from_rules(raw_text='private command', branch_ids=[self.branch.pk], rules=[rule], actor=self.user, reason='private reason')
        self.assertEqual(self.read().json()['branches'][0]['status'], 'NO_OVERRIDE')
        approve_request(request_obj=obj, actor=self.user, expected_revision=obj.canonical_payload['revision'])
        response = self.read()
        self.assertEqual(response.status_code, 200)
        row = response.json()['branches'][0]
        self.assertEqual(row['revision'], obj.canonical_payload['revision'])
        self.assertEqual(row['status'], 'VERIFIED')
        self.assertTrue(row['closed_all_day'])
        self.assertEqual(set(row), {'branch_code','branch_name','target_date','timezone','checked_at','status','source','revision','closed_all_day','windows'})
        for private in ('private command', 'private reason', 'hours_owner', 'request_id', 'branch_id', 'approved_at'):
            self.assertNotIn(private, response.content.decode())
        self.assertEqual(response['Cache-Control'], 'no-store')
        read_regular.assert_not_called()

    def test_bounded_dates_exact_codes_and_get_only(self):
        for kwargs in ({'target_date': '2026-02-30'}, {'target_date': '20261122'},
                       {'target_date': (self.day + timedelta(days=367)).isoformat()},
                       {'branch_codes': 'EL'}, {'branch_codes': 'EL_TUNEL,EL_TUNEL'}, {'branch_codes': ''}):
            self.assertEqual(self.read(**kwargs).status_code, 400)
        self.assertEqual(self.client.post('/api/public/branch-hours/', {}).status_code, 405)
