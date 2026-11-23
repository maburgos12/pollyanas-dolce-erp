from datetime import timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from horarios_especiales.services.requests import create_request_from_rules, approve_request
from horarios_especiales.services.consumer_receipts import check_pair, consumer_receipts, matches


class ConsumerReceiptsTests(TestCase):
    def setUp(self):
        self.branch = Sucursal.objects.create(codigo='EL_TUNEL', nombre='Sucursal El Túnel', activa=True)
        self.user = User.objects.create_user(username='consumer_owner')
        self.day = timezone.now().astimezone(ZoneInfo('America/Mazatlan')).date()
        self.rule = {'kind':'date','start_date':self.day.isoformat(),'closed_all_day':True,'time_windows':[]}
        self.obj = self.create(self.rule)
        self.obj.execution_summary_json = {'details':[{'branch_code':'EL_TUNEL','status':'GOOGLE_CONFIRMED'}]}
        self.obj.save()
        self.expected = {'branch_code':'EL_TUNEL','branch_name':self.branch.nombre,'target_date':self.day.isoformat(),
                         'revision':self.obj.canonical_payload['revision'],'closed_all_day':True,'windows':[]}
        self.row = {**self.expected,'checked_at':timezone.now().isoformat(),'timezone':'America/Mazatlan',
                    'status':'VERIFIED','source':'ERP_APPROVED_SPECIAL_HOURS'}

    def create(self, rule):
        obj, _ = create_request_from_rules(raw_text='fixture',branch_ids=[self.branch.pk],rules=[rule],actor=self.user)
        approve_request(request_obj=obj,actor=self.user,expected_revision=obj.canonical_payload['revision'])
        return obj

    def test_success_requires_real_contract_not_http_success(self):
        store = {'target_date':self.day.isoformat(),'timezone':'America/Mazatlan','branches':[self.row]}
        maya = {**self.row,'consumer':'MAYA_BRANCH_SERVICE'}
        with patch('horarios_especiales.services.consumer_receipts.read_json',side_effect=[store,maya]):
            result = check_pair(self.expected)
        self.assertTrue(result['store']['confirmed'])
        self.assertTrue(result['maya']['confirmed'])
        with patch('horarios_especiales.services.consumer_receipts.read_json',return_value={'status':'ok'}):
            result = check_pair(self.expected)
        self.assertFalse(result['store']['confirmed'])
        self.assertFalse(result['maya']['confirmed'])

    def test_wrong_revision_date_stale_and_missing_source_fail(self):
        for changes in ({'revision':'b'*64},{'target_date':'2026-01-01'},{'source':'LOCAL'},
                        {'checked_at':(timezone.now()-timedelta(minutes=16)).isoformat()}, {'closed_all_day':False}):
            self.assertFalse(matches({**self.row,**changes},self.expected,timezone.now()))

    def test_verified_cache_avoids_calls_but_superseded_never_finalizes(self):
        result = {dest:{'confirmed':True,'checked_at':timezone.now().isoformat()} for dest in ('store','maya')}
        with patch('horarios_especiales.services.consumer_receipts.check_pair',return_value=result) as read:
            labels = consumer_receipts(self.obj)
            self.assertIn('Finalizado',labels[-1])
            consumer_receipts(self.obj)
            self.assertEqual(read.call_count,1)
            self.create({**self.rule,'closed_all_day':False,'time_windows':[{'open':'10:00','close':'18:00'}]})
            self.assertNotIn('Finalizado',' '.join(consumer_receipts(self.obj)))

    def test_future_and_long_ranges_stay_programmed(self):
        future = self.create({**self.rule,'start_date':(self.day+timedelta(days=20)).isoformat()})
        with patch('horarios_especiales.services.consumer_receipts.check_pair') as read:
            labels = consumer_receipts(future)
            self.assertTrue(all('programado' in label for label in labels))
            read.assert_not_called()

    def test_google_preflight_reads_candidate_without_approval_or_mutation(self):
        from horarios_especiales.models import SucursalPlataformaExterna
        from horarios_especiales.services.google_projection import preview_google_conflicts
        SucursalPlataformaExterna.objects.create(sucursal=self.branch,external_location_name='locations/fixture')
        self.obj.cancelled_at = timezone.now()
        self.obj.save(update_fields=['cancelled_at'])
        rule = {**self.rule,'kind':'weekly','weekdays':[self.day.weekday()]}
        obj, _ = create_request_from_rules(raw_text='draft',branch_ids=[self.branch.pk],rules=[rule],actor=self.user)
        current = {'regularHours':{'periods':[]},'specialHours':{'specialHourPeriods':[{
            'startDate':{'year':self.day.year,'month':self.day.month,'day':self.day.day},'closed':True}]}}
        with patch('integraciones.services.google_business_profile.client.GoogleBusinessProfileClient') as client:
            client.return_value.get_location.return_value = current
            with self.assertRaisesRegex(ValueError,'excepciones manuales'):
                preview_google_conflicts(obj)
            client.return_value.patch_hours.assert_not_called()
            client.return_value.patch_special_hours.assert_not_called()
        obj.refresh_from_db()
        self.assertIsNone(obj.approved_at)
        self.assertEqual(obj.status,'BORRADOR')
        with patch('integraciones.services.google_business_profile.client.GoogleBusinessProfileClient',side_effect=RuntimeError('outage')):
            self.assertIn('pendiente',preview_google_conflicts(obj))


    def test_one_destination_failure_keeps_other_receipts_and_rotates_past_first_four(self):
        branches = [self.branch] + [Sucursal.objects.create(codigo=f'QA{i}', nombre=f'QA {i}', activa=True) for i in range(5)]
        obj, _ = create_request_from_rules(raw_text='six branches', branch_ids=[b.pk for b in branches],
                                          rules=[self.rule], actor=self.user)
        approve_request(request_obj=obj, actor=self.user, expected_revision=obj.canonical_payload['revision'])
        codes = [location['branch_code'] for location in obj.canonical_payload['locations']]
        failing_maya = set(codes[:4])
        attempts = []
        first_checked = timezone.now()

        def read(expected):
            code = expected['branch_code']
            first_attempt = code not in attempts
            attempts.append(code)
            return {'store': {'confirmed': first_attempt, 'checked_at': first_checked.isoformat()},
                    'maya': {'confirmed': code not in failing_maya, 'checked_at': first_checked.isoformat()}}

        with patch('horarios_especiales.services.consumer_receipts.check_pair', side_effect=read), \
                patch('horarios_especiales.services.consumer_receipts.timezone.now', return_value=first_checked):
            consumer_receipts(obj)
        self.assertEqual(set(attempts), failing_maya)
        with patch('horarios_especiales.services.consumer_receipts.check_pair', side_effect=read), \
                patch('horarios_especiales.services.consumer_receipts.timezone.now', return_value=first_checked + timedelta(seconds=31)):
            labels = consumer_receipts(obj)
        self.assertEqual(set(attempts), set(codes))
        self.assertIn('confirmado por lectura', labels[0])
        self.assertIn('pendiente', labels[1])
        self.assertNotIn('Finalizado', ' '.join(labels))
        saved = obj.execution_summary_json['consumer_readback']['pairs']
        self.assertEqual(len(saved), 6)
        self.assertTrue(all(pair['store']['confirmed'] for pair in saved.values()))
        self.assertEqual(sum(pair.get('maya', {}).get('confirmed', False) for pair in saved.values()), 2)

    def test_indefinite_weekly_never_finishes_from_seven_day_readback(self):
        self.obj.cancelled_at = timezone.now()
        self.obj.save(update_fields=['cancelled_at'])
        obj = self.create({**self.rule, 'kind':'weekly', 'weekdays':[self.day.weekday()]})
        obj.execution_summary_json = {'details':[{'branch_code':'EL_TUNEL','status':'GOOGLE_CONFIRMED'}]}
        obj.save()
        result = {dest:{'confirmed':True,'checked_at':timezone.now().isoformat()} for dest in ('store','maya')}
        with patch('horarios_especiales.services.consumer_receipts.check_pair',return_value=result):
            labels = consumer_receipts(obj)
        self.assertTrue(all('programado' in label for label in labels))
        self.assertNotIn('Finalizado',' '.join(labels))
