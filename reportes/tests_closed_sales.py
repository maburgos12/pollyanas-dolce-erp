from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailyBranchIndicator, PointSyncJob
from reportes.dashboard_sales_dataset import get_dashboard_sales_dataset
from reportes.models import CorteOficialDiario


class ClosedSalesTests(TestCase):
    today = date(2026, 9, 7)

    def setUp(self):
        self.branches = []
        for code in ('MATRIZ', 'COLOSIO'):
            branch = Sucursal.objects.create(codigo=code, nombre=code, activa=True)
            self.branches.append(PointBranch.objects.create(
                external_id=str(branch.id), name=code, erp_branch=branch,
                status=PointBranch.STATUS_ACTIVE,
            ))

    def close_day(self, day, branches=None, status=PointSyncJob.STATUS_SUCCESS, started_day=None):
        start = timezone.make_aware(datetime.combine(started_day or day + timedelta(days=1), datetime.min.time()))
        job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_SALES,
            status=status, started_at=start, finished_at=start + timedelta(minutes=5))
        for branch in branches if branches is not None else self.branches:
            PointDailyBranchIndicator.objects.create(branch=branch, sync_job=job,
                indicator_date=day, total_amount=Decimal('100'), total_tickets=1)
        return job

    def cutoff(self):
        from reportes.closed_sales import latest_closed_sales_date
        return latest_closed_sales_date(today=self.today)

    def test_today_never_advances_closed_cutoff(self):
        self.close_day(date(2026, 9, 6))
        self.close_day(self.today)
        self.assertEqual(self.cutoff(), date(2026, 9, 6))

    def test_missing_branch_retains_previous_complete_day(self):
        self.close_day(date(2026, 9, 5))
        self.close_day(date(2026, 9, 6), self.branches[:1])
        self.assertEqual(self.cutoff(), date(2026, 9, 5))

    def test_intraday_capture_is_not_a_closure_next_morning(self):
        self.close_day(date(2026, 9, 5))
        self.close_day(date(2026, 9, 6), started_day=date(2026, 9, 6))
        self.assertEqual(self.cutoff(), date(2026, 9, 5))

    def test_failed_sync_does_not_publish_a_partial_day(self):
        self.close_day(date(2026, 9, 6), status=PointSyncJob.STATUS_FAILED)
        self.assertIsNone(self.cutoff())

    def test_confirmed_zero_is_present_not_missing(self):
        self.close_day(date(2026, 9, 6))
        PointDailyBranchIndicator.objects.update(total_amount=0, total_tickets=0)
        self.assertEqual(self.cutoff(), date(2026, 9, 6))

    def test_new_branch_without_point_mapping_blocks_closure(self):
        self.close_day(date(2026, 9, 6))
        Sucursal.objects.create(codigo='PAYAN', nombre='Payan', activa=True)
        self.assertIsNone(self.cutoff())

    def test_daily_snapshot_cannot_use_today_official_cut_or_canonical_fallback(self):
        self.close_day(date(2026, 9, 6))
        CorteOficialDiario.objects.create(corte_date=self.today, total_amount=9999)
        dataset = get_dashboard_sales_dataset(today=self.today, months=1)
        self.assertEqual(dataset['daily_sales_snapshot']['date'], date(2026, 9, 6))

    def test_no_closed_day_is_unavailable_not_zero_sales(self):
        dataset = get_dashboard_sales_dataset(today=self.today, months=1)
        self.assertIsNone(dataset['daily_sales_snapshot']['date'])
        self.assertIsNone(dataset['daily_sales_snapshot']['total_amount'])

    def yoy(self, day, days):
        from reportes.closed_sales import closed_month_comparison
        with patch('reportes.closed_sales.get_daily_sales_bulk', return_value={'dates': days}):
            return closed_month_comparison(cutoff=day)

    def day_payload(self, amount, branches=None):
        return {'coverage_accepted': True, 'rows': [
            {'branch_id': b.erp_branch_id, 'amount': Decimal(amount), 'units': 1}
            for b in (branches if branches is not None else self.branches)
        ], 'indicator_map': {}}

    def test_comparison_preserves_negative_sign_and_exact_dates(self):
        row = self.yoy(date(2026, 9, 1), {
            '2026-09-01': self.day_payload('90'),
            '2025-09-01': self.day_payload('100'),
        })
        self.assertEqual(row['amount'], Decimal('180'))
        self.assertEqual(row['prev_amount'], Decimal('200'))
        self.assertEqual(row['amount_delta_pct'], Decimal('-10'))
        self.assertEqual(row['comparison_tone'], 'danger')
        self.assertIn('1–1 sep 2026 vs 1–1 sep 2025', row['comparison_note'])

    def test_missing_previous_branch_is_pending_not_positive_or_zero(self):
        row = self.yoy(date(2026, 9, 1), {
            '2026-09-01': self.day_payload('90'),
            '2025-09-01': self.day_payload('100', self.branches[:1]),
        })
        self.assertIsNone(row['prev_amount'])
        self.assertIsNone(row['amount_delta_pct'])
        self.assertEqual(row['comparison_label'], 'Comparativo pendiente')
        self.assertIn('COLOSIO', row['coverage_note'])

    def test_missing_middle_day_is_not_a_complete_month(self):
        row = self.yoy(date(2026, 9, 3), {
            f'{year}-09-{day:02d}': self.day_payload('100')
            for year in (2025, 2026) for day in (1, 3)
        })
        self.assertIsNone(row['amount_delta_pct'])
        self.assertIn('2025-09-02', row['coverage_note'])

    def test_unclosed_zero_indicator_does_not_fill_historical_gap(self):
        previous = date(2025, 9, 1)
        self.close_day(previous, started_day=previous)
        PointDailyBranchIndicator.objects.update(total_amount=0)
        row = self.yoy(date(2026, 9, 1), {
            '2026-09-01': self.day_payload('90'),
            '2025-09-01': {'rows': [], 'indicator_map': {
                b.erp_branch_id: {'amount': 0} for b in self.branches}},
        })
        self.assertIsNone(row['prev_amount'])
        self.assertIsNone(row['amount_delta_pct'])

    def test_closed_zero_indicator_is_valid_but_has_no_percentage_base(self):
        self.close_day(date(2025, 9, 1))
        PointDailyBranchIndicator.objects.update(total_amount=0)
        row = self.yoy(date(2026, 9, 1), {'2026-09-01': self.day_payload('90')})
        self.assertEqual(row['prev_amount'], Decimal('0'))
        self.assertIsNone(row['amount_delta_pct'])
        self.assertIn('venta cero', row['coverage_note'])

    def test_leap_day_uses_equal_days_in_both_years(self):
        from reportes.closed_sales import closed_month_comparison
        with patch('reportes.closed_sales.get_daily_sales_bulk', return_value={'dates': {}}) as bulk:
            row = closed_month_comparison(cutoff=date(2024, 2, 29))
        self.assertEqual(row['period_end'], date(2024, 2, 28))
        self.assertEqual(row['prev_period_end'], date(2023, 2, 28))
        self.assertNotIn(date(2024, 2, 29), bulk.call_args.kwargs['fechas'])

    def test_new_month_uses_last_closed_month(self):
        self.close_day(date(2026, 8, 31))
        from reportes.closed_sales import latest_closed_sales_date
        self.assertEqual(latest_closed_sales_date(today=date(2026, 9, 1)), date(2026, 8, 31))

    def test_materialized_dashboard_does_not_reuse_an_old_sales_cutoff(self):
        from reportes.dashboard_full_dataset import _hydrate_dashboard_full_payload
        from reportes.dashboard_sales_dataset import get_dashboard_sales_dataset
        self.close_day(date(2026, 9, 6))
        with patch('reportes.dashboard_sales_dataset.timezone.localdate', return_value=self.today):
            dataset = get_dashboard_sales_dataset(today=self.today, months=1)
            with patch('reportes.dashboard_full_dataset.get_dashboard_sales_dataset', return_value=dataset):
                payload = _hydrate_dashboard_full_payload({
                    'months_window': 6,
                    'daily_sales_snapshot': {'date': '2026-09-07'},
                    'yoy_panel': {'hero_row': {'amount_delta_pct': 7.2}},
                })
        self.assertEqual(payload['daily_sales_snapshot']['date'], date(2026, 9, 6))
        self.assertEqual(payload['yoy_panel']['cutoff_date'], date(2026, 9, 6))
        self.assertIsNone(payload['yoy_panel']['hero_row']['amount_delta_pct'])

    def test_comparison_reads_real_persisted_sales_and_excludes_today(self):
        from reportes.models import FactVentaDiaria
        from reportes.executive_panels import build_closed_yoy_panel
        for year, amount in [(2025, '100'), (2026, '90')]:
            for branch in self.branches:
                FactVentaDiaria.objects.create(fecha=date(year, 9, 1), sucursal=branch.erp_branch,
                    producto_clave='TEST', cantidad=1, venta_total=Decimal(amount),
                    source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE)
        FactVentaDiaria.objects.create(fecha=self.today, sucursal=self.branches[0].erp_branch,
            producto_clave='TODAY', cantidad=1, venta_total=9999,
            source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE)
        panel = build_closed_yoy_panel(cutoff=date(2026, 9, 1), months=1)
        self.assertEqual(panel['hero_row']['amount_delta_pct'], Decimal('-10'))
        self.assertEqual(panel['hero_row']['amount'], Decimal('180'))
        self.assertEqual(panel['rows'][-1], panel['hero_row'])

    @patch('reportes.views.task_visible_cut_refresh_cycle.delay')
    def test_refresh_cannot_request_today(self, queue):
        from django.contrib.auth import get_user_model
        from django.core.cache import cache
        from django.urls import reverse
        cache.clear()
        user = get_user_model().objects.create_user(username='closed-sales-admin', is_superuser=True)
        self.client.force_login(user)
        with patch('reportes.views.timezone.localdate', return_value=self.today):
            self.client.post(reverse('reportes:bi_force_refresh'),
                {'refresh_scope': 'cutoff', 'reference_date': self.today.isoformat()})
        self.assertEqual(queue.call_args.kwargs['reference_date_iso'], '2026-09-06')

    def test_sales_page_uses_same_closed_comparison_as_dashboard(self):
        from django.contrib.auth import get_user_model
        from django.urls import reverse
        from reportes.models import FactVentaDiaria, AnalyticRefreshWindow
        self.close_day(date(2026, 9, 1))
        for year, amount in [(2025, 100), (2026, 90)]:
            for branch in self.branches:
                FactVentaDiaria.objects.create(fecha=date(year, 9, 1), sucursal=branch.erp_branch,
                    producto_clave='TEST', cantidad=1, venta_total=amount,
                    source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE)
        AnalyticRefreshWindow.objects.filter(dataset=AnalyticRefreshWindow.DATASET_SALES).update(
            status=AnalyticRefreshWindow.STATUS_DONE)
        user = get_user_model().objects.create_user(username='closed-view-admin', is_superuser=True)
        self.client.force_login(user)
        with patch('reportes.views.timezone.localdate', return_value=self.today):
            response = self.client.get(reverse('reportes:ventas'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['yoy_panel']['cutoff_date'], date(2026, 9, 1))
        self.assertContains(response, '1–1 sep 2026 vs 1–1 sep 2025')
        self.assertContains(response, '-10.0%')
