from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointSalesQualityAlert, PointSyncJob
from pos_bridge.services.official_sales_backfill_service import OfficialSalesBackfillService
from pos_bridge.utils.exceptions import ExtractionError


class OfficialSalesBackfillServiceTests(SimpleTestCase):
    def test_fetch_branch_day_reports_retries_and_succeeds(self):
        branch = SimpleNamespace(external_id="1", name="MATRIZ")
        sync_job = SimpleNamespace(id=1)
        auth_session = SimpleNamespace(session=SimpleNamespace(close=lambda: None))
        sync_service = SimpleNamespace(
            settings=SimpleNamespace(retry_attempts=3),
            record_log=lambda *args, **kwargs: None,
        )
        report_service = SimpleNamespace(
            http_session_service=SimpleNamespace(create=lambda **kwargs: auth_session),
        )
        attempts = {"count": 0}

        def fake_fetch_report_with_session(**kwargs):
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise RuntimeError("Point 500")
            return SimpleNamespace(report_path="/tmp/report.xls")

        report_service.fetch_report_with_session = fake_fetch_report_with_session
        report_service.parse_report = lambda *, report_path: SimpleNamespace(rows=[{"Codigo": "0108"}])

        service = OfficialSalesBackfillService(report_service=report_service, sync_service=sync_service)

        with patch("pos_bridge.services.official_sales_backfill_service.time.sleep", return_value=None):
            parsed_reports, raw_paths = service._fetch_branch_day_reports_with_retry(
                branch=branch,
                sale_date=date(2025, 10, 1),
                credito_scopes=["null"],
                sync_job=sync_job,
                session_cache={},
            )

        self.assertEqual(attempts["count"], 2)
        self.assertEqual(len(parsed_reports), 1)
        self.assertEqual(raw_paths, ["/tmp/report.xls"])

    def test_is_no_aplica_por_apertura_skips_future_opening_branches(self):
        branch = SimpleNamespace(
            erp_branch=SimpleNamespace(esta_operativa=lambda sale_date: sale_date >= date(2026, 3, 31))
        )
        self.assertTrue(OfficialSalesBackfillService._is_no_aplica_por_apertura(branch, date(2024, 7, 1)))
        self.assertFalse(OfficialSalesBackfillService._is_no_aplica_por_apertura(branch, date(2026, 3, 31)))
        self.assertFalse(OfficialSalesBackfillService._is_no_aplica_por_apertura(SimpleNamespace(erp_branch=None), date(2024, 7, 1)))

    def test_fetch_branch_day_reports_falls_back_to_generic_session_when_branch_login_fails(self):
        branch = SimpleNamespace(external_id="13", name="Guamuchil")
        sync_job = SimpleNamespace(id=1)
        sync_service = SimpleNamespace(
            settings=SimpleNamespace(retry_attempts=1),
            record_log=lambda *args, **kwargs: None,
        )
        create_calls: list[dict] = []
        generic_auth_session = SimpleNamespace(session=SimpleNamespace(close=lambda: None))

        def fake_create(**kwargs):
            create_calls.append(kwargs)
            if kwargs.get("branch_external_id") == "13":
                raise RuntimeError("branch auth failed")
            return generic_auth_session

        report_service = SimpleNamespace(
            http_session_service=SimpleNamespace(create=fake_create),
            fetch_report_with_session=lambda **kwargs: SimpleNamespace(report_path="/tmp/report.xls"),
            parse_report=lambda *, report_path: SimpleNamespace(rows=[{"Codigo": "0108"}]),
        )

        service = OfficialSalesBackfillService(report_service=report_service, sync_service=sync_service)

        parsed_reports, raw_paths = service._fetch_branch_day_reports_with_retry(
            branch=branch,
            sale_date=date(2025, 10, 1),
            credito_scopes=["null"],
            sync_job=sync_job,
            session_cache={},
        )

        self.assertEqual(len(parsed_reports), 1)
        self.assertEqual(raw_paths, ["/tmp/report.xls"])
        self.assertEqual(
            create_calls,
            [
                {"branch_external_id": "13", "branch_display_name": "Guamuchil"},
                {"branch_external_id": None, "branch_display_name": "Guamuchil"},
            ],
        )

    def test_fetch_branch_day_reports_raises_when_point_report_catalog_omits_branch(self):
        branch = SimpleNamespace(external_id="13", name="Guamuchil")
        sync_job = SimpleNamespace(id=1)
        auth_session = SimpleNamespace(session=SimpleNamespace(close=lambda: None))
        sync_service = SimpleNamespace(
            settings=SimpleNamespace(retry_attempts=1),
            record_log=lambda *args, **kwargs: None,
        )
        report_service = SimpleNamespace(
            http_session_service=SimpleNamespace(create=lambda **kwargs: auth_session),
            list_available_branches_with_session=lambda **kwargs: [
                {"external_id": "1", "name": "MATRIZ"},
                {"external_id": "2", "name": "Crucero"},
            ],
            fetch_report_with_session=lambda **kwargs: SimpleNamespace(report_path="/tmp/report.xls"),
            parse_report=lambda *, report_path: SimpleNamespace(rows=[{"Codigo": "0108"}]),
        )

        service = OfficialSalesBackfillService(report_service=report_service, sync_service=sync_service)

        with self.assertRaises(ExtractionError):
            service._fetch_branch_day_reports_with_retry(
                branch=branch,
                sale_date=date(2026, 4, 1),
                credito_scopes=["null"],
                sync_job=sync_job,
                session_cache={},
            )


class OfficialSalesBackfillPersistenceTests(TestCase):
    def test_replace_branch_day_sales_persists_sync_job_for_auditability(self):
        branch = PointBranch.objects.create(external_id="1", name="MATRIZ")
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
            parameters={
                "source": "POINT_OFFICIAL_REPORT",
                "start_date": "2025-10-01",
                "end_date": "2025-10-31",
            },
        )
        service = OfficialSalesBackfillService()

        deleted, imported = service._replace_branch_day_sales(
            branch=branch,
            sale_date=date(2025, 10, 1),
            sync_job=sync_job,
            aggregated_rows={
                ("0108", "Pastel de 3 Pecados Mediano", "Pasteles"): {
                    "sku": "0108",
                    "name": "Pastel de 3 Pecados Mediano",
                    "category": "Pasteles",
                    "quantity": Decimal("12"),
                    "gross_amount": Decimal("1200"),
                    "discount_amount": Decimal("0"),
                    "total_amount": Decimal("1200"),
                    "tax_amount": Decimal("0"),
                    "net_amount": Decimal("1200"),
                    "scopes": {"null"},
                }
            },
        )

        self.assertEqual(deleted, 0)
        self.assertEqual(imported, 1)
        row = PointDailySale.objects.get(branch=branch, sale_date=date(2025, 10, 1))
        self.assertEqual(row.sync_job_id, sync_job.id)

    def test_fetch_branch_day_reports_raises_after_max_attempts(self):
        branch = SimpleNamespace(external_id="1", name="MATRIZ")
        sync_job = SimpleNamespace(id=1)
        auth_session = SimpleNamespace(session=SimpleNamespace(close=lambda: None))
        sync_service = SimpleNamespace(
            settings=SimpleNamespace(retry_attempts=2),
            record_log=lambda *args, **kwargs: None,
        )
        report_service = SimpleNamespace(
            http_session_service=SimpleNamespace(create=lambda **kwargs: auth_session),
            fetch_report_with_session=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("Point 500")),
            parse_report=lambda *, report_path: SimpleNamespace(rows=[]),
        )

        service = OfficialSalesBackfillService(report_service=report_service, sync_service=sync_service)

        with patch("pos_bridge.services.official_sales_backfill_service.time.sleep", return_value=None):
            with self.assertRaisesMessage(RuntimeError, "Point 500"):
                service._fetch_branch_day_reports_with_retry(
                    branch=branch,
                    sale_date=date(2025, 10, 1),
                    credito_scopes=["null"],
                    sync_job=sync_job,
                    session_cache={},
                )

    def test_fetch_branch_day_reports_reuses_cached_session(self):
        branch = SimpleNamespace(external_id="1", name="MATRIZ")
        sync_job = SimpleNamespace(id=1)
        auth_session = SimpleNamespace(session=SimpleNamespace(close=lambda: None))
        sync_service = SimpleNamespace(
            settings=SimpleNamespace(retry_attempts=1),
            record_log=lambda *args, **kwargs: None,
        )
        report_service = SimpleNamespace(
            http_session_service=SimpleNamespace(create=lambda **kwargs: (_ for _ in ()).throw(AssertionError("unexpected"))),
            fetch_report_with_session=lambda **kwargs: SimpleNamespace(report_path="/tmp/report.xls"),
            parse_report=lambda *, report_path: SimpleNamespace(rows=[{"Codigo": "0108"}]),
        )

        service = OfficialSalesBackfillService(report_service=report_service, sync_service=sync_service)

        parsed_reports, raw_paths = service._fetch_branch_day_reports_with_retry(
            branch=branch,
            sale_date=date(2025, 10, 1),
            credito_scopes=["null"],
            sync_job=sync_job,
            session_cache={branch.external_id: auth_session},
        )

        self.assertEqual(len(parsed_reports), 1)
        self.assertEqual(raw_paths, ["/tmp/report.xls"])

    def test_run_creates_quality_alert_when_branch_day_fails(self):
        sucursal = Sucursal.objects.create(codigo="GUAMUCHIL", nombre="Guamuchil", activa=True)
        PointBranch.objects.create(external_id="13", name="Guamuchil", erp_branch=sucursal)
        report_service = SimpleNamespace(
            http_session_service=SimpleNamespace(create=lambda **kwargs: SimpleNamespace(session=SimpleNamespace(close=lambda: None))),
            list_available_branches_with_session=lambda **kwargs: [],
        )
        service = OfficialSalesBackfillService(report_service=report_service)

        job = service.run(
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 1),
            branch_filter="13",
            credito_scopes=["null"],
        )

        self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
        alert = PointSalesQualityAlert.objects.get(sync_job=job)
        self.assertEqual(alert.alert_type, "OFFICIAL_BACKFILL_EXTRACTION_ERROR")
        self.assertEqual(alert.branch.external_id, "13")
        self.assertEqual(str(alert.fecha), "2026-04-01")
