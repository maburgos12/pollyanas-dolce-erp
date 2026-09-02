from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointProduct, PointSalesQualityAlert, PointSyncJob
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
    @staticmethod
    def _aggregated_row(*, sku="NEW-1", quantity=Decimal("12")):
        return {
            (sku, "Producto nuevo", "Pasteles"): {
                "sku": sku,
                "name": "Producto nuevo",
                "category": "Pasteles",
                "quantity": quantity,
                "gross_amount": Decimal("1200"),
                "discount_amount": Decimal("0"),
                "total_amount": Decimal("1200"),
                "tax_amount": Decimal("0"),
                "net_amount": Decimal("1200"),
                "scopes": {"null"},
            }
        }

    @staticmethod
    def _existing_sale(*, branch, sale_date):
        product = PointProduct.objects.create(
            external_id=f"old-{branch.external_id}-{sale_date}",
            sku="OLD-1",
            name="Producto anterior",
        )
        return PointDailySale.objects.create(
            branch=branch,
            product=product,
            sale_date=sale_date,
            quantity=Decimal("7"),
            total_amount=Decimal("700"),
            source_endpoint="old-source",
        )

    def test_run_fails_when_canonical_sales_branch_catalog_is_empty(self):
        service = OfficialSalesBackfillService()
        service.repair_service = SimpleNamespace(
            repair=lambda **kwargs: SimpleNamespace(
                bridge_history_deleted=0,
                bridge_history_created=0,
                recipe_rows_updated=0,
                recipe_rows_cleared=0,
                unresolved_rows=0,
                non_recipe_rows=0,
            )
        )

        with patch.object(service, "_sales_branches", return_value=[]):
            job = service.run(
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 31),
            )

        self.assertEqual(job.status, PointSyncJob.STATUS_FAILED)
        self.assertIn("catálogo", job.error_message.lower())

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

    def test_replace_branch_day_sales_locks_branch_before_resolution_and_orders_products(self):
        branch = PointBranch.objects.create(external_id="serialized", name="MATRIZ")
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
        )
        service = OfficialSalesBackfillService()
        resolved: list[str] = []
        original_lock = PointBranch.objects.select_for_update

        def resolve_after_lock(*, sku, name, category):
            self.assertTrue(lock_mock.called)
            resolved.append(sku)
            return PointProduct.objects.create(
                external_id=f"serialized-{sku}", sku=sku, name=name, category=category
            )

        rows = {
            ("Z", "Producto Z", "Cat"): {**self._aggregated_row().popitem()[1], "sku": "Z", "name": "Producto Z"},
            ("A", "Producto A", "Cat"): {**self._aggregated_row().popitem()[1], "sku": "A", "name": "Producto A"},
        }
        with (
            patch.object(PointBranch.objects, "select_for_update", wraps=original_lock) as lock_mock,
            patch.object(service, "_resolve_product", side_effect=resolve_after_lock),
        ):
            service._replace_branch_day_sales(
                branch=branch,
                sale_date=date(2025, 10, 6),
                sync_job=sync_job,
                aggregated_rows=rows,
            )

        self.assertEqual(resolved, ["A", "Z"])
        lock_mock.assert_called_once_with()

    def test_replace_branch_day_sales_rolls_back_delete_when_resolution_fails(self):
        branch = PointBranch.objects.create(external_id="rollback-delete", name="MATRIZ")
        sale_date = date(2025, 10, 2)
        old_row = self._existing_sale(branch=branch, sale_date=sale_date)
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
        )
        service = OfficialSalesBackfillService()

        with patch.object(service, "_resolve_product", side_effect=RuntimeError("resolution failed")):
            with self.assertRaisesMessage(RuntimeError, "resolution failed"):
                service._replace_branch_day_sales(
                    branch=branch,
                    sale_date=sale_date,
                    sync_job=sync_job,
                    aggregated_rows=self._aggregated_row(),
                )

        preserved = PointDailySale.objects.get(branch=branch, sale_date=sale_date)
        self.assertEqual(preserved.pk, old_row.pk)
        self.assertEqual(preserved.quantity, Decimal("7"))

    def test_replace_branch_day_sales_rolls_back_new_rows_when_post_create_step_fails(self):
        branch = PointBranch.objects.create(external_id="rollback-create", name="MATRIZ")
        sale_date = date(2025, 10, 3)
        old_row = self._existing_sale(branch=branch, sale_date=sale_date)
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
        )
        service = OfficialSalesBackfillService()

        with patch(
            "pos_bridge.services.official_sales_backfill_service.mark_analytics_dirty_for_range",
            side_effect=RuntimeError("dirty marker failed"),
        ):
            with self.assertRaisesMessage(RuntimeError, "dirty marker failed"):
                service._replace_branch_day_sales(
                    branch=branch,
                    sale_date=sale_date,
                    sync_job=sync_job,
                    aggregated_rows=self._aggregated_row(),
                )

        rows = list(PointDailySale.objects.filter(branch=branch, sale_date=sale_date))
        self.assertEqual([row.pk for row in rows], [old_row.pk])
        self.assertEqual(rows[0].quantity, Decimal("7"))

    def test_replace_branch_day_sales_invalidates_cache_only_after_commit(self):
        branch = PointBranch.objects.create(external_id="cache-on-commit", name="MATRIZ")
        sale_date = date(2025, 10, 4)
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
        )
        service = OfficialSalesBackfillService()

        with patch("pos_bridge.services.official_sales_backfill_service.bump_cache_scopes") as bump_cache:
            with self.captureOnCommitCallbacks(execute=True) as callbacks:
                service._replace_branch_day_sales(
                    branch=branch,
                    sale_date=sale_date,
                    sync_job=sync_job,
                    aggregated_rows=self._aggregated_row(),
                )
                self.assertEqual(bump_cache.call_count, 0)

            bump_cache.assert_called_once_with("ventas", "dashboard")

    def test_replace_branch_day_sales_invalidates_zero_result_after_commit(self):
        branch = PointBranch.objects.create(external_id="cache-zero", name="MATRIZ")
        sale_date = date(2025, 10, 5)
        self._existing_sale(branch=branch, sale_date=sale_date)
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_RUNNING,
        )
        service = OfficialSalesBackfillService()

        with (
            patch("pos_bridge.services.official_sales_backfill_service.bump_cache_scopes") as bump_cache,
            patch(
                "pos_bridge.services.official_sales_backfill_service.mark_analytics_dirty_for_range"
            ) as mark_dirty,
        ):
            with self.captureOnCommitCallbacks(execute=True) as callbacks:
                deleted, imported = service._replace_branch_day_sales(
                    branch=branch,
                    sale_date=sale_date,
                    sync_job=sync_job,
                    aggregated_rows={},
                )
                self.assertEqual(bump_cache.call_count, 0)

            self.assertEqual((deleted, imported), (1, 0))
            self.assertFalse(PointDailySale.objects.filter(branch=branch, sale_date=sale_date).exists())
            bump_cache.assert_called_once_with("ventas", "dashboard")
            mark_dirty.assert_called_once_with(
                start_date=sale_date,
                end_date=sale_date,
                include_sales=True,
                include_production=True,
                include_forecast=True,
                reason="official_sales_backfill_service",
            )

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
