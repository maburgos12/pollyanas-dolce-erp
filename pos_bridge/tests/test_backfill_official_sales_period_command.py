from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from django.core.management import CommandError, call_command
from django.test import SimpleTestCase


class BackfillOfficialSalesPeriodCommandTests(SimpleTestCase):
    def _service_payload(self):
        report_result = Mock(report_path="/tmp/point-report.xlsx", request_url="https://point.local/report")
        parsed = Mock(
            rows=[{"Cantidad": "2"}],
            summary={
                "bruto": "120.00",
                "descuentos": "0.00",
                "venta": "120.00",
                "impuestos": "0.00",
                "venta_neta": "120.00",
            },
        )
        return report_result, parsed

    def test_writes_file_cache_when_monthly_row_is_missing(self):
        report_result, parsed = self._service_payload()

        with TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "official_partial_sales_cache.json"
            with patch(
                "pos_bridge.management.commands.backfill_official_sales_period.Command.FILE_CACHE_PATH",
                cache_path,
            ), patch(
                "pos_bridge.management.commands.backfill_official_sales_period.PointSalesCategoryReportService"
            ) as service_cls, patch(
                "pos_bridge.management.commands.backfill_official_sales_period.PointMonthlySalesOfficial.objects.filter"
            ) as filter_mock:
                service = service_cls.return_value
                service.fetch_report.return_value = report_result
                service.parse_report.return_value = parsed
                filter_mock.return_value.first.return_value = None

                call_command(
                    "backfill_official_sales_period",
                    start_date="2026-03-01",
                    end_date="2026-03-03",
                )

            self.assertTrue(cache_path.exists())
            self.assertIn("2026-03-01_2026-03-03", cache_path.read_text(encoding="utf-8"))

    def test_raises_when_db_persistence_fails(self):
        report_result, parsed = self._service_payload()
        monthly_row = Mock(raw_payload={}, save=Mock(side_effect=RuntimeError("db write failed")))

        with TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "official_partial_sales_cache.json"
            with patch(
                "pos_bridge.management.commands.backfill_official_sales_period.Command.FILE_CACHE_PATH",
                cache_path,
            ), patch(
                "pos_bridge.management.commands.backfill_official_sales_period.PointSalesCategoryReportService"
            ) as service_cls, patch(
                "pos_bridge.management.commands.backfill_official_sales_period.PointMonthlySalesOfficial.objects.filter"
            ) as filter_mock:
                service = service_cls.return_value
                service.fetch_report.return_value = report_result
                service.parse_report.return_value = parsed
                filter_mock.return_value.first.return_value = monthly_row

                with self.assertRaisesMessage(
                    CommandError,
                    "Falló la persistencia del cache mensual oficial en PostgreSQL.",
                ):
                    call_command(
                        "backfill_official_sales_period",
                        start_date="2026-03-01",
                        end_date="2026-03-03",
                    )

            self.assertTrue(cache_path.exists())
