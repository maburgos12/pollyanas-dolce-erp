from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
from django.test import SimpleTestCase, override_settings

from pos_bridge.services.sales_category_report_service import PointSalesCategoryReportService


class PointSalesCategoryReportServiceTests(SimpleTestCase):
    @override_settings(USE_TZ=True, TIME_ZONE="America/Phoenix")
    def test_build_params_uses_expected_report_contract(self):
        service = PointSalesCategoryReportService()

        params = service._build_params(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 20),
            branch_external_id=None,
            branch_display_name=None,
            credito="false",
        )

        self.assertEqual(params["ext"], "Excel")
        self.assertEqual(params["idreporte"], "3")
        self.assertEqual(params["idtipo"], "0")
        self.assertEqual(params["sucursal"], "null")
        self.assertEqual(params["credito"], "false")
        self.assertEqual(params["nomSucursal"], "Todas las sucursales")
        self.assertTrue(params["fi"].isdigit())
        self.assertTrue(params["ff"].isdigit())

    def test_extract_detail_rows_supports_category_header_and_sparse_rows(self):
        service = PointSalesCategoryReportService()
        dataframe = pd.DataFrame(
            [
                [],
                ["VENTA POR CATEGORÍA"],
                ["CATEGORÍA", "CÓDIGO", "PRODUCTO", "CANTIDAD", "BRUTO", "DESCUENTOS", "VENTA", "IMPUESTOS", "VENTA NETA"],
                ["Alegría", "0170", "Pirotecnia Alegría Ch", 739, 18475, 0, 18475, 2548.27, 15926.72],
                ["CND000980", "VELA ROSA", 13, 754, 0, 754, 104, 650],
                ["Total de la categoría"],
            ]
        )

        rows = service._extract_detail_rows_from_dataframe(dataframe)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["Categoria"], "Alegría")
        self.assertEqual(rows[0]["Codigo"], "0170")
        self.assertEqual(rows[1]["Categoria"], "Alegría")
        self.assertEqual(rows[1]["Codigo"], "CND000980")

    def test_fetch_report_closes_created_session_on_success(self):
        service = PointSalesCategoryReportService()
        fake_session = SimpleNamespace(session=SimpleNamespace(close=Mock()))
        service.http_session_service = SimpleNamespace(create=Mock(return_value=fake_session))
        service.fetch_report_with_session = Mock(return_value="ok")

        result = service.fetch_report(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 1),
            branch_external_id="1",
            branch_display_name="Matriz",
            credito=None,
        )

        self.assertEqual(result, "ok")
        fake_session.session.close.assert_called_once()

    def test_fetch_report_closes_created_session_on_failure(self):
        service = PointSalesCategoryReportService()
        fake_session = SimpleNamespace(session=SimpleNamespace(close=Mock()))
        service.http_session_service = SimpleNamespace(create=Mock(return_value=fake_session))
        service.fetch_report_with_session = Mock(side_effect=RuntimeError("Point 500"))

        with self.assertRaises(RuntimeError):
            service.fetch_report(
                start_date=date(2026, 3, 1),
                end_date=date(2026, 3, 1),
                branch_external_id="1",
                branch_display_name="Matriz",
                credito=None,
            )

        fake_session.session.close.assert_called_once()

    def test_list_available_branches_with_session_normalizes_point_catalog(self):
        service = PointSalesCategoryReportService()
        fake_response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: [
                {
                    "PK_Sucursal": 13,
                    "Sucursal": "Guamuchil",
                    "Sucursal_Corto": "Guamuchil",
                    "FK_Plaza": 1,
                }
            ],
        )
        fake_session = SimpleNamespace(session=SimpleNamespace(get=Mock(return_value=fake_response)))

        branches = service.list_available_branches_with_session(auth_session=fake_session)

        self.assertEqual(
            branches,
            [
                {
                    "external_id": "13",
                    "name": "Guamuchil",
                    "short_name": "Guamuchil",
                    "plaza_id": 1,
                }
            ],
        )
