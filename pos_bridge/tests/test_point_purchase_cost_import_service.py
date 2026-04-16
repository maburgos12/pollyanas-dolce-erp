from __future__ import annotations

import json
import tempfile
from pathlib import Path

from django.test import TestCase

from maestros.models import CostoInsumo, Insumo
from pos_bridge.services.point_purchase_cost_import_service import PointPurchaseCostImportService


class PointPurchaseCostImportServiceTests(TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.summary_path = Path(self.temp_dir.name) / "summary.json"
        self.details_path = Path(self.temp_dir.name) / "details.json"
        self.service = PointPurchaseCostImportService()

    def test_import_creates_historical_purchase_costs_and_resolves_alias(self):
        exact = Insumo.objects.create(nombre="AL-1310", codigo_point="AL-1310", activo=True)
        alias = Insumo.objects.create(nombre="Colorante Rojo", codigo_point="085", activo=True)

        self.summary_path.write_text(
            json.dumps(
                {
                    "js_results": [
                        {
                            "rows": [
                                {
                                    "compra_id": "1001",
                                    "cells": [
                                        "FOL-1",
                                        "Almacen",
                                        "Proveedor Demo",
                                        "20260115 14:00:0015/Jan/2026",
                                    ],
                                }
                            ]
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.details_path.write_text(
            json.dumps(
                {
                    "js_results": [
                        [
                            {
                                "purchase_id": "1001",
                                "matches": [
                                    {
                                        "articulo": "AL-1310",
                                        "cantidad": 300,
                                        "unidad": "PZA",
                                        "costo_unitario": 4.58,
                                        "costo_total": 1374.0,
                                        "raw": {},
                                    },
                                    {
                                        "articulo": "COLORANTE ROJO REDVELVET",
                                        "cantidad": 3.72,
                                        "unidad": "Litro",
                                        "costo_unitario": 506.89,
                                        "costo_total": 1885.61,
                                        "raw": {},
                                    },
                                ],
                            }
                        ]
                    ]
                }
            ),
            encoding="utf-8",
        )

        result = self.service.import_from_browser_exports(
            summary_path=self.summary_path,
            details_path=self.details_path,
        )

        self.assertEqual(result.created, 2)
        self.assertEqual(result.unresolved, 0)
        self.assertTrue(CostoInsumo.objects.filter(insumo=exact, proveedor__nombre="Proveedor Demo").exists())
        self.assertTrue(CostoInsumo.objects.filter(insumo=alias, proveedor__nombre="Proveedor Demo").exists())
