from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from maestros.models import CostoInsumo, Insumo, UnidadMedida
from recetas.models import LineaReceta, Receta
from reportes.management.commands.audit_point_cost_integrity import classify_point_cost
from reportes.models import InsumoCostoHistoricoMensual, RecetaCostoHistoricoMensual


class PointCostIntegrityAuditCommandTests(TestCase):
    def setUp(self):
        self.unit_kg = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.unit_pza = UnidadMedida.objects.create(
            codigo="pza",
            nombre="Pieza",
            tipo=UnidadMedida.TIPO_PIEZA,
            factor_to_base=Decimal("1"),
        )
        self.crunch = Insumo.objects.create(
            nombre="Crunch Rocks",
            nombre_point="CHOCOLATE CRUNCH ROCKS",
            codigo_point="50161813",
            unidad_base=self.unit_kg,
            activo=True,
        )
        self.huevo = Insumo.objects.create(
            nombre="HUEVO",
            nombre_point="HUEVO",
            codigo_point="007",
            unidad_base=self.unit_pza,
            activo=True,
        )
        self.bad_crunch_cost = CostoInsumo.objects.create(
            insumo=self.crunch,
            fecha=date(2026, 3, 5),
            costo_unitario=Decimal("2.920000"),
            source_hash="bad-crunch-point",
            raw={
                "source": "POINT_EXISTENCIA_ALMACEN",
                "point_code": "50161813",
                "point_name": "HERSHEYS MINIATURA",
                "quantity": "0",
                "unit": "PZA",
                "unit_cost": "2.92",
                "total_cost": "0",
            },
        )
        self.safe_crunch_cost = CostoInsumo.objects.create(
            insumo=self.crunch,
            fecha=date(2026, 4, 20),
            costo_unitario=Decimal("319.830000"),
            source_hash="safe-crunch-point",
            raw={
                "source": "POINT_EXISTENCIA_ALMACEN",
                "point_code": "50161813",
                "point_name": "CHOCOLATE CRUNCH ROCKS",
                "quantity": "14.5",
                "unit": "KG",
                "unit_cost": "319.83",
                "total_cost": "4637.535",
            },
        )
        self.bad_huevo_cost = CostoInsumo.objects.create(
            insumo=self.huevo,
            fecha=date(2026, 6, 1),
            costo_unitario=Decimal("1.555667"),
            source_hash="bad-huevo-point",
            raw={
                "source": "POINT_EXISTENCIA_ALMACEN",
                "point_code": "007",
                "point_name": "HUEVO",
                "quantity": "-1800",
                "unit": "PZA",
                "unit_cost": "1.555667",
                "total_cost": "-2800.20",
            },
        )
        self.recipe = Receta.objects.create(
            nombre="Galleta de prueba",
            hash_contenido="hash_cost_integrity_recipe",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            insumo=self.huevo,
            insumo_texto="HUEVO",
            cantidad=Decimal("2"),
            unidad=self.unit_pza,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("1.555667"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        InsumoCostoHistoricoMensual.objects.create(
            periodo=date(2026, 6, 1),
            insumo=self.huevo,
            costo_unitario=Decimal("1.555667"),
            metodo=InsumoCostoHistoricoMensual.METODO_POINT_EXISTENCIA,
            source_date=date(2026, 6, 1),
            sample_count=1,
            weighted_quantity=Decimal("1"),
            metadata={"source_rows": [self.bad_huevo_cost.id]},
        )
        RecetaCostoHistoricoMensual.objects.create(
            periodo=date(2026, 6, 1),
            receta=self.recipe,
            costo_total=Decimal("3.111334"),
            costo_por_unidad_rendimiento=Decimal("3.111334"),
            lineas_costeadas=1,
            lineas_totales=1,
            coverage_pct=Decimal("100.000000"),
        )

    def test_classify_point_cost_detects_identity_quantity_and_unit_issues(self):
        self.assertEqual(classify_point_cost(self.safe_crunch_cost), [])

        classes = classify_point_cost(self.bad_crunch_cost)

        self.assertIn("QTY_NO_POSITIVA_CON_COSTO", classes)
        self.assertIn("NOMBRE_POINT_NO_COINCIDE", classes)
        self.assertIn("UNIDAD_INCOMPATIBLE", classes)

    def test_audit_point_cost_integrity_reports_impact_without_mutating_data(self):
        before_costs = CostoInsumo.objects.count()
        before_monthly = InsumoCostoHistoricoMensual.objects.count()
        stdout = StringIO()

        call_command("audit_point_cost_integrity", limit=10, stdout=stdout)
        payload = json.loads(stdout.getvalue())

        self.assertEqual(payload["mode"], "dry_run_read_only")
        self.assertEqual(payload["summary"]["point_existencia_rows"], 3)
        self.assertEqual(payload["summary"]["bad_cost_rows_total"], 2)
        self.assertEqual(payload["summary"]["monthly_bad_count"], 1)
        self.assertEqual(payload["summary"]["recipe_month_impacts_count"], 1)
        self.assertEqual(payload["summary"]["latest_unsafe_count"], 1)
        self.assertEqual(payload["latest_unsafe"][0]["insumo"], "HUEVO")
        self.assertEqual(payload["monthly_bad"][0]["insumo"], "HUEVO")
        self.assertEqual(payload["recipe_month_impacts_top"][0]["receta"], "Galleta de prueba")
        self.assertEqual(payload["summary"]["point_code_collision_count"], 1)

        self.assertEqual(CostoInsumo.objects.count(), before_costs)
        self.assertEqual(InsumoCostoHistoricoMensual.objects.count(), before_monthly)
