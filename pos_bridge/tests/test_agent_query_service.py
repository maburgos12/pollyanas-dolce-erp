from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from core.models import AuditLog, Sucursal
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import (
    PointBranch,
    PointDailySale,
    PointInventorySnapshot,
    PointMonthlySalesOfficial,
    PointProduct,
    PointSyncJob,
)
from pos_bridge.services.agent_query_service import PosAgentQueryService
from recetas.models import LineaReceta, Receta, VentaHistorica


class PosAgentQueryServiceTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="agent_user",
            email="agent_user@example.com",
            password="test12345",
            is_staff=True,
        )
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.branch = PointBranch.objects.create(
            external_id="1",
            name="MATRIZ",
            status=PointBranch.STATUS_ACTIVE,
            erp_branch=self.sucursal,
        )
        self.product = PointProduct.objects.create(
            external_id="100",
            sku="0100",
            name="Pastel de Fresas Con Crema Mediano",
            category="PASTEL MEDIANO",
        )
        self.sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            triggered_by=self.user,
        )
        self.sales_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_SUCCESS,
            triggered_by=self.user,
        )
        PointInventorySnapshot.objects.create(
            branch=self.branch,
            product=self.product,
            stock=Decimal("10"),
            min_stock=Decimal("2"),
            max_stock=Decimal("12"),
            captured_at=timezone.now(),
            sync_job=self.sync_job,
        )
        self.receta = Receta.objects.create(
            nombre="Pastel Fresas Con Crema - Mediano",
            codigo_point="0100",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"hash-{uuid4()}",
        )
        unidad = UnidadMedida.objects.create(
            codigo="pza",
            nombre="Pieza",
            tipo=UnidadMedida.TIPO_PIEZA,
            factor_to_base=Decimal("1"),
        )
        insumo = Insumo.objects.create(nombre="Caja pastel mediano", unidad_base=unidad, activo=True)
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=insumo,
            insumo_texto="Caja pastel mediano",
            cantidad=Decimal("1"),
            unidad=unidad,
            unidad_texto="pza",
            costo_unitario_snapshot=Decimal("12"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        PointDailySale.objects.create(
            branch=self.branch,
            product=self.product,
            receta=self.receta,
            sync_job=self.sales_job,
            sale_date=timezone.localdate(),
            quantity=Decimal("4"),
            tickets=2,
            gross_amount=Decimal("600"),
            discount_amount=Decimal("50"),
            total_amount=Decimal("550"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("550"),
        )

    def test_sales_summary_query_uses_rule_based_intent(self):
        result = PosAgentQueryService().process_query(
            query="Cuanto vendimos en Matriz este mes",
            user=self.user,
        )
        self.assertEqual(result["query_type"], "sales_summary")
        self.assertEqual(result["data"]["branch_filter"], "MATRIZ")
        self.assertEqual(result["data"]["total_sales"], "550")
        self.assertTrue(AuditLog.objects.filter(action="agent_query").exists())

    def test_sales_summary_blocks_recipe_linked_history_as_official_total(self):
        VentaHistorica.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=timezone.localdate(),
            cantidad=Decimal("25"),
            tickets=5,
            monto_total=Decimal("3200.50"),
            fuente="POINT_BRIDGE_SALES",
        )

        result = PosAgentQueryService().process_query(
            query="Cuanto vendimos en Matriz este mes",
            user=self.user,
        )

        self.assertEqual(result["query_type"], "reconciliation_required")
        self.assertEqual(result["data"]["status"], "RECIPE_LINKED_ONLY")
        self.assertEqual(result["data"]["source"], "VentaHistorica")

    def test_recipe_query_returns_bom(self):
        result = PosAgentQueryService().process_query(
            query="Dame la receta de Pastel de Fresas Con Crema Mediano",
            user=self.user,
        )
        self.assertEqual(result["query_type"], "recipe")
        self.assertEqual(result["data"]["receta_id"], self.receta.id)
        self.assertEqual(len(result["data"]["bom"]), 1)

    @override_settings(OPENAI_API_KEY="")
    def test_general_query_without_openai_returns_guidance(self):
        result = PosAgentQueryService().process_query(query="Necesito ayuda", user=self.user)
        self.assertEqual(result["query_type"], "general")
        self.assertIn("No logre clasificar", result["answer"])

    def test_sales_summary_warns_when_historical_sources_overlap(self):
        fecha = timezone.localdate().replace(day=1)
        VentaHistorica.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=fecha,
            cantidad=Decimal("10"),
            tickets=2,
            monto_total=Decimal("1000.00"),
            fuente="POINT_HIST_2026_Q1",
        )
        VentaHistorica.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=fecha,
            cantidad=Decimal("8"),
            tickets=1,
            monto_total=Decimal("900.00"),
            fuente="POINT_BRIDGE_SALES",
        )

        result = PosAgentQueryService().process_query(
            query="Cuanto vendimos en Matriz este mes",
            user=self.user,
        )

        self.assertEqual(result["query_type"], "reconciliation_required")
        self.assertEqual(result["data"]["status"], "NOT_RECONCILED")

    def test_sales_summary_prefers_official_pointdaily_sales_over_historical(self):
        fecha = timezone.localdate().replace(day=1)
        self.branch.name = "Matriz"
        self.branch.save(update_fields=["name"])
        PointDailySale.objects.filter(branch=self.branch).delete()
        PointDailySale.objects.create(
            branch=self.branch,
            product=self.product,
            receta=self.receta,
            sync_job=self.sales_job,
            sale_date=fecha,
            quantity=Decimal("10"),
            tickets=3,
            gross_amount=Decimal("1000"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("1000"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("1000"),
            source_endpoint="/Report/PrintReportes?idreporte=3",
        )
        VentaHistorica.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=fecha,
            cantidad=Decimal("8"),
            tickets=1,
            monto_total=Decimal("900.00"),
            fuente="POINT_BRIDGE_SALES",
        )

        result = PosAgentQueryService().process_query(
            query="Cuanto vendimos en Matriz este mes",
            user=self.user,
        )

        self.assertEqual(result["query_type"], "sales_summary")
        self.assertEqual(result["data"]["source"], "PointDailySaleOfficial")
        self.assertEqual(result["data"]["total_sales"], "1000")

    def test_sales_summary_prefers_official_monthly_cache_for_closed_month_without_branch(self):
        PointDailySale.objects.all().delete()
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2025, 10, 1).date(),
            month_end=timezone.datetime(2025, 10, 31).date(),
            total_quantity=Decimal("1000"),
            gross_amount=Decimal("3500000.00"),
            discount_amount=Decimal("54750.00"),
            total_amount=Decimal("3445250.00"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("3445250.00"),
        )

        result = PosAgentQueryService().process_query(
            query="Cuanto vendimos en octubre 2025",
            user=self.user,
        )

        self.assertEqual(result["query_type"], "sales_summary")
        self.assertEqual(result["data"]["source"], "PointMonthlySalesOfficial")
        self.assertEqual(result["data"]["total_sales"], "3445250.00")
        self.assertIsNone(result["data"]["total_tickets"])

    def test_sales_trend_prefers_official_monthly_cache_for_closed_months(self):
        PointDailySale.objects.all().delete()
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2025, 10, 1).date(),
            month_end=timezone.datetime(2025, 10, 31).date(),
            total_quantity=Decimal("1000"),
            gross_amount=Decimal("3500000.00"),
            discount_amount=Decimal("54750.00"),
            total_amount=Decimal("3445250.00"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("3445250.00"),
        )
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2025, 11, 1).date(),
            month_end=timezone.datetime(2025, 11, 30).date(),
            total_quantity=Decimal("900"),
            gross_amount=Decimal("3300000.00"),
            discount_amount=Decimal("53007.52"),
            total_amount=Decimal("3246992.48"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("3246992.48"),
        )
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2025, 12, 1).date(),
            month_end=timezone.datetime(2025, 12, 31).date(),
            total_quantity=Decimal("1200"),
            gross_amount=Decimal("4900000.00"),
            discount_amount=Decimal("75531.09"),
            total_amount=Decimal("4824468.91"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("4824468.91"),
        )

        result = PosAgentQueryService().process_query(
            query="Dame la tendencia mensual de octubre 2025 a diciembre 2025",
            user=self.user,
        )

        self.assertEqual(result["query_type"], "sales_trend")
        self.assertEqual(result["data"]["source"], "PointMonthlySalesOfficial")
        self.assertEqual(result["data"]["trends"][0]["month"], "2025-10")
        self.assertEqual(result["data"]["trends"][0]["total_sales"], "3445250.00")
        self.assertIsNone(result["data"]["trends"][0]["avg_ticket"])
