from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ROLE_DG
from core.models import Sucursal
from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, Proveedor, UnidadMedida
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from recetas.models import LineaReceta, Receta
from reportes.alert_service import generate_operational_alerts
from reportes.auto_production_service import (
    approve_production_order,
    execute_production_order,
    generate_daily_production_orders,
    release_production_order,
    sync_production_execution_logs,
)
from reportes.auto_purchase_service import generate_purchase_requests_from_production
from reportes.models import (
    Alert,
    AutoControlSettings,
    AutoPurchaseRequestSnapshot,
    FactProduccionDiaria,
    FactVentaDiaria,
    OperationsMetricSnapshot,
    ProductoCostoOperativoMensual,
    ProductoSucursalContribucionMensual,
    ProductionOrder,
    SupplierLeadTime,
)
from reportes.operations_metrics_service import rebuild_operations_metrics


ZERO = Decimal("0")


class OperationsAutomationFlowTests(TestCase):
    def setUp(self):
        self.target_date = date(2026, 4, 6)
        self.branch = Sucursal.objects.create(codigo="SUC-AUTO", nombre="Sucursal Auto")
        self.point_branch = PointBranch.objects.create(external_id="PB-AUTO", name="Sucursal Auto", erp_branch=self.branch)
        self.sync_job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)
        self.recipe = Receta.objects.create(
            nombre="Pastel Fresa",
            codigo_point="PFRE01",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Retail",
            hash_contenido="hash-auto-purchase",
        )
        self.point_product = PointProduct.objects.create(external_id="PP-AUTO", sku="PFRE01", name="Pastel Fresa", active=True)
        self.provider = Proveedor.objects.create(nombre="Proveedor Auto", lead_time_dias=2)
        self.unit = UnidadMedida.objects.create(codigo="pza-auto", nombre="Pieza auto", tipo=UnidadMedida.TIPO_PIEZA)
        self.insumo = Insumo.objects.create(
            nombre="Harina Auto",
            nombre_normalizado="harina auto",
            unidad_base=self.unit,
            proveedor_principal=self.provider,
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            posicion=1,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo=self.insumo,
            insumo_texto="Harina Auto",
            cantidad=Decimal("2"),
            unidad=self.unit,
            unidad_texto="pza-auto",
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
            costo_unitario_snapshot=Decimal("8"),
        )
        ExistenciaInsumo.objects.create(
            insumo=self.insumo,
            stock_actual=Decimal("6"),
            stock_minimo=Decimal("10"),
            punto_reorden=Decimal("8"),
        )
        SupplierLeadTime.objects.create(
            insumo=self.insumo,
            proveedor=self.provider,
            lead_time_dias=3,
            frecuencia_pedido_dias=5,
            lote_minimo=Decimal("6"),
        )
        controls = AutoControlSettings.get_solo()
        controls.max_compra_diaria = Decimal("250")
        controls.min_stock_seguridad = Decimal("5")
        controls.enable_auto_purchase = True
        controls.enable_alerts = True
        controls.save()
        self._seed_history()
        self._seed_profitability()
        self._seed_stock()
        self.user = get_user_model().objects.create_user(username="dg_auto_ops", password="secret")
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user.groups.add(dg_group)
        self.client = Client()
        self.client.force_login(self.user)

    def _seed_history(self):
        for offset in range(56, 0, -1):
            current_day = self.target_date - timedelta(days=offset)
            qty = Decimal("10")
            if offset <= 7:
                qty = Decimal("14")
            if current_day.weekday() == self.target_date.weekday():
                qty += Decimal("2")
            FactVentaDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe,
                point_product=self.point_product,
                producto_clave=self.recipe.codigo_point,
                producto_nombre=self.recipe.nombre,
                categoria=self.recipe.categoria,
                cantidad=qty,
                tickets=6,
                venta_bruta=qty * Decimal("250"),
                descuento=ZERO,
                venta_total=qty * Decimal("250"),
                venta_neta=qty * Decimal("250"),
                costo_estimado=qty * Decimal("120"),
                margen=qty * Decimal("130"),
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )
        for offset in range(28, 0, -1):
            current_day = self.target_date - timedelta(days=offset)
            FactProduccionDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe,
                producido=Decimal("18"),
                vendido=Decimal("14"),
                merma=Decimal("1"),
                transferido=ZERO,
            )

    def _seed_profitability(self):
        latest_period = date(2026, 3, 1)
        ProductoSucursalContribucionMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe,
            sucursal=self.branch,
            unidades_vendidas=Decimal("300"),
            venta_total=Decimal("75000"),
            asp=Decimal("250"),
            costo_producto_unit=Decimal("120"),
            costo_producto_total=Decimal("36000"),
            gasto_comercial_unit=Decimal("20"),
            gasto_comercial_total=Decimal("6000"),
            contribucion_total=Decimal("33000"),
            contribucion_unit=Decimal("110"),
            margen_contribucion_pct=Decimal("0.44"),
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe,
            unidades_base=Decimal("300"),
            venta_total=Decimal("75000"),
            asp=Decimal("250"),
            costo_mp_unit=Decimal("96"),
            mano_obra_prod_unit=Decimal("12"),
            indirecto_prod_unit=Decimal("7"),
            empaque_prod_unit=Decimal("5"),
            costo_fabricacion_unit=Decimal("120"),
        )

    def _seed_stock(self):
        captured_at = timezone.make_aware(datetime(2026, 4, 6, 5, 0, 0))
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=self.point_product,
            stock=Decimal("3"),
            min_stock=Decimal("2"),
            max_stock=Decimal("20"),
            captured_at=captured_at,
            sync_job=self.sync_job,
        )

    def test_purchase_alert_metrics_flow(self):
        generation = generate_daily_production_orders(self.target_date, created_by=self.user)
        self.assertEqual(generation["generated_orders"], 1)
        order = ProductionOrder.objects.prefetch_related("lines").get()

        approve_production_order(
            order,
            approved_by=self.user,
            approved_quantities={self.recipe.id: Decimal("5")},
        )
        order.refresh_from_db()
        self.assertEqual(order.status, ProductionOrder.STATUS_APPROVED)

        purchase_result = generate_purchase_requests_from_production(self.target_date, actor=self.user)
        self.assertEqual(purchase_result["generated"], 1)
        self.assertEqual(purchase_result["lines"], 1)
        solicitud = purchase_result["branches"][0]["lines"][0]
        self.assertEqual(Decimal(solicitud["shortage_immediate"]), Decimal("4.000"))
        self.assertEqual(solicitud["lead_time_dias"], 3)
        self.assertGreaterEqual(Decimal(solicitud["stock_target"]), Decimal("5.000"))
        self.assertGreater(Decimal(solicitud["priority_score"]), ZERO)
        snapshot = AutoPurchaseRequestSnapshot.objects.get()
        self.assertEqual(snapshot.fecha_sugerida_compra, self.target_date - timedelta(days=3))
        self.assertEqual(snapshot.frecuencia_pedido_dias, 5)
        self.assertEqual(snapshot.lote_minimo, Decimal("6"))

        second_purchase_result = generate_purchase_requests_from_production(self.target_date, actor=self.user)
        self.assertEqual(second_purchase_result["generated"], 0)
        self.assertEqual(second_purchase_result["updated"], 1)

        release_production_order(order)
        execute_production_order(order, executed_quantities={self.recipe.id: Decimal("8")})
        FactProduccionDiaria.objects.create(
            fecha=self.target_date,
            sucursal=self.branch,
            receta=self.recipe,
            producido=Decimal("8"),
            vendido=Decimal("4"),
            merma=Decimal("3"),
            transferido=ZERO,
        )
        FactVentaDiaria.objects.create(
            fecha=self.target_date,
            sucursal=self.branch,
            receta=self.recipe,
            point_product=self.point_product,
            producto_clave=self.recipe.codigo_point,
            producto_nombre=self.recipe.nombre,
            categoria=self.recipe.categoria,
            cantidad=Decimal("4"),
            tickets=3,
            venta_bruta=Decimal("1000"),
            descuento=ZERO,
            venta_total=Decimal("1000"),
            venta_neta=Decimal("1000"),
            costo_estimado=Decimal("480"),
            margen=Decimal("520"),
            source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
        )
        sync_production_execution_logs(target_date=self.target_date, actor=self.user)

        alert_result = generate_operational_alerts(target_date=self.target_date)
        self.assertGreaterEqual(alert_result["created_or_updated"], 2)
        self.assertTrue(Alert.objects.filter(fecha=self.target_date, tipo=Alert.TYPE_DESVIACION).exists())
        self.assertTrue(Alert.objects.filter(fecha=self.target_date, tipo=Alert.TYPE_STOCK).exists())
        alert_count = Alert.objects.count()
        second_alert_result = generate_operational_alerts(target_date=self.target_date)
        self.assertGreaterEqual(second_alert_result["created_or_updated"], 2)
        self.assertEqual(Alert.objects.count(), alert_count)

        metrics_result = rebuild_operations_metrics(target_date=self.target_date)
        self.assertEqual(OperationsMetricSnapshot.objects.count(), 1)
        self.assertEqual(metrics_result["orders"], 1)
        self.assertEqual(metrics_result["logs"], 1)
        snapshot_metrics = OperationsMetricSnapshot.objects.get()
        self.assertIsNotNone(snapshot_metrics.impacto_real)
        self.assertIsNotNone(snapshot_metrics.adopcion_real)
        self.assertIsNotNone(snapshot_metrics.efectividad_recomendaciones)

        response = self.client.get(reverse("reportes:production_orders"), {"fecha": self.target_date.isoformat()})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Solicitudes de compra generadas")
        self.assertContains(response, "Alertas críticas")
        self.assertContains(response, "Adopción del sistema")
        self.assertContains(response, "Alertas resueltas")

        alert = Alert.objects.filter(fecha=self.target_date).first()
        self.assertIsNotNone(alert)
        resolve_response = self.client.post(
            reverse("reportes:production_orders"),
            {
                "fecha": self.target_date.isoformat(),
                "action": "resolve_alert",
                "alert_id": alert.id,
                "resolution_note": "Atendida por operación",
                "impacto_real": "123.45",
            },
            follow=True,
        )
        self.assertEqual(resolve_response.status_code, 200)
        alert.refresh_from_db()
        self.assertTrue(alert.resuelta)
        self.assertEqual(alert.resolution_note, "Atendida por operación")
