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
from inventario.models import AlmacenSyncRun, ExistenciaInsumo
from inventario.stock_trace import TRACE_MANUAL_SYNC, build_stock_trace
from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob
from recetas.models import LineaReceta, Receta, RecetaCostoVersion
from recetas.utils.costeo_snapshot import resolve_line_snapshot_cost, resolve_preparation_recipe_for_insumo
from reportes.auto_production_service import (
    approve_production_order,
    execute_production_order,
    generate_daily_production_orders,
    release_production_order,
    sync_production_execution_logs,
)
from reportes.production_projection_supply_service import build_projection_supply_context
from reportes.production_supply_service import build_production_supply_context
from reportes.models import (
    FactProduccionDiaria,
    FactVentaDiaria,
    ProductoCostoOperativoMensual,
    ProductoSucursalContribucionMensual,
    ProductionExecutionLog,
    ProductionOrder,
)


ZERO = Decimal("0")


class AutoProductionFlowTests(TestCase):
    def setUp(self):
        self.target_date = date(2026, 4, 6)
        self.branch = Sucursal.objects.create(codigo="SUC-OPS", nombre="Sucursal Operación")
        self.point_branch = PointBranch.objects.create(external_id="PB-OPS", name="Sucursal Operación", erp_branch=self.branch)
        self.sync_job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)
        self.recipe_hot = Receta.objects.create(
            nombre="Pastel Mango",
            codigo_point="PMAN01",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Especial",
            hash_contenido="hash-auto-hot",
        )
        self.recipe_cold = Receta.objects.create(
            nombre="Galleta Nuez",
            codigo_point="GNUEZ1",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Galletas",
            categoria="Retail",
            hash_contenido="hash-auto-cold",
        )
        self.point_product_hot = PointProduct.objects.create(external_id="PP-HOT-OPS", sku="PMAN01", name="Pastel Mango", active=True)
        self.point_product_cold = PointProduct.objects.create(external_id="PP-COLD-OPS", sku="GNUEZ1", name="Galleta Nuez", active=True)
        self._seed_history()
        self._seed_profitability()
        self._seed_stock()
        self.user = get_user_model().objects.create_user(username="dg_ops", password="secret")
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user.groups.add(dg_group)

    def _seed_history(self):
        for offset in range(56, 0, -1):
            current_day = self.target_date - timedelta(days=offset)
            weekday_boost = Decimal("1.35") if current_day.weekday() == self.target_date.weekday() else Decimal("1.00")
            hot_qty = Decimal("12") * weekday_boost
            cold_qty = Decimal("3")
            if offset <= 7:
                hot_qty += Decimal("5")
                cold_qty = Decimal("1")
            FactVentaDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe_hot,
                point_product=self.point_product_hot,
                producto_clave="PMAN01",
                producto_nombre="Pastel Mango",
                categoria="Especial",
                cantidad=hot_qty,
                tickets=8,
                venta_bruta=hot_qty * Decimal("250"),
                descuento=ZERO,
                venta_total=hot_qty * Decimal("250"),
                venta_neta=hot_qty * Decimal("250"),
                costo_estimado=hot_qty * Decimal("115"),
                margen=hot_qty * Decimal("135"),
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )
            FactVentaDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe_cold,
                point_product=self.point_product_cold,
                producto_clave="GNUEZ1",
                producto_nombre="Galleta Nuez",
                categoria="Retail",
                cantidad=cold_qty,
                tickets=3,
                venta_bruta=cold_qty * Decimal("80"),
                descuento=ZERO,
                venta_total=cold_qty * Decimal("80"),
                venta_neta=cold_qty * Decimal("80"),
                costo_estimado=cold_qty * Decimal("45"),
                margen=cold_qty * Decimal("35"),
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )
        for offset in range(28, 0, -1):
            current_day = self.target_date - timedelta(days=offset)
            FactProduccionDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe_hot,
                producido=Decimal("16"),
                vendido=Decimal("13"),
                merma=Decimal("1"),
                transferido=ZERO,
            )
            FactProduccionDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe_cold,
                producido=Decimal("5"),
                vendido=Decimal("2"),
                merma=Decimal("1"),
                transferido=ZERO,
            )

    def _seed_profitability(self):
        latest_period = date(2026, 3, 1)
        ProductoSucursalContribucionMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe_hot,
            sucursal=self.branch,
            unidades_vendidas=Decimal("320"),
            venta_total=Decimal("80000"),
            asp=Decimal("250"),
            costo_producto_unit=Decimal("115"),
            costo_producto_total=Decimal("36800"),
            gasto_comercial_unit=Decimal("20"),
            gasto_comercial_total=Decimal("6400"),
            contribucion_total=Decimal("36800"),
            contribucion_unit=Decimal("115"),
            margen_contribucion_pct=Decimal("0.46"),
        )
        ProductoSucursalContribucionMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe_cold,
            sucursal=self.branch,
            unidades_vendidas=Decimal("85"),
            venta_total=Decimal("6800"),
            asp=Decimal("80"),
            costo_producto_unit=Decimal("45"),
            costo_producto_total=Decimal("3825"),
            gasto_comercial_unit=Decimal("10"),
            gasto_comercial_total=Decimal("850"),
            contribucion_total=Decimal("2125"),
            contribucion_unit=Decimal("25"),
            margen_contribucion_pct=Decimal("0.31"),
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe_hot,
            unidades_base=Decimal("320"),
            venta_total=Decimal("80000"),
            asp=Decimal("250"),
            costo_mp_unit=Decimal("94"),
            mano_obra_prod_unit=Decimal("11"),
            indirecto_prod_unit=Decimal("6"),
            empaque_prod_unit=Decimal("4"),
            costo_fabricacion_unit=Decimal("115"),
        )
        ProductoCostoOperativoMensual.objects.create(
            periodo=latest_period,
            receta=self.recipe_cold,
            unidades_base=Decimal("85"),
            venta_total=Decimal("6800"),
            asp=Decimal("80"),
            costo_mp_unit=Decimal("34"),
            mano_obra_prod_unit=Decimal("6"),
            indirecto_prod_unit=Decimal("3"),
            empaque_prod_unit=Decimal("2"),
            costo_fabricacion_unit=Decimal("45"),
        )

    def _seed_stock(self):
        captured_at = timezone.make_aware(datetime(2026, 4, 6, 5, 0, 0))
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=self.point_product_hot,
            stock=Decimal("4"),
            min_stock=Decimal("2"),
            max_stock=Decimal("20"),
            captured_at=captured_at,
            sync_job=self.sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=self.point_product_cold,
            stock=Decimal("10"),
            min_stock=Decimal("2"),
            max_stock=Decimal("20"),
            captured_at=captured_at,
            sync_job=self.sync_job,
        )

    def test_auto_production_flow_end_to_end(self):
        generation = generate_daily_production_orders(self.target_date, created_by=self.user)
        self.assertEqual(generation["generated_orders"], 1)
        self.assertEqual(ProductionOrder.objects.count(), 1)
        order = ProductionOrder.objects.prefetch_related("lines").get()
        self.assertEqual(order.status, ProductionOrder.STATUS_PROPOSED)
        self.assertGreater(order.lines.count(), 0)

        second_pass = generate_daily_production_orders(self.target_date, created_by=self.user)
        self.assertEqual(second_pass["generated_orders"], 0)
        self.assertEqual(second_pass["updated_orders"], 1)
        self.assertEqual(ProductionOrder.objects.count(), 1)

        approve_production_order(order, approved_by=self.user)
        order.refresh_from_db()
        self.assertEqual(order.status, ProductionOrder.STATUS_APPROVED)
        self.assertTrue(order.lines.filter(cantidad_aprobada__gt=ZERO).exists())

        release_production_order(order)
        order.refresh_from_db()
        self.assertEqual(order.status, ProductionOrder.STATUS_RELEASED)

        executed_quantities = {line.receta_id: line.cantidad_aprobada + Decimal("1") for line in order.lines.all()}
        execute_production_order(order, executed_quantities=executed_quantities)
        order.refresh_from_db()
        self.assertEqual(order.status, ProductionOrder.STATUS_EXECUTED)

        for line in order.lines.all():
            FactProduccionDiaria.objects.create(
                fecha=self.target_date,
                sucursal=self.branch,
                receta=line.receta,
                producido=line.cantidad_ejecutada,
                vendido=max(line.cantidad_ejecutada - Decimal("1"), ZERO),
                merma=Decimal("1"),
                transferido=ZERO,
            )
            FactVentaDiaria.objects.create(
                fecha=self.target_date,
                sucursal=self.branch,
                receta=line.receta,
                point_product=self.point_product_hot if line.receta_id == self.recipe_hot.id else self.point_product_cold,
                producto_clave=line.receta.codigo_point,
                producto_nombre=line.receta.nombre,
                categoria=line.receta.categoria,
                cantidad=max(line.cantidad_ejecutada - Decimal("1"), ZERO),
                tickets=5,
                venta_bruta=Decimal("1000"),
                descuento=ZERO,
                venta_total=Decimal("1000"),
                venta_neta=Decimal("1000"),
                costo_estimado=Decimal("500"),
                margen=Decimal("500"),
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )

        sync_result = sync_production_execution_logs(target_date=self.target_date, actor=self.user)
        self.assertEqual(sync_result["orders"], 1)
        self.assertEqual(sync_result["logs"], order.lines.count())
        self.assertEqual(ProductionExecutionLog.objects.count(), order.lines.count())
        self.assertTrue(ProductionExecutionLog.objects.filter(aprobado__gt=ZERO).exists())


class AutoProductionViewTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(username="dg_view", password="secret")
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user.groups.add(dg_group)
        self.client.force_login(self.user)
        self.branch = Sucursal.objects.create(codigo="SUC-VIEW", nombre="Sucursal View")

    def test_view_loads_for_dg_user(self):
        response = self.client.get(reverse("reportes:production_orders"), {"fecha": "2026-04-06"})
        self.assertEqual(response.status_code, 200)


class ProductionSupplyContextTests(TestCase):
    def setUp(self):
        self.target_date = date(2026, 4, 8)
        self.branch = Sucursal.objects.create(codigo="SUC-SUP", nombre="Sucursal Supply")
        self.unit_piece = UnidadMedida.objects.create(codigo="pza-test", nombre="Pieza test", tipo=UnidadMedida.TIPO_PIEZA)
        self.unit_kg = UnidadMedida.objects.create(codigo="kg-test", nombre="Kilo test", tipo=UnidadMedida.TIPO_MASA)
        self.provider = Proveedor.objects.create(nombre="Proveedor Supply")
        self.sync_run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_MANUAL,
            status=AlmacenSyncRun.STATUS_OK,
        )
        self.insumo_huevo = Insumo.objects.create(
            nombre="Huevo Supply",
            nombre_normalizado="huevo supply",
            unidad_base=self.unit_piece,
            proveedor_principal=self.provider,
        )
        self.insumo_harina = Insumo.objects.create(
            nombre="Harina Supply",
            nombre_normalizado="harina supply",
            unidad_base=self.unit_kg,
            proveedor_principal=self.provider,
        )
        self.recipe = Receta.objects.create(
            nombre="Pastel Supply",
            codigo_point="SUP001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pasteles",
            categoria="Supply",
            hash_contenido="hash-supply-context",
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            posicion=1,
            insumo=self.insumo_huevo,
            insumo_texto="Huevo Supply",
            cantidad=Decimal("2"),
            unidad_texto="pza",
            unidad=self.unit_piece,
            match_status=LineaReceta.STATUS_AUTO,
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            posicion=2,
            insumo=self.insumo_harina,
            insumo_texto="Harina Supply",
            cantidad=Decimal("1"),
            unidad_texto="kg",
            unidad=self.unit_kg,
            match_status=LineaReceta.STATUS_AUTO,
        )
        eggs = ExistenciaInsumo.objects.create(insumo=self.insumo_huevo, stock_actual=Decimal("30"))
        flour = ExistenciaInsumo.objects.create(insumo=self.insumo_harina, stock_actual=Decimal("4"))
        eggs.trazabilidad_stock = build_stock_trace(
            source=TRACE_MANUAL_SYNC,
            process="inventario.sync_almacen",
            effective_at=self.target_date,
            reference="manual-test-eggs",
            run=self.sync_run,
            quality="DIRECT",
        )
        eggs.save(update_fields=["trazabilidad_stock"])
        flour.trazabilidad_stock = build_stock_trace(
            source=TRACE_MANUAL_SYNC,
            process="inventario.sync_almacen",
            effective_at=self.target_date,
            reference="manual-test-flour",
            run=self.sync_run,
            quality="DIRECT",
        )
        flour.save(update_fields=["trazabilidad_stock"])
        self.order = ProductionOrder.objects.create(
            fecha=self.target_date,
            sucursal=self.branch,
            status=ProductionOrder.STATUS_APPROVED,
            source=ProductionOrder.SOURCE_AUTO,
        )
        self.order_line = self.order.lines.create(
            receta=self.recipe,
            cantidad_recomendada=Decimal("10"),
            cantidad_aprobada=Decimal("10"),
            decision_score=Decimal("88"),
        )

    def test_supply_context_explodes_bom_and_detects_shortage(self):
        context = build_production_supply_context(target_date=self.target_date)
        self.assertFalse(context["source_is_point"])
        self.assertEqual(context["inventory_scope"], "GLOBAL_INSUMO")
        self.assertEqual(context["summary"]["active_orders"], 1)
        self.assertEqual(context["summary"]["unique_insumos"], 2)
        self.assertEqual(context["summary"]["shortage_insumos"], 1)
        rows = {row["insumo_nombre"]: row for row in context["rows"]}
        self.assertEqual(rows["Huevo Supply"]["required_qty"], Decimal("20.000"))
        self.assertEqual(rows["Huevo Supply"]["shortage_qty"], Decimal("0.000"))
        self.assertEqual(rows["Huevo Supply"]["status_label"], "Disponible")
        self.assertEqual(rows["Harina Supply"]["required_qty"], Decimal("10.000"))
        self.assertEqual(rows["Harina Supply"]["shortage_qty"], Decimal("6.000"))
        self.assertEqual(rows["Harina Supply"]["status_label"], "Stock parcial")
        order_context = context["orders"][self.order.id]
        self.assertEqual(order_context["item_count"], 2)
        self.assertEqual(order_context["shortage_items"], 1)
        self.assertTrue(order_context["has_shortage"])
        self.assertEqual(len(order_context["recipe_rows"]), 1)
        recipe_row = order_context["recipe_rows"][0]
        self.assertEqual(recipe_row["recipe_name"], "Pastel Supply")
        self.assertEqual(recipe_row["status_label"], "Stock parcial")
        recipe_items = {item["insumo_nombre"]: item for item in recipe_row["items"]}
        self.assertEqual(recipe_items["Huevo Supply"]["status_label"], "Disponible")
        self.assertEqual(recipe_items["Harina Supply"]["status_label"], "Stock parcial")


class ProductionSupplyViewTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(username="dg_supply", password="secret")
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user.groups.add(dg_group)
        self.client.force_login(self.user)

        self.target_date = date(2026, 4, 8)
        self.branch = Sucursal.objects.create(codigo="SUC-SCR", nombre="Sucursal Screen")
        unit_piece = UnidadMedida.objects.create(codigo="pza-screen", nombre="Pieza screen", tipo=UnidadMedida.TIPO_PIEZA)
        provider = Proveedor.objects.create(nombre="Proveedor Screen")
        insumo = Insumo.objects.create(
            nombre="Azúcar Screen",
            nombre_normalizado="azucar screen",
            unidad_base=unit_piece,
            proveedor_principal=provider,
        )
        recipe = Receta.objects.create(
            nombre="Receta Screen",
            codigo_point="SCR001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Screen",
            categoria="Screen",
            hash_contenido="hash-screen-production-supply",
        )
        LineaReceta.objects.create(
            receta=recipe,
            posicion=1,
            insumo=insumo,
            insumo_texto="Azúcar Screen",
            cantidad=Decimal("5"),
            unidad_texto="pza",
            unidad=unit_piece,
            match_status=LineaReceta.STATUS_AUTO,
        )
        existencia = ExistenciaInsumo.objects.create(insumo=insumo, stock_actual=Decimal("12"))
        existencia.trazabilidad_stock = {
            "source": TRACE_MANUAL_SYNC,
            "label": "Sync manual",
            "process": "inventario.sync_almacen",
            "effective_at": self.target_date.isoformat(),
            "quality": "DIRECT",
        }
        existencia.save(update_fields=["trazabilidad_stock"])
        order = ProductionOrder.objects.create(
            fecha=self.target_date,
            sucursal=self.branch,
            status=ProductionOrder.STATUS_PROPOSED,
            source=ProductionOrder.SOURCE_AUTO,
        )
        order.lines.create(
            receta=recipe,
            cantidad_recomendada=Decimal("3"),
            cantidad_aprobada=Decimal("0"),
            decision_score=Decimal("70"),
        )

    def test_view_exposes_supply_reconciliation(self):
        response = self.client.get(reverse("reportes:production_orders"), {"fecha": self.target_date.isoformat()})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Conciliación de insumos para Producción")
        self.assertContains(response, "Disponibilidad por receta")
        self.assertContains(response, "Azúcar Screen")
        self.assertContains(response, "Stock parcial")
        self.assertContains(response, "La fuente de verdad del inventario físico para Producción")


class ProjectionSupplyContextTests(TestCase):
    def setUp(self):
        self.target_date = date(2026, 4, 8)
        self.branch = Sucursal.objects.create(codigo="SUC-PRJ", nombre="Sucursal Proyección")
        self.unit_kg = UnidadMedida.objects.create(codigo="kg-prj", nombre="Kilo proyección", tipo=UnidadMedida.TIPO_MASA)
        self.provider = Proveedor.objects.create(nombre="Proveedor Proyección")
        self.insumo = Insumo.objects.create(
            nombre="Harina Proyección",
            nombre_normalizado="harina proyeccion",
            unidad_base=self.unit_kg,
            proveedor_principal=self.provider,
        )
        self.recipe = Receta.objects.create(
            nombre="Pastel Proyección",
            codigo_point="PRJ001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pasteles",
            categoria="Evento",
            hash_contenido="hash-projection-supply",
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Harina Proyección",
            cantidad=Decimal("1.5"),
            unidad_texto="kg",
            unidad=self.unit_kg,
            match_status=LineaReceta.STATUS_AUTO,
        )
        for offset in range(56, 0, -1):
            current_day = self.target_date - timedelta(days=offset)
            FactVentaDiaria.objects.create(
                fecha=current_day,
                sucursal=self.branch,
                receta=self.recipe,
                producto_clave=self.recipe.codigo_point,
                producto_nombre=self.recipe.nombre,
                categoria=self.recipe.categoria,
                cantidad=Decimal("10"),
                tickets=5,
                venta_bruta=Decimal("1000"),
                descuento=ZERO,
                venta_total=Decimal("1000"),
                venta_neta=Decimal("1000"),
                costo_estimado=Decimal("500"),
                margen=Decimal("500"),
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )
        ExistenciaInsumo.objects.create(insumo=self.insumo, stock_actual=Decimal("999"))

    def test_projection_supply_uses_forecast_without_discounting_stock(self):
        context = build_projection_supply_context(target_date=self.target_date, top_n=10)
        self.assertEqual(context["mode"], "PROJECTION_EVENT")
        self.assertFalse(context["uses_stock"])
        self.assertEqual(context["summary"]["projected_products"], 1)
        self.assertEqual(context["summary"]["projected_insumos"], 1)
        product_row = context["products"][0]
        insumo_row = context["insumos"][0]
        self.assertEqual(product_row["recipe_name"], "Pastel Proyección")
        self.assertGreater(product_row["forecast_qty"], ZERO)
        self.assertEqual(
            insumo_row["required_gross_qty"],
            (product_row["forecast_qty"] * Decimal("1.5")).quantize(Decimal("0.001")),
        )

    def test_projection_supply_ignores_rejected_recipe_lines(self):
        rejected_insumo = Insumo.objects.create(
            codigo_point="OLD",
            nombre="Insumo obsoleto",
            nombre_normalizado="insumo obsoleto",
            unidad_base=self.unit_kg,
            proveedor_principal=self.provider,
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            posicion=2,
            insumo=rejected_insumo,
            insumo_texto=rejected_insumo.nombre,
            cantidad=Decimal("4"),
            unidad_texto="kg",
            unidad=self.unit_kg,
            match_status=LineaReceta.STATUS_REJECTED,
        )

        context = build_projection_supply_context(target_date=self.target_date, top_n=10)

        insumo_ids = {row["insumo_id"] for row in context["insumos"]}
        self.assertIn(self.insumo.id, insumo_ids)
        self.assertNotIn(rejected_insumo.id, insumo_ids)

    def test_projection_supply_calculates_estimated_spend_from_latest_cost(self):
        CostoInsumo.objects.create(
            insumo=self.insumo,
            proveedor=self.provider,
            fecha=self.target_date,
            costo_unitario=Decimal("12.50"),
            source_hash="projection-supply-cost",
        )
        context = build_projection_supply_context(
            target_date=self.target_date,
            forecast_context={
                "target_label": "Forecast prueba",
                "summary": {"forecast_units": Decimal("20")},
                "rows": [
                    {
                        "branch_id": self.branch.id,
                        "branch_code": self.branch.codigo,
                        "branch_name": self.branch.nombre,
                        "recipe_id": self.recipe.id,
                        "recipe_name": self.recipe.nombre,
                        "forecast_qty": Decimal("20"),
                        "buffer_units": Decimal("0"),
                    }
                ],
            },
        )

        insumo_row = context["insumos"][0]
        self.assertEqual(insumo_row["article_class_label"], "Materia prima")
        self.assertEqual(insumo_row["unit_cost"], Decimal("12.50"))
        self.assertEqual(insumo_row["estimated_spend"], Decimal("375.00"))
        self.assertEqual(context["summary"]["estimated_spend"], Decimal("375.00"))

    def test_projection_supply_uses_costed_preparation_for_internal_input(self):
        prep_recipe = Receta.objects.create(
            nombre="Batida Proyección",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("1"),
            rendimiento_unidad=self.unit_kg,
            hash_contenido="hash-projection-prep-cost",
        )
        internal_input = Insumo.objects.create(
            codigo=f"DERIVADO:RECETA:{prep_recipe.id}:PREPARACION",
            nombre="Batida Proyección",
            nombre_normalizado="batida proyeccion",
            unidad_base=self.unit_kg,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        RecetaCostoVersion.objects.create(
            receta=prep_recipe,
            version_num=1,
            hash_snapshot="hash-projection-prep-cost-v1",
            costo_total=Decimal("5.00"),
            fuente="POINT_PRODUCTION_REPORT",
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            posicion=2,
            insumo=internal_input,
            insumo_texto=internal_input.nombre,
            cantidad=Decimal("3"),
            unidad_texto="kg",
            unidad=self.unit_kg,
            match_status=LineaReceta.STATUS_AUTO,
        )

        context = build_projection_supply_context(
            target_date=self.target_date,
            forecast_context={
                "target_label": "Forecast preparación",
                "summary": {"forecast_units": Decimal("2")},
                "rows": [
                    {
                        "branch_id": self.branch.id,
                        "branch_code": self.branch.codigo,
                        "branch_name": self.branch.nombre,
                        "recipe_id": self.recipe.id,
                        "recipe_name": self.recipe.nombre,
                        "forecast_qty": Decimal("2"),
                        "buffer_units": Decimal("0"),
                    }
                ],
            },
        )

        internal_row = next(row for row in context["insumos"] if row["insumo_id"] == internal_input.id)
        self.assertEqual(internal_row["article_class_label"], "Insumo interno")
        self.assertEqual(internal_row["unit_cost"], Decimal("5.000000"))
        self.assertEqual(internal_row["estimated_spend"], Decimal("30.00"))
        self.assertEqual(internal_row["cost_sources_text"], "POINT_PRODUCTION_REPORT")

    def test_preparation_resolution_rejects_stale_derived_recipe_when_name_mismatches(self):
        stale_recipe = Receta.objects.create(
            nombre="Mermelada Fresa Liquida Proyección",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("1"),
            rendimiento_unidad=self.unit_kg,
            hash_contenido="hash-stale-derived-prep",
        )
        internal_input = Insumo.objects.create(
            codigo=f"DERIVADO:RECETA:{stale_recipe.id}:PREPARACION",
            nombre="Galleta Para Pay Proyección",
            nombre_normalizado="galleta para pay proyeccion",
            unidad_base=self.unit_kg,
            tipo_item=Insumo.TIPO_INTERNO,
        )

        self.assertIsNone(resolve_preparation_recipe_for_insumo(internal_input))

    def test_projection_supply_uses_canonical_cost_when_stale_derived_recipe_mismatches(self):
        unit_g = UnidadMedida.objects.create(
            codigo="g-prj",
            nombre="Gramo proyección",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1"),
        )
        unit_kg = UnidadMedida.objects.create(
            codigo="kg-prj-canon",
            nombre="Kilo canónico proyección",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        stale_recipe = Receta.objects.create(
            nombre="Mermelada Fresa Liquida Proyección",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("1"),
            rendimiento_unidad=self.unit_kg,
            hash_contenido="hash-stale-derived-canonical",
        )
        internal_input = Insumo.objects.create(
            codigo=f"DERIVADO:RECETA:{stale_recipe.id}:PREPARACION",
            nombre="Galleta Para Pay Proyección",
            nombre_normalizado="galleta para pay proyeccion",
            unidad_base=unit_kg,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        CostoInsumo.objects.create(
            insumo=internal_input,
            proveedor=self.provider,
            fecha=self.target_date,
            costo_unitario=Decimal("250.00"),
            source_hash="projection-stale-canonical-cost",
        )
        recipe = Receta.objects.create(
            nombre="Pay Galleta Proyección",
            codigo_point="PAYGALPRJ",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pay-galleta-proj",
        )
        LineaReceta.objects.create(
            receta=recipe,
            posicion=1,
            insumo=internal_input,
            insumo_texto=internal_input.nombre,
            cantidad=Decimal("100"),
            unidad=unit_g,
            unidad_texto="g",
            match_status=LineaReceta.STATUS_AUTO,
        )

        context = build_projection_supply_context(
            target_date=self.target_date,
            forecast_context={
                "target_label": "Forecast costo canónico",
                "summary": {"forecast_units": Decimal("1")},
                "rows": [
                    {
                        "branch_id": self.branch.id,
                        "branch_code": self.branch.codigo,
                        "branch_name": self.branch.nombre,
                        "recipe_id": recipe.id,
                        "recipe_name": recipe.nombre,
                        "forecast_qty": Decimal("1"),
                        "buffer_units": Decimal("0"),
                    }
                ],
            },
        )

        insumo_row = context["insumos"][0]
        self.assertEqual(insumo_row["unit_cost"], Decimal("0.250000"))
        self.assertEqual(insumo_row["estimated_spend"], Decimal("25.00"))
        self.assertEqual(insumo_row["cost_sources_text"], "COSTO_CANONICO")
        self.assertEqual(context["summary"]["missing_cost_insumos"], 0)

    def test_line_snapshot_cost_is_used_when_preparation_unit_is_incompatible(self):
        unit_pz = UnidadMedida.objects.create(codigo="pz-prj-snap", nombre="Pieza snapshot", tipo=UnidadMedida.TIPO_PIEZA)
        prep_recipe = Receta.objects.create(
            nombre="Pan Snapshot Proyección",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("1"),
            rendimiento_unidad=unit_pz,
            hash_contenido="hash-snapshot-incompatible-prep",
        )
        internal_input = Insumo.objects.create(
            codigo=f"DERIVADO:RECETA:{prep_recipe.id}:PREPARACION",
            nombre="Pan Snapshot Proyección",
            nombre_normalizado="pan snapshot proyeccion",
            unidad_base=self.unit_kg,
            tipo_item=Insumo.TIPO_INTERNO,
        )
        RecetaCostoVersion.objects.create(
            receta=prep_recipe,
            version_num=1,
            hash_snapshot="hash-snapshot-incompatible-cost",
            costo_total=Decimal("10.00"),
            fuente="POINT_PRODUCTION_REPORT",
        )
        line = LineaReceta.objects.create(
            receta=self.recipe,
            posicion=2,
            insumo=internal_input,
            insumo_texto=internal_input.nombre,
            cantidad=Decimal("2"),
            unidad=self.unit_kg,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("3.50"),
            match_status=LineaReceta.STATUS_AUTO,
        )

        resolved_cost, source = resolve_line_snapshot_cost(line)
        self.assertEqual(resolved_cost, Decimal("3.500000"))
        self.assertEqual(source, "POINT_PRODUCTION_REPORT_UNIDAD_INCOMPATIBLE_LINEA_SNAPSHOT")

    def test_projection_supply_explodes_internal_preparations_without_double_costing(self):
        unit_pz = UnidadMedida.objects.create(codigo="pz-prj", nombre="Pieza proyección", tipo=UnidadMedida.TIPO_PIEZA)
        unit_lt = UnidadMedida.objects.create(codigo="lt-prj", nombre="Litro proyección", tipo=UnidadMedida.TIPO_VOLUMEN)
        huevo = Insumo.objects.create(
            nombre="Huevo Proyección",
            nombre_normalizado="huevo proyeccion",
            unidad_base=unit_pz,
            tipo_item=Insumo.TIPO_MATERIA_PRIMA,
            categoria="Lácteos y huevo",
            proveedor_principal=self.provider,
        )
        aceite = Insumo.objects.create(
            nombre="Aceite Proyección",
            nombre_normalizado="aceite proyeccion",
            unidad_base=unit_lt,
            tipo_item=Insumo.TIPO_MATERIA_PRIMA,
            categoria="Aceites",
            proveedor_principal=self.provider,
        )
        caja = Insumo.objects.create(
            nombre="Caja Proyección",
            nombre_normalizado="caja proyeccion",
            unidad_base=unit_pz,
            tipo_item=Insumo.TIPO_EMPAQUE,
            categoria="Cajas",
            proveedor_principal=self.provider,
        )
        for insumo, costo, source_hash in [
            (huevo, "2.00", "projection-huevo-cost"),
            (aceite, "30.00", "projection-aceite-cost"),
            (caja, "1.00", "projection-caja-cost"),
        ]:
            CostoInsumo.objects.create(
                insumo=insumo,
                proveedor=self.provider,
                fecha=self.target_date,
                costo_unitario=Decimal(costo),
                source_hash=source_hash,
            )
        prep_recipe = Receta.objects.create(
            nombre="Betún Mantequilla Proyección",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("10"),
            rendimiento_unidad=self.unit_kg,
            familia="Betunes",
            categoria="Mantequilla",
            hash_contenido="hash-projection-betun-cost",
        )
        betun = Insumo.objects.create(
            codigo=f"DERIVADO:RECETA:{prep_recipe.id}:PREPARACION",
            nombre="Betún Mantequilla Proyección",
            nombre_normalizado="betun mantequilla proyeccion",
            unidad_base=self.unit_kg,
            tipo_item=Insumo.TIPO_INTERNO,
            categoria="Betunes",
        )
        RecetaCostoVersion.objects.create(
            receta=prep_recipe,
            version_num=1,
            hash_snapshot="hash-projection-betun-cost-v1",
            costo_total=Decimal("150.00"),
            fuente="POINT_PRODUCTION_REPORT",
        )
        LineaReceta.objects.create(
            receta=prep_recipe,
            posicion=1,
            insumo=huevo,
            insumo_texto=huevo.nombre,
            cantidad=Decimal("20"),
            unidad=unit_pz,
            unidad_texto="pz",
            match_status=LineaReceta.STATUS_AUTO,
        )
        LineaReceta.objects.create(
            receta=prep_recipe,
            posicion=2,
            insumo=aceite,
            insumo_texto=aceite.nombre,
            cantidad=Decimal("2"),
            unidad=unit_lt,
            unidad_texto="lt",
            match_status=LineaReceta.STATUS_AUTO,
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            posicion=2,
            insumo=betun,
            insumo_texto=betun.nombre,
            cantidad=Decimal("5"),
            unidad=self.unit_kg,
            unidad_texto="kg",
            match_status=LineaReceta.STATUS_AUTO,
        )
        LineaReceta.objects.create(
            receta=self.recipe,
            posicion=3,
            insumo=caja,
            insumo_texto=caja.nombre,
            cantidad=Decimal("2"),
            unidad=unit_pz,
            unidad_texto="pz",
            match_status=LineaReceta.STATUS_AUTO,
        )

        context = build_projection_supply_context(
            target_date=self.target_date,
            forecast_context={
                "target_label": "Forecast multinivel",
                "summary": {"forecast_units": Decimal("3")},
                "rows": [
                    {
                        "branch_id": self.branch.id,
                        "branch_code": self.branch.codigo,
                        "branch_name": self.branch.nombre,
                        "recipe_id": self.recipe.id,
                        "recipe_name": self.recipe.nombre,
                        "forecast_qty": Decimal("3"),
                        "buffer_units": Decimal("0"),
                    }
                ],
            },
        )

        prepared = next(row for row in context["prepared_insumos"] if row["insumo_id"] == betun.id)
        self.assertEqual(prepared["required_gross_qty"], Decimal("15.000"))
        self.assertEqual(prepared["unidad_codigo"], "kg-prj")
        self.assertEqual(prepared["family"], "Betunes")
        self.assertEqual(prepared["category"], "Mantequilla")

        purchase_by_name = {row["insumo_nombre"]: row for row in context["insumos"]}
        self.assertEqual(purchase_by_name["Huevo Proyección"]["required_gross_qty"], Decimal("30.000"))
        self.assertEqual(purchase_by_name["Aceite Proyección"]["required_gross_qty"], Decimal("3.000"))
        self.assertEqual(purchase_by_name["Caja Proyección"]["required_gross_qty"], Decimal("6.000"))
        self.assertNotIn("Betún Mantequilla Proyección", purchase_by_name)
        self.assertEqual(context["summary"]["estimated_spend"], Decimal("156.00"))
        self.assertEqual(context["summary"]["prepared_insumos"], 1)

    def test_preparation_resolver_prefers_point_code_over_stale_derived_id(self):
        wrong_prep = Receta.objects.create(
            nombre="Preparación Stale",
            codigo_point="STALE-PREP",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("1"),
            rendimiento_unidad=self.unit_kg,
            hash_contenido="hash-stale-prep",
        )
        correct_prep = Receta.objects.create(
            nombre="Pan Vainilla Dawn Mini Test",
            codigo_point="01VDMINI-TEST",
            tipo=Receta.TIPO_PREPARACION,
            rendimiento_cantidad=Decimal("1"),
            rendimiento_unidad=self.unit_kg,
            hash_contenido="hash-correct-point-prep",
        )
        stale_internal = Insumo.objects.create(
            codigo=f"DERIVADO:RECETA:{wrong_prep.id}:PREPARACION",
            codigo_point=correct_prep.codigo_point,
            nombre="Pan Vainilla Dawn Mini Test",
            nombre_normalizado="pan vainilla dawn mini test",
            unidad_base=self.unit_kg,
            tipo_item=Insumo.TIPO_INTERNO,
        )

        self.assertEqual(resolve_preparation_recipe_for_insumo(stale_internal), correct_prep)


class ProjectionSupplyViewTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(username="dg_projection", password="secret")
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user.groups.add(dg_group)
        self.client.force_login(self.user)
        target_date = date(2026, 4, 8)
        branch = Sucursal.objects.create(codigo="SUC-PRV", nombre="Sucursal Projection View")
        unit_piece = UnidadMedida.objects.create(codigo="pza-prv", nombre="Pieza projection", tipo=UnidadMedida.TIPO_PIEZA)
        provider = Proveedor.objects.create(nombre="Proveedor Projection View")
        insumo = Insumo.objects.create(
            nombre="Fresa Projection",
            nombre_normalizado="fresa projection",
            unidad_base=unit_piece,
            proveedor_principal=provider,
        )
        recipe = Receta.objects.create(
            nombre="Postre Projection",
            codigo_point="PRV001",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Postres",
            categoria="Evento",
            hash_contenido="hash-projection-view",
        )
        LineaReceta.objects.create(
            receta=recipe,
            posicion=1,
            insumo=insumo,
            insumo_texto="Fresa Projection",
            cantidad=Decimal("3"),
            unidad_texto="pza",
            unidad=unit_piece,
            match_status=LineaReceta.STATUS_AUTO,
        )
        for offset in range(56, 0, -1):
            current_day = target_date - timedelta(days=offset)
            FactVentaDiaria.objects.create(
                fecha=current_day,
                sucursal=branch,
                receta=recipe,
                producto_clave=recipe.codigo_point,
                producto_nombre=recipe.nombre,
                categoria=recipe.categoria,
                cantidad=Decimal("8"),
                tickets=4,
                venta_bruta=Decimal("800"),
                descuento=ZERO,
                venta_total=Decimal("800"),
                venta_neta=Decimal("800"),
                costo_estimado=Decimal("400"),
                margen=Decimal("400"),
                source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE,
            )
        self.target_date = target_date

    def test_view_exposes_projection_section_separately(self):
        response = self.client.get(reverse("reportes:production_orders"), {"fecha": self.target_date.isoformat()})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Planeación por proyección / evento")
        self.assertContains(response, "No descuenta stock actual")
        self.assertContains(response, "Fresa Projection")
