from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Sucursal
from control.models import MermaPOS
from inventario.models import ExistenciaInsumo
from inventario.models import MovimientoInventario
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import (
    PointBranch,
    PointDailySale,
    PointInventorySnapshot,
    PointProduct,
    PointProductionLine,
    PointSyncJob,
    PointTransferLine,
    PointWasteLine,
)
from recetas.models import (
    InventarioCedisProducto,
    LineaReceta,
    MovimientoProductoCedis,
    PlanProduccion,
    PlanProduccionItem,
    Receta,
    RecetaPresentacionDerivada,
    SolicitudReabastoCedis,
    VentaHistorica,
)
from recetas.utils.costeo_versionado import calcular_costeo_receta
from recetas.views import (
    _apply_plan_consumption,
    _export_periodo_mrp_csv,
    _plan_enterprise_board,
    _plan_explosion,
    _plan_status_dashboard,
    _product_upstream_snapshot,
)


class DerivedProductPresentationCostingTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_superuser(
            username="derived-cost-user",
            email="derived-cost-user@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.unit = UnidadMedida.objects.create(
            codigo="pza",
            nombre="Pieza",
            tipo=UnidadMedida.TIPO_PIEZA,
            factor_to_base=Decimal("1"),
        )
        self.parent_input = Insumo.objects.create(
            codigo="MP-PAN",
            nombre="Base pastel mediano",
            tipo_item=Insumo.TIPO_MATERIA_PRIMA,
            unidad_base=self.unit,
        )
        self.packaging = Insumo.objects.create(
            codigo="EMP-REB",
            nombre="Empaque rebanada",
            tipo_item=Insumo.TIPO_EMPAQUE,
            unidad_base=self.unit,
        )
        self.parent = Receta.objects.create(
            nombre="Pastel 3 Leches - Mediano",
            codigo_point="0105",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Mediano",
            hash_contenido="hash-parent-derived-cost",
        )
        LineaReceta.objects.create(
            receta=self.parent,
            posicion=1,
            insumo=self.parent_input,
            insumo_texto="Base pastel mediano",
            cantidad=Decimal("1"),
            unidad_texto="pza",
            unidad=self.unit,
            costo_unitario_snapshot=Decimal("80"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        self.derived = Receta.objects.create(
            nombre="Pastel 3 Leches - Rebanada",
            codigo_point="0106",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Rebanada",
            hash_contenido="hash-derived-slice-cost",
        )
        LineaReceta.objects.create(
            receta=self.derived,
            posicion=1,
            insumo=self.packaging,
            insumo_texto="Empaque rebanada",
            cantidad=Decimal("1"),
            unidad_texto="pza",
            unidad=self.unit,
            costo_unitario_snapshot=Decimal("2"),
            match_status=LineaReceta.STATUS_AUTO,
            match_method=LineaReceta.MATCH_EXACT,
        )
        self.relation = RecetaPresentacionDerivada.objects.create(
            receta_padre=self.parent,
            receta_derivada=self.derived,
            codigo_point_derivado="0106",
            nombre_derivado=self.derived.nombre,
            unidades_por_padre=Decimal("6"),
            padre_size_hint="MEDIANO",
            requiere_componentes_directos=True,
            notas="1 pastel mediano = 6 rebanadas",
        )

    def test_total_cost_uses_parent_proration_plus_direct_components(self):
        self.parent.refresh_from_db()
        self.derived.refresh_from_db()

        self.assertEqual(self.parent.costo_total_estimado_decimal, Decimal("80"))
        self.assertEqual(self.derived.costo_total_estimado_decimal, Decimal("15.33333333333333333333333333"))

        breakdown = calcular_costeo_receta(self.derived)

        self.assertIsNone(breakdown.driver)
        self.assertEqual(breakdown.costo_mp, Decimal("15.333333"))
        self.assertEqual(breakdown.costo_mo, Decimal("0.000000"))
        self.assertEqual(breakdown.costo_indirecto, Decimal("0.000000"))
        self.assertEqual(breakdown.snapshot_payload["costos"]["derived_parent_unit_cost"], "13.333333")
        self.assertEqual(breakdown.snapshot_payload["costos"]["direct_components_cost"], "2.000000")

    def test_upstream_snapshot_exposes_parent_even_without_internal_line(self):
        lineas = list(self.derived.lineas.select_related("insumo"))

        snapshot = _product_upstream_snapshot(lineas, receta=self.derived)

        self.assertEqual(snapshot["upstream_base_count"], 1)
        self.assertEqual(snapshot["internal_count"], 0)
        self.assertEqual(snapshot["empaque_count"], 1)
        self.assertEqual(snapshot["derived_parent_snapshot"]["parent_recipe_id"], self.parent.id)
        self.assertEqual(snapshot["derived_parent_snapshot"]["units_per_parent"], Decimal("6"))

    def test_receta_detail_uses_derived_total_and_breakdown(self):
        response = self.client.get(reverse("recetas:receta_detail", args=[self.derived.id]))

        self.assertEqual(response.status_code, 200)
        self.assertAlmostEqual(response.context["total_costo_directo"], 2.0, places=2)
        self.assertAlmostEqual(response.context["total_costo_estimado"], 15.333333333333334, places=6)
        self.assertEqual(
            response.context["derived_parent_snapshot"]["parent_recipe_id"],
            self.parent.id,
        )
        self.assertEqual(
            response.context["component_breakdown"][0]["key"],
            "parent_base_prorrated",
        )

    def test_plan_explosion_adds_parent_product_requirement_for_derived_recipe(self):
        ExistenciaInsumo.objects.create(insumo=self.packaging, stock_actual=Decimal("20"))
        InventarioCedisProducto.objects.create(
            receta=self.parent,
            stock_actual=Decimal("1"),
            stock_reservado=Decimal("0"),
        )
        plan = PlanProduccion.objects.create(nombre="Plan rebanadas", fecha_produccion=date(2026, 3, 18))
        PlanProduccionItem.objects.create(plan=plan, receta=self.derived, cantidad=Decimal("12"))

        explosion = _plan_explosion(plan)

        parent_row = next(row for row in explosion["insumos"] if row.get("is_derived_parent"))
        packaging_row = next(row for row in explosion["insumos"] if row["nombre"] == "Empaque rebanada")
        item_row = explosion["items_detalle"][0]

        self.assertEqual(parent_row["parent_recipe_id"], self.parent.id)
        self.assertEqual(parent_row["display_origen"], "Producto padre")
        self.assertEqual(parent_row["cantidad"], Decimal("2"))
        self.assertEqual(parent_row["stock_actual"], Decimal("1"))
        self.assertEqual(parent_row["faltante"], Decimal("1"))
        self.assertEqual(parent_row["workflow_health_label"], "Preparar padre")
        self.assertEqual(packaging_row["cantidad"], Decimal("12"))
        self.assertEqual(item_row["workflow_health_label"], "Preparar padre")
        self.assertEqual(explosion["alertas_capacidad"], 1)

    def test_mrp_form_shows_parent_product_requirement_for_derived_recipe(self):
        ExistenciaInsumo.objects.create(insumo=self.packaging, stock_actual=Decimal("20"))
        InventarioCedisProducto.objects.create(
            receta=self.parent,
            stock_actual=Decimal("0"),
            stock_reservado=Decimal("0"),
        )

        response = self.client.post(
            reverse("recetas:mrp_form"),
            {"receta_id": self.derived.id, "multiplicador": "6"},
        )

        self.assertEqual(response.status_code, 200)
        resultado = response.context["resultado"]
        parent_row = next(item for item in resultado["items"] if item.get("is_derived_parent"))

        self.assertEqual(parent_row["parent_recipe_id"], self.parent.id)
        self.assertEqual(parent_row["cantidad"], Decimal("1"))
        self.assertEqual(parent_row["workflow_health_label"], "Preparar padre")
        self.assertContains(response, "Producto padre")
        self.assertContains(response, "Consume")
        self.assertContains(response, self.parent.nombre)

    def test_plan_consumption_apply_creates_idempotent_ledger_movements(self):
        packaging_ex = ExistenciaInsumo.objects.create(insumo=self.packaging, stock_actual=Decimal("20"))
        parent_inventory = InventarioCedisProducto.objects.create(
            receta=self.parent,
            stock_actual=Decimal("3"),
            stock_reservado=Decimal("0"),
        )
        plan = PlanProduccion.objects.create(nombre="Plan aplicar consumo", fecha_produccion=date(2026, 3, 19))
        PlanProduccionItem.objects.create(plan=plan, receta=self.derived, cantidad=Decimal("12"))

        first_stats = _apply_plan_consumption(plan, self.user)
        packaging_ex.refresh_from_db()
        parent_inventory.refresh_from_db()
        self.assertEqual(packaging_ex.stock_actual, Decimal("8"))
        self.assertEqual(parent_inventory.stock_actual, Decimal("1"))
        self.assertEqual(MovimientoInventario.objects.filter(referencia=f"PLAN-PROD:{plan.id}").count(), 1)
        self.assertEqual(MovimientoProductoCedis.objects.filter(referencia=f"PLAN-PROD:{plan.id}").count(), 1)
        self.assertEqual(first_stats["insumos_created"], 1)
        self.assertEqual(first_stats["productos_created"], 1)
        plan.refresh_from_db()
        self.assertTrue(plan.consumo_aplicado)
        self.assertEqual(plan.estado, PlanProduccion.ESTADO_CONSUMO_APLICADO)
        self.assertEqual(plan.consumo_aplicado_por_id, self.user.id)
        self.assertIsNotNone(plan.consumo_aplicado_en)

        second_stats = _apply_plan_consumption(plan, self.user)
        packaging_ex.refresh_from_db()
        parent_inventory.refresh_from_db()
        self.assertEqual(packaging_ex.stock_actual, Decimal("8"))
        self.assertEqual(parent_inventory.stock_actual, Decimal("1"))
        self.assertEqual(MovimientoInventario.objects.filter(referencia=f"PLAN-PROD:{plan.id}").count(), 1)
        self.assertEqual(MovimientoProductoCedis.objects.filter(referencia=f"PLAN-PROD:{plan.id}").count(), 1)
        self.assertEqual(second_stats["insumos_skipped"], 1)
        self.assertEqual(second_stats["productos_skipped"], 1)

    def test_plan_detail_shows_simulation_and_applied_consumption_status(self):
        ExistenciaInsumo.objects.create(insumo=self.packaging, stock_actual=Decimal("20"))
        InventarioCedisProducto.objects.create(
            receta=self.parent,
            stock_actual=Decimal("3"),
            stock_reservado=Decimal("0"),
        )
        plan = PlanProduccion.objects.create(nombre="Plan visibilidad consumo", fecha_produccion=date(2026, 3, 19))
        PlanProduccionItem.objects.create(plan=plan, receta=self.derived, cantidad=Decimal("6"))

        response_before = self.client.get(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")
        self.assertEqual(response_before.status_code, 200)
        self.assertContains(response_before, "SOLO SIMULACIÓN")
        self.assertContains(response_before, "sigue en simulación")

        _apply_plan_consumption(plan, self.user)

        response_after = self.client.get(f"{reverse('recetas:plan_produccion')}?plan_id={plan.id}")
        self.assertEqual(response_after.status_code, 200)
        self.assertContains(response_after, "CONSUMO APLICADO")
        self.assertContains(response_after, "Aplicado")

    def test_plan_close_marks_formal_closed_state(self):
        ExistenciaInsumo.objects.create(insumo=self.packaging, stock_actual=Decimal("20"))
        InventarioCedisProducto.objects.create(
            receta=self.parent,
            stock_actual=Decimal("3"),
            stock_reservado=Decimal("0"),
        )
        plan = PlanProduccion.objects.create(nombre="Plan cierre formal", fecha_produccion=date(2026, 3, 19))
        PlanProduccionItem.objects.create(plan=plan, receta=self.derived, cantidad=Decimal("6"))

        _apply_plan_consumption(plan, self.user)
        plan.refresh_from_db()
        self.assertEqual(plan.estado, PlanProduccion.ESTADO_CONSUMO_APLICADO)

        with patch(
            "recetas.views._plan_document_control",
            return_value={"blocked_total": 0, "closure_summary": {"pending_count": 0}},
        ):
            response = self.client.post(reverse("recetas:plan_produccion_cerrar", args=[plan.id]))

        self.assertEqual(response.status_code, 302)
        plan.refresh_from_db()
        self.assertEqual(plan.estado, PlanProduccion.ESTADO_CERRADO)
        self.assertEqual(plan.cerrado_por_id, self.user.id)
        self.assertIsNotNone(plan.cerrado_en)

    def test_plan_list_filters_by_formal_status(self):
        borrador = PlanProduccion.objects.create(nombre="Plan borrador", fecha_produccion=date(2026, 3, 17))
        consumido = PlanProduccion.objects.create(
            nombre="Plan consumido",
            fecha_produccion=date(2026, 3, 18),
            estado=PlanProduccion.ESTADO_CONSUMO_APLICADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
        )
        cerrado = PlanProduccion.objects.create(
            nombre="Plan cerrado",
            fecha_produccion=date(2026, 3, 19),
            estado=PlanProduccion.ESTADO_CERRADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
            cerrado_en=self.parent.creado_en,
            cerrado_por=self.user,
        )

        response = self.client.get(f"{reverse('recetas:plan_produccion')}?estado_plan=cerrado&plan_id={cerrado.id}")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Plan cerrado")
        visible_plan_names = [plan.nombre for plan in response.context["planes"]]
        self.assertEqual(visible_plan_names, ["Plan cerrado"])
        self.assertEqual(response.context["selected_plan_status_filter"], "cerrado")
        self.assertEqual(response.context["plan_status_cards"][1]["count"], 1)
        self.assertEqual(response.context["plan_status_cards"][2]["count"], 1)
        self.assertEqual(response.context["plan_status_cards"][3]["count"], 1)

    def test_plan_export_includes_formal_state_fields(self):
        plan = PlanProduccion.objects.create(
            nombre="Plan exportable",
            fecha_produccion=date(2026, 3, 19),
            estado=PlanProduccion.ESTADO_CERRADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
            cerrado_en=self.parent.creado_en,
            cerrado_por=self.user,
        )
        PlanProduccionItem.objects.create(plan=plan, receta=self.derived, cantidad=Decimal("2"))

        response = self.client.get(f"{reverse('recetas:plan_produccion_export', args=[plan.id])}?format=csv")

        self.assertEqual(response.status_code, 200)
        payload = response.content.decode("utf-8")
        self.assertIn("Estado,Cerrado", payload)
        self.assertIn("Consumo aplicado,SI", payload)
        self.assertIn("Cerrado por,derived-cost-user", payload)

    def test_period_export_includes_plan_status_columns(self):
        plan = PlanProduccion.objects.create(
            nombre="Plan periodo cerrado",
            fecha_produccion=date(2026, 3, 19),
            estado=PlanProduccion.ESTADO_CERRADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
            cerrado_en=self.parent.creado_en,
            cerrado_por=self.user,
        )

        response = _export_periodo_mrp_csv(
            {
                "periodo": "2026-03",
                "periodo_tipo": "mes",
                "planes_count": 1,
                "insumos_count": 0,
                "costo_total": Decimal("0"),
                "alertas_capacidad": 0,
                "planes": [
                    {
                        "id": plan.id,
                        "nombre": plan.nombre,
                        "fecha_produccion": plan.fecha_produccion,
                        "estado": plan.estado,
                        "estado_label": plan.get_estado_display(),
                        "consumo_aplicado": plan.consumo_aplicado,
                        "items_count": 0,
                    }
                ],
                "insumos": [],
            }
        )

        payload = response.content.decode("utf-8")
        self.assertIn("Plan,Fecha producción,Estado,Consumo aplicado,Cerrado,Renglones", payload)
        self.assertIn("Plan periodo cerrado,2026-03-19,Cerrado,SI,SI,0", payload)

    def test_enterprise_board_exposes_formal_plan_status(self):
        plan = PlanProduccion.objects.create(
            nombre="Plan board estado",
            fecha_produccion=date(2026, 3, 19),
            estado=PlanProduccion.ESTADO_CONSUMO_APLICADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
        )
        board = _plan_enterprise_board(
            plan,
            {
                "lineas_sin_match": 0,
                "lineas_sin_cantidad": [],
                "lineas_sin_costo_unitario": [],
                "alertas_capacidad": 0,
                "insumos": [],
                "items_detalle": [],
            },
            None,
            None,
        )

        self.assertEqual(board["plan_status_label"], "Consumo aplicado")
        self.assertEqual(board["plan_status_tone"], "primary")

    def test_plan_status_dashboard_summarizes_open_and_closed_plans(self):
        PlanProduccion.objects.create(nombre="Plan dg borrador", fecha_produccion=date(2026, 3, 17))
        PlanProduccion.objects.create(
            nombre="Plan dg consumido",
            fecha_produccion=date(2026, 3, 18),
            estado=PlanProduccion.ESTADO_CONSUMO_APLICADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
        )
        PlanProduccion.objects.create(
            nombre="Plan dg cerrado",
            fecha_produccion=date(2026, 3, 19),
            estado=PlanProduccion.ESTADO_CERRADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
            cerrado_en=self.parent.creado_en,
            cerrado_por=self.user,
        )

        dashboard = _plan_status_dashboard(PlanProduccion.objects.all())

        self.assertEqual(dashboard["total"], 3)
        self.assertEqual(dashboard["abiertos"], 2)
        self.assertEqual(dashboard["borrador"], 1)
        self.assertEqual(dashboard["consumo_aplicado"], 1)
        self.assertEqual(dashboard["cerrados"], 1)
        self.assertEqual(dashboard["status"], "Pendientes de cierre")
        self.assertEqual(dashboard["group_by"], "day")
        self.assertEqual(dashboard["group_by_label"], "Fecha producción")
        self.assertEqual(dashboard["rows"][0]["group_date"], date(2026, 3, 19))

    def test_plan_status_dashboard_supports_weekly_grouping_and_date_range(self):
        PlanProduccion.objects.create(nombre="Plan semana 1", fecha_produccion=date(2026, 3, 2))
        PlanProduccion.objects.create(
            nombre="Plan semana 1 consumido",
            fecha_produccion=date(2026, 3, 4),
            estado=PlanProduccion.ESTADO_CONSUMO_APLICADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
        )
        PlanProduccion.objects.create(
            nombre="Plan semana 2 cerrado",
            fecha_produccion=date(2026, 3, 10),
            estado=PlanProduccion.ESTADO_CERRADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
            cerrado_en=self.parent.creado_en,
            cerrado_por=self.user,
        )
        PlanProduccion.objects.create(nombre="Plan fuera de rango", fecha_produccion=date(2026, 4, 2))

        dashboard = _plan_status_dashboard(
            PlanProduccion.objects.all(),
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            group_by="week",
            limit=10,
        )

        self.assertEqual(dashboard["total"], 3)
        self.assertEqual(dashboard["group_by"], "week")
        self.assertEqual(dashboard["group_by_label"], "Semana")
        self.assertEqual(dashboard["rows"][0]["label"], "Semana 2026-03-09")
        self.assertEqual(dashboard["rows"][0]["cerrado"], 1)
        self.assertEqual(dashboard["rows"][1]["label"], "Semana 2026-03-02")
        self.assertEqual(dashboard["rows"][1]["total"], 2)
        self.assertEqual(dashboard["rows"][1]["abiertos"], 2)

    def test_plan_page_exposes_dg_dashboard_context(self):
        PlanProduccion.objects.create(nombre="Plan dg abierto", fecha_produccion=date(2026, 3, 17))
        response = self.client.get(reverse("recetas:plan_produccion"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("plan_status_dashboard", response.context)
        self.assertContains(response, "Tablero DG de estados del plan")

    def test_plan_page_applies_dg_grouping_filters(self):
        PlanProduccion.objects.create(nombre="Plan marzo A", fecha_produccion=date(2026, 3, 5))
        PlanProduccion.objects.create(nombre="Plan marzo B", fecha_produccion=date(2026, 3, 18))
        PlanProduccion.objects.create(nombre="Plan abril", fecha_produccion=date(2026, 4, 1))

        response = self.client.get(
            reverse("recetas:plan_produccion"),
            {
                "dg_start_date": "2026-03-01",
                "dg_end_date": "2026-03-31",
                "dg_group_by": "month",
            },
        )

        self.assertEqual(response.status_code, 200)
        dashboard = response.context["plan_status_dashboard"]
        self.assertEqual(dashboard["group_by"], "month")
        self.assertEqual(dashboard["total"], 2)
        self.assertEqual(dashboard["rows"][0]["label"], "2026-03")
        self.assertContains(response, "Por mes")

    def test_plan_dg_dashboard_view_is_available_with_filtered_context(self):
        abierto = PlanProduccion.objects.create(nombre="Plan DG abierto", fecha_produccion=date(2026, 3, 17))
        cerrado = PlanProduccion.objects.create(
            nombre="Plan DG cerrado",
            fecha_produccion=date(2026, 3, 18),
            estado=PlanProduccion.ESTADO_CERRADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
            cerrado_en=self.parent.creado_en,
            cerrado_por=self.user,
        )
        PlanProduccion.objects.create(nombre="Plan DG abril", fecha_produccion=date(2026, 4, 1))

        response = self.client.get(
            reverse("recetas:plan_produccion_dg_dashboard"),
            {
                "dg_start_date": "2026-03-01",
                "dg_end_date": "2026-03-31",
                "dg_group_by": "week",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tablero DG independiente")
        self.assertEqual(response.context["plan_status_dashboard"]["total"], 2)
        self.assertEqual(response.context["plan_status_dashboard"]["group_by"], "week")
        self.assertEqual(list(response.context["planes_abiertos"]), [abierto])
        self.assertEqual(list(response.context["planes_cerrados"]), [cerrado])
        self.assertContains(response, f"{reverse('recetas:plan_produccion')}?plan_id={abierto.id}")

    def test_dg_operacion_dashboard_consolidates_plan_reabasto_and_sales(self):
        sucursal_a = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        sucursal_b = Sucursal.objects.create(codigo="LEYVA", nombre="Leyva", activa=True)
        PlanProduccion.objects.create(nombre="Plan DG marzo", fecha_produccion=date(2026, 3, 17))
        SolicitudReabastoCedis.objects.create(
            fecha_operacion=date(2026, 3, 20),
            sucursal=sucursal_a,
            estado=SolicitudReabastoCedis.ESTADO_ENVIADA,
            creado_por=self.user,
        )
        VentaHistorica.objects.create(
            fecha=date(2026, 3, 10),
            sucursal=sucursal_a,
            receta=self.parent,
            cantidad=Decimal("12"),
            tickets=2,
            monto_total=Decimal("480"),
            fuente="POINT_BRIDGE_SALES",
        )

        response = self.client.get(
            reverse("recetas:dg_operacion_dashboard"),
            {
                "fecha_operacion": "2026-03-20",
                "dg_start_date": "2026-03-01",
                "dg_end_date": "2026-03-31",
                "dg_group_by": "month",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Cockpit DG consolidado")
        self.assertContains(response, "Dirección General · Operación Integrada")
        self.assertContains(response, "Mermas Point del día operativo")
        self.assertContains(response, "Flujo central Point")
        self.assertEqual(response.context["plan_status_dashboard"]["total"], 1)
        self.assertEqual(response.context["resumen_cierre"]["total"], 2)
        self.assertEqual(
            response.context["resumen_cierre"]["en_tiempo"]
            + response.context["resumen_cierre"]["tardias"]
            + response.context["resumen_cierre"]["pendientes"],
            2,
        )
        self.assertEqual(response.context["ventas_historicas_summary"]["total_rows"], 1)

    def test_dg_operacion_dashboard_includes_point_closure_summary(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        point_branch = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=sucursal)
        point_product = PointProduct.objects.create(external_id="0100", sku="0100", name="Pastel Fresa")
        sync_job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)
        PointInventorySnapshot.objects.create(
            branch=point_branch,
            product=point_product,
            stock=Decimal("10"),
            captured_at=timezone.make_aware(datetime(2026, 3, 18, 23, 30)),
            sync_job=sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=point_branch,
            product=point_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 3, 20, 2, 30)),
            sync_job=sync_job,
        )
        PointDailySale.objects.create(
            branch=point_branch,
            product=point_product,
            sale_date=date(2026, 3, 19),
            source_endpoint="/Report/PrintReportes?idreporte=3",
            quantity=Decimal("1"),
            tickets=2,
            total_amount=Decimal("100"),
            net_amount=Decimal("100"),
        )
        MermaPOS.objects.create(
            sucursal=sucursal,
            fecha=date(2026, 3, 19),
            codigo_point="0100",
            producto_texto="Pastel Fresa",
            cantidad=Decimal("1"),
            fuente="POINT_BRIDGE_WASTE",
        )

        response = self.client.get(
            reverse("recetas:dg_operacion_dashboard"),
            {
                "fecha_operacion": "2026-03-20",
                "dg_start_date": "2026-03-01",
                "dg_end_date": "2026-03-31",
                "dg_group_by": "month",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Cuadre operativo Point")
        self.assertEqual(response.context["point_closure_summary"]["closure_date"], date(2026, 3, 19))
        self.assertEqual(response.context["point_closure_summary"]["branch_count"], 1)
        self.assertEqual(response.context["point_closure_summary"]["cuadra"], 1)
        self.assertEqual(response.context["point_closure_summary"]["rows"][0]["sold_tickets"], 2)
        self.assertEqual(response.context["point_closure_summary"]["rows"][0]["avg_ticket"], Decimal("50"))

    def test_dg_operacion_dashboard_excludes_inactive_preopening_branch(self):
        sucursal_activa = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        sucursal_inactiva = Sucursal.objects.create(codigo="GUAMUCHIL", nombre="Guamuchil", activa=False)
        point_branch_activa = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=sucursal_activa)
        point_branch_inactiva = PointBranch.objects.create(external_id="2", name="Guamuchil", erp_branch=sucursal_inactiva)
        point_product = PointProduct.objects.create(external_id="0100", sku="0100", name="Pastel Fresa")
        sync_job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)
        PointInventorySnapshot.objects.create(
            branch=point_branch_activa,
            product=point_product,
            stock=Decimal("10"),
            captured_at=timezone.make_aware(datetime(2026, 3, 18, 23, 30)),
            sync_job=sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=point_branch_activa,
            product=point_product,
            stock=Decimal("8"),
            captured_at=timezone.make_aware(datetime(2026, 3, 20, 2, 30)),
            sync_job=sync_job,
        )
        PointInventorySnapshot.objects.create(
            branch=point_branch_inactiva,
            product=point_product,
            stock=Decimal("5"),
            captured_at=timezone.make_aware(datetime(2026, 3, 20, 2, 30)),
            sync_job=sync_job,
        )
        PointDailySale.objects.create(
            branch=point_branch_activa,
            product=point_product,
            sale_date=date(2026, 3, 19),
            source_endpoint="/Report/PrintReportes?idreporte=3",
            quantity=Decimal("1"),
            total_amount=Decimal("100"),
            net_amount=Decimal("100"),
        )
        PointDailySale.objects.create(
            branch=point_branch_inactiva,
            product=point_product,
            sale_date=date(2026, 3, 19),
            source_endpoint="/Report/PrintReportes?idreporte=3",
            quantity=Decimal("1"),
            total_amount=Decimal("100"),
            net_amount=Decimal("100"),
        )

        response = self.client.get(
            reverse("recetas:dg_operacion_dashboard"),
            {
                "fecha_operacion": "2026-03-20",
                "dg_start_date": "2026-03-01",
                "dg_end_date": "2026-03-31",
                "dg_group_by": "month",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["point_closure_summary"]["branch_count"], 1)
        self.assertEqual(len(response.context["point_closure_summary"]["rows"]), 1)
        self.assertIn("MATRIZ", response.context["point_closure_summary"]["rows"][0]["branch_label"])

    def test_dg_operacion_dashboard_export_csv_contains_three_fronts(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        PlanProduccion.objects.create(nombre="Plan export cockpit", fecha_produccion=date(2026, 3, 17))
        SolicitudReabastoCedis.objects.create(
            fecha_operacion=date(2026, 3, 20),
            sucursal=sucursal,
            estado=SolicitudReabastoCedis.ESTADO_ENVIADA,
            creado_por=self.user,
        )
        VentaHistorica.objects.create(
            fecha=date(2026, 3, 10),
            sucursal=sucursal,
            receta=self.parent,
            cantidad=Decimal("8"),
            tickets=1,
            monto_total=Decimal("320"),
            fuente="POINT_BRIDGE_SALES",
        )

        response = self.client.get(
            reverse("recetas:dg_operacion_dashboard_export"),
            {
                "format": "csv",
                "fecha_operacion": "2026-03-20",
                "dg_start_date": "2026-03-01",
                "dg_end_date": "2026-03-31",
                "dg_group_by": "month",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.content.decode("utf-8")
        self.assertIn("COCKPIT DG OPERACION INTEGRADA", payload)
        self.assertIn("PLAN DE PRODUCCION", payload)
        self.assertIn("REABASTO CEDIS", payload)
        self.assertIn("VENTAS HISTORICAS", payload)
        self.assertIn("MERMAS POINT", payload)
        self.assertIn("FLUJO CENTRAL POINT", payload)
        self.assertIn("CUADRE OPERATIVO POINT", payload)
        self.assertIn("Ticket promedio", payload)
        self.assertIn("Agrupacion planes,Mes", payload)

    def test_generar_snapshot_dg_operacion_command_writes_json_snapshot(self):
        sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        PlanProduccion.objects.create(nombre="Plan snapshot", fecha_produccion=date(2026, 3, 17))
        SolicitudReabastoCedis.objects.create(
            fecha_operacion=date(2026, 3, 20),
            sucursal=sucursal,
            estado=SolicitudReabastoCedis.ESTADO_ENVIADA,
            creado_por=self.user,
        )
        VentaHistorica.objects.create(
            fecha=date(2026, 3, 10),
            sucursal=sucursal,
            receta=self.parent,
            cantidad=Decimal("5"),
            tickets=1,
            monto_total=Decimal("200"),
            fuente="POINT_BRIDGE_SALES",
        )

        with TemporaryDirectory() as tmpdir:
            call_command(
                "generar_snapshot_dg_operacion",
                "--format",
                "json",
                "--output-dir",
                tmpdir,
                "--fecha-operacion",
                "2026-03-20",
                "--dg-start-date",
                "2026-03-01",
                "--dg-end-date",
                "2026-03-31",
                "--dg-group-by",
                "month",
            )

            snapshots = sorted(Path(tmpdir).glob("dg_operacion_snapshot_*.json"))
            self.assertEqual(len(snapshots), 1)
            payload = snapshots[0].read_text(encoding="utf-8")
            self.assertIn('"fecha_operacion": "2026-03-20"', payload)
            self.assertIn('"reabasto_stage":', payload)
            self.assertIn('"status": "Pendientes de ejecucion"', payload)

    def test_produccion_cedis_weekly_dashboard_is_available(self):
        branch_cedis = Sucursal.objects.create(codigo="CEDIS", nombre="CEDIS", activa=True)
        branch_matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        point_branch_cedis = PointBranch.objects.create(external_id="8", name="CEDIS", erp_branch=branch_cedis)
        point_branch_matriz = PointBranch.objects.create(external_id="1", name="Matriz", erp_branch=branch_matriz)
        point_product = PointProduct.objects.create(external_id="0100", sku="0100", name="Pastel Fresa", category="Pastel Mediano")
        sync_job = PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_INVENTORY, status=PointSyncJob.STATUS_SUCCESS)
        receta_producto = Receta.objects.create(
            nombre="Pastel Fresa Mediano",
            codigo_point="0100",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia="Pastel",
            categoria="Pastel Mediano",
            hash_contenido="hash-dashboard-produccion-cedis",
        )
        PointDailySale.objects.create(
            branch=point_branch_matriz,
            product=point_product,
            sale_date=date(2026, 3, 18),
            source_endpoint="/Report/PrintReportes?idreporte=3",
            quantity=Decimal("12"),
            tickets=4,
            total_amount=Decimal("1200"),
            net_amount=Decimal("1200"),
        )
        PointProductionLine.objects.create(
            branch=point_branch_cedis,
            erp_branch=branch_cedis,
            receta=receta_producto,
            sync_job=sync_job,
            production_external_id="P1",
            detail_external_id="P1-1",
            source_hash="dashboard-prod-cedis-p1",
            production_date=date(2026, 3, 18),
            item_name="Pastel Fresa Mediano",
            item_code="0100",
            produced_quantity=Decimal("15"),
            requested_quantity=Decimal("15"),
            is_insumo=False,
        )
        PointWasteLine.objects.create(
            branch=point_branch_matriz,
            erp_branch=branch_matriz,
            sync_job=sync_job,
            movement_external_id="W1",
            source_hash="dashboard-prod-cedis-w1",
            movement_at=timezone.make_aware(datetime(2026, 3, 18, 14, 0)),
            item_name="Pastel Fresa Mediano",
            item_code="0100",
            quantity=Decimal("1"),
            total_cost=Decimal("95"),
        )
        PointTransferLine.objects.create(
            origin_branch=point_branch_cedis,
            destination_branch=point_branch_cedis,
            erp_origin_branch=branch_cedis,
            erp_destination_branch=branch_cedis,
            sync_job=sync_job,
            transfer_external_id="T1",
            detail_external_id="T1-1",
            source_hash="dashboard-prod-cedis-t1",
            registered_at=timezone.make_aware(datetime(2026, 3, 18, 10, 0)),
            received_at=timezone.make_aware(datetime(2026, 3, 18, 12, 0)),
            item_name="Pastel Fresa Mediano",
            item_code="0100",
            received_quantity=Decimal("3"),
            is_received=True,
        )

        response = self.client.get(reverse("recetas:produccion_cedis_weekly_dashboard"), {"week_of": "2026-03-18"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Dashboard · Producción CEDIS semanal")
        self.assertContains(response, "Producción CEDIS vs ventas, merma y pronóstico")
        self.assertEqual(response.context["week_label"], "2026-W12")
        self.assertEqual(response.context["production_units"], Decimal("15"))
        self.assertEqual(response.context["sales_units"], Decimal("12"))

    def test_plan_status_dashboard_export_csv_contains_summary_and_rows(self):
        PlanProduccion.objects.create(nombre="Plan dg borrador export", fecha_produccion=date(2026, 3, 17))
        PlanProduccion.objects.create(
            nombre="Plan dg cerrado export",
            fecha_produccion=date(2026, 3, 18),
            estado=PlanProduccion.ESTADO_CERRADO,
            consumo_aplicado=True,
            consumo_aplicado_en=self.parent.creado_en,
            consumo_aplicado_por=self.user,
            cerrado_en=self.parent.creado_en,
            cerrado_por=self.user,
        )

        response = self.client.get(f"{reverse('recetas:plan_produccion_estado_dashboard_export')}?format=csv")

        self.assertEqual(response.status_code, 200)
        payload = response.content.decode("utf-8")
        self.assertIn("TABLERO DG ESTADO DE PLANES", payload)
        self.assertIn("Fecha producción,Total,Borrador,Consumo aplicado,Cerrado,Abiertos", payload)

    def test_plan_status_dashboard_export_respects_range_and_grouping(self):
        PlanProduccion.objects.create(nombre="Plan marzo export", fecha_produccion=date(2026, 3, 17))
        PlanProduccion.objects.create(nombre="Plan abril export", fecha_produccion=date(2026, 4, 3))

        response = self.client.get(
            reverse("recetas:plan_produccion_estado_dashboard_export"),
            {
                "format": "csv",
                "dg_start_date": "2026-03-01",
                "dg_end_date": "2026-03-31",
                "dg_group_by": "month",
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.content.decode("utf-8")
        self.assertIn("Fecha inicio,2026-03-01", payload)
        self.assertIn("Fecha fin,2026-03-31", payload)
        self.assertIn("Agrupacion,Mes", payload)
        self.assertIn("Mes,Total,Borrador,Consumo aplicado,Cerrado,Abiertos", payload)
        self.assertIn("2026-03,1,1,0,0,1", payload)
        self.assertNotIn("2026-04", payload)
