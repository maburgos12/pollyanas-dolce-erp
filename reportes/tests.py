from django.contrib.auth.models import Group, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from datetime import timedelta
from decimal import Decimal

from maestros.models import CostoInsumo
from compras.models import OrdenCompra
from crm.models import Cliente, PedidoCliente
from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import Insumo, Proveedor, UnidadMedida
from pos_bridge.models import (
    PointBranch,
    PointDailyBranchIndicator,
    PointDailySale,
    PointMonthlySalesOfficial,
    PointProduct,
)
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta, VentaHistorica
from reportes.executive_panels import build_monthly_yoy_panel


class ReportesBITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="lectura_reportes", password="pass123")
        group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user.groups.add(group)
        self.client.login(username="lectura_reportes", password="pass123")

        cliente = Cliente.objects.create(nombre="Cliente BI")
        PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido BI", monto_estimado=1200)
        prov = Proveedor.objects.create(nombre="Proveedor BI")
        solicitud_insumo = None
        # Orden sin solicitud para no depender de más catálogos en este test.
        OrdenCompra.objects.create(proveedor=prov, monto_estimado=950, solicitud=solicitud_insumo)

    def test_bi_view_renders(self):
        sucursal = self._create_sucursal("BI-01", "Sucursal BI 01")
        receta = Receta.objects.create(nombre="Pastel BI Histórico", hash_contenido="hash-bi-historico-001")
        fecha_actual = timezone.localdate() - timedelta(days=1)
        fecha_comparable = fecha_actual - timedelta(days=7)
        point_branch = PointBranch.objects.create(external_id="BI-01", name="Sucursal BI 01", erp_branch=sucursal)
        point_product = PointProduct.objects.create(external_id="PBI-01", sku="BI001", name="Pastel BI Histórico", active=True)
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal,
            fecha=fecha_comparable,
            cantidad=Decimal("8"),
            monto_total=Decimal("800"),
            fuente="POINT_HIST_2026_Q1",
        )
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal,
            fecha=fecha_actual,
            cantidad=Decimal("10"),
            monto_total=Decimal("1000"),
            fuente="POINT_HIST_2026_Q1",
        )
        PointDailySale.objects.create(
            branch=point_branch,
            product=point_product,
            receta=receta,
            sale_date=fecha_actual,
            quantity=Decimal("10"),
            total_amount=Decimal("1000"),
            source_endpoint="/Report/PrintReportes?idreporte=3",
        )
        PointDailySale.objects.create(
            branch=point_branch,
            product=point_product,
            receta=receta,
            sale_date=fecha_comparable,
            quantity=Decimal("8"),
            total_amount=Decimal("800"),
            source_endpoint="/Report/PrintReportes?idreporte=3",
        )
        PointDailyBranchIndicator.objects.create(
            branch=point_branch,
            indicator_date=fecha_actual,
            total_amount=Decimal("1000"),
            total_tickets=4,
            total_avg_ticket=Decimal("250"),
        )
        PointDailyBranchIndicator.objects.create(
            branch=point_branch,
            indicator_date=fecha_comparable,
            total_amount=Decimal("800"),
            total_tickets=4,
            total_avg_ticket=Decimal("200"),
        )
        resp = self.client.get(reverse("reportes:bi"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "BI Ejecutivo")
        self.assertContains(resp, "Tablero ejecutivo del negocio")
        self.assertContains(resp, "Tendencia semanal de venta")
        self.assertContains(resp, "Mes contra mismo mes del año anterior")
        self.assertContains(resp, "Margen vs volumen por producto")
        self.assertContains(resp, "Producido contra vendido")
        self.assertContains(resp, "Flujo histórico mensual del centro")
        self.assertContains(resp, "Ticket promedio")
        self.assertContains(resp, "$1,000.00")
        self.assertContains(resp, "$250.00")
        self.assertContains(resp, "forecastTrendChart")
        self.assertContains(resp, "yoyMonthlyChart")
        self.assertContains(resp, "profitabilityScatterChart")
        self.assertContains(resp, "productionWeeklyChart")
        self.assertContains(resp, "productionCategoryChart")
        self.assertContains(resp, "centralFlowChart")
        self.assertNotContains(resp, "Ver control ERP del BI")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Entrega de reportes a downstream")
        self.assertNotContains(resp, "Ruta troncal ERP")
        self.assertNotContains(resp, "Mesa de gobierno ERP")
        self.assertNotContains(resp, "Ruta crítica ERP")
        self.assertNotContains(resp, "Radar ejecutivo ERP")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Madurez ERP de reportes")
        self.assertNotContains(resp, "Criterios de cierre ERP")
        self.assertIn("maturity_summary", resp.context)
        self.assertIn("daily_decision_rows", resp.context)
        self.assertIn("branch_weekday_rows", resp.context)
        self.assertIn("product_weekday_rows", resp.context)
        self.assertIn("handoff_map", resp.context)
        self.assertIn("release_gate_rows", resp.context)
        self.assertIn("release_gate_completion", resp.context)
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
        self.assertIn("erp_governance_rows", resp.context)
        self.assertIn("critical_path_rows", resp.context)
        self.assertIn("executive_radar_rows", resp.context)
        self.assertIn("erp_command_center", resp.context)
        self.assertIn("ventas_historicas_summary", resp.context)
        self.assertIn("forecast_panel", resp.context)
        self.assertIn("yoy_panel", resp.context)
        self.assertIn("profitability_panel", resp.context)
        self.assertIn("production_sales_panel", resp.context)
        self.assertIn("central_flow_panel", resp.context)
        self.assertIn("inventory_ledger_panel", resp.context)
        self.assertEqual(resp.context["ventas_historicas_summary"]["total_rows"], 2)

    def test_bi_shows_plan_supply_watchlist(self):
        sucursal = self._create_sucursal("BI-SUP-01", "Sucursal BI Supply")
        unidad = UnidadMedida.objects.create(
            codigo="kg-bi-supply",
            nombre="Kilogramo BI Supply",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        insumo = Insumo.objects.create(
            nombre="Chocolate BI Supply",
            unidad_base=unidad,
            tipo_item=Insumo.TIPO_MATERIA_PRIMA,
            activo=True,
        )
        receta = Receta.objects.create(
            nombre="Pastel BI Supply",
            hash_contenido="hash-bi-supply",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo=insumo,
            insumo_texto=insumo.nombre,
            unidad=unidad,
            unidad_texto="kg",
            cantidad=Decimal("2"),
            match_status=LineaReceta.STATUS_AUTO,
        )
        plan = PlanProduccion.objects.create(
            nombre="Plan BI Supply",
            fecha_produccion=timezone.localdate(),
        )
        PlanProduccionItem.objects.create(plan=plan, receta=receta, cantidad=Decimal("4"))
        ExistenciaInsumo.objects.create(insumo=insumo, stock_actual=Decimal("1"), punto_reorden=Decimal("2"))
        VentaHistorica.objects.create(
            receta=receta,
            sucursal=sucursal,
            fecha=timezone.localdate() - timedelta(days=2),
            cantidad=Decimal("25"),
            monto_total=Decimal("500"),
            fuente="BI_SUPPLY_TEST",
        )

        resp = self.client.get(reverse("reportes:bi"))

        self.assertEqual(resp.status_code, 200)
        self.assertIn("supply_watchlist", resp.context)
        self.assertTrue(resp.context["supply_watchlist"])
        self.assertEqual(resp.context["supply_watchlist"]["plan_nombre"], "Plan BI Supply")
        self.assertEqual(resp.context["supply_watchlist"]["rows"][0]["insumo_nombre"], "Chocolate BI Supply")

    def _create_sucursal(self, codigo: str, nombre: str):
        from core.models import Sucursal

        return Sucursal.objects.create(codigo=codigo, nombre=nombre, activa=True)

    def test_bi_exports(self):
        resp_csv = self.client.get(reverse("reportes:bi"), {"export": "csv"})
        self.assertEqual(resp_csv.status_code, 200)
        self.assertIn("text/csv", resp_csv["Content-Type"])

        resp_xlsx = self.client.get(reverse("reportes:bi"), {"export": "xlsx"})
        self.assertEqual(resp_xlsx.status_code, 200)
        self.assertIn("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resp_xlsx["Content-Type"])

    def test_requires_login(self):
        self.client.logout()
        resp = self.client.get(reverse("reportes:bi"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)

    def test_yoy_panel_uses_cached_official_period_for_partial_previous_year(self):
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2025, 10, 1).date(),
            month_end=timezone.datetime(2025, 10, 31).date(),
            total_quantity=Decimal("26971"),
            gross_amount=Decimal("3448997.00"),
            discount_amount=Decimal("3747.00"),
            total_amount=Decimal("3445250.00"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("3406462.00"),
        )
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2025, 11, 1).date(),
            month_end=timezone.datetime(2025, 11, 30).date(),
            total_quantity=Decimal("23292"),
            gross_amount=Decimal("3300000.00"),
            discount_amount=Decimal("53007.52"),
            total_amount=Decimal("3246992.48"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("3200000.00"),
        )
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2025, 12, 1).date(),
            month_end=timezone.datetime(2025, 12, 31).date(),
            total_quantity=Decimal("30202"),
            gross_amount=Decimal("4900000.00"),
            discount_amount=Decimal("75531.09"),
            total_amount=Decimal("4824468.91"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("4700000.00"),
        )
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2026, 1, 1).date(),
            month_end=timezone.datetime(2026, 1, 31).date(),
            total_quantity=Decimal("25644"),
            gross_amount=Decimal("3550000.00"),
            discount_amount=Decimal("30623.99"),
            total_amount=Decimal("3519376.01"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("3519376.01"),
        )
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2026, 2, 1).date(),
            month_end=timezone.datetime(2026, 2, 28).date(),
            total_quantity=Decimal("25786"),
            gross_amount=Decimal("3320000.00"),
            discount_amount=Decimal("31321.81"),
            total_amount=Decimal("3288678.19"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("3288678.19"),
        )
        PointMonthlySalesOfficial.objects.create(
            month_start=timezone.datetime(2025, 3, 1).date(),
            month_end=timezone.datetime(2025, 3, 31).date(),
            total_quantity=Decimal("30785"),
            gross_amount=Decimal("3900000.00"),
            discount_amount=Decimal("43259.29"),
            total_amount=Decimal("3856740.71"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("3810000.00"),
            raw_payload={
                "partial_ranges": {
                    "2025-03-01_2025-03-21": {
                        "period_start": "2025-03-01",
                        "period_end": "2025-03-21",
                        "total_quantity": "22000",
                        "total_amount": "3333333.33",
                        "gross_amount": "3400000.00",
                        "discount_amount": "50000.00",
                        "tax_amount": "0",
                        "net_amount": "3300000.00",
                    }
                }
            },
        )
        sucursal = self._create_sucursal("BI-PARTIAL", "Sucursal Parcial")
        point_branch = PointBranch.objects.create(external_id="BI-PARTIAL", name="Sucursal Parcial", erp_branch=sucursal)
        point_product = PointProduct.objects.create(external_id="PBI-PARTIAL", sku="BI-PARTIAL", name="Producto parcial", active=True)
        fecha_actual = timezone.datetime(2026, 3, 21).date()
        for idx in range(21):
            PointDailySale.objects.create(
                branch=point_branch,
                product=point_product,
                sale_date=fecha_actual.replace(day=1) + timedelta(days=idx),
                quantity=Decimal("10"),
                total_amount=Decimal("126829.35"),
                source_endpoint="/Report/PrintReportes?idreporte=3",
            )

        panel = build_monthly_yoy_panel(latest_date=fecha_actual, months=6)
        march_row = panel["rows"][-1]
        self.assertEqual(march_row["month_label"], "2026-03")
        self.assertEqual(march_row["prev_amount"], Decimal("3333333.33"))


class ReportesCanonicosTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="lectura_reportes_cat", password="pass123")
        group, _ = Group.objects.get_or_create(name="LECTURA")
        self.user.groups.add(group)
        self.client.login(username="lectura_reportes_cat", password="pass123")

    def test_consumo_agrupa_movimientos_de_variantes_en_canonico(self):
        unidad = UnidadMedida.objects.create(codigo="kg-rpt", nombre="Kg Reporte", tipo=UnidadMedida.TIPO_MASA)
        canonical = Insumo.objects.create(
            nombre="Harina Canonica Reporte",
            unidad_base=unidad,
            activo=True,
            codigo_point="RPT-CAN-001",
        )
        variant = Insumo.objects.create(
            nombre="HARINA CANONICA REPORTE",
            unidad_base=unidad,
            activo=True,
        )
        MovimientoInventario.objects.create(
            tipo=MovimientoInventario.TIPO_SALIDA,
            insumo=canonical,
            cantidad=Decimal("2"),
            referencia="RPT-1",
        )
        MovimientoInventario.objects.create(
            tipo=MovimientoInventario.TIPO_SALIDA,
            insumo=variant,
            cantidad=Decimal("3"),
            referencia="RPT-2",
        )

        response = self.client.get(reverse("reportes:consumo"))
        self.assertEqual(response.status_code, 200)
        rows = [row for row in response.context["rows"] if row["insumo__nombre"] == canonical.nombre]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["cantidad_total"], Decimal("5"))
        self.assertEqual(response.context["total_insumos"], 1)
        self.assertContains(response, "Maestro listo")
        self.assertContains(response, "Maestro ERP")
        self.assertContains(response, "Cadena documental ERP")
        self.assertContains(response, "Entrega de reportes a downstream")
        self.assertContains(response, "Ruta troncal ERP")
        self.assertContains(response, "Mesa de gobierno ERP")
        self.assertContains(response, "Ruta crítica ERP")
        self.assertContains(response, "Radar ejecutivo ERP")
        self.assertContains(response, "Centro de mando ERP")
        self.assertContains(response, "Madurez ERP de reportes")
        self.assertContains(response, "Criterios de cierre ERP")
        self.assertContains(response, "Cierre global")
        self.assertContains(response, "Cadena de control de reportes")
        self.assertContains(response, "Cierre por etapa documental")
        self.assertContains(response, "Responsable")
        self.assertContains(response, "Cierre")
        self.assertContains(response, "Salud operativa ERP")
        self.assertContains(response, "Depende de")
        self.assertContains(response, "Dependencia")
        self.assertIn("maturity_summary", response.context)
        self.assertIn("handoff_map", response.context)
        self.assertIn("release_gate_rows", response.context)
        self.assertIn("release_gate_completion", response.context)
        self.assertIn("dependency_status", response.context["enterprise_chain"][0])
        self.assertIn("owner", response.context["document_stage_rows"][0])
        self.assertIn("completion", response.context["document_stage_rows"][0])
        self.assertIn("erp_governance_rows", response.context)
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)
        self.assertIn("erp_command_center", response.context)

    def test_faltantes_agrupa_existencias_de_variantes_en_canonico(self):
        unidad = UnidadMedida.objects.create(codigo="pz-rpt", nombre="Pza Reporte", tipo=UnidadMedida.TIPO_PIEZA)
        canonical = Insumo.objects.create(
            nombre="Caja Canonica Reporte",
            unidad_base=unidad,
            activo=True,
            codigo_point="RPT-EX-001",
        )
        variant = Insumo.objects.create(
            nombre="CAJA CANONICA REPORTE",
            unidad_base=unidad,
            activo=True,
        )
        ExistenciaInsumo.objects.create(insumo=canonical, stock_actual=Decimal("4"), punto_reorden=Decimal("10"))
        ExistenciaInsumo.objects.create(insumo=variant, stock_actual=Decimal("3"), punto_reorden=Decimal("2"))

        response = self.client.get(reverse("reportes:faltantes"), {"nivel": "all"})
        self.assertEqual(response.status_code, 200)
        rows = [row for row in response.context["rows"] if row.insumo.nombre == canonical.nombre]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].stock_actual, Decimal("7"))
        self.assertContains(response, "Maestro bloqueado")
        self.assertContains(response, "Maestro ERP")
        self.assertContains(response, "Cadena documental ERP")
        self.assertContains(response, "Entrega de reportes a downstream")
        self.assertContains(response, "Ruta troncal ERP")
        self.assertContains(response, "Mesa de gobierno ERP")
        self.assertContains(response, "Ruta crítica ERP")
        self.assertContains(response, "Radar ejecutivo ERP")
        self.assertContains(response, "Centro de mando ERP")
        self.assertContains(response, "Madurez ERP de reportes")
        self.assertContains(response, "Criterios de cierre ERP")
        self.assertContains(response, "Cierre global")
        self.assertContains(response, "Cadena de control de reportes")
        self.assertContains(response, "Cierre por etapa documental")
        self.assertContains(response, "Responsable")
        self.assertContains(response, "Cierre")
        self.assertContains(response, "Salud operativa ERP")
        self.assertContains(response, "Depende de")
        self.assertContains(response, "Dependencia")
        self.assertIn("maturity_summary", response.context)
        self.assertIn("handoff_map", response.context)
        self.assertIn("release_gate_rows", response.context)
        self.assertIn("release_gate_completion", response.context)
        self.assertIn("dependency_status", response.context["enterprise_chain"][0])
        self.assertIn("owner", response.context["document_stage_rows"][0])
        self.assertIn("completion", response.context["document_stage_rows"][0])
        self.assertIn("erp_governance_rows", response.context)
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)
        self.assertIn("erp_command_center", response.context)

    def test_costo_receta_usa_costo_canonico_para_variante_historica(self):
        unidad = UnidadMedida.objects.create(codigo="kg-rpt-cost", nombre="Kg Reporte Costo", tipo=UnidadMedida.TIPO_MASA)
        canonical = Insumo.objects.create(
            nombre="Crema Canonica Reporte",
            unidad_base=unidad,
            activo=True,
            codigo_point="RPT-COST-001",
        )
        variant = Insumo.objects.create(
            nombre="CREMA CANONICA REPORTE",
            unidad_base=unidad,
            activo=True,
        )
        CostoInsumo.objects.create(insumo=canonical, costo_unitario=Decimal("42.50"))
        receta = Receta.objects.create(nombre="Receta Canonica Reporte", hash_contenido="hash-rpt-cost-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            tipo_linea=LineaReceta.TIPO_NORMAL,
            insumo=variant,
            insumo_texto=variant.nombre,
            cantidad=Decimal("2.000"),
            unidad_texto="kg",
            match_status=LineaReceta.STATUS_AUTO,
        )

        response = self.client.get(reverse("reportes:costo_receta"))
        self.assertEqual(response.status_code, 200)
        rows = [row for row in response.context["rows"] if row["receta"].id == receta.id]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["costo_total"], Decimal("85.00"))
        self.assertEqual(rows[0]["lineas_costeadas"], 1)
        self.assertContains(response, "Cockpit ERP de costeo")
        self.assertContains(response, "Maestro ERP")
        self.assertContains(response, "Precio sugerido")
        self.assertContains(response, "Cadena documental ERP")
        self.assertContains(response, "Entrega de reportes a downstream")
        self.assertContains(response, "Ruta troncal ERP")
        self.assertContains(response, "Mesa de gobierno ERP")
        self.assertContains(response, "Ruta crítica ERP")
        self.assertContains(response, "Radar ejecutivo ERP")
        self.assertContains(response, "Centro de mando ERP")
        self.assertContains(response, "Madurez ERP de reportes")
        self.assertContains(response, "Criterios de cierre ERP")
        self.assertContains(response, "Cierre global")
        self.assertContains(response, "Cadena de control de reportes")
        self.assertContains(response, "Cierre por etapa documental")
        self.assertContains(response, "Responsable")
        self.assertContains(response, "Cierre")
        self.assertContains(response, "Salud operativa ERP")
        self.assertContains(response, "Depende de")
        self.assertContains(response, "Dependencia")
        self.assertIn("maturity_summary", response.context)
        self.assertIn("handoff_map", response.context)
        self.assertIn("release_gate_rows", response.context)
        self.assertIn("release_gate_completion", response.context)
        self.assertIn("dependency_status", response.context["enterprise_chain"][0])
        self.assertIn("owner", response.context["document_stage_rows"][0])
        self.assertIn("completion", response.context["document_stage_rows"][0])
        self.assertIn("erp_governance_rows", response.context)
        self.assertIn("critical_path_rows", response.context)
        self.assertIn("executive_radar_rows", response.context)
        self.assertIn("erp_command_center", response.context)
