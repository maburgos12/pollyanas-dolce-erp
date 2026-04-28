from decimal import Decimal
from datetime import datetime

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from core.access import ROLE_ALMACEN, ROLE_LECTURA
from core.models import Sucursal
from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointBranch, PointTransferLine, PointWasteLine
from recetas.models import LineaReceta, Receta
from recetas.models import VentaHistorica

from .models import DevolucionSucursalMatriz, MermaMensualSucursal, MermaPOS, VentaPOS
from .services import build_discrepancias_report
from .services_mermas_devoluciones import MermaDevolucionAuditService


class ControlViewsTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user_lectura = user_model.objects.create_user(
            username="lectura_control_view",
            email="lectura_control_view@example.com",
            password="test12345",
        )
        group_lectura, _ = Group.objects.get_or_create(name=ROLE_LECTURA)
        self.user_lectura.groups.add(group_lectura)

        self.user_almacen = user_model.objects.create_user(
            username="almacen_control_view",
            email="almacen_control_view@example.com",
            password="test12345",
        )
        group_almacen, _ = Group.objects.get_or_create(name=ROLE_ALMACEN)
        self.user_almacen.groups.add(group_almacen)

        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        insumo = Insumo.objects.create(nombre="Insumo control view", unidad_base=unidad, activo=True)
        ExistenciaInsumo.objects.create(insumo=insumo, stock_actual=Decimal("5.000"))
        self.sucursal = Sucursal.objects.create(codigo="MTRZ", nombre="Matriz", activa=True)
        self.receta = Receta.objects.create(
            nombre="Pastel prueba control",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="control-tests-hash-1",
        )

    def test_discrepancias_view_loads(self):
        self.client.force_login(self.user_lectura)
        resp = self.client.get(reverse("control:discrepancias"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Control · Discrepancias")
        self.assertContains(resp, "Resumen del corte")
        self.assertContains(resp, "Foco operativo")
        self.assertContains(resp, "Semáforo")
        self.assertContains(resp, "Filtro de corte")
        self.assertContains(resp, "Discrepancias del corte")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Cadena troncal de control")
        self.assertNotContains(resp, "Ruta crítica ERP")
        self.assertNotContains(resp, "Radar ejecutivo ERP")
        self.assertNotContains(resp, "Mesa de gobierno ERP")
        self.assertNotContains(resp, "Madurez ERP de control")
        self.assertNotContains(resp, "Criterios de cierre ERP")
        self.assertNotContains(resp, "Cadena de control operativo")
        self.assertNotContains(resp, "Entrega de control a downstream")
        self.assertNotContains(resp, "Cierre por etapa documental")
        self.assertNotContains(resp, "Salud operativa ERP")
        self.assertIn("focus_cards", resp.context)
        self.assertIn("focus_summary", resp.context)
        self.assertEqual(resp.context["enterprise_focus"], "")

    def test_discrepancias_can_focus_enterprise_subset(self):
        self.client.force_login(self.user_lectura)
        unidad = UnidadMedida.objects.create(
            codigo="kg-focus-ctrl",
            nombre="Kilogramo Focus Control",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        insumo = Insumo.objects.create(nombre="Insumo foco control", unidad_base=unidad, activo=True)
        ExistenciaInsumo.objects.filter(insumo__nombre="Insumo control view").delete()
        ExistenciaInsumo.objects.create(insumo=insumo, stock_actual=Decimal("1.000"))
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=insumo,
            insumo_texto=insumo.nombre,
            cantidad=Decimal("3"),
            unidad=unidad,
            unidad_texto=unidad.codigo,
            match_status=LineaReceta.STATUS_AUTO,
        )
        VentaPOS.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=timezone.localdate(),
            cantidad=Decimal("1"),
            tickets=1,
            monto_total=Decimal("10"),
            fuente="CAPTURA_MOVIL",
        )
        resp = self.client.get(reverse("control:discrepancias"), {"enterprise_focus": "ALERTAS"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "ALERTAS")
        self.assertEqual(resp.context["focus_summary"]["title"], "Alertas abiertas")

    def test_captura_movil_forbidden_for_lectura(self):
        self.client.force_login(self.user_lectura)
        resp = self.client.get(reverse("control:captura_movil"))
        self.assertEqual(resp.status_code, 403)

    def test_captura_movil_creates_venta(self):
        self.client.force_login(self.user_almacen)
        resp = self.client.post(
            reverse("control:captura_movil"),
            {
                "capture_type": "venta",
                "fecha": "2026-02-21",
                "sucursal_id": str(self.sucursal.id),
                "receta_id": str(self.receta.id),
                "cantidad": "5",
                "tickets": "2",
                "monto_total": "550.40",
            },
            follow=True,
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(VentaPOS.objects.count(), 1)
        venta = VentaPOS.objects.first()
        self.assertEqual(venta.receta_id, self.receta.id)
        self.assertEqual(venta.sucursal_id, self.sucursal.id)
        self.assertEqual(venta.fuente, "CAPTURA_MOVIL")
        self.assertContains(resp, "Centro de mando ERP")
        self.assertContains(resp, "Cadena documental ERP")
        self.assertContains(resp, "Cadena troncal de control")
        self.assertContains(resp, "Ruta crítica ERP")
        self.assertContains(resp, "Mesa de gobierno ERP")
        self.assertContains(resp, "Depende de")
        self.assertContains(resp, "Dependencia")
        self.assertContains(resp, "Madurez ERP de control")
        self.assertContains(resp, "Criterios de cierre ERP")
        self.assertContains(resp, "Cierre global")
        self.assertContains(resp, "Cadena de control operativo")
        self.assertContains(resp, "Entrega de control a downstream")
        self.assertContains(resp, "Cierre por etapa documental")
        self.assertContains(resp, "Responsable")
        self.assertContains(resp, "Cierre")
        self.assertContains(resp, "Salud operativa ERP")
        self.assertIn("enterprise_chain", resp.context)
        self.assertIn("erp_command_center", resp.context)
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertIn("release_gate_rows", resp.context)

        self.assertIn("release_gate_completion", resp.context)
        self.assertIn("critical_path_rows", resp.context)
        self.assertIn("maturity_summary", resp.context)
        self.assertIn("handoff_map", resp.context)
        self.assertIn("owner", resp.context["handoff_map"][0])
        self.assertIn("depends_on", resp.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp.context["handoff_map"][0])
        self.assertIn("next_step", resp.context["handoff_map"][0])
        self.assertIn("completion", resp.context["handoff_map"][0])
        self.assertIn("document_stage_rows", resp.context)
        self.assertIn("erp_governance_rows", resp.context)
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
        self.assertTrue(resp.context["operational_health_cards"])

    def test_captura_movil_creates_merma(self):
        self.client.force_login(self.user_almacen)
        resp = self.client.post(
            reverse("control:captura_movil"),
            {
                "capture_type": "merma",
                "fecha": "2026-02-21",
                "producto_texto": "Cheesecake Lotus individual",
                "cantidad": "1.5",
                "motivo": "Producto dañado en traslado",
            },
            follow=True,
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(MermaPOS.objects.count(), 1)
        merma = MermaPOS.objects.first()
        self.assertEqual(merma.producto_texto, "Cheesecake Lotus individual")
        self.assertEqual(merma.fuente, "CAPTURA_MOVIL")
        self.assertEqual(merma.responsable_texto, "")
        self.assertContains(resp, "Centro de mando ERP")
        self.assertContains(resp, "Cadena documental ERP")
        self.assertContains(resp, "Cadena troncal de control")
        self.assertContains(resp, "Radar ejecutivo ERP")
        self.assertContains(resp, "Mesa de gobierno ERP")
        self.assertContains(resp, "Depende de")
        self.assertContains(resp, "Dependencia")
        self.assertContains(resp, "Madurez ERP de control")
        self.assertContains(resp, "Criterios de cierre ERP")
        self.assertContains(resp, "Cierre global")
        self.assertContains(resp, "Cadena de control operativo")
        self.assertContains(resp, "Entrega de control a downstream")
        self.assertContains(resp, "Cierre por etapa documental")
        self.assertContains(resp, "Responsable")
        self.assertContains(resp, "Cierre")
        self.assertContains(resp, "Salud operativa ERP")
        self.assertIn("enterprise_chain", resp.context)
        self.assertIn("erp_command_center", resp.context)
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertIn("release_gate_rows", resp.context)
        self.assertIn("release_gate_completion", resp.context)
        self.assertIn("maturity_summary", resp.context)
        self.assertIn("handoff_map", resp.context)
        self.assertIn("owner", resp.context["handoff_map"][0])
        self.assertIn("depends_on", resp.context["handoff_map"][0])
        self.assertIn("exit_criteria", resp.context["handoff_map"][0])
        self.assertIn("next_step", resp.context["handoff_map"][0])
        self.assertIn("completion", resp.context["handoff_map"][0])
        self.assertIn("document_stage_rows", resp.context)
        self.assertIn("erp_governance_rows", resp.context)
        self.assertIn("owner", resp.context["document_stage_rows"][0])
        self.assertIn("completion", resp.context["document_stage_rows"][0])
        self.assertTrue(resp.context["operational_health_cards"])

    def test_discrepancias_report_agrupa_variantes_en_canonico(self):
        unidad = UnidadMedida.objects.create(
            codigo="kg-can-ctrl",
            nombre="Kilogramo Canon Control",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        canonical = Insumo.objects.create(
            nombre="Harina Canonica Control",
            unidad_base=unidad,
            activo=True,
            codigo_point="CTRL-CAN-001",
        )
        variant = Insumo.objects.create(
            nombre="HARINA CANONICA CONTROL",
            unidad_base=unidad,
            activo=True,
        )
        receta = Receta.objects.create(
            nombre="Receta canon control",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="ctrl-canon-hash-002",
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=variant,
            insumo_texto=variant.nombre,
            cantidad=Decimal("2"),
            unidad=unidad,
            unidad_texto=unidad.codigo,
            costo_unitario_snapshot=Decimal("1"),
            match_status=LineaReceta.STATUS_AUTO,
        )
        ExistenciaInsumo.objects.create(insumo=canonical, stock_actual=Decimal("4"))
        ExistenciaInsumo.objects.create(insumo=variant, stock_actual=Decimal("6"))
        VentaPOS.objects.create(
            fecha=timezone.localdate(),
            sucursal=self.sucursal,
            receta=receta,
            cantidad=Decimal("3"),
            tickets=1,
            monto_total=Decimal("300"),
        )

        payload = build_discrepancias_report(
            date_from=timezone.localdate(),
            date_to=timezone.localdate(),
            sucursal_id=self.sucursal.id,
            threshold_pct=Decimal("0"),
            top=50,
        )
        rows = [row for row in payload["rows"] if row["insumo"] == canonical.nombre]
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["ventas_pos"], 6.0, places=2)
        self.assertAlmostEqual(rows[0]["inventario_real"], 10.0, places=2)


class MermaDevolucionAuditServiceTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        self.branch = PointBranch.objects.create(external_id="1", name="MATRIZ", erp_branch=self.sucursal)
        self.store = Sucursal.objects.create(codigo="CRUCERO", nombre="Crucero", activa=True)
        self.origin = PointBranch.objects.create(external_id="12", name="CRUCERO", erp_branch=self.store)
        self.devoluciones = PointBranch.objects.create(external_id="98", name="DEVOLUCIONES")
        self.receta = Receta.objects.create(
            nombre="Pastel merma test",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            codigo_point="PMT",
            hash_contenido="hash-pastel-merma-test",
        )
        self.service = MermaDevolucionAuditService()

    def test_consolidar_mermas_groups_cost_and_sales_pct(self):
        PointWasteLine.objects.create(
            branch=self.branch,
            erp_branch=self.sucursal,
            receta=self.receta,
            movement_external_id="waste-test-1",
            source_hash="waste-test-hash-1",
            movement_at=timezone.make_aware(datetime(2026, 4, 10, 10, 0, 0)),
            item_name=self.receta.nombre,
            item_code=self.receta.codigo_point,
            quantity=Decimal("2"),
            unit_cost=Decimal("10"),
            total_cost=Decimal("20"),
            justification="Vencimiento",
        )
        VentaHistorica.objects.create(
            receta=self.receta,
            sucursal=self.sucursal,
            fecha=datetime(2026, 4, 10).date(),
            cantidad=Decimal("100"),
            fuente="POINT_BRIDGE_SALES",
        )

        dry = self.service.consolidar_mermas(period="2026-04", dry_run=True)
        persisted = self.service.consolidar_mermas(period="2026-04", dry_run=False)

        self.assertEqual(dry.grouped_rows, 1)
        self.assertEqual(dry.total_cost, Decimal("20.00"))
        self.assertEqual(persisted.created, 1)
        row = MermaMensualSucursal.objects.get()
        self.assertEqual(row.unidades_merma, Decimal("2"))
        self.assertEqual(row.costo_merma, Decimal("20.00"))
        self.assertEqual(row.unidades_vendidas, Decimal("100"))
        self.assertEqual(row.pct_merma_sobre_venta, Decimal("2.00"))

    def test_consolidar_mermas_homologates_missing_erp_branch_by_name(self):
        point_branch = PointBranch.objects.create(external_id="13", name="Crucero")
        PointWasteLine.objects.create(
            branch=point_branch,
            receta=self.receta,
            movement_external_id="waste-test-branch-match",
            source_hash="waste-test-branch-match-hash",
            movement_at=timezone.make_aware(datetime(2026, 4, 10, 10, 0, 0)),
            item_name=self.receta.nombre,
            item_code=self.receta.codigo_point,
            quantity=Decimal("2"),
            unit_cost=Decimal("10"),
            total_cost=Decimal("20"),
            justification="Vencimiento",
        )

        result = self.service.consolidar_mermas(period="2026-04", dry_run=False)

        self.assertEqual(result.branch_homologated_rows, 1)
        row = MermaMensualSucursal.objects.get()
        self.assertEqual(row.sucursal, self.store)
        self.assertEqual(row.metadata["branch_point"], "Crucero")
        self.assertEqual(row.metadata["branch_resolution"], "NAME_MATCH")

    def test_consolidar_mermas_keeps_unresolved_branch_instead_of_skipping(self):
        point_branch = PointBranch.objects.create(external_id="14", name="Sucursal fantasma")
        PointWasteLine.objects.create(
            branch=point_branch,
            receta=self.receta,
            movement_external_id="waste-test-unresolved-branch",
            source_hash="waste-test-unresolved-branch-hash",
            movement_at=timezone.make_aware(datetime(2026, 4, 10, 10, 0, 0)),
            item_name=self.receta.nombre,
            item_code=self.receta.codigo_point,
            quantity=Decimal("2"),
            unit_cost=Decimal("10"),
            total_cost=Decimal("20"),
            justification="Vencimiento",
        )

        result = self.service.consolidar_mermas(period="2026-04", dry_run=False)

        self.assertEqual(result.unresolved_branch_rows, 1)
        self.assertEqual(result.skipped, 0)
        row = MermaMensualSucursal.objects.get()
        self.assertIsNone(row.sucursal)
        self.assertEqual(row.metadata["branch_point"], "Sucursal fantasma")
        self.assertEqual(row.metadata["branch_resolution"], "UNRESOLVED")

    def test_clasificar_devoluciones_reads_transfer_without_modifying_source(self):
        transfer = PointTransferLine.objects.create(
            origin_branch=self.origin,
            destination_branch=self.devoluciones,
            erp_origin_branch=self.store,
            receta=self.receta,
            transfer_external_id="transfer-test-1",
            detail_external_id="transfer-detail-test-1",
            source_hash="transfer-hash-test-1",
            registered_at=timezone.make_aware(datetime(2026, 4, 12, 12, 0, 0)),
            item_name=self.receta.nombre,
            item_code=self.receta.codigo_point,
            unit_cost=Decimal("15"),
            requested_quantity=Decimal("3"),
            sent_quantity=Decimal("3"),
            received_quantity=Decimal("3"),
        )

        dry = self.service.clasificar_devoluciones(period="2026-04", dry_run=True)
        persisted = self.service.clasificar_devoluciones(period="2026-04", dry_run=False)

        self.assertEqual(dry.source_rows, 1)
        self.assertEqual(dry.total_cost, Decimal("45.00"))
        self.assertEqual(persisted.created, 1)
        row = DevolucionSucursalMatriz.objects.get()
        self.assertEqual(row.transfer_line, transfer)
        self.assertEqual(row.unidades, Decimal("3"))
        self.assertEqual(row.costo_estimado, Decimal("45.00"))
        self.assertEqual(row.motivo, DevolucionSucursalMatriz.MOTIVO_VIDA_UTIL)
        self.assertFalse(row.metadata["es_perdida"])
        self.assertEqual(row.metadata["flujo"], "SUCURSAL_DEVOLUCIONES")

    def test_clasificar_devoluciones_ignores_devoluciones_to_matriz_recovery(self):
        PointTransferLine.objects.create(
            origin_branch=self.devoluciones,
            destination_branch=self.branch,
            erp_destination_branch=self.sucursal,
            receta=self.receta,
            transfer_external_id="transfer-test-2",
            detail_external_id="transfer-detail-test-2",
            source_hash="transfer-hash-test-2",
            registered_at=timezone.make_aware(datetime(2026, 4, 12, 12, 0, 0)),
            item_name=self.receta.nombre,
            item_code=self.receta.codigo_point,
            unit_cost=Decimal("15"),
            requested_quantity=Decimal("3"),
            sent_quantity=Decimal("3"),
            received_quantity=Decimal("3"),
        )

        result = self.service.clasificar_devoluciones(period="2026-04", dry_run=True)

        self.assertEqual(result.source_rows, 0)
        self.assertEqual(result.total_cost, Decimal("0.00"))
