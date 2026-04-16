from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from io import StringIO
from tempfile import NamedTemporaryFile

from django.core.management import call_command
from django.test import TestCase
from django.test.utils import override_settings
from django.utils import timezone
from openpyxl import Workbook

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointDailySale, PointInventorySnapshot, PointProduct, PointSyncJob, PointWasteLine
from pos_bridge.services.product_month_closure_service import ProductMonthClosureError, ProductMonthClosureService
from recetas.models import (
    ProductoMonthClosure,
    ProductoMonthClosureLine,
    Receta,
    RecetaPresentacionDerivada,
    VentaHistorica,
)
from pos_bridge.models.movements import PointProductionLine


@override_settings(PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE="BRIDGE_HISTORY")
class ProductMonthClosureServiceTests(TestCase):
    def setUp(self):
        self.service = ProductMonthClosureService()
        self.sucursal = Sucursal.objects.create(codigo="CEDIS", nombre="CEDIS")
        self.point_branch = PointBranch.objects.create(
            external_id="CEDIS",
            name="CEDIS",
            erp_branch=self.sucursal,
        )
        self.sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
        )
        self.parent = Receta.objects.create(
            nombre="Pastel de Snickers Mediano",
            codigo_point="SNK-M",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-parent-snk-mediano",
        )
        self.derived = Receta.objects.create(
            nombre="Pastel de Snickers Rebanada",
            codigo_point="SNK-R",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-derived-snk-rebanada",
        )
        self.derived_relation = RecetaPresentacionDerivada.objects.create(
            receta_padre=self.parent,
            receta_derivada=self.derived,
            codigo_point_derivado="SNK-R",
            nombre_derivado=self.derived.nombre,
            unidades_por_padre=Decimal("10"),
        )

    def test_build_uses_previous_closure_and_rolls_slice_sales_and_waste_to_parent(self):
        previous = ProductoMonthClosure.objects.create(
            month_start=date(2025, 8, 1),
            month_end=date(2025, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
            opening_reference_date=date(2025, 7, 31),
            built_at=timezone.now(),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=self.parent,
            inventario_final_teorico=Decimal("20"),
        )

        PointProductionLine.objects.create(
            branch=self.point_branch,
            erp_branch=self.sucursal,
            receta=self.parent,
            production_external_id="prod-1",
            detail_external_id="detail-1",
            source_hash="prod-hash-1",
            production_date=date(2025, 9, 5),
            item_name=self.parent.nombre,
            item_code=self.parent.codigo_point,
            produced_quantity=Decimal("15"),
        )
        VentaHistorica.objects.create(
            receta=self.parent,
            sucursal=self.sucursal,
            fecha=date(2025, 9, 10),
            cantidad=Decimal("5"),
            fuente="POINT_BRIDGE_SALES",
        )
        VentaHistorica.objects.create(
            receta=self.derived,
            sucursal=self.sucursal,
            fecha=date(2025, 9, 11),
            cantidad=Decimal("20"),
            fuente="POINT_BRIDGE_SALES",
        )
        PointWasteLine.objects.create(
            branch=self.point_branch,
            erp_branch=self.sucursal,
            receta=self.derived,
            sync_job=self.sync_job,
            movement_external_id="waste-1",
            source_hash="waste-hash-1",
            movement_at=timezone.make_aware(datetime(2025, 9, 12, 10, 0, 0), timezone.get_current_timezone()),
            item_name=self.derived.nombre,
            quantity=Decimal("5"),
        )

        closure = self.service.build(month="2025-09")

        self.assertEqual(closure.opening_source, ProductoMonthClosure.OPENING_SOURCE_PREVIOUS_CLOSURE)
        line = closure.lines.get(receta_padre=self.parent)
        self.assertEqual(line.inventario_inicial_teorico, Decimal("20"))
        self.assertEqual(line.produccion_mes, Decimal("15"))
        self.assertEqual(line.venta_directa_enteros, Decimal("5"))
        self.assertEqual(line.venta_derivada_equivalente, Decimal("2"))
        self.assertEqual(line.merma_derivada_equivalente, Decimal("0.5"))
        self.assertEqual(line.inventario_final_teorico, Decimal("27.5"))

    def test_build_uses_snapshot_opening_when_previous_closure_missing(self):
        point_parent = PointProduct.objects.create(external_id="point-parent", sku="SNK-M", name=self.parent.nombre)
        point_derived = PointProduct.objects.create(external_id="point-derived", sku="SNK-R", name=self.derived.nombre)

        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=point_parent,
            stock=Decimal("2"),
            sync_job=self.sync_job,
            captured_at=timezone.make_aware(datetime(2025, 8, 31, 23, 0, 0), timezone.get_current_timezone()),
        )
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=point_derived,
            stock=Decimal("5"),
            sync_job=self.sync_job,
            captured_at=timezone.make_aware(datetime(2025, 8, 31, 23, 0, 0), timezone.get_current_timezone()),
        )

        closure = self.service.build(month="2025-09")

        self.assertEqual(closure.opening_source, ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT)
        line = closure.lines.get(receta_padre=self.parent)
        self.assertEqual(line.inventario_inicial_teorico, Decimal("2.5"))
        self.assertEqual(line.source_snapshot_count, 2)
        self.assertEqual(line.inventario_final_teorico, Decimal("2.5"))

    def test_build_rejects_locked_closure_rebuild(self):
        ProductoMonthClosure.objects.create(
            month_start=date(2025, 9, 1),
            month_end=date(2025, 9, 30),
            status=ProductoMonthClosure.STATUS_LOCKED,
            is_locked=True,
        )
        with self.assertRaisesMessage(ProductMonthClosureError, "bloqueado"):
            self.service.build(month="2025-09", rebuild=True)

    def test_lock_marks_built_closure_as_locked_with_audit_metadata(self):
        closure = ProductoMonthClosure.objects.create(
            month_start=date(2025, 9, 1),
            month_end=date(2025, 9, 30),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
            opening_reference_date=date(2025, 8, 31),
        )
        ProductoMonthClosureLine.objects.create(
            closure=closure,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("5"),
            inventario_final_teorico=Decimal("5"),
        )

        locked = self.service.lock(closure=closure)

        self.assertTrue(locked.is_locked)
        self.assertEqual(locked.status, ProductoMonthClosure.STATUS_LOCKED)
        self.assertIn("lock_event", locked.metadata)
        self.assertEqual(locked.metadata["lock_event"]["line_count"], 1)

    def test_lock_rejects_closure_with_catalog_issues(self):
        closure = ProductoMonthClosure.objects.create(
            month_start=date(2025, 9, 1),
            month_end=date(2025, 9, 30),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
            opening_reference_date=date(2025, 8, 31),
        )
        ProductoMonthClosureLine.objects.create(
            closure=closure,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("5"),
            inventario_final_teorico=Decimal("5"),
            has_catalog_issue=True,
            catalog_issue_note="Relacion derivada faltante",
        )

        with self.assertRaisesMessage(ProductMonthClosureError, "incidencias de catalogo"):
            self.service.lock(closure=closure)

    @override_settings(PRODUCT_MONTH_CLOSURE_SNAPSHOT_TOLERANCE_DAYS=3)
    def test_preview_uses_snapshot_fallback_within_tolerance_and_marks_warning(self):
        point_parent = PointProduct.objects.create(external_id="point-parent-fallback", sku="SNK-M", name=self.parent.nombre)
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=point_parent,
            stock=Decimal("4"),
            sync_job=self.sync_job,
            captured_at=timezone.make_aware(datetime(2025, 8, 29, 12, 0, 0), timezone.get_current_timezone()),
        )

        preview = self.service.preview(month="2025-09")

        self.assertEqual(preview["opening_reference_date"], date(2025, 8, 29))
        self.assertTrue(preview["metadata"]["validation"]["snapshot_fallback_used"])
        self.assertIn("tolerancia", preview["metadata"]["validation"]["warnings"][0].lower())

    def test_lock_rejects_closure_with_unmatched_opening_products(self):
        closure = ProductoMonthClosure.objects.create(
            month_start=date(2025, 9, 1),
            month_end=date(2025, 9, 30),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
            opening_reference_date=date(2025, 8, 31),
            metadata={
                "opening_meta": {"unmatched_products": ["Producto Sin Match"]},
                "validation": {"blocking_issues": ["Productos sin homologacion"]},
            },
        )
        ProductoMonthClosureLine.objects.create(
            closure=closure,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("5"),
            inventario_final_teorico=Decimal("5"),
        )

        with self.assertRaisesMessage(ProductMonthClosureError, "opening sin homologacion"):
            self.service.lock(closure=closure)

    def test_build_chain_uses_previous_month_closure_as_next_opening(self):
        point_parent = PointProduct.objects.create(external_id="point-parent-chain", sku="SNK-M", name=self.parent.nombre)
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=point_parent,
            stock=Decimal("10"),
            sync_job=self.sync_job,
            captured_at=timezone.make_aware(datetime(2025, 8, 31, 23, 0, 0), timezone.get_current_timezone()),
        )
        september = self.service.build(month="2025-09")
        september_line = september.lines.get(receta_padre=self.parent)
        self.assertEqual(september_line.inventario_final_teorico, Decimal("10"))

        PointProductionLine.objects.create(
            branch=self.point_branch,
            erp_branch=self.sucursal,
            receta=self.parent,
            production_external_id="prod-chain-1",
            detail_external_id="detail-chain-1",
            source_hash="prod-chain-hash-1",
            production_date=date(2025, 10, 2),
            item_name=self.parent.nombre,
            item_code=self.parent.codigo_point,
            produced_quantity=Decimal("3"),
        )

        october = self.service.build(month="2025-10")
        october_line = october.lines.get(receta_padre=self.parent)
        self.assertEqual(october.opening_source, ProductoMonthClosure.OPENING_SOURCE_PREVIOUS_CLOSURE)
        self.assertEqual(october_line.inventario_inicial_teorico, Decimal("10"))
        self.assertEqual(october_line.inventario_final_teorico, Decimal("13"))

    def test_backfill_command_dry_run_reports_month_summary(self):
        point_parent = PointProduct.objects.create(external_id="point-parent-backfill", sku="SNK-M", name=self.parent.nombre)
        PointInventorySnapshot.objects.create(
            branch=self.point_branch,
            product=point_parent,
            stock=Decimal("7"),
            sync_job=self.sync_job,
            captured_at=timezone.make_aware(datetime(2025, 8, 31, 23, 0, 0), timezone.get_current_timezone()),
        )

        out = StringIO()
        call_command(
            "backfill_product_month_closure",
            from_month="2025-09",
            to_month="2025-09",
            dry_run=True,
            stdout=out,
        )

        payload = out.getvalue()
        self.assertIn('"dry_run": true', payload)
        self.assertIn('"month": "2025-09"', payload)
        self.assertIn('"status": "warning"', payload)
        self.assertIn("requiere validacion manual previa al lock", payload)

    def test_build_carries_forward_previous_opening_issues(self):
        previous = ProductoMonthClosure.objects.create(
            month_start=date(2025, 8, 1),
            month_end=date(2025, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED,
            opening_reference_date=date(2025, 8, 31),
            metadata={
                "opening_meta": {"bootstrap_seeded": True, "unmatched_products": ["Producto Sin Match"]},
                "validation": {"blocking_issues": ["Productos sin homologacion"]},
                "bootstrap_seed": {"is_seed": True, "source_label": "bootstrap.xlsx::SEPT 25::D"},
            },
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("8"),
            inventario_final_teorico=Decimal("8"),
        )

        closure = self.service.build(month="2025-09")

        validation = (closure.metadata or {}).get("validation", {})
        self.assertEqual(closure.opening_source, ProductoMonthClosure.OPENING_SOURCE_PREVIOUS_CLOSURE)
        self.assertEqual(validation["unmatched_opening_products_count"], 1)
        self.assertGreaterEqual(validation["upstream_opening_issue_count"], 1)
        with self.assertRaisesMessage(ProductMonthClosureError, "opening sin homologacion"):
            self.service.lock(closure=closure)

    def test_bootstrap_command_builds_seed_closure_from_excel(self):
        out = StringIO()
        wb = Workbook()
        ws = wb.active
        ws.title = "SEPT 25"
        ws["A1"] = "PRODUCTO"
        ws["D1"] = "INVENTARIO INICIAL"
        ws["A2"] = self.parent.nombre
        ws["D2"] = 2
        ws["A3"] = self.derived.nombre
        ws["D3"] = 5
        ws["A4"] = "PRODUCTO / REBANADAS"
        ws["D4"] = ""

        with NamedTemporaryFile(suffix=".xlsx") as tmp:
            wb.save(tmp.name)
            call_command(
                "bootstrap_product_month_closure",
                tmp.name,
                sheet="SEPT 25",
                seed_month="2025-08",
                name_column="A",
                stdout=out,
            )

        closure = ProductoMonthClosure.objects.get(month_start=date(2025, 8, 1))
        self.assertEqual(closure.opening_source, ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED)
        self.assertEqual(closure.status, ProductoMonthClosure.STATUS_BUILT)
        line = closure.lines.get(receta_padre=self.parent)
        self.assertEqual(line.inventario_inicial_teorico, Decimal("2"))
        self.assertEqual(line.inventario_final_teorico, Decimal("2"))
        validation = (closure.metadata or {}).get("validation", {})
        self.assertTrue(validation["bootstrap_seeded"])
        self.assertEqual(validation["unmatched_opening_products_count"], 0)
        opening_meta = (closure.metadata or {}).get("opening_meta", {})
        self.assertEqual(opening_meta.get("derived_rows_ignored"), 1)
        self.assertIn('"opening_source": "BOOTSTRAP_SEED"', out.getvalue())

    def test_build_excludes_preparations_vasos_and_accessory_like_products(self):
        vaso = Receta.objects.create(
            nombre="Vaso Fresas con Crema Mini",
            codigo_point="VASO-FCM",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Vaso Preparado Mini",
            familia="Vasos Preparados",
            hash_contenido="hash-vaso-fcm",
        )
        letrero = Receta.objects.create(
            nombre="Letrero Chispas Felicidades",
            codigo_point="LETRERO-1",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-letrero-1",
        )
        preparacion = Receta.objects.create(
            nombre="Crema pastelera",
            codigo_point="CREMA-1",
            tipo=Receta.TIPO_PREPARACION,
            categoria="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)",
            familia="Betún, Cremas, Rellenos (INSUMO PRODUCIDO)",
            hash_contenido="hash-crema-1",
        )

        previous = ProductoMonthClosure.objects.create(
            month_start=date(2025, 8, 1),
            month_end=date(2025, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED,
            opening_reference_date=date(2025, 8, 31),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("10"),
            inventario_final_teorico=Decimal("10"),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=vaso,
            inventario_inicial_teorico=Decimal("3"),
            inventario_final_teorico=Decimal("3"),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=letrero,
            inventario_inicial_teorico=Decimal("2"),
            inventario_final_teorico=Decimal("2"),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=preparacion,
            inventario_inicial_teorico=Decimal("4"),
            inventario_final_teorico=Decimal("4"),
        )

        closure = self.service.build(month="2025-09")

        names = list(closure.lines.select_related("receta_padre").values_list("receta_padre__nombre", flat=True))
        self.assertEqual(names, [self.parent.nombre])

    def test_build_excludes_kg_and_sabor_modifier_products(self):
        kg_recipe = Receta.objects.create(
            nombre="Bolitas de Nuez KG",
            codigo_point="05021",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Galletas",
            familia="Galletas",
            hash_contenido="hash-bolitas-kg",
        )
        sabor_recipe = Receta.objects.create(
            nombre="Sabor Fresa Grande Pay",
            codigo_point="SFRESAG",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pay Grande",
            familia="Pay",
            hash_contenido="hash-sabor-fresa-pay",
        )

        previous = ProductoMonthClosure.objects.create(
            month_start=date(2025, 8, 1),
            month_end=date(2025, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED,
            opening_reference_date=date(2025, 8, 31),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("10"),
            inventario_final_teorico=Decimal("10"),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=kg_recipe,
            inventario_inicial_teorico=Decimal("3"),
            inventario_final_teorico=Decimal("3"),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=sabor_recipe,
            inventario_inicial_teorico=Decimal("5"),
            inventario_final_teorico=Decimal("5"),
        )

        closure = self.service.build(month="2025-09")

        names = list(closure.lines.select_related("receta_padre").values_list("receta_padre__nombre", flat=True))
        self.assertEqual(names, [self.parent.nombre])

    def test_build_excludes_topping_and_sin_preparar_products(self):
        topping_recipe = Receta.objects.create(
            nombre="TOPPING FRESA M",
            codigo_point="SFRESAPM",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            categoria="Pastel Mediano",
            hash_contenido="hash-topping-fresa-m",
        )
        sin_preparar_recipe = Receta.objects.create(
            nombre="Pan de Muerto Sin Preparar",
            codigo_point="0124",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="hash-pan-muerto-sin-preparar",
        )

        previous = ProductoMonthClosure.objects.create(
            month_start=date(2025, 8, 1),
            month_end=date(2025, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED,
            opening_reference_date=date(2025, 8, 31),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("10"),
            inventario_final_teorico=Decimal("10"),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=topping_recipe,
            inventario_inicial_teorico=Decimal("8"),
            inventario_final_teorico=Decimal("8"),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=sin_preparar_recipe,
            inventario_inicial_teorico=Decimal("4"),
            inventario_final_teorico=Decimal("4"),
        )

        closure = self.service.build(month="2025-09")

        names = list(closure.lines.select_related("receta_padre").values_list("receta_padre__nombre", flat=True))
        self.assertEqual(names, [self.parent.nombre])

    @override_settings(PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE="OFFICIAL_MONTHLY_REPORT")
    def test_build_uses_official_monthly_report_for_sales_when_configured(self):
        previous = ProductoMonthClosure.objects.create(
            month_start=date(2025, 8, 1),
            month_end=date(2025, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED,
            opening_reference_date=date(2025, 8, 31),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("10"),
            inventario_final_teorico=Decimal("10"),
        )
        PointProductionLine.objects.create(
            branch=self.point_branch,
            erp_branch=self.sucursal,
            receta=self.parent,
            production_external_id="prod-official-1",
            detail_external_id="detail-official-1",
            source_hash="prod-official-hash-1",
            production_date=date(2025, 9, 5),
            item_name=self.parent.nombre,
            item_code=self.parent.codigo_point,
            produced_quantity=Decimal("15"),
        )
        VentaHistorica.objects.create(
            receta=self.parent,
            sucursal=self.sucursal,
            fecha=date(2025, 9, 10),
            cantidad=Decimal("999"),
            fuente="POINT_BRIDGE_SALES",
        )

        parent_code = self.parent.codigo_point
        parent_name = self.parent.nombre

        class FakeOfficialSalesReportService:
            def fetch_report(self, **kwargs):
                return type(
                    "Report",
                    (),
                    {
                        "report_path": "/tmp/official-september.xls",
                        "request_url": "https://point.example/report",
                    },
                )()

            def parse_report(self, *, report_path: str):
                return type(
                    "ParsedReport",
                    (),
                    {
                        "rows": [
                            {
                                "Codigo": parent_code,
                                "Nombre": parent_name,
                                "Cantidad": Decimal("7"),
                            }
                        ],
                        "summary": {"venta": Decimal("100")},
                    },
                )()

        self.service.official_sales_report_service = FakeOfficialSalesReportService()

        closure = self.service.build(month="2025-09")

        line = closure.lines.get(receta_padre=self.parent)
        self.assertEqual(line.venta_directa_enteros, Decimal("7"))
        sales_meta = (closure.metadata or {}).get("sales_meta", {})
        self.assertEqual(sales_meta.get("mode"), "official_monthly_report")
        self.assertEqual(sales_meta.get("report_path"), "/tmp/official-september.xls")

    @override_settings(PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE="AUTO")
    def test_build_falls_back_to_official_point_daily_sales_when_monthly_report_fails(self):
        previous = ProductoMonthClosure.objects.create(
            month_start=date(2025, 8, 1),
            month_end=date(2025, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED,
            opening_reference_date=date(2025, 8, 31),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("10"),
            inventario_final_teorico=Decimal("10"),
        )
        PointDailySale.objects.create(
            branch=self.point_branch,
            product=PointProduct.objects.create(
                external_id="parent-fallback-1",
                sku=self.parent.codigo_point,
                name=self.parent.nombre,
                category="Pasteles",
            ),
            receta=self.parent,
            sync_job=self.sync_job,
            sale_date=date(2025, 9, 10),
            quantity=Decimal("9"),
            tickets=0,
            gross_amount=Decimal("900"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("900"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("900"),
            source_endpoint="/Report/PrintReportes?idreporte=3",
        )
        VentaHistorica.objects.create(
            receta=self.parent,
            sucursal=self.sucursal,
            fecha=date(2025, 9, 10),
            cantidad=Decimal("999"),
            fuente="POINT_BRIDGE_SALES",
        )

        class FailingOfficialSalesReportService:
            def fetch_report(self, **kwargs):
                raise RuntimeError("Point 500")

        self.service.official_sales_report_service = FailingOfficialSalesReportService()

        closure = self.service.build(month="2025-09")

        line = closure.lines.get(receta_padre=self.parent)
        self.assertEqual(line.venta_directa_enteros, Decimal("9"))
        sales_meta = (closure.metadata or {}).get("sales_meta", {})
        self.assertEqual(sales_meta.get("mode"), "official_point_daily_sales")
        self.assertIn("PointDailySale oficial", " ".join(sales_meta.get("warnings") or []))

    @override_settings(PRODUCT_MONTH_CLOSURE_SALES_SOURCE_MODE="AUTO")
    def test_lock_rejects_closure_when_official_daily_sales_job_is_partial(self):
        previous = ProductoMonthClosure.objects.create(
            month_start=date(2025, 8, 1),
            month_end=date(2025, 8, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_BOOTSTRAP_SEED,
            opening_reference_date=date(2025, 8, 31),
        )
        ProductoMonthClosureLine.objects.create(
            closure=previous,
            receta_padre=self.parent,
            inventario_inicial_teorico=Decimal("10"),
            inventario_final_teorico=Decimal("10"),
        )
        partial_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_PARTIAL,
            parameters={
                "source": "POINT_OFFICIAL_REPORT",
                "start_date": "2025-09-01",
                "end_date": "2025-09-30",
            },
            error_message="Backfill oficial completado con 1 branch-day(s) omitidos por error.",
        )
        PointDailySale.objects.create(
            branch=self.point_branch,
            product=PointProduct.objects.create(
                external_id="parent-partial-1",
                sku=self.parent.codigo_point,
                name=self.parent.nombre,
                category="Pasteles",
            ),
            receta=self.parent,
            sync_job=partial_job,
            sale_date=date(2025, 9, 10),
            quantity=Decimal("9"),
            tickets=0,
            gross_amount=Decimal("900"),
            discount_amount=Decimal("0"),
            total_amount=Decimal("900"),
            tax_amount=Decimal("0"),
            net_amount=Decimal("900"),
            source_endpoint="/Report/PrintReportes?idreporte=3",
        )

        class FailingOfficialSalesReportService:
            def fetch_report(self, **kwargs):
                raise RuntimeError("Point 500")

        self.service.official_sales_report_service = FailingOfficialSalesReportService()

        closure = self.service.build(month="2025-09")

        validation = dict((closure.metadata or {}).get("validation") or {})
        self.assertFalse(validation.get("lock_ready"))
        self.assertEqual(validation.get("sales_job_status"), PointSyncJob.STATUS_PARTIAL)
        self.assertTrue(
            any("termino en estado PARTIAL" in issue for issue in validation.get("blocking_issues") or [])
        )
        with self.assertRaises(ProductMonthClosureError):
            self.service.lock(closure=closure, reason="test")
