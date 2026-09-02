from __future__ import annotations

from datetime import date
from decimal import Decimal
from threading import Barrier, Event, Thread
from unittest.mock import patch

from django.db import close_old_connections
from django.test import TransactionTestCase

from pos_bridge.models import PointBranch, PointDailySale, PointSyncJob
from pos_bridge.services.official_sales_backfill_service import OfficialSalesBackfillService
from pos_bridge.services.product_month_closure_service import ProductMonthClosureService
from recetas.models import ProductoMonthClosure, ProductoMonthClosureLine, Receta


class WriterSerializationTests(TransactionTestCase):
    reset_sequences = True

    @staticmethod
    def _plan(note):
        return {
            "month_end": date(2026, 8, 31),
            "notes": note,
            "opening_source": ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
            "opening_reference_date": date(2026, 7, 31),
            "metadata": {},
            "line_rows": [],
        }

    @staticmethod
    def _line(recipe):
        zero = Decimal("0")
        return {"receta": recipe, "inventario_inicial_teorico": zero, "produccion_mes": zero,
                "venta_directa_enteros": zero, "venta_derivada_equivalente": zero,
                "venta_total_equivalente": zero, "merma_directa_enteros": zero,
                "merma_derivada_equivalente": zero, "merma_total_equivalente": zero,
                "inventario_final_teorico": zero, "inventario_final_point_cedis": zero,
                "inventario_final_point_sucursales": zero, "inventario_final_point_total": zero,
                "diferencia_teorico_vs_point": zero, "estado_auditoria": "COINCIDE",
                "detalle_auditoria": "", "source_closing_snapshot_count": 0,
                "source_snapshot_count": 0, "source_sale_rows": 0, "source_production_rows": 0,
                "source_waste_rows": 0, "has_catalog_issue": False, "catalog_issue_note": "", "metadata": {}}

    def _thread(self, target, errors):
        def run():
            close_old_connections()
            try:
                target()
            except Exception as exc:  # pragma: no cover - asserted by caller
                errors.append(exc)
            finally:
                close_old_connections()
        thread = Thread(target=run, daemon=True)
        thread.start()
        return thread

    def test_two_rebuilds_and_following_lock_serialize_on_month_row(self):
        closure = ProductoMonthClosure.objects.create(
            month_start=date(2026, 8, 1), month_end=date(2026, 8, 31), status=ProductoMonthClosure.STATUS_BUILT
        )
        first_inside = Event()
        release_first = Event()
        second_inside = Event()
        errors = []
        first = ProductMonthClosureService()
        second = ProductMonthClosureService()

        def first_preview(**kwargs):
            first_inside.set()
            self.assertTrue(release_first.wait(5))
            return self._plan("first")

        def second_preview(**kwargs):
            second_inside.set()
            return self._plan("second")

        first.preview = first_preview
        second.preview = second_preview
        t1 = self._thread(lambda: first.build(month="2026-08", rebuild=True), errors)
        self.assertTrue(first_inside.wait(5))
        t2 = self._thread(lambda: second.build(month="2026-08", rebuild=True), errors)
        self.assertFalse(second_inside.wait(0.25))
        release_first.set()
        t1.join(5)
        t2.join(5)
        self.assertFalse(t1.is_alive() or t2.is_alive())
        self.assertEqual(errors, [])
        closure.refresh_from_db()
        self.assertEqual(closure.notes, "second")

        recipe = Receta.objects.create(
            nombre="Producto serializado", tipo=Receta.TIPO_PRODUCTO_FINAL, hash_contenido="writer-serialization"
        )
        ProductoMonthClosureLine.objects.create(closure=closure, receta_padre=recipe)
        ProductMonthClosureService().lock(closure=closure, reason="concurrency-test")
        closure.refresh_from_db()
        self.assertTrue(closure.is_locked)

    def test_same_branch_day_replacements_serialize_and_last_result_is_complete(self):
        branch = PointBranch.objects.create(external_id="serial-day", name="Matriz")
        jobs = [PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_SALES) for _ in range(2)]
        first_inside = Event()
        release_first = Event()
        second_inside = Event()
        errors = []
        services = [OfficialSalesBackfillService(), OfficialSalesBackfillService()]
        originals = [service._resolve_product for service in services]

        def resolver(index):
            def resolve(**kwargs):
                (first_inside if index == 0 else second_inside).set()
                if index == 0:
                    self.assertTrue(release_first.wait(5))
                return originals[index](**kwargs)
            return resolve

        for index, service in enumerate(services):
            service._resolve_product = resolver(index)

        def rows(quantity):
            return {("SKU", "Producto", "Cat"): {
                "sku": "SKU", "name": "Producto", "category": "Cat", "quantity": Decimal(quantity),
                "gross_amount": Decimal(quantity), "discount_amount": Decimal("0"),
                "total_amount": Decimal(quantity), "tax_amount": Decimal("0"),
                "net_amount": Decimal(quantity), "scopes": {"null"},
            }}

        with patch("pos_bridge.services.official_sales_backfill_service.mark_analytics_dirty_for_range"):
            t1 = self._thread(lambda: services[0]._replace_branch_day_sales(
                branch=branch, sale_date=date(2026, 8, 1), sync_job=jobs[0], aggregated_rows=rows("1")
            ), errors)
            self.assertTrue(first_inside.wait(5))
            t2 = self._thread(lambda: services[1]._replace_branch_day_sales(
                branch=branch, sale_date=date(2026, 8, 1), sync_job=jobs[1], aggregated_rows=rows("2")
            ), errors)
            self.assertFalse(second_inside.wait(0.25))
            release_first.set()
            t1.join(5)
            t2.join(5)

        self.assertFalse(t1.is_alive() or t2.is_alive())
        self.assertEqual(errors, [])
        sale = PointDailySale.objects.get(branch=branch, sale_date=date(2026, 8, 1))
        self.assertEqual(sale.quantity, Decimal("2"))
        self.assertEqual(sale.sync_job_id, jobs[1].id)

    def test_different_branches_with_shared_product_finish_consistently(self):
        branches = [
            PointBranch.objects.create(external_id=f"parallel-{index}", name=f"Sucursal {index}")
            for index in range(2)
        ]
        jobs = [PointSyncJob.objects.create(job_type=PointSyncJob.JOB_TYPE_SALES) for _ in range(2)]
        start = Event()
        overlap = Barrier(2, timeout=5)
        errors = []

        def writer(index):
            self.assertTrue(start.wait(5))
            service = OfficialSalesBackfillService()
            original = service._resolve_product
            def resolve_together(**kwargs):
                overlap.wait()
                return original(**kwargs)
            service._resolve_product = resolve_together
            service._replace_branch_day_sales(
                branch=branches[index], sale_date=date(2026, 8, 2), sync_job=jobs[index],
                aggregated_rows={("SHARED", "Compartido", "Cat"): {
                    "sku": "SHARED", "name": "Compartido", "category": "Cat", "quantity": Decimal(index + 1),
                    "gross_amount": Decimal("1"), "discount_amount": Decimal("0"), "total_amount": Decimal("1"),
                    "tax_amount": Decimal("0"), "net_amount": Decimal("1"), "scopes": {"null"},
                }},
            )

        with patch("pos_bridge.services.official_sales_backfill_service.mark_analytics_dirty_for_range"):
            threads = [self._thread(lambda index=index: writer(index), errors) for index in range(2)]
            start.set()
            for thread in threads:
                thread.join(5)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        rows = list(PointDailySale.objects.filter(sale_date=date(2026, 8, 2)).order_by("branch_id"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].product_id, rows[1].product_id)

    def test_lock_waits_for_rebuild_and_never_gets_overwritten_or_unlocked(self):
        recipe = Receta.objects.create(
            nombre="Producto lock versus rebuild", tipo=Receta.TIPO_PRODUCTO_FINAL, hash_contenido="lock-v-rebuild"
        )
        closure = ProductoMonthClosure.objects.create(
            month_start=date(2026, 9, 1), month_end=date(2026, 9, 30), status=ProductoMonthClosure.STATUS_BUILT
        )
        ProductoMonthClosureLine.objects.create(closure=closure, receta_padre=recipe)
        rebuild_inside = Event()
        release_rebuild = Event()
        lock_finished = Event()
        errors = []
        service = ProductMonthClosureService()

        def preview(**kwargs):
            rebuild_inside.set()
            self.assertTrue(release_rebuild.wait(5))
            plan = self._plan("rebuilt-before-lock")
            plan["month_end"] = date(2026, 9, 30)
            plan["line_rows"] = [self._line(recipe)]
            return plan

        service.preview = preview
        rebuild_thread = self._thread(lambda: service.build(month="2026-09", rebuild=True), errors)
        self.assertTrue(rebuild_inside.wait(5))

        def lock_writer():
            ProductMonthClosureService().lock(closure=closure, reason="parallel-lock")
            lock_finished.set()

        lock_thread = self._thread(lock_writer, errors)
        self.assertFalse(lock_finished.wait(0.25))
        release_rebuild.set()
        rebuild_thread.join(5)
        lock_thread.join(5)
        self.assertFalse(rebuild_thread.is_alive() or lock_thread.is_alive())
        self.assertEqual(errors, [])
        closure.refresh_from_db()
        self.assertTrue(closure.is_locked)
        self.assertEqual(closure.status, ProductoMonthClosure.STATUS_LOCKED)
        self.assertEqual(closure.notes, "rebuilt-before-lock")
        self.assertEqual(closure.lines.count(), 1)
