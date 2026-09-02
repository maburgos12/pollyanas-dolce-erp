from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from threading import Barrier, Event, Thread
from unittest.mock import patch

from django.db import DatabaseError, close_old_connections, transaction
from django.test import TransactionTestCase
from django.utils import timezone

from pos_bridge.models import PointBranch, PointDailySale, PointExtractionLog, PointSyncJob
from pos_bridge.services.official_sales_backfill_service import OfficialSalesBackfillService
from pos_bridge.services.product_month_closure_service import ProductMonthClosureService
from recetas.models import ProductoMonthClosure, ProductoMonthClosureLine, Receta, RecetaCodigoPointAlias


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

    def _assert_source_writer_waits(self, writer):
        source_locked = Event()
        release_source_lock = Event()
        writer_finished = Event()
        errors = []

        def hold_source_lock():
            with transaction.atomic():
                ProductMonthClosureService._lock_canonical_source_tables()
                source_locked.set()
                self.assertTrue(release_source_lock.wait(5))

        lock_thread = self._thread(hold_source_lock, errors)
        self.assertTrue(source_locked.wait(5))

        def run_writer():
            writer()
            writer_finished.set()

        writer_thread = self._thread(run_writer, errors)
        self.assertFalse(writer_finished.wait(0.25))
        release_source_lock.set()
        lock_thread.join(5)
        writer_thread.join(5)
        self.assertFalse(lock_thread.is_alive() or writer_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(writer_finished.is_set())

    def _canonical_closure_for_line_race(self, *, month_start, recipe):
        closure = ProductoMonthClosure.objects.create(
            month_start=month_start,
            month_end=date(month_start.year, month_start.month, 28),
            status=ProductoMonthClosure.STATUS_BUILT,
            opening_source=ProductoMonthClosure.OPENING_SOURCE_POINT_SNAPSHOT,
        )
        line = ProductoMonthClosureLine.objects.create(
            closure=closure,
            receta_padre=recipe,
            inventario_inicial_teorico=Decimal("1"),
            inventario_final_teorico=Decimal("1"),
            inventario_final_point_total=Decimal("1"),
            metadata={"balance_contract": "POINT_PRODUCT_BALANCE_V1", "issues": []},
        )
        line.refresh_from_db()
        metadata = {
            "balance": {"contract": "POINT_PRODUCT_BALANCE_V1"},
            "validation": {"lock_ready": True, "blocking_issues": []},
        }
        for metadata_key, _label in ProductMonthClosureService.CANONICAL_LOCK_REQUIRED_SOURCES:
            metadata[metadata_key] = {"authoritative": True, "source_present": True}
        projected_lines_digest = ProductMonthClosureService._persisted_lines_digest([line])
        metadata["source_fingerprint"] = {
            "algorithm": "sha256",
            "digest": "stable-source-digest",
            "metadata_digest": ProductMonthClosureService._canonical_source_metadata_digest(metadata),
            "projected_lines_digest": projected_lines_digest,
            "raw_sources_digest": "stable-raw-digest",
        }
        closure.metadata = metadata
        closure.save(update_fields=["metadata", "updated_at"])
        current_plan = {"metadata": {"source_fingerprint": dict(metadata["source_fingerprint"])}}
        return closure, line, current_plan

    def _assert_line_dml_waits_for_lock_and_remains_detectable(self, *, month_start, writer):
        recipe = Receta.objects.create(
            nombre=f"Línea protegida {month_start:%Y-%m}",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido=f"line-lock-{month_start:%Y-%m}",
        )
        closure, line, current_plan = self._canonical_closure_for_line_race(
            month_start=month_start,
            recipe=recipe,
        )
        validation_inside = Event()
        release_validation = Event()
        writer_finished = Event()
        errors = []
        service = ProductMonthClosureService()

        def fresh_preview(**kwargs):
            validation_inside.set()
            self.assertTrue(release_validation.wait(5))
            return current_plan

        lock_thread = None
        with patch.object(service, "_fresh_canonical_preview", side_effect=fresh_preview):
            lock_thread = self._thread(lambda: service.lock(closure=closure), errors)
            self.assertTrue(validation_inside.wait(5), errors)

            def run_writer():
                try:
                    writer(closure, line)
                finally:
                    writer_finished.set()

            writer_thread = self._thread(run_writer, errors)
            self.assertFalse(writer_finished.wait(0.25))
            release_validation.set()
            lock_thread.join(5)
            writer_thread.join(5)

        self.assertFalse(lock_thread.is_alive() or writer_thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], DatabaseError)
        self.assertIn("cierre mensual bloqueado", str(errors[0]))
        self.assertTrue(writer_finished.is_set())
        closure.refresh_from_db()
        self.assertTrue(closure.is_locked)
        current_lines = list(closure.lines.all())
        self.assertEqual(
            ProductMonthClosureService._persisted_lines_digest(current_lines),
            closure.metadata["source_fingerprint"]["projected_lines_digest"],
        )

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

    def test_source_lock_blocks_extraction_log_insert_and_following_fingerprint_sees_it(self):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_SALES,
            status=PointSyncJob.STATUS_SUCCESS,
            started_at=timezone.make_aware(datetime(2026, 8, 15, 12, 0)),
            parameters={"start_date": "2026-08-01", "end_date": "2026-08-31"},
        )
        before = ProductMonthClosureService._raw_source_evidence(month_start=date(2026, 8, 1))

        self._assert_source_writer_waits(
            lambda: PointExtractionLog.objects.create(
                sync_job=job,
                level=PointExtractionLog.LEVEL_INFO,
                message="Backfill oficial branch 2026-08-15",
                context={"branch_external_id": "branch", "sale_date": "2026-08-15"},
            )
        )

        after = ProductMonthClosureService._raw_source_evidence(month_start=date(2026, 8, 1))
        self.assertNotEqual(before["extraction_logs"]["digest"], after["extraction_logs"]["digest"])

    def test_source_lock_blocks_alias_remap_and_fresh_fingerprint_sees_it(self):
        first = Receta.objects.create(
            nombre="Alias origen",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="alias-lock-origin",
        )
        second = Receta.objects.create(
            nombre="Alias destino",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="alias-lock-destination",
        )
        alias = RecetaCodigoPointAlias.objects.create(
            receta=first,
            codigo_point="ALIAS-LOCK",
            nombre_point="Alias lock",
        )
        before = ProductMonthClosureService._raw_source_evidence(month_start=date(2026, 8, 1))

        self._assert_source_writer_waits(
            lambda: RecetaCodigoPointAlias.objects.filter(pk=alias.pk).update(receta=second)
        )

        after = ProductMonthClosureService._raw_source_evidence(month_start=date(2026, 8, 1))
        self.assertNotEqual(
            before["recipe_point_aliases"]["digest"],
            after["recipe_point_aliases"]["digest"],
        )

    def test_closure_lock_blocks_concurrent_line_update_until_commit(self):
        self._assert_line_dml_waits_for_lock_and_remains_detectable(
            month_start=date(2026, 5, 1),
            writer=lambda _closure, line: ProductoMonthClosureLine.objects.filter(pk=line.pk).update(
                produccion_mes=Decimal("9")
            ),
        )

    def test_closure_lock_blocks_concurrent_line_delete_until_commit(self):
        self._assert_line_dml_waits_for_lock_and_remains_detectable(
            month_start=date(2026, 6, 1),
            writer=lambda _closure, line: ProductoMonthClosureLine.objects.filter(pk=line.pk).delete(),
        )

    def test_closure_lock_blocks_concurrent_line_insert_until_commit(self):
        inserted_recipe = Receta.objects.create(
            nombre="Línea concurrente insertada",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="line-lock-inserted",
        )
        self._assert_line_dml_waits_for_lock_and_remains_detectable(
            month_start=date(2026, 7, 1),
            writer=lambda closure, _line: ProductoMonthClosureLine.objects.create(
                closure=closure,
                receta_padre=inserted_recipe,
                metadata={"balance_contract": "POINT_PRODUCT_BALANCE_V1", "issues": []},
            ),
        )

    def test_writer_started_first_does_not_deadlock_with_closure_lock(self):
        target_recipe = Receta.objects.create(
            nombre="Línea objetivo sin deadlock",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="line-lock-target-no-deadlock",
        )
        target, _line, current_plan = self._canonical_closure_for_line_race(
            month_start=date(2026, 4, 1),
            recipe=target_recipe,
        )
        other = ProductoMonthClosure.objects.create(
            month_start=date(2026, 3, 1),
            month_end=date(2026, 3, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
        )
        first_insert_recipe = Receta.objects.create(
            nombre="Línea que toma ROW EXCLUSIVE primero",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="line-lock-row-exclusive-first",
        )
        target_insert_recipe = Receta.objects.create(
            nombre="Línea tardía del cierre objetivo",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="line-lock-target-late-insert",
        )
        first_inserted = Event()
        attempt_target_insert = Event()
        validation_inside = Event()
        release_validation = Event()
        writer_finished = Event()
        lock_finished = Event()
        errors = []
        service = ProductMonthClosureService()

        def writer():
            try:
                with transaction.atomic():
                    ProductoMonthClosureLine.objects.create(
                        closure=other,
                        receta_padre=first_insert_recipe,
                    )
                    first_inserted.set()
                    self.assertTrue(attempt_target_insert.wait(5))
                    ProductoMonthClosureLine.objects.create(
                        closure=target,
                        receta_padre=target_insert_recipe,
                        metadata={"balance_contract": "POINT_PRODUCT_BALANCE_V1", "issues": []},
                    )
            finally:
                writer_finished.set()

        def fresh_preview(**kwargs):
            validation_inside.set()
            self.assertTrue(release_validation.wait(5))
            return current_plan

        def locker():
            service.lock(closure=target)
            lock_finished.set()

        writer_thread = self._thread(writer, errors)
        self.assertTrue(first_inserted.wait(5))
        with patch.object(service, "_fresh_canonical_preview", side_effect=fresh_preview):
            lock_thread = self._thread(locker, errors)
            # Sin un lock global de la tabla hija, el locker puede avanzar a la
            # revalidación aunque el writer ya tenga ROW EXCLUSIVE por otro INSERT.
            validation_inside.wait(0.3)
            attempt_target_insert.set()
            self.assertTrue(validation_inside.wait(5), errors)
            self.assertFalse(writer_finished.wait(0.25))
            release_validation.set()
            lock_thread.join(5)
            writer_thread.join(5)

        self.assertFalse(lock_thread.is_alive() or writer_thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], DatabaseError)
        self.assertIn("cierre mensual bloqueado", str(errors[0]))
        self.assertTrue(lock_finished.is_set())
        self.assertTrue(writer_finished.is_set())
        target.refresh_from_db()
        self.assertTrue(target.is_locked)
        self.assertEqual(other.lines.count(), 0)
        self.assertEqual(
            ProductMonthClosureService._persisted_lines_digest(list(target.lines.all())),
            target.metadata["source_fingerprint"]["projected_lines_digest"],
        )

    def test_database_rejects_direct_insert_update_and_delete_on_locked_closure(self):
        locked = ProductoMonthClosure.objects.create(
            month_start=date(2026, 2, 1),
            month_end=date(2026, 2, 28),
            status=ProductoMonthClosure.STATUS_LOCKED,
            is_locked=True,
        )
        recipe = Receta.objects.create(
            nombre="Línea DB protegida",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="db-guard-locked-line",
        )
        line = ProductoMonthClosureLine.objects.create(
            closure=ProductoMonthClosure.objects.create(
                month_start=date(2026, 1, 1),
                month_end=date(2026, 1, 31),
                status=ProductoMonthClosure.STATUS_BUILT,
            ),
            receta_padre=recipe,
            produccion_mes=Decimal("1"),
        )

        with self.assertRaisesMessage(DatabaseError, "cierre mensual bloqueado"):
            with transaction.atomic():
                ProductoMonthClosureLine.objects.create(closure=locked, receta_padre=recipe)
        self.assertFalse(locked.lines.exists())

        line.closure = locked
        with self.assertRaisesMessage(DatabaseError, "cierre mensual bloqueado"):
            with transaction.atomic():
                line.save(update_fields=["closure", "updated_at"])
        line.refresh_from_db()
        self.assertNotEqual(line.closure_id, locked.id)

        ProductoMonthClosure.objects.filter(pk=line.closure_id).update(is_locked=True)
        with self.assertRaisesMessage(DatabaseError, "cierre mensual bloqueado"):
            with transaction.atomic():
                ProductoMonthClosureLine.objects.filter(pk=line.pk).update(produccion_mes=Decimal("8"))
        line.refresh_from_db()
        self.assertEqual(line.produccion_mes, Decimal("1"))

        with self.assertRaisesMessage(DatabaseError, "cierre mensual bloqueado"):
            with transaction.atomic():
                ProductoMonthClosureLine.objects.filter(pk=line.pk).delete()
        self.assertTrue(ProductoMonthClosureLine.objects.filter(pk=line.pk).exists())

    def test_database_allows_line_dml_on_unlocked_closure_and_rolls_back_failed_batch(self):
        unlocked = ProductoMonthClosure.objects.create(
            month_start=date(2025, 12, 1),
            month_end=date(2025, 12, 31),
            status=ProductoMonthClosure.STATUS_BUILT,
        )
        locked = ProductoMonthClosure.objects.create(
            month_start=date(2025, 11, 1),
            month_end=date(2025, 11, 30),
            status=ProductoMonthClosure.STATUS_LOCKED,
            is_locked=True,
        )
        recipe = Receta.objects.create(
            nombre="Línea DB abierta",
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            hash_contenido="db-guard-unlocked-line",
        )
        line = ProductoMonthClosureLine.objects.create(closure=unlocked, receta_padre=recipe)
        ProductoMonthClosureLine.objects.filter(pk=line.pk).update(produccion_mes=Decimal("3"))
        line.refresh_from_db()
        self.assertEqual(line.produccion_mes, Decimal("3"))

        with self.assertRaisesMessage(DatabaseError, "cierre mensual bloqueado"):
            with transaction.atomic():
                ProductoMonthClosureLine.objects.filter(pk=line.pk).update(produccion_mes=Decimal("7"))
                ProductoMonthClosureLine.objects.create(closure=locked, receta_padre=recipe)
        line.refresh_from_db()
        self.assertEqual(line.produccion_mes, Decimal("3"))

        ProductoMonthClosureLine.objects.filter(pk=line.pk).delete()
        self.assertFalse(ProductoMonthClosureLine.objects.filter(pk=line.pk).exists())

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
