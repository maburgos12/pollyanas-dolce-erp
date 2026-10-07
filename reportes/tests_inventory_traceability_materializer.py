from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone as datetime_timezone
from decimal import Decimal
from io import StringIO
from threading import Event, Lock, Thread
from time import monotonic
from types import SimpleNamespace
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import close_old_connections, connection
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointProduct, PointSyncJob, PointTransferLine
from pos_bridge.services.branch_inventory_traceability_service import (
    BranchInventoryTraceability,
    BranchProductBalance,
    TraceSourceIssue,
)
from pos_bridge.services.audit_stock_history_service import (
    AuditStockHistoryService, PointHistoryReconciliation,
)
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from reportes.models import (
    ProductInventoryAuditCase,
    ProductInventoryAuditEvent,
    ProductInventoryAuditRun,
)
from reportes.services_inventory_traceability import InventoryAuditMaterializer


ZERO = Decimal("0")
MONTH = date(2026, 8, 1)


class MutableTraceabilityService:
    def __init__(self, result):
        self.result = result
        self.months = []

    def build(self, month, *, allow_partial=False):
        self.months.append(month)
        return self.result


class TimestampTraceabilityService(MutableTraceabilityService):
    def build(self, month):
        self.build_started_at = timezone.now()
        result = super().build(month)
        self.build_finished_at = timezone.now()
        return result


class TraceabilityTestFixtures:
    @classmethod
    def setUpTestData(cls):
        cls.branch = PointBranch.objects.create(external_id="CENTRO", name="Centro")
        cls.product = PointProduct.objects.create(
            external_id="PASTEL-001",
            sku="PASTEL-001",
            name="Pastel de prueba",
        )

    def _line(
        self,
        *,
        closing=Decimal("11"),
        issues=(),
        source_trace=None,
    ):
        opening = Decimal("10")
        production = Decimal("2")
        sales = Decimal("2")
        expected = Decimal("10")
        return BranchProductBalance(
            branch=self.branch,
            product=self.product,
            opening=opening,
            production=production,
            sales=sales,
            waste=ZERO,
            transfer_in=ZERO,
            transfer_out=ZERO,
            conversion_in=ZERO,
            conversion_out=ZERO,
            identified_adjustment=ZERO,
            expected_closing=expected,
            point_closing=closing,
            difference=closing - expected,
            source_trace=source_trace
            or {
                "opening": (11,),
                "closing": (22,),
                "sales": (33, 34),
                "production": (44,),
                "waste": (),
                "transfers": (),
                "conversions": (),
                "transfer_in": (),
                "transfer_out": (),
                "conversion_in": (),
                "conversion_out": (),
                "adjustments": (),
            },
            issues=tuple(issues),
        )

    def _result(self, *lines, source_complete=True, global_issues=()):
        return BranchInventoryTraceability(
            month=MONTH,
            lines=tuple(lines),
            global_issues=tuple(global_issues),
            company_difference=sum((line.difference for line in lines), ZERO),
            exception_count=sum(line.difference != ZERO for line in lines),
            source_complete=source_complete,
        )

    def _materializer(self, result):
        return InventoryAuditMaterializer(
            traceability_service=MutableTraceabilityService(result)
        )

    def _approve_case(self, case):
        explanation = ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.EXPLAIN,
            reason_code="CONTEO_VALIDADO",
            notes="Explicación validada.",
        )
        ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.APPROVE,
            reason_code="APROBACION_OPERATIVA",
            related_event=explanation,
        )
        case.movement_status = ProductInventoryAuditCase.MovementStatus.RESOLVED
        case.save(update_fields=["movement_status", "updated_at"])
        return case


class InventoryAuditMaterializerTests(TraceabilityTestFixtures, TestCase):
    def test_rebuild_does_not_rewrite_json_normalized_point_history(self):
        history = PointHistoryReconciliation(
            coverage_status="COMPLETE", production=Decimal("3"), sales=Decimal("2"),
            original_batch_evidence={
                "duplicate_movement_ids": (123,), "stock_chain_gaps": (),
            },
        )
        materializer = self._materializer(self._result(self._line()))
        with patch(
            "reportes.services_inventory_traceability.AuditStockHistoryService.reconcile_many",
            return_value={(self.branch.id, self.product.id): history},
        ):
            materializer.rebuild(MONTH)
            case = ProductInventoryAuditCase.objects.get(month=MONTH)
            first_updated_at = case.updated_at
            second = materializer.rebuild(MONTH)
            case.refresh_from_db()

        self.assertEqual(second["updated"], 0)
        self.assertEqual(case.updated_at, first_updated_at)
        self.assertEqual(case.source_trace["point_history"]["original_batch_evidence"], {
            "duplicate_movement_ids": [123], "stock_chain_gaps": [],
        })

    @patch("reportes.services_inventory_traceability.transaction.on_commit")
    def test_partial_rebuild_publishes_only_identified_lines_without_claiming_success(self, on_commit):
        incomplete = self._line(issues=(TraceSourceIssue(
            code="SOURCE_INCOMPLETE", message="Falta cierre", branch_id=self.branch.pk,
            product_id=self.product.pk,
        ),))
        materializer = self._materializer(self._result(incomplete, source_complete=False))

        first = materializer.rebuild(MONTH, allow_partial=True)
        second = materializer.rebuild(MONTH, allow_partial=True)

        run = ProductInventoryAuditRun.objects.get(month=MONTH)
        case = ProductInventoryAuditCase.objects.get(month=MONTH)
        self.assertEqual((first["created"], second["unchanged"]), (1, 1))
        self.assertFalse(first.required_sources_available)
        self.assertTrue(first.partial_published)
        self.assertEqual(run.status, ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE)
        self.assertTrue(run.partial_published)
        self.assertIsNone(run.last_successful_rebuild_at)
        self.assertEqual(case.movement_status, "SOURCE_INCOMPLETE")
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 1)
        on_commit.assert_not_called()

    def test_partial_rebuild_rejects_month_wide_source_issue(self):
        issue = TraceSourceIssue(code="UNEXPECTED_SOURCE", message="Fuente no verificable")
        counts = self._materializer(self._result(
            self._line(), source_complete=False, global_issues=(issue,),
        )).rebuild(MONTH, allow_partial=True)
        self.assertFalse(counts.partial_published)
        self.assertFalse(ProductInventoryAuditCase.objects.exists())

    def test_partial_rebuild_preserves_known_global_issues_without_claiming_month_ready(self):
        issues = (
            TraceSourceIssue(code="SOURCE_INCOMPLETE", message="Cierre Point incompleto"),
            TraceSourceIssue(code="MISSING_CONVERSION_DESTINATION", message="Destino desconocido"),
        )
        counts = self._materializer(self._result(
            self._line(issues=(TraceSourceIssue(
                code="SOURCE_INCOMPLETE", message="Falta frontera", branch_id=self.branch.pk,
                product_id=self.product.pk,
            ),)), source_complete=False, global_issues=issues,
        )).rebuild(MONTH, allow_partial=True)
        run = ProductInventoryAuditRun.objects.get(month=MONTH)
        self.assertTrue(counts.partial_published)
        self.assertFalse(counts.required_sources_available)
        self.assertEqual(run.status, ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE)
        self.assertEqual({issue["code"] for issue in run.source_issues},
                         {"SOURCE_INCOMPLETE", "MISSING_CONVERSION_DESTINATION"})

    def test_documentary_utc_boundary_evidence_survives_materialization_and_fingerprint(self):
        from types import MappingProxyType
        proof = MappingProxyType({"opening": MappingProxyType({
            "original_stock": "10", "effective_stock": "7",
            "movement_ids": (101, 102), "contract": "POINT_STOCK_RAW_UTC",
        })})
        line = self._line(source_trace={"opening": (11,), "historical_boundary_evidence": proof})
        first = InventoryAuditMaterializer()._prepare_line(line)
        self.assertEqual(first["source_trace"]["historical_boundary_evidence"]["opening"]["movement_ids"], [101, 102])
        changed = self._line(source_trace={"opening": (11,), "historical_boundary_evidence": {
            "opening": {**dict(proof["opening"]), "movement_ids": (201, 202)},
        }})
        self.assertNotEqual(first["fingerprint"], InventoryAuditMaterializer()._prepare_line(changed)["fingerprint"])

    def test_history_does_not_replace_commercial_sales_with_unverified_stock_effect(self):
        line = replace(self._line(closing=ZERO), production=ZERO,
                       sales=Decimal("5"), expected_closing=Decimal("5"),
                       difference=Decimal("-5"))
        history = PointHistoryReconciliation(
            coverage_status="COMPLETE", identified_adjustment=Decimal("-10"),
            movement_ids=(901,), movement_ids_by_category={"identified_adjustment": (901,)},
        )
        prepared = InventoryAuditMaterializer()._prepare_line(line, point_history=history)
        self.assertEqual(prepared["normalized_quantities"]["sales"], Decimal("5"))
        self.assertEqual(prepared["movement_status"], "SOURCE_INCOMPLETE")
        issue = prepared["source_trace"]["source_issues"][0]
        self.assertEqual(issue["source_ids"], [33, 34])
        self.assertIn("5", issue["message"])
        self.assertIn("0", issue["message"])
        self.assertEqual(prepared["source_trace"]["point_history"]["unapplied_reason"],
                         "SALES_STOCK_EFFECT_UNVERIFIED")

    def test_history_does_not_certify_a_balance_with_missing_closings(self):
        line = self._line(issues=(TraceSourceIssue(code="SOURCE_INCOMPLETE", message="Falta cierre"),))
        history = SimpleNamespace(
            coverage_status="COMPLETE", unknown_movement_ids=(),
            unexplained_remainder=lambda opening, closing: ZERO,
        )
        prepared = InventoryAuditMaterializer()._prepare_line(line, point_history=history)
        self.assertEqual(prepared["normalized_quantities"]["difference"], line.difference)
        self.assertNotIn("point_history", prepared["source_trace"])

    def test_persists_source_issue_detail_without_losing_ids(self):
        issue = TraceSourceIssue(code='SOURCE_INCOMPLETE',
            message='Falta apertura 31/07 para producto y sucursal.',
            branch_id=self.branch.pk, product_id=self.product.pk, source_ids=(123,))
        prepared = self._materializer(self._result())._prepare_line(self._line(issues=(issue,)))
        self.assertEqual(prepared['source_trace']['source_issues'][0]['message'], issue.message)
        self.assertEqual(prepared['source_trace']['source_issues'][0]['source_ids'], [123])

    def test_identity_provenance_does_not_make_balanced_stock_an_exception(self):
        issues = tuple(
            TraceSourceIssue(code=code, message="Identidad resuelta")
            for code in ("PRODUCT_RESOLVED_BY_SKU", "PRODUCT_RESOLVED_BY_NAME")
        )
        with self.captureOnCommitCallbacks(execute=True):
            self._materializer(self._result(self._line(closing=Decimal("10"), issues=issues))).rebuild(MONTH)
        case = ProductInventoryAuditCase.objects.get()
        self.assertEqual(case.movement_status, ProductInventoryAuditCase.MovementStatus.BALANCED)
        self.assertEqual(set(case.issue_codes), {issue.code for issue in issues})
        self.assertIsNotNone(case.investigated_at)
        self.assertEqual(case.attention_level, ProductInventoryAuditCase.AttentionLevel.GROUPED)

    def test_real_trace_issue_remains_pending_when_stock_balances(self):
        issue = TraceSourceIssue(code="TRANSFER_QUANTITY_MISMATCH", message="Recepción pendiente")
        self._materializer(self._result(self._line(closing=Decimal("10"), issues=(issue,)))).rebuild(MONTH)
        case = ProductInventoryAuditCase.objects.get()
        self.assertEqual(case.movement_status, ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION)

    def test_rebuild_corrects_stale_classification_without_duplicate_case(self):
        materializer = self._materializer(self._result(self._line(closing=Decimal("10"))))
        materializer.rebuild(MONTH)
        case = ProductInventoryAuditCase.objects.get()
        case.movement_status = ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION
        case.save(update_fields=["movement_status", "updated_at"])
        counts = materializer.rebuild(MONTH)
        case.refresh_from_db()
        self.assertEqual(case.movement_status, ProductInventoryAuditCase.MovementStatus.BALANCED)
        self.assertEqual(counts["updated"], 1)
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 1)
        self.assertEqual(materializer.rebuild(MONTH)["unchanged"], 1)

    def test_complete_point_history_closes_missing_conversion_output(self):
        line = replace(
            self._line(closing=Decimal("6")),
            opening=Decimal("23"),
            production=Decimal("518"),
            sales=ZERO,
            transfer_in=Decimal("2"),
            transfer_out=Decimal("529"),
            conversion_in=Decimal("2"),
            conversion_out=ZERO,
            expected_closing=Decimal("16"),
            point_closing=Decimal("6"),
            difference=Decimal("-10"),
        )

        class Client:
            @staticmethod
            def get_stock_history(product_id, branch_id, *, movements=500):
                movements = (
                    (1, "ENTRADA POR PRODUCCIÓN", 518, 23, 541),
                    (2, "ENTRADA POR CONVERSIÓN", 2, 541, 543),
                    (3, "AJUSTE ENTRADA INVENTARIO", 4, 543, 547),
                    (4, "RETORNO POR TRANSFERENCIA", 2, 547, 549),
                    (5, "SALIDA POR CONVERSIÓN", -10, 549, 539),
                    (6, "SALIDA POR TRANSFERENCIA", -533, 539, 6),
                )
                return [
                    {
                        "FK_Movimiento": movement_id,
                        "Movimiento": movement,
                        "Fecha": f"2026-08-{movement_id + 1:02d}T10:00:00-07:00",
                        "Cantidad": quantity,
                        "Existencia_anterior": previous,
                        "Existencia_nueva": new,
                        "Cancelado": False,
                    }
                    for movement_id, movement, quantity, previous, new in movements
                ]

        AuditStockHistoryService(client=Client()).capture(
            self.branch,
            self.product,
            MONTH,
        )

        self._materializer(self._result(line)).rebuild(MONTH)

        case = ProductInventoryAuditCase.objects.get()
        self.assertEqual(case.conversion_out, Decimal("10"))
        self.assertEqual(case.transfer_out, Decimal("533"))
        self.assertEqual(case.identified_adjustment, Decimal("4"))
        self.assertEqual(case.expected_closing, Decimal("6"))
        self.assertEqual(case.point_closing, Decimal("6"))
        self.assertEqual(case.difference, ZERO)
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.BALANCED,
        )
        self.assertEqual(
            case.source_trace["point_history"]["movement_ids_by_category"]
            ["conversion_out"],
            [5],
        )

    def test_cached_point_history_reconciles_existing_case_without_month_rebuild(self):
        line = replace(
            self._line(closing=Decimal("6")),
            opening=Decimal("23"),
            production=Decimal("518"),
            sales=ZERO,
            transfer_in=Decimal("2"),
            transfer_out=Decimal("529"),
            conversion_in=Decimal("2"),
            conversion_out=ZERO,
            expected_closing=Decimal("16"),
            point_closing=Decimal("6"),
            difference=Decimal("-10"),
            issues=(
                TraceSourceIssue(
                    code="TRANSFER_QUANTITY_MISMATCH",
                    message="El agregado no coincide.",
                ),
            ),
        )
        self._materializer(self._result(line)).rebuild(MONTH)
        case = ProductInventoryAuditCase.objects.get()

        class Client:
            @staticmethod
            def get_stock_history(product_id, branch_id, *, movements=500):
                movements = (
                    (1, "ENTRADA POR PRODUCCIÓN", 518, 23, 541),
                    (2, "ENTRADA POR CONVERSIÓN", 2, 541, 543),
                    (3, "AJUSTE ENTRADA INVENTARIO", 4, 543, 547),
                    (4, "RETORNO POR TRANSFERENCIA", 2, 547, 549),
                    (5, "SALIDA POR CONVERSIÓN", 10, 549, 539),
                    (6, "SALIDA POR TRANSFERENCIA", 533, 539, 6),
                )
                return [
                    {
                        "FK_Movimiento": movement_id,
                        "Movimiento": movement,
                        "Fecha": f"2026-08-{movement_id + 1:02d}T10:00:00-07:00",
                        "Cantidad": quantity,
                        "Existencia_anterior": previous,
                        "Existencia_nueva": new,
                        "Cancelado": False,
                    }
                    for movement_id, movement, quantity, previous, new in movements
                ]

        AuditStockHistoryService(client=Client()).capture(
            self.branch,
            self.product,
            MONTH,
        )
        incomplete = MutableTraceabilityService(
            self._result(
                source_complete=False,
                global_issues=(
                    TraceSourceIssue(code="SOURCE_INCOMPLETE", message="Falta fuente"),
                ),
            )
        )

        counts = InventoryAuditMaterializer(
            traceability_service=incomplete
        ).reconcile_existing_cases_from_point_history(MONTH, case_ids=[case.id])

        case.refresh_from_db()
        self.assertEqual(incomplete.months, [])
        self.assertEqual(counts, {"selected": 1, "reconciled": 0, "pending": 1})
        self.assertEqual(case.conversion_out, Decimal("10"))
        self.assertEqual(case.transfer_out, Decimal("533"))
        self.assertEqual(case.identified_adjustment, Decimal("4"))
        self.assertEqual(case.expected_closing, Decimal("6"))
        self.assertEqual(case.difference, ZERO)
        self.assertEqual(case.issue_codes, ["TRANSFER_QUANTITY_MISMATCH"])
        self.assertEqual(case.source_trace["source_issues"][0]["message"], "El agregado no coincide.")
        self.assertEqual(
            case.source_trace["point_history"]["superseded_issue_codes"],
            [],
        )
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
        )

    @patch(
        "reportes.services_inventory_traceability._POINT_HISTORY_BATCH_SIZE",
        1,
    )
    @patch(
        "reportes.services_inventory_traceability."
        "AuditStockHistoryService.reconcile_many",
        return_value={},
    )
    def test_existing_cases_load_cached_histories_in_bounded_batches(
        self, reconcile_many
    ):
        second_branch = PointBranch.objects.create(
            external_id="SUCURSAL-2",
            name="Sucursal 2",
        )
        second_product = PointProduct.objects.create(
            external_id="PASTEL-002",
            sku="PASTEL-002",
            name="Segundo pastel",
        )
        second_line = replace(
            self._line(),
            branch=second_branch,
            product=second_product,
        )
        self._materializer(self._result(self._line(), second_line)).rebuild(MONTH)
        reconcile_many.reset_mock()

        counts = InventoryAuditMaterializer().reconcile_existing_cases_from_point_history(
            MONTH
        )

        self.assertEqual(counts, {"selected": 2, "reconciled": 0, "pending": 2})
        self.assertEqual(reconcile_many.call_count, 2)
        self.assertTrue(all(len(call.args[0]) == 1 for call in reconcile_many.call_args_list))

    def test_unknown_point_history_movement_does_not_replace_aggregate_balance(self):
        line = self._line(closing=Decimal("11"))

        class Client:
            @staticmethod
            def get_stock_history(product_id, branch_id, *, movements=500):
                return [
                    {
                        "FK_Movimiento": 99,
                        "Movimiento": "MOVIMIENTO ESPECIAL",
                        "Fecha": "2026-08-10T10:00:00-07:00",
                        "Cantidad": 1,
                        "Existencia_anterior": 10,
                        "Existencia_nueva": 11,
                        "Cancelado": False,
                    }
                ]

        AuditStockHistoryService(client=Client()).capture(
            self.branch,
            self.product,
            MONTH,
        )

        self._materializer(self._result(line)).rebuild(MONTH)

        case = ProductInventoryAuditCase.objects.get()
        self.assertEqual(case.expected_closing, Decimal("10"))
        self.assertEqual(case.difference, Decimal("1"))
        self.assertNotIn("point_history", case.source_trace)

    @patch("reportes.services_inventory_traceability.transaction.on_commit")
    def test_complete_rebuild_schedules_agent_after_commit(self, on_commit):
        self._materializer(self._result(self._line())).rebuild(MONTH)

        on_commit.assert_called_once()
        self.assertTrue(callable(on_commit.call_args.args[0]))

    @patch("reportes.services_inventory_traceability.transaction.on_commit")
    def test_incomplete_rebuild_does_not_schedule_agent(self, on_commit):
        issue = TraceSourceIssue(code="SOURCE_INCOMPLETE", message="Falta cierre")

        self._materializer(
            self._result(source_complete=False, global_issues=(issue,))
        ).rebuild(MONTH)

        on_commit.assert_not_called()

    def test_rebuild_moves_resolved_alias_case_to_canonical_branch_with_history(self):
        erp_branch = Sucursal.objects.create(codigo="AUD-MAT", nombre="Centro")
        self.branch.external_id = "1"
        self.branch.erp_branch = erp_branch
        self.branch.save(
            update_fields=["external_id", "erp_branch", "updated_at"]
        )
        alias = PointBranch.objects.create(
            external_id="Centro",
            name="Centro alias",
            erp_branch=erp_branch,
        )
        legacy_line = replace(self._line(), branch=alias)
        self._materializer(self._result(legacy_line)).rebuild(MONTH)
        legacy_case = ProductInventoryAuditCase.objects.get(branch=alias)
        self._approve_case(legacy_case)
        original_case_id = legacy_case.id
        original_event_ids = list(legacy_case.events.values_list("id", flat=True))

        counts = self._materializer(self._result(self._line())).rebuild(MONTH)

        legacy_case.refresh_from_db()
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 1)
        self.assertEqual(legacy_case.id, original_case_id)
        self.assertEqual(legacy_case.branch, self.branch)
        self.assertEqual(
            list(legacy_case.events.values_list("id", flat=True)), original_event_ids
        )
        self.assertEqual(
            legacy_case.movement_status,
            ProductInventoryAuditCase.MovementStatus.RESOLVED,
        )
        self.assertEqual(counts["created"], 0)
        self.assertEqual(counts["unchanged"], 1)
        self.assertEqual(counts["reopened"], 0)
        self.assertEqual(counts["source_incomplete"], 0)

    def test_directional_source_trace_is_persisted_without_flattening(self):
        directional_trace = {
            "opening": (11,),
            "closing": (22,),
            "sales": (),
            "production": (),
            "waste": (),
            "transfers": (51, 52),
            "conversions": (61, 62),
            "transfer_in": (51,),
            "transfer_out": (52,),
            "conversion_in": (61,),
            "conversion_out": (62,),
            "adjustments": (),
        }
        materializer = self._materializer(
            self._result(self._line(source_trace=directional_trace))
        )

        materializer.rebuild(MONTH)

        self.assertEqual(
            ProductInventoryAuditCase.objects.get().source_trace,
            {key: list(value) for key, value in directional_trace.items()},
        )

    def test_two_identical_rebuilds_are_idempotent(self):
        materializer = self._materializer(self._result(self._line()))

        first = materializer.rebuild(MONTH)
        second = materializer.rebuild(date(2026, 8, 19))

        self.assertEqual(
            first,
            {
                "created": 1,
                "updated": 0,
                "unchanged": 0,
                "reopened": 0,
                "balanced": 0,
                "exceptions": 1,
                "source_incomplete": 0,
            },
        )
        self.assertEqual(second["created"], 0)
        self.assertEqual(second["updated"], 0)
        self.assertEqual(second["unchanged"], 1)
        self.assertEqual(second["reopened"], 0)
        self.assertEqual(ProductInventoryAuditRun.objects.count(), 1)
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 1)
        self.assertEqual(ProductInventoryAuditEvent.objects.count(), 0)

    def test_identical_fingerprint_preserves_approved_resolution(self):
        materializer = self._materializer(self._result(self._line()))
        materializer.rebuild(MONTH)
        case = self._approve_case(ProductInventoryAuditCase.objects.get())
        event_count = case.events.count()

        counts = materializer.rebuild(MONTH)

        case.refresh_from_db()
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.RESOLVED,
        )
        self.assertEqual(counts["unchanged"], 1)
        self.assertEqual(counts["reopened"], 0)
        self.assertEqual(case.events.count(), event_count)

    def test_directional_projection_upgrade_preserves_approved_resolution(self):
        legacy_trace = {
            "opening": (11,),
            "closing": (22,),
            "sales": (33, 34),
            "production": (44,),
            "waste": (),
            "transfers": (51,),
            "conversions": (61,),
            "adjustments": (),
        }
        service = MutableTraceabilityService(
            self._result(self._line(source_trace=legacy_trace))
        )
        materializer = InventoryAuditMaterializer(traceability_service=service)
        materializer.rebuild(MONTH)
        case = self._approve_case(ProductInventoryAuditCase.objects.get())
        fingerprint = case.calculation_fingerprint
        event_count = case.events.count()
        service.result = self._result(
            self._line(
                source_trace={
                    **legacy_trace,
                    "transfer_in": (51,),
                    "transfer_out": (),
                    "conversion_in": (61,),
                    "conversion_out": (),
                    "conversion_in_impacts": {61: Decimal("12")},
                    "conversion_out_impacts": {},
                }
            )
        )

        counts = materializer.rebuild(MONTH)

        case.refresh_from_db()
        self.assertEqual(case.calculation_fingerprint, fingerprint)
        self.assertEqual(case.movement_status, ProductInventoryAuditCase.MovementStatus.RESOLVED)
        self.assertEqual(case.events.count(), event_count)
        self.assertEqual(counts["unchanged"], 1)
        self.assertEqual(counts["reopened"], 0)
        self.assertEqual(case.source_trace["conversion_in_impacts"], {"61": "12.0000"})

    def test_directional_projection_upgrade_preserves_pending_approval(self):
        legacy_trace = {
            "opening": (11,),
            "closing": (22,),
            "sales": (33, 34),
            "production": (44,),
            "waste": (),
            "transfers": (),
            "conversions": (61,),
            "adjustments": (),
        }
        service = MutableTraceabilityService(
            self._result(self._line(source_trace=legacy_trace))
        )
        materializer = InventoryAuditMaterializer(traceability_service=service)
        materializer.rebuild(MONTH)
        case = ProductInventoryAuditCase.objects.get()
        case.movement_status = ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL
        case.save(update_fields=["movement_status", "updated_at"])
        fingerprint = case.calculation_fingerprint
        service.result = self._result(
            self._line(
                source_trace={
                    **legacy_trace,
                    "transfer_in": (),
                    "transfer_out": (),
                    "conversion_in": (),
                    "conversion_out": (61,),
                    "conversion_in_impacts": {},
                    "conversion_out_impacts": {61: Decimal("1")},
                }
            )
        )

        counts = materializer.rebuild(MONTH)

        case.refresh_from_db()
        self.assertEqual(case.calculation_fingerprint, fingerprint)
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        )
        self.assertEqual(counts["unchanged"], 1)
        self.assertEqual(counts["reopened"], 0)
        self.assertFalse(
            case.events.filter(
                action=ProductInventoryAuditEvent.Action.REOPEN
            ).exists()
        )

    def test_changed_fingerprint_reopens_approved_case_with_system_event(self):
        service = MutableTraceabilityService(self._result(self._line()))
        materializer = InventoryAuditMaterializer(traceability_service=service)
        materializer.rebuild(MONTH)
        case = self._approve_case(ProductInventoryAuditCase.objects.get())
        previous_fingerprint = case.calculation_fingerprint
        service.result = self._result(self._line(closing=Decimal("12")))

        counts = materializer.rebuild(MONTH)

        case.refresh_from_db()
        event = case.events.filter(
            action=ProductInventoryAuditEvent.Action.REOPEN
        ).get()
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
        )
        self.assertEqual(counts["reopened"], 1)
        self.assertEqual(event.reason_code, "SOURCE_FINGERPRINT_CHANGED")
        self.assertIsNone(event.actor)
        self.assertEqual(event.metadata["previous_fingerprint"], previous_fingerprint)
        self.assertEqual(
            event.metadata["new_fingerprint"], case.calculation_fingerprint
        )

    def test_changed_fingerprint_reopens_approved_case_even_if_new_balance_is_zero(self):
        service = MutableTraceabilityService(self._result(self._line()))
        materializer = InventoryAuditMaterializer(traceability_service=service)
        materializer.rebuild(MONTH)
        self._approve_case(ProductInventoryAuditCase.objects.get())
        service.result = self._result(self._line(closing=Decimal("10")))

        counts = materializer.rebuild(MONTH)

        case = ProductInventoryAuditCase.objects.get()
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION,
        )
        self.assertEqual(counts["reopened"], 1)
        self.assertEqual(counts["exceptions"], 1)
        self.assertEqual(counts["balanced"], 0)

    def test_required_source_failure_updates_only_header_and_preserves_cases(self):
        service = MutableTraceabilityService(self._result(self._line()))
        materializer = InventoryAuditMaterializer(traceability_service=service)
        materializer.rebuild(MONTH)
        run = ProductInventoryAuditRun.objects.get()
        successful_at = run.last_successful_rebuild_at
        case = ProductInventoryAuditCase.objects.get()
        previous_values = (
            case.point_closing,
            case.calculation_fingerprint,
            case.movement_status,
        )
        issue = TraceSourceIssue(
            code="SOURCE_INCOMPLETE",
            message="Falta cierre Point verificado para 2026-08-31.",
        )
        service.result = self._result(
            source_complete=False,
            global_issues=(issue,),
        )

        counts = materializer.rebuild(MONTH)

        run.refresh_from_db()
        case.refresh_from_db()
        self.assertEqual(run.status, ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE)
        self.assertEqual(run.source_issues[0]["code"], "SOURCE_INCOMPLETE")
        self.assertEqual(run.last_successful_rebuild_at, successful_at)
        self.assertEqual(
            (
                case.point_closing,
                case.calculation_fingerprint,
                case.movement_status,
            ),
            previous_values,
        )
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 1)
        self.assertEqual(counts["source_incomplete"], 1)
        self.assertEqual(counts["unchanged"], 1)

    def test_case_missing_from_complete_rebuild_is_retained_as_source_incomplete(self):
        service = MutableTraceabilityService(self._result(self._line()))
        materializer = InventoryAuditMaterializer(traceability_service=service)
        materializer.rebuild(MONTH)
        service.result = self._result()

        counts = materializer.rebuild(MONTH)

        case = ProductInventoryAuditCase.objects.get()
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
        )
        self.assertIn("CASE_MISSING_FROM_REBUILD", case.issue_codes)
        self.assertEqual(counts["updated"], 1)
        self.assertEqual(counts["source_incomplete"], 1)

    def test_fingerprint_is_stable_for_reordered_issues_and_source_ids(self):
        first_issues = (
            TraceSourceIssue("B", "Segundo", self.branch.id, self.product.id, (8, 7)),
            TraceSourceIssue("A", "Primero", self.branch.id, self.product.id, (6,)),
        )
        first_trace = {
            "closing": (22,),
            "opening": (11,),
            "sales": (34, 33),
            "production": (44,),
            "waste": (),
            "transfers": (),
            "conversions": (),
            "adjustments": (),
        }
        service = MutableTraceabilityService(
            self._result(self._line(issues=first_issues, source_trace=first_trace))
        )
        materializer = InventoryAuditMaterializer(traceability_service=service)
        materializer.rebuild(MONTH)
        fingerprint = ProductInventoryAuditCase.objects.get().calculation_fingerprint
        service.result = self._result(
            self._line(
                issues=tuple(reversed(first_issues)),
                source_trace={
                    **first_trace,
                    "sales": tuple(reversed(first_trace["sales"])),
                },
            )
        )

        counts = materializer.rebuild(MONTH)

        self.assertEqual(counts["unchanged"], 1)
        self.assertEqual(
            ProductInventoryAuditCase.objects.get().calculation_fingerprint,
            fingerprint,
        )

    def test_line_with_source_issue_is_classified_source_incomplete(self):
        issue = TraceSourceIssue(
            code="SOURCE_INCOMPLETE",
            message="Falta evidencia de transferencia.",
            branch_id=self.branch.id,
            product_id=self.product.id,
            source_ids=(91,),
        )

        counts = self._materializer(
            self._result(self._line(closing=Decimal("10"), issues=(issue,)))
        ).rebuild(MONTH)

        case = ProductInventoryAuditCase.objects.get()
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
        )
        self.assertEqual(counts["source_incomplete"], 1)
        self.assertEqual(counts["balanced"], 0)
        self.assertEqual(
            ProductInventoryAuditRun.objects.get().status,
            ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE,
        )

    def test_dry_run_returns_counts_without_any_database_write(self):
        counts = self._materializer(self._result(self._line())).rebuild(
            MONTH,
            dry_run=True,
        )

        self.assertEqual(counts["created"], 1)
        self.assertEqual(counts["exceptions"], 1)
        self.assertEqual(
            set(counts),
            {
                "created",
                "updated",
                "unchanged",
                "reopened",
                "balanced",
                "exceptions",
                "source_incomplete",
            },
        )
        self.assertFalse(ProductInventoryAuditRun.objects.exists())
        self.assertFalse(ProductInventoryAuditCase.objects.exists())
        self.assertFalse(ProductInventoryAuditEvent.objects.exists())

    def test_incomplete_source_dry_run_does_not_write_header(self):
        issue = TraceSourceIssue(
            code="SOURCE_INCOMPLETE",
            message="Falta cierre Point verificado para 2026-08-31.",
        )

        counts = self._materializer(
            self._result(source_complete=False, global_issues=(issue,))
        ).rebuild(MONTH, dry_run=True)

        self.assertFalse(counts.required_sources_available)
        self.assertEqual(counts["source_incomplete"], 1)
        self.assertFalse(ProductInventoryAuditRun.objects.exists())
        self.assertFalse(ProductInventoryAuditCase.objects.exists())

    def test_quantizes_before_classification_fingerprint_and_persistence(self):
        service = MutableTraceabilityService(
            self._result(self._line(closing=Decimal("10.00001")))
        )
        materializer = InventoryAuditMaterializer(traceability_service=service)

        first = materializer.rebuild(MONTH)
        case = ProductInventoryAuditCase.objects.get()
        first_fingerprint = case.calculation_fingerprint
        service.result = self._result(self._line(closing=Decimal("10.00000")))
        second = materializer.rebuild(MONTH)

        case.refresh_from_db()
        self.assertEqual(case.point_closing, Decimal("10.0000"))
        self.assertEqual(case.difference, Decimal("0.0000"))
        self.assertEqual(
            case.movement_status,
            ProductInventoryAuditCase.MovementStatus.BALANCED,
        )
        self.assertEqual(first["balanced"], 1)
        self.assertEqual(first["exceptions"], 0)
        self.assertEqual(second["unchanged"], 1)
        self.assertEqual(case.calculation_fingerprint, first_fingerprint)

    def test_run_timestamps_bracket_source_build(self):
        service = TimestampTraceabilityService(self._result(self._line()))

        InventoryAuditMaterializer(traceability_service=service).rebuild(MONTH)

        run = ProductInventoryAuditRun.objects.get()
        self.assertLessEqual(run.started_at, service.build_started_at)
        self.assertGreaterEqual(run.rebuilt_at, service.build_finished_at)


class RebuildProductInventoryAuditCommandTests(TraceabilityTestFixtures, TestCase):
    def test_command_explicit_partial_publication_retains_incomplete_status(self):
        issue = TraceSourceIssue(code="SOURCE_INCOMPLETE", message="Falta cierre",
                                 branch_id=self.branch.pk, product_id=self.product.pk)
        fake = MutableTraceabilityService(self._result(
            self._line(issues=(issue,)), source_complete=False,
        ))
        with patch("reportes.services_inventory_traceability.BranchInventoryTraceabilityService",
                   return_value=fake):
            call_command("rebuild_product_inventory_audit", month="2026-08", allow_partial=True,
                         stdout=StringIO())
        self.assertTrue(ProductInventoryAuditRun.objects.get(month=MONTH).partial_published)
        self.assertEqual(ProductInventoryAuditCase.objects.count(), 1)

    def test_command_rejects_non_strict_month_format(self):
        for invalid in ("2026-8", "2026-08-01", "08-2026", "2026/08"):
            with self.subTest(invalid=invalid), self.assertRaises(CommandError):
                call_command("rebuild_product_inventory_audit", month=invalid)

    def test_command_prints_exact_counters_and_dry_run_writes_nothing(self):
        fake = MutableTraceabilityService(self._result(self._line()))
        stdout = StringIO()

        with patch(
            "reportes.services_inventory_traceability.BranchInventoryTraceabilityService",
            return_value=fake,
        ):
            call_command(
                "rebuild_product_inventory_audit",
                month="2026-08",
                dry_run=True,
                stdout=stdout,
            )

        output = stdout.getvalue()
        for key in (
            "created",
            "updated",
            "unchanged",
            "reopened",
            "balanced",
            "exceptions",
            "source_incomplete",
        ):
            self.assertIn(f"{key}=", output)
        self.assertFalse(ProductInventoryAuditRun.objects.exists())

    def test_command_errors_for_missing_required_closing_after_recording_header(self):
        issue = TraceSourceIssue(
            code="SOURCE_INCOMPLETE",
            message="Falta cierre Point verificado para 2026-08-31.",
        )
        fake = MutableTraceabilityService(
            self._result(source_complete=False, global_issues=(issue,))
        )

        with patch(
            "reportes.services_inventory_traceability.BranchInventoryTraceabilityService",
            return_value=fake,
        ), self.assertRaisesMessage(CommandError, "fuentes requeridas"):
            call_command("rebuild_product_inventory_audit", month="2026-08")

        run = ProductInventoryAuditRun.objects.get(month=MONTH)
        self.assertEqual(run.status, ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE)
        self.assertFalse(ProductInventoryAuditCase.objects.exists())

    def test_command_does_not_error_when_required_closings_exist_but_case_is_incomplete(self):
        issue = TraceSourceIssue(
            code="SOURCE_INCOMPLETE",
            message="Falta evidencia de un movimiento.",
            branch_id=self.branch.id,
            product_id=self.product.id,
            source_ids=(91,),
        )
        fake = MutableTraceabilityService(
            self._result(self._line(issues=(issue,)), source_complete=True)
        )

        with patch(
            "reportes.services_inventory_traceability.BranchInventoryTraceabilityService",
            return_value=fake,
        ):
            call_command("rebuild_product_inventory_audit", month="2026-08")

        self.assertEqual(
            ProductInventoryAuditRun.objects.get().status,
            ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE,
        )


class MonthlyAuditLockConcurrencyTests(TraceabilityTestFixtures, TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        self.branch = PointBranch.objects.create(external_id="CENTRO", name="Centro")
        self.product = PointProduct.objects.create(
            external_id="PASTEL-001",
            sku="PASTEL-001",
            name="Pastel de prueba",
        )

    def test_same_month_rebuilds_serialize_before_building_source_snapshot(self):
        first_build_entered = Event()
        release_first_build = Event()
        second_backend_ready = Event()
        build_order_lock = Lock()
        build_order = []
        errors = []
        results = {}
        second_backend_pid = {}

        first_result = self._result(self._line(closing=Decimal("11")))
        second_result = self._result(self._line(closing=Decimal("12")))

        class FirstService:
            def build(inner_self, month):
                with build_order_lock:
                    build_order.append("first")
                first_build_entered.set()
                if not release_first_build.wait(timeout=10):
                    raise AssertionError("No se liberó la primera reconstrucción.")
                return first_result

        class SecondService:
            def build(inner_self, month):
                with build_order_lock:
                    build_order.append("second")
                return second_result

        def run_materializer(name, materializer, *, backend_pid=None, ready=None):
            close_old_connections()
            try:
                if backend_pid is not None:
                    connection.ensure_connection()
                    backend_pid["pid"] = connection.connection.get_backend_pid()
                    ready.set()
                results[name] = materializer.rebuild(MONTH)
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)
            finally:
                close_old_connections()

        first_thread = Thread(
            target=run_materializer,
            args=("first", InventoryAuditMaterializer(traceability_service=FirstService())),
        )
        second_thread = Thread(
            target=run_materializer,
            args=(
                "second",
                InventoryAuditMaterializer(traceability_service=SecondService()),
            ),
            kwargs={
                "backend_pid": second_backend_pid,
                "ready": second_backend_ready,
            },
        )

        first_thread.start()
        self.assertTrue(first_build_entered.wait(timeout=10))
        second_thread.start()
        self.assertTrue(second_backend_ready.wait(timeout=10))
        self._wait_until_advisory_lock_is_blocked(second_backend_pid["pid"])
        self.assertEqual(build_order, ["first"])

        release_first_build.set()
        first_thread.join(timeout=10)
        second_thread.join(timeout=10)

        self.assertFalse(first_thread.is_alive())
        self.assertFalse(second_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(build_order, ["first", "second"])
        self.assertEqual(results["first"]["created"], 1)
        self.assertEqual(results["second"]["updated"], 1)
        case = ProductInventoryAuditCase.objects.get()
        run = ProductInventoryAuditRun.objects.get()
        self.assertEqual(case.point_closing, Decimal("12.0000"))
        self.assertEqual(case.run_id, run.id)
        self.assertEqual(run.summary, dict(results["second"]))
        self.assertEqual(run.status, ProductInventoryAuditRun.Status.READY)

    def test_materializer_blocks_transfer_writer_moving_august_row_to_september(self):
        build_entered = Event()
        release_build = Event()
        writer_backend_ready = Event()
        writer_entered = Event()
        source_guard = Lock()
        source = {"closing": Decimal("11")}
        observations = []
        errors = []
        writer_backend_pid = {}
        destination = PointBranch.objects.create(
            external_id="DESTINATION",
            name="Destination",
        )
        august_stamp = datetime(2026, 8, 15, 18, tzinfo=datetime_timezone.utc)
        existing = PointTransferLine.objects.create(
            origin_branch=self.branch,
            destination_branch=destination,
            transfer_external_id="T-1",
            detail_external_id="D-1",
            source_hash="TRANSFER-MATERIALIZER-T-1-D-1",
            registered_at=august_stamp,
            sent_at=august_stamp,
            received_at=august_stamp,
            item_name="Producto de prueba",
            is_cancelled=True,
            is_finalized=True,
        )
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            parameters={
                "start_date": "2026-09-01",
                "end_date": "2026-09-30",
            },
        )
        september_stamp = datetime(2026, 9, 15, 18, tzinfo=datetime_timezone.utc)
        incoming = SimpleNamespace(
            registered_at=september_stamp,
            sent_at=september_stamp,
            received_at=september_stamp,
            transfer_external_id=existing.transfer_external_id,
            detail_external_id=existing.detail_external_id,
            source_hash=existing.source_hash,
            origin_branch={
                "external_id": self.branch.external_id,
                "name": self.branch.name,
                "status": PointBranch.STATUS_ACTIVE,
                "metadata": {},
            },
            destination_branch={
                "external_id": destination.external_id,
                "name": destination.name,
                "status": PointBranch.STATUS_ACTIVE,
                "metadata": {},
            },
            requested_by="Operación",
            sent_by="Operación",
            received_by="Operación",
            item_name="Producto de prueba",
            item_code="TEST",
            unit="PZA",
            unit_cost=Decimal("0"),
            requested_quantity=Decimal("1"),
            sent_quantity=Decimal("1"),
            received_quantity=Decimal("1"),
            is_insumo=False,
            is_received=False,
            is_cancelled=True,
            is_finalized=True,
            is_open=False,
            raw_payload={},
        )

        class CoordinatedSourceService:
            def build(inner_self, month):
                with source_guard:
                    observations.append(source["closing"])
                build_entered.set()
                if not release_build.wait(timeout=10):
                    raise AssertionError("No se liberó la lectura de fuentes.")
                with source_guard:
                    observations.append(source["closing"])
                    closing = source["closing"]
                return self._result(self._line(closing=closing))

        def materialize():
            close_old_connections()
            try:
                InventoryAuditMaterializer(
                    traceability_service=CoordinatedSourceService()
                ).rebuild(MONTH)
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)
            finally:
                close_old_connections()

        def write_source():
            close_old_connections()
            try:
                connection.ensure_connection()
                writer_backend_pid["pid"] = connection.connection.get_backend_pid()
                writer_backend_ready.set()
                PointMovementSyncService().persist_transfer_lines(sync_job, [incoming])
                with source_guard:
                    source["closing"] = Decimal("12")
                writer_entered.set()
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)
            finally:
                close_old_connections()

        materializer_thread = Thread(target=materialize)
        writer_thread = Thread(target=write_source)
        materializer_thread.start()
        self.assertTrue(build_entered.wait(timeout=10))
        writer_thread.start()
        self.assertTrue(writer_backend_ready.wait(timeout=10))
        self._wait_until_advisory_lock_is_blocked(writer_backend_pid["pid"])
        self.assertFalse(writer_entered.is_set())
        self.assertEqual(observations, [Decimal("11")])

        release_build.set()
        materializer_thread.join(timeout=10)
        writer_thread.join(timeout=10)

        self.assertFalse(materializer_thread.is_alive())
        self.assertFalse(writer_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(observations, [Decimal("11"), Decimal("11")])
        self.assertTrue(writer_entered.is_set())
        self.assertEqual(source["closing"], Decimal("12"))
        existing.refresh_from_db()
        self.assertEqual(existing.received_at.month, 9)
        self.assertEqual(
            ProductInventoryAuditCase.objects.get().point_closing,
            Decimal("11.0000"),
        )

    def _wait_until_advisory_lock_is_blocked(self, backend_pid):
        deadline = monotonic() + 10
        while monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks
                        WHERE pid = %s
                          AND locktype = 'advisory'
                          AND NOT granted
                    )
                    """,
                    [backend_pid],
                )
                if cursor.fetchone()[0]:
                    return
            Event().wait(0.01)
        self.fail("La segunda reconstrucción no esperó el candado mensual.")
