from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

from django.db import transaction
from django.db.models import Count
from django.utils import timezone

from pos_bridge.services.branch_inventory_traceability_service import (
    BranchInventoryTraceabilityService,
    TraceSourceIssue,
    canonical_point_branch_identity,
)
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService
from pos_bridge.services.product_month_source_mutex import (
    lock_product_month_sources,
)
from reportes.models import (
    PRODUCT_INVENTORY_AUDIT_SUMMARY_KEYS,
    ProductInventoryAuditCase,
    ProductInventoryAuditEvent,
    ProductInventoryAuditRun,
)


_QUANTITY = Decimal("0.0001")
_POINT_HISTORY_BATCH_SIZE = 50
_MISSING_CASE_ISSUE = "CASE_MISSING_FROM_REBUILD"
_LEGACY_FINGERPRINT_TRACE_KEYS = (
    "opening",
    "closing",
    "sales",
    "production",
    "waste",
    "transfers",
    "open_transfer_snapshot_in",
    "open_transfer_snapshot_out",
    "conversions",
    "adjustments",
)
_TRACE_IMPACT_KEYS = (
    "conversion_in_impacts",
    "conversion_out_impacts",
)


class InventoryAuditRebuildCounts(dict):
    """Seven public counters plus non-serialized source availability."""

    def __init__(self, *, required_sources_available: bool):
        super().__init__(
            {key: 0 for key in PRODUCT_INVENTORY_AUDIT_SUMMARY_KEYS}
        )
        self.required_sources_available = required_sources_available


def _empty_counts(*, required_sources_available=True) -> InventoryAuditRebuildCounts:
    return InventoryAuditRebuildCounts(
        required_sources_available=required_sources_available
    )


def _decimal_text(value: Decimal) -> str:
    return format(Decimal(value).quantize(_QUANTITY), "f")


def _issue_payload(issue: TraceSourceIssue) -> dict[str, object]:
    return {
        "code": issue.code,
        "message": issue.message,
        "branch_id": issue.branch_id,
        "product_id": issue.product_id,
        "source_ids": sorted({int(source_id) for source_id in issue.source_ids}),
    }


def _sorted_issue_payloads(issues) -> list[dict[str, object]]:
    payloads = [_issue_payload(issue) for issue in issues]
    return sorted(
        payloads,
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
    )


def _source_trace_payload(source_trace) -> dict[str, object]:
    payload: dict[str, object] = {}
    for source_name, source_value in sorted(source_trace.items()):
        name = str(source_name)
        if name == "point_history":
            payload[name] = source_value if isinstance(source_value, Mapping) else {}
            continue
        if name in _TRACE_IMPACT_KEYS:
            if not isinstance(source_value, Mapping):
                payload[name] = {}
                continue
            payload[name] = {
                str(int(source_id)): _decimal_text(Decimal(quantity))
                for source_id, quantity in sorted(
                    source_value.items(), key=lambda item: int(item[0])
                )
            }
            continue
        payload[name] = sorted({int(source_id) for source_id in source_value})
    return payload


def _fingerprint_source_trace(source_trace: dict[str, object]) -> dict[str, list[int]]:
    """Keep the deployed fingerprint contract independent of UI projections."""
    return {
        source_name: list(source_trace.get(source_name, []))
        for source_name in _LEGACY_FINGERPRINT_TRACE_KEYS
    }


def _sha256(payload: object) -> str:
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class InventoryAuditMaterializer:
    def __init__(self, *, traceability_service=None):
        self.traceability_service = (
            traceability_service or BranchInventoryTraceabilityService()
        )

    def rebuild(self, month: date, dry_run: bool = False) -> dict[str, int]:
        month_start = month.replace(day=1)
        started_at = timezone.now()

        with transaction.atomic():
            # The lock intentionally covers the potentially slow source build. Otherwise
            # two rebuilds can read interleaved snapshots and apply them in reverse order.
            # Dry runs take the same lock so their preview is comparable; the xact lock
            # is released automatically and never persists data.
            self._lock_source_months(month_start)
            traceability = self.traceability_service.build(month_start)
            source_built_at = timezone.now()

            if not traceability.source_complete:
                return self._record_incomplete_run(
                    month=month_start,
                    traceability=traceability,
                    started_at=started_at,
                    dry_run=dry_run,
                )

            point_histories = AuditStockHistoryService().reconcile_many(
                traceability.lines,
                month_start,
            )
            prepared_lines = [
                self._prepare_line(
                    line,
                    point_history=point_histories.get(
                        (line.branch.id, line.product.id)
                    ),
                )
                for line in traceability.lines
            ]
            branch_aliases, _branch_objects = canonical_point_branch_identity()
            existing_cases = self._canonical_existing_cases(
                month=month_start,
                branch_aliases=branch_aliases,
                prepared_lines=prepared_lines,
                dry_run=dry_run,
            )
            counts = self._preview_complete_counts(
                prepared_lines=prepared_lines,
                existing_cases=existing_cases,
            )
            run_fingerprint = self._run_fingerprint(
                month=month_start,
                line_fingerprints=[item["fingerprint"] for item in prepared_lines],
                global_issues=traceability.global_issues,
            )

            if dry_run:
                return counts

            run, _ = ProductInventoryAuditRun.objects.update_or_create(
                month=month_start,
                defaults={
                    "status": (
                        ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE
                        if counts["source_incomplete"]
                        else ProductInventoryAuditRun.Status.READY
                    ),
                    "source_issues": _sorted_issue_payloads(
                        traceability.global_issues
                    ),
                    "summary": counts,
                    "calculation_fingerprint": run_fingerprint,
                    "started_at": started_at,
                    "rebuilt_at": source_built_at,
                },
            )
            seen_keys = set()
            for prepared in prepared_lines:
                key = prepared["key"]
                seen_keys.add(key)
                existing = existing_cases.get(key)
                if (
                    existing is not None
                    and existing.calculation_fingerprint == prepared["fingerprint"]
                ):
                    effective_status = self._effective_status(
                        prepared_status=prepared["movement_status"],
                        existing=existing,
                        unchanged=True,
                    )
                    if existing.movement_status != effective_status:
                        existing.movement_status = effective_status
                        existing.rebuilt_at = source_built_at
                        existing.save(
                            update_fields=["movement_status", "rebuilt_at", "updated_at"]
                        )
                    if existing.source_trace != prepared["source_trace"]:
                        existing.source_trace = prepared["source_trace"]
                        existing.rebuilt_at = source_built_at
                        existing.save(
                            update_fields=["source_trace", "rebuilt_at", "updated_at"]
                        )
                    continue
                self._persist_line(
                    run=run,
                    month=month_start,
                    prepared=prepared,
                    existing=existing,
                    now=source_built_at,
                )

            for key, existing in existing_cases.items():
                if key in seen_keys:
                    continue
                self._mark_missing_case(run=run, case=existing, now=source_built_at)

            rebuilt_at = timezone.now()
            run.rebuilt_at = rebuilt_at
            run.last_successful_rebuild_at = rebuilt_at
            run.save(
                update_fields=[
                    "rebuilt_at",
                    "last_successful_rebuild_at",
                    "updated_at",
                ]
            )
            transaction.on_commit(
                lambda audit_month=month_start: self._investigate_committed_month(
                    audit_month
                )
            )
            return counts

    def reconcile_existing_cases_from_point_history(
        self, month: date, *, case_ids=None
    ) -> dict[str, int]:
        month_start = month.replace(day=1)
        with transaction.atomic():
            self._lock_source_months(month_start)
            branch_aliases, _ = canonical_point_branch_identity()
            candidates = ProductInventoryAuditCase.objects.sold_products().filter(
                month=month_start,
                branch_id__in=set(branch_aliases.values()),
            ).exclude(difference=0)
            if case_ids is not None:
                candidates = candidates.filter(id__in=case_ids)
            selected_ids = list(
                candidates.order_by("id").values_list("id", flat=True)
            )
            counts = {"selected": len(selected_ids), "reconciled": 0, "pending": 0}
            changed = False
            now = timezone.now()

            for offset in range(0, len(selected_ids), _POINT_HISTORY_BATCH_SIZE):
                batch_ids = selected_ids[offset : offset + _POINT_HISTORY_BATCH_SIZE]
                cases = list(
                    ProductInventoryAuditCase.objects.select_for_update()
                    .filter(id__in=batch_ids)
                    .select_related("branch", "product", "run")
                    .order_by("id")
                )
                histories = AuditStockHistoryService().reconcile_many(
                    cases, month_start
                )
                for case in cases:
                    history = histories.get((case.branch_id, case.product_id))
                    if (
                        history is None
                        or history.coverage_status != "COMPLETE"
                        or history.unknown_movement_ids
                        or history.unexplained_remainder(
                            case.opening_point, case.point_closing
                        )
                        != 0
                    ):
                        counts["pending"] += 1
                        continue

                    line = SimpleNamespace(
                        branch=case.branch,
                        product=case.product,
                        opening=case.opening_point,
                        production=case.production,
                        sales=case.sales,
                        waste=case.waste,
                        transfer_in=case.transfer_in,
                        transfer_out=case.transfer_out,
                        conversion_in=case.conversion_in,
                        conversion_out=case.conversion_out,
                        identified_adjustment=case.identified_adjustment,
                        expected_closing=case.expected_closing,
                        point_closing=case.point_closing,
                        difference=case.difference,
                        source_trace=case.source_trace,
                        issues=(),
                    )
                    prepared = self._prepare_line(line, point_history=history)
                    if case.issue_codes:
                        prepared["source_trace"]["point_history"][
                            "superseded_issue_codes"
                        ] = sorted(case.issue_codes)
                    normalized = prepared["normalized_quantities"]
                    previous_fingerprint = case.calculation_fingerprint
                    should_reopen = case.movement_status in {
                        ProductInventoryAuditCase.MovementStatus.RESOLVED,
                        ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
                    }
                    movement_status = self._effective_status(
                        prepared_status=prepared["movement_status"],
                        existing=case,
                        unchanged=False,
                    )
                    for field, value in normalized.items():
                        setattr(case, field, value)
                    case.issue_codes = prepared["issue_codes"]
                    case.source_trace = prepared["source_trace"]
                    case.calculation_fingerprint = prepared["fingerprint"]
                    case.movement_status = movement_status
                    case.rebuilt_at = now
                    case.save(
                        update_fields=[
                            *normalized,
                            "issue_codes",
                            "source_trace",
                            "calculation_fingerprint",
                            "movement_status",
                            "rebuilt_at",
                            "updated_at",
                        ]
                    )
                    changed = True
                    if should_reopen:
                        self._create_reopen_event(
                            case=case,
                            previous_fingerprint=previous_fingerprint,
                            new_fingerprint=prepared["fingerprint"],
                        )
                    if (
                        movement_status
                        == ProductInventoryAuditCase.MovementStatus.BALANCED
                    ):
                        counts["reconciled"] += 1
                    else:
                        counts["pending"] += 1

            if changed:
                transaction.on_commit(
                    lambda audit_month=month_start: self._investigate_committed_month(
                        audit_month
                    )
                )
            return counts

    @staticmethod
    def _investigate_committed_month(month: date) -> None:
        from reportes.services_inventory_audit_agent import InventoryAuditAgent

        InventoryAuditAgent().run_month(month)

    @staticmethod
    def _case_matches_prepared(case, prepared) -> bool:
        normalized = prepared["normalized_quantities"]
        return (
            all(
                Decimal(getattr(case, field)).quantize(_QUANTITY) == value
                for field, value in normalized.items()
            )
            and sorted(case.issue_codes) == prepared["issue_codes"]
            and case.source_trace == prepared["source_trace"]
        )

    def _canonical_existing_cases(
        self, *, month, branch_aliases, prepared_lines, dry_run
    ):
        prepared_by_key = {prepared["key"]: prepared for prepared in prepared_lines}
        event_counts = dict(
            ProductInventoryAuditEvent.objects.filter(case__month=month)
            .values("case_id")
            .annotate(total=Count("id"))
            .values_list("case_id", "total")
        )
        cases = list(
            ProductInventoryAuditCase.objects.sold_products().select_for_update().filter(
                month=month
            )
        )
        for case in cases:
            case.audit_event_count = event_counts.get(case.id, 0)
        grouped = {}
        for case in cases:
            key = (
                branch_aliases.get(case.branch_id, case.branch_id),
                case.product_id,
            )
            grouped.setdefault(key, []).append(case)

        selected = {}
        status_priority = {
            ProductInventoryAuditCase.MovementStatus.RESOLVED: 3,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL: 2,
            ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION: 1,
        }
        for key, candidates in grouped.items():
            canonical_branch_id, _product_id = key
            canonical = next(
                (
                    case
                    for case in candidates
                    if case.branch_id == canonical_branch_id
                ),
                None,
            )
            survivor = canonical or max(
                candidates,
                key=lambda case: (
                    case.audit_event_count,
                    status_priority.get(case.movement_status, 0),
                    case.updated_at,
                    case.id,
                ),
            )
            prepared = prepared_by_key.get(key)
            if (
                not dry_run
                and canonical is None
                and survivor.branch_id != canonical_branch_id
            ):
                update_fields = ["branch", "updated_at"]
                survivor.branch_id = canonical_branch_id
                if prepared is not None and self._case_matches_prepared(
                    survivor, prepared
                ):
                    survivor.calculation_fingerprint = prepared["fingerprint"]
                    update_fields.append("calculation_fingerprint")
                survivor.save(update_fields=update_fields)
            selected[key] = survivor
        return selected

    @staticmethod
    def _lock_source_months(month: date) -> tuple[date, ...]:
        previous_month = (month - timedelta(days=1)).replace(day=1)
        # The opening close belongs to the prior month; movements and the final
        # close belong to the requested month. The shared helper sorts both locks,
        # matching every canonical writer's acquisition order and avoiding deadlocks.
        return lock_product_month_sources([previous_month, month])

    def _record_incomplete_run(
        self,
        *,
        month,
        traceability,
        started_at,
        dry_run,
    ) -> dict[str, int]:
        existing_count = ProductInventoryAuditCase.objects.sold_products().filter(month=month).count()
        counts = _empty_counts(required_sources_available=False)
        counts["unchanged"] = existing_count
        counts["source_incomplete"] = max(1, len(traceability.global_issues))
        issues = _sorted_issue_payloads(traceability.global_issues)
        fingerprint = self._run_fingerprint(
            month=month,
            line_fingerprints=(),
            global_issues=traceability.global_issues,
        )
        if dry_run:
            return counts

        rebuilt_at = timezone.now()
        ProductInventoryAuditRun.objects.update_or_create(
            month=month,
            defaults={
                "status": ProductInventoryAuditRun.Status.SOURCE_INCOMPLETE,
                "source_issues": issues,
                "summary": counts,
                "calculation_fingerprint": fingerprint,
                "started_at": started_at,
                "rebuilt_at": rebuilt_at,
            },
        )
        return counts

    def _prepare_line(self, line, *, point_history=None) -> dict[str, object]:
        issues = _sorted_issue_payloads(line.issues)
        source_trace = _source_trace_payload(line.source_trace)
        normalized_quantities = {
            "opening_point": Decimal(line.opening).quantize(_QUANTITY),
            "production": Decimal(line.production).quantize(_QUANTITY),
            "sales": Decimal(line.sales).quantize(_QUANTITY),
            "waste": Decimal(line.waste).quantize(_QUANTITY),
            "transfer_in": Decimal(line.transfer_in).quantize(_QUANTITY),
            "transfer_out": Decimal(line.transfer_out).quantize(_QUANTITY),
            "conversion_in": Decimal(line.conversion_in).quantize(_QUANTITY),
            "conversion_out": Decimal(line.conversion_out).quantize(_QUANTITY),
            "identified_adjustment": Decimal(line.identified_adjustment).quantize(
                _QUANTITY
            ),
            "expected_closing": Decimal(line.expected_closing).quantize(_QUANTITY),
            "point_closing": Decimal(line.point_closing).quantize(_QUANTITY),
            "difference": Decimal(line.difference).quantize(_QUANTITY),
        }
        history_remainder = (
            point_history.unexplained_remainder(
                Decimal(line.opening),
                Decimal(line.point_closing),
            )
            if point_history is not None
            else None
        )
        if (
            Decimal(line.difference) != 0
            and point_history is not None
            and point_history.coverage_status == "COMPLETE"
            and not point_history.unknown_movement_ids
            and history_remainder == 0
        ):
            history_values = {
                "production": point_history.production,
                "sales": point_history.sales,
                "waste": point_history.waste,
                "transfer_in": point_history.transfer_in,
                "transfer_out": point_history.transfer_out,
                "conversion_in": point_history.conversion_in,
                "conversion_out": point_history.conversion_out,
                "identified_adjustment": point_history.identified_adjustment,
            }
            comparison = {}
            for field, history_value in history_values.items():
                source_value = Decimal(getattr(line, field))
                if source_value != history_value:
                    comparison[field] = {
                        "aggregate": _decimal_text(source_value),
                        "point_history": _decimal_text(history_value),
                        "difference": _decimal_text(history_value - source_value),
                    }
                normalized_quantities[field] = history_value.quantize(_QUANTITY)
            expected = point_history.expected_closing(Decimal(line.opening))
            normalized_quantities["expected_closing"] = expected.quantize(_QUANTITY)
            normalized_quantities["difference"] = (
                Decimal(line.point_closing) - expected
            ).quantize(_QUANTITY)
            point_history_payload = point_history.as_dict(
                opening=Decimal(line.opening),
                point_closing=Decimal(line.point_closing),
            )
            point_history_payload["aggregate_comparison"] = comparison
            source_trace["point_history"] = point_history_payload
        quantities = {
            name: _decimal_text(value)
            for name, value in normalized_quantities.items()
        }
        fingerprint = _sha256(
            {
                "branch_id": line.branch.id,
                "product_id": line.product.id,
                "quantities": quantities,
                "issues": issues,
                "source_trace": _fingerprint_source_trace(source_trace),
            }
        )
        issue_codes = sorted({str(issue["code"]) for issue in issues})
        if "SOURCE_INCOMPLETE" in issue_codes:
            movement_status = ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE
        elif normalized_quantities["difference"] == 0 and not (
            set(issue_codes) - {"PRODUCT_RESOLVED_BY_SKU", "PRODUCT_RESOLVED_BY_NAME"}
        ):
            movement_status = ProductInventoryAuditCase.MovementStatus.BALANCED
        else:
            movement_status = ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION
        return {
            "key": (line.branch.id, line.product.id),
            "line": line,
            "normalized_quantities": normalized_quantities,
            "quantities": quantities,
            "issues": issues,
            "issue_codes": issue_codes,
            "source_trace": source_trace,
            "fingerprint": fingerprint,
            "movement_status": movement_status,
        }

    def _preview_complete_counts(self, *, prepared_lines, existing_cases):
        counts = _empty_counts()
        seen_keys = set()
        for prepared in prepared_lines:
            key = prepared["key"]
            seen_keys.add(key)
            existing = existing_cases.get(key)
            unchanged = (
                existing is not None
                and existing.calculation_fingerprint == prepared["fingerprint"]
            )
            classification_unchanged = (
                unchanged
                and existing.movement_status == self._effective_status(
                    prepared_status=prepared["movement_status"],
                    existing=existing,
                    unchanged=True,
                )
            )
            if existing is None:
                counts["created"] += 1
            elif classification_unchanged:
                counts["unchanged"] += 1
            else:
                counts["updated"] += 1
                if existing.movement_status in {
                    ProductInventoryAuditCase.MovementStatus.RESOLVED,
                    ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
                }:
                    counts["reopened"] += 1
            self._increment_classification(
                counts,
                self._effective_status(
                    prepared_status=prepared["movement_status"],
                    existing=existing,
                    unchanged=unchanged,
                ),
            )

        for key, existing in existing_cases.items():
            if key in seen_keys:
                continue
            missing_fingerprint = self._missing_case_fingerprint(existing)
            if (
                existing.movement_status
                == ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE
                and existing.calculation_fingerprint == missing_fingerprint
                and _MISSING_CASE_ISSUE in existing.issue_codes
            ):
                counts["unchanged"] += 1
            else:
                counts["updated"] += 1
                if existing.movement_status in {
                    ProductInventoryAuditCase.MovementStatus.RESOLVED,
                    ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
                }:
                    counts["reopened"] += 1
            counts["source_incomplete"] += 1
        return counts

    @staticmethod
    def _increment_classification(counts, movement_status):
        if movement_status == ProductInventoryAuditCase.MovementStatus.BALANCED:
            counts["balanced"] += 1
        elif movement_status == ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE:
            counts["source_incomplete"] += 1
        else:
            counts["exceptions"] += 1

    @staticmethod
    def _effective_status(*, prepared_status, existing, unchanged):
        if unchanged and existing.movement_status in {
            ProductInventoryAuditCase.MovementStatus.RESOLVED,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        }:
            return existing.movement_status
        if (
            existing is not None
            and existing.movement_status
            in {
                ProductInventoryAuditCase.MovementStatus.RESOLVED,
                ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
            }
            and prepared_status
            != ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE
        ):
            return ProductInventoryAuditCase.MovementStatus.NEEDS_EXPLANATION
        return prepared_status

    def _persist_line(self, *, run, month, prepared, existing, now):
        line = prepared["line"]
        normalized = prepared["normalized_quantities"]
        movement_status = self._effective_status(
            prepared_status=prepared["movement_status"],
            existing=existing,
            unchanged=False,
        )
        previous_fingerprint = None
        should_reopen = False
        if existing is not None:
            previous_fingerprint = existing.calculation_fingerprint
            should_reopen = existing.movement_status in {
                ProductInventoryAuditCase.MovementStatus.RESOLVED,
                ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
            }

        defaults = {
            "run": run,
            "opening_point": normalized["opening_point"],
            "production": normalized["production"],
            "sales": normalized["sales"],
            "waste": normalized["waste"],
            "transfer_in": normalized["transfer_in"],
            "transfer_out": normalized["transfer_out"],
            "conversion_in": normalized["conversion_in"],
            "conversion_out": normalized["conversion_out"],
            "identified_adjustment": normalized["identified_adjustment"],
            "expected_closing": normalized["expected_closing"],
            "point_closing": normalized["point_closing"],
            "difference": normalized["difference"],
            "point_closing_status": ProductInventoryAuditCase.PointClosingStatus.PROTECTED,
            "movement_status": movement_status,
            "physical_status": ProductInventoryAuditCase.PhysicalStatus.NOT_AVAILABLE,
            "issue_codes": prepared["issue_codes"],
            "source_trace": prepared["source_trace"],
            "calculation_fingerprint": prepared["fingerprint"],
            "rebuilt_at": now,
        }
        if existing is None:
            case = ProductInventoryAuditCase.objects.create(
                month=month,
                branch=line.branch,
                product=line.product,
                **defaults,
            )
        else:
            case = existing
            for field, value in defaults.items():
                setattr(case, field, value)
            case.save(update_fields=[*defaults, "updated_at"])
        if should_reopen:
            self._create_reopen_event(
                case=case,
                previous_fingerprint=previous_fingerprint,
                new_fingerprint=prepared["fingerprint"],
            )

    def _mark_missing_case(self, *, run, case, now):
        new_fingerprint = self._missing_case_fingerprint(case)
        if (
            case.movement_status
            == ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE
            and case.calculation_fingerprint == new_fingerprint
            and _MISSING_CASE_ISSUE in case.issue_codes
        ):
            return
        previous_fingerprint = case.calculation_fingerprint
        should_reopen = case.movement_status in {
            ProductInventoryAuditCase.MovementStatus.RESOLVED,
            ProductInventoryAuditCase.MovementStatus.PENDING_APPROVAL,
        }
        issue_codes = sorted({*case.issue_codes, _MISSING_CASE_ISSUE})
        updated_case, _ = ProductInventoryAuditCase.objects.update_or_create(
            month=case.month,
            branch_id=case.branch_id,
            product_id=case.product_id,
            defaults={
                "run": run,
                "opening_point": case.opening_point,
                "production": case.production,
                "sales": case.sales,
                "waste": case.waste,
                "transfer_in": case.transfer_in,
                "transfer_out": case.transfer_out,
                "conversion_in": case.conversion_in,
                "conversion_out": case.conversion_out,
                "identified_adjustment": case.identified_adjustment,
                "expected_closing": case.expected_closing,
                "point_closing": case.point_closing,
                "difference": case.difference,
                "point_closing_status": case.point_closing_status,
                "movement_status": ProductInventoryAuditCase.MovementStatus.SOURCE_INCOMPLETE,
                "physical_status": case.physical_status,
                "issue_codes": issue_codes,
                "source_trace": case.source_trace,
                "calculation_fingerprint": new_fingerprint,
                "rebuilt_at": now,
            },
        )
        if should_reopen:
            self._create_reopen_event(
                case=updated_case,
                previous_fingerprint=previous_fingerprint,
                new_fingerprint=new_fingerprint,
            )

    @staticmethod
    def _missing_case_fingerprint(case):
        return _sha256(
            {
                "month": case.month.isoformat(),
                "branch_id": case.branch_id,
                "product_id": case.product_id,
                "issue": _MISSING_CASE_ISSUE,
            }
        )

    @staticmethod
    def _create_reopen_event(*, case, previous_fingerprint, new_fingerprint):
        ProductInventoryAuditEvent.objects.create(
            case=case,
            action=ProductInventoryAuditEvent.Action.REOPEN,
            reason_code="SOURCE_FINGERPRINT_CHANGED",
            notes="Las fuentes cambiaron después de la revisión anterior.",
            actor=None,
            metadata={
                "previous_fingerprint": previous_fingerprint,
                "new_fingerprint": new_fingerprint,
            },
        )

    @staticmethod
    def _run_fingerprint(*, month, line_fingerprints, global_issues):
        return _sha256(
            {
                "month": month.isoformat(),
                "line_fingerprints": sorted(line_fingerprints),
                "global_issues": _sorted_issue_payloads(global_issues),
            }
        )
