from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo

from pos_bridge.models import (
    PointConversionLine,
    PointInventorySnapshot,
    PointProductionLine,
    PointTransferLine,
    PointWasteLine,
)
from pos_bridge.services.branch_inventory_traceability_service import (
    canonical_point_branch_identity,
)
from ventas.services.sales_canonical_source import official_point_sales_rows_for_range


LOCAL_TZ = ZoneInfo("America/Mazatlan")


class DailyBreakStatus(StrEnum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    INCONCLUSIVE = "INCONCLUSIVE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


@dataclass(frozen=True)
class DailyMovement:
    source: str
    occurred_at: date | datetime
    impact: Decimal
    source_ids: tuple[int, ...]


@dataclass(frozen=True)
class StockCheckpoint:
    captured_at: datetime
    observed: Decimal


@dataclass(frozen=True)
class EvaluatedCheckpoint:
    captured_at: datetime
    observed: Decimal
    minimum: Decimal
    maximum: Decimal
    difference: Decimal | None


@dataclass(frozen=True)
class DailyBreakProjection:
    status: DailyBreakStatus
    last_matching_checkpoint: EvaluatedCheckpoint | None
    first_mismatch_checkpoint: EvaluatedCheckpoint | None
    minimum: Decimal | None
    maximum: Decimal | None
    movement_ids_by_source: dict[str, tuple[int, ...]]
    warnings: tuple[str, ...]

    def as_dict(self) -> dict:
        return {
            "status": self.status.value,
            "last_matching_checkpoint": _serialize_checkpoint(
                self.last_matching_checkpoint
            ),
            "first_mismatch_checkpoint": _serialize_checkpoint(
                self.first_mismatch_checkpoint
            ),
            "minimum": _decimal_text(self.minimum),
            "maximum": _decimal_text(self.maximum),
            "movement_ids_by_source": {
                key: list(value)
                for key, value in sorted(self.movement_ids_by_source.items())
            },
            "warnings": list(self.warnings),
        }


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _serialize_checkpoint(checkpoint: EvaluatedCheckpoint | None) -> dict | None:
    if checkpoint is None:
        return None
    return {
        "captured_at": checkpoint.captured_at.isoformat(),
        "observed": _decimal_text(checkpoint.observed),
        "minimum": _decimal_text(checkpoint.minimum),
        "maximum": _decimal_text(checkpoint.maximum),
        "difference": _decimal_text(checkpoint.difference),
    }


def _local_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=LOCAL_TZ)
    return value.astimezone(LOCAL_TZ)


def _movement_ids(movements) -> dict[str, tuple[int, ...]]:
    grouped: dict[str, list[int]] = {}
    for movement in movements:
        grouped.setdefault(movement.source, []).extend(movement.source_ids)
    return {
        source: tuple(dict.fromkeys(ids))
        for source, ids in sorted(grouped.items())
    }


def project_checkpoints(
    *,
    opening: Decimal,
    movements: tuple[DailyMovement, ...],
    checkpoints: tuple[StockCheckpoint, ...],
) -> DailyBreakProjection:
    if not checkpoints:
        return DailyBreakProjection(
            status=DailyBreakStatus.INSUFFICIENT_EVIDENCE,
            last_matching_checkpoint=None,
            first_mismatch_checkpoint=None,
            minimum=None,
            maximum=None,
            movement_ids_by_source={},
            warnings=("No existen cortes intermedios de Point para este caso.",),
        )

    ordered_checkpoints = sorted(
        checkpoints, key=lambda checkpoint: _local_datetime(checkpoint.captured_at)
    )
    ordered_movements = sorted(
        movements,
        key=lambda movement: (
            movement.occurred_at.date()
            if isinstance(movement.occurred_at, datetime)
            else movement.occurred_at,
            _local_datetime(movement.occurred_at)
            if isinstance(movement.occurred_at, datetime)
            else datetime.min.replace(tzinfo=LOCAL_TZ),
            movement.source,
            movement.source_ids,
        ),
    )
    last_matching = None
    last_evaluated = None
    saw_ambiguous_range = False

    for checkpoint in ordered_checkpoints:
        captured_at = _local_datetime(checkpoint.captured_at)
        captured_date = captured_at.date()
        completed = []
        same_day_without_time = []
        for movement in ordered_movements:
            occurred_at = movement.occurred_at
            if isinstance(occurred_at, datetime):
                if _local_datetime(occurred_at) <= captured_at:
                    completed.append(movement)
            elif occurred_at < captured_date:
                completed.append(movement)
            elif occurred_at == captured_date:
                same_day_without_time.append(movement)

        base = Decimal(opening) + sum(
            (Decimal(movement.impact) for movement in completed), Decimal("0")
        )
        minimum = base + sum(
            (
                Decimal(movement.impact)
                for movement in same_day_without_time
                if movement.impact < 0
            ),
            Decimal("0"),
        )
        maximum = base + sum(
            (
                Decimal(movement.impact)
                for movement in same_day_without_time
                if movement.impact > 0
            ),
            Decimal("0"),
        )
        observed = Decimal(checkpoint.observed)
        difference = None
        if observed < minimum:
            difference = observed - minimum
        elif observed > maximum:
            difference = observed - maximum
        elif minimum == maximum:
            difference = Decimal("0")
        else:
            saw_ambiguous_range = True

        evaluated = EvaluatedCheckpoint(
            captured_at=captured_at,
            observed=observed,
            minimum=minimum,
            maximum=maximum,
            difference=difference,
        )
        considered = (*completed, *same_day_without_time)
        if observed < minimum or observed > maximum:
            return DailyBreakProjection(
                status=DailyBreakStatus.FOUND,
                last_matching_checkpoint=last_matching,
                first_mismatch_checkpoint=evaluated,
                minimum=minimum,
                maximum=maximum,
                movement_ids_by_source=_movement_ids(considered),
                warnings=(),
            )
        last_matching = evaluated
        last_evaluated = evaluated

    return DailyBreakProjection(
        status=(
            DailyBreakStatus.INCONCLUSIVE
            if saw_ambiguous_range
            else DailyBreakStatus.NOT_FOUND
        ),
        last_matching_checkpoint=last_matching,
        first_mismatch_checkpoint=None,
        minimum=last_evaluated.minimum,
        maximum=last_evaluated.maximum,
        movement_ids_by_source=_movement_ids(ordered_movements),
        warnings=(),
    )


class DailyInventoryBreakService:
    TRACE_KEYS = (
        "sales",
        "production",
        "waste",
        "transfers",
        "transfer_in",
        "transfer_out",
        "conversions",
        "conversion_in",
        "conversion_out",
    )

    def build_month(self, month: date, cases) -> dict[int, DailyBreakProjection]:
        month = month.replace(day=1)
        cases = list(cases)
        if not cases:
            return {}
        month_end = date(month.year, month.month, monthrange(month.year, month.month)[1])
        lower_bound = datetime.combine(month, time.min, tzinfo=LOCAL_TZ)
        upper_bound = datetime.combine(month_end + timedelta(days=1), time.min, tzinfo=LOCAL_TZ)
        aliases, _branches = canonical_point_branch_identity()
        trace_ids = self._collect_trace_ids(cases)

        rows = {
            "sales": {
                row.id: row
                for row in official_point_sales_rows_for_range(
                    start_date=month, end_date=month_end
                ).filter(id__in=trace_ids["sales"])
            },
            "production": {
                row.id: row
                for row in PointProductionLine.objects.filter(
                    id__in=trace_ids["production"]
                )
            },
            "waste": {
                row.id: row
                for row in PointWasteLine.objects.filter(id__in=trace_ids["waste"])
            },
            "transfers": {
                row.id: row
                for row in PointTransferLine.objects.filter(
                    id__in=trace_ids["transfers"]
                )
            },
            "conversions": {
                row.id: row
                for row in PointConversionLine.objects.filter(
                    id__in=trace_ids["conversions"]
                )
            },
        }

        canonical_branch_ids = {aliases.get(case.branch_id, case.branch_id) for case in cases}
        relevant_branch_ids = {
            branch_id
            for branch_id, canonical_id in aliases.items()
            if canonical_id in canonical_branch_ids
        }
        product_ids = {case.product_id for case in cases}
        snapshots = PointInventorySnapshot.objects.filter(
            branch_id__in=relevant_branch_ids,
            product_id__in=product_ids,
            captured_at__gte=lower_bound,
            captured_at__lt=upper_bound,
        ).order_by("captured_at", "id")
        checkpoints_by_key: dict[tuple[int, int], dict[date, StockCheckpoint]] = {}
        for snapshot in snapshots:
            captured_at = _local_datetime(snapshot.captured_at)
            key = (aliases.get(snapshot.branch_id, snapshot.branch_id), snapshot.product_id)
            checkpoints_by_key.setdefault(key, {})[captured_at.date()] = StockCheckpoint(
                captured_at=captured_at,
                observed=Decimal(snapshot.stock),
            )

        results = {}
        for case in cases:
            canonical_branch_id = aliases.get(case.branch_id, case.branch_id)
            movements, warnings = self._case_movements(
                case=case,
                canonical_branch_id=canonical_branch_id,
                aliases=aliases,
                rows=rows,
            )
            checkpoints = tuple(
                checkpoints_by_key.get(
                    (canonical_branch_id, case.product_id), {}
                ).values()
            )
            projection = project_checkpoints(
                opening=Decimal(case.opening_point),
                movements=tuple(movements),
                checkpoints=checkpoints,
            )
            if warnings:
                projection = replace(
                    projection,
                    warnings=tuple(dict.fromkeys((*projection.warnings, *warnings))),
                )
            results[case.id] = projection
        return results

    def _collect_trace_ids(self, cases) -> dict[str, set[int]]:
        collected = {key: set() for key in self.TRACE_KEYS}
        for case in cases:
            trace = case.source_trace or {}
            for key in self.TRACE_KEYS:
                collected[key].update(int(value) for value in trace.get(key, ()))
        collected["transfers"].update(collected["transfer_in"])
        collected["transfers"].update(collected["transfer_out"])
        collected["conversions"].update(collected["conversion_in"])
        collected["conversions"].update(collected["conversion_out"])
        return collected

    def _case_movements(self, *, case, canonical_branch_id, aliases, rows):
        trace = case.source_trace or {}
        movements: list[DailyMovement] = []
        warnings: list[str] = []

        self._append_rows(
            movements,
            warnings,
            source="sales",
            ids=trace.get("sales", ()),
            rows=rows["sales"],
            movement=lambda row: DailyMovement(
                "sales", row.sale_date, -Decimal(row.quantity), (row.id,)
            ),
        )
        self._append_rows(
            movements,
            warnings,
            source="production",
            ids=trace.get("production", ()),
            rows=rows["production"],
            movement=lambda row: DailyMovement(
                "production",
                row.production_date,
                Decimal(row.produced_quantity),
                (row.id,),
            ),
        )
        self._append_rows(
            movements,
            warnings,
            source="waste",
            ids=trace.get("waste", ()),
            rows=rows["waste"],
            movement=lambda row: DailyMovement(
                "waste", row.movement_at, -Decimal(row.quantity), (row.id,)
            ),
        )

        transfer_in_ids = {int(value) for value in trace.get("transfer_in", ())}
        transfer_out_ids = {int(value) for value in trace.get("transfer_out", ())}
        for transfer_id in sorted(transfer_in_ids | transfer_out_ids):
            row = rows["transfers"].get(transfer_id)
            if row is None:
                warnings.append(f"No se encontró la transferencia conservada #{transfer_id}.")
                continue
            if transfer_id in transfer_out_ids:
                occurred_at = row.sent_at or (row.registered_at if row.is_finalized else None)
                if occurred_at is not None:
                    movements.append(
                        DailyMovement(
                            "transfer_out",
                            occurred_at,
                            -Decimal(row.sent_quantity),
                            (row.id,),
                        )
                    )
            if transfer_id not in transfer_in_ids or row.received_at is None:
                continue
            origin_id = aliases.get(row.origin_branch_id, row.origin_branch_id)
            destination_id = aliases.get(
                row.destination_branch_id, row.destination_branch_id
            )
            if destination_id == canonical_branch_id and row.is_received:
                movements.append(
                    DailyMovement(
                        "transfer_in",
                        row.received_at,
                        Decimal(row.received_quantity),
                        (row.id,),
                    )
                )
            returned = Decimal(row.sent_quantity) - Decimal(row.received_quantity)
            if origin_id == canonical_branch_id and row.is_finalized and returned > 0:
                movements.append(
                    DailyMovement(
                        "transfer_return", row.received_at, returned, (row.id,)
                    )
                )

        conversion_in_ids = {int(value) for value in trace.get("conversion_in", ())}
        conversion_out_ids = {int(value) for value in trace.get("conversion_out", ())}
        conversion_in_impacts = trace.get("conversion_in_impacts", {})
        conversion_out_impacts = trace.get("conversion_out_impacts", {})
        for conversion_id in sorted(conversion_in_ids | conversion_out_ids):
            row = rows["conversions"].get(conversion_id)
            if row is None:
                warnings.append(f"No se encontró la conversión conservada #{conversion_id}.")
                continue
            if conversion_id in conversion_in_ids:
                impact = Decimal(
                    str(conversion_in_impacts.get(str(conversion_id), row.quantity))
                )
                movements.append(
                    DailyMovement(
                        "conversion_in", row.movement_at, impact, (row.id,)
                    )
                )
            if conversion_id in conversion_out_ids:
                raw_impact = conversion_out_impacts.get(str(conversion_id))
                if raw_impact is None:
                    warnings.append(
                        f"La conversión #{conversion_id} no identifica su impacto de salida."
                    )
                    continue
                movements.append(
                    DailyMovement(
                        "conversion_out",
                        row.movement_at,
                        -Decimal(str(raw_impact)),
                        (row.id,),
                    )
                )
        return movements, warnings

    @staticmethod
    def _append_rows(movements, warnings, *, source, ids, rows, movement):
        for source_id in sorted({int(value) for value in ids}):
            row = rows.get(source_id)
            if row is None:
                warnings.append(
                    f"No se encontró el movimiento conservado de {source} #{source_id}."
                )
            else:
                movements.append(movement(row))
