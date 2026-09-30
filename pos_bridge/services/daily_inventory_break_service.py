from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo


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
