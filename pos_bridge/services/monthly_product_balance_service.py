from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from django.utils import timezone

from pos_bridge.models import PointConversionLine
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from recetas.models import RecetaEquivalencia

ZERO = Decimal("0")

ORIGIN_POINT = "POINT"
ORIGIN_CONFIGURED_EQUIVALENCE = "EQUIVALENCIA_CONFIGURADA"
ORIGIN_UNRESOLVED = "UNRESOLVED"
ORIGIN_MIXED = "MIXED"
ISSUE_CONVERSION_ORIGIN_UNRESOLVED = "CONVERSION_ORIGIN_UNRESOLVED"


def _empty_counts() -> Mapping[str, int]:
    return MappingProxyType({})


@dataclass(frozen=True, slots=True)
class MonthlyPointBalanceRow:
    receta_id: int
    opening: Decimal = ZERO
    production: Decimal = ZERO
    sales: Decimal = ZERO
    waste: Decimal = ZERO
    conversion_in: Decimal = ZERO
    conversion_out: Decimal = ZERO
    conversion_origin: str = ""
    issues: tuple[str, ...] = ()
    source_counts: Mapping[str, int] = field(default_factory=_empty_counts)


@dataclass(frozen=True, slots=True)
class MonthlyPointBalance:
    month_start: date
    month_end: date
    rows: tuple[MonthlyPointBalanceRow, ...] = ()
    issues: tuple[str, ...] = ()
    source_counts: Mapping[str, int] = field(default_factory=_empty_counts)

    def row_for(self, receta_id: int) -> MonthlyPointBalanceRow:
        for row in self.rows:
            if row.receta_id == receta_id:
                return row
        return MonthlyPointBalanceRow(receta_id=receta_id)


@dataclass(slots=True)
class _MutableBalanceRow:
    conversion_in: Decimal = ZERO
    conversion_out: Decimal = ZERO
    origins: set[str] = field(default_factory=set)
    issues: set[str] = field(default_factory=set)
    source_counts: dict[str, int] = field(default_factory=dict)

    def record_origin(self, origin: str) -> None:
        self.origins.add(origin)
        self.source_counts[origin] = self.source_counts.get(origin, 0) + 1

    def freeze(self, receta_id: int) -> MonthlyPointBalanceRow:
        if len(self.origins) == 1:
            conversion_origin = next(iter(self.origins))
        elif self.origins:
            conversion_origin = ORIGIN_MIXED
        else:
            conversion_origin = ""
        return MonthlyPointBalanceRow(
            receta_id=receta_id,
            conversion_in=self.conversion_in,
            conversion_out=self.conversion_out,
            conversion_origin=conversion_origin,
            issues=tuple(sorted(self.issues)),
            source_counts=MappingProxyType(dict(sorted(self.source_counts.items()))),
        )


class MonthlyPointProductBalanceService:
    def __init__(self, identity_service: PointRecipeIdentityService | None = None):
        self.identity_service = identity_service or PointRecipeIdentityService()

    def build(self, month: str | date) -> MonthlyPointBalance:
        month_start = self._parse_month(month)
        month_end = date(
            month_start.year,
            month_start.month,
            monthrange(month_start.year, month_start.month)[1],
        )
        rows, source_counts = self._load_conversions(month_start=month_start)
        frozen_rows = tuple(rows[receta_id].freeze(receta_id) for receta_id in sorted(rows))
        issues = tuple(sorted({issue for row in frozen_rows for issue in row.issues}))
        return MonthlyPointBalance(
            month_start=month_start,
            month_end=month_end,
            rows=frozen_rows,
            issues=issues,
            source_counts=MappingProxyType(dict(sorted(source_counts.items()))),
        )

    def _load_conversions(
        self,
        *,
        month_start: date,
    ) -> tuple[dict[int, _MutableBalanceRow], dict[str, int]]:
        next_month = (
            date(month_start.year + 1, 1, 1)
            if month_start.month == 12
            else date(month_start.year, month_start.month + 1, 1)
        )
        current_timezone = timezone.get_current_timezone()
        lower_bound = timezone.make_aware(datetime.combine(month_start, time.min), current_timezone)
        upper_bound = timezone.make_aware(datetime.combine(next_month, time.min), current_timezone)
        conversions = list(
            PointConversionLine.objects.filter(
                movement_at__gte=lower_bound,
                movement_at__lt=upper_bound,
                receta_id__isnull=False,
            ).order_by("movement_at", "id")
        )
        destination_ids = {conversion.receta_id for conversion in conversions}
        equivalences = {
            equivalence.receta_porcion_id: equivalence
            for equivalence in RecetaEquivalencia.objects.filter(
                receta_porcion_id__in=destination_ids,
                tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
                activo=True,
                factor_conversion__gt=ZERO,
            ).select_related("receta_padre")
        }

        result: dict[int, _MutableBalanceRow] = {}
        source_counts: dict[str, int] = {}
        for conversion in conversions:
            destination = result.setdefault(conversion.receta_id, _MutableBalanceRow())
            destination.conversion_in += Decimal(conversion.quantity)
            equivalence = equivalences.get(conversion.receta_id)
            source_recipe_id, factor, origin = self._resolve_source(conversion, equivalence)
            destination.record_origin(origin)
            source_counts[origin] = source_counts.get(origin, 0) + 1

            if source_recipe_id is None or factor is None:
                destination.issues.add(ISSUE_CONVERSION_ORIGIN_UNRESOLVED)
                continue

            source = result.setdefault(source_recipe_id, _MutableBalanceRow())
            source.conversion_out += Decimal(conversion.quantity) / factor
            source.record_origin(origin)

        return result, source_counts

    def _resolve_source(
        self,
        conversion: PointConversionLine,
        equivalence: RecetaEquivalencia | None,
    ) -> tuple[int | None, Decimal | None, str]:
        if conversion.source_item_code or conversion.source_item_name:
            point_source = self.identity_service.resolve_recipe(
                point_code=conversion.source_item_code,
                point_name=conversion.source_item_name,
            )
            if point_source is not None and equivalence is not None:
                return point_source.id, Decimal(equivalence.factor_conversion), ORIGIN_POINT

        if equivalence is not None:
            return (
                equivalence.receta_padre_id,
                Decimal(equivalence.factor_conversion),
                ORIGIN_CONFIGURED_EQUIVALENCE,
            )

        return None, None, ORIGIN_UNRESOLVED

    @staticmethod
    def _parse_month(month: str | date) -> date:
        if isinstance(month, datetime):
            month = month.date()
        if isinstance(month, date):
            return month.replace(day=1)
        try:
            parsed = datetime.strptime(month, "%Y-%m").date()
        except (TypeError, ValueError) as exc:
            raise ValueError("month must use YYYY-MM format or be a date") from exc
        return parsed.replace(day=1)
