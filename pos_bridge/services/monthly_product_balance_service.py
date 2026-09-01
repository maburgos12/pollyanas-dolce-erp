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
from recetas.models import Receta, RecetaEquivalencia

ZERO = Decimal("0")

ORIGIN_POINT = "POINT"
ORIGIN_CONFIGURED_EQUIVALENCE = "EQUIVALENCIA_CONFIGURADA"
ORIGIN_UNRESOLVED = "UNRESOLVED"
ORIGIN_MIXED = "MIXED"
ISSUE_CONVERSION_ORIGIN_UNRESOLVED = "CONVERSION_ORIGIN_UNRESOLVED"
ISSUE_POINT_SOURCE_UNRESOLVED = "POINT_CONVERSION_SOURCE_UNRESOLVED"
ISSUE_SOURCE_FACTOR_MISMATCH = "CONVERSION_SOURCE_FACTOR_MISMATCH"
ISSUE_FACTOR_INVALID = "CONVERSION_FACTOR_INVALID"
ISSUE_DESTINATION_UNRESOLVED = "CONVERSION_DESTINATION_UNRESOLVED"


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


def _empty_rows() -> Mapping[int, MonthlyPointBalanceRow]:
    return MappingProxyType({})


@dataclass(frozen=True, slots=True)
class MonthlyPointUnresolvedConversion:
    movement_external_id: str
    source_hash: str
    item_code: str
    item_name: str
    quantity: Decimal
    issue: str = ISSUE_DESTINATION_UNRESOLVED


@dataclass(frozen=True, slots=True)
class MonthlyPointBalance:
    month_start: date
    month_end: date
    rows: Mapping[int, MonthlyPointBalanceRow] = field(default_factory=_empty_rows)
    unresolved_conversions: tuple[MonthlyPointUnresolvedConversion, ...] = ()
    issues: tuple[str, ...] = ()
    source_counts: Mapping[str, int] = field(default_factory=_empty_counts)


@dataclass(slots=True)
class _MutableBalanceRow:
    conversion_in: Decimal = ZERO
    conversion_out: Decimal = ZERO
    origins: set[str] = field(default_factory=set)
    issues: set[str] = field(default_factory=set)
    conversion_in_rows: int = 0
    conversion_out_rows: int = 0

    def record_origin(self, origin: str) -> None:
        self.origins.add(origin)

    def add_conversion_in(self, quantity: Decimal) -> None:
        self.conversion_in += quantity
        self.conversion_in_rows += 1

    def add_conversion_out(self, quantity: Decimal) -> None:
        self.conversion_out += quantity
        self.conversion_out_rows += 1

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
            source_counts=MappingProxyType(
                {
                    "conversion_in_rows": self.conversion_in_rows,
                    "conversion_out_rows": self.conversion_out_rows,
                }
            ),
        )


class MonthlyPointProductBalanceService:
    """Project real Point conversions; negative quantities reverse both sides and zero rows are ignored."""

    def __init__(self, identity_service: PointRecipeIdentityService | None = None):
        self.identity_service = identity_service or PointRecipeIdentityService()

    def build(self, month: str | date) -> MonthlyPointBalance:
        month_start = self._parse_month(month)
        month_end = date(
            month_start.year,
            month_start.month,
            monthrange(month_start.year, month_start.month)[1],
        )
        rows, unresolved_conversions, source_counts = self._load_conversions(month_start=month_start)
        frozen_rows = MappingProxyType(
            {receta_id: rows[receta_id].freeze(receta_id) for receta_id in sorted(rows)}
        )
        issues = tuple(
            sorted(
                {issue for row in frozen_rows.values() for issue in row.issues}
                | {conversion.issue for conversion in unresolved_conversions}
            )
        )
        return MonthlyPointBalance(
            month_start=month_start,
            month_end=month_end,
            rows=frozen_rows,
            unresolved_conversions=tuple(unresolved_conversions),
            issues=issues,
            source_counts=MappingProxyType(source_counts),
        )

    def _load_conversions(
        self,
        *,
        month_start: date,
    ) -> tuple[
        dict[int, _MutableBalanceRow],
        list[MonthlyPointUnresolvedConversion],
        dict[str, int],
    ]:
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
            )
            .only(
                "id",
                "receta_id",
                "movement_external_id",
                "source_hash",
                "movement_at",
                "item_code",
                "item_name",
                "quantity",
                "source_item_code",
                "source_item_name",
            )
            .order_by("movement_at", "id")
        )
        destination_ids = {
            conversion.receta_id for conversion in conversions if conversion.receta_id is not None
        }
        equivalences = {
            equivalence.receta_porcion_id: equivalence
            for equivalence in RecetaEquivalencia.objects.filter(
                receta_porcion_id__in=destination_ids,
                tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
                activo=True,
            )
        }

        result: dict[int, _MutableBalanceRow] = {}
        unresolved_conversions: list[MonthlyPointUnresolvedConversion] = []
        source_counts = {
            "conversion_rows_read": len(conversions),
            "conversion_rows_applied": 0,
        }
        identity_cache: dict[tuple[str, str], Receta | None] = {}
        for conversion in conversions:
            quantity = Decimal(conversion.quantity)
            if conversion.receta_id is None:
                unresolved_conversions.append(
                    MonthlyPointUnresolvedConversion(
                        movement_external_id=conversion.movement_external_id,
                        source_hash=conversion.source_hash,
                        item_code=conversion.item_code,
                        item_name=conversion.item_name,
                        quantity=quantity,
                    )
                )
                continue
            if quantity == ZERO:
                continue

            source_counts["conversion_rows_applied"] += 1
            destination = result.setdefault(conversion.receta_id, _MutableBalanceRow())
            destination.add_conversion_in(quantity)
            equivalence = equivalences.get(conversion.receta_id)
            source_recipe_id, factor, origin, issue = self._resolve_source(
                conversion,
                equivalence,
                identity_cache,
            )
            destination.record_origin(origin)
            if issue:
                destination.issues.add(issue)

            if source_recipe_id is None or factor is None:
                continue

            source = result.setdefault(source_recipe_id, _MutableBalanceRow())
            source.add_conversion_out(quantity / factor)
            source.record_origin(origin)

        return result, unresolved_conversions, source_counts

    def _resolve_source(
        self,
        conversion: PointConversionLine,
        equivalence: RecetaEquivalencia | None,
        identity_cache: dict[tuple[str, str], Receta | None],
    ) -> tuple[int | None, Decimal | None, str, str]:
        point_code = (conversion.source_item_code or "").strip()
        point_name = (conversion.source_item_name or "").strip()
        if point_code or point_name:
            identity_key = (point_code.casefold(), point_name.casefold())
            if identity_key not in identity_cache:
                identity_cache[identity_key] = self.identity_service.resolve_recipe(
                    point_code=point_code,
                    point_name=point_name,
                )
            point_source = identity_cache[identity_key]
            if point_source is None:
                return None, None, ORIGIN_POINT, ISSUE_POINT_SOURCE_UNRESOLVED
            if equivalence is None or equivalence.receta_padre_id != point_source.id:
                return point_source.id, None, ORIGIN_POINT, ISSUE_SOURCE_FACTOR_MISMATCH
            factor = Decimal(equivalence.factor_conversion)
            if factor <= ZERO:
                return point_source.id, None, ORIGIN_POINT, ISSUE_FACTOR_INVALID
            return point_source.id, factor, ORIGIN_POINT, ""

        if equivalence is not None:
            factor = Decimal(equivalence.factor_conversion)
            if factor <= ZERO:
                return (
                    equivalence.receta_padre_id,
                    None,
                    ORIGIN_CONFIGURED_EQUIVALENCE,
                    ISSUE_FACTOR_INVALID,
                )
            return (
                equivalence.receta_padre_id,
                factor,
                ORIGIN_CONFIGURED_EQUIVALENCE,
                "",
            )

        return None, None, ORIGIN_UNRESOLVED, ISSUE_CONVERSION_ORIGIN_UNRESOLVED

    @staticmethod
    def _parse_month(month: str | date) -> date:
        if isinstance(month, datetime):
            raise ValueError("datetime month values are not supported; use a date or YYYY-MM string")
        if isinstance(month, date):
            return month.replace(day=1)
        try:
            parsed = datetime.strptime(month, "%Y-%m").date()
        except (TypeError, ValueError) as exc:
            raise ValueError("month must use YYYY-MM format or be a date") from exc
        return parsed.replace(day=1)
