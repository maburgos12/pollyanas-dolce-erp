from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from django.db import transaction

from recetas.models import Receta, RecetaAgrupacionAddon, RecetaCostoSemanal
from recetas.utils.addon_grouping import calculate_grouped_addon_cost
from recetas.utils.costeo_versionado import asegurar_version_costeo


ZERO = Decimal("0")
Q6 = Decimal("0.000001")


@dataclass(slots=True)
class WeeklySnapshotSummary:
    week_start: date
    week_end: date
    recipes_created: int = 0
    recipes_updated: int = 0
    addons_created: int = 0
    addons_updated: int = 0
    total_items: int = 0


def _q6(value: Decimal | int | float | str | None) -> Decimal:
    return Decimal(str(value or 0)).quantize(Q6)


def week_bounds(anchor: date | None = None) -> tuple[date, date]:
    anchor = anchor or date.today()
    week_start = anchor - timedelta(days=anchor.weekday())
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


def _delta_from_previous(*, identity_key: str, week_start: date, current_total: Decimal) -> tuple[Decimal | None, Decimal | None]:
    previous = (
        RecetaCostoSemanal.objects.filter(identity_key=identity_key, week_start__lt=week_start)
        .order_by("-week_start", "-id")
        .first()
    )
    if previous is None:
        return None, None
    delta_total = _q6(current_total - Decimal(previous.costo_total))
    delta_pct = None
    if Decimal(previous.costo_total) > 0:
        delta_pct = _q6((delta_total / Decimal(previous.costo_total)) * Decimal("100"))
    return delta_total, delta_pct


def snapshot_weekly_costs(
    *,
    anchor_date: date | None = None,
    receta_ids: list[int] | None = None,
    include_recipes: bool = True,
    include_addons: bool = True,
) -> WeeklySnapshotSummary:
    week_start, week_end = week_bounds(anchor_date)
    summary = WeeklySnapshotSummary(week_start=week_start, week_end=week_end)

    recipe_qs = Receta.objects.all().order_by("nombre", "id")
    if receta_ids:
        recipe_qs = recipe_qs.filter(id__in=receta_ids)
    addon_qs = (
        RecetaAgrupacionAddon.objects.filter(
            activo=True,
            status=RecetaAgrupacionAddon.STATUS_APPROVED,
            addon_receta__isnull=False,
        )
        .select_related("base_receta", "addon_receta")
        .order_by("base_receta__nombre", "addon_nombre_point", "id")
    )
    if receta_ids:
        addon_qs = addon_qs.filter(base_receta_id__in=receta_ids)

    with transaction.atomic():
        if include_recipes:
            for receta in recipe_qs:
                version, _ = asegurar_version_costeo(receta, fuente="WEEKLY_SNAPSHOT")
                delta_total, delta_pct = _delta_from_previous(
                    identity_key=f"RECIPE:{receta.id}",
                    week_start=week_start,
                    current_total=Decimal(version.costo_total),
                )
                _, created = RecetaCostoSemanal.objects.update_or_create(
                    identity_key=f"RECIPE:{receta.id}",
                    week_start=week_start,
                    defaults={
                        "scope_type": RecetaCostoSemanal.SCOPE_RECIPE,
                        "label": receta.nombre[:260],
                        "week_end": week_end,
                        "receta": receta,
                        "base_receta": receta,
                        "addon_receta": None,
                        "addon_rule": None,
                        "temporalidad": receta.temporalidad,
                        "temporalidad_detalle": receta.temporalidad_detalle[:120],
                        "familia": (receta.familia or "")[:120],
                        "categoria": (receta.categoria or "")[:120],
                        "costo_mp": version.costo_mp,
                        "costo_mo": version.costo_mo,
                        "costo_indirecto": version.costo_indirecto,
                        "costo_total": version.costo_total,
                        "delta_total": delta_total,
                        "delta_pct": delta_pct,
                        "version_receta": version.version_num,
                        "version_base": None,
                        "version_addon": None,
                        "metadata": {
                            "tipo": receta.tipo,
                            "codigo_point": receta.codigo_point,
                            "sheet_name": receta.sheet_name,
                        },
                    },
                )
                if created:
                    summary.recipes_created += 1
                else:
                    summary.recipes_updated += 1

        if include_addons:
            for rule in addon_qs:
                base_version, _ = asegurar_version_costeo(rule.base_receta, fuente="WEEKLY_SNAPSHOT")
                addon_version, _ = asegurar_version_costeo(rule.addon_receta, fuente="WEEKLY_SNAPSHOT")
                grouped = calculate_grouped_addon_cost(rule=rule)
                temporalidad = rule.addon_receta.temporalidad or rule.base_receta.temporalidad
                temporalidad_detalle = (
                    rule.addon_receta.temporalidad_detalle
                    or rule.base_receta.temporalidad_detalle
                    or rule.notas
                )
                delta_total, delta_pct = _delta_from_previous(
                    identity_key=f"GROUPED_ADDON:{rule.id}",
                    week_start=week_start,
                    current_total=Decimal(grouped.grouped_cost),
                )
                _, created = RecetaCostoSemanal.objects.update_or_create(
                    identity_key=f"GROUPED_ADDON:{rule.id}",
                    week_start=week_start,
                    defaults={
                        "scope_type": RecetaCostoSemanal.SCOPE_GROUPED_ADDON,
                        "label": f"{rule.base_receta.nombre} + {rule.addon_nombre_point}"[:260],
                        "week_end": week_end,
                        "receta": None,
                        "base_receta": rule.base_receta,
                        "addon_receta": rule.addon_receta,
                        "addon_rule": rule,
                        "temporalidad": temporalidad,
                        "temporalidad_detalle": temporalidad_detalle[:120],
                        "familia": (rule.base_receta.familia or "")[:120],
                        "categoria": (rule.base_receta.categoria or "")[:120],
                        "costo_mp": grouped.grouped_cost,
                        "costo_mo": ZERO,
                        "costo_indirecto": ZERO,
                        "costo_total": grouped.grouped_cost,
                        "delta_total": delta_total,
                        "delta_pct": delta_pct,
                        "version_receta": None,
                        "version_base": base_version.version_num,
                        "version_addon": addon_version.version_num,
                        "metadata": {
                            "addon_codigo_point": rule.addon_codigo_point,
                            "addon_nombre_point": rule.addon_nombre_point,
                            "base_cost": str(grouped.base_cost),
                            "addon_cost": str(grouped.addon_cost),
                            "confidence_score": str(rule.confidence_score),
                            "cooccurrence_qty": str(rule.cooccurrence_qty),
                        },
                    },
                )
                if created:
                    summary.addons_created += 1
                else:
                    summary.addons_updated += 1

    summary.total_items = (
        summary.recipes_created
        + summary.recipes_updated
        + summary.addons_created
        + summary.addons_updated
    )
    return summary
