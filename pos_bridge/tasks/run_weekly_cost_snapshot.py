from __future__ import annotations

from datetime import date

from recetas.utils.costeo_semanal import snapshot_weekly_costs


def run_weekly_cost_snapshot(
    *,
    anchor_date: date | None = None,
    receta_ids: list[int] | None = None,
    include_recipes: bool = True,
    include_addons: bool = True,
    triggered_by=None,
):
    del triggered_by
    summary = snapshot_weekly_costs(
        anchor_date=anchor_date,
        receta_ids=receta_ids,
        include_recipes=include_recipes,
        include_addons=include_addons,
    )
    return {
        "week_start": summary.week_start.isoformat(),
        "week_end": summary.week_end.isoformat(),
        "recipes_created": summary.recipes_created,
        "recipes_updated": summary.recipes_updated,
        "addons_created": summary.addons_created,
        "addons_updated": summary.addons_updated,
        "total_items": summary.total_items,
    }
