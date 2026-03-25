from __future__ import annotations

import json
from datetime import date

from django.core.management.base import BaseCommand, CommandError

from recetas.models import Receta
from recetas.utils.costeo_semanal import snapshot_weekly_costs


class Command(BaseCommand):
    help = "Genera o actualiza el snapshot semanal de costeo de recetas y agrupaciones base+addon."

    def add_arguments(self, parser):
        parser.add_argument("--anchor-date", type=str, default="", help="Fecha de referencia YYYY-MM-DD para la semana.")
        parser.add_argument("--recipe-codes", nargs="*", default=[], help="Códigos Point de recetas a snapshotear.")
        parser.add_argument("--skip-recipes", action="store_true", help="Omitir recetas individuales.")
        parser.add_argument("--skip-addons", action="store_true", help="Omitir agrupaciones base+addon aprobadas.")

    def handle(self, *args, **options):
        anchor_date = None
        raw_date = (options.get("anchor_date") or "").strip()
        if raw_date:
            try:
                anchor_date = date.fromisoformat(raw_date)
            except ValueError as exc:
                raise CommandError("anchor-date debe venir en formato YYYY-MM-DD.") from exc

        recipe_codes = [str(code).strip().upper() for code in options.get("recipe_codes") or [] if str(code).strip()]
        receta_ids = None
        if recipe_codes:
            receta_ids = list(Receta.objects.filter(codigo_point__in=recipe_codes).values_list("id", flat=True))
            missing = sorted(set(recipe_codes) - set(Receta.objects.filter(id__in=receta_ids).values_list("codigo_point", flat=True)))
            if missing:
                raise CommandError(f"No se encontraron recetas para los códigos: {', '.join(missing)}")

        summary = snapshot_weekly_costs(
            anchor_date=anchor_date,
            receta_ids=receta_ids,
            include_recipes=not bool(options.get("skip_recipes")),
            include_addons=not bool(options.get("skip_addons")),
        )
        payload = {
            "week_start": summary.week_start.isoformat(),
            "week_end": summary.week_end.isoformat(),
            "recipes_created": summary.recipes_created,
            "recipes_updated": summary.recipes_updated,
            "addons_created": summary.addons_created,
            "addons_updated": summary.addons_updated,
            "total_items": summary.total_items,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
