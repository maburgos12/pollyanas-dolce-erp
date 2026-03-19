from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from recetas.models import Receta
from recetas.utils.temporalidad import inferir_temporalidad_receta


class Command(BaseCommand):
    help = "Clasifica temporalidad para recetas creadas desde AUTO_POINT_SALES."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="No persiste cambios; solo reporta.")

    def handle(self, *args, **options):
        dry_run = bool(options.get("dry_run"))
        updated = 0
        classified = {"PERMANENTE": 0, "TEMPORAL": 0, "FECHA_ESPECIAL": 0}

        qs = Receta.objects.filter(sheet_name="AUTO_POINT_SALES").order_by("id")
        for receta in qs.iterator(chunk_size=500):
            temporalidad, detalle = inferir_temporalidad_receta(receta.nombre)
            classified[temporalidad] += 1
            if receta.temporalidad == temporalidad and (receta.temporalidad_detalle or "") == detalle[:120]:
                continue
            if not dry_run:
                receta.temporalidad = temporalidad
                receta.temporalidad_detalle = detalle[:120]
                receta.save(update_fields=["temporalidad", "temporalidad_detalle"])
            updated += 1

        payload = {
            "dry_run": dry_run,
            "recipes_seen": qs.count(),
            "recipes_updated": updated,
            "classified": classified,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
