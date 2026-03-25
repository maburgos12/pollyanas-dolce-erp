from __future__ import annotations

from django.core.management.base import BaseCommand
from recetas.models import LineaReceta
from recetas.utils.costeo_snapshot import resolve_line_snapshot_cost


class Command(BaseCommand):
    help = "Refresca costo_unitario_snapshot en líneas con insumo ligado usando la mejor fuente vigente."

    def handle(self, *args, **options):
        qs = LineaReceta.objects.select_related("insumo", "unidad").filter(insumo__isnull=False)
        total = qs.count()
        to_update: list[LineaReceta] = []
        missing_cost = 0
        incompatible_unit = 0

        for linea in qs.iterator(chunk_size=2000):
            latest, source = resolve_line_snapshot_cost(linea)
            if latest is None or latest <= 0:
                if "UNIDAD_INCOMPATIBLE" in source:
                    incompatible_unit += 1
                else:
                    missing_cost += 1
                continue
            if linea.costo_unitario_snapshot == latest:
                continue
            linea.costo_unitario_snapshot = latest
            to_update.append(linea)

        if to_update:
            LineaReceta.objects.bulk_update(to_update, ["costo_unitario_snapshot"], batch_size=2000)

        self.stdout.write(self.style.SUCCESS("Backfill de snapshots completado"))
        self.stdout.write(f"  - lineas_evaluadas: {total}")
        self.stdout.write(f"  - lineas_actualizadas: {len(to_update)}")
        self.stdout.write(f"  - lineas_sin_costo_fuente: {missing_cost}")
        self.stdout.write(f"  - lineas_unidad_incompatible: {incompatible_unit}")
