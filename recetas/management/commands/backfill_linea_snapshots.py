from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Q

from maestros.models import CostoInsumo
from recetas.models import LineaReceta


class Command(BaseCommand):
    help = "Completa costo_unitario_snapshot en líneas con insumo ligado donde falta costo base."

    def handle(self, *args, **options):
        latest_positive_by_insumo: dict[int, object] = {}
        for insumo_id, costo in (
            CostoInsumo.objects.order_by("insumo_id", "-fecha", "-id")
            .values_list("insumo_id", "costo_unitario")
            .iterator(chunk_size=2000)
        ):
            if insumo_id in latest_positive_by_insumo:
                continue
            if costo is None or costo <= 0:
                continue
            latest_positive_by_insumo[insumo_id] = costo

        qs = (
            LineaReceta.objects.select_related("insumo")
            .filter(insumo__isnull=False)
            .filter(Q(costo_unitario_snapshot__isnull=True) | Q(costo_unitario_snapshot__lte=0))
        )
        total = qs.count()
        to_update: list[LineaReceta] = []
        missing_cost = 0

        for linea in qs.iterator(chunk_size=2000):
            latest = latest_positive_by_insumo.get(linea.insumo_id)
            if latest is None:
                missing_cost += 1
                continue
            linea.costo_unitario_snapshot = latest
            to_update.append(linea)

        if to_update:
            LineaReceta.objects.bulk_update(to_update, ["costo_unitario_snapshot"], batch_size=2000)

        self.stdout.write(self.style.SUCCESS("Backfill de snapshots completado"))
        self.stdout.write(f"  - lineas_evaluadas: {total}")
        self.stdout.write(f"  - lineas_actualizadas: {len(to_update)}")
        self.stdout.write(f"  - lineas_sin_costo_fuente: {missing_cost}")
