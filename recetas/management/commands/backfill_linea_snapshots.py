from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Q

from maestros.models import CostoInsumo
from recetas.models import LineaReceta


class Command(BaseCommand):
    help = "Completa costo_unitario_snapshot en líneas con insumo ligado donde falta costo base."

    def handle(self, *args, **options):
        qs = (
            LineaReceta.objects.select_related("insumo")
            .filter(insumo__isnull=False)
            .filter(Q(costo_unitario_snapshot__isnull=True) | Q(costo_unitario_snapshot__lte=0))
        )
        total = qs.count()
        updated = 0
        missing_cost = 0

        for linea in qs.iterator(chunk_size=500):
            latest = (
                CostoInsumo.objects.filter(insumo=linea.insumo)
                .order_by("-fecha", "-id")
                .values_list("costo_unitario", flat=True)
                .first()
            )
            if latest is None:
                missing_cost += 1
                continue
            linea.costo_unitario_snapshot = latest
            linea.save(update_fields=["costo_unitario_snapshot"])
            updated += 1

        self.stdout.write(self.style.SUCCESS("Backfill de snapshots completado"))
        self.stdout.write(f"  - lineas_evaluadas: {total}")
        self.stdout.write(f"  - lineas_actualizadas: {updated}")
        self.stdout.write(f"  - lineas_sin_costo_fuente: {missing_cost}")
