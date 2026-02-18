from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Q

from recetas.models import Receta, LineaReceta


class Command(BaseCommand):
    help = "Audita calidad de datos de recetas importadas."

    def handle(self, *args, **options):
        total_recetas = Receta.objects.count()
        total_lineas = LineaReceta.objects.count()
        sin_match = LineaReceta.objects.filter(
            ~Q(tipo_linea=LineaReceta.TIPO_SUBSECCION),
            costo_linea_excel__isnull=True,
        ).filter(
            Q(insumo__isnull=True) | Q(match_status=LineaReceta.STATUS_NEEDS_REVIEW) | Q(match_status=LineaReceta.STATUS_REJECTED)
        ).count()
        sin_unidad = LineaReceta.objects.filter(
            Q(unidad__isnull=True),
            Q(unidad_texto=""),
            costo_linea_excel__isnull=True,
        ).count()
        sin_cantidad = LineaReceta.objects.filter(
            ~Q(tipo_linea=LineaReceta.TIPO_SUBSECCION),
            cantidad__isnull=True,
            costo_linea_excel__isnull=True,
        ).count()
        sin_costo = LineaReceta.objects.filter(
            ~Q(tipo_linea=LineaReceta.TIPO_SUBSECCION),
            costo_linea_excel__isnull=True,
            costo_unitario_snapshot__isnull=True,
        ).count()

        self.stdout.write(self.style.SUCCESS("Auditoría de recetas importadas"))
        self.stdout.write(f"  - recetas: {total_recetas}")
        self.stdout.write(f"  - líneas: {total_lineas}")
        self.stdout.write(f"  - líneas sin match: {sin_match}")
        self.stdout.write(f"  - líneas sin unidad: {sin_unidad}")
        self.stdout.write(f"  - líneas sin cantidad: {sin_cantidad}")
        self.stdout.write(f"  - líneas sin costo base: {sin_costo}")

        self.stdout.write("  - Top recetas con más líneas sin match:")
        problematic = (
            Receta.objects.all()
            .order_by("nombre")
        )
        shown = 0
        for receta in problematic:
            count_bad = receta.lineas.filter(
                ~Q(tipo_linea=LineaReceta.TIPO_SUBSECCION),
                costo_linea_excel__isnull=True,
            ).filter(
                Q(insumo__isnull=True)
                | Q(match_status=LineaReceta.STATUS_NEEDS_REVIEW)
                | Q(match_status=LineaReceta.STATUS_REJECTED)
            ).count()
            if count_bad > 0:
                self.stdout.write(f"    * {receta.nombre}: {count_bad}")
                shown += 1
            if shown >= 15:
                break
        if shown == 0:
            self.stdout.write("    * Sin pendientes de match")
