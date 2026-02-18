from __future__ import annotations

from django.core.management.base import BaseCommand

from recetas.models import Receta
from recetas.utils.derived_insumos import sync_receta_presentaciones


class Command(BaseCommand):
    help = "Sincroniza insumos derivados desde presentaciones de recetas (crear/actualizar costo por unidad)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--receta",
            dest="receta",
            default="",
            help="Filtra por nombre de receta (contains).",
        )

    def handle(self, *args, **options):
        receta_filter = (options.get("receta") or "").strip()
        recetas = Receta.objects.filter(usa_presentaciones=True).order_by("nombre")
        if receta_filter:
            recetas = recetas.filter(nombre__icontains=receta_filter)

        recetas_procesadas = 0
        presentaciones = 0
        insumos_creados = 0
        insumos_actualizados = 0
        insumos_desactivados = 0
        costos_creados = 0

        for receta in recetas:
            s = sync_receta_presentaciones(receta)
            recetas_procesadas += 1
            presentaciones += s.presentaciones
            insumos_creados += s.insumos_creados
            insumos_actualizados += s.insumos_actualizados
            insumos_desactivados += s.insumos_desactivados
            costos_creados += s.costos_creados

        self.stdout.write(self.style.SUCCESS("Sincronización completada"))
        self.stdout.write(f"  - recetas procesadas: {recetas_procesadas}")
        self.stdout.write(f"  - presentaciones procesadas: {presentaciones}")
        self.stdout.write(f"  - insumos creados: {insumos_creados}")
        self.stdout.write(f"  - insumos actualizados: {insumos_actualizados}")
        self.stdout.write(f"  - insumos desactivados: {insumos_desactivados}")
        self.stdout.write(f"  - costos creados: {costos_creados}")
