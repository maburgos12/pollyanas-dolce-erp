from django.core.management.base import BaseCommand
from django.db.models import Q

from recetas.models import LineaReceta, Receta


class Command(BaseCommand):
    help = (
        "Limpia líneas placeholder heredadas de importación: "
        "insumo ligado + cantidad vacía + costo fijo 0/null."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica la limpieza. Sin esta bandera solo muestra conteo (dry-run).",
        )
        parser.add_argument(
            "--all-recipes",
            action="store_true",
            help="Incluye preparaciones base. Por defecto solo producto final.",
        )

    def handle(self, *args, **options):
        qs = LineaReceta.objects.filter(
            insumo__isnull=False,
            cantidad__isnull=True,
        ).filter(Q(costo_linea_excel__isnull=True) | Q(costo_linea_excel=0))

        if not options["all_recipes"]:
            qs = qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)

        total = qs.count()
        self.stdout.write("Limpieza de placeholders")
        self.stdout.write(f"  - líneas detectadas: {total}")

        sample = list(qs.select_related("receta", "insumo").order_by("receta__nombre", "posicion")[:15])
        if sample:
            self.stdout.write("  - muestra:")
            for line in sample:
                self.stdout.write(
                    f"    * receta={line.receta.nombre} | pos={line.posicion} | insumo={line.insumo.nombre}"
                )

        if not options["apply"]:
            self.stdout.write(self.style.WARNING("Dry-run: no se eliminaron registros. Usa --apply para confirmar."))
            return

        deleted, _ = qs.delete()
        self.stdout.write(self.style.SUCCESS(f"Líneas eliminadas: {deleted}"))
