from django.core.management.base import BaseCommand
from django.db import transaction

from inventario.models import ExistenciaInsumo
from maestros.models import Insumo


class Command(BaseCommand):
    help = "Crea registros faltantes de ExistenciaInsumo para insumos activos."

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios. Sin esta bandera corre en dry-run.",
        )

    def handle(self, *args, **options):
        missing_qs = Insumo.objects.filter(activo=True, existenciainsumo__isnull=True).order_by("nombre")
        total_missing = missing_qs.count()

        self.stdout.write("Backfill de existencias por insumo")
        self.stdout.write(f"  - insumos activos sin existencia: {total_missing}")
        sample = list(missing_qs.values_list("nombre", flat=True)[:15])
        if sample:
            self.stdout.write("  - muestra:")
            for name in sample:
                self.stdout.write(f"    * {name}")

        if not options["apply"]:
            self.stdout.write("Dry-run: no se creó ninguna existencia. Usa --apply para confirmar.")
            return

        created = 0
        with transaction.atomic():
            for insumo in missing_qs.iterator():
                _, was_created = ExistenciaInsumo.objects.get_or_create(
                    insumo=insumo,
                    defaults={
                        "stock_actual": 0,
                        "punto_reorden": 0,
                        "stock_minimo": 0,
                        "stock_maximo": 0,
                        "inventario_promedio": 0,
                        "dias_llegada_pedido": 0,
                        "consumo_diario_promedio": 0,
                    },
                )
                if was_created:
                    created += 1

        self.stdout.write(self.style.SUCCESS(f"Existencias creadas: {created}"))
