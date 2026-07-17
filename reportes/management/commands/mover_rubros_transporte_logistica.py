"""Mueve los rubros de flotilla/transporte de administración a logística.

Dirección (2026-07-17): el combustible y los gastos de las unidades son de
logística. El Excel los traía en la hoja de administración; se mueven con
todo su historial (el legado ene–jun viaja con el rubro).
"""

from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.models import AreaPresupuesto, RubroPresupuesto

CONCEPTOS = [
    "Diesel",
    "Gasolina",
    "Mantenimiento equipo de transporte",
    "Puentes y peajes",
]


class Command(BaseCommand):
    help = "Mueve Diesel/Gasolina/Mant. transporte/Peajes de administración a logística"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    @transaction.atomic
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        logistica = AreaPresupuesto.objects.filter(codigo="logistica").first()
        if logistica is None:
            self.stdout.write(self.style.ERROR("No existe el área logistica."))
            return
        movidos = 0
        for concepto in CONCEPTOS:
            for rubro in RubroPresupuesto.objects.filter(
                area__codigo="administracion", concepto__iexact=concepto, activo=True
            ):
                self.stdout.write(f"  administracion → logistica: {rubro.concepto}")
                movidos += 1
                if not dry_run:
                    metadata = dict(rubro.metadata or {})
                    metadata["area_anterior"] = "administracion"
                    metadata["reclasificado_motivo"] = "Gasto de flotilla: pertenece a logística (dirección 2026-07-17)"
                    rubro.area = logistica
                    rubro.metadata = metadata
                    rubro.save(update_fields=["area", "metadata", "actualizado_en"])
        if dry_run:
            transaction.set_rollback(True)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(self.style.SUCCESS(f"[{modo}] rubros movidos: {movidos}"))
