"""Mueve los rubros del P&L de empresa fuera del área administración.

La hoja "Administración" del Excel original era el estado de resultados de
toda la empresa: traía la venta total y los costos junto con los gastos
administrativos. Esos rubros duplican lo que ya viven en las áreas de ventas
y producción, así que se mueven a un área de control "Resultados (P&L)" que
se consulta pero no suma a los KPIs globales (mismo trato que Nómina).
"""

from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.models import AreaPresupuesto, RubroPresupuesto

CONCEPTOS_PNL = [
    "Venta postres",
    "Venta complementos",
    "Costos insumos/productos",
    "Costos complementos",
    "Merma",
]


class Command(BaseCommand):
    help = "Mueve los rubros del P&L de empresa de administración al área resultados"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    @transaction.atomic
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        rubros = RubroPresupuesto.objects.filter(
            area__codigo="administracion", concepto__in=CONCEPTOS_PNL
        )
        if not rubros.exists():
            self.stdout.write("Nada que mover (¿ya se ejecutó?).")
            return

        ultimo_orden = (
            AreaPresupuesto.objects.order_by("-orden").values_list("orden", flat=True).first() or 0
        )
        area, creada = AreaPresupuesto.objects.get_or_create(
            codigo="resultados",
            defaults={"nombre": "Resultados (P&L)", "orden": ultimo_orden + 1},
        )
        self.stdout.write(f"Área resultados: {'creada' if creada else 'ya existía'} (id {area.id})")

        for rubro in rubros:
            self.stdout.write(f"  mover rubro {rubro.id} '{rubro.concepto}' → resultados")
        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN: sin cambios."))
            transaction.set_rollback(True)
            return

        movidos = rubros.update(area=area)
        self.stdout.write(self.style.SUCCESS(f"{movidos} rubro(s) movidos a Resultados (P&L)."))
