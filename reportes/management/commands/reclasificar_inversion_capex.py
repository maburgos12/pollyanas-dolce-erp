"""Mueve al área CAPEX los rubros de inversión que vivían como gasto operativo.

Instrucción de dirección (2026-07-17): separar bien gasto de inversión —
el equipamiento y las aperturas de sucursal (Guamúchil, El Túnel) estaban
inflando los gastos operativos (~$1.2M real 2026). Se mueven los rubros de
inversión clara; las refacciones menores (Refrigerador 1/2, Vitrina de
$800, herramienta menor) se quedan como gasto.

El estado de resultados ya resta CAPEX después de la utilidad operativa,
así que el resultado final no cambia — solo se ubica cada peso donde va.
"""

from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.models import AreaPresupuesto, RubroPresupuesto

# (area_origen, concepto exacto) — case-insensitive, todas las sucursales.
RUBROS_INVERSION = [
    ("produccion", "Adquisición de equipo/maquinaria"),
    ("produccion", "Batidora"),
    ("produccion", "Mesa de trabajo"),
    ("produccion", "Bascula 1"),
    ("produccion", "Horno"),
    ("produccion", "Horno baxter"),
    ("produccion", "Horno turbolino"),
    ("produccion", "Horno turbolino 1"),
    ("produccion", "Horno turbolino 2"),
    ("produccion", "Horno zucchelli"),
    ("produccion", "Hornos"),
    ("gastos-venta", "Adquisición de equipo/maquinaria"),
    ("gastos-venta", "Apertura sucursal"),
    ("gastos-venta", "Compras para sucursal"),
    ("gastos-venta", "Refrigerador"),   # exacto: NO toca "Refrigerador 1/2" (refacciones)
    ("gastos-venta", "Vitrinas"),       # exacto: NO toca "Vitrina" ($800, refacción)
    ("administracion", "Adquisición de equipo/maquinaria"),
    ("administracion", "Apertura sucursal"),
]


class Command(BaseCommand):
    help = "Reclasifica rubros de inversión de áreas de gasto al área CAPEX"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    @transaction.atomic
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        capex = AreaPresupuesto.objects.filter(codigo="capex").first()
        if capex is None:
            self.stdout.write(self.style.ERROR("No existe el área capex; nada que hacer."))
            return
        movidos = 0
        for area_origen, concepto in RUBROS_INVERSION:
            rubros = RubroPresupuesto.objects.filter(
                area__codigo=area_origen, concepto__iexact=concepto, activo=True
            ).select_related("sucursal")
            for rubro in rubros:
                suc = rubro.sucursal.codigo if rubro.sucursal_id else "—"
                self.stdout.write(f"  {area_origen:15} → capex  {rubro.concepto[:40]:40} [{suc}]")
                movidos += 1
                if not dry_run:
                    metadata = dict(rubro.metadata or {})
                    metadata["area_anterior"] = area_origen
                    metadata["reclasificado_motivo"] = "Inversión, no gasto operativo (dirección 2026-07-17)"
                    rubro.area = capex
                    rubro.metadata = metadata
                    rubro.save(update_fields=["area", "metadata", "actualizado_en"])
        if dry_run:
            transaction.set_rollback(True)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(self.style.SUCCESS(f"[{modo}] rubros movidos a CAPEX: {movidos}"))
