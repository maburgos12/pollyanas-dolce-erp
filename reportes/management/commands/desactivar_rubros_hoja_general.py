"""Desactiva los rubros fantasma creados por la hoja GENERAL de Gastos de Ventas.

El import del paquete REAL leyó la hoja GENERAL (consolidado de las 9
sucursales) como si fuera una sucursal más y creó rubros sin sucursal cuyo
real duplica el de las hojas por sucursal (~$2.5M en 2026). Se desactivan
(quedan auditables); el importador ya no vuelve a leer hojas de resumen.
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from reportes.models import LineaPresupuestoMensual, RubroPresupuesto


class Command(BaseCommand):
    help = "Desactiva rubros sin sucursal de gastos-venta creados por la hoja GENERAL"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    @transaction.atomic
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        rubros = RubroPresupuesto.objects.filter(
            area__codigo="gastos-venta", sucursal__isnull=True, activo=True
        )
        desactivados = 0
        for rubro in rubros:
            if (rubro.metadata or {}).get("source") != "PAQUETE_2026_REAL":
                self.stdout.write(f"  omitido (otro origen): {rubro.concepto}")
                continue
            capturas = LineaPresupuestoMensual.objects.filter(
                rubro=rubro, fuente_real__startswith="MANUAL:"
            ).count()
            if capturas:
                self.stdout.write(
                    self.style.WARNING(f"  omitido ({capturas} captura(s) manual(es)): {rubro.concepto}")
                )
                continue
            desactivados += 1
            self.stdout.write(f"  desactivar: {rubro.id} '{rubro.concepto}'")
            if not dry_run:
                metadata = dict(rubro.metadata or {})
                metadata["desactivado_motivo"] = (
                    "Duplicado: hoja GENERAL (consolidado) importada como sucursal"
                )
                metadata["desactivado_fecha"] = timezone.now().isoformat()
                rubro.activo = False
                rubro.metadata = metadata
                rubro.save(update_fields=["activo", "metadata", "actualizado_en"])
        if dry_run:
            transaction.set_rollback(True)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(self.style.SUCCESS(f"[{modo}] desactivados: {desactivados}"))
