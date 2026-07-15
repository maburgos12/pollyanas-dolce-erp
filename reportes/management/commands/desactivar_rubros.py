"""Desactiva rubros que no corresponden a ningún producto/gasto real.

Uso: manage.py desactivar_rubros --area ventas --conceptos "X" "Y" --motivo "..."
El rubro y su presupuesto se conservan en la base (auditables) pero salen de
las pantallas. Nunca toca rubros con capturas manuales sin --forzar.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from reportes.models import LineaPresupuestoMensual, RubroPresupuesto


class Command(BaseCommand):
    help = "Desactiva rubros sin contraparte real (salen de pantallas, quedan auditables)."

    def add_arguments(self, parser):
        parser.add_argument("--area", required=True)
        parser.add_argument("--conceptos", nargs="+", required=True)
        parser.add_argument("--motivo", default="")
        parser.add_argument("--forzar", action="store_true")
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        desactivados = 0
        with transaction.atomic():
            for concepto in options["conceptos"]:
                for rubro in RubroPresupuesto.objects.filter(
                    area__codigo=options["area"], concepto=concepto, activo=True
                ):
                    capturas = LineaPresupuestoMensual.objects.filter(
                        rubro=rubro, fuente_real__startswith="MANUAL:"
                    ).count()
                    if capturas and not options["forzar"]:
                        self.stdout.write(
                            self.style.WARNING(
                                f"  '{rubro.concepto}' tiene {capturas} captura(s) manual(es); usa --forzar"
                            )
                        )
                        continue
                    desactivados += 1
                    self.stdout.write(f"  desactivado: {rubro.concepto}")
                    if not dry_run:
                        metadata = dict(rubro.metadata or {})
                        metadata["desactivado_motivo"] = options["motivo"][:200]
                        metadata["desactivado_fecha"] = timezone.now().isoformat()
                        rubro.activo = False
                        rubro.metadata = metadata
                        rubro.save(update_fields=["activo", "metadata", "actualizado_en"])
            if dry_run:
                transaction.set_rollback(True)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(f"[{modo}] desactivados: {desactivados}")
