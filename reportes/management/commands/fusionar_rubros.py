"""Fusiona rubros duplicados del presupuesto en uno canónico.

Uso:
  manage.py fusionar_rubros --area logistica --destino "Chevrolet Cheyenne" \
      --origenes "Cheyenne" "Camioneta Cheyenne" [--dry-run]

Si el destino no existe, el primer origen se renombra a ese nombre y los
demás se fusionan encima. Por cada mes: los presupuestos se SUMAN; el real
se conserva (si dos líneas tienen real distinto se reporta y NO se fusiona
esa línea — nunca se pierde dinero en silencio). Los orígenes quedan
inactivos con la referencia del destino en metadata.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from reportes.models import LineaPresupuestoMensual, ReglaFuenteRubro, RubroPresupuesto


class Command(BaseCommand):
    help = "Fusiona rubros duplicados en uno canónico (presupuesto sumado, real conservado)."

    def add_arguments(self, parser):
        parser.add_argument("--area", required=True)
        parser.add_argument("--destino", required=True)
        parser.add_argument("--origenes", nargs="+", required=True)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        area = options["area"]
        dry_run = options["dry_run"]

        def rubros_de(concepto):
            return list(
                RubroPresupuesto.objects.filter(
                    area__codigo=area, concepto=concepto, activo=True
                )
            )

        origenes: list[RubroPresupuesto] = []
        for concepto in options["origenes"]:
            encontrados = rubros_de(concepto)
            if not encontrados:
                self.stdout.write(self.style.WARNING(f"  origen no encontrado: '{concepto}'"))
            origenes.extend(encontrados)
        if not origenes:
            raise CommandError("Ningún rubro de origen encontrado; nada que fusionar.")

        destinos = rubros_de(options["destino"])
        with transaction.atomic():
            if destinos:
                destino = destinos[0]
            else:
                destino = origenes.pop(0)
                self.stdout.write(f"  renombrando '{destino.concepto}' → '{options['destino']}'")
                metadata = dict(destino.metadata or {})
                metadata.setdefault("nombre_excel", destino.concepto)
                destino.concepto = options["destino"]
                destino.metadata = metadata
                if not dry_run:
                    destino.save(update_fields=["concepto", "metadata", "actualizado_en"])

            fusionadas = 0
            conflictos = 0
            for origen in origenes:
                for linea in LineaPresupuestoMensual.objects.filter(rubro=origen):
                    par = LineaPresupuestoMensual.objects.filter(
                        rubro=destino, periodo=linea.periodo, version=linea.version
                    ).first()
                    if par is None:
                        if not dry_run:
                            linea.rubro = destino
                            linea.save(update_fields=["rubro", "actualizado_en"])
                        fusionadas += 1
                        continue
                    if (
                        linea.monto_real is not None
                        and par.monto_real is not None
                        and linea.monto_real != par.monto_real
                    ):
                        conflictos += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f"  CONFLICTO de real en {linea.periodo:%Y-%m} "
                                f"({origen.concepto}: {linea.monto_real} vs destino: {par.monto_real}) — línea no fusionada"
                            )
                        )
                        continue
                    if not dry_run:
                        par.monto_presupuesto = (par.monto_presupuesto or 0) + (linea.monto_presupuesto or 0)
                        if par.monto_real is None and linea.monto_real is not None:
                            par.monto_real = linea.monto_real
                            par.fuente_real = linea.fuente_real
                        par.save(update_fields=["monto_presupuesto", "monto_real", "fuente_real", "actualizado_en"])
                        linea.delete()
                    fusionadas += 1

                if not dry_run:
                    ReglaFuenteRubro.objects.filter(
                        rubro=origen, origen=ReglaFuenteRubro.ORIGEN_SEED
                    ).delete()
                    ReglaFuenteRubro.objects.filter(rubro=origen).update(rubro=destino)
                    metadata = dict(origen.metadata or {})
                    metadata["fusionado_en"] = options["destino"]
                    metadata["fusionado_fecha"] = timezone.now().isoformat()
                    origen.activo = False
                    origen.metadata = metadata
                    origen.save(update_fields=["activo", "metadata", "actualizado_en"])
                self.stdout.write(f"  fusionado: '{origen.concepto}' → '{options['destino']}'")

            if dry_run:
                transaction.set_rollback(True)

        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(f"[{modo}] líneas fusionadas: {fusionadas}, conflictos: {conflictos}")
