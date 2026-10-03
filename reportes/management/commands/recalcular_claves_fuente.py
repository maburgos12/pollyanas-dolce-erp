"""Repara las claves canónicas que quedaron apuntando a otro rubro.

`ReglaFuenteRubro` guarda en `clave_fuente` la identidad de su fuente, y un
índice único impide que dos reglas canónicas activas compartan identidad. La
clave se calcula en `save()`, así que mover una regla con `queryset.update()`
—que no pasa por `save()`— la deja con la clave del rubro anterior.

El síntoma no aparece al mover: aparece después, cuando otra regla legítima
reclama esa identidad y la base la rechaza por duplicada. Pasó al separar Bamoa
de Crucero: cuatro reglas de obligación viajaron al espejo de Bamoa con la clave
de Crucero, y la primera obligación de renta de Crucero chocó contra ellas.

Recalcular es seguro y repetible: la clave se deriva del estado de la regla, así
que una ya correcta no cambia.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from reportes.models import ReglaFuenteRubro


class Command(BaseCommand):
    help = "Recalcula las claves canónicas de fuente que quedaron desactualizadas."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        desfasadas = []
        for regla in ReglaFuenteRubro.objects.all().select_related(
            "rubro", "rubro__sucursal", "rubro__area"
        ):
            guardada = regla.clave_fuente or ""
            correcta = regla.calcular_clave_fuente()
            if guardada != correcta:
                desfasadas.append((regla, guardada, correcta))

        self.stdout.write("CLAVES QUE NO CORRESPONDEN A SU REGLA")
        if not desfasadas:
            self.stdout.write("  ninguna: todas las claves corresponden a su rubro")
        with transaction.atomic():
            for regla, guardada, correcta in desfasadas:
                destino = regla.rubro.sucursal or regla.rubro.area
                self.stdout.write(
                    f"  regla {regla.id:<6} {str(destino)[:24]:<26} "
                    f"{regla.rubro.concepto[:24]:<26} {guardada[:12]} → {correcta[:12] or '(vacía)'}"
                )
                # save() recalcula la clave; es lo que faltó cuando se movieron.
                regla.save(update_fields=["clave_fuente", "actualizado_en"])
            self.stdout.write("")
            self.stdout.write(f"  {len(desfasadas)} reglas recalculadas")
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))
