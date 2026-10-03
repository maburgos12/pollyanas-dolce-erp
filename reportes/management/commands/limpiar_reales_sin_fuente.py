"""Borra los importes que quedaron congelados al desactivar una regla.

La consolidación protege el valor cuando la fuente viene vacía: así un dato bueno
no se pierde porque un día la fuente no respondió. El efecto secundario es que al
desactivar una regla su último importe se queda en la pantalla para siempre, y
nadie nota que ya no se actualiza.

Esto borra sólo lo que venía de una fuente que el rubro ya no tiene. El legado
del Excel (`AUTO:LEGADO`), que nunca tuvo regla, y las capturas manuales no se
tocan: el barrido es por fuente, nunca general.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from collections import defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Sum

from reportes.models import LineaPresupuestoMensual, ReglaFuenteRubro

# El legado del Excel no viene de ninguna regla: borrarlo sería perder historia.
NUNCA = {"AUTO:LEGADO"}
# Un rubro desactivado ya no se muestra en ningún reporte, así que su importe
# congelado no engaña a nadie: es el rastro de lo que decía el Excel.


class Command(BaseCommand):
    help = "Borra los importes AUTO de rubros que ya no tienen esa fuente activa."

    def add_arguments(self, parser):
        parser.add_argument("--fuente", nargs="*", help="Tipos de fuente a revisar (por omisión todos).")
        parser.add_argument("--anio", type=int, default=2026)
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        anio = options["anio"]
        fuentes = options.get("fuente") or [
            tipo for tipo, _etiqueta in ReglaFuenteRubro.FUENTE_CHOICES
        ]

        self.stdout.write(f"IMPORTES SIN FUENTE VIVA · {anio}")
        total_lineas = 0
        for tipo in sorted(fuentes):
            etiqueta = f"AUTO:{tipo}"
            if etiqueta in NUNCA:
                continue
            con_regla = ReglaFuenteRubro.objects.filter(
                tipo_fuente=tipo, activa=True
            ).values_list("rubro_id", flat=True)
            huerfanas = (
                LineaPresupuestoMensual.objects.filter(
                    periodo__year=anio, fuente_real=etiqueta, rubro__activo=True
                )
                .exclude(rubro_id__in=con_regla)
            )
            cuantas = huerfanas.count()
            if not cuantas:
                continue
            monto = huerfanas.aggregate(t=Sum("monto_real"))["t"] or 0
            detalle = self._detalle(huerfanas)
            self.stdout.write("")
            self.stdout.write(f"  {etiqueta} · {cuantas} líneas · {monto:,.2f}")
            for donde, (n, m) in sorted(detalle.items()):
                self.stdout.write(f"    {donde:<34} {n:>3} líneas {m:>12,.2f}")
            if aplicar:
                huerfanas.update(monto_real=None, fuente_real="")
            total_lineas += cuantas

        self.stdout.write("")
        if not total_lineas:
            self.stdout.write("  nada colgando: todas las fuentes tienen su regla viva")
        else:
            self.stdout.write(f"  {total_lineas} líneas vuelven a pendiente")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    @staticmethod
    def _detalle(lineas):
        agrupado = defaultdict(lambda: [0, 0])
        for fila in lineas.select_related("rubro", "rubro__sucursal", "rubro__area"):
            destino = fila.rubro.sucursal or fila.rubro.area
            clave = f"{destino} · {fila.rubro.concepto[:22]}"
            agrupado[clave][0] += 1
            agrupado[clave][1] += fila.monto_real or 0
        return agrupado
