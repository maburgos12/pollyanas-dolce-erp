"""Genera las obligaciones del maestro de gastos fijos por rango de meses.

El maestro tiene los contratos bien cargados, pero un contrato por sí solo no
llena nada: la regla `OBLIGACION_GASTO` lee obligaciones, y sin ellas devuelve
cero. Hasta ahora la única forma de crearlas era un botón por contrato y por
mes, así que el maestro completo estaba configurado y a la vez invisible en el
P&L.

Respeta las reglas del servicio: no genera dos veces el mismo mes, no parte un
ciclo bimestral o anual a la mitad, y se detiene en el mes que no tiene versión
vigente en vez de inventar una. Lo que no puede generar se reporta con su
motivo.

Y antes de generar revisa que el rubro no esté ya leyendo una captura de ese
mes. La obligación crea su propia fila en `GastoOperativoMensual`; si el rubro
conserva una regla `GASTO_OPERATIVO` que también lee la captura vieja del Excel,
las dos filas se suman y el rubro reporta el doble. Pasó una vez con 92
obligaciones y $477,918.63 de más, así que el contrato se omite y se reporta en
vez de duplicar.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import GastoOperativoMensual, GastoRecurrente, ReglaFuenteRubro
from reportes.services_gastos_compromisos import generar_obligacion_recurrente


def _mes(texto: str) -> date:
    try:
        anio, mes = str(texto).split("-")
        return date(int(anio), int(mes), 1)
    except (ValueError, AttributeError) as exc:
        raise CommandError(f"Mes inválido '{texto}' (formato YYYY-MM)") from exc


def _meses(desde: date, hasta: date):
    actual = desde
    while actual <= hasta:
        yield actual
        actual = date(actual.year + (actual.month == 12), actual.month % 12 + 1, 1)


class Command(BaseCommand):
    help = "Genera las obligaciones del maestro de gastos fijos en un rango de meses."

    def add_arguments(self, parser):
        parser.add_argument("--desde", required=True, help="Mes inicial YYYY-MM.")
        parser.add_argument("--hasta", required=True, help="Mes final YYYY-MM.")
        parser.add_argument("--categoria", nargs="*", help="Códigos de categoría a generar.")
        parser.add_argument("--usuario", default="maburgos12")
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        desde, hasta = _mes(options["desde"]), _mes(options["hasta"])
        if desde > hasta:
            raise CommandError("El mes inicial va después del final.")
        usuario = get_user_model().objects.filter(username=options["usuario"]).first()
        if usuario is None:
            raise CommandError(f"No existe el usuario '{options['usuario']}'.")

        contratos = GastoRecurrente.objects.filter(activo=True).select_related(
            "categoria_gasto", "centro_costo", "rubro", "area"
        )
        if options.get("categoria"):
            contratos = contratos.filter(categoria_gasto__codigo__in=options["categoria"])
        contratos = list(contratos.order_by("categoria_gasto__codigo", "pk"))
        if not contratos:
            raise CommandError("Ningún contrato activo coincide con el filtro.")

        creadas = defaultdict(lambda: [0, Decimal("0")])
        existentes = 0
        omitidas = defaultdict(int)
        with transaction.atomic():
            for contrato in contratos:
                for periodo in _meses(desde, hasta):
                    if self._duplicaria(contrato, periodo):
                        omitidas["el rubro ya lee una captura de ese mes"] += 1
                        continue
                    try:
                        obligacion, nueva = generar_obligacion_recurrente(
                            usuario=usuario, recurrente=contrato, periodo=periodo
                        )
                    except ValidationError as exc:
                        omitidas[self._motivo(exc)] += 1
                        continue
                    if nueva:
                        clave = contrato.categoria_gasto.codigo
                        creadas[clave][0] += 1
                        creadas[clave][1] += obligacion.monto_reconocido
                    else:
                        existentes += 1
            self._reportar(creadas, existentes, omitidas, len(contratos), desde, hasta)
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    @staticmethod
    def _duplicaria(contrato: GastoRecurrente, periodo: date) -> bool:
        """¿El rubro ya lee una captura de ese mes por su propia regla de gasto?

        La obligación se materializa como una captura más. Mientras el rubro
        tenga una regla canónica `GASTO_OPERATIVO` de la misma categoría y
        centro, esa regla sumará la captura nueva junto con la que ya existía.
        """
        lee_capturas = ReglaFuenteRubro.objects.filter(
            rubro_id=contrato.rubro_id,
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            modo_asignacion=ReglaFuenteRubro.MODO_CANONICA,
            categoria_gasto_id=contrato.categoria_gasto_id,
            activa=True,
        ).exists()
        if not lee_capturas:
            return False
        return (
            GastoOperativoMensual.objects.filter(
                centro_costo_id=contrato.centro_costo_id,
                categoria_gasto_id=contrato.categoria_gasto_id,
                periodo=periodo,
            )
            .exclude(obligacion_gasto__gasto_recurrente_id=contrato.pk)
            .exists()
        )

    @staticmethod
    def _motivo(exc: ValidationError) -> str:
        """Agrupa por causa: un listado de 400 renglones no se lee."""
        texto = " ".join(exc.messages) if hasattr(exc, "messages") else str(exc)
        if "versión vigente" in texto:
            return "el contrato no estaba vigente ese mes"
        if "ciclo de" in texto:
            return "mes cubierto por un ciclo anterior (bimestral o anual)"
        if "ya cubre ese mes" in texto:
            return "ya había una obligación cubriendo ese mes"
        return texto[:70]

    def _reportar(self, creadas, existentes, omitidas, contratos, desde, hasta):
        self.stdout.write(
            f"MAESTRO · {contratos} contratos × {desde:%b-%Y} a {hasta:%b-%Y}"
        )
        self.stdout.write("")
        self.stdout.write("OBLIGACIONES NUEVAS")
        total_n = 0
        total_m = Decimal("0")
        for categoria in sorted(creadas):
            n, monto = creadas[categoria]
            self.stdout.write(f"  {categoria:<26} {n:>5} obligaciones {monto:>14,.2f}")
            total_n += n
            total_m += monto
        self.stdout.write(f"  {'total':<26} {total_n:>5} obligaciones {total_m:>14,.2f}")
        if existentes:
            self.stdout.write(f"  ya existían, no se repiten: {existentes}")
        if omitidas:
            self.stdout.write("")
            self.stdout.write("NO SE GENERARON")
            for motivo in sorted(omitidas, key=lambda m: -omitidas[m]):
                self.stdout.write(f"  {omitidas[motivo]:>5} · {motivo}")
