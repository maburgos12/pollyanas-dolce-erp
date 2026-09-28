"""Baja el presupuesto de los agregados de cadena a los rubros por sucursal.

Cuatro rubros de administración concentran presupuesto que pertenece a las
sucursales, y los rubros por sucursal del maestro quedaron en cero. Este
comando reparte el presupuesto al detalle y desactiva el agregado, para que
cada obligación del maestro tenga contra qué compararse.

Dos de ellos no se reparten porque son duplicados: su gasto ya está
presupuestado en otro rubro. Ahí el presupuesto se retira.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from reportes.models import (
    GastoRecurrente,
    LineaPresupuestoMensual,
    RubroPresupuesto,
)

CENT = Decimal("0.01")

# Agregados que se reparten al detalle, con la categoría del maestro que
# define a qué rubros va y con qué peso.
REPARTOS = [
    {
        "agregado": 766,
        "etiqueta": "Monitoreo de alarmas",
        "categoria": "ALARMAS_SUC",
    },
    {
        "agregado": 769,
        "etiqueta": "Point",
        "categoria": "SISTEMAS_SUC",
    },
]

# Agregados cuyo gasto ya está presupuestado en otro rubro: se retira el
# presupuesto y se desactiva el rubro, sin repartir.
DUPLICADOS = [
    {
        "agregado": 805,
        "etiqueta": "Renta y agua Leyva",
        "motivo": "Duplicado: la renta de Leyva ya está presupuestada en el rubro 1039",
    },
    {
        "agregado": 776,
        "etiqueta": "Fumigación y sanitización",
        "motivo": "Agregado de cadena: su presupuesto es la suma de los 9 rubros por sucursal más producción",
    },
]

# El 767 arrastra un cargo mensual que es el mismo Point del 769; sólo sus
# excedentes corresponden a licencias anuales propias.
SISTEMAS_MENSUAL_DUPLICADO = Decimal("10800.00")
AGREGADO_SISTEMAS = 767


def _cuantiza(valor: Decimal) -> Decimal:
    return valor.quantize(CENT)


def _pesos_por_rubro(categoria: str) -> dict[int, Decimal]:
    """Importe contractual vigente de cada rubro destino de esa categoría."""
    pesos: dict[int, Decimal] = {}
    recurrentes = GastoRecurrente.objects.filter(
        activo=True, categoria_gasto__codigo=categoria
    ).select_related("rubro")
    for recurrente in recurrentes:
        version = recurrente.versiones.order_by("-vigencia_inicio").first()
        if version is None or not recurrente.rubro_id:
            continue
        pesos[recurrente.rubro_id] = pesos.get(recurrente.rubro_id, Decimal("0")) + version.monto
    return pesos


def _repartir(monto: Decimal, pesos: dict[int, Decimal]) -> dict[int, Decimal]:
    """Reparte `monto` en proporción a los pesos, sin perder ni inventar centavos."""
    total_peso = sum(pesos.values())
    if not total_peso:
        return {}
    asignado: dict[int, Decimal] = {}
    for rubro_id, peso in pesos.items():
        asignado[rubro_id] = _cuantiza(monto * peso / total_peso)
    # El redondeo deja una diferencia de centavos; se carga al rubro mayor
    # para que la suma del detalle sea idéntica al agregado.
    diferencia = monto - sum(asignado.values())
    if diferencia:
        mayor = max(pesos, key=lambda r: pesos[r])
        asignado[mayor] = _cuantiza(asignado[mayor] + diferencia)
    return asignado


class Command(BaseCommand):
    help = "Reparte el presupuesto de los agregados de cadena a los rubros por sucursal."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        with transaction.atomic():
            total_repartido = self._repartos()
            total_sistemas = self._sistemas()
            total_retirado = self._duplicados()

            self.stdout.write("")
            self.stdout.write(f"  repartido al detalle : {total_repartido:>12,.2f}")
            self.stdout.write(f"  movido a corporativo : {total_sistemas:>12,.2f}")
            self.stdout.write(f"  retirado por duplicado: {total_retirado:>12,.2f}")

            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    # ---------------------------------------------------------------- #

    def _lineas(self, rubro_id: int):
        return LineaPresupuestoMensual.objects.filter(
            rubro_id=rubro_id, periodo__year=2026
        ).order_by("periodo")

    def _acumular(self, rubro_id: int, periodo, version: str, monto: Decimal) -> None:
        linea, creada = LineaPresupuestoMensual.objects.get_or_create(
            rubro_id=rubro_id,
            periodo=periodo,
            version=version,
            defaults={"monto_presupuesto": Decimal("0")},
        )
        linea.monto_presupuesto = _cuantiza((linea.monto_presupuesto or Decimal("0")) + monto)
        linea.save(update_fields=["monto_presupuesto", "actualizado_en"])
        return creada

    def _desactivar(self, rubro_id: int, motivo: str) -> None:
        rubro = RubroPresupuesto.objects.get(pk=rubro_id)
        metadata = dict(rubro.metadata or {})
        metadata["desactivado_motivo"] = motivo[:200]
        metadata["desactivado_fecha"] = timezone.now().isoformat()
        rubro.activo = False
        rubro.metadata = metadata
        rubro.save(update_fields=["activo", "metadata", "actualizado_en"])

    # ---------------------------------------------------------------- #

    def _repartos(self) -> Decimal:
        total = Decimal("0")
        for caso in REPARTOS:
            pesos = _pesos_por_rubro(caso["categoria"])
            if not pesos:
                self.stdout.write(self.style.WARNING(f"  {caso['etiqueta']}: sin contratos, se omite"))
                continue
            self.stdout.write("")
            self.stdout.write(f"REPARTO · {caso['etiqueta']} (rubro {caso['agregado']}) → {len(pesos)} rubros")
            movido = Decimal("0")
            for linea in self._lineas(caso["agregado"]):
                monto = linea.monto_presupuesto or Decimal("0")
                if not monto:
                    continue
                for rubro_id, parte in _repartir(monto, pesos).items():
                    self._acumular(rubro_id, linea.periodo, linea.version, parte)
                linea.monto_presupuesto = Decimal("0")
                linea.save(update_fields=["monto_presupuesto", "actualizado_en"])
                movido += monto
            self._desactivar(
                caso["agregado"],
                f"Agregado de cadena: presupuesto repartido a los rubros por sucursal ({caso['categoria']})",
            )
            self.stdout.write(f"  movido: {movido:,.2f}")
            total += movido
        return total

    def _sistemas(self) -> Decimal:
        """El 767 sólo conserva sus licencias anuales; su mensual es Point duplicado."""
        pesos = _pesos_por_rubro("SISTEMAS_CORP")
        if not pesos:
            self.stdout.write(self.style.WARNING("  SISTEMAS_CORP sin contratos, se omite el 767"))
            return Decimal("0")
        self.stdout.write("")
        self.stdout.write(f"SISTEMAS · rubro {AGREGADO_SISTEMAS} → {len(pesos)} rubros corporativos")
        movido = Decimal("0")
        descartado = Decimal("0")
        for linea in self._lineas(AGREGADO_SISTEMAS):
            monto = linea.monto_presupuesto or Decimal("0")
            excedente = monto - SISTEMAS_MENSUAL_DUPLICADO
            if excedente > 0:
                for rubro_id, parte in _repartir(excedente, pesos).items():
                    self._acumular(rubro_id, linea.periodo, linea.version, parte)
                movido += excedente
                self.stdout.write(f"  {linea.periodo:%Y-%m}: excedente {excedente:,.2f} → corporativo")
            descartado += min(monto, SISTEMAS_MENSUAL_DUPLICADO)
            linea.monto_presupuesto = Decimal("0")
            linea.save(update_fields=["monto_presupuesto", "actualizado_en"])
        self._desactivar(
            AGREGADO_SISTEMAS,
            "Su cargo mensual es el mismo Point del rubro 769; sus licencias anuales pasaron a los rubros corporativos",
        )
        self.stdout.write(f"  mensual duplicado retirado: {descartado:,.2f}")
        return movido

    def _duplicados(self) -> Decimal:
        total = Decimal("0")
        for caso in DUPLICADOS:
            self.stdout.write("")
            self.stdout.write(f"DUPLICADO · {caso['etiqueta']} (rubro {caso['agregado']})")
            retirado = Decimal("0")
            for linea in self._lineas(caso["agregado"]):
                retirado += linea.monto_presupuesto or Decimal("0")
                linea.monto_presupuesto = Decimal("0")
                linea.save(update_fields=["monto_presupuesto", "actualizado_en"])
            self._desactivar(caso["agregado"], caso["motivo"])
            self.stdout.write(f"  retirado: {retirado:,.2f}")
            total += retirado
        return total
