"""Pone el maestro de fumigación en la tarifa que dice la factura.

El maestro se cargó con los $450 por sucursal que traía el Excel de
administración, más $950 de planta por separado. La factura de FUMIFER dice otra
cosa: es **una sola** por todas las instalaciones, planta incluida, y su
subtotal es $3,760 hasta febrero y $4,410 desde abril.

Los dos importes se dividen exacto entre las diez instalaciones —$376 y $441—,
y que dos totales distintos den residuo cero es lo que confirma que el proveedor
cobra tarifa plana por sitio. El Excel registraba el subtotal sin IVA: su captura
de enero es $3,760 al centavo.

De los $950 de planta no queda nada que cobrar aparte: ya venían dentro.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import (
    CategoriaGasto,
    CentroCosto,
    GastoRecurrente,
    GastoRecurrenteVersion,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
from reportes.services_gastos_compromisos import _asegurar_fuente_rubro

# Subtotal de la factura entre las diez instalaciones. Ambos dan residuo cero.
TARIFAS = [
    (date(2026, 1, 1), Decimal("376.00"), "Factura FUMIFER 3,760.00 ÷ 10 instalaciones"),
    (date(2026, 4, 1), Decimal("441.00"), "Factura FUMIFER 4,410.00 ÷ 10 instalaciones"),
]
PROVEEDOR = "Grecia Paulina Fernandez Cervantes (FUMIFER)"

# Las diez instalaciones que cubre la factura, por su centro de costo.
INSTALACIONES = [
    "MATRIZ", "PAYAN", "COLOSIO", "EL_TUNEL", "GUAMUCHIL",
    "LAS_GLORIAS", "LEYVA", "PLAZA_NIO", "PROD",
]
# La décima cambió de tienda a media año: Crucero cerró el 21-jun y Bamoa abrió
# el 14-jul, así que el sitio es el mismo pero el centro que lo paga no.
CRUCERO = ("CRUCERO", None, date(2026, 6, 30))
BAMOA = ("BAMOA", date(2026, 7, 1), None)


class Command(BaseCommand):
    help = "Corrige el maestro de fumigación con la tarifa de la factura de FUMIFER."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")
        parser.add_argument("--usuario", default="maburgos12")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        categoria_suc = CategoriaGasto.objects.filter(codigo="INDIRECTO_SUC").first()
        categoria_prod = CategoriaGasto.objects.filter(codigo="INDIRECTO_PROD").first()
        if categoria_suc is None or categoria_prod is None:
            raise CommandError("Faltan las categorías INDIRECTO_SUC o INDIRECTO_PROD.")

        with transaction.atomic():
            self._quitar_reglas_que_duplican()
            creados, corregidos = self._contratos(categoria_suc, categoria_prod)
            self._verificar()
            self.stdout.write("")
            self.stdout.write(f"  contratos dados de alta: {creados}")
            self.stdout.write(f"  contratos corregidos   : {corregidos}")
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    # ------------------------------------------------------------------ #

    def _quitar_reglas_que_duplican(self) -> None:
        """Con tarifa plana por sitio, el reparto 65/35 de fumigación sobra.

        Esas reglas venían del modelo del Excel, donde el local de Crucero
        compartía su fumigación con producción. La factura dice que cada
        instalación paga lo suyo, y producción ya tiene su propio renglón entre
        las diez: dejarlas la cobraría dos veces.
        """
        self.stdout.write("REGLAS QUE YA NO APLICAN")
        reglas = ReglaFuenteRubro.objects.filter(
            tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
            activa=True,
            rubro__concepto__icontains="umigaci",
        ).select_related("rubro", "rubro__sucursal", "rubro__area")
        for regla in reglas:
            destino = regla.rubro.sucursal or regla.rubro.area
            regla.activa = False
            regla.notas = (
                (regla.notas or "")
                + " · Desactivada: la factura de FUMIFER cobra tarifa plana por "
                "instalación, no un importe compartido."
            ).strip()
            regla.save(update_fields=["activa", "notas", "actualizado_en"])
            self.stdout.write(f"  regla {regla.id} · {destino} · {regla.modo_asignacion}")
        self.stdout.write(f"  {len(reglas)} reglas de gasto operativo desactivadas")

    def _contratos(self, categoria_suc, categoria_prod):
        self.stdout.write("")
        self.stdout.write("CONTRATOS · una versión por tarifa")
        creados = corregidos = 0
        casos = [(c, None, None) for c in INSTALACIONES] + [CRUCERO, BAMOA]
        for codigo, desde, hasta in casos:
            centro = CentroCosto.objects.filter(codigo=codigo).first()
            if centro is None:
                self.stdout.write(self.style.WARNING(f"  {codigo}: no existe el centro, se omite"))
                continue
            rubro = self._rubro(centro)
            if rubro is None:
                self.stdout.write(self.style.WARNING(f"  {codigo}: sin rubro de fumigación, se omite"))
                continue
            categoria = categoria_prod if codigo == "PROD" else categoria_suc
            contrato = GastoRecurrente.objects.filter(
                rubro=rubro, categoria_gasto=categoria, activo=True
            ).first()
            nuevo = contrato is None
            if nuevo:
                contrato = GastoRecurrente.objects.create(
                    area=rubro.area,
                    rubro=rubro,
                    centro_costo=centro,
                    categoria_gasto=categoria,
                    concepto=f"Fumigación y sanitización {centro.nombre or codigo}",
                    proveedor=PROVEEDOR,
                )
                creados += 1
            else:
                contrato.proveedor = PROVEEDOR
                contrato.centro_costo = centro
                contrato.save(update_fields=["proveedor", "centro_costo", "actualizado_en"])
                corregidos += 1

            # Sin obligaciones generadas las versiones se pueden reescribir; con
            # ellas habría que versionar hacia adelante y no borrar nada.
            if contrato.obligaciones.exists():
                raise CommandError(
                    f"El contrato {contrato.pk} ya tiene obligaciones: no se reescriben versiones."
                )
            contrato.versiones.all().delete()
            for vigencia, monto, nota in self._tarifas_de(desde, hasta):
                GastoRecurrenteVersion.objects.create(
                    gasto_recurrente=contrato,
                    vigencia_inicio=vigencia,
                    vigencia_fin=hasta,
                    monto=monto,
                    periodicidad_meses=1,
                    dia_vencimiento=1,
                    condicion_pago="CONTADO",
                    motivo=nota,
                )
            _asegurar_fuente_rubro(rubro=rubro, centro_costo=centro, categoria_gasto=categoria)
            marca = "alta" if nuevo else "corregido"
            rango = f" · hasta {hasta:%b-%Y}" if hasta else (f" · desde {desde:%b-%Y}" if desde else "")
            self.stdout.write(f"  {marca:<9} {codigo:<13} 376 → 441{rango}")
        return creados, corregidos

    @staticmethod
    def _tarifas_de(desde, hasta):
        """Recorta las tarifas a la vida del sitio.

        Un sitio que abrió en julio no vivió la tarifa vieja: si las dos se
        recortan a la misma fecha de arranque, sólo vale la última.
        """
        recortadas = {}
        for inicio, monto, nota in TARIFAS:
            vigencia = max(inicio, desde) if desde else inicio
            if hasta and vigencia > hasta:
                continue
            recortadas[vigencia] = (monto, nota)
        return [(v, m, n) for v, (m, n) in sorted(recortadas.items())]

    @staticmethod
    def _rubro(centro):
        base = RubroPresupuesto.objects.filter(concepto__icontains="umigaci", activo=True)
        if centro.sucursal_id:
            return base.filter(sucursal_id=centro.sucursal_id).first()
        return base.filter(sucursal__isnull=True, area__codigo="produccion").first()

    def _verificar(self) -> None:
        """La suma de los contratos vigentes tiene que dar el subtotal de la factura."""
        self.stdout.write("")
        self.stdout.write("COMPROBACIÓN CONTRA LA FACTURA")
        for periodo, esperado in ((date(2026, 2, 1), Decimal("3760.00")),
                                  (date(2026, 9, 1), Decimal("4410.00"))):
            total = Decimal("0")
            sitios = 0
            for contrato in GastoRecurrente.objects.filter(
                activo=True, rubro__concepto__icontains="umigaci"
            ):
                version = (
                    contrato.versiones.filter(vigencia_inicio__lte=periodo)
                    .exclude(vigencia_fin__lt=periodo)
                    .order_by("-vigencia_inicio")
                    .first()
                )
                if version is not None:
                    total += version.monto
                    sitios += 1
            estado = "cuadra" if total == esperado else f"NO CUADRA (esperado {esperado:,.2f})"
            self.stdout.write(f"  {periodo:%b-%Y}: {sitios} instalaciones × tarifa = {total:,.2f} · {estado}")
