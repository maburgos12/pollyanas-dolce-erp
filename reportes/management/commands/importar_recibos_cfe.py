"""Lleva la luz de CFE al gasto por sucursal, cruzando factura, pago y recibo.

El CFDI de CFE no dice a qué servicio pertenece: son 5 KB con un solo concepto,
«Energia», sin número de servicio, sin medidor y sin domicilio. El complemento
de pago tampoco lo trae, y los pagos se hacen con tarjeta en terminal de CFE,
así que la descripción bancaria no lleva referencia.

Lo que sí se puede afirmar es el importe, y las tres fuentes coinciden en él: la
factura del SAT, el movimiento del banco y el recibo que llega al domicilio. En
2026 los importes de CFE no se repiten ni una vez, así que el empate a tres
bandas identifica el medidor sin adivinar.

Cada recibo de `reportes/data/recibos_cfe.csv` es uno que alguien tuvo en la
mano. Lo que no empate contra un recibo no se asigna: se reporta para que se
capture el recibo que falta.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual
from sat_client.models import CfdiDescargado, CfdiPagoRelacionado

CATEGORIA_LUZ = "LUZ_SUC"
EMISOR_CFE = "comision federal"
CENTAVO = Decimal("0.01")


def _ruta_por_defecto() -> Path:
    return Path(settings.BASE_DIR) / "reportes" / "data" / "recibos_cfe.csv"


def _mes(texto: str) -> date | None:
    texto = (texto or "").strip()
    if not texto:
        return None
    anio, mes = texto.split("-")
    return date(int(anio), int(mes), 1)


def _meses_entre(inicio: date, fin: date) -> list[date]:
    meses = []
    actual = inicio
    while actual <= fin:
        meses.append(actual)
        actual = date(actual.year + (actual.month == 12), actual.month % 12 + 1, 1)
    return meses


class Command(BaseCommand):
    help = "Asigna los CFDI de CFE a su medidor y los registra como gasto de luz."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")
        parser.add_argument("--archivo", help="CSV de recibos (por omisión el del repo).")
        parser.add_argument("--desde", default="2026-01", help="Mes inicial YYYY-MM.")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        desde = _mes(options["desde"])
        recibos = self._leer(Path(options["archivo"]) if options.get("archivo") else _ruta_por_defecto())
        categoria = CategoriaGasto.objects.filter(codigo=CATEGORIA_LUZ).first()
        if categoria is None:
            raise CommandError(f"No existe la categoría de gasto {CATEGORIA_LUZ}.")

        facturas = list(
            CfdiDescargado.objects.filter(
                nombre_emisor__icontains=EMISOR_CFE,
                tipo_comprobante="I",
                fecha_emision__gte=desde,
            ).order_by("fecha_emision")
        )

        with transaction.atomic():
            asignadas, sin_recibo, ya_capturadas = self._asignar(facturas, recibos, categoria)
            self._reportar(asignadas, sin_recibo, ya_capturadas)
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    # ------------------------------------------------------------------ #

    def _leer(self, ruta: Path) -> dict[Decimal, dict]:
        if not ruta.exists():
            raise CommandError(f"No encuentro el archivo de recibos: {ruta}")
        recibos: dict[Decimal, dict] = {}
        with ruta.open(encoding="utf-8") as fh:
            for fila in csv.DictReader(l for l in fh if not l.startswith("#")):
                importe = Decimal(fila["importe"]).quantize(CENTAVO)
                if importe in recibos:
                    raise CommandError(
                        f"El importe {importe} aparece en dos recibos; el empate por importe "
                        "deja de ser único y habría que distinguirlos de otra forma."
                    )
                recibos[importe] = {
                    "no_servicio": fila["no_servicio"].strip(),
                    "centro": fila["centro_costo"].strip(),
                    "inicio": _mes(fila.get("cobertura_inicio", "")),
                    "fin": _mes(fila.get("cobertura_fin", "")),
                    "evidencia": (fila.get("evidencia") or "").strip(),
                }
        return recibos

    def _asignar(self, facturas, recibos, categoria):
        """Una captura por mes cubierto; sin cobertura conocida, el mes de emisión."""
        centros = {c.codigo: c for c in CentroCosto.objects.all()}
        asignadas, sin_recibo, ya_capturadas = [], [], []
        for cfdi in facturas:
            total = Decimal(cfdi.total).quantize(CENTAVO)
            if total <= 0:
                continue  # los de cero son notas o complementos, no gasto
            recibo = recibos.get(total)
            if recibo is None:
                sin_recibo.append(cfdi)
                continue
            centro = centros.get(recibo["centro"])
            if centro is None:
                raise CommandError(f"No existe el centro de costo {recibo['centro']}.")

            emision = cfdi.fecha_emision.date().replace(day=1)
            if recibo["inicio"] and recibo["fin"]:
                meses = _meses_entre(recibo["inicio"], recibo["fin"])
            else:
                meses = [emision]
            # Un recibo bimestral repartido entre sus meses es una estimación, y
            # se marca como tal; uno mensual es el dato tal cual.
            prorrateado = len(meses) > 1
            parte = (total / len(meses)).quantize(CENTAVO)
            restante = total - parte * len(meses)
            for i, periodo in enumerate(meses):
                monto = parte + (restante if i == 0 else Decimal("0"))
                clave = f"CFE:{cfdi.uuid}:{periodo:%Y%m}"
                # Alguien pudo capturar este mismo recibo a mano antes. Duplicarlo
                # inflaría el gasto del centro, así que se respeta lo capturado y
                # se reporta para que una persona decida cuál conservar.
                previa = (
                    GastoOperativoMensual.objects.filter(
                        periodo=periodo,
                        centro_costo=centro,
                        categoria_gasto=categoria,
                        monto=monto,
                    )
                    .exclude(external_key=clave)
                    .first()
                )
                if previa is not None:
                    ya_capturadas.append((cfdi, centro.codigo, periodo, previa.external_key))
                    continue
                GastoOperativoMensual.objects.update_or_create(
                    external_key=clave,
                    defaults={
                        "periodo": periodo,
                        "monto": monto,
                        "categoria_gasto": categoria,
                        "centro_costo": centro,
                        "fuente": "CFE_CFDI",
                        "es_estimado": prorrateado,
                        "tipo_dato": GastoOperativoMensual.TIPO_DATO_REAL,
                        "cobertura_mes_inicio": meses[0],
                        "cobertura_mes_fin": meses[-1],
                        "comentario": (
                            f"Servicio {recibo['no_servicio']} · CFDI {cfdi.uuid} · "
                            f"factura {cfdi.fecha_emision:%d-%b-%Y} por {total:,.2f}. "
                            + recibo["evidencia"]
                        ),
                    },
                )
            asignadas.append((cfdi, recibo, len(meses)))
        return asignadas, sin_recibo, ya_capturadas

    def _reportar(self, asignadas, sin_recibo, ya_capturadas):
        por_centro = defaultdict(lambda: [0, Decimal("0")])
        for cfdi, recibo, _n in asignadas:
            por_centro[recibo["centro"]][0] += 1
            por_centro[recibo["centro"]][1] += Decimal(cfdi.total)

        self.stdout.write("ASIGNADO")
        for centro in sorted(por_centro):
            n, monto = por_centro[centro]
            self.stdout.write(f"  {centro:<20} {n:>3} facturas {monto:>14,.2f}")
        total = sum(m for _n, m in por_centro.values())
        self.stdout.write(f"  {'total':<20} {len(asignadas):>3} facturas {total:>14,.2f}")
        pagadas = CfdiPagoRelacionado.objects.filter(
            uuid_relacionado__in=[c.uuid for c, _r, _n in asignadas]
        ).count()
        self.stdout.write(f"  de ellas, con su pago ya registrado en el SAT: {pagadas}")

        if ya_capturadas:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING(
                f"YA CAPTURADO A MANO · {len(ya_capturadas)} recibos"))
            self.stdout.write("  Se respeta la captura existente para no duplicar el gasto del centro.")
            for cfdi, centro, periodo, clave in ya_capturadas:
                self.stdout.write(f"    {periodo:%Y-%m}  {centro:<20} {Decimal(cfdi.total):>12,.2f}  ya estaba como «{clave}»")

        if sin_recibo:
            pendiente = sum(Decimal(c.total) for c in sin_recibo)
            self.stdout.write("")
            self.stdout.write(self.style.WARNING(
                f"SIN RECIBO · {len(sin_recibo)} facturas por {pendiente:,.2f}"))
            self.stdout.write("  No se asignan: falta el recibo que diga a qué medidor son.")
            for cfdi in sin_recibo:
                self.stdout.write(
                    f"    {cfdi.fecha_emision:%Y-%m-%d}  {Decimal(cfdi.total):>12,.2f}  {cfdi.uuid}")
