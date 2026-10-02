"""Pone las rentas en la vigencia que su factura prueba.

El maestro se cargó en septiembre, así que todas las vigencias arrancan ahí y
de enero a agosto no se puede generar nada. Pero la renta tiene CFDI mes por
mes: no hay que extrapolar, hay que leer.

Dos correcciones salen de esa lectura:

- **El Túnel** siempre ha sido $6,244.98. Los $6,960 de enero a mayo traían
  $715.02 de un servicio de limpieza del inmueble, que no es renta.
- **Leyva** son $3,960, no los $3,500 que tenía cargados.

Y una más grande: el inmueble de Polyana Fonseca aloja Matriz **y** CEDIS, y su
renta se reparte con la misma regla que ya divide luz, agua y seguro. El maestro
tenía $13,242.02 para Matriz, que es la factura entre cinco en vez del 35%.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import (
    AreaPresupuesto,
    CategoriaGasto,
    CentroCosto,
    GastoRecurrente,
    GastoRecurrenteVersion,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
from reportes.services_gastos_compromisos import _asegurar_fuente_rubro

CATEGORIA = "RENTA"
CONCEPTO_RUBRO = "Arrendamiento local"

# El inmueble de Hernando de Villafañe aloja tienda, planta y corporativo. Su
# renta se reparte igual que la luz, el agua y el seguro de ese mismo domicilio.
COMPARTIDO = "COMPARTIDO_MC"
RENTA_INMUEBLE = Decimal("66210.12")
VIGENCIA_INMUEBLE = date(2026, 1, 1)
REPARTO = [("produccion", 65), ("MATRIZ", 35)]
EVIDENCIA_INMUEBLE = "POLYANA JOSEFINA FONSECA ORTEGON · clave catastral 005-000-002-043-050-001"


def _mes(texto: str) -> date:
    anio, mes = texto.strip().split("-")
    return date(int(anio), int(mes), 1)


class Command(BaseCommand):
    help = "Pone las rentas en la vigencia que su CFDI prueba y reparte el inmueble compartido."

    def add_arguments(self, parser):
        parser.add_argument("--archivo", help="CSV de rentas (por omisión el del repo).")
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        ruta = (
            Path(options["archivo"])
            if options.get("archivo")
            else Path(settings.BASE_DIR) / "reportes" / "data" / "rentas_cfdi_2026.csv"
        )
        if not ruta.exists():
            raise CommandError(f"No encuentro el archivo de rentas: {ruta}")
        categoria = CategoriaGasto.objects.filter(codigo=CATEGORIA).first()
        if categoria is None:
            raise CommandError(f"No existe la categoría de gasto {CATEGORIA}.")

        with transaction.atomic():
            self._por_sucursal(ruta, categoria)
            self._inmueble_compartido(categoria)
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    # ------------------------------------------------------------------ #

    def _por_sucursal(self, ruta: Path, categoria) -> None:
        self.stdout.write("RENTAS POR SUCURSAL · vigencia que prueba el CFDI")
        versiones: dict[str, list] = {}
        with ruta.open(encoding="utf-8") as fh:
            for fila in csv.DictReader(l for l in fh if not l.startswith("#")):
                fin = (fila.get("vigencia_fin") or "").strip()
                versiones.setdefault(fila["centro_costo"].strip(), []).append(
                    (
                        _mes(fila["vigencia_inicio"]),
                        Decimal(fila["monto"]),
                        fila["evidencia"].strip(),
                        date.fromisoformat(fin) if fin else None,
                    )
                )

        for codigo, tarifas in versiones.items():
            centro = CentroCosto.objects.filter(codigo=codigo).first()
            if centro is None:
                self.stdout.write(self.style.WARNING(f"  {codigo}: no existe el centro, se omite"))
                continue
            contrato = (
                GastoRecurrente.objects.filter(
                    centro_costo=centro, categoria_gasto=categoria, activo=True
                ).first()
            )
            if contrato is None:
                # El local de Crucero perdió su contrato al separarse Bamoa: el
                # suyo viajó a la tienda nueva y su renta de ene-jun quedó sin
                # dónde reconocerse.
                contrato = self._contrato_nuevo(centro, categoria, codigo)
                if contrato is None:
                    self.stdout.write(
                        self.style.WARNING(f"  {codigo}: sin contrato ni rubro de renta, se omite")
                    )
                    continue
            if contrato.obligaciones.exists():
                raise CommandError(
                    f"El contrato {contrato.pk} ({codigo}) ya tiene obligaciones: "
                    "retroceder su vigencia alteraría cargos ya reconocidos."
                )
            anterior = contrato.versiones.order_by("-vigencia_inicio").first()
            viejo = anterior.monto if anterior else Decimal("0")
            contrato.versiones.all().delete()
            for inicio, monto, evidencia, fin in sorted(tarifas):
                GastoRecurrenteVersion.objects.create(
                    gasto_recurrente=contrato,
                    vigencia_inicio=inicio,
                    vigencia_fin=fin,
                    monto=monto,
                    periodicidad_meses=1,
                    dia_vencimiento=1,
                    condicion_pago="CONTADO",
                    motivo=evidencia[:200],
                )
            cambio = "" if viejo == tarifas[0][1] else f"  (antes {viejo:,.2f})"
            detalle = " · ".join(
                f"{m:%b}: {v:,.2f}" + (f" hasta {f:%b}" if f else "")
                for m, v, _e, f in sorted(tarifas)
            )
            self.stdout.write(f"  {codigo:<13} {detalle}{cambio}")

    def _contrato_nuevo(self, centro, categoria, codigo):
        rubro = RubroPresupuesto.objects.filter(
            concepto=CONCEPTO_RUBRO, sucursal_id=centro.sucursal_id, activo=True
        ).first()
        if rubro is None:
            return None
        return GastoRecurrente.objects.create(
            area=rubro.area,
            rubro=rubro,
            centro_costo=centro,
            categoria_gasto=categoria,
            concepto=f"Arrendamiento local {centro.nombre or codigo}",
        )

    def _inmueble_compartido(self, categoria) -> None:
        """La renta del inmueble se reparte, no se asigna a una sola sucursal."""
        self.stdout.write("")
        self.stdout.write("INMUEBLE MATRIZ-CEDIS · se reparte como la luz, el agua y el seguro")
        centro = CentroCosto.objects.filter(codigo=COMPARTIDO).first()
        if centro is None:
            raise CommandError(f"No existe el centro de costo {COMPARTIDO}.")

        destinos = {}
        for clave, porcentaje in REPARTO:
            rubro = self._rubro_destino(clave)
            destinos[clave] = rubro
            regla, nueva = ReglaFuenteRubro.objects.get_or_create(
                rubro=rubro,
                tipo_fuente=ReglaFuenteRubro.FUENTE_GASTO_OPERATIVO,
                categoria_gasto=categoria,
                centro_costo=centro,
                defaults={
                    "activa": True,
                    "modo_asignacion": ReglaFuenteRubro.MODO_DISTRIBUCION,
                    "filtros": {"porcentaje": porcentaje},
                    "notas": "Reparto del inmueble compartido, igual que luz, agua y seguro.",
                },
            )
            if not nueva:
                regla.activa = True
                regla.modo_asignacion = ReglaFuenteRubro.MODO_DISTRIBUCION
                regla.filtros = {"porcentaje": porcentaje}
                regla.save(update_fields=["activa", "modo_asignacion", "filtros", "actualizado_en"])
            parte = (RENTA_INMUEBLE * porcentaje / 100).quantize(Decimal("0.01"))
            self.stdout.write(
                f"  {'alta' if nueva else 'ya existía':<11} {porcentaje}% → {clave:<12} {parte:>12,.2f}"
            )

        # El contrato vive en el centro compartido: ahí lo recogen las dos reglas.
        contrato, creado = GastoRecurrente.objects.get_or_create(
            centro_costo=centro,
            categoria_gasto=categoria,
            activo=True,
            defaults={
                "area": destinos["MATRIZ"].area,
                "rubro": destinos["MATRIZ"],
                "concepto": "Arrendamiento del inmueble Matriz-CEDIS",
                "proveedor": "Polyana Josefina Fonseca Ortegon",
            },
        )
        if contrato.obligaciones.exists():
            raise CommandError("El contrato del inmueble ya tiene obligaciones; no se reescribe.")
        contrato.versiones.all().delete()
        GastoRecurrenteVersion.objects.create(
            gasto_recurrente=contrato,
            vigencia_inicio=VIGENCIA_INMUEBLE,
            monto=RENTA_INMUEBLE,
            periodicidad_meses=1,
            dia_vencimiento=1,
            condicion_pago="CONTADO",
            motivo=EVIDENCIA_INMUEBLE,
        )
        self.stdout.write(
            f"  {'alta' if creado else 'corregido':<11} contrato del inmueble {RENTA_INMUEBLE:>12,.2f}"
        )

        # El contrato de Matriz tenía la factura entre cinco: lo reemplaza el reparto.
        matriz = CentroCosto.objects.filter(codigo="MATRIZ").first()
        viejo = GastoRecurrente.objects.filter(
            centro_costo=matriz, categoria_gasto=categoria, activo=True
        ).first()
        if viejo is not None and viejo.pk != contrato.pk:
            if viejo.obligaciones.exists():
                raise CommandError(f"El contrato {viejo.pk} de Matriz ya tiene obligaciones.")
            version = viejo.versiones.order_by("-vigencia_inicio").first()
            importe = version.monto if version else Decimal("0")
            viejo.activo = False
            viejo.save(update_fields=["activo", "actualizado_en"])
            self.stdout.write(
                f"  retirado    contrato de Matriz por {importe:,.2f} "
                f"(era la factura entre cinco, no el 35%)"
            )

    def _rubro_destino(self, clave: str) -> RubroPresupuesto:
        """Producción no tenía rubro de renta: se abre, como se hizo con luz y agua."""
        if clave == "MATRIZ":
            rubro = RubroPresupuesto.objects.filter(
                concepto=CONCEPTO_RUBRO, sucursal__codigo="MATRIZ", activo=True
            ).first()
            if rubro is None:
                raise CommandError("No existe el rubro de arrendamiento de Matriz.")
            return rubro
        area = AreaPresupuesto.objects.filter(codigo="produccion").first()
        if area is None:
            raise CommandError("No existe el área de presupuesto 'produccion'.")
        rubro, creado = RubroPresupuesto.objects.get_or_create(
            concepto=CONCEPTO_RUBRO,
            area=area,
            sucursal=None,
            defaults={"tipo": RubroPresupuesto.TIPO_EGRESO, "activo": True},
        )
        if creado:
            for mes in range(1, 13):
                LineaPresupuestoMensual.objects.get_or_create(
                    rubro=rubro,
                    periodo=date(2026, mes, 1),
                    version=LineaPresupuestoMensual.VERSION_ORIGINAL,
                    defaults={"monto_presupuesto": Decimal("0")},
                )
            self.stdout.write("  alta        rubro de renta en producción, con sus 12 renglones")
        return rubro
