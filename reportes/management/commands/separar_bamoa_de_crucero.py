"""Separa Bamoa de Crucero: son dos tiendas distintas en un mismo registro.

El catálogo tiene una sola sucursal que hoy se llama Bamoa pero arrastra la
historia de Crucero desde 2025. Las ventas lo confirman: Crucero cerró el
21-jun-2026 y Bamoa abrió el 14-jul, con 23 días sin una sola venta en medio.
Point ya las distingue —tiene una sucursal «Crucero» que dejó de reportar el
20-jun y una «Bamoa» creada el 28-jul— pero ambas apuntan al mismo registro.

El registro existente conserva su historia y vuelve a llamarse Crucero; Bamoa
nace vacía y recibe sólo lo que le corresponde, de julio en adelante.

Esta es la etapa 1: catálogo, Point, centros de costo, rubros y presupuesto.
La operación —ventas, producción, inventario, nómina— va por separado.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import Sucursal
from pos_bridge.models.branch import PointBranch
from reportes.models import (
    CentroCosto,
    GastoOperativoMensual,
    GastoRecurrente,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
    RubroPresupuesto,
)

CORTE = date(2026, 7, 1)          # el presupuesto y los gastos son mensuales
APERTURA_BAMOA = date(2026, 7, 14)  # primera venta, tras 23 días sin ninguna

SUCURSAL_ORIGEN = 4
NOMBRE_CRUCERO = "Sucursal Crucero"
NOMBRE_BAMOA = "Sucursal Bamoa"
CODIGO_BAMOA = "BAMOA"
CENTRO_BAMOA = "BAMOA"

# Mapeos de Point que pasan a la sucursal nueva. El de «Crucero» se queda.
POINT_A_BAMOA = ("Bamoa", "2")


class Command(BaseCommand):
    help = "Separa la sucursal Bamoa de Crucero en el catálogo (etapa 1)."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        with transaction.atomic():
            crucero, bamoa = self._catalogo()
            self._point(crucero, bamoa)
            centro = self._centros(bamoa)
            rubros = self._rubros(crucero, bamoa)
            self._presupuesto(rubros)
            self._gastos(centro)
            self._contratos(centro, rubros)
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))

    # ------------------------------------------------------------------ #

    def _catalogo(self):
        crucero = Sucursal.objects.get(pk=SUCURSAL_ORIGEN)
        self.stdout.write("CATÁLOGO")
        self.stdout.write(f"  {crucero.codigo} «{crucero.nombre}» → «{NOMBRE_CRUCERO}», inactiva")
        crucero.nombre = NOMBRE_CRUCERO
        crucero.activa = False
        crucero.save(update_fields=["nombre", "activa"])

        bamoa, creada = Sucursal.objects.get_or_create(
            codigo=CODIGO_BAMOA,
            defaults={
                "nombre": NOMBRE_BAMOA,
                "activa": True,
                "fecha_apertura": APERTURA_BAMOA,
            },
        )
        self.stdout.write(
            f"  {'alta' if creada else 'ya existía'}: {bamoa.codigo} «{bamoa.nombre}»"
            f" · apertura {APERTURA_BAMOA:%d-%b-%Y}"
        )
        return crucero, bamoa

    def _point(self, crucero, bamoa):
        self.stdout.write("")
        self.stdout.write("POINT")
        for sucursal_point in PointBranch.objects.filter(erp_branch_id=SUCURSAL_ORIGEN).order_by("id"):
            if sucursal_point.external_id in POINT_A_BAMOA:
                sucursal_point.erp_branch = bamoa
                sucursal_point.save(update_fields=["erp_branch", "updated_at"])
                destino = bamoa.nombre
            else:
                destino = crucero.nombre
            visto = sucursal_point.last_seen_at
            self.stdout.write(
                f"  «{sucursal_point.name}» (id externo {sucursal_point.external_id}) → {destino}"
                + (f" · última señal {visto:%d-%b-%Y}" if visto else "")
            )

    def _centros(self, bamoa):
        self.stdout.write("")
        self.stdout.write("CENTROS DE COSTO")
        self.stdout.write("  CRUCERO y SUC_CRUCERO se quedan con Crucero: su historia es suya")
        centro, creado = CentroCosto.objects.get_or_create(
            codigo=CENTRO_BAMOA,
            defaults={
                "nombre": NOMBRE_BAMOA,
                "tipo": CentroCosto.TIPO_SUCURSAL,
                "sucursal": bamoa,
            },
        )
        self.stdout.write(f"  {'alta' if creado else 'ya existía'}: centro {centro.codigo} → {bamoa.nombre}")
        return centro

    def _rubros(self, crucero, bamoa):
        """Clona los rubros para Bamoa. Los originales se quedan con Crucero."""
        self.stdout.write("")
        self.stdout.write("RUBROS")
        equivalencia: dict[int, RubroPresupuesto] = {}
        originales = RubroPresupuesto.objects.filter(sucursal=crucero).order_by("id")
        for original in originales:
            metadata = dict(original.metadata or {})
            metadata["separado_de_rubro"] = original.pk
            metadata["separado_motivo"] = "Bamoa se separó de Crucero: son dos tiendas distintas"
            espejo, creado = RubroPresupuesto.objects.get_or_create(
                sucursal=bamoa,
                concepto=original.concepto,
                area=original.area,
                defaults={
                    "codigo_cuenta": original.codigo_cuenta,
                    "tipo": original.tipo,
                    "activo": original.activo,
                    "metadata": metadata,
                },
            )
            equivalencia[original.pk] = espejo
        self.stdout.write(f"  {len(originales)} rubros se quedan con Crucero")
        self.stdout.write(f"  {len(equivalencia)} rubros espejo creados para Bamoa")
        return equivalencia

    def _presupuesto(self, equivalencia):
        """De julio en adelante el presupuesto es de Bamoa; antes, de Crucero."""
        self.stdout.write("")
        self.stdout.write("PRESUPUESTO")
        movidas = 0
        monto_ppto = monto_real = 0
        for original_id, espejo in equivalencia.items():
            lineas = LineaPresupuestoMensual.objects.filter(
                rubro_id=original_id, periodo__gte=CORTE, periodo__year=2026
            )
            for linea in lineas:
                monto_ppto += linea.monto_presupuesto or 0
                monto_real += linea.monto_real or 0
            movidas += lineas.update(rubro=espejo)
        self.stdout.write(f"  {movidas} líneas de jul–dic movidas a Bamoa")
        self.stdout.write(f"  presupuesto: {monto_ppto:,.2f} · real: {monto_real:,.2f}")

    def _gastos(self, centro):
        """Las capturas desde julio en el centro de Crucero son de Bamoa."""
        self.stdout.write("")
        self.stdout.write("CAPTURAS DE GASTO")
        capturas = GastoOperativoMensual.objects.filter(
            centro_costo__codigo__in=("CRUCERO", "SUC_CRUCERO"), periodo__gte=CORTE
        )
        monto = sum(c.monto for c in capturas)
        movidas = capturas.update(centro_costo=centro)
        self.stdout.write(f"  {movidas} capturas desde julio movidas al centro de Bamoa · {monto:,.2f}")

    def _contratos(self, centro, equivalencia):
        """Los contratos del maestro se cargaron para la tienda viva: Bamoa."""
        self.stdout.write("")
        self.stdout.write("CONTRATOS DEL MAESTRO")
        contratos = GastoRecurrente.objects.filter(
            centro_costo__codigo="CRUCERO", activo=True
        ).select_related("categoria_gasto")
        for contrato in contratos:
            espejo = equivalencia.get(contrato.rubro_id)
            contrato.centro_costo = centro
            contrato.concepto = contrato.concepto.replace("Crucero", "Bamoa")
            campos = ["centro_costo", "concepto", "actualizado_en"]
            if espejo is not None:
                # La regla que lee la obligación tiene que seguir al rubro.
                ReglaFuenteRubro.objects.filter(
                    rubro_id=contrato.rubro_id,
                    tipo_fuente=ReglaFuenteRubro.FUENTE_OBLIGACION_GASTO,
                ).update(rubro=espejo)
                contrato.rubro = espejo
                campos.append("rubro")
            contrato.save(update_fields=campos)
            self.stdout.write(f"  {contrato.categoria_gasto.codigo}: {contrato.concepto}")
