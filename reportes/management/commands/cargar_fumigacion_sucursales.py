"""Da de alta el contrato de fumigación de cada sucursal en el maestro.

Los rubros por sucursal traen presupuesto de $450 y real capturado del mismo
importe, pero ninguna regla viva: su gasto dejó de llegar cuando se agotó el
Excel. Este comando registra el contrato para que el maestro lo alimente.

Quedan fuera dos sucursales, a propósito:
  · Bamoa ya recibe su parte por la regla de distribución al 35% sobre el
    centro compartido de Crucero; un contrato propio la duplicaría.
  · Plaza Las Glorias no tiene una sola captura de fumigación en 2026, así que
    no hay con qué respaldar un importe.

Por omisión es simulacro. Escribe sólo con --apply.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from reportes.models import (
    AreaPresupuesto,
    CategoriaGasto,
    CentroCosto,
    GastoRecurrente,
    RubroPresupuesto,
)
from reportes.services_gastos_compromisos import (
    _asegurar_fuente_rubro,
    crear_gasto_recurrente,
)

MONTO = Decimal("450.00")
CATEGORIA = "INDIRECTO_SUC"
PROVEEDOR = ""

# (rubro, centro de costo, mes desde el que hay evidencia de servicio)
SUCURSALES = [
    (944, "COLOSIO", date(2026, 1, 1)),
    (1522, "EL_TUNEL", date(2026, 1, 1)),
    (1046, "LEYVA", date(2026, 1, 1)),
    (1081, "MATRIZ", date(2026, 1, 1)),
    (1012, "PAYAN", date(2026, 1, 1)),
    (1558, "PLAZA_NIO", date(2026, 1, 1)),
    (835, "GUAMUCHIL", date(2026, 3, 1)),  # sus capturas empiezan en marzo
]


class Command(BaseCommand):
    help = "Registra el contrato de fumigación mensual de cada sucursal en el maestro."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Escribe (por omisión simula).")
        parser.add_argument("--usuario", default="admin")

    def handle(self, *args, **options):
        aplicar = options["apply"]
        usuario = get_user_model().objects.filter(username=options["usuario"]).first()
        if usuario is None:
            raise CommandError(f"No existe el usuario '{options['usuario']}'.")
        categoria = CategoriaGasto.objects.get(codigo=CATEGORIA)

        with transaction.atomic():
            creados = 0
            for rubro_id, centro_codigo, vigencia in SUCURSALES:
                rubro = RubroPresupuesto.objects.get(pk=rubro_id)
                centro = CentroCosto.objects.get(codigo=centro_codigo)
                if GastoRecurrente.objects.filter(
                    rubro=rubro, categoria_gasto=categoria, activo=True
                ).exists():
                    self.stdout.write(f"  ya existe, se omite: {rubro.sucursal}")
                    continue
                area = AreaPresupuesto.objects.get(pk=rubro.area_id)
                crear_gasto_recurrente(
                    usuario=usuario,
                    area=area,
                    rubro=rubro,
                    centro_costo=centro,
                    categoria_gasto=categoria,
                    concepto=f"Fumigación y sanitización {rubro.sucursal}",
                    vigencia_inicio=vigencia,
                    monto=MONTO,
                    dia_vencimiento=1,
                    condicion_pago="CONTADO",
                    proveedor=PROVEEDOR,
                    periodicidad_meses=1,
                    motivo="Alta del contrato de fumigación por sucursal",
                )
                # crear_gasto_recurrente no cablea la fuente; sin esto el rubro
                # queda mudo y la obligación no se lee.
                _asegurar_fuente_rubro(
                    rubro=rubro, centro_costo=centro, categoria_gasto=categoria
                )
                creados += 1
                self.stdout.write(
                    f"  alta: {rubro.sucursal} · {MONTO} /mes · desde {vigencia:%Y-%m}"
                )

            self.stdout.write("")
            self.stdout.write(f"  contratos dados de alta: {creados}")
            self.stdout.write(f"  compromiso mensual      : {MONTO * creados:,.2f}")
            if not aplicar:
                transaction.set_rollback(True)
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("[APLICADO]" if aplicar else "[SIMULACRO] nada se escribió"))
