"""Corrige movimientos de inventario históricos con la unidad Point sin convertir.

Los syncs de traspasos y producción copiaban la cantidad cruda de Point
(kg/litro) a MovimientoInventario, cuyo campo se interpreta en la unidad
base del insumo (g/ml) — entradas 1000× subestimadas en ~60 insumos
(Queso crema, Media crema, Aceite...). La unidad original está guardada
en cada PointTransferLine/PointProductionLine, así que la corrección es
exacta (se re-deriva, no se adivina). Idempotente vía source_hash.

Después de aplicar, recalcular el consumo: calcular_consumo_insumos por
cada mes afectado (el comando los lista al final).
"""

from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db import transaction

from inventario.models import MovimientoInventario
from pos_bridge.models import PointProductionLine, PointTransferLine
from pos_bridge.services.unidades import cantidad_en_unidad_erp


class Command(BaseCommand):
    help = "Re-deriva la cantidad de movimientos POINT-TRANSFER/POINT-PROD desde su unidad original"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    @transaction.atomic
    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        corregidos = 0
        sin_linea = 0
        meses_afectados = set()

        fuentes = [
            (PointTransferLine, "received_quantity"),
            (PointProductionLine, "produced_quantity"),
        ]
        for modelo, campo in fuentes:
            lineas = modelo.objects.filter(insumo__isnull=False).select_related(
                "insumo", "insumo__unidad_base"
            )
            for linea in lineas.iterator():
                mov = MovimientoInventario.objects.filter(source_hash=linea.source_hash).first()
                if mov is None:
                    sin_linea += 1
                    continue
                cantidad_point = Decimal(str(getattr(linea, campo) or 0))
                esperada, nota = cantidad_en_unidad_erp(cantidad_point, linea.unit, linea.insumo)
                if nota.startswith("UNIDAD INCOMPATIBLE"):
                    self.stdout.write(self.style.WARNING(f"  {linea.insumo.nombre}: {nota}"))
                    continue
                if mov.cantidad == esperada:
                    continue
                corregidos += 1
                meses_afectados.add(mov.fecha.strftime("%Y-%m"))
                if corregidos <= 30:
                    self.stdout.write(
                        f"  {mov.fecha.date()} {linea.insumo.nombre[:32]:32} "
                        f"{mov.cantidad} → {esperada} ({linea.unit})"
                    )
                if not dry_run:
                    mov.cantidad = esperada
                    mov.notas = (mov.notas or "") + " | unidad corregida 2026-07-17"
                    mov.save(update_fields=["cantidad", "notas"])
        if corregidos > 30:
            self.stdout.write(f"  ... y {corregidos - 30} más")
        if dry_run:
            transaction.set_rollback(True)
        modo = "DRY-RUN" if dry_run else "APLICADO"
        self.stdout.write(self.style.SUCCESS(
            f"[{modo}] corregidos: {corregidos} · líneas sin movimiento: {sin_linea}"
        ))
        if meses_afectados:
            self.stdout.write(
                "Recalcular consumo de: " + ", ".join(sorted(meses_afectados))
                + "  (manage.py calcular_consumo_insumos --period YYYY-MM)"
            )
