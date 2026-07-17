"""
Sincroniza existencias de insumos desde Point ALMACEN como fuente única de verdad.

Obtiene la cantidad actual de cada insumo en ALMACEN directamente de Point vía
browser automation, luego actualiza ExistenciaInsumo.stock_actual en el ERP para
que coincida exactamente.

Por defecto es dry-run. Usa --apply para guardar cambios.
"""
from __future__ import annotations

import hashlib
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from inventario.models import ExistenciaInsumo, MovimientoInventario
from pos_bridge.services.point_inventory_cost_capture_service import PointInventoryCostCaptureService
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService
from pos_bridge.services.unidades import cantidad_en_unidad_erp as _cantidad_compartida


def _cantidad_en_unidad_erp(cantidad, unidad_point, insumo):
    """Delegado al helper compartido (pos_bridge.services.unidades)."""
    return _cantidad_compartida(cantidad, unidad_point, insumo)


class Command(BaseCommand):
    help = (
        "Sincroniza existencias de insumos desde Point ALMACEN (fuente única de verdad). "
        "Dry-run por defecto. Usa --apply para guardar."
    )

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Aplica cambios a la BD.")
        parser.add_argument("--branch", default="ALMACEN", help="Sucursal Point de almacén (default: ALMACEN).")
        parser.add_argument("--solo-reporte", action="store_true", help="Solo imprime el reporte sin aplicar nada.")

    @transaction.atomic
    def handle(self, *args, **options):
        apply = bool(options["apply"])
        branch_hint = (options["branch"] or "ALMACEN").strip()
        dry = not apply

        if dry:
            self.stdout.write(self.style.WARNING(
                "── DRY-RUN: ningún cambio se guardará. Usa --apply para confirmar.\n"
            ))

        self.stdout.write(f"Conectando a Point ({branch_hint})...")
        service = PointInventoryCostCaptureService()
        identity = PointRecipeIdentityService()

        rows = service.capture_all_rows(branch_hint=branch_hint)
        self.stdout.write(f"Point entregó {len(rows)} filas de insumos desde {branch_hint}.\n")

        if not rows:
            self.stdout.write(self.style.WARNING("Sin filas de Point. Verifica la conexión o el nombre de la sucursal."))
            if dry:
                transaction.set_rollback(True)
            return

        synced = 0
        sin_match = 0
        sin_cantidad = 0
        sin_cambio = 0
        errores: list[str] = []

        now = timezone.now()
        sync_ref = f"SYNC_POINT_{branch_hint}_{now.strftime('%Y%m%d_%H%M%S')}"

        for row in rows:
            if row.quantity <= 0:
                sin_cantidad += 1
                continue

            resolved = identity.resolve_insumo(point_code=row.point_code, point_name=row.point_name)
            if resolved.insumo is None:
                sin_match += 1
                self.stdout.write(
                    self.style.WARNING(f"  SIN MATCH: {row.point_name!r} (código: {row.point_code})")
                )
                continue

            insumo = resolved.insumo
            existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=insumo)
            stock_previo = Decimal(str(existencia.stock_actual or 0))
            stock_point, nota_conversion = _cantidad_en_unidad_erp(row.quantity, row.unit, insumo)
            if nota_conversion.startswith("UNIDAD INCOMPATIBLE"):
                errores.append(f"{insumo.nombre}: {nota_conversion}")
                continue

            if stock_previo == stock_point:
                sin_cambio += 1
                continue

            unidad_erp = insumo.unidad_base.codigo if insumo.unidad_base else row.unit
            delta_str = f"{stock_previo:.3f} → {stock_point:.3f} {unidad_erp}"
            if nota_conversion:
                delta_str += f" ({nota_conversion})"
            self.stdout.write(
                self.style.SUCCESS(f"  {insumo.nombre}: {delta_str}")
            )

            if not dry:
                existencia.stock_actual = stock_point
                existencia.actualizado_en = now
                existencia.save(update_fields=["stock_actual", "actualizado_en"])

                source_hash = hashlib.sha256(
                    f"SYNC_POINT|{insumo.id}|{branch_hint}|{stock_point}|{now.isoformat()}".encode()
                ).hexdigest()
                MovimientoInventario.objects.get_or_create(
                    source_hash=source_hash,
                    defaults={
                        "fecha": now,
                        "tipo": MovimientoInventario.TIPO_AJUSTE,
                        "insumo": insumo,
                        "cantidad": stock_point,
                        "referencia": sync_ref,
                    },
                )

            synced += 1

        self.stdout.write(f"\n{'─'*60}")
        self.stdout.write(f"  Filas Point        : {len(rows)}")
        self.stdout.write(f"  Actualizados       : {synced}")
        self.stdout.write(f"  Sin cambio         : {sin_cambio}")
        self.stdout.write(f"  Sin match ERP      : {sin_match}  ← no encontrados en catálogo ERP")
        self.stdout.write(f"  Sin cantidad (=0)  : {sin_cantidad}")
        if errores:
            self.stdout.write(self.style.ERROR(f"  Errores            : {len(errores)}"))
            for e in errores:
                self.stdout.write(self.style.ERROR(f"    · {e}"))

        if dry:
            transaction.set_rollback(True)
            self.stdout.write(self.style.WARNING("\nDRY-RUN completo. Usa --apply para guardar.\n"))
        else:
            self.stdout.write(self.style.SUCCESS(f"\n✓ Inventario sincronizado desde Point ({branch_hint}).\n"))
