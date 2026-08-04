"""
Sincroniza existencias de insumos desde Point ALMACEN como fuente única de verdad.

Obtiene la cantidad actual de cada insumo en ALMACEN directamente de Point vía
browser automation, luego actualiza ExistenciaInsumo.stock_actual en el ERP para
que coincida exactamente.

Por defecto es dry-run. Usa --apply para guardar cambios.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from inventario.models import AlmacenSyncRun, MovimientoInventario
from inventario.services_existencias import establecer_stock, stock_ubicacion
from inventario.stock_trace import TRACE_MANUAL_SYNC, set_stock_trace
from pos_bridge.services.inventory_baseline import (
    POINT_ALMACEN_BASELINE_PREFIX,
    TRUSTED_INSUMO_MATCH_METHODS,
)
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
        apply = bool(options["apply"]) and not bool(options["solo_reporte"])
        branch_hint = (options["branch"] or "ALMACEN").strip()
        dry = not apply

        if apply and branch_hint.upper() != "ALMACEN":
            raise CommandError("El corte absoluto solo puede aplicarse contra la sucursal Point ALMACEN.")

        if dry:
            self.stdout.write(self.style.WARNING(
                "── DRY-RUN: ningún cambio se guardará. Usa --apply para confirmar.\n"
            ))

        self.stdout.write(f"Conectando a Point ({branch_hint})...")
        service = PointInventoryCostCaptureService()
        identity = PointRecipeIdentityService()

        rows = service.capture_all_rows(branch_hint=branch_hint)
        supply_rows = [row for row in rows if getattr(row, "kind", "supply") == "supply"]
        product_rows = len(rows) - len(supply_rows)
        self.stdout.write(
            f"Point entregó {len(supply_rows)} filas de insumos desde {branch_hint}; "
            f"{product_rows} filas de productos fueron excluidas.\n"
        )

        if not supply_rows:
            raise CommandError("Point no entregó insumos. No se creó ni modificó la línea base de ALMACÉN.")

        candidates: dict[int, dict] = {}
        duplicate_rows: dict[int, list[dict]] = defaultdict(list)
        blockers: list[dict] = []
        for row in supply_rows:
            resolved = identity.resolve_insumo(point_code=row.point_code, point_name=row.point_name)
            if resolved.insumo is None:
                blockers.append(
                    {
                        "reason": "SIN_MATCH",
                        "point_code": row.point_code,
                        "point_name": row.point_name,
                        "quantity": str(row.quantity),
                        "unit": row.unit,
                    }
                )
                continue
            method = str(getattr(resolved, "method", "") or "")
            score = float(getattr(resolved, "score", 0) or 0)
            if method not in TRUSTED_INSUMO_MATCH_METHODS or score < 100:
                blockers.append(
                    {
                        "reason": "MATCH_NO_CONFIABLE",
                        "point_code": row.point_code,
                        "point_name": row.point_name,
                        "insumo_id": resolved.insumo.id,
                        "insumo_name": resolved.insumo.nombre,
                        "method": method,
                        "score": score,
                    }
                )
                continue

            insumo = resolved.insumo
            stock_point, nota_conversion = _cantidad_en_unidad_erp(row.quantity, row.unit, insumo)
            stock_point = Decimal(str(stock_point))
            if nota_conversion.startswith("UNIDAD INCOMPATIBLE"):
                blockers.append(
                    {
                        "reason": "UNIDAD_INCOMPATIBLE",
                        "point_code": row.point_code,
                        "point_name": row.point_name,
                        "insumo_id": insumo.id,
                        "insumo_name": insumo.nombre,
                        "detail": nota_conversion,
                    }
                )
                continue
            if stock_point < 0:
                blockers.append(
                    {
                        "reason": "SALDO_POINT_NEGATIVO",
                        "point_code": row.point_code,
                        "point_name": row.point_name,
                        "point_quantity": str(row.quantity),
                        "point_unit": row.unit,
                        "insumo_id": insumo.id,
                        "insumo_name": insumo.nombre,
                        "stock_erp_convertido": str(stock_point),
                    }
                )
                continue

            candidate = {
                "insumo": insumo,
                "stock_point": stock_point,
                "point_code": row.point_code,
                "point_name": row.point_name,
                "point_unit": row.unit,
                "match_method": method,
                "conversion_note": nota_conversion,
            }
            duplicate_rows[insumo.id].append(candidate)
            existing_candidate = candidates.get(insumo.id)
            if existing_candidate is None:
                candidates[insumo.id] = candidate
                continue
            if existing_candidate["stock_point"] != candidate["stock_point"]:
                raise CommandError(
                    "Conflicto de saldo Point para "
                    f"{insumo.nombre}: {existing_candidate['stock_point']} vs {candidate['stock_point']}. "
                    "No se aplicó ningún cambio."
                )

        if not candidates:
            raise CommandError("Point no entregó ningún insumo confiable para establecer la línea base.")

        now = timezone.now()
        sync_ref = f"SYNC_POINT_{branch_hint}_{now.strftime('%Y%m%d_%H%M%S')}"
        changed = 0
        unchanged = 0
        zero_targets = 0
        movement_count = 0

        run = None
        if not dry:
            run = AlmacenSyncRun.objects.create(
                source=AlmacenSyncRun.SOURCE_MANUAL,
                status=AlmacenSyncRun.STATUS_OK,
                started_at=now,
                rows_stock_read=len(supply_rows),
                matched=len(candidates),
                unmatched=len(blockers),
                pending_preview=blockers,
                message=f"{POINT_ALMACEN_BASELINE_PREFIX}branch={branch_hint}|status=APPLYING",
            )

        for candidate in candidates.values():
            insumo = candidate["insumo"]
            stock_point = candidate["stock_point"]
            stock_previo = stock_ubicacion(insumo, "ALMACEN_1")
            delta = stock_point - stock_previo
            if stock_point == 0:
                zero_targets += 1

            if stock_previo == stock_point:
                unchanged += 1
            else:
                changed += 1

            unidad_erp = insumo.unidad_base.codigo if insumo.unidad_base else candidate["point_unit"]
            delta_str = f"{stock_previo:.3f} → {stock_point:.3f} {unidad_erp}"
            if candidate["conversion_note"]:
                delta_str += f" ({candidate['conversion_note']})"
            self.stdout.write(
                self.style.SUCCESS(f"  {insumo.nombre}: {delta_str}")
            )

            if not dry:
                existencia = establecer_stock(insumo, "ALMACEN_1", stock_point)
                set_stock_trace(
                    existencia,
                    source=TRACE_MANUAL_SYNC,
                    process="pos_bridge.sync_inventario_desde_point",
                    effective_at=now,
                    reference=sync_ref,
                    run=run,
                    details={
                        "branch": branch_hint,
                        "point_code": candidate["point_code"],
                        "point_name": candidate["point_name"],
                        "point_stock": str(stock_point),
                        "previous_stock": str(stock_previo),
                        "match_method": candidate["match_method"],
                    },
                    save=True,
                )
                if delta != 0:
                    source_hash = hashlib.sha256(
                        f"{sync_ref}|{insumo.id}|ALMACEN_1".encode()
                    ).hexdigest()
                    _, movement_created = MovimientoInventario.objects.get_or_create(
                        source_hash=source_hash,
                        defaults={
                            "fecha": now,
                            "tipo": MovimientoInventario.TIPO_AJUSTE,
                            "insumo": insumo,
                            "cantidad": delta,
                            "almacen": "ALMACEN_1",
                            "referencia": sync_ref,
                            "trazabilidad": {
                                "source": "POINT_ALMACEN_BASELINE",
                                "previous_stock": str(stock_previo),
                                "point_stock": str(stock_point),
                                "run_id": run.id,
                            },
                        },
                    )
                    movement_count += int(movement_created)

        if run is not None:
            cutover_at = timezone.now()
            run.finished_at = cutover_at
            run.existencias_updated = changed
            run.movimientos_created = movement_count
            run.message = (
                f"{POINT_ALMACEN_BASELINE_PREFIX}branch={branch_hint}|"
                f"cutover_at={cutover_at.isoformat()}|reference={sync_ref}"
            )
            run.save(
                update_fields=[
                    "finished_at",
                    "existencias_updated",
                    "movimientos_created",
                    "message",
                ]
            )

        self.stdout.write(f"\n{'─'*60}")
        self.stdout.write(f"  Filas insumos Point: {len(supply_rows)}")
        self.stdout.write(f"  Productos excluidos: {product_rows}")
        self.stdout.write(f"  Insumos confiables : {len(candidates)}")
        self.stdout.write(f"  Actualizados       : {changed}")
        self.stdout.write(f"  Sin cambio         : {unchanged}")
        self.stdout.write(f"  Establecidos en 0  : {zero_targets}")
        self.stdout.write(f"  Bloqueados         : {len(blockers)}")
        self.stdout.write(f"  Duplicados iguales : {sum(max(len(items) - 1, 0) for items in duplicate_rows.values())}")
        for blocker in blockers:
            self.stdout.write(self.style.WARNING(f"    · {blocker}"))

        if dry:
            self.stdout.write(self.style.WARNING("\nDRY-RUN completo. Usa --apply para guardar.\n"))
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n✓ Línea base ALMACÉN aplicada desde Point ({branch_hint}); "
                    f"corte={run.finished_at.isoformat()}.\n"
                )
            )
