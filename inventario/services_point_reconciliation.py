from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from hashlib import sha256

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from core.models import Sucursal
from inventario.models import (
    ExistenciaInsumo,
    MovimientoInventario,
    UBICACION_ALMACEN,
    UBICACION_CEDIS,
)
from inventario.services_existencias import aplicar_delta
from inventario.stock_trace import set_stock_trace
from pos_bridge.models import PointBranch
from pos_bridge.services.live_inventory_lookup_service import (
    PointLiveInventoryLookupError,
    PointLiveInventoryLookupService,
)
from pos_bridge.services.unidades import cantidad_en_unidad_erp
from recetas.utils.costeo_snapshot import POINT_UNIT_ALIASES


@dataclass(frozen=True, slots=True)
class PointLedgerReconciliation:
    ledger: str
    point_branch_id: str
    point_branch_name: str
    point_qty: Decimal
    point_unit: str
    current_qty: Decimal
    target_qty: Decimal
    delta: Decimal
    captured_at: object
    applied: bool = False
    movement_id: int | None = None


_LEDGERS = (
    (UBICACION_ALMACEN, "ALMACEN", "almacen"),
    (UBICACION_CEDIS, "CEDIS", "cedis"),
)
_QTY_QUANTUM = Decimal("0.001")


def _official_numeric_point_branch(normalized_name: str) -> PointBranch:
    candidates = list(
        PointBranch.objects.filter(
            normalized_name=normalized_name,
            status=PointBranch.STATUS_ACTIVE,
        ).order_by("id")
    )
    numeric = [branch for branch in candidates if str(branch.external_id or "").strip().isdigit()]
    if len(numeric) != 1:
        raise ValidationError(
            f"Se requiere exactamente una sucursal Point numérica activa para {normalized_name}; "
            f"se encontraron {len(numeric)}."
        )
    return numeric[0]


def _point_unit(raw_payload: dict) -> str:
    for key in ("Unidad", "unidad", "Unit", "unit"):
        value = raw_payload.get(key)
        if value not in (None, ""):
            return str(value).strip()
    raise PointLiveInventoryLookupError("Point no devolvió la unidad del insumo; no se aplicó ningún ajuste.")


def _capture_ledger(*, insumo, ledger: str, sucursal_code: str, point_name: str, live_service):
    # ALMACEN es una ubicación logística, no una sucursal comercial; por eso
    # puede estar inactiva para ventas y aun así ser el contexto correcto de Point.
    sucursal = Sucursal.objects.filter(codigo=sucursal_code).first()
    if sucursal is None:
        raise ValidationError(f"No existe la ubicación ERP {sucursal_code}.")
    point_branch = _official_numeric_point_branch(point_name)
    result = live_service.get_stock(
        product_codes=[insumo.codigo_point],
        sucursal=sucursal,
        point_branch=point_branch,
    )
    if result is None:
        raise PointLiveInventoryLookupError("La consulta de inventario en vivo de Point está deshabilitada.")
    point_unit = _point_unit(result.raw_payload)
    normalized_unit = " ".join(point_unit.lower().split())
    if normalized_unit not in POINT_UNIT_ALIASES:
        raise PointLiveInventoryLookupError(
            f"Point devolvió la unidad desconocida '{point_unit}'; no se aplicó ningún ajuste."
        )
    converted, note = cantidad_en_unidad_erp(result.stock_qty, point_unit, insumo)
    if note.startswith("UNIDAD INCOMPATIBLE"):
        raise PointLiveInventoryLookupError(note)
    target = Decimal(str(converted)).quantize(_QTY_QUANTUM)
    current = (
        ExistenciaInsumo.objects.filter(insumo=insumo, almacen=ledger)
        .values_list("stock_actual", flat=True)
        .first()
    )
    current = Decimal(str(current or 0)).quantize(_QTY_QUANTUM)
    return PointLedgerReconciliation(
        ledger=ledger,
        point_branch_id=str(point_branch.external_id),
        point_branch_name=point_branch.name,
        point_qty=Decimal(str(result.stock_qty)),
        point_unit=point_unit,
        current_qty=current,
        target_qty=target,
        delta=(target - current).quantize(_QTY_QUANTUM),
        captured_at=result.captured_at,
    )


def reconcile_insumo_from_point(*, insumo, apply: bool = False, live_service=None, user=None):
    if not (insumo.codigo_point or "").strip():
        raise ValidationError("El insumo requiere código Point para conciliarse.")
    if insumo.unidad_base_id is None:
        raise ValidationError("El insumo requiere unidad base para conciliarse.")

    service = live_service or PointLiveInventoryLookupService()
    captures = [
        _capture_ledger(
            insumo=insumo,
            ledger=ledger,
            sucursal_code=sucursal_code,
            point_name=point_name,
            live_service=service,
        )
        for ledger, sucursal_code, point_name in _LEDGERS
    ]
    if not apply:
        return captures

    applied_results = []
    with transaction.atomic():
        for capture in captures:
            existencia, _ = ExistenciaInsumo.objects.select_for_update().get_or_create(
                insumo=insumo,
                almacen=capture.ledger,
            )
            current = Decimal(str(existencia.stock_actual or 0)).quantize(_QTY_QUANTUM)
            delta = (capture.target_qty - current).quantize(_QTY_QUANTUM)
            if delta == 0:
                applied_results.append(
                    replace(capture, current_qty=current, delta=delta, applied=True, movement_id=None)
                )
                continue

            existencia = aplicar_delta(
                insumo,
                capture.ledger,
                delta,
                permitir_negativo=True,
            )
            reference = f"POINT-{capture.point_branch_id}-{capture.captured_at:%Y%m%d%H%M%S}"
            identity = (
                f"point-reconcile|{insumo.id}|{capture.ledger}|{capture.target_qty}|"
                f"{current}|{delta}|{capture.captured_at.isoformat()}"
            )
            details = {
                "point_branch_id": capture.point_branch_id,
                "point_branch_name": capture.point_branch_name,
                "point_quantity": str(capture.point_qty),
                "point_unit": capture.point_unit,
                "previous_base_quantity": str(current),
                "target_base_quantity": str(capture.target_qty),
                "delta_base_quantity": str(delta),
                "erp_base_unit": insumo.unidad_base.codigo,
            }
            movement = MovimientoInventario.objects.create(
                fecha=timezone.now(),
                tipo=MovimientoInventario.TIPO_AJUSTE,
                insumo=insumo,
                cantidad=delta,
                referencia=reference,
                almacen=capture.ledger,
                notas="Conciliación autoritativa y separada con existencia en vivo de Point",
                registrado_por=getattr(user, "username", "") or "sistema",
                registrado_por_usuario=user if getattr(user, "is_authenticated", False) else None,
                source_hash=sha256(identity.encode("utf-8")).hexdigest(),
                trazabilidad={"source": "POINT_LIVE_RECONCILIATION", **details},
            )
            set_stock_trace(
                existencia,
                source="POINT_LIVE_RECONCILIATION",
                process="inventario.reconcile_insumo_from_point",
                effective_at=capture.captured_at,
                reference=reference,
                user=user,
                details=details,
                save=True,
            )
            applied_results.append(
                replace(
                    capture,
                    current_qty=current,
                    delta=delta,
                    applied=True,
                    movement_id=movement.id,
                )
            )
    return applied_results
