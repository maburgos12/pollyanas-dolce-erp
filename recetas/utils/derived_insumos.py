from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal

from django.db import IntegrityError
from django.utils import timezone

from maestros.models import CostoInsumo, Insumo, UnidadMedida
from recetas.models import Receta, RecetaPresentacion
from recetas.utils.normalizacion import normalizar_nombre


DERIVED_SOURCE = "RECETA_PRESENTACION"


@dataclass
class DerivedSyncStats:
    presentaciones: int = 0
    insumos_creados: int = 0
    insumos_actualizados: int = 0
    insumos_desactivados: int = 0
    costos_creados: int = 0


def _pza_unit() -> UnidadMedida:
    unit = UnidadMedida.objects.filter(codigo="pza").first()
    if unit:
        return unit
    unit, _ = UnidadMedida.objects.get_or_create(
        codigo="pza",
        defaults={
            "nombre": "Pieza",
            "tipo": UnidadMedida.TIPO_PIEZA,
            "factor_to_base": Decimal("1"),
        },
    )
    return unit


def _derived_code(presentacion: RecetaPresentacion) -> str:
    return f"DERIVADO:RECETA:{presentacion.receta_id}:PRESENTACION:{presentacion.id}"


def _derived_name(presentacion: RecetaPresentacion) -> str:
    return f"{presentacion.receta.nombre} - {presentacion.nombre}"[:250]


def _get_or_create_derived_insumo(presentacion: RecetaPresentacion, pza_unit: UnidadMedida) -> tuple[Insumo, bool]:
    code = _derived_code(presentacion)
    insumo = Insumo.objects.filter(codigo=code).order_by("id").first()
    if insumo:
        return insumo, False

    nombre_norm = normalizar_nombre(_derived_name(presentacion))
    insumo = Insumo.objects.filter(nombre_normalizado=nombre_norm).order_by("id").first()
    if insumo:
        if not insumo.codigo:
            insumo.codigo = code
            insumo.save(update_fields=["codigo"])
        return insumo, False

    insumo = Insumo.objects.create(
        codigo=code,
        nombre=_derived_name(presentacion),
        unidad_base=pza_unit,
        activo=bool(presentacion.activo),
    )
    return insumo, True


def _sync_cost_snapshot(
    insumo: Insumo,
    presentacion: RecetaPresentacion,
    costo_unitario: Decimal | None,
) -> bool:
    if costo_unitario is None or costo_unitario <= 0:
        return False

    latest = (
        CostoInsumo.objects.filter(insumo=insumo)
        .order_by("-fecha", "-id")
        .values_list("costo_unitario", flat=True)
        .first()
    )
    if latest is not None and abs(Decimal(str(latest)) - Decimal(str(costo_unitario))) < Decimal("0.000001"):
        return False

    normalized_cost = Decimal(str(costo_unitario)).quantize(Decimal("0.000001"))
    hash_payload = f"{DERIVED_SOURCE}|{insumo.id}|{normalized_cost}"
    source_hash = hashlib.sha256(hash_payload.encode("utf-8")).hexdigest()
    if CostoInsumo.objects.filter(source_hash=source_hash).exists():
        hash_payload = f"{DERIVED_SOURCE}|{insumo.id}|{normalized_cost}|{timezone.now().isoformat()}"
        source_hash = hashlib.sha256(hash_payload.encode("utf-8")).hexdigest()

    try:
        CostoInsumo.objects.create(
            insumo=insumo,
            fecha=timezone.localdate(),
            moneda="MXN",
            costo_unitario=normalized_cost,
            source_hash=source_hash,
            raw={
                "source": DERIVED_SOURCE,
                "receta_id": presentacion.receta_id,
                "presentacion_id": presentacion.id,
                "receta_nombre": presentacion.receta.nombre,
                "presentacion_nombre": presentacion.nombre,
            },
        )
        return True
    except IntegrityError:
        return False


def sync_presentacion_insumo(
    presentacion: RecetaPresentacion,
    *,
    deactivate: bool = False,
) -> DerivedSyncStats:
    stats = DerivedSyncStats(presentaciones=1)
    pza_unit = _pza_unit()
    insumo, created = _get_or_create_derived_insumo(presentacion, pza_unit)
    if created:
        stats.insumos_creados += 1

    updates: list[str] = []
    desired_name = _derived_name(presentacion)
    if insumo.nombre != desired_name:
        insumo.nombre = desired_name
        updates.append("nombre")
    if insumo.unidad_base_id != pza_unit.id:
        insumo.unidad_base = pza_unit
        updates.append("unidad_base")

    desired_active = False if deactivate else bool(presentacion.activo)
    if insumo.activo != desired_active:
        insumo.activo = desired_active
        updates.append("activo")

    expected_code = _derived_code(presentacion)
    if insumo.codigo != expected_code:
        insumo.codigo = expected_code
        updates.append("codigo")

    if updates:
        insumo.save(update_fields=updates)
        if deactivate and "activo" in updates:
            stats.insumos_desactivados += 1
        else:
            stats.insumos_actualizados += 1

    if not deactivate and presentacion.activo:
        if _sync_cost_snapshot(insumo, presentacion, presentacion.costo_por_unidad_estimado):
            stats.costos_creados += 1

    return stats


def sync_receta_presentaciones(receta: Receta) -> DerivedSyncStats:
    total = DerivedSyncStats()
    for presentacion in receta.presentaciones.all():
        s = sync_presentacion_insumo(presentacion)
        total.presentaciones += s.presentaciones
        total.insumos_creados += s.insumos_creados
        total.insumos_actualizados += s.insumos_actualizados
        total.insumos_desactivados += s.insumos_desactivados
        total.costos_creados += s.costos_creados
    return total
