from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from types import SimpleNamespace

from django.conf import settings
from django.utils import timezone

from inventario.units import presentation_quantity
from pos_bridge.models import PointInsumoInventorySnapshot, PointSyncJob


class InventoryLocation(StrEnum):
    ALMACEN = "ALMACEN"
    CEDIS = "CEDIS"


class InventoryFreshness(StrEnum):
    FRESH = "FRESH"
    STALE = "STALE"
    MISSING = "MISSING"
    ERROR = "ERROR"


@dataclass(frozen=True, slots=True)
class CanonicalInventoryReading:
    insumo_id: int
    codigo_point: str
    location: InventoryLocation
    quantity_base: Decimal | None
    base_unit: str
    display_quantity: Decimal | None
    display_unit: str
    point_quantity: Decimal | None
    point_unit: str
    captured_at: datetime | None
    freshness: InventoryFreshness
    source: str = "POINT"
    sync_job_id: int | None = None
    error: str = ""


def require_inventory_location(value: InventoryLocation | str | None) -> InventoryLocation:
    if value is None:
        raise ValueError("La ubicación de inventario es obligatoria.")
    try:
        return value if isinstance(value, InventoryLocation) else InventoryLocation(str(value).upper())
    except ValueError as exc:
        raise ValueError("La ubicación debe ser ALMACEN o CEDIS.") from exc


def display_quantity(quantity: Decimal, unidad) -> tuple[Decimal, str]:
    return presentation_quantity(quantity, unidad)


class CanonicalInventoryUnavailable(RuntimeError):
    def __init__(self, readings: list[CanonicalInventoryReading]):
        self.readings = readings
        super().__init__("Point no tiene inventario vigente para completar esta decisión.")


class CanonicalPointInventoryService:
    BRANCH_BY_LOCATION = {
        InventoryLocation.ALMACEN: "almacen",
        InventoryLocation.CEDIS: "cedis",
    }

    def read_many(self, insumos, *, location, now=None):
        from maestros.utils.canonical_catalog import canonical_insumo_by_id

        location = require_inventory_location(location)
        now = now or timezone.now()
        insumos = list(insumos)
        canonical_by_input = {
            item.id: canonical_insumo_by_id(item.id) or item
            for item in insumos
        }
        canonical_items = {item.id: item for item in canonical_by_input.values()}
        latest_job = (
            PointSyncJob.objects.filter(
                job_type=PointSyncJob.JOB_TYPE_INVENTORY,
                parameters__canonical_insumo_inventory=True,
                status__in=[
                    PointSyncJob.STATUS_SUCCESS,
                    PointSyncJob.STATUS_FAILED,
                    PointSyncJob.STATUS_PARTIAL,
                ],
            )
            .order_by("-started_at", "-id")
            .first()
        )
        location_summary = (
            ((latest_job.result_summary or {}).get("locations") or {}).get(location.value, {})
            if latest_job
            else {}
        )
        location_is_usable = bool(
            latest_job
            and (
                latest_job.status == PointSyncJob.STATUS_SUCCESS
                or (
                    latest_job.status == PointSyncJob.STATUS_PARTIAL
                    and int(location_summary.get("rows") or 0) > 0
                    and int(location_summary.get("snapshots") or 0) > 0
                )
            )
        )
        if not location_is_usable:
            return {
                item.id: self._cycle_error(canonical_by_input[item.id], location, latest_job)
                for item in insumos
            }
        rows = (
            PointInsumoInventorySnapshot.objects.filter(
                sync_job=latest_job,
                branch__normalized_name=self.BRANCH_BY_LOCATION[location],
                insumo_id__in=list(canonical_items),
            )
            .select_related("insumo", "sync_job")
        )
        latest = {row.insumo_id: row for row in rows}
        return {
            item.id: self._reading(
                item=canonical_by_input[item.id],
                location=location,
                snapshot=latest.get(canonical_by_input[item.id].id),
                now=now,
            )
            for item in insumos
        }

    def require_fresh(self, insumos, *, location, now=None):
        readings = self.read_many(insumos, location=location, now=now)
        invalid = [
            reading
            for reading in readings.values()
            if reading.freshness is not InventoryFreshness.FRESH
        ]
        if invalid:
            raise CanonicalInventoryUnavailable(invalid)
        return readings

    def _reading(self, *, item, location, snapshot, now):
        code = (item.codigo_point or "").strip()
        base_unit = getattr(item.unidad_base, "codigo", "") or ""
        if not code:
            return self._missing(item, location, code, base_unit, "El insumo no tiene código Point.")
        if item.unidad_base_id is None:
            return self._missing(item, location, code, base_unit, "El insumo no tiene unidad base ERP.")
        if snapshot is None:
            return self._missing(item, location, code, base_unit, "Point no tiene captura para esta ubicación.")

        point_unit = snapshot.point_unit
        if not point_unit:
            return self._error(
                item,
                location,
                code,
                base_unit,
                snapshot,
                point_unit,
                "Point no reportó una unidad reconocida.",
            )

        quantity_base = snapshot.quantity_base
        display_value, display_unit = display_quantity(quantity_base, item.unidad_base)
        max_age_minutes = int(settings.POINT_INVENTORY_CANONICAL_MAX_AGE_MINUTES)
        freshness = (
            InventoryFreshness.STALE
            if now - snapshot.captured_at > timedelta(minutes=max_age_minutes)
            else InventoryFreshness.FRESH
        )
        return CanonicalInventoryReading(
            insumo_id=item.id,
            codigo_point=code,
            location=location,
            quantity_base=quantity_base,
            base_unit=base_unit,
            display_quantity=display_value,
            display_unit=display_unit,
            point_quantity=snapshot.point_quantity,
            point_unit=point_unit,
            captured_at=snapshot.captured_at,
            freshness=freshness,
            sync_job_id=snapshot.sync_job_id,
        )

    def _cycle_error(self, item, location, sync_job):
        code = (item.codigo_point or "").strip()
        base_unit = getattr(item.unidad_base, "codigo", "") or ""
        status = getattr(sync_job, "status", "MISSING")
        return CanonicalInventoryReading(
            insumo_id=item.id,
            codigo_point=code,
            location=location,
            quantity_base=None,
            base_unit=base_unit,
            display_quantity=None,
            display_unit="",
            point_quantity=None,
            point_unit="",
            captured_at=getattr(sync_job, "finished_at", None),
            freshness=InventoryFreshness.ERROR if sync_job else InventoryFreshness.MISSING,
            sync_job_id=getattr(sync_job, "id", None),
            error=f"El último ciclo canónico Point no es válido ({status}).",
        )

    @staticmethod
    def _missing(item, location, code, base_unit, error):
        return CanonicalInventoryReading(
            insumo_id=item.id,
            codigo_point=code,
            location=location,
            quantity_base=None,
            base_unit=base_unit,
            display_quantity=None,
            display_unit="",
            point_quantity=None,
            point_unit="",
            captured_at=None,
            freshness=InventoryFreshness.MISSING,
            error=error,
        )

    @staticmethod
    def _error(item, location, code, base_unit, snapshot, point_unit, error):
        return CanonicalInventoryReading(
            insumo_id=item.id,
            codigo_point=code,
            location=location,
            quantity_base=None,
            base_unit=base_unit,
            display_quantity=None,
            display_unit="",
            point_quantity=snapshot.point_quantity,
            point_unit=point_unit,
            captured_at=snapshot.captured_at,
            freshness=InventoryFreshness.ERROR,
            sync_job_id=snapshot.sync_job_id,
            error=error,
        )


def canonical_point_inventory_report_rows(*, location, insumos=None, limit=2000):
    from inventario.models import ExistenciaInsumo, UBICACION_ALMACEN, UBICACION_CEDIS
    from maestros.utils.canonical_catalog import canonical_insumo_by_id, canonicalized_active_insumos

    location = require_inventory_location(location)
    if insumos is None:
        catalog_rows = canonicalized_active_insumos(limit=limit)
    else:
        grouped = {}
        for item in insumos:
            canonical = canonical_insumo_by_id(item.id) or item
            bucket = grouped.setdefault(canonical.id, {"canonical": canonical, "member_ids": []})
            if item.id not in bucket["member_ids"]:
                bucket["member_ids"].append(item.id)
            if canonical.id not in bucket["member_ids"]:
                bucket["member_ids"].append(canonical.id)
        catalog_rows = list(grouped.values())[:limit]

    canonical_items = [row["canonical"] for row in catalog_rows]
    readings = CanonicalPointInventoryService().read_many(canonical_items, location=location)
    member_ids = [member_id for row in catalog_rows for member_id in row["member_ids"]]
    erp_location = UBICACION_CEDIS if location is InventoryLocation.CEDIS else UBICACION_ALMACEN
    policies = {}
    for policy in ExistenciaInsumo.objects.filter(
        insumo_id__in=member_ids,
        almacen=erp_location,
    ).order_by("insumo_id", "-actualizado_en", "-id"):
        policies.setdefault(policy.insumo_id, policy)

    result = []
    for catalog_row in catalog_rows:
        item = catalog_row["canonical"]
        reading = readings[item.id]
        policy = policies.get(item.id)
        if policy is None:
            policy = next(
                (policies[member_id] for member_id in catalog_row["member_ids"] if member_id in policies),
                None,
            )
        stock_minimo = Decimal(str(getattr(policy, "stock_minimo", 0) or 0))
        stock_maximo = Decimal(str(getattr(policy, "stock_maximo", 0) or 0))
        punto_reorden = Decimal(str(getattr(policy, "punto_reorden", 0) or 0))
        inventario_promedio = Decimal(str(getattr(policy, "inventario_promedio", 0) or 0))
        consumo_diario_promedio = Decimal(
            str(getattr(policy, "consumo_diario_promedio", 0) or 0)
        )
        stock_minimo_display, policy_display_unit = display_quantity(stock_minimo, item.unidad_base)
        stock_maximo_display, _ = display_quantity(stock_maximo, item.unidad_base)
        punto_reorden_display, _ = display_quantity(punto_reorden, item.unidad_base)
        inventario_promedio_display, _ = display_quantity(inventario_promedio, item.unidad_base)
        consumo_diario_promedio_display, _ = display_quantity(
            consumo_diario_promedio,
            item.unidad_base,
        )
        result.append(
            SimpleNamespace(
                insumo=item,
                insumo_id=item.id,
                stock_actual=reading.quantity_base,
                stock_actual_display=reading.display_quantity,
                display_unit=reading.display_unit or policy_display_unit,
                stock_minimo=stock_minimo,
                stock_minimo_display=stock_minimo_display,
                stock_maximo=stock_maximo,
                stock_maximo_display=stock_maximo_display,
                punto_reorden=punto_reorden,
                punto_reorden_display=punto_reorden_display,
                inventario_promedio=inventario_promedio,
                inventario_promedio_display=inventario_promedio_display,
                dias_llegada_pedido=int(getattr(policy, "dias_llegada_pedido", 0) or 0),
                consumo_diario_promedio=consumo_diario_promedio,
                consumo_diario_promedio_display=consumo_diario_promedio_display,
                inventory_source=reading.source,
                inventory_location=location,
                inventory_freshness=reading.freshness,
                inventory_captured_at=reading.captured_at,
                inventory_error=reading.error,
                inventory_decision_ready=reading.freshness is InventoryFreshness.FRESH,
                canonical_variant_count=len(catalog_row["member_ids"]),
            )
        )
    return result
