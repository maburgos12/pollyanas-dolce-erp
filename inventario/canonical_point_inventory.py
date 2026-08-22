from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from django.conf import settings
from django.utils import timezone

from inventario.units import presentation_quantity
from pos_bridge.models import PointInventorySnapshot
from pos_bridge.services.unidades import cantidad_en_unidad_erp
from recetas.utils.costeo_snapshot import POINT_UNIT_ALIASES


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
        location = require_inventory_location(location)
        now = now or timezone.now()
        insumos = list(insumos)
        codes = {
            (item.codigo_point or "").strip()
            for item in insumos
            if (item.codigo_point or "").strip()
        }
        rows = (
            PointInventorySnapshot.objects.filter(
                branch__normalized_name=self.BRANCH_BY_LOCATION[location],
                product__sku__in=codes,
            )
            .select_related("product", "sync_job")
            .order_by("product__sku", "-captured_at", "-id")
            .distinct("product__sku")
        )
        latest = {row.product.sku.strip(): row for row in rows}
        return {
            item.id: self._reading(
                item=item,
                location=location,
                snapshot=latest.get((item.codigo_point or "").strip()),
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

        point_unit = self._point_unit(snapshot.raw_payload)
        normalized_point_unit = " ".join(point_unit.lower().split())
        if not point_unit or normalized_point_unit not in POINT_UNIT_ALIASES:
            return self._error(
                item,
                location,
                code,
                base_unit,
                snapshot,
                point_unit,
                "Point no reportó una unidad reconocida.",
            )

        quantity_base, conversion_note = cantidad_en_unidad_erp(snapshot.stock, point_unit, item)
        if conversion_note.startswith("UNIDAD INCOMPATIBLE"):
            return self._error(
                item,
                location,
                code,
                base_unit,
                snapshot,
                point_unit,
                conversion_note,
            )

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
            point_quantity=snapshot.stock,
            point_unit=point_unit,
            captured_at=snapshot.captured_at,
            freshness=freshness,
            sync_job_id=snapshot.sync_job_id,
        )

    @staticmethod
    def _point_unit(raw_payload):
        payload = raw_payload or {}
        for key in ("Unidad", "unidad", "Unit", "unit"):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value).strip()
        return ""

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
            point_quantity=snapshot.stock,
            point_unit=point_unit,
            captured_at=snapshot.captured_at,
            freshness=InventoryFreshness.ERROR,
            sync_job_id=snapshot.sync_job_id,
            error=error,
        )
