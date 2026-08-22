from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from inventario.units import presentation_quantity


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
