from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from maestros.models import Insumo
from recetas.utils.normalizacion import normalizar_nombre


STATUS_OK = "OK"
STATUS_QTY_NO_POSITIVA_CON_COSTO = "QTY_NO_POSITIVA_CON_COSTO"
STATUS_NOMBRE_POINT_NO_COINCIDE = "NOMBRE_POINT_NO_COINCIDE"
STATUS_UNIDAD_INCOMPATIBLE = "UNIDAD_INCOMPATIBLE"
STATUS_UNIDAD_DESCONOCIDA = "UNIDAD_DESCONOCIDA"

UNIT_ALIAS = {
    "g": "g",
    "gr": "g",
    "gramo": "g",
    "gramos": "g",
    "kg": "kg",
    "kilo": "kg",
    "kilogramo": "kg",
    "kilogramos": "kg",
    "ml": "ml",
    "mililitro": "ml",
    "mililitros": "ml",
    "l": "lt",
    "lt": "lt",
    "lts": "lt",
    "litro": "lt",
    "litros": "lt",
    "pza": "pza",
    "pz": "pza",
    "pieza": "pza",
    "piezas": "pza",
    "unidad": "unidad",
    "unidades": "unidad",
    "u": "unidad",
}
UNIT_TYPE = {
    "g": "MASS",
    "kg": "MASS",
    "ml": "VOLUME",
    "lt": "VOLUME",
    "pza": "UNIT",
    "unidad": "UNIT",
}
STOPWORDS = {"de", "del", "la", "el", "y", "para", "con"}


@dataclass(frozen=True, slots=True)
class PointCostValidationResult:
    status: str
    reasons: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.status == STATUS_OK


def decimal_or_none(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except Exception:
        return None


def point_unit_code(value: str) -> str | None:
    return UNIT_ALIAS.get(str(value or "").strip().lower().rstrip("."))


def point_unit_type(value: str) -> str | None:
    return UNIT_TYPE.get(point_unit_code(value) or "")


def name_matches_insumo(insumo: Insumo, point_name: str) -> bool:
    point_norm = normalizar_nombre(point_name or "")
    erp_names = [
        normalizar_nombre(insumo.nombre or ""),
        normalizar_nombre(insumo.nombre_point or ""),
        normalizar_nombre(insumo.nombre_normalizado or ""),
    ]
    erp_names.extend(
        normalizar_nombre(alias_name or "")
        for alias_name in insumo.aliases.values_list("nombre", flat=True)
    )
    erp_names = [name for name in erp_names if name]
    if not point_norm or not erp_names:
        return True
    if any(point_norm == name or point_norm in name or name in point_norm for name in erp_names):
        return True

    point_tokens = {token for token in point_norm.split() if len(token) > 2 and token not in STOPWORDS}
    erp_tokens = {
        token
        for token in " ".join(erp_names).split()
        if len(token) > 2 and token not in STOPWORDS
    }
    required_overlap = min(2, len(point_tokens))
    return bool(point_tokens and erp_tokens and len(point_tokens & erp_tokens) >= required_overlap)


def validate_point_inventory_cost_row(row: Any, insumo: Insumo) -> PointCostValidationResult:
    reasons: list[str] = []
    quantity = decimal_or_none(getattr(row, "quantity", None))
    unit_cost = decimal_or_none(getattr(row, "unit_cost", None)) or Decimal("0")
    raw_unit = str(getattr(row, "unit", "") or "").strip()

    if quantity is not None and quantity <= 0 and unit_cost > 0:
        reasons.append(STATUS_QTY_NO_POSITIVA_CON_COSTO)

    if not name_matches_insumo(insumo, str(getattr(row, "point_name", "") or "")):
        reasons.append(STATUS_NOMBRE_POINT_NO_COINCIDE)

    point_type = point_unit_type(raw_unit)
    erp_type = getattr(insumo.unidad_base, "tipo", None) if insumo.unidad_base_id else None
    if raw_unit and point_type and erp_type and point_type != erp_type:
        reasons.append(STATUS_UNIDAD_INCOMPATIBLE)
    if raw_unit and not point_unit_code(raw_unit):
        reasons.append(STATUS_UNIDAD_DESCONOCIDA)

    if not reasons:
        return PointCostValidationResult(status=STATUS_OK)
    return PointCostValidationResult(status=reasons[0], reasons=tuple(reasons))
