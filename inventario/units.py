from __future__ import annotations

from decimal import Decimal


_PRESENTATION_UNITS = {
    "g": (Decimal("1000"), "kg"),
    "ml": (Decimal("1000"), "L"),
}


def _presentation_rule(unidad) -> tuple[Decimal, str]:
    code = str(getattr(unidad, "codigo", "") or "").strip()
    return _PRESENTATION_UNITS.get(code.lower(), (Decimal("1"), code or "-"))


def presentation_quantity(quantity, unidad) -> tuple[Decimal, str]:
    factor, label = _presentation_rule(unidad)
    return Decimal(str(quantity or 0)) / factor, label


def from_presentation_quantity(quantity, unidad) -> Decimal:
    factor, _label = _presentation_rule(unidad)
    return Decimal(str(quantity or 0)) * factor
