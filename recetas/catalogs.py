from __future__ import annotations

from typing import Iterable


POINT_FAMILIAS_BASE = [
    "Accesorios",
    "Bebidas",
    "Bollo",
    "Cheesecakes",
    "Dona",
    "Empanadas",
    "Flan",
    "Galletas",
    "Pastel",
    "Pie",
    "Vasos preparados",
]


def familias_producto_catalogo(extra_values: Iterable[str] | None = None) -> list[str]:
    """Retorna catálogo de familias (base Point + valores existentes en DB)."""
    seen: set[str] = set()
    result: list[str] = []

    def _add(value: str):
        txt = (value or "").strip()
        if not txt:
            return
        key = txt.lower()
        if key in seen:
            return
        seen.add(key)
        result.append(txt)

    for item in POINT_FAMILIAS_BASE:
        _add(item)
    for item in (extra_values or []):
        _add(item)
    return result

