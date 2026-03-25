from __future__ import annotations

import json

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

FAMILIA_CATEGORIAS_PRESETS = {
    "Accesorios": ["Velas", "Cubiertos", "Complementos"],
    "Bebidas": ["Café", "Frías", "Especiales"],
    "Bollo": ["Chocolate", "Vainilla", "Canela", "Especial"],
    "Cheesecakes": ["Frutales", "Chocolate", "Especiales"],
    "Dona": ["Glaseadas", "Rellenas", "Especiales"],
    "Empanadas": ["Dulces", "Saladas", "Especiales"],
    "Flan": ["Clásico", "Especial"],
    "Galletas": ["Clásicas", "Rellenas", "Especiales"],
    "Pastel": ["Chocolate", "Frutales", "Tres leches", "Especiales"],
    "Pie": ["Queso", "Frutales", "Especiales"],
    "Vasos preparados": ["Fresas", "Chocolate", "Especiales"],
}


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


def familia_categoria_catalogo(extra_values: Iterable[str] | None = None) -> dict[str, list[str]]:
    result = {key: list(values) for key, values in FAMILIA_CATEGORIAS_PRESETS.items()}
    for item in extra_values or []:
        txt = (item or "").strip()
        if not txt:
            continue
        for family, values in result.items():
            if txt not in values:
                continue
            break
    return result


def familia_categoria_catalogo_json(extra_values: Iterable[str] | None = None) -> str:
    return json.dumps(familia_categoria_catalogo(extra_values), ensure_ascii=False)
