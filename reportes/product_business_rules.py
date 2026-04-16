from __future__ import annotations


CRITICAL_FIXED_REVENTA_PRODUCT_NAMES = (
    "CAJA CH PARA VENTA",
    "CAJA G PARA VENTA",
    "COCA-COLA 450 ML",
    "TE DEL JARDIN",
    "CAFE STARBUCKS FRAPPUCINO",
)


def normalize_product_name(value: str) -> str:
    return (value or "").strip().upper()
