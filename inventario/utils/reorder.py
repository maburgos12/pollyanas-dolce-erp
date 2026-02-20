from __future__ import annotations

from decimal import Decimal


FORMULA_EXCEL_LEGACY = "excel_legacy"
FORMULA_LEADTIME_PLUS_SAFETY = "leadtime_plus_safety"


def calcular_punto_reorden(
    *,
    stock_minimo: Decimal,
    dias_llegada_pedido: int,
    consumo_diario_promedio: Decimal,
    formula: str = FORMULA_EXCEL_LEGACY,
) -> Decimal:
    m = Decimal(str(stock_minimo or 0))
    s = max(int(dias_llegada_pedido or 0), 0)
    t = Decimal(str(consumo_diario_promedio or 0))

    if formula == FORMULA_LEADTIME_PLUS_SAFETY:
        # Variante estándar: demanda durante lead time + stock de seguridad.
        return (Decimal(s) * t) + m

    # Fórmula heredada de archivo de almacén:
    # Punto retorno = (días llegada + consumo diario promedio) * stock mínimo.
    return (Decimal(s) + t) * m
