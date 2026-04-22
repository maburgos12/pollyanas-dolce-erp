"""
Guardarraíl de integridad histórica.
FactVentaDiaria 2022-2024 fue saneada el 2025-04-22.
Ningún pipeline debe sobreescribir ese rango sin aprobación explícita.
"""
from datetime import date

HISTORICAL_FREEZE_START = date(2022, 1, 1)
HISTORICAL_FREEZE_END = date(2024, 12, 31)


def is_frozen(target_date: date) -> bool:
    """Retorna True si la fecha está en el rango protegido."""
    return HISTORICAL_FREEZE_START <= target_date <= HISTORICAL_FREEZE_END


def assert_not_frozen(target_date: date, caller: str = "") -> None:
    """Lanza ValueError si se intenta escribir en el rango protegido."""
    if is_frozen(target_date):
        raise ValueError(
            f"[FREEZE] {caller} intentó escribir FactVentaDiaria "
            f"para {target_date} (rango protegido 2022-2024). "
            f"Usa --override-freeze para operaciones autorizadas."
        )
