from __future__ import annotations

from recetas.models import Receta
from recetas.utils.normalizacion import normalizar_nombre


SPECIAL_EVENT_TOKENS = {
    "dia de la madre": "Día de la Madre",
    "dia de las madres": "Día de las Madres",
    "dia del padre": "Día del Padre",
    "san valentin": "San Valentín",
    "valentin": "San Valentín",
    "halloween": "Halloween",
    "dia de muertos": "Día de Muertos",
    "navidad": "Navidad",
    "navideno": "Navidad",
    "navidena": "Navidad",
    "reyes": "Día de Reyes",
    "rosca": "Día de Reyes",
    "independencia": "Independencia",
    "patrio": "Fiestas Patrias",
    "patria": "Fiestas Patrias",
    "pascua": "Pascua",
}

TEMPORARY_TOKENS = (
    "temporada",
    "edicion limitada",
    "edicion especial",
    "especial ",
    "promo",
    "promocion",
    "limited",
)


def inferir_temporalidad_receta(nombre: str) -> tuple[str, str]:
    normalized_name = normalizar_nombre(nombre)
    if not normalized_name:
        return Receta.TEMPORALIDAD_PERMANENTE, ""

    for token, label in SPECIAL_EVENT_TOKENS.items():
        if token in normalized_name:
            return Receta.TEMPORALIDAD_FECHA_ESPECIAL, label

    if any(token in normalized_name for token in TEMPORARY_TOKENS):
        return Receta.TEMPORALIDAD_TEMPORAL, "Temporal / campaña"

    return Receta.TEMPORALIDAD_PERMANENTE, ""
