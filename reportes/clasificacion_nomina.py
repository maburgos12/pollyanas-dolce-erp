"""A qué área pertenece el costo de cada persona de la nómina.

El departamento dice de quién depende alguien, no qué hace. Dentro de
Producción conviven gente que fabrica, quien resguarda inventarios, quien
lleva producto a las sucursales y el jefe que reporta a administración. Para
costear hay que separarlos, y el departamento por sí solo no alcanza.
"""
from __future__ import annotations

from unidecode import unidecode

DESTINO_PRODUCCION = "PRODUCCION"
DESTINO_CEDIS = "CEDIS"
DESTINO_LOGISTICA = "LOGISTICA"
DESTINO_ADMINISTRACION = "ADMINISTRACION"

DESTINOS_VALIDOS = (
    DESTINO_PRODUCCION,
    DESTINO_CEDIS,
    DESTINO_LOGISTICA,
    DESTINO_ADMINISTRACION,
)

# Reglas dadas por dirección el 2026-09-21. El puesto de RRHH manda sobre el
# área operativa porque dentro de una misma área conviven destinos distintos:
# cuartos fríos y envío a sucursales comparten `ENVIO_SUCURSAL`.
DESTINO_POR_PUESTO = {
    # Reporta a administración, no al costo de fabricar.
    "jefe de produccion": DESTINO_ADMINISTRACION,
    # Resguarda inventarios de insumos preparados y producto terminado.
    "cuartos frios": DESTINO_CEDIS,
    # Es logística, aunque hoy dependa de producción por no haber jefe de área.
    "envio a sucursales": DESTINO_LOGISTICA,
}

DESTINO_POR_AREA = {
    "ENVIO_SUCURSAL": DESTINO_LOGISTICA,
}

# «Crucero» marcaba a quien producía fuera de CEDIS. Esa división por locación
# ya no existe: hoy están en la planta, así que cuentan como producción y por
# eso no aparecen en DESTINO_POR_AREA.


def _normalizar(valor) -> str:
    return unidecode(str(valor or "")).strip().lower()


def destino_de(puesto, puesto_operativo, *, predeterminado=DESTINO_PRODUCCION) -> str:
    """Dónde cae el costo de una persona. El predeterminado es el caso normal."""
    puesto_normalizado = _normalizar(puesto)
    if puesto_normalizado in DESTINO_POR_PUESTO:
        return DESTINO_POR_PUESTO[puesto_normalizado]
    area = str(puesto_operativo or "").strip().upper()
    return DESTINO_POR_AREA.get(area, predeterminado)
