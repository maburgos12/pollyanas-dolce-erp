"""A qué área pertenece el costo de cada persona de la nómina.

El departamento dice de quién depende alguien, no qué hace. Dentro de
Producción conviven gente que fabrica, quien resguarda inventarios, quien
lleva producto a las sucursales y el jefe que reporta a administración. Para
costear hay que separarlos.

La clasificación se apoya en los dos campos estructurados del expediente
—`nivel_organizacional` y `puesto_operativo`, este último ligado al
`CatalogoFuncionOperativa`— y no en el texto libre `puesto`, que está vacío en
dos de cada tres expedientes.
"""
from __future__ import annotations

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

# Los jefes de área pertenecen a corporativo, no al costo de lo que dirigen
# (dirección, 2026-09-21). Encargadas y supervisión sí se quedan en su área:
# están en el piso.
NIVELES_CORPORATIVOS = frozenset({"JEFATURA", "DIRECCION"})

# Funciones operativas cuyo costo no es del área de la que dependen.
DESTINO_POR_AREA = {
    # Lleva producto a las sucursales: es logística, aunque hoy dependa de
    # producción por no haber jefe de área.
    "ENVIO_SUCURSAL": DESTINO_LOGISTICA,
    # Resguarda inventarios de insumos preparados y producto terminado.
    "CUARTOS_FRIOS": DESTINO_CEDIS,
}

# Todo lo demás cuenta en el área de su departamento. En producción eso incluye
# HORNOS, EMBETUNADO, ARMADO y PREPARACIONES —masas, bases, galletas, betunes—,
# que son las recetas de tipo PREPARACION del catálogo.
#
# CRUCERO sigue apareciendo en expedientes sin reasignar: marcaba a quien
# producía fuera de CEDIS y esa división por locación ya no existe, así que
# cuenta como producción y por eso no está en DESTINO_POR_AREA.


def destino_de(nivel_organizacional, puesto_operativo, *, predeterminado=DESTINO_PRODUCCION) -> str:
    """Dónde cae el costo de una persona. El predeterminado es el caso normal."""
    nivel = str(nivel_organizacional or "").strip().upper()
    if nivel in NIVELES_CORPORATIVOS:
        return DESTINO_ADMINISTRACION
    area = str(puesto_operativo or "").strip().upper()
    return DESTINO_POR_AREA.get(area, predeterminado)
