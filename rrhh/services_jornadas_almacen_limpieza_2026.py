"""Carga cerrada de jornadas 08:00-16:00 para almacén y limpieza en 2026."""

from datetime import date, time

from .services_jornadas_administrativas_2026 import (
    ConfiguracionJornadasError,
    ConfiguracionJornadasSpec,
    configurar_jornadas_2026,
)


NOMBRE_TURNO = "Almacén y limpieza 2026 08:00-16:00"
NOMBRE_JORNADA = "Almacén y limpieza 2026"
SPEC_ALMACEN_LIMPIEZA = ConfiguracionJornadasSpec(
    inicio=date(2026, 9, 1),
    fin=date(2026, 12, 31),
    motivo="Jornada de almacén y limpieza 2026 aprobada; vigencia septiembre a diciembre",
    lock_key=2026090104,
    manifiesto=(
        (15, "GARCIA HIGUERA BEATRIZ", NOMBRE_JORNADA),
        (16, "GARCIA HIGUERA CLARISELA", NOMBRE_JORNADA),
        (23, "LARA VILLANUEVA ERNESTO", NOMBRE_JORNADA),
        (43, "GALVEZ GALVEZ JOSE ANTONIO", NOMBRE_JORNADA),
    ),
    turnos=((NOMBRE_TURNO, time(8), time(16)),),
    perfiles={NOMBRE_JORNADA: (NOMBRE_TURNO,) * 6 + (None,)},
    modelo_auditoria="rrhh.JornadasAlmacenLimpieza2026",
    reutilizar_turnos_compatibles=False,
)


def configurar_jornadas_almacen_limpieza_2026(
    *, aplicar=False, hoy=None, actor=None, expected_fingerprint=None,
) -> dict:
    """Previsualiza sin escribir y aplica únicamente con una huella fresca."""
    return configurar_jornadas_2026(
        spec=SPEC_ALMACEN_LIMPIEZA,
        aplicar=aplicar,
        hoy=hoy,
        actor=actor,
        expected_fingerprint=expected_fingerprint,
    )


__all__ = (
    "ConfiguracionJornadasError",
    "SPEC_ALMACEN_LIMPIEZA",
    "configurar_jornadas_almacen_limpieza_2026",
)
