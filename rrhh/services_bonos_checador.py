from __future__ import annotations

import logging
from datetime import date

from django.db import transaction

from rrhh.models import Empleado


log = logging.getLogger(__name__)


def sincronizar_bonos_desde_checador(empleado: Empleado, fecha: date) -> dict:
    resultado = {
        "produccion": {},
        "ventas": {},
    }

    try:
        from bonos_produccion.services_checador import sincronizar_empleado_dia_desde_checador

        resultado["produccion"] = sincronizar_empleado_dia_desde_checador(empleado.id, fecha)
    except Exception as exc:
        log.warning("Error sincronizando bonos produccion desde checador para %s %s: %s", empleado, fecha, exc)
        resultado["produccion"] = {"error": str(exc)}

    try:
        from bonos_ventas.services_checador import sincronizar_empleado_dia_desde_checador

        resultado["ventas"] = sincronizar_empleado_dia_desde_checador(empleado.id, fecha)
    except Exception as exc:
        log.warning("Error sincronizando bonos ventas desde checador para %s %s: %s", empleado, fecha, exc)
        resultado["ventas"] = {"error": str(exc)}

    return resultado


def programar_sincronizacion_bonos_desde_checador(empleado_id: int, fecha: date) -> None:
    def _sync() -> None:
        empleado = Empleado.objects.filter(pk=empleado_id).first()
        if empleado is None:
            log.warning("No se sincronizaron bonos: empleado %s no existe.", empleado_id)
            return
        sincronizar_bonos_desde_checador(empleado, fecha)

    transaction.on_commit(_sync)
