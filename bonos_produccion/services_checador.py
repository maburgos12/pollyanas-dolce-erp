from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, timedelta

from django.db import transaction

from rrhh.models import AsistenciaEmpleado, IncidenciaAsistencia

from .models import BonoProduccionEmpleado, ConfigBonoPeriodo, RegistroDiarioProduccion
from .services_recalculo import recalcular_desde_registros


TIPOS_LLEGADA_TARDE = {
    IncidenciaAsistencia.TIPO_USO_TOLERANCIA,
    IncidenciaAsistencia.TIPO_RETARDO,
    IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
    IncidenciaAsistencia.TIPO_FALTA,
}


def _rango_periodo(periodo: ConfigBonoPeriodo) -> tuple[date, date]:
    if periodo.fecha_inicio and periodo.fecha_fin:
        return periodo.fecha_inicio, periodo.fecha_fin
    ultimo_dia = calendar.monthrange(periodo.anio, periodo.mes)[1]
    return date(periodo.anio, periodo.mes, 1), date(periodo.anio, periodo.mes, ultimo_dia)


def _fechas(inicio: date, fin: date) -> list[date]:
    dias = (fin - inicio).days
    return [inicio + timedelta(days=offset) for offset in range(dias + 1)]


def _cargar_asistencias(empleado_ids: list[int], inicio: date, fin: date) -> set[tuple[int, date]]:
    return set(
        AsistenciaEmpleado.objects.filter(
            empleado_id__in=empleado_ids,
            fecha__range=(inicio, fin),
        ).values_list("empleado_id", "fecha")
    )


def _cargar_incidencias(
    empleado_ids: list[int],
    inicio: date,
    fin: date,
) -> dict[tuple[int, date], set[tuple[str, str]]]:
    incidencias = defaultdict(set)
    for empleado_id, fecha, tipo, estado in IncidenciaAsistencia.objects.filter(
        empleado_id__in=empleado_ids,
        fecha__range=(inicio, fin),
        tipo__in=TIPOS_LLEGADA_TARDE | {IncidenciaAsistencia.TIPO_SUSPENSION},
    ).values_list("empleado_id", "fecha", "tipo", "estado"):
        incidencias[(empleado_id, fecha)].add((tipo, estado))
    return incidencias


def _evaluar_dia(
    empleado_id: int,
    fecha: date,
    asistencias: set[tuple[int, date]],
    incidencias: dict[tuple[int, date], set[tuple[str, str]]],
) -> tuple[bool, bool]:
    key = (empleado_id, fecha)
    incidencias_dia = incidencias.get(key, set())
    tipos_pendientes = {
        tipo for tipo, estado in incidencias_dia
        if estado == IncidenciaAsistencia.ESTADO_PENDIENTE
    }
    tiene_suspension_activa = (
        IncidenciaAsistencia.TIPO_SUSPENSION,
        IncidenciaAsistencia.ESTADO_CONCILIADO,
    ) in incidencias_dia

    if tiene_suspension_activa:
        return False, False
    if key not in asistencias:
        return False, False

    falta_pendiente = (
        IncidenciaAsistencia.TIPO_FALTA,
        IncidenciaAsistencia.ESTADO_PENDIENTE,
    ) in incidencias_dia
    tiene_asistencia = not falta_pendiente
    tiene_puntualidad = not bool(tipos_pendientes & TIPOS_LLEGADA_TARDE)
    return tiene_asistencia, tiene_puntualidad


def sincronizar_asistencia_desde_checador(periodo: ConfigBonoPeriodo) -> dict:
    inicio, fin = _rango_periodo(periodo)
    dias = _fechas(inicio, fin)
    bonos_borrador = list(
        periodo.bonos.select_related("empleado").filter(estatus=BonoProduccionEmpleado.ESTATUS_BORRADOR)
    )
    bonos_omitidos = periodo.bonos.exclude(estatus=BonoProduccionEmpleado.ESTATUS_BORRADOR).count()
    empleado_ids = [bono.empleado_id for bono in bonos_borrador]

    asistencias = _cargar_asistencias(empleado_ids, inicio, fin) if empleado_ids else set()
    incidencias = _cargar_incidencias(empleado_ids, inicio, fin) if empleado_ids else {}

    resultado = {
        "bonos_sincronizados": 0,
        "bonos_omitidos": bonos_omitidos,
        "registros_creados": 0,
        "registros_actualizados": 0,
    }

    for bono in bonos_borrador:
        with transaction.atomic():
            for fecha in dias:
                tiene_asistencia, tiene_puntualidad = _evaluar_dia(
                    bono.empleado_id,
                    fecha,
                    asistencias,
                    incidencias,
                )
                registro, created = RegistroDiarioProduccion.objects.get_or_create(
                    bono=bono,
                    dia=fecha.day,
                    defaults={
                        "tiene_asistencia": tiene_asistencia,
                        "tiene_puntualidad": tiene_puntualidad,
                    },
                )
                if created:
                    resultado["registros_creados"] += 1
                elif (
                    registro.tiene_asistencia != tiene_asistencia
                    or registro.tiene_puntualidad != tiene_puntualidad
                ):
                    registro.tiene_asistencia = tiene_asistencia
                    registro.tiene_puntualidad = tiene_puntualidad
                    registro.save(update_fields=["tiene_asistencia", "tiene_puntualidad"])
                    resultado["registros_actualizados"] += 1
            recalcular_desde_registros(bono)
            resultado["bonos_sincronizados"] += 1

    return resultado
