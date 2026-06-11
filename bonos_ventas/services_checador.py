from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, timedelta

from django.db import transaction
from django.db.models import Q

from rrhh.models import AsistenciaEmpleado, IncidenciaAsistencia

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, RegistroDiarioVentas
from .services_recalculo import recalcular_desde_registros


ESTATUS_BORRADOR = "BORRADOR"
TIPOS_LLEGADA_TARDE = {
    IncidenciaAsistencia.TIPO_USO_TOLERANCIA,
    IncidenciaAsistencia.TIPO_RETARDO,
    IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
    IncidenciaAsistencia.TIPO_FALTA,
}


def _rango_periodo(periodo: ConfigBonoVentasPeriodo) -> tuple[date, date]:
    if periodo.fecha_inicio and periodo.fecha_fin:
        return periodo.fecha_inicio, periodo.fecha_fin
    ultimo_dia = calendar.monthrange(periodo.anio, periodo.mes)[1]
    return date(periodo.anio, periodo.mes, 1), date(periodo.anio, periodo.mes, ultimo_dia)


def _fechas(inicio: date, fin: date) -> list[date]:
    dias = (fin - inicio).days
    return [inicio + timedelta(days=offset) for offset in range(dias + 1)]


def _fecha_visible_en_periodo(periodo: ConfigBonoVentasPeriodo, fecha: date) -> bool:
    return fecha.month == periodo.mes and fecha.year == periodo.anio


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


def sincronizar_asistencia_desde_checador(periodo: ConfigBonoVentasPeriodo) -> dict:
    inicio, fin = _rango_periodo(periodo)
    dias = [fecha for fecha in _fechas(inicio, fin) if _fecha_visible_en_periodo(periodo, fecha)]
    estatus_borrador = getattr(BonoVentasEmpleado, "ESTATUS_BORRADOR", ESTATUS_BORRADOR)
    bonos_borrador = list(periodo.bonos.select_related("empleado").filter(estatus=estatus_borrador))
    bonos_omitidos = periodo.bonos.exclude(estatus=estatus_borrador).count()
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
                registro, created = RegistroDiarioVentas.objects.get_or_create(
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


def _periodos_para_fecha(fecha: date):
    return ConfigBonoVentasPeriodo.objects.filter(
        (
            (Q(fecha_inicio__isnull=True) | Q(fecha_fin__isnull=True))
            & Q(mes=fecha.month, anio=fecha.year)
        )
        | Q(fecha_inicio__lte=fecha, fecha_fin__gte=fecha, mes=fecha.month, anio=fecha.year)
    )


def sincronizar_empleado_dia_desde_checador(empleado_id: int, fecha: date) -> dict:
    resultado = {
        "bonos_sincronizados": 0,
        "bonos_omitidos": 0,
        "registros_creados": 0,
        "registros_actualizados": 0,
    }

    asistencias = _cargar_asistencias([empleado_id], fecha, fecha)
    incidencias = _cargar_incidencias([empleado_id], fecha, fecha)
    tiene_asistencia, tiene_puntualidad = _evaluar_dia(
        empleado_id,
        fecha,
        asistencias,
        incidencias,
    )
    estatus_borrador = getattr(BonoVentasEmpleado, "ESTATUS_BORRADOR", ESTATUS_BORRADOR)

    for periodo in _periodos_para_fecha(fecha):
        bono = (
            periodo.bonos.select_related("empleado")
            .filter(empleado_id=empleado_id)
            .first()
        )
        if bono is None:
            continue
        if bono.estatus != estatus_borrador:
            resultado["bonos_omitidos"] += 1
            continue

        with transaction.atomic():
            registro, created = RegistroDiarioVentas.objects.get_or_create(
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
