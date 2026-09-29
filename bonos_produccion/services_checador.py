from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, timedelta

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from rrhh.models import AsistenciaEmpleado, IncidenciaAsistencia
from rrhh.services_asistencia_contexto import (
    CODIGO_ASISTENCIA,
    CODIGO_RETARDO,
    cargar_contexto_asistencia,
)

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


def _fecha_visible_en_periodo(periodo: ConfigBonoPeriodo, fecha: date) -> bool:
    inicio, fin = _rango_periodo(periodo)
    return inicio <= fecha <= fin


def _periodo_en_curso(periodo: ConfigBonoPeriodo) -> bool:
    hoy = timezone.localdate()
    inicio, fin = _rango_periodo(periodo)
    return inicio <= hoy <= fin


def _fecha_futura_en_periodo_en_curso(periodo: ConfigBonoPeriodo, fecha: date) -> bool:
    return _periodo_en_curso(periodo) and fecha > timezone.localdate()


def _debe_eliminar_registro(periodo: ConfigBonoPeriodo, bono: BonoProduccionEmpleado, fecha: date) -> bool:
    if bono.empleado.fecha_ingreso and fecha < bono.empleado.fecha_ingreso:
        return True
    return _fecha_futura_en_periodo_en_curso(periodo, fecha)


def _fecha_sincronizable(periodo: ConfigBonoPeriodo, fecha: date) -> bool:
    if not _fecha_visible_en_periodo(periodo, fecha):
        return False
    return not _fecha_futura_en_periodo_en_curso(periodo, fecha)


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


def _fecha_registro_legacy(periodo: ConfigBonoPeriodo, dia: int) -> date | None:
    coincidencias = [fecha for fecha in _fechas(*_rango_periodo(periodo)) if fecha.day == dia]
    return coincidencias[0] if len(coincidencias) == 1 else None


def _valores_checador(resultado) -> tuple[bool, bool]:
    tiene_asistencia = resultado.codigo in {CODIGO_ASISTENCIA, CODIGO_RETARDO}
    tiene_puntualidad = resultado.codigo == CODIGO_ASISTENCIA
    return tiene_asistencia, tiene_puntualidad


def _sincronizar_bonos_fechas(
    *,
    periodo: ConfigBonoPeriodo,
    bonos: list[BonoProduccionEmpleado],
    fechas: list[date],
) -> dict:
    resultado_sync = {
        "bonos_sincronizados": 0,
        "bonos_omitidos": 0,
        "registros_creados": 0,
        "registros_actualizados": 0,
        "registros_eliminados": 0,
    }
    if not bonos:
        return resultado_sync

    inicio, fin = _rango_periodo(periodo)
    contexto = cargar_contexto_asistencia(
        empleados=[bono.empleado for bono in bonos],
        fecha_inicio=inicio,
        fecha_fin=fin,
    )
    existentes = list(
        RegistroDiarioProduccion.objects.filter(bono__in=bonos).select_related("bono__periodo")
    )
    por_clave = {}
    for registro in existentes:
        fecha_registro = registro.fecha or _fecha_registro_legacy(registro.bono.periodo, registro.dia)
        if fecha_registro is not None:
            por_clave[(registro.bono_id, fecha_registro)] = registro

    por_crear = []
    por_actualizar = []
    por_eliminar = []
    campos_contexto = ["fecha", "estado_rrhh", "motivo_rrhh", "falta_penalizable"]
    campos_automaticos = campos_contexto + ["tiene_asistencia", "tiene_puntualidad"]

    for bono in bonos:
        for fecha in fechas:
            registro = por_clave.get((bono.id, fecha))
            if _debe_eliminar_registro(periodo, bono, fecha):
                if registro is not None:
                    por_eliminar.append(registro.pk)
                continue
            if not _fecha_sincronizable(periodo, fecha):
                continue

            dia = contexto.clasificar(bono.empleado, fecha)
            tiene_asistencia, tiene_puntualidad = _valores_checador(dia)
            if registro is None:
                por_crear.append(RegistroDiarioProduccion(
                    bono=bono,
                    dia=fecha.day,
                    fecha=fecha,
                    estado_rrhh=dia.codigo,
                    motivo_rrhh=dia.motivo,
                    falta_penalizable=dia.falta_penalizable,
                    tiene_asistencia=tiene_asistencia,
                    tiene_puntualidad=tiene_puntualidad,
                ))
                continue

            valores = {
                "fecha": fecha,
                "estado_rrhh": dia.codigo,
                "motivo_rrhh": dia.motivo,
                "falta_penalizable": dia.falta_penalizable,
            }
            campos = campos_contexto
            if registro.capturado_por_id:
                valores["falta_penalizable"] = bool(dia.es_exigible and not registro.tiene_asistencia)
            else:
                valores.update({
                    "tiene_asistencia": tiene_asistencia,
                    "tiene_puntualidad": tiene_puntualidad,
                })
                campos = campos_automaticos
            if any(getattr(registro, campo) != valor for campo, valor in valores.items()):
                for campo, valor in valores.items():
                    setattr(registro, campo, valor)
                registro._sync_campos = campos
                por_actualizar.append(registro)

    with transaction.atomic():
        if por_eliminar:
            RegistroDiarioProduccion.objects.filter(pk__in=por_eliminar).delete()
        if por_crear:
            RegistroDiarioProduccion.objects.bulk_create(por_crear)
        if por_actualizar:
            # La lista de campos es el superset; los registros manuales conservan
            # sus booleanos porque nunca se cambian en memoria.
            RegistroDiarioProduccion.objects.bulk_update(por_actualizar, campos_automaticos)
        for bono in bonos:
            recalcular_desde_registros(bono)

    resultado_sync.update({
        "bonos_sincronizados": len(bonos),
        "registros_creados": len(por_crear),
        "registros_actualizados": len(por_actualizar),
        "registros_eliminados": len(por_eliminar),
    })
    return resultado_sync


def sincronizar_asistencia_desde_checador(periodo: ConfigBonoPeriodo) -> dict:
    inicio, fin = _rango_periodo(periodo)
    dias = [fecha for fecha in _fechas(inicio, fin) if _fecha_visible_en_periodo(periodo, fecha)]
    bonos_borrador = list(
        periodo.bonos.select_related("empleado").filter(estatus=BonoProduccionEmpleado.ESTATUS_BORRADOR)
    )
    bonos_omitidos = periodo.bonos.exclude(estatus=BonoProduccionEmpleado.ESTATUS_BORRADOR).count()
    resultado = _sincronizar_bonos_fechas(periodo=periodo, bonos=bonos_borrador, fechas=dias)
    resultado["bonos_omitidos"] = bonos_omitidos
    return resultado


def _periodos_para_fecha(fecha: date):
    return ConfigBonoPeriodo.objects.filter(
        (
            (Q(fecha_inicio__isnull=True) | Q(fecha_fin__isnull=True))
            & Q(mes=fecha.month, anio=fecha.year)
        )
        | Q(fecha_inicio__lte=fecha, fecha_fin__gte=fecha)
    )


def sincronizar_empleado_dia_desde_checador(empleado_id: int, fecha: date) -> dict:
    resultado = {
        "bonos_sincronizados": 0,
        "bonos_omitidos": 0,
        "registros_creados": 0,
        "registros_actualizados": 0,
        "registros_eliminados": 0,
    }

    for periodo in _periodos_para_fecha(fecha):
        bono = (
            periodo.bonos.select_related("empleado")
            .filter(empleado_id=empleado_id)
            .first()
        )
        if bono is None:
            continue
        if bono.estatus != BonoProduccionEmpleado.ESTATUS_BORRADOR:
            resultado["bonos_omitidos"] += 1
            continue

        parcial = _sincronizar_bonos_fechas(periodo=periodo, bonos=[bono], fechas=[fecha])
        for key in resultado:
            resultado[key] += parcial[key]

    return resultado
