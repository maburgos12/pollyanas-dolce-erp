from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, timedelta

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from rrhh.models import AsistenciaEmpleado, IncidenciaAsistencia

from .models import BonoVentasEmpleado, ConfigBonoVentasPeriodo, RegistroDiarioVentas, fecha_registro_legacy
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


def _periodo_en_curso(periodo: ConfigBonoVentasPeriodo) -> bool:
    hoy = timezone.localdate()
    inicio, fin = _rango_periodo(periodo)
    return inicio <= hoy <= fin and periodo.mes == hoy.month and periodo.anio == hoy.year


def _fecha_futura_en_periodo_en_curso(periodo: ConfigBonoVentasPeriodo, fecha: date) -> bool:
    return _periodo_en_curso(periodo) and fecha > timezone.localdate()


def _debe_eliminar_registro(periodo: ConfigBonoVentasPeriodo, bono: BonoVentasEmpleado, fecha: date) -> bool:
    if bono.empleado.fecha_ingreso and fecha < bono.empleado.fecha_ingreso:
        return True
    return _fecha_futura_en_periodo_en_curso(periodo, fecha)


def _fecha_sincronizable(periodo: ConfigBonoVentasPeriodo, fecha: date) -> bool:
    if not _fecha_visible_en_periodo(periodo, fecha):
        return False
    return fecha <= timezone.localdate() and not _fecha_futura_en_periodo_en_curso(periodo, fecha)


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


def _registros_por_fecha(bono):
    registros = {}
    for registro in bono.registros.all():
        fecha = registro.fecha or fecha_registro_legacy(bono.periodo, registro.dia, bool(registro.capturado_por_id))
        if fecha is None or fecha in registros:
            return None
        registros[fecha] = registro
    return registros


def _sincronizar_dia(bono, fecha, registros, asistencias, incidencias, resultado):
    registro = registros.get(fecha)
    if _debe_eliminar_registro(bono.periodo, bono, fecha):
        if registro is not None and not registro.capturado_por_id:
            registro.delete()
            resultado["registros_eliminados"] += 1
        return
    if not _fecha_sincronizable(bono.periodo, fecha):
        return
    tiene_asistencia, tiene_puntualidad = _evaluar_dia(bono.empleado_id, fecha, asistencias, incidencias)
    if registro is None:
        registro, created = RegistroDiarioVentas.objects.get_or_create(
            bono=bono, fecha=fecha,
            defaults={"dia": fecha.day, "tiene_asistencia": tiene_asistencia, "tiene_puntualidad": tiene_puntualidad},
        )
        if created:
            resultado["registros_creados"] += 1
            return
    valores = {"fecha": fecha}
    if not registro.capturado_por_id:
        valores.update(tiene_asistencia=tiene_asistencia, tiene_puntualidad=tiene_puntualidad)
    campos = [campo for campo, valor in valores.items() if getattr(registro, campo) != valor]
    if campos:
        for campo in campos:
            setattr(registro, campo, valores[campo])
        registro.save(update_fields=campos)
        resultado["registros_actualizados"] += 1


def sincronizar_asistencia_desde_checador(periodo: ConfigBonoVentasPeriodo) -> dict:
    inicio, fin = _rango_periodo(periodo)
    dias = [fecha for fecha in _fechas(inicio, fin) if _fecha_visible_en_periodo(periodo, fecha)]
    bonos_borrador = list(periodo.bonos.select_related("empleado").filter(estatus=ESTATUS_BORRADOR))
    empleado_ids = [bono.empleado_id for bono in bonos_borrador]
    asistencias = _cargar_asistencias(empleado_ids, inicio, fin) if empleado_ids else set()
    incidencias = _cargar_incidencias(empleado_ids, inicio, fin) if empleado_ids else {}
    resultado = {
        "bonos_sincronizados": 0,
        "bonos_omitidos": periodo.bonos.exclude(estatus=ESTATUS_BORRADOR).count(),
        "registros_creados": 0, "registros_actualizados": 0, "registros_eliminados": 0,
    }
    for bono in bonos_borrador:
        with transaction.atomic():
            registros = _registros_por_fecha(bono)
            if registros is None:
                resultado["bonos_omitidos"] += 1
                continue
            for fecha in dias:
                _sincronizar_dia(bono, fecha, registros, asistencias, incidencias, resultado)
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
        "bonos_sincronizados": 0, "bonos_omitidos": 0,
        "registros_creados": 0, "registros_actualizados": 0, "registros_eliminados": 0,
    }
    asistencias = _cargar_asistencias([empleado_id], fecha, fecha)
    incidencias = _cargar_incidencias([empleado_id], fecha, fecha)
    for periodo in _periodos_para_fecha(fecha):
        bono = periodo.bonos.select_related("empleado").filter(empleado_id=empleado_id).first()
        if bono is None:
            continue
        if bono.estatus != ESTATUS_BORRADOR:
            resultado["bonos_omitidos"] += 1
            continue
        if not _debe_eliminar_registro(periodo, bono, fecha) and not _fecha_sincronizable(periodo, fecha):
            continue
        with transaction.atomic():
            registros = _registros_por_fecha(bono)
            if registros is None:
                resultado["bonos_omitidos"] += 1
                continue
            _sincronizar_dia(bono, fecha, registros, asistencias, incidencias, resultado)
            recalcular_desde_registros(bono)
            resultado["bonos_sincronizados"] += 1
    return resultado
