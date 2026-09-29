"""Clasificación diaria de asistencia reutilizable y sin efectos secundarios.

Este módulo proyecta las fuentes autoritativas de RRHH para consumidores como
bonos. No crea ni concilia incidencias: su contrato es estrictamente de lectura.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time

from django.db.models import Q
from django.utils import timezone

from .models import (
    AsignacionJornadaEmpleado,
    AsignacionTurnoEmpleado,
    AsistenciaEmpleado,
    Empleado,
    EmpleadoBaja,
    IncapacidadEmpleado,
    IncidenciaAsistencia,
    PermisoSalida,
    SolicitudVacaciones,
    SuspensionEmpleado,
)
from .services_vacaciones import es_descanso_oficial, es_dia_laborable


CODIGO_PREINGRESO = "preingreso"
CODIGO_POST_BAJA = "post_baja"
CODIGO_FESTIVO = "festivo"
CODIGO_DESCANSO = "descanso"
CODIGO_INCAPACIDAD = "incapacidad"
CODIGO_SUSPENSION = "suspension"
CODIGO_VACACIONES = "vacaciones"
CODIGO_PERMISO = "permiso"
CODIGO_EXENTO = "exento"
CODIGO_ASISTENCIA = "asistencia"
CODIGO_RETARDO = "retardo"
CODIGO_FALTA = "falta"

TIPOS_RETARDO = {
    IncidenciaAsistencia.TIPO_USO_TOLERANCIA,
    IncidenciaAsistencia.TIPO_RETARDO,
    IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
}


@dataclass(frozen=True)
class ContextoDiaEmpleado:
    codigo: str
    es_exigible: bool
    falta_penalizable: bool
    motivo: str
    fuente_modelo: str = ""
    fuente_id: int | None = None


def _resultado(
    codigo: str,
    *,
    exigible: bool,
    falta: bool,
    motivo: str,
    fuente=None,
) -> ContextoDiaEmpleado:
    return ContextoDiaEmpleado(
        codigo=codigo,
        es_exigible=exigible,
        falta_penalizable=falta,
        motivo=motivo,
        fuente_modelo=fuente._meta.label if fuente is not None else "",
        fuente_id=fuente.pk if fuente is not None else None,
    )


class ContextoAsistenciaLote:
    def __init__(
        self,
        *,
        fecha_inicio: date,
        fecha_fin: date,
        asistencias,
        incidencias,
        incapacidades,
        suspensiones,
        vacaciones,
        permisos,
        bajas,
        jornadas,
        turnos_legacy,
    ):
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin
        self.asistencias = asistencias
        self.incidencias = incidencias
        self.incapacidades = incapacidades
        self.suspensiones = suspensiones
        self.vacaciones = vacaciones
        self.permisos = permisos
        self.bajas = bajas
        self.jornadas = jornadas
        self.turnos_legacy = turnos_legacy

    @staticmethod
    def _vigente(registro, fecha: date) -> bool:
        return registro.fecha_inicio <= fecha <= registro.fecha_fin

    def _primero_vigente(self, registros, fecha: date):
        return next((registro for registro in registros if self._vigente(registro, fecha)), None)

    def _estado_jornada(self, empleado_id: int, fecha: date) -> str:
        asignaciones = [
            item for item in self.jornadas.get(empleado_id, ())
            if item.fecha_inicio <= fecha and (item.fecha_fin is None or item.fecha_fin >= fecha)
        ]
        if len(asignaciones) > 1:
            return "ambigua"
        if asignaciones:
            dias = {dia.dia_semana: dia for dia in asignaciones[0].jornada.dias.all()}
            detalle = dias.get(fecha.weekday())
            if detalle is not None:
                return "laborable" if detalle.turno_id else "descanso"

        legacy = [
            item for item in self.turnos_legacy.get(empleado_id, ())
            if item.fecha_inicio <= fecha and (item.fecha_fin is None or item.fecha_fin >= fecha)
        ]
        if legacy:
            return "laborable"
        return "laborable" if es_dia_laborable(fecha) else "descanso"

    def clasificar(self, empleado: Empleado, fecha: date) -> ContextoDiaEmpleado:
        if not self.fecha_inicio <= fecha <= self.fecha_fin:
            raise ValueError("La fecha está fuera del rango cargado.")
        empleado_id = empleado.pk
        if empleado.fecha_ingreso and fecha < empleado.fecha_ingreso:
            return _resultado(
                CODIGO_PREINGRESO,
                exigible=False,
                falta=False,
                motivo="Fecha anterior al ingreso del colaborador.",
            )

        baja = self.bajas.get(empleado_id)
        if not empleado.activo and (baja is None or fecha > baja.fecha_baja):
            return _resultado(
                CODIGO_POST_BAJA,
                exigible=False,
                falta=False,
                motivo="Fecha posterior a la baja del colaborador.",
                fuente=baja,
            )

        if es_descanso_oficial(fecha):
            return _resultado(
                CODIGO_FESTIVO,
                exigible=False,
                falta=False,
                motivo="Día de descanso obligatorio reconocido por RRHH.",
            )

        if self._estado_jornada(empleado_id, fecha) == "descanso":
            return _resultado(
                CODIGO_DESCANSO,
                exigible=False,
                falta=False,
                motivo="Descanso de la jornada asignada.",
            )

        incapacidad = self._primero_vigente(self.incapacidades.get(empleado_id, ()), fecha)
        if incapacidad:
            return _resultado(
                CODIGO_INCAPACIDAD,
                exigible=False,
                falta=False,
                motivo="Incapacidad vigente registrada en Capital Humano.",
                fuente=incapacidad,
            )

        suspension = self._primero_vigente(self.suspensiones.get(empleado_id, ()), fecha)
        if suspension:
            return _resultado(
                CODIGO_SUSPENSION,
                exigible=False,
                falta=False,
                motivo="Suspensión vigente registrada en Capital Humano.",
                fuente=suspension,
            )

        key = (empleado_id, fecha)
        incidencias_dia = self.incidencias.get(key, ())
        suspension_conciliada = next(
            (
                incidencia for incidencia in incidencias_dia
                if incidencia.tipo == IncidenciaAsistencia.TIPO_SUSPENSION
                and incidencia.estado == IncidenciaAsistencia.ESTADO_CONCILIADO
            ),
            None,
        )
        if suspension_conciliada:
            return _resultado(
                CODIGO_SUSPENSION,
                exigible=False,
                falta=False,
                motivo="Suspensión conciliada en Capital Humano.",
                fuente=suspension_conciliada,
            )

        vacaciones = self._primero_vigente(self.vacaciones.get(empleado_id, ()), fecha)
        if vacaciones:
            return _resultado(
                CODIGO_VACACIONES,
                exigible=False,
                falta=False,
                motivo="Vacaciones con reserva vigente en Capital Humano.",
                fuente=vacaciones,
            )

        permiso = next(
            (
                item for item in self.permisos.get(empleado_id, ())
                if timezone.localtime(item.fecha_inicio).date() <= fecha
                and timezone.localtime(item.fecha_fin or item.fecha_inicio).date() >= fecha
            ),
            None,
        )
        if permiso:
            return _resultado(
                CODIGO_PERMISO,
                exigible=False,
                falta=False,
                motivo="Permiso aprobado registrado en Capital Humano.",
                fuente=permiso,
            )

        asistencia = self.asistencias.get(key)
        incidencias = incidencias_dia
        tipos_pendientes = {
            incidencia.tipo
            for incidencia in incidencias
            if incidencia.estado == IncidenciaAsistencia.ESTADO_PENDIENTE
        }
        if IncidenciaAsistencia.TIPO_FALTA in tipos_pendientes:
            return _resultado(
                CODIGO_FALTA,
                exigible=True,
                falta=True,
                motivo="Falta pendiente sin conciliación en Capital Humano.",
                fuente=next(i for i in incidencias if i.tipo == IncidenciaAsistencia.TIPO_FALTA),
            )
        if asistencia:
            if tipos_pendientes & TIPOS_RETARDO:
                incidencia = next(i for i in incidencias if i.tipo in tipos_pendientes & TIPOS_RETARDO)
                return _resultado(
                    CODIGO_RETARDO,
                    exigible=True,
                    falta=False,
                    motivo="Asistencia con retardo pendiente en Capital Humano.",
                    fuente=incidencia,
                )
            return _resultado(
                CODIGO_ASISTENCIA,
                exigible=True,
                falta=False,
                motivo="Asistencia registrada en Capital Humano.",
                fuente=asistencia,
            )

        if empleado.exento_checador:
            return _resultado(
                CODIGO_EXENTO,
                exigible=False,
                falta=False,
                motivo=empleado.exento_checador_motivo or "Colaborador exento de checador.",
            )

        return _resultado(
            CODIGO_FALTA,
            exigible=True,
            falta=True,
            motivo="Sin asistencia ni justificación registrada en Capital Humano.",
        )


def _agrupar(registros) -> dict[int, list]:
    agrupados = defaultdict(list)
    for registro in registros:
        agrupados[registro.empleado_id].append(registro)
    return dict(agrupados)


def cargar_contexto_asistencia(
    *,
    empleados,
    fecha_inicio: date,
    fecha_fin: date,
) -> ContextoAsistenciaLote:
    """Carga una ventana completa con un número constante de consultas."""
    if fecha_fin < fecha_inicio:
        raise ValueError("La fecha final no puede ser anterior a la inicial.")
    empleados = list(empleados)
    empleado_ids = [empleado.pk for empleado in empleados]
    if not empleado_ids:
        return ContextoAsistenciaLote(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            asistencias={}, incidencias={}, incapacidades={}, suspensiones={},
            vacaciones={}, permisos={}, bajas={}, jornadas={}, turnos_legacy={},
        )

    asistencias = {
        (registro.empleado_id, registro.fecha): registro
        for registro in AsistenciaEmpleado.objects.filter(
            empleado_id__in=empleado_ids,
            fecha__range=(fecha_inicio, fecha_fin),
        )
    }
    incidencias = defaultdict(list)
    for registro in IncidenciaAsistencia.objects.filter(
        empleado_id__in=empleado_ids,
        fecha__range=(fecha_inicio, fecha_fin),
    ):
        incidencias[(registro.empleado_id, registro.fecha)].append(registro)

    incapacidades = _agrupar(IncapacidadEmpleado.objects.filter(
        empleado_id__in=empleado_ids,
        estado__in=[IncapacidadEmpleado.ESTADO_ACTIVA, IncapacidadEmpleado.ESTADO_CERRADA],
        fecha_inicio__lte=fecha_fin,
        fecha_fin__gte=fecha_inicio,
    ).order_by("fecha_inicio", "id"))
    suspensiones = _agrupar(SuspensionEmpleado.objects.filter(
        empleado_id__in=empleado_ids,
        estado=SuspensionEmpleado.ESTADO_ACTIVA,
        fecha_inicio__lte=fecha_fin,
        fecha_fin__gte=fecha_inicio,
    ).order_by("fecha_inicio", "id"))
    vacaciones = _agrupar(SolicitudVacaciones.objects.filter(
        empleado_id__in=empleado_ids,
        estado__in=[
            SolicitudVacaciones.ESTADO_SOLICITADA,
            SolicitudVacaciones.ESTADO_PREAUTORIZADA,
            SolicitudVacaciones.ESTADO_APROBADA,
        ],
        fecha_inicio__lte=fecha_fin,
        fecha_fin__gte=fecha_inicio,
    ).order_by("fecha_inicio", "id"))

    inicio_dt = timezone.make_aware(datetime.combine(fecha_inicio, time.min))
    fin_dt = timezone.make_aware(datetime.combine(fecha_fin, time.max))
    permisos = _agrupar(PermisoSalida.objects.filter(
        empleado_id__in=empleado_ids,
        estado=PermisoSalida.ESTADO_APROBADO,
        fecha_inicio__lte=fin_dt,
    ).filter(Q(fecha_fin__isnull=True, fecha_inicio__gte=inicio_dt) | Q(fecha_fin__gte=inicio_dt)))

    bajas = {}
    for baja in EmpleadoBaja.objects.filter(empleado_id__in=empleado_ids).order_by("empleado_id", "-fecha_baja", "-id"):
        bajas.setdefault(baja.empleado_id, baja)

    jornadas = _agrupar(AsignacionJornadaEmpleado.objects.filter(
        empleado_id__in=empleado_ids,
        fecha_inicio__lte=fecha_fin,
    ).filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=fecha_inicio)).select_related("jornada").prefetch_related("jornada__dias"))
    turnos_legacy = _agrupar(AsignacionTurnoEmpleado.objects.filter(
        empleado_id__in=empleado_ids,
        fecha_inicio__lte=fecha_fin,
    ).filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=fecha_inicio)))

    return ContextoAsistenciaLote(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        asistencias=asistencias,
        incidencias=dict(incidencias),
        incapacidades=incapacidades,
        suspensiones=suspensiones,
        vacaciones=vacaciones,
        permisos=permisos,
        bajas=bajas,
        jornadas=jornadas,
        turnos_legacy=turnos_legacy,
    )
