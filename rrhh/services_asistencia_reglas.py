from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .models import (
    AsistenciaEmpleado,
    Empleado,
    HoraExtra,
    IncidenciaAsistencia,
    PermisoSalida,
    SolicitudVacaciones,
    SuspensionEmpleado,
    Turno,
)
from .services import TIEMPO_COMIDA_MINUTOS, calcular_horas_extra, generar_horas_extra_automatico, minutos_jornada_programada
from .services_vacaciones import es_dia_laborable


VENTANA_RETARDOS_DIAS = 15
VENTANA_FALTAS_DIAS = 30
RETARDOS_POR_FALTA = 3
FALTAS_AVISO_BAJA = 3
FALTAS_BAJA = 4
MARCAS_TOLERANCIA_POR_RETARDO = 3


@dataclass(frozen=True)
class ResultadoEvaluacionAsistencia:
    evaluados: int = 0
    creados: int = 0
    actualizados: int = 0
    resueltos: int = 0

    def sumar(self, other: "ResultadoEvaluacionAsistencia") -> "ResultadoEvaluacionAsistencia":
        return ResultadoEvaluacionAsistencia(
            evaluados=self.evaluados + other.evaluados,
            creados=self.creados + other.creados,
            actualizados=self.actualizados + other.actualizados,
            resueltos=self.resueltos + other.resueltos,
        )


def _dia_bounds(fecha: date) -> tuple[datetime, datetime]:
    tz = timezone.get_current_timezone()
    inicio = timezone.make_aware(datetime.combine(fecha, time.min), tz)
    fin = timezone.make_aware(datetime.combine(fecha, time.max), tz)
    return inicio, fin


def _aware_datetime(fecha: date, hora: time) -> datetime:
    tz = timezone.get_current_timezone()
    return timezone.make_aware(datetime.combine(fecha, hora), tz)


def _minutos_entre(inicio: datetime | None, fin: datetime | None) -> int:
    if not inicio or not fin:
        return 0
    return max(int((fin - inicio).total_seconds() // 60), 0)


def _permiso_aprobado_en_rango(
    empleado: Empleado,
    fecha: date,
    *,
    inicio: datetime | None = None,
    fin: datetime | None = None,
) -> PermisoSalida | None:
    dia_inicio, dia_fin = _dia_bounds(fecha)
    inicio = inicio or dia_inicio
    fin = fin or dia_fin
    return (
        PermisoSalida.objects.filter(
            empleado=empleado,
            estado=PermisoSalida.ESTADO_APROBADO,
            fecha_inicio__lte=fin,
        )
        .filter(Q(fecha_fin__isnull=True, fecha_inicio__gte=dia_inicio, fecha_inicio__lte=dia_fin) | Q(fecha_fin__gte=inicio))
        .order_by("fecha_inicio")
        .first()
    )


def _vacaciones_aprobadas_en_fecha(empleado: Empleado, fecha: date) -> SolicitudVacaciones | None:
    return (
        SolicitudVacaciones.objects.filter(
            empleado=empleado,
            estado=SolicitudVacaciones.ESTADO_APROBADA,
            fecha_inicio__lte=fecha,
            fecha_fin__gte=fecha,
        )
        .order_by("fecha_inicio")
        .first()
    )


def _suspension_activa_en_fecha(empleado: Empleado, fecha: date) -> SuspensionEmpleado | None:
    return (
        SuspensionEmpleado.objects.filter(
            empleado=empleado,
            estado=SuspensionEmpleado.ESTADO_ACTIVA,
            fecha_inicio__lte=fecha,
            fecha_fin__gte=fecha,
        )
        .order_by("fecha_inicio")
        .first()
    )


def _upsert_incidencia(
    *,
    empleado: Empleado,
    fecha: date,
    tipo: str,
    estado: str,
    severidad: str,
    asistencia: AsistenciaEmpleado | None = None,
    permiso: PermisoSalida | None = None,
    solicitud_vacaciones: SolicitudVacaciones | None = None,
    hora_extra: HoraExtra | None = None,
    minutos: int = 0,
    goce_sueldo: bool | None = None,
    ventana_inicio: date | None = None,
    ventana_fin: date | None = None,
    conteo_retardos_15d: int = 0,
    conteo_faltas_30d: int = 0,
    detalle: str = "",
    metadata: dict | None = None,
) -> tuple[IncidenciaAsistencia, bool, bool]:
    defaults = {
        "estado": estado,
        "severidad": severidad,
        "asistencia": asistencia,
        "permiso": permiso,
        "solicitud_vacaciones": solicitud_vacaciones,
        "hora_extra": hora_extra,
        "minutos": minutos,
        "goce_sueldo": goce_sueldo,
        "ventana_inicio": ventana_inicio,
        "ventana_fin": ventana_fin,
        "conteo_retardos_15d": conteo_retardos_15d,
        "conteo_faltas_30d": conteo_faltas_30d,
        "detalle": detalle,
        "metadata": metadata or {},
    }
    incidencia, creada = IncidenciaAsistencia.objects.get_or_create(
        empleado=empleado,
        fecha=fecha,
        tipo=tipo,
        defaults=defaults,
    )
    if creada:
        return incidencia, True, False
    if incidencia.editado_manual:
        return incidencia, False, False

    for field, value in defaults.items():
        setattr(incidencia, field, value)
    incidencia.save(update_fields=[*defaults.keys(), "actualizado_en"])
    return incidencia, creada, not creada


def _retardos_vigentes(empleado: Empleado, fecha: date) -> int:
    desde = fecha - timedelta(days=VENTANA_RETARDOS_DIAS - 1)
    return IncidenciaAsistencia.objects.filter(
        empleado=empleado,
        fecha__gte=desde,
        fecha__lte=fecha,
        tipo__in=[
            IncidenciaAsistencia.TIPO_RETARDO,
            IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
        ],
        estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
    ).count()


def _faltas_vigentes(empleado: Empleado, fecha: date) -> int:
    desde = fecha - timedelta(days=VENTANA_FALTAS_DIAS - 1)
    return IncidenciaAsistencia.objects.filter(
        empleado=empleado,
        fecha__gte=desde,
        fecha__lte=fecha,
        tipo=IncidenciaAsistencia.TIPO_FALTA,
        estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
    ).count()


def _faltas_retardos_existentes(empleado: Empleado, fecha: date) -> int:
    desde = fecha - timedelta(days=VENTANA_RETARDOS_DIAS - 1)
    return IncidenciaAsistencia.objects.filter(
        empleado=empleado,
        fecha__gte=desde,
        fecha__lte=fecha,
        tipo=IncidenciaAsistencia.TIPO_FALTA_RETARDOS,
        estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
    ).count()


def _evaluar_entrada(asistencia: AsistenciaEmpleado, touched: set[str]) -> tuple[int, int]:
    creados = 0
    actualizados = 0
    if not asistencia.turno or not asistencia.entrada:
        return creados, actualizados

    empleado = asistencia.empleado
    fecha = asistencia.fecha
    entrada_local = timezone.localtime(asistencia.entrada)
    hora_programada = _aware_datetime(fecha, asistencia.turno.hora_entrada)
    minutos_tarde = _minutos_entre(hora_programada, entrada_local)
    if minutos_tarde <= 0:
        return creados, actualizados

    if minutos_tarde <= int(asistencia.turno.tolerancia_minutos or 0):
        permiso = _permiso_aprobado_en_rango(empleado, fecha, inicio=hora_programada, fin=entrada_local)
        tipo = IncidenciaAsistencia.TIPO_USO_TOLERANCIA
        touched.add(tipo)
        _, creada, actualizada = _upsert_incidencia(
            empleado=empleado,
            fecha=fecha,
            tipo=tipo,
            estado=IncidenciaAsistencia.ESTADO_CONCILIADO if permiso else IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_INFO,
            asistencia=asistencia,
            permiso=permiso,
            minutos=minutos_tarde,
            goce_sueldo=getattr(permiso, "goce_sueldo", None),
            detalle="Entrada dentro de tolerancia." if not permiso else "Entrada dentro de tolerancia conciliada con permiso.",
            metadata={"minutos_tarde": minutos_tarde},
        )
        creados += int(creada)
        actualizados += int(actualizada)

        desde = fecha - timedelta(days=VENTANA_RETARDOS_DIAS - 1)
        usos_tolerancia = IncidenciaAsistencia.objects.filter(
            empleado=empleado,
            fecha__gte=desde,
            fecha__lte=fecha,
            tipo=IncidenciaAsistencia.TIPO_USO_TOLERANCIA,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
        ).count()
        if usos_tolerancia >= MARCAS_TOLERANCIA_POR_RETARDO:
            tipo = IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA
            touched.add(tipo)
            _, creada, actualizada = _upsert_incidencia(
                empleado=empleado,
                fecha=fecha,
                tipo=tipo,
                estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
                severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA,
                asistencia=asistencia,
                minutos=minutos_tarde,
                ventana_inicio=desde,
                ventana_fin=fecha,
                detalle="Uso recurrente de tolerancia en ventana de 15 dias.",
                metadata={"usos_tolerancia_15d": usos_tolerancia},
            )
            creados += int(creada)
            actualizados += int(actualizada)
        return creados, actualizados

    permiso = _permiso_aprobado_en_rango(empleado, fecha, inicio=hora_programada, fin=entrada_local)
    if not permiso:
        tipo = IncidenciaAsistencia.TIPO_FALTA
        touched.add(tipo)
        _, creada, actualizada = _upsert_incidencia(
            empleado=empleado,
            fecha=fecha,
            tipo=tipo,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            asistencia=asistencia,
            minutos=minutos_tarde,
            detalle="Entrada posterior a tolerancia sin permiso; se considera falta.",
            metadata={"minutos_tarde": minutos_tarde, "tolerancia_minutos": asistencia.turno.tolerancia_minutos},
        )
        creados += int(creada)
        actualizados += int(actualizada)
        return creados, actualizados

    tipo = IncidenciaAsistencia.TIPO_RETARDO
    touched.add(tipo)
    _, creada, actualizada = _upsert_incidencia(
        empleado=empleado,
        fecha=fecha,
        tipo=tipo,
        estado=IncidenciaAsistencia.ESTADO_CONCILIADO,
        severidad=IncidenciaAsistencia.SEVERIDAD_INFO,
        asistencia=asistencia,
        permiso=permiso,
        minutos=minutos_tarde,
        goce_sueldo=getattr(permiso, "goce_sueldo", None),
        detalle="Entrada posterior a tolerancia conciliada con permiso de ingreso tarde.",
        metadata={"minutos_tarde": minutos_tarde, "tolerancia_minutos": asistencia.turno.tolerancia_minutos},
    )
    creados += int(creada)
    actualizados += int(actualizada)
    return creados, actualizados


def _evaluar_jornada(asistencia: AsistenciaEmpleado, touched: set[str]) -> tuple[int, int]:
    creados = 0
    actualizados = 0
    if not asistencia.turno or not asistencia.entrada:
        return creados, actualizados

    fin_turno = _aware_datetime(asistencia.fecha, asistencia.turno.hora_salida)
    inicio_turno = _aware_datetime(asistencia.fecha, asistencia.turno.hora_entrada)
    if fin_turno <= inicio_turno:
        fin_turno += timedelta(days=1)
    minutos_jornada = minutos_jornada_programada(asistencia)
    minutos_trabajados = int(asistencia.minutos_trabajados or 0)
    faltante = max(minutos_jornada - minutos_trabajados, 0)
    if faltante <= 0:
        return creados, actualizados

    inicio_ausencia = timezone.localtime(asistencia.salida) if asistencia.salida else timezone.localtime(asistencia.entrada)
    permiso = _permiso_aprobado_en_rango(asistencia.empleado, asistencia.fecha, inicio=inicio_ausencia, fin=fin_turno)
    tipo = IncidenciaAsistencia.TIPO_JORNADA_INCOMPLETA
    touched.add(tipo)
    _, creada, actualizada = _upsert_incidencia(
        empleado=asistencia.empleado,
        fecha=asistencia.fecha,
        tipo=tipo,
        estado=IncidenciaAsistencia.ESTADO_CONCILIADO if permiso else IncidenciaAsistencia.ESTADO_PENDIENTE,
        severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA,
        asistencia=asistencia,
        permiso=permiso,
        minutos=faltante,
        goce_sueldo=getattr(permiso, "goce_sueldo", None),
        detalle="Jornada incompleta conciliada con permiso." if permiso else "Jornada incompleta sin permiso asociado.",
        metadata={"minutos_jornada": minutos_jornada, "minutos_trabajados": minutos_trabajados},
    )
    creados += int(creada)
    actualizados += int(actualizada)
    return creados, actualizados


def _evaluar_comida(asistencia: AsistenciaEmpleado, touched: set[str]) -> tuple[int, int]:
    creados = 0
    actualizados = 0
    if not asistencia.salida_comida or not asistencia.regreso_comida:
        return creados, actualizados

    minutos_comida = int(asistencia.minutos_comida or 0)
    if minutos_comida <= TIEMPO_COMIDA_MINUTOS:
        return creados, actualizados

    exceso = minutos_comida - TIEMPO_COMIDA_MINUTOS
    permiso = _permiso_aprobado_en_rango(
        asistencia.empleado,
        asistencia.fecha,
        inicio=asistencia.salida_comida,
        fin=asistencia.regreso_comida,
    )
    tipo = IncidenciaAsistencia.TIPO_COMIDA_EXCEDIDA
    touched.add(tipo)
    _, creada, actualizada = _upsert_incidencia(
        empleado=asistencia.empleado,
        fecha=asistencia.fecha,
        tipo=tipo,
        estado=IncidenciaAsistencia.ESTADO_CONCILIADO if permiso else IncidenciaAsistencia.ESTADO_PENDIENTE,
        severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA,
        asistencia=asistencia,
        permiso=permiso,
        minutos=exceso,
        goce_sueldo=getattr(permiso, "goce_sueldo", None),
        detalle="Comida excedida conciliada con permiso." if permiso else "Comida excedida sin permiso asociado.",
        metadata={"minutos_comida": minutos_comida, "exceso": exceso},
    )
    creados += int(creada)
    actualizados += int(actualizada)
    return creados, actualizados


def _evaluar_hora_extra(asistencia: AsistenciaEmpleado, touched: set[str]) -> tuple[int, int]:
    creados = 0
    actualizados = 0
    if not asistencia.turno or not asistencia.entrada or not asistencia.salida:
        return creados, actualizados
    if calcular_horas_extra(asistencia) <= Decimal("0"):
        return creados, actualizados
    hora_extra = getattr(asistencia, "hora_extra", None) or generar_horas_extra_automatico(asistencia)
    if not hora_extra:
        return creados, actualizados

    tipo = IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE
    touched.add(tipo)
    estado = (
        IncidenciaAsistencia.ESTADO_CONCILIADO
        if hora_extra.estado == HoraExtra.ESTADO_AUTORIZADO
        else IncidenciaAsistencia.ESTADO_PENDIENTE
    )
    _, creada, actualizada = _upsert_incidencia(
        empleado=asistencia.empleado,
        fecha=asistencia.fecha,
        tipo=tipo,
        estado=estado,
        severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA if estado == IncidenciaAsistencia.ESTADO_PENDIENTE else IncidenciaAsistencia.SEVERIDAD_INFO,
        asistencia=asistencia,
        hora_extra=hora_extra,
        minutos=int(Decimal(str(hora_extra.horas or "0")) * Decimal("60")),
        detalle="Hora extra autorizada por jefe directo." if estado == IncidenciaAsistencia.ESTADO_CONCILIADO else "Hora extra detectada por checador pendiente de autorizacion.",
        metadata={"horas": str(hora_extra.horas), "estado_hora_extra": hora_extra.estado},
    )
    creados += int(creada)
    actualizados += int(actualizada)
    return creados, actualizados


def _evaluar_falta_sin_registro(empleado: Empleado, fecha: date, touched: set[str]) -> tuple[int, int]:
    vacaciones = _vacaciones_aprobadas_en_fecha(empleado, fecha)
    permiso = None if vacaciones else _permiso_aprobado_en_rango(empleado, fecha)
    estado = IncidenciaAsistencia.ESTADO_CONCILIADO if vacaciones or permiso else IncidenciaAsistencia.ESTADO_PENDIENTE
    detalle = "Falta de registro sin conciliacion."
    if vacaciones:
        detalle = "Falta de registro conciliada con vacaciones aprobadas."
    elif permiso:
        detalle = "Falta de registro conciliada con permiso aprobado."

    tipo = IncidenciaAsistencia.TIPO_FALTA
    touched.add(tipo)
    _, creada, actualizada = _upsert_incidencia(
        empleado=empleado,
        fecha=fecha,
        tipo=tipo,
        estado=estado,
        severidad=IncidenciaAsistencia.SEVERIDAD_ALTA if estado == IncidenciaAsistencia.ESTADO_PENDIENTE else IncidenciaAsistencia.SEVERIDAD_INFO,
        permiso=permiso,
        solicitud_vacaciones=vacaciones,
        goce_sueldo=getattr(permiso, "goce_sueldo", None),
        detalle=detalle,
        metadata={
            "conciliacion": "vacaciones" if vacaciones else ("permiso" if permiso else "sin_conciliacion"),
        },
    )
    return int(creada), int(actualizada)


def _evaluar_suspension(
    empleado: Empleado,
    fecha: date,
    suspension: SuspensionEmpleado,
    touched: set[str],
) -> tuple[int, int]:
    tipo = IncidenciaAsistencia.TIPO_SUSPENSION
    touched.add(tipo)
    _, creada, actualizada = _upsert_incidencia(
        empleado=empleado,
        fecha=fecha,
        tipo=tipo,
        estado=IncidenciaAsistencia.ESTADO_CONCILIADO,
        severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA,
        minutos=0,
        goce_sueldo=suspension.con_goce,
        detalle="Día de suspensión disciplinaria.",
        metadata={"suspension_id": suspension.id, "con_goce": bool(suspension.con_goce)},
    )
    return int(creada), int(actualizada)


def _evaluar_escalamientos(empleado: Empleado, fecha: date, touched: set[str]) -> tuple[int, int]:
    creados = 0
    actualizados = 0
    desde_retardos = fecha - timedelta(days=VENTANA_RETARDOS_DIAS - 1)
    retardos = _retardos_vigentes(empleado, fecha)
    faltas_retardos_esperadas = retardos // RETARDOS_POR_FALTA
    faltas_retardos_actuales = _faltas_retardos_existentes(empleado, fecha)
    if faltas_retardos_esperadas > faltas_retardos_actuales:
        tipo = IncidenciaAsistencia.TIPO_FALTA_RETARDOS
        touched.add(tipo)
        _, creada, actualizada = _upsert_incidencia(
            empleado=empleado,
            fecha=fecha,
            tipo=tipo,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            ventana_inicio=desde_retardos,
            ventana_fin=fecha,
            conteo_retardos_15d=retardos,
            detalle="Tres retardos en ventana de 15 dias equivalen a una falta.",
            metadata={"retardos_15d": retardos, "faltas_retardos_esperadas": faltas_retardos_esperadas},
        )
        creados += int(creada)
        actualizados += int(actualizada)

    desde_faltas = fecha - timedelta(days=VENTANA_FALTAS_DIAS - 1)
    faltas = _faltas_vigentes(empleado, fecha)
    if faltas >= FALTAS_AVISO_BAJA:
        tipo = IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS
        touched.add(tipo)
        _, creada, actualizada = _upsert_incidencia(
            empleado=empleado,
            fecha=fecha,
            tipo=tipo,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_ALTA,
            ventana_inicio=desde_faltas,
            ventana_fin=fecha,
            conteo_faltas_30d=faltas,
            detalle="Tres faltas en 30 dias: aviso de riesgo de baja por faltas.",
            metadata={"faltas_30d": faltas},
        )
        creados += int(creada)
        actualizados += int(actualizada)

    if faltas >= FALTAS_BAJA:
        tipo = IncidenciaAsistencia.TIPO_BAJA_FALTAS
        touched.add(tipo)
        _, creada, actualizada = _upsert_incidencia(
            empleado=empleado,
            fecha=fecha,
            tipo=tipo,
            estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
            severidad=IncidenciaAsistencia.SEVERIDAD_CRITICA,
            ventana_inicio=desde_faltas,
            ventana_fin=fecha,
            conteo_faltas_30d=faltas,
            detalle="Cuarta falta en 30 dias: baja por faltas conforme a politica interna.",
            metadata={"faltas_30d": faltas},
        )
        creados += int(creada)
        actualizados += int(actualizada)

    return creados, actualizados


def _resolver_incidencias_stale(empleado: Empleado, fecha: date, touched: set[str]) -> int:
    stale = IncidenciaAsistencia.objects.filter(
        empleado=empleado,
        fecha=fecha,
        editado_manual=False,
    ).exclude(tipo__in=touched)
    return stale.exclude(estado=IncidenciaAsistencia.ESTADO_RESUELTO).update(
        estado=IncidenciaAsistencia.ESTADO_RESUELTO,
        severidad=IncidenciaAsistencia.SEVERIDAD_INFO,
        detalle="Incidencia resuelta por reevaluacion automatica.",
    )


@transaction.atomic
def evaluar_dia_empleado(empleado: Empleado, fecha: date) -> ResultadoEvaluacionAsistencia:
    touched: set[str] = set()
    creados = 0
    actualizados = 0
    resueltos = 0

    asistencia = (
        AsistenciaEmpleado.objects.select_related("empleado", "turno")
        .filter(empleado=empleado, fecha=fecha)
        .first()
    )
    if not asistencia:
        if not es_dia_laborable(fecha):
            return ResultadoEvaluacionAsistencia(evaluados=1)

    suspension = _suspension_activa_en_fecha(empleado, fecha)
    if suspension:
        creados_suspension, actualizados_suspension = _evaluar_suspension(empleado, fecha, suspension, touched)
        creados += creados_suspension
        actualizados += actualizados_suspension
        resueltos = _resolver_incidencias_stale(empleado, fecha, touched)
        return ResultadoEvaluacionAsistencia(
            evaluados=1,
            creados=creados,
            actualizados=actualizados,
            resueltos=resueltos,
        )

    if not asistencia:
        creados_falta, actualizados_falta = _evaluar_falta_sin_registro(empleado, fecha, touched)
        creados += creados_falta
        actualizados += actualizados_falta
    else:
        creados_entrada, actualizados_entrada = _evaluar_entrada(asistencia, touched)
        creados += creados_entrada
        actualizados += actualizados_entrada
        creados_jornada, actualizados_jornada = _evaluar_jornada(asistencia, touched)
        creados += creados_jornada
        actualizados += actualizados_jornada
        creados_comida, actualizados_comida = _evaluar_comida(asistencia, touched)
        creados += creados_comida
        actualizados += actualizados_comida
        creados_he, actualizados_he = _evaluar_hora_extra(asistencia, touched)
        creados += creados_he
        actualizados += actualizados_he

    creados_escalamiento, actualizados_escalamiento = _evaluar_escalamientos(empleado, fecha, touched)
    creados += creados_escalamiento
    actualizados += actualizados_escalamiento

    resueltos = _resolver_incidencias_stale(empleado, fecha, touched)

    return ResultadoEvaluacionAsistencia(evaluados=1, creados=creados, actualizados=actualizados, resueltos=resueltos)


def evaluar_rango_asistencia(
    fecha_inicio: date,
    fecha_fin: date,
    *,
    incluir_sin_asistencia: bool = True,
    empleados: list[Empleado] | None = None,
) -> ResultadoEvaluacionAsistencia:
    if fecha_fin < fecha_inicio:
        raise ValueError("La fecha final no puede ser anterior a la inicial.")

    if empleados is None:
        qs = Empleado.objects.filter(activo=True).order_by("nombre", "id")
        if not incluir_sin_asistencia:
            empleados_con_asistencia = AsistenciaEmpleado.objects.filter(
                fecha__gte=fecha_inicio,
                fecha__lte=fecha_fin,
            ).values_list("empleado_id", flat=True)
            qs = qs.filter(id__in=empleados_con_asistencia)
        empleados = list(qs)

    resultado = ResultadoEvaluacionAsistencia()
    cursor = fecha_inicio
    while cursor <= fecha_fin:
        for empleado in empleados:
            resultado = resultado.sumar(evaluar_dia_empleado(empleado, cursor))
        cursor += timedelta(days=1)
    return resultado
