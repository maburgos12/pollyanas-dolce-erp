from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from django.db import transaction

from rrhh.models import (
    AjusteAsistencia,
    AsistenciaEmpleado,
    Empleado,
    HoraExtra,
    IncidenciaAsistencia,
    PrenominaCorte,
    PrenominaEmpleadoResumen,
    PrenominaMovimiento,
)


TIPOS_RETARDO = {
    IncidenciaAsistencia.TIPO_RETARDO,
    IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA,
    IncidenciaAsistencia.TIPO_USO_TOLERANCIA,
}
TIPOS_ALERTA_OPERATIVA = {
    IncidenciaAsistencia.TIPO_JORNADA_INCOMPLETA,
    IncidenciaAsistencia.TIPO_COMIDA_EXCEDIDA,
    IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE,
    IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS,
    IncidenciaAsistencia.TIPO_BAJA_FALTAS,
}


def _fechas(inicio: date, fin: date) -> list[date]:
    return [inicio + timedelta(days=offset) for offset in range((fin - inicio).days + 1)]


def _empleados_del_periodo(inicio: date, fin: date, sucursal: str = "", area: str = ""):
    qs = Empleado.objects.filter(activo=True, fecha_ingreso__lte=fin)
    if sucursal:
        qs = qs.filter(sucursal__icontains=sucursal)
    if area:
        qs = qs.filter(area__icontains=area)
    return list(qs.order_by("nombre", "codigo", "id"))


@transaction.atomic
def crear_corte_prenomina(
    *,
    fecha_inicio,
    fecha_fin,
    fecha_corte,
    creado_por,
    tipo_periodo=PrenominaCorte.TIPO_QUINCENAL,
    sucursal="",
    area="",
    notas="",
):
    corte = PrenominaCorte.objects.create(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        fecha_corte=fecha_corte,
        tipo_periodo=tipo_periodo,
        sucursal=sucursal,
        area=area,
        notas=notas,
        creado_por=creado_por,
    )
    return recalcular_corte_prenomina(corte)


@transaction.atomic
def recalcular_corte_prenomina(corte: PrenominaCorte) -> PrenominaCorte:
    corte = PrenominaCorte.objects.select_for_update().get(pk=corte.pk)
    corte.resumenes.all().delete()
    corte.movimientos.all().delete()

    fechas = _fechas(corte.fecha_inicio, corte.fecha_fin)
    empleados = _empleados_del_periodo(corte.fecha_inicio, corte.fecha_fin, corte.sucursal, corte.area)
    empleado_ids = [empleado.id for empleado in empleados]

    incidencias_por_empleado = _incidencias_por_empleado(corte, empleado_ids)
    asistencias_por_empleado = _asistencias_por_empleado(corte, empleado_ids)
    ajustes_pendientes_por_empleado = _ajustes_pendientes_por_empleado(corte, empleado_ids)
    horas_extra_por_empleado = _horas_extra_por_empleado(corte, empleado_ids)

    for empleado in empleados:
        resumen_data = _calcular_resumen_empleado(
            corte=corte,
            empleado=empleado,
            fechas=fechas,
            incidencias=incidencias_por_empleado.get(empleado.id, []),
            asistencias=asistencias_por_empleado.get(empleado.id, set()),
            ajustes_pendientes=ajustes_pendientes_por_empleado.get(empleado.id, 0),
            horas_extra=horas_extra_por_empleado.get(empleado.id, []),
        )
        PrenominaEmpleadoResumen.objects.create(corte=corte, empleado=empleado, **resumen_data)

    corte.resumen = _resumen_corte(corte)
    corte.estado = (
        PrenominaCorte.ESTADO_LISTO
        if corte.resumen.get("bloqueados", 0) == 0 and corte.resumen.get("ajustes_pendientes", 0) == 0
        else PrenominaCorte.ESTADO_EN_REVISION
    )
    corte.save(update_fields=["resumen", "estado", "actualizado_en"])
    return corte


def _incidencias_por_empleado(corte: PrenominaCorte, empleado_ids: list[int]):
    incidencias = (
        IncidenciaAsistencia.objects.filter(
            empleado_id__in=empleado_ids,
            fecha__range=(corte.fecha_inicio, corte.fecha_fin),
        )
        .exclude(estado=IncidenciaAsistencia.ESTADO_RESUELTO)
        .order_by("empleado_id", "fecha", "tipo", "id")
    )
    grouped = defaultdict(list)
    for incidencia in incidencias:
        grouped[incidencia.empleado_id].append(incidencia)
    return grouped


def _asistencias_por_empleado(corte: PrenominaCorte, empleado_ids: list[int]):
    grouped = defaultdict(set)
    for empleado_id, fecha in AsistenciaEmpleado.objects.filter(
        empleado_id__in=empleado_ids,
        fecha__range=(corte.fecha_inicio, corte.fecha_fin),
    ).values_list("empleado_id", "fecha"):
        grouped[empleado_id].add(fecha)
    return grouped


def _ajustes_pendientes_por_empleado(corte: PrenominaCorte, empleado_ids: list[int]):
    grouped = defaultdict(int)
    rows = (
        AjusteAsistencia.objects.filter(
            empleado_id__in=empleado_ids,
            fecha__range=(corte.fecha_inicio, corte.fecha_fin),
            estado=AjusteAsistencia.ESTADO_PENDIENTE,
        )
        .values_list("empleado_id", flat=True)
        .order_by("empleado_id")
    )
    for empleado_id in rows:
        grouped[empleado_id] += 1
    return grouped


def _horas_extra_por_empleado(corte: PrenominaCorte, empleado_ids: list[int]):
    grouped = defaultdict(list)
    horas_extra = HoraExtra.objects.filter(
        empleado_id__in=empleado_ids,
        fecha__range=(corte.fecha_inicio, corte.fecha_fin),
        estado=HoraExtra.ESTADO_AUTORIZADO,
    ).order_by("empleado_id", "fecha", "id")
    for hora_extra in horas_extra:
        grouped[hora_extra.empleado_id].append(hora_extra)
    return grouped


def _calcular_resumen_empleado(
    *,
    corte: PrenominaCorte,
    empleado: Empleado,
    fechas: list[date],
    incidencias: list[IncidenciaAsistencia],
    asistencias: set[date],
    ajustes_pendientes: int,
    horas_extra: list[HoraExtra],
) -> dict:
    fechas_laborables = [
        fecha for fecha in fechas if not empleado.fecha_ingreso or fecha >= empleado.fecha_ingreso
    ]
    dias_pre_ingreso = len(fechas) - len(fechas_laborables)
    faltas = 0
    retardos = 0
    suspensiones = 0
    alertas = 0
    mensajes = []

    for incidencia in incidencias:
        if empleado.fecha_ingreso and incidencia.fecha < empleado.fecha_ingreso:
            continue
        if incidencia.tipo in {IncidenciaAsistencia.TIPO_FALTA, IncidenciaAsistencia.TIPO_FALTA_RETARDOS}:
            faltas += 1
            if incidencia.estado == IncidenciaAsistencia.ESTADO_CONCILIADO:
                _crear_o_actualizar_movimiento_incidencia(
                    corte,
                    empleado,
                    incidencia,
                    PrenominaMovimiento.TIPO_FALTA,
                    valor=Decimal("1"),
                )
        elif incidencia.tipo in TIPOS_RETARDO:
            retardos += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_SUSPENSION:
            suspensiones += 1
            if incidencia.estado == IncidenciaAsistencia.ESTADO_CONCILIADO:
                _crear_o_actualizar_movimiento_incidencia(
                    corte,
                    empleado,
                    incidencia,
                    PrenominaMovimiento.TIPO_SUSPENSION,
                    valor=Decimal("1"),
                )

        if _incidencia_bloquea(incidencia):
            alertas += 1
        if incidencia.tipo in TIPOS_ALERTA_OPERATIVA or incidencia.severidad in {
            IncidenciaAsistencia.SEVERIDAD_ALTA,
            IncidenciaAsistencia.SEVERIDAD_CRITICA,
        }:
            mensajes.append(
                {
                    "fecha": incidencia.fecha.isoformat(),
                    "tipo": incidencia.tipo,
                    "estado": incidencia.estado,
                    "severidad": incidencia.severidad,
                    "detalle": incidencia.detalle,
                }
            )

    horas_extra_autorizadas = Decimal("0")
    for hora_extra in horas_extra:
        horas_extra_autorizadas += Decimal(str(hora_extra.horas or "0"))
        _crear_o_actualizar_movimiento_hora_extra(corte, empleado, hora_extra)

    estado = PrenominaEmpleadoResumen.ESTADO_LISTO
    if alertas:
        estado = PrenominaEmpleadoResumen.ESTADO_BLOQUEADO
    elif ajustes_pendientes:
        estado = PrenominaEmpleadoResumen.ESTADO_REVISAR

    return {
        "dias_periodo": len(fechas),
        "dias_laborables": len(fechas_laborables),
        "dias_no_laborados_pre_ingreso": dias_pre_ingreso,
        "dias_asistencia": len([fecha for fecha in asistencias if fecha in set(fechas_laborables)]),
        "faltas": faltas,
        "retardos": retardos,
        "suspensiones": suspensiones,
        "horas_extra_autorizadas": horas_extra_autorizadas,
        "ajustes_pendientes": ajustes_pendientes,
        "alertas_bloqueantes": alertas,
        "estado": estado,
        "observaciones": _observaciones(mensajes, ajustes_pendientes),
        "snapshot": {
            "dias_pre_ingreso": dias_pre_ingreso,
            "incidencias": mensajes,
        },
    }


def _incidencia_bloquea(incidencia: IncidenciaAsistencia) -> bool:
    return (
        incidencia.estado == IncidenciaAsistencia.ESTADO_PENDIENTE
        and incidencia.severidad in {
            IncidenciaAsistencia.SEVERIDAD_ALTA,
            IncidenciaAsistencia.SEVERIDAD_CRITICA,
        }
    )


def _observaciones(mensajes: list[dict], ajustes_pendientes: int) -> str:
    partes = []
    if ajustes_pendientes:
        partes.append(f"{ajustes_pendientes} ajuste(s) de asistencia pendiente(s).")
    partes.extend(mensaje["detalle"] for mensaje in mensajes if mensaje.get("detalle"))
    return "\n".join(partes)


def _crear_o_actualizar_movimiento_incidencia(
    corte: PrenominaCorte,
    empleado: Empleado,
    incidencia: IncidenciaAsistencia,
    tipo_movimiento: str,
    *,
    valor: Decimal,
) -> PrenominaMovimiento:
    return _crear_o_actualizar_movimiento(
        corte=corte,
        empleado=empleado,
        fecha=incidencia.fecha,
        tipo_movimiento=tipo_movimiento,
        fuente_modelo="rrhh.IncidenciaAsistencia",
        fuente_id=str(incidencia.id),
        valor=valor,
        horas=None,
        notas=incidencia.detalle,
        metadata={"incidencia_tipo": incidencia.tipo, "incidencia_estado": incidencia.estado},
    )


def _crear_o_actualizar_movimiento_hora_extra(
    corte: PrenominaCorte,
    empleado: Empleado,
    hora_extra: HoraExtra,
) -> PrenominaMovimiento:
    return _crear_o_actualizar_movimiento(
        corte=corte,
        empleado=empleado,
        fecha=hora_extra.fecha,
        tipo_movimiento=PrenominaMovimiento.TIPO_HORA_EXTRA,
        fuente_modelo="rrhh.HoraExtra",
        fuente_id=str(hora_extra.id),
        valor=None,
        horas=Decimal(str(hora_extra.horas or "0")),
        notas=hora_extra.notas,
        metadata={"hora_extra_estado": hora_extra.estado},
    )


def _crear_o_actualizar_movimiento(
    *,
    corte: PrenominaCorte,
    empleado: Empleado,
    fecha: date,
    tipo_movimiento: str,
    fuente_modelo: str,
    fuente_id: str,
    valor: Decimal | None,
    horas: Decimal | None,
    notas: str,
    metadata: dict,
) -> PrenominaMovimiento:
    movimiento, _ = PrenominaMovimiento.objects.update_or_create(
        corte=corte,
        fuente_modelo=fuente_modelo,
        fuente_id=fuente_id,
        tipo_movimiento_erp=tipo_movimiento,
        defaults={
            "empleado": empleado,
            "fecha": fecha,
            "valor": valor,
            "horas": horas,
            "referencia": f"{fuente_modelo}:{fuente_id}",
            "notas": notas or "",
            "metadata": metadata,
        },
    )
    movimiento.aplicar_equivalencia()
    movimiento.save(update_fields=["clave_contpaqi", "estado", "actualizado_en"])
    return movimiento


def _resumen_corte(corte: PrenominaCorte) -> dict:
    resumenes = list(corte.resumenes.all())
    return {
        "colaboradores": len(resumenes),
        "faltas": sum(row.faltas for row in resumenes),
        "retardos": sum(row.retardos for row in resumenes),
        "suspensiones": sum(row.suspensiones for row in resumenes),
        "horas_extra": str(sum((row.horas_extra_autorizadas for row in resumenes), Decimal("0"))),
        "ajustes_pendientes": sum(row.ajustes_pendientes for row in resumenes),
        "bloqueados": sum(1 for row in resumenes if row.estado == PrenominaEmpleadoResumen.ESTADO_BLOQUEADO),
        "movimientos_listos": corte.movimientos.filter(estado=PrenominaMovimiento.ESTADO_LISTO).count(),
        "movimientos_pendientes_configuracion": corte.movimientos.filter(
            estado=PrenominaMovimiento.ESTADO_PENDIENTE_CONFIGURACION
        ).count(),
    }
