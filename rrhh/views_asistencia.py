from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from io import BytesIO
from urllib.parse import urlencode

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import render
from django.utils.dateparse import parse_date
from django.utils import timezone
from openpyxl import Workbook

from core.access import can_manage_rrhh, can_view_rrhh

from .models import AsistenciaEmpleado, Empleado, IncidenciaAsistencia, PermisoSalida, SolicitudVacaciones
from .views import _module_tabs


REPORTE_ASISTENCIA_HEADERS = [
    "codigo",
    "nombre",
    "sucursal",
    "fecha",
    "tipo_incidencia",
    "estado",
    "severidad",
    "minutos",
    "detalle",
]


def _parse_fecha(value: str | None, default: date) -> date:
    parsed = parse_date((value or "").strip())
    return parsed or default


def _date_range(fecha_inicio: date, fecha_fin: date) -> list[date]:
    dias = (fecha_fin - fecha_inicio).days
    return [fecha_inicio + timedelta(days=offset) for offset in range(dias + 1)]


def _badge_class(severidad: str) -> str:
    if severidad in {IncidenciaAsistencia.SEVERIDAD_ALTA, IncidenciaAsistencia.SEVERIDAD_CRITICA}:
        return "bg-danger"
    if severidad == IncidenciaAsistencia.SEVERIDAD_MEDIA:
        return "bg-warning"
    return "bg-primary"


def _localized_range(fecha_inicio: date, fecha_fin: date) -> tuple[datetime, datetime]:
    tz = timezone.get_current_timezone()
    inicio = timezone.make_aware(datetime.combine(fecha_inicio, time.min), tz)
    fin = timezone.make_aware(datetime.combine(fecha_fin, time.max), tz)
    return inicio, fin


def _build_export_rows(reportes: list[dict]) -> list[list]:
    rows = []
    for reporte in reportes:
        datos = reporte["datos"]
        for fila in reporte["filas"]:
            for incidencia in fila["incidencias"]:
                rows.append(
                    [
                        datos["codigo"],
                        datos["nombre"],
                        datos["sucursal"],
                        fila["fecha"].isoformat(),
                        incidencia["tipo"],
                        incidencia["estado"],
                        incidencia["severidad"],
                        incidencia["minutos"],
                        incidencia["detalle"],
                    ]
                )
    return rows


def _export_csv(rows: list[list]) -> HttpResponse:
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="reporte_asistencia.csv"'
    writer = csv.writer(response)
    writer.writerow(REPORTE_ASISTENCIA_HEADERS)
    writer.writerows(rows)
    return response


def _export_xlsx(rows: list[list]) -> HttpResponse:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "reporte_asistencia"
    sheet.append(REPORTE_ASISTENCIA_HEADERS)
    for row in rows:
        sheet.append(row)

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="reporte_asistencia.xlsx"'
    return response


def _build_reporte_asistencia(fecha_inicio: date, fecha_fin: date, empleado_id: str, sucursal: str) -> tuple[list[dict], int]:
    empleados_qs = Empleado.objects.filter(activo=True).order_by("nombre", "codigo")
    if empleado_id.isdigit():
        empleados_qs = empleados_qs.filter(id=int(empleado_id))
    if sucursal:
        empleados_qs = empleados_qs.filter(sucursal__icontains=sucursal)

    empleados = list(empleados_qs)
    empleado_ids = [empleado.id for empleado in empleados]
    fechas = _date_range(fecha_inicio, fecha_fin)
    inicio_dt, fin_dt = _localized_range(fecha_inicio, fecha_fin)

    asistencias = (
        AsistenciaEmpleado.objects.filter(empleado_id__in=empleado_ids, fecha__range=(fecha_inicio, fecha_fin))
        .select_related("empleado", "turno", "sucursal")
        .order_by("empleado__nombre", "fecha")
    )
    asistencias_por_dia = {(asistencia.empleado_id, asistencia.fecha): asistencia for asistencia in asistencias}

    incidencias = (
        IncidenciaAsistencia.objects.filter(empleado_id__in=empleado_ids, fecha__range=(fecha_inicio, fecha_fin))
        .select_related("empleado")
        .order_by("empleado__nombre", "fecha", "tipo")
    )
    incidencias_por_dia = defaultdict(list)
    resumenes = defaultdict(
        lambda: {
            "faltas": 0,
            "retardos": 0,
            "comida_excedida": 0,
            "jornada_incompleta": 0,
            "hora_extra": 0,
            "permisos": 0,
            "vacaciones": 0,
            "suspensiones": 0,
        }
    )

    for incidencia in incidencias:
        resumen = resumenes[incidencia.empleado_id]
        if incidencia.tipo == IncidenciaAsistencia.TIPO_FALTA:
            resumen["faltas"] += 1
        elif incidencia.tipo in {IncidenciaAsistencia.TIPO_RETARDO, IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA}:
            resumen["retardos"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_COMIDA_EXCEDIDA:
            resumen["comida_excedida"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_JORNADA_INCOMPLETA:
            resumen["jornada_incompleta"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE:
            resumen["hora_extra"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_SUSPENSION:
            resumen["suspensiones"] += 1

        incidencias_por_dia[(incidencia.empleado_id, incidencia.fecha)].append(
            {
                "tipo": incidencia.get_tipo_display(),
                "estado": incidencia.get_estado_display(),
                "severidad": incidencia.get_severidad_display(),
                "badge_class": _badge_class(incidencia.severidad),
                "minutos": incidencia.minutos,
                "detalle": incidencia.detalle,
            }
        )

    permisos = PermisoSalida.objects.filter(
        empleado_id__in=empleado_ids,
        estado=PermisoSalida.ESTADO_APROBADO,
        fecha_inicio__lte=fin_dt,
    ).filter(Q(fecha_fin__gte=inicio_dt) | Q(fecha_fin__isnull=True))
    for permiso in permisos:
        resumenes[permiso.empleado_id]["permisos"] += 1

    vacaciones = SolicitudVacaciones.objects.filter(
        empleado_id__in=empleado_ids,
        estado=SolicitudVacaciones.ESTADO_APROBADA,
        fecha_inicio__lte=fecha_fin,
        fecha_fin__gte=fecha_inicio,
    )
    for solicitud in vacaciones:
        inicio = max(solicitud.fecha_inicio, fecha_inicio)
        fin = min(solicitud.fecha_fin, fecha_fin)
        resumenes[solicitud.empleado_id]["vacaciones"] += (fin - inicio).days + 1

    reportes = []
    total_incidencias = 0
    for empleado in empleados:
        filas = []
        for fecha in fechas:
            incidencia_dia = incidencias_por_dia.get((empleado.id, fecha), [])
            total_incidencias += len(incidencia_dia)
            asistencia = asistencias_por_dia.get((empleado.id, fecha))
            filas.append(
                {
                    "fecha": fecha,
                    "asistencia": asistencia,
                    "incidencias": incidencia_dia,
                }
            )
        reportes.append(
            {
                "empleado": empleado,
                "datos": {
                    "nombre": empleado.nombre,
                    "codigo": empleado.codigo,
                    "puesto": empleado.puesto,
                    "sucursal": empleado.sucursal,
                    "departamento": empleado.get_departamento_display() if empleado.departamento else "",
                },
                "resumen": resumenes[empleado.id],
                "filas": filas,
            }
        )
    return reportes, total_incidencias


@login_required
def monitor_sincronizacion(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver el monitor de asistencia")

    hoy = timezone.localdate()
    empleados_activos = Empleado.objects.filter(activo=True).count()
    asistencias_hoy = AsistenciaEmpleado.objects.filter(fecha=hoy).select_related("empleado", "turno", "sucursal")
    emp_con_asistencia = set(asistencias_hoy.values_list("empleado_id", flat=True))
    emp_sin_asistencia = Empleado.objects.filter(activo=True).exclude(id__in=emp_con_asistencia).order_by("nombre")
    ultimas_por_api = (
        AsistenciaEmpleado.objects.filter(
            fuente__in=[AsistenciaEmpleado.FUENTE_HIKCONNECT_API, AsistenciaEmpleado.FUENTE_POINT]
        )
        .select_related("empleado", "turno", "sucursal")
        .order_by("-creado_en")[:20]
    )

    return render(
        request,
        "rrhh/monitor_sincronizacion.html",
        {
            "module_tabs": _module_tabs("monitor_sync", request.user),
            "hoy": hoy,
            "ayer": hoy - timedelta(days=1),
            "empleados_activos": empleados_activos,
            "asistencias_hoy": asistencias_hoy.order_by("empleado__nombre"),
            "emp_sin_asistencia": emp_sin_asistencia,
            "ultimas_por_api": ultimas_por_api,
            "resumen": {
                "total_activos": empleados_activos,
                "con_asistencia_hoy": len(emp_con_asistencia),
                "sin_asistencia_hoy": emp_sin_asistencia.count(),
            },
        },
    )


@login_required
def reporte_asistencia(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver el reporte de asistencia")

    hoy = timezone.localdate()
    fecha_fin = _parse_fecha(request.GET.get("fecha_fin"), hoy)
    fecha_inicio = _parse_fecha(request.GET.get("fecha_inicio"), fecha_fin - timedelta(days=14))
    if fecha_inicio > fecha_fin:
        fecha_inicio, fecha_fin = fecha_fin, fecha_inicio

    empleado_id = (request.GET.get("empleado") or "").strip()
    sucursal = (request.GET.get("sucursal") or "").strip()
    reportes, total_incidencias = _build_reporte_asistencia(fecha_inicio, fecha_fin, empleado_id, sucursal)

    export = (request.GET.get("export") or "").strip().lower()
    if export in {"csv", "xlsx"}:
        rows = _build_export_rows(reportes)
        if export == "csv":
            return _export_csv(rows)
        return _export_xlsx(rows)

    export_params = {
        "fecha_inicio": fecha_inicio.isoformat(),
        "fecha_fin": fecha_fin.isoformat(),
        "empleado": empleado_id,
        "sucursal": sucursal,
    }
    query_csv = urlencode({**export_params, "export": "csv"})
    query_xlsx = urlencode({**export_params, "export": "xlsx"})

    return render(
        request,
        "rrhh/reporte_asistencia.html",
        {
            "module_tabs": _module_tabs("reporte_asistencia", request.user),
            "reportes": reportes,
            "empleados": Empleado.objects.filter(activo=True).order_by("nombre", "codigo"),
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin": fecha_fin.isoformat(),
            "empleado_id": empleado_id,
            "sucursal": sucursal,
            "can_manage": can_manage_rrhh(request.user),
            "total_incidencias": total_incidencias,
            "query_csv": query_csv,
            "query_xlsx": query_xlsx,
        },
    )
