from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from io import BytesIO
from urllib.parse import urlencode

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import connection, transaction
from django.db.models import Exists, OuterRef, Q, Subquery
from django.http import HttpResponse, JsonResponse
from django.core.paginator import Paginator
from django.views.decorators.cache import never_cache
import logging
import calendar
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.dateparse import parse_date
from django.utils import timezone
from django.views.decorators.http import require_POST
from openpyxl import Workbook

from core.access import can_manage_rrhh, can_view_rrhh

from .models import (
    AjusteAsistencia,
    AsistenciaEmpleado,
    Empleado,
    EmpleadoBaja,
    IncidenciaAsistencia,
    IncidenciaAsistenciaBitacora,
    HoraExtra,
    PermisoSalida,
    SolicitudVacaciones,
)
from .services import can_edit_incidencia
from .services_extra_conciliacion import conciliar_extra_diario, formato_minutos
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
    "entrada", "salida", "minutos_comida", "extra_detectado_minutos",
    "extra_autorizado_minutos", "extra_pendiente_minutos", "extra_rechazado_minutos",
    "conciliacion_extra",
]


def _empleados_reporte_asistencia(fecha_inicio: date, fecha_fin: date):
    """Mantiene activos y recupera bajas con actividad en el rango consultado."""
    asistencias_en_rango = AsistenciaEmpleado.objects.filter(
        empleado_id=OuterRef("pk"),
        fecha__range=(fecha_inicio, fecha_fin),
    )
    incidencias_en_rango = IncidenciaAsistencia.objects.filter(
        empleado_id=OuterRef("pk"),
        fecha__range=(fecha_inicio, fecha_fin),
    )
    ultima_baja = (
        EmpleadoBaja.objects.filter(empleado_id=OuterRef("pk"))
        .order_by("-fecha_baja", "-id")
        .values("fecha_baja")[:1]
    )
    extras_en_rango = HoraExtra.objects.filter(empleado_id=OuterRef('pk'), fecha__range=(fecha_inicio, fecha_fin))
    return (
        Empleado.objects.alias(
            tiene_extra=Exists(extras_en_rango),
            tiene_asistencia=Exists(asistencias_en_rango),
            tiene_incidencia=Exists(incidencias_en_rango),
        )
        .filter(Q(activo=True) | Q(tiene_asistencia=True) | Q(tiene_incidencia=True) | Q(tiene_extra=True))
        .annotate(fecha_baja_reporte=Subquery(ultima_baja))
        .order_by("nombre", "codigo")
    )


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


def _es_fecha_pre_ingreso(empleado: Empleado, fecha: date) -> bool:
    return bool(empleado.fecha_ingreso and fecha < empleado.fecha_ingreso)


def _build_fila_pre_ingreso(empleado: Empleado, fecha: date) -> dict:
    detalle = f"Ingreso: {empleado.fecha_ingreso.isoformat()}" if empleado.fecha_ingreso else ""
    return {
        "fecha": fecha,
        "asistencia": None,
        "incidencias": [],
        "estado_laboral": "pre_ingreso",
        "estado_laboral_label": "No laborado (previo ingreso)",
        "detalle_laboral": detalle,
    }


def _build_export_rows(reportes: list[dict]) -> list[list]:
    rows = []
    for reporte in reportes:
        datos = reporte['datos']
        for fila in reporte['filas']:
            asistencia = fila['asistencia']
            extra = fila.get('extra', {})
            suffix = [
                timezone.localtime(asistencia.entrada).strftime('%H:%M') if asistencia and asistencia.entrada else '',
                timezone.localtime(asistencia.salida).strftime('%H:%M') if asistencia and asistencia.salida else '',
                asistencia.minutos_comida if asistencia else '',
                extra.get('detectado_minutos'), extra.get('autorizado_minutos'),
                extra.get('pendiente_minutos'), extra.get('rechazado_minutos'), extra.get('estado', 'No aplica'),
            ]
            incidencias = fila['incidencias'] or [{
                'tipo': fila.get('estado_laboral_label', ''), 'estado': 'No aplica' if fila.get('estado_laboral_label') else '',
                'severidad': '', 'minutos': 0, 'detalle': fila.get('detalle_laboral', ''),
            }]
            for index, incidencia in enumerate(incidencias):
                rows.append([datos['codigo'], datos['nombre'], datos['sucursal'], fila['fecha'].isoformat(),
                    incidencia['tipo'], incidencia['estado'], incidencia['severidad'], incidencia['minutos'], incidencia['detalle'],
                    *(suffix if index == 0 else [''] * len(suffix))])
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


def _build_reporte_asistencia(
    fecha_inicio: date,
    fecha_fin: date,
    empleado_id: str,
    sucursal: str,
    user=None,
    empleados_disponibles: list[Empleado] | None = None,
) -> tuple[list[dict], int]:
    if empleados_disponibles is None:
        empleados_qs = _empleados_reporte_asistencia(fecha_inicio, fecha_fin).select_related(
            "sucursal_ref",
            "jefe_directo__usuario_erp",
        )
        if empleado_id.isdigit():
            empleados_qs = empleados_qs.filter(id=int(empleado_id))
        if sucursal:
            empleados_qs = empleados_qs.filter(sucursal__icontains=sucursal)
        empleados = list(empleados_qs)
    else:
        empleados = empleados_disponibles
        if empleado_id.isdigit():
            empleado_pk = int(empleado_id)
            empleados = [empleado for empleado in empleados if empleado.id == empleado_pk]
        if sucursal:
            sucursal_normalizada = sucursal.casefold()
            empleados = [
                empleado
                for empleado in empleados
                if sucursal_normalizada in (empleado.sucursal or "").casefold()
            ]
    empleado_ids = [empleado.id for empleado in empleados]
    empleados_por_id = {empleado.id: empleado for empleado in empleados}
    fechas = _date_range(fecha_inicio, fecha_fin)
    inicio_dt, fin_dt = _localized_range(fecha_inicio, fecha_fin)

    asistencias = (
        AsistenciaEmpleado.objects.filter(empleado_id__in=empleado_ids, fecha__range=(fecha_inicio, fecha_fin))
        .select_related("empleado", "turno", "sucursal")
        .order_by("empleado__nombre", "fecha")
    )
    asistencias_por_dia = {(asistencia.empleado_id, asistencia.fecha): asistencia for asistencia in asistencias}
    extras_por_dia = defaultdict(list)
    for extra in HoraExtra.objects.filter(empleado_id__in=empleado_ids, fecha__range=(fecha_inicio, fecha_fin)):
        extras_por_dia[(extra.empleado_id, extra.fecha)].append(extra)

    incidencias = (
        IncidenciaAsistencia.objects.filter(empleado_id__in=empleado_ids, fecha__range=(fecha_inicio, fecha_fin))
        .exclude(estado=IncidenciaAsistencia.ESTADO_RESUELTO)
        .select_related("empleado")
        .order_by("empleado__nombre", "fecha", "tipo")
    )
    incidencias_por_dia = defaultdict(list)
    resumenes = defaultdict(
        lambda: {
            "faltas": 0,
            "faltas_conciliadas": 0,
            "falta_retardos": 0,
            "retardos": 0,
            "comida_excedida": 0,
            "jornada_incompleta": 0,
            "hora_extra": 0,
            "avisos_baja": 0,
            "permisos": 0,
            "vacaciones": 0,
            "suspensiones": 0,
        }
    )

    for incidencia in incidencias:
        empleado = empleados_por_id.get(incidencia.empleado_id)
        if empleado and _es_fecha_pre_ingreso(empleado, incidencia.fecha):
            continue
        resumen = resumenes[incidencia.empleado_id]
        # Los KPI disciplinarios cuentan solo pendientes; una incidencia
        # conciliada (permiso/vacaciones) no debe sumar como sanción.
        pendiente = incidencia.estado == IncidenciaAsistencia.ESTADO_PENDIENTE
        if incidencia.tipo == IncidenciaAsistencia.TIPO_FALTA:
            if pendiente:
                resumen["faltas"] += 1
            else:
                resumen["faltas_conciliadas"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_FALTA_RETARDOS:
            if pendiente:
                resumen["falta_retardos"] += 1
        elif incidencia.tipo in {IncidenciaAsistencia.TIPO_RETARDO, IncidenciaAsistencia.TIPO_RETARDO_TOLERANCIA}:
            if pendiente:
                resumen["retardos"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_COMIDA_EXCEDIDA:
            if pendiente:
                resumen["comida_excedida"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_JORNADA_INCOMPLETA:
            if pendiente:
                resumen["jornada_incompleta"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE:
            resumen["hora_extra"] += 1
        elif incidencia.tipo in {IncidenciaAsistencia.TIPO_AVISO_BAJA_FALTAS, IncidenciaAsistencia.TIPO_BAJA_FALTAS}:
            if pendiente:
                resumen["avisos_baja"] += 1
        elif incidencia.tipo == IncidenciaAsistencia.TIPO_SUSPENSION:
            resumen["suspensiones"] += 1

        incidencias_por_dia[(incidencia.empleado_id, incidencia.fecha)].append(
            {
                "id": incidencia.id,
                "tipo": incidencia.get_tipo_display(),
                "estado": incidencia.get_estado_display(),
                "estado_codigo": incidencia.estado,
                "severidad": incidencia.get_severidad_display(),
                "badge_class": _badge_class(incidencia.severidad),
                "minutos": incidencia.minutos,
                "detalle": incidencia.detalle,
                "editado_manual": incidencia.editado_manual,
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

    empleado_especifico = empleado_id.isdigit()
    reportes = []
    total_incidencias = 0
    puede_gestionar_rrhh = can_manage_rrhh(user) if user else False
    for empleado in empleados:
        jefe_usuario_id = getattr(getattr(empleado, "jefe_directo", None), "usuario_erp_id", None)
        puede_editar = bool(user and (puede_gestionar_rrhh or jefe_usuario_id == user.id))
        resumen = resumenes[empleado.id]
        totales_extra = {'detectado_minutos': 0, 'autorizado_minutos': 0, 'pendiente_minutos': 0, 'rechazado_minutos': 0, 'dias_no_calculables': 0}
        filas = []
        for fecha in fechas:
            if _es_fecha_pre_ingreso(empleado, fecha):
                if empleado_especifico:
                    filas.append(_build_fila_pre_ingreso(empleado, fecha))
                continue
            incidencia_dia = incidencias_por_dia.get((empleado.id, fecha), [])
            total_incidencias += len(incidencia_dia)
            asistencia = asistencias_por_dia.get((empleado.id, fecha))
            registros_extra = extras_por_dia.get((empleado.id, fecha), [])
            # Solo días con evento: registro de checador o incidencia.
            if not incidencia_dia and not asistencia and not registros_extra:
                continue
            extra = conciliar_extra_diario(asistencia, registros_extra)
            for key in ('detectado_minutos', 'autorizado_minutos', 'pendiente_minutos', 'rechazado_minutos'):
                totales_extra[key] += extra[key] or 0
            if extra['detectado_minutos'] is None:
                totales_extra['dias_no_calculables'] += 1
            filas.append(
                {
                    "fecha": fecha,
                    "asistencia": asistencia,
                    "incidencias": incidencia_dia,
                    "extra": extra,
                }
            )
        # Omitir empleados sin actividad, salvo cuando se pidió uno específico.
        if not empleado_especifico and not filas and not any(resumen.values()):
            continue
        reportes.append(
            {
                "empleado": empleado,
                "datos": {
                    "nombre": empleado.nombre,
                    "codigo": empleado.codigo,
                    "puesto": empleado.puesto,
                    "sucursal": empleado.sucursal_display,
                    "departamento": empleado.get_departamento_display() if empleado.departamento else "",
                },
                "resumen": resumen,
                "extra_resumen": {**totales_extra, **{key.removesuffix('_minutos'): formato_minutos(totales_extra[key]) for key in ('detectado_minutos', 'autorizado_minutos', 'pendiente_minutos', 'rechazado_minutos')}},
                "filas": filas,
                "puede_editar": puede_editar,
            }
        )
    return reportes, total_incidencias


def _redirect_reporte_asistencia_from_post(request):
    params = {
        "fecha_inicio": (request.POST.get("fecha_inicio") or "").strip(),
        "fecha_fin": (request.POST.get("fecha_fin") or "").strip(),
        "empleado": (request.POST.get("empleado") or "").strip(),
        "sucursal": (request.POST.get("sucursal") or "").strip(),
        "vista": "incidencias",
    }
    query = urlencode({key: value for key, value in params.items() if value})
    url = reverse("rrhh:rrhh_reporte_asistencia")
    return redirect(f"{url}?{query}" if query else url)


@login_required
@require_POST
@transaction.atomic
def editar_incidencia(request, incidencia_id):
    incidencia = get_object_or_404(
        IncidenciaAsistencia.objects.select_related("empleado", "empleado__jefe_directo__usuario_erp"),
        pk=incidencia_id,
    )
    if not can_edit_incidencia(request.user, incidencia):
        raise PermissionDenied("No tienes permisos para editar esta incidencia")

    comentario = (request.POST.get("comentario") or "").strip()
    if not comentario:
        messages.error(request, "El comentario es obligatorio para editar una incidencia.")
        return _redirect_reporte_asistencia_from_post(request)

    estado = (request.POST.get("estado") or "").strip()
    estados_validos = {value for value, _ in IncidenciaAsistencia.ESTADO_CHOICES}
    if estado not in estados_validos:
        messages.error(request, "El estado seleccionado no es válido.")
        return _redirect_reporte_asistencia_from_post(request)

    try:
        minutos = int((request.POST.get("minutos") or "0").strip())
    except ValueError:
        messages.error(request, "Los minutos deben ser un número entero.")
        return _redirect_reporte_asistencia_from_post(request)
    if minutos < 0:
        messages.error(request, "Los minutos no pueden ser negativos.")
        return _redirect_reporte_asistencia_from_post(request)

    detalle = (request.POST.get("detalle") or "").strip()
    cambios = {
        "estado": estado,
        "minutos": minutos,
        "detalle": detalle,
    }
    campos_actualizados = []
    for campo, valor_nuevo in cambios.items():
        valor_anterior = getattr(incidencia, campo)
        if valor_anterior == valor_nuevo:
            continue
        IncidenciaAsistenciaBitacora.objects.create(
            incidencia=incidencia,
            usuario=request.user,
            campo=campo,
            valor_anterior=str(valor_anterior),
            valor_nuevo=str(valor_nuevo),
            comentario=comentario,
        )
        setattr(incidencia, campo, valor_nuevo)
        campos_actualizados.append(campo)

    if campos_actualizados:
        incidencia.editado_manual = True
        incidencia.save(update_fields=[*campos_actualizados, "editado_manual", "actualizado_en"])
        messages.success(request, "Incidencia actualizada correctamente.")
    else:
        messages.info(request, "No hubo cambios para guardar.")

    return _redirect_reporte_asistencia_from_post(request)


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
    from .models import EmpleadoIdentidadPendiente, EstadoIntegracionHik, EventoHikCloud

    estado_hik = EstadoIntegracionHik.objects.filter(nombre="hikconnect_cloud").first()
    estado_hik_stale = (
        not estado_hik
        or estado_hik.reportado_en < timezone.now() - timedelta(minutes=10)
    )
    hik_deferred = EventoHikCloud.objects.filter(
        estado=EventoHikCloud.ESTADO_DIFERIDO
    ).count()
    identidades_hik_pendientes = EmpleadoIdentidadPendiente.objects.filter(
        fuente=EmpleadoIdentidadPendiente.FUENTE_HIKVISION,
        estado=EmpleadoIdentidadPendiente.ESTADO_PENDIENTE,
    ).count()

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
            "estado_hik": estado_hik,
            "estado_hik_stale": estado_hik_stale,
            "hik_deferred": hik_deferred,
            "identidades_hik_pendientes": identidades_hik_pendientes,
            "resumen": {
                "total_activos": empleados_activos,
                "con_asistencia_hoy": len(emp_con_asistencia),
                "sin_asistencia_hoy": emp_sin_asistencia.count(),
            },
        },
    )


@login_required
def reporte_asistencia_incidencias(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver el reporte de asistencia")

    hoy = timezone.localdate()
    fecha_fin = _parse_fecha(request.GET.get("fecha_fin"), hoy)
    fecha_inicio = _parse_fecha(request.GET.get("fecha_inicio"), fecha_fin - timedelta(days=14))
    if fecha_inicio > fecha_fin:
        fecha_inicio, fecha_fin = fecha_fin, fecha_inicio

    empleado_id = (request.GET.get("empleado") or "").strip()
    sucursal = (request.GET.get("sucursal") or "").strip()
    export = (request.GET.get("export") or "").strip().lower()
    empleados_disponibles = None
    if export not in {"csv", "xlsx"}:
        empleados_disponibles = list(
            _empleados_reporte_asistencia(fecha_inicio, fecha_fin).select_related(
                "sucursal_ref",
                "jefe_directo__usuario_erp",
            )
        )
    reportes, total_incidencias = _build_reporte_asistencia(
        fecha_inicio,
        fecha_fin,
        empleado_id,
        sucursal,
        request.user,
        empleados_disponibles=empleados_disponibles,
    )

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
            "empleados": empleados_disponibles,
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin": fecha_fin.isoformat(),
            "empleado_id": empleado_id,
            "sucursal": sucursal,
            "can_manage": can_manage_rrhh(request.user),
            "incidencia_estado_choices": IncidenciaAsistencia.ESTADO_CHOICES,
            "total_incidencias": total_incidencias,
            "query_csv": query_csv,
            "query_xlsx": query_xlsx,
        },
    )


@login_required
@never_cache
def reporte_asistencia(request):
    """Consulta completa compartida por pantalla, PDF e impresión; sin escrituras."""
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver el reporte de asistencia")
    export = (request.GET.get("export") or "").strip().lower()
    if (request.GET.get("vista") == "incidencias" or export in {"csv", "xlsx"}) and not (request.GET.get("departamento") or request.GET.get("area")):
        return reporte_asistencia_incidencias(request)

    from .services_reporte_asistencia import build_reporte_departamento

    hoy = timezone.localdate()
    defaults = {"fecha_inicio": (hoy - timedelta(days=14)).isoformat(), "fecha_fin": hoy.isoformat()}
    filtros = {key: (request.GET.get(key) or "").strip() for key in
               ("fecha_inicio", "fecha_fin", "departamento", "area", "empleado", "sucursal")}
    for key, value in defaults.items():
        if not filtros[key]:
            filtros[key] = value
    error = ""
    data = None
    catalogo = []
    try:
        inicio, fin = parse_date(filtros["fecha_inicio"]), parse_date(filtros["fecha_fin"])
        if inicio is None or fin is None:
            raise ValidationError("Indica fechas válidas para la consulta.")
        if export not in {"", "pdf", "imprimir"}:
            raise ValidationError("El formato de reporte solicitado no es válido.")
        # In production this is the first statement of a fresh read-only snapshot.
        # Django TestCase already has an outer transaction; do not change it.
        outer_transaction = connection.in_atomic_block
        with transaction.atomic():
            if not outer_transaction:
                with connection.cursor() as cursor:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            data = build_reporte_departamento(inicio, fin, departamento=filtros["departamento"],
                area=filtros["area"], empleado_id=filtros["empleado"], sucursal=filtros["sucursal"], user=request.user)
            # Catalog and results use the same snapshot. The legacy helper remains
            # the reference for active staff and former staff with recorded activity.
            catalogo = list(_empleados_reporte_asistencia(inicio, fin).select_related("sucursal_ref"))
            ids = {e.id for e in catalogo}
            catalogo.extend(r["empleado"] for r in data["reportes"] if r["empleado"].id not in ids)
    except (ValidationError, ValueError, OverflowError) as exc:
        error = " ".join(exc.messages) if isinstance(exc, ValidationError) else "Indica fechas válidas para la consulta."

    base_params = {key: value for key, value in filtros.items() if value}
    context = {**filtros, "empleado_id": filtros["empleado"], "module_tabs": _module_tabs("reporte_asistencia", request.user),
        "error_reporte": error, "data": data, "resumen_global": data["resumen"] if data else None,
        "empleados": catalogo, "departamentos": Empleado.DEP_CHOICES,
        "empleado_seleccionado_disponible": any(str(e.id) == filtros["empleado"] for e in catalogo),
        "departamento_valido": not filtros["departamento"] or filtros["departamento"] in dict(Empleado.DEP_CHOICES),
        "departamento_nombre": dict(Empleado.DEP_CHOICES).get(filtros["departamento"], "Todos los departamentos"),
        "areas": sorted({e.area for e in catalogo if e.area}),
        "sucursales": sorted({e.sucursal for e in catalogo if e.sucursal}),
        "query_volver": urlencode(base_params),
        "query_pdf": urlencode({**base_params, "export": "pdf"}),
        "query_imprimir": urlencode({**base_params, "export": "imprimir"}),
        "query_incidencias": urlencode({"vista": "incidencias", **{k:v for k,v in base_params.items() if k not in {"departamento", "area"}}}),
        "mes_periodo": filtros["fecha_inicio"][:7], "quincena_seleccionada": "", "reportes": [], "total_incidencias": 0}
    if data:
        if inicio.year == fin.year and inicio.month == fin.month:
            if inicio.day == 1 and fin.day == 15:
                context["quincena_seleccionada"] = "1"
            elif inicio.day == 16 and fin.day == calendar.monthrange(fin.year, fin.month)[1]:
                context["quincena_seleccionada"] = "2"
    if error:
        return render(request, "rrhh/reporte_asistencia_completo.html", context, status=400)
    for reporte in data["reportes"]:
        employee_params = {**base_params, "empleado": str(reporte["empleado"].id)}
        reporte["query_pdf"] = urlencode({**employee_params, "export": "pdf"})
        reporte["query_consulta"] = urlencode(employee_params)
    context["total_incidencias"] = sum(len(f["incidencias"]) for r in data["reportes"] for f in r["filas"])
    if export == "pdf":
        try:
            from .exports_reporte_asistencia import exportar_pdf_reporte
            return exportar_pdf_reporte(data)
        except Exception:
            logging.getLogger(__name__).exception("No se pudo generar el PDF de asistencia")
            context["error_reporte"] = "No se pudo generar el PDF. Conservamos los filtros para que puedas reintentar."
            return render(request, "rrhh/reporte_asistencia_completo.html", context, status=503)
    if export == "imprimir":
        context["reportes"] = data["reportes"]
        return render(request, "rrhh/reporte_asistencia_imprimir.html", context)
    paginator = Paginator(data["reportes"], 10)
    pagina = paginator.get_page(request.GET.get("page", 1))
    context.update({"reportes": list(pagina.object_list), "pagina": pagina,
        "query_pagina": urlencode(base_params), "total_empleados": paginator.count})
    return render(request, "rrhh/reporte_asistencia_completo.html", context)


AJUSTE_CAMPOS_HORAS = {
    "entrada": AjusteAsistencia.TIPO_ENTRADA,
    "salida_comida": AjusteAsistencia.TIPO_SALIDA_COMIDA,
    "regreso_comida": AjusteAsistencia.TIPO_REGRESO_COMIDA,
    "salida": AjusteAsistencia.TIPO_SALIDA,
}


def _redirect_asistencias(request, *, fragment: str = "") -> str:
    url = reverse("rrhh:rrhh_asistencias")
    query = (request.POST.get("next_query") or "").strip()
    if query:
        url = f"{url}?{query}"
    if fragment:
        url = f"{url}#{fragment}"
    return url


@login_required
@require_POST
def ajustar_asistencia(request):
    """Captura o corrige los horarios de un día desde la pantalla de asistencias.

    Reutiliza el flujo formal de AjusteAsistencia (bitácora + re-evaluación de
    incidencias) aplicado en un paso, porque solo usuarios con manage de RRHH
    llegan aquí. Dispara la sincronización de bonos del día afectado.
    """
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para corregir asistencias.")

    from .services_ajustes_asistencia import aprobar_ajuste_asistencia, crear_ajuste_asistencia
    from .services_bonos_checador import programar_sincronizacion_bonos_desde_checador
    from .views import _wants_progressive_response

    progressive = _wants_progressive_response(request)

    def _responder_error(mensaje: str):
        if progressive:
            return JsonResponse(
                {"ok": False, "toast": {"type": "error", "message": mensaje, "persistent": True}},
                status=400,
            )
        messages.error(request, mensaje)
        return redirect(_redirect_asistencias(request))

    empleado = Empleado.objects.filter(pk=request.POST.get("empleado_id") or 0).first()
    if empleado is None:
        return _responder_error("Selecciona un empleado válido.")
    fecha = parse_date(request.POST.get("fecha") or "")
    if fecha is None:
        return _responder_error("Captura una fecha válida.")
    if fecha > timezone.localdate():
        return _responder_error("No se pueden capturar asistencias de fechas futuras.")
    motivo = (request.POST.get("motivo") or "").strip()
    if not motivo:
        return _responder_error("El motivo es obligatorio para corregir una asistencia.")

    tz = timezone.get_current_timezone()
    cambios = []
    for campo, tipo_ajuste in AJUSTE_CAMPOS_HORAS.items():
        crudo = (request.POST.get(campo) or "").strip()
        if not crudo:
            continue
        try:
            hora = time.fromisoformat(crudo)
        except ValueError:
            return _responder_error(f"Hora inválida en {campo.replace('_', ' ')}.")
        cambios.append((campo, tipo_ajuste, datetime.combine(fecha, hora, tzinfo=tz)))
    if not cambios:
        return _responder_error("Captura al menos un horario a corregir.")

    try:
        with transaction.atomic():
            for campo, tipo_ajuste, valor in cambios:
                ajuste = crear_ajuste_asistencia(
                    empleado,
                    fecha,
                    tipo_ajuste,
                    {campo: valor.isoformat()},
                    motivo,
                    request.user,
                )
                aprobar_ajuste_asistencia(
                    ajuste, request.user, comentario="Corrección desde pantalla de asistencias"
                )
            programar_sincronizacion_bonos_desde_checador(empleado.id, fecha)
    except ValidationError as exc:
        return _responder_error("; ".join(exc.messages))

    success = f"Asistencia de {empleado.nombre} del {fecha.isoformat()} guardada."
    redirect_url = _redirect_asistencias(request, fragment=f"asistencia-{empleado.id}-{fecha.isoformat()}")
    if progressive:
        return JsonResponse(
            {
                "ok": True,
                "toast": {"type": "success", "message": success},
                "redirect": redirect_url,
                "reload": True,
            }
        )
    messages.success(request, success)
    return redirect(redirect_url)
