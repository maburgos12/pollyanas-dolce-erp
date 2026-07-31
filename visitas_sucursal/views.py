from __future__ import annotations

import calendar
from datetime import date
from decimal import Decimal, InvalidOperation
from uuid import UUID, uuid4

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Count, Q
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.access import can_manage_submodule, can_view_submodule
from core.models import Sucursal, sucursales_operativas_q
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla
from logistica.models import PuntoLogistico
from rrhh.models import Empleado

from .models import ChecklistVisita, FotoVisita, HallazgoVisita, VisitaSucursal
from .services import (
    AuditoriaVisitaError,
    crear_borrador_extraordinario,
    crear_checklist_base,
    ejecutar_auditoria,
    programaciones_pendientes,
)

MES_LABELS = [
    "",
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
]


def _require_visitas(user, *, manage=False):
    allowed = can_manage_submodule(user, "ventas", "visitas_sucursal") if manage else can_view_submodule(
        user, "ventas", "visitas_sucursal"
    )
    if not allowed:
        raise PermissionDenied("No tienes permisos para visitas a sucursal.")


def _active_users():
    return get_user_model().objects.filter(is_active=True).order_by("first_name", "last_name", "username")


def _sucursales_visitables_q():
    return sucursales_operativas_q() & ~Q(codigo__iexact="CEDIS") & ~Q(nombre__iexact="CEDIS")


def _sucursales_visitables():
    return Sucursal.objects.filter(_sucursales_visitables_q())


def _sucursal_es_visitable(sucursal):
    return bool(sucursal and _sucursales_visitables().filter(pk=sucursal.pk).exists())


def _visitas_visitables():
    return VisitaSucursal.objects.filter(sucursal__in=_sucursales_visitables())


def _parse_date(value):
    try:
        return date.fromisoformat(value) if value else None
    except ValueError:
        return None


def _parse_decimal(value):
    try:
        return Decimal(str(value)) if value not in (None, "") else None
    except (InvalidOperation, ValueError):
        return None


def _wants_progressive_response(request):
    return "application/json" in request.headers.get("Accept", "")


def _action_json(*, ok, message, redirect_url=None, status=200):
    payload = {
        "ok": ok,
        "toast": {
            "type": "success" if ok else "error",
            "message": message,
            "persistent": not ok,
        },
    }
    if redirect_url:
        payload["redirect"] = redirect_url
    return JsonResponse(payload, status=status)


def _crear_checklist_base(visita):
    crear_checklist_base(visita)


def _sucursal_usuario(user):
    profile = getattr(user, "userprofile", None)
    return getattr(profile, "sucursal", None)


def _visitas_de_sucursal(sucursal):
    if not _sucursal_es_visitable(sucursal):
        return VisitaSucursal.objects.none()
    return (
        VisitaSucursal.objects.filter(sucursal=sucursal)
        .select_related("sucursal", "responsable", "auditor")
        .order_by("-fecha_programada", "-id")
    )


def _visitas_app(user, sucursal, manage):
    if manage:
        return (
            _visitas_visitables()
            .select_related("sucursal", "responsable", "auditor")
            .order_by("-fecha_programada", "sucursal__nombre", "-id")
        )
    return _visitas_de_sucursal(sucursal)


def _empleados_de_sucursal(sucursal):
    if not sucursal:
        return Empleado.objects.none()
    return Empleado.objects.filter(
        Q(sucursal__iexact=sucursal.nombre) | Q(sucursal__iexact=sucursal.codigo),
        activo=True,
    ).order_by("nombre")


def _punto_logistico_sucursal(sucursal):
    if not sucursal:
        return None
    return (
        PuntoLogistico.objects.filter(
            sucursal=sucursal,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            activo=True,
        )
        .order_by("id")
        .first()
    )


def _month_params(request):
    hoy = timezone.localdate()
    try:
        year = int(request.GET.get("anio") or request.POST.get("anio") or hoy.year)
        month = int(request.GET.get("mes") or request.POST.get("mes") or hoy.month)
        first_day = date(year, month, 1)
    except (TypeError, ValueError):
        first_day = date(hoy.year, hoy.month, 1)
    last_day = date(first_day.year, first_day.month, calendar.monthrange(first_day.year, first_day.month)[1])
    return first_day, last_day


def _month_shift(first_day, offset):
    month = first_day.month + offset
    year = first_day.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    return {"anio": year, "mes": month}


@login_required
def lista_visitas(request):
    _require_visitas(request.user)
    first_day, last_day = _month_params(request)
    can_manage = can_manage_submodule(request.user, "ventas", "visitas_sucursal")

    if request.method == "POST":
        _require_visitas(request.user, manage=True)
        fecha = _parse_date(request.POST.get("fecha_programada"))
        sucursal_id = request.POST.get("sucursal")
        sucursal = _sucursales_visitables().filter(pk=sucursal_id).first() if sucursal_id else None
        if not fecha or not sucursal:
            messages.error(request, "Selecciona sucursal y fecha para programar.")
        else:
            visita = VisitaSucursal.objects.filter(sucursal=sucursal, fecha_programada=fecha).first()
            if visita:
                messages.info(request, "La sucursal ya tiene visita programada ese día.")
            else:
                with transaction.atomic():
                    visita = VisitaSucursal.objects.create(
                        sucursal=sucursal,
                        fecha_programada=fecha,
                        tipo=VisitaSucursal.TIPO_QUINCENAL,
                        creado_por=request.user,
                    )
                    _crear_checklist_base(visita)
                messages.success(request, "Visita programada en el cronograma.")
        return redirect(f"{request.path}?anio={first_day.year}&mes={first_day.month}")

    hoy = timezone.localdate()
    sucursales = list(_sucursales_visitables().order_by("nombre"))
    plan_mes = list(
        _visitas_visitables()
        .exclude(tipo=VisitaSucursal.TIPO_EXTRAORDINARIA)
        .filter(fecha_programada__range=(first_day, last_day))
        .select_related("sucursal", "responsable", "auditor")
        .annotate(
            hallazgos_abiertos=Count(
                "hallazgos",
                filter=Q(
                    hallazgos__estatus__in=[
                        HallazgoVisita.ESTATUS_ABIERTO,
                        HallazgoVisita.ESTATUS_EN_PROCESO,
                    ]
                ),
            )
        )
        .order_by("fecha_programada", "sucursal__nombre", "id")
    )
    ejecuciones_mes = list(
        _visitas_visitables()
        .filter(
            estatus=VisitaSucursal.ESTATUS_REALIZADA,
            fecha_real__range=(first_day, last_day),
        )
        .select_related("sucursal", "responsable", "auditor")
        .order_by("fecha_real", "sucursal__nombre", "id")
    )
    plan_by_cell = {}
    for visita in plan_mes:
        plan_by_cell.setdefault((visita.sucursal_id, visita.fecha_programada.day), []).append(visita)
    ejecuciones_by_cell = {}
    for visita in ejecuciones_mes:
        ejecuciones_by_cell.setdefault((visita.sucursal_id, visita.fecha_real.day), []).append(visita)

    total_programadas = len(plan_mes)
    total_realizadas = sum(1 for visita in plan_mes if visita.estatus == VisitaSucursal.ESTATUS_REALIZADA)
    extraordinarias_mes = sum(
        1 for visita in ejecuciones_mes if visita.tipo == VisitaSucursal.TIPO_EXTRAORDINARIA
    )
    days = [
        {"day": day, "date": date(first_day.year, first_day.month, day), "is_sunday": date(first_day.year, first_day.month, day).weekday() == 6}
        for day in range(1, last_day.day + 1)
    ]
    month_weeks = calendar.Calendar().monthdatescalendar(first_day.year, first_day.month)
    rows = []
    for sucursal in sucursales:
        row_visits = 0
        cells = []
        for day in days:
            planeadas = plan_by_cell.get((sucursal.id, day["day"]), [])
            ejecutadas = ejecuciones_by_cell.get((sucursal.id, day["day"]), [])
            visita = planeadas[0] if planeadas else (ejecutadas[0] if ejecutadas else None)
            row_visits += len(planeadas)
            cells.append(
                {
                    "day": day,
                    "visita": visita,
                    "planeadas": planeadas,
                    "ejecutadas": ejecutadas,
                    "count": len({item.pk for item in planeadas + ejecutadas}),
                }
            )
        weeks = [
            [
                {
                    "pad": cell_date.month != first_day.month,
                    "date": cell_date,
                    "planeadas": (
                        plan_by_cell.get((sucursal.id, cell_date.day), [])
                        if cell_date.month == first_day.month else []
                    ),
                    "ejecutadas": (
                        ejecuciones_by_cell.get((sucursal.id, cell_date.day), [])
                        if cell_date.month == first_day.month else []
                    ),
                    "visita": (
                        (plan_by_cell.get((sucursal.id, cell_date.day), [])
                         or ejecuciones_by_cell.get((sucursal.id, cell_date.day), [])
                         or [None])[0]
                        if cell_date.month == first_day.month else None
                    ),
                    "es_hoy": cell_date == hoy,
                }
                for cell_date in week
            ]
            for week in month_weeks
        ]
        rows.append(
            {
                "sucursal": sucursal,
                "cells": cells,
                "weeks": weeks,
                "avance": round((row_visits / total_programadas) * 100) if total_programadas else 0,
                "visitas_count": row_visits,
            }
        )

    vista_movil = request.GET.get("vista") or "dia"
    if vista_movil not in ("dia", "sucursal"):
        vista_movil = "dia"
    actividad_por_dia = {}
    for visita in plan_mes:
        actividad_por_dia.setdefault(visita.fecha_programada, {"planeadas": [], "ejecutadas": []})["planeadas"].append(visita)
    for visita in ejecuciones_mes:
        actividad_por_dia.setdefault(visita.fecha_real, {"planeadas": [], "ejecutadas": []})["ejecutadas"].append(visita)
    agenda_dias = [
        {
            "fecha": fecha,
            "planeadas": items["planeadas"],
            "ejecutadas": items["ejecutadas"],
            "visitas": list({visita.pk: visita for visita in items["planeadas"] + items["ejecutadas"]}.values()),
            "es_hoy": fecha == hoy,
            "vencida": fecha < hoy and any(
                v.estatus == VisitaSucursal.ESTATUS_PROGRAMADA for v in items["planeadas"]
            ),
        }
        for fecha, items in sorted(actividad_por_dia.items())
    ]

    daily_estimated = []
    daily_real = []
    for day in days:
        visitas_planeadas_dia = [
            visita for (_sucursal_id, cell_day), visitas in plan_by_cell.items()
            if cell_day == day["day"] for visita in visitas
        ]
        visitas_ejecutadas_dia = [
            visita for (_sucursal_id, cell_day), visitas in ejecuciones_by_cell.items()
            if cell_day == day["day"] for visita in visitas
            if visita.tipo != VisitaSucursal.TIPO_EXTRAORDINARIA
        ]
        daily_estimated.append(round((len(visitas_planeadas_dia) / total_programadas) * 100) if total_programadas else 0)
        daily_real.append(
            round((len(visitas_ejecutadas_dia) / total_programadas) * 100)
            if total_programadas
            else 0
        )

    context = {
        "days": days,
        "rows": rows,
        "month_label": MES_LABELS[first_day.month],
        "year": first_day.year,
        "month": first_day.month,
        "prev_month": _month_shift(first_day, -1),
        "next_month": _month_shift(first_day, 1),
        "can_manage": can_manage,
        "programadas": total_programadas,
        "vencidas": sum(
            1 for visita in plan_mes
            if visita.estatus == VisitaSucursal.ESTATUS_PROGRAMADA and visita.fecha_programada < hoy
        ),
        "hallazgos_abiertos": HallazgoVisita.objects.filter(
            visita__sucursal__in=_sucursales_visitables(),
            estatus__in=[HallazgoVisita.ESTATUS_ABIERTO, HallazgoVisita.ESTATUS_EN_PROCESO]
        ).count(),
        "vista_movil": vista_movil,
        "agenda_dias": agenda_dias,
        "hoy": hoy,
        "avance_estimado": 100 if total_programadas else 0,
        "avance_real": round((total_realizadas / total_programadas) * 100) if total_programadas else 0,
        "total_plan_mes": total_programadas,
        "total_ejecutadas_mes": total_realizadas,
        "extraordinarias_mes": extraordinarias_mes,
        "daily_estimated": daily_estimated,
        "daily_real": daily_real,
    }
    return render(request, "visitas_sucursal/lista.html", context)


@login_required
def nueva_visita(request):
    _require_visitas(request.user, manage=True)
    sucursales = _sucursales_visitables().order_by("nombre")
    users = _active_users()
    tipo_choices = [
        choice for choice in VisitaSucursal.TIPO_CHOICES
        if choice[0] != VisitaSucursal.TIPO_EXTRAORDINARIA
    ]
    tipos_programables = {value for value, _label in tipo_choices}
    if request.method == "POST":
        sucursal_id = request.POST.get("sucursal")
        fecha = _parse_date(request.POST.get("fecha_programada"))
        tipo = request.POST.get("tipo") or VisitaSucursal.TIPO_QUINCENAL
        if tipo not in tipos_programables:
            tipo = VisitaSucursal.TIPO_QUINCENAL
        sucursal = _sucursales_visitables().filter(pk=sucursal_id).first() if sucursal_id else None
        if not sucursal or not fecha:
            messages.error(request, "Selecciona sucursal y fecha programada.")
        else:
            with transaction.atomic():
                visita = VisitaSucursal.objects.create(
                    sucursal=sucursal,
                    fecha_programada=fecha,
                    tipo=tipo,
                    responsable_id=request.POST.get("responsable") or None,
                    auditor_id=request.POST.get("auditor") or None,
                    observaciones=(request.POST.get("observaciones") or "").strip(),
                    creado_por=request.user,
                )
                _crear_checklist_base(visita)
            messages.success(request, "Visita creada con checklist base.")
            return redirect("visitas_sucursal:detalle", pk=visita.pk)
    preset_sucursal = request.GET.get("sucursal") or ""
    preset_fecha = _parse_date(request.GET.get("fecha")) or timezone.localdate()
    return render(
        request,
        "visitas_sucursal/nueva.html",
        {
            "sucursales": sucursales,
            "users": users,
            "tipo_choices": tipo_choices,
            "today": preset_fecha,
            "preset_sucursal": preset_sucursal,
        },
    )


@login_required
def app_visitas_sucursal(request):
    _require_visitas(request.user)
    base_can_manage = can_manage_submodule(request.user, "ventas", "visitas_sucursal")
    is_superuser = bool(request.user.is_superuser)
    app_mode = request.GET.get("modo") if is_superuser else ""
    preview_read_only = False
    preview_sucursal = None
    can_manage = base_can_manage
    sucursal = _sucursal_usuario(request.user)
    if is_superuser and app_mode == "sucursal":
        preview_sucursal = (
            _sucursales_visitables().filter(pk=request.GET.get("sucursal")).first()
            or _sucursales_visitables().order_by("nombre").first()
        )
        sucursal = preview_sucursal
        can_manage = False
        preview_read_only = True
    else:
        app_mode = "auditor" if base_can_manage else "sucursal"
    if not sucursal and not can_manage:
        raise PermissionDenied("Tu usuario no tiene sucursal de venta asignada.")
    if not can_manage:
        preview_read_only = True

    filtro_sucursal = None
    if can_manage:
        suc_param = request.POST.get("sucursal") or request.GET.get("sucursal") or ""
        if suc_param.isdigit():
            filtro_sucursal = _sucursales_visitables().filter(pk=suc_param).first()

    if can_manage and filtro_sucursal:
        pendientes = programaciones_pendientes(filtro_sucursal)
    else:
        pendientes = VisitaSucursal.objects.none()

    visitas = _visitas_app(request.user, sucursal, can_manage)
    if can_manage:
        visitas = visitas.filter(sucursal=filtro_sucursal) if filtro_sucursal else visitas.none()
    visita_id = request.POST.get("visita_id") or request.GET.get("visita")
    visita = visitas.filter(pk=visita_id).first() if visita_id else None

    if can_manage and filtro_sucursal and not visita_id:
        visita = pendientes.first()

    if can_manage and visita_id and not visita:
        visita = _visitas_visitables().filter(pk=visita_id).first()
        if visita and filtro_sucursal and visita.sucursal_id != filtro_sucursal.id:
            visita = None
    elif not can_manage and not visita:
        visita = visitas.first()

    if request.method == "POST":
        if preview_read_only:
            messages.info(request, "Vista de sucursal: las capturas están bloqueadas.")
            return redirect(request.get_full_path())
        action = request.POST.get("action")
        if action == "iniciar_extraordinaria":
            if not filtro_sucursal:
                if _wants_progressive_response(request):
                    return _action_json(ok=False, message="Selecciona la sucursal que vas a auditar.", status=400)
                messages.error(request, "Selecciona la sucursal que vas a auditar.")
                return redirect(request.path)
            try:
                token = UUID(request.POST.get("clave_idempotencia") or "")
                visita = crear_borrador_extraordinario(
                    user=request.user,
                    sucursal=filtro_sucursal,
                    motivo=request.POST.get("motivo_extraordinaria") or "",
                    detalle=request.POST.get("detalle_extraordinaria") or "",
                    clave_idempotencia=token,
                )
            except (AuditoriaVisitaError, ValidationError, ValueError) as exc:
                error_message = f"No se pudo iniciar la extraordinaria: {exc}"
                if _wants_progressive_response(request):
                    return _action_json(ok=False, message=error_message, status=400)
                messages.error(request, error_message)
                return redirect(f"{request.path}?sucursal={filtro_sucursal.pk}")
            success_message = "Auditoría extraordinaria preparada. Captura el checklist y valida tu ubicación."
            redirect_url = f"{request.path}?sucursal={filtro_sucursal.pk}&visita={visita.pk}"
            if _wants_progressive_response(request):
                return _action_json(ok=True, message=success_message, redirect_url=redirect_url)
            messages.success(request, success_message)
            return redirect(redirect_url)

        visita_base = _visitas_visitables().filter(pk=request.POST.get("visita_id")).first()
        if not visita_base:
            if _wants_progressive_response(request):
                return _action_json(ok=False, message="Selecciona una visita pendiente para ejecutar.", status=400)
            messages.error(request, "Selecciona una visita pendiente para ejecutar.")
            return redirect(request.path)
        filtro_sucursal = filtro_sucursal or visita_base.sucursal
        respuestas = {
            item.id: (
                request.POST.get(f"respuesta_{item.id}") or item.respuesta,
                request.POST.get(f"observaciones_{item.id}") or "",
            )
            for item in visita_base.checklist.all()
        }
        try:
            with transaction.atomic():
                visita = ejecutar_auditoria(
                    visita_id=visita_base.pk,
                    sucursal=filtro_sucursal,
                    user=request.user,
                    latitud=_parse_decimal(request.POST.get("gps_latitud")),
                    longitud=_parse_decimal(request.POST.get("gps_longitud")),
                    precision_m=_parse_decimal(request.POST.get("gps_precision_m")),
                    respuestas=respuestas,
                    observaciones=request.POST.get("observaciones") or "",
                    personal_ids=request.POST.getlist("personal_presente"),
                )
                for foto in request.FILES.getlist("fotos"):
                    FotoVisita.objects.create(visita=visita, foto=foto, creado_por=request.user)
        except AuditoriaVisitaError as exc:
            if _wants_progressive_response(request):
                return _action_json(ok=False, message=str(exc), status=422)
            messages.error(request, str(exc))
            return redirect(f"{request.path}?sucursal={filtro_sucursal.pk}&visita={visita_base.pk}")
        success_message = "Auditoría ejecutada y ubicación comprobada."
        redirect_url = f"{request.path}?sucursal={filtro_sucursal.pk}&visita={visita.pk}"
        if _wants_progressive_response(request):
            return _action_json(ok=True, message=success_message, redirect_url=redirect_url)
        messages.success(request, success_message)
        return redirect(redirect_url)

    return render(
        request,
        "visitas_sucursal/app.html",
        {
            "sucursal": sucursal,
            "visita": visita,
            "visitas": pendientes[:12] if can_manage else visitas[:12],
            "programaciones_pendientes": pendientes,
            "checklist": visita.checklist.all() if visita else [],
            "empleados_sucursal": _empleados_de_sucursal(visita.sucursal) if visita and can_manage else [],
            "punto_logistico": _punto_logistico_sucursal(visita.sucursal) if visita and can_manage else None,
            "respuesta_choices": ChecklistVisita.RESPUESTA_CHOICES,
            "can_manage": can_manage,
            "app_mode": app_mode,
            "preview_read_only": preview_read_only,
            "preview_sucursal": preview_sucursal,
            "filtro_sucursal": filtro_sucursal,
            "sucursales_preview": _sucursales_visitables().order_by("nombre") if is_superuser else [],
            "sucursales_auditoria": _sucursales_visitables().order_by("nombre") if can_manage else [],
            "motivo_extraordinaria_choices": VisitaSucursal.MOTIVO_EXTRAORDINARIA_CHOICES,
            "clave_extraordinaria": uuid4(),
            "visita_ejecutable": bool(
                visita
                and visita.estatus in (
                    VisitaSucursal.ESTATUS_PROGRAMADA,
                    VisitaSucursal.ESTATUS_BORRADOR,
                )
            ),
            "is_superuser": is_superuser,
        },
    )


@login_required
def detalle_visita(request, pk):
    _require_visitas(request.user)
    visita = get_object_or_404(
        VisitaSucursal.objects.select_related("sucursal", "responsable", "auditor"),
        pk=pk,
    )
    if request.method == "POST":
        _require_visitas(request.user, manage=True)
        action = request.POST.get("action") or ""
        if action == "reprogramar":
            nueva_fecha = _parse_date(request.POST.get("nueva_fecha"))
            if visita.estatus != VisitaSucursal.ESTATUS_PROGRAMADA:
                messages.error(request, "Solo se pueden reprogramar visitas en estatus Programada.")
            elif not nueva_fecha:
                messages.error(request, "Selecciona una fecha válida para reprogramar.")
            elif (
                VisitaSucursal.objects.filter(sucursal=visita.sucursal, fecha_programada=nueva_fecha)
                .exclude(pk=visita.pk)
                .exists()
            ):
                messages.error(request, "La sucursal ya tiene visita programada ese día.")
            else:
                visita.fecha_programada = nueva_fecha
                visita.save(update_fields=["fecha_programada", "actualizado_en"])
                messages.success(request, f"Visita reprogramada al {nueva_fecha:%d/%m/%Y}.")
            return redirect("visitas_sucursal:detalle", pk=visita.pk)
        if action == "cancelar":
            if visita.estatus != VisitaSucursal.ESTATUS_PROGRAMADA:
                messages.error(request, "Solo se pueden cancelar visitas en estatus Programada.")
            else:
                visita.estatus = VisitaSucursal.ESTATUS_CANCELADA
                visita.save(update_fields=["estatus", "actualizado_en"])
                messages.success(request, "Visita cancelada. Sigue visible en el cronograma como C.")
            return redirect("visitas_sucursal:detalle", pk=visita.pk)
        if action == "eliminar":
            if visita.estatus == VisitaSucursal.ESTATUS_REALIZADA:
                messages.error(request, "No se puede eliminar una visita ya realizada.")
                return redirect("visitas_sucursal:detalle", pk=visita.pk)
            fecha = visita.fecha_programada or visita.fecha_real or timezone.localdate()
            visita.delete()
            messages.success(request, "Visita eliminada del cronograma.")
            return redirect(f"{reverse('visitas_sucursal:lista')}?anio={fecha.year}&mes={fecha.month}")
        visita.observaciones = (request.POST.get("observaciones") or "").strip()
        visita.save(update_fields=["observaciones", "actualizado_en"])

        for item in visita.checklist.all():
            item.respuesta = request.POST.get(f"respuesta_{item.id}") or item.respuesta
            item.observaciones = (request.POST.get(f"observaciones_{item.id}") or "").strip()
            item.save(update_fields=["respuesta", "observaciones"])

        hallazgo_descripcion = (request.POST.get("hallazgo_descripcion") or "").strip()
        if hallazgo_descripcion:
            HallazgoVisita.objects.create(
                visita=visita,
                categoria=(request.POST.get("hallazgo_categoria") or "General").strip(),
                descripcion=hallazgo_descripcion,
                accion_correctiva=(request.POST.get("hallazgo_accion") or "").strip(),
                responsable=(request.POST.get("hallazgo_responsable") or "").strip(),
                fecha_compromiso=_parse_date(request.POST.get("hallazgo_fecha_compromiso")),
                prioridad=request.POST.get("hallazgo_prioridad") or HallazgoVisita.PRIORIDAD_MEDIA,
                requiere_falla=bool(request.POST.get("hallazgo_requiere_falla")),
                creado_por=request.user,
            )
        messages.success(request, "Visita actualizada.")
        return redirect("visitas_sucursal:detalle", pk=visita.pk)

    checklist = visita.checklist.all()
    return render(
        request,
        "visitas_sucursal/detalle.html",
        {
            "visita": visita,
            "checklist": checklist,
            "hallazgos": visita.hallazgos.select_related("reporte_falla"),
            "estatus_choices": VisitaSucursal.ESTATUS_CHOICES,
            "respuesta_choices": ChecklistVisita.RESPUESTA_CHOICES,
            "prioridad_choices": HallazgoVisita.PRIORIDAD_CHOICES,
            "can_manage": can_manage_submodule(request.user, "ventas", "visitas_sucursal"),
        },
    )


@login_required
@require_POST
def convertir_hallazgo_falla(request, pk):
    _require_visitas(request.user, manage=True)
    hallazgo = get_object_or_404(HallazgoVisita.objects.select_related("visita__sucursal", "reporte_falla"), pk=pk)
    if hallazgo.reporte_falla_id:
        messages.info(request, "Este hallazgo ya tiene reporte de falla.")
        return redirect("visitas_sucursal:detalle", pk=hallazgo.visita_id)
    categoria, _created = CategoriaFalla.objects.get_or_create(
        nombre="Auditoría de sucursal",
        defaults={"tipo": CategoriaFalla.TIPO_OTRO, "activo": True, "orden": 99},
    )
    prioridad_map = {
        HallazgoVisita.PRIORIDAD_BAJA: ReporteFalla.PRIORIDAD_BAJA,
        HallazgoVisita.PRIORIDAD_MEDIA: ReporteFalla.PRIORIDAD_MEDIA,
        HallazgoVisita.PRIORIDAD_ALTA: ReporteFalla.PRIORIDAD_ALTA,
        HallazgoVisita.PRIORIDAD_CRITICA: ReporteFalla.PRIORIDAD_CRITICA,
    }
    reporte = ReporteFalla.objects.create(
        sucursal=hallazgo.visita.sucursal,
        categoria=categoria,
        area=ReporteFalla.AREA_VENTAS,
        titulo=f"Hallazgo visita: {hallazgo.categoria}",
        descripcion=(
            f"{hallazgo.descripcion}\n\n"
            f"Acción correctiva: {hallazgo.accion_correctiva or 'Pendiente'}\n"
            f"Origen: visita a sucursal #{hallazgo.visita_id}"
        ),
        prioridad=prioridad_map.get(hallazgo.prioridad, ReporteFalla.PRIORIDAD_MEDIA),
        reportado_por=request.user,
    )
    BitacoraFalla.objects.create(
        reporte=reporte,
        usuario=request.user,
        estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
        comentario=f"Creado desde hallazgo de visita #{hallazgo.visita_id}.",
    )
    hallazgo.reporte_falla = reporte
    hallazgo.requiere_falla = True
    hallazgo.save(update_fields=["reporte_falla", "requiere_falla", "actualizado_en"])
    messages.success(request, f"Reporte de falla #{reporte.id} creado desde el hallazgo.")
    return redirect("visitas_sucursal:detalle", pk=hallazgo.visita_id)
