from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.core.exceptions import PermissionDenied
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_date
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie

from core.access import can_manage_submodule, can_view_module, can_view_submodule, is_bonos_produccion_capture_only

from .empleados import bonos_produccion_elegibles_queryset
from .models import AREA_PRODUCCION, AREAS_PRODUCCION, BonoProduccionEmpleado, ConfigBonoArea, ConfigBonoPeriodo
from .services_checador import sincronizar_asistencia_desde_checador


AREA_AMOUNT_FIELDS = {
    "HORNOS": "monto_hornos",
    "PRODUCCION": "monto_area_produccion",
    "ARMADO": "monto_armado",
    "LOGISTICA": "monto_logistica",
    "CRUCERO": "monto_crucero",
}


def _parse_int(raw: str | None, default: int = 0) -> int:
    try:
        return int(raw or default)
    except (TypeError, ValueError):
        return default


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _dashboard_redirect(mes: int, anio: int):
    return redirect(f"{reverse('bonos_produccion:bonos-produccion-dashboard')}?mes={mes}&anio={anio}")


@login_required
@never_cache
def bonos_produccion_dashboard(request):
    if not can_view_submodule(request.user, "produccion", "bonos"):
        raise PermissionDenied

    today = timezone.localdate()
    mes = _parse_int(request.GET.get("mes"), today.month)
    anio = _parse_int(request.GET.get("anio"), today.year)
    can_manage = can_manage_submodule(request.user, "produccion", "bonos")

    if request.method == "POST":
        if not can_manage:
            raise PermissionDenied
        action = request.POST.get("action")
        mes = _parse_int(request.POST.get("mes"), mes)
        anio = _parse_int(request.POST.get("anio"), anio)

        if action == "config":
            periodo, _ = ConfigBonoPeriodo.objects.get_or_create(mes=mes, anio=anio)
            periodo.dias_laborables = _parse_int(request.POST.get("dias_laborables"), periodo.dias_laborables)
            for area, amount_field in AREA_AMOUNT_FIELDS.items():
                setattr(periodo, amount_field, _parse_decimal(request.POST.get(amount_field)))
            periodo.premio_embetunado = _parse_decimal(request.POST.get("premio_embetunado"))
            fecha_inicio = parse_date(request.POST.get("fecha_inicio") or "")
            if fecha_inicio is not None:
                periodo.fecha_inicio = fecha_inicio
            fecha_fin = parse_date(request.POST.get("fecha_fin") or "")
            if fecha_fin is not None:
                periodo.fecha_fin = fecha_fin
            periodo.creado_por = periodo.creado_por or request.user
            periodo.save()

            periodo.asegurar_reglas_area()
            for area, _label in AREAS_PRODUCCION:
                regla = periodo.get_regla_area(area)
                prefix = f"regla_{area.lower()}"
                usa_produccion = request.POST.get(f"{prefix}_usa_produccion") == "on"
                regla.usa_produccion = usa_produccion
                regla.pct_produccion = _parse_decimal(request.POST.get(f"{prefix}_pct_produccion")) if usa_produccion else Decimal("0.00")
                regla.pct_asistencia = _parse_decimal(request.POST.get(f"{prefix}_pct_asistencia"))
                regla.pct_puntualidad = _parse_decimal(request.POST.get(f"{prefix}_pct_puntualidad"))
                regla.pct_uniforme = _parse_decimal(request.POST.get(f"{prefix}_pct_uniforme"))
                regla.cancela_por_asistencia = request.POST.get(f"{prefix}_cancela_por_asistencia") == "on"
                regla.limite_asistencia_cancelacion = _parse_int(
                    request.POST.get(f"{prefix}_limite_asistencia_cancelacion"), regla.limite_asistencia_cancelacion
                )
                regla.cancela_por_puntualidad = request.POST.get(f"{prefix}_cancela_por_puntualidad") == "on"
                regla.limite_retardos_cancelacion = _parse_int(
                    request.POST.get(f"{prefix}_limite_retardos_cancelacion"), regla.limite_retardos_cancelacion
                )
                regla.limite_produccion = (
                    _parse_int(request.POST.get(f"{prefix}_limite_produccion"), regla.limite_produccion)
                    if usa_produccion
                    else 0
                )
                regla.limite_asistencia = _parse_int(request.POST.get(f"{prefix}_limite_asistencia"), regla.limite_asistencia)
                regla.limite_puntualidad = _parse_int(request.POST.get(f"{prefix}_limite_puntualidad"), regla.limite_puntualidad)
                regla.limite_uniforme = _parse_int(request.POST.get(f"{prefix}_limite_uniforme"), regla.limite_uniforme)
                regla.save()
            periodo.recalcular_todos()
            messages.success(request, "Configuración de bonos producción guardada.")
            return _dashboard_redirect(mes, anio)

        periodo = get_object_or_404(ConfigBonoPeriodo, mes=mes, anio=anio)
        if action == "recalcular":
            total = periodo.recalcular_todos()
            messages.success(request, f"Bonos recalculados: {total}.")
            return _dashboard_redirect(mes, anio)

        if action == "sync_checador":
            resultado = sincronizar_asistencia_desde_checador(periodo)
            messages.success(
                request,
                "Checador sincronizado: "
                f"{resultado['bonos_sincronizados']} bonos, "
                f"{resultado['registros_creados']} registros creados, "
                f"{resultado['registros_actualizados']} actualizados, "
                f"{resultado['bonos_omitidos']} omitidos.",
            )
            return _dashboard_redirect(mes, anio)

        if action == "ajuste_bono":
            bono = get_object_or_404(BonoProduccionEmpleado, pk=request.POST.get("bono_id"), periodo=periodo)
            bono.dias_trabajados = _parse_int(request.POST.get("dias_trabajados"), bono.dias_trabajados)
            bono.dias_uniforme = _parse_int(request.POST.get("dias_uniforme"), bono.dias_uniforme)
            bono.dias_asistencia = _parse_int(request.POST.get("dias_asistencia"), bono.dias_asistencia)
            bono.dias_puntualidad = _parse_int(request.POST.get("dias_puntualidad"), bono.dias_puntualidad)
            bono.dias_produccion = _parse_int(request.POST.get("dias_produccion"), bono.dias_produccion)
            if bono.area == AREA_PRODUCCION:
                bono.total_embetunados = _parse_int(request.POST.get("total_embetunados"), bono.total_embetunados)
            bono.ajuste_positivo = _parse_decimal(request.POST.get("ajuste_positivo"))
            bono.ajuste_negativo = _parse_decimal(request.POST.get("ajuste_negativo"))
            bono.bono_extra = _parse_decimal(request.POST.get("bono_extra"))
            bono.desc_ajuste_positivo = (request.POST.get("desc_ajuste_positivo") or "").strip()[:200]
            bono.desc_ajuste_negativo = (request.POST.get("desc_ajuste_negativo") or "").strip()[:200]
            bono.desc_bono_extra = (request.POST.get("desc_bono_extra") or "").strip()[:200]
            bono.observaciones = (request.POST.get("observaciones") or "").strip()
            bono.save()
            periodo.recalcular_todos()
            messages.success(request, f"Ajuste guardado para {bono.empleado.nombre}.")
            return _dashboard_redirect(mes, anio)

    periodo = ConfigBonoPeriodo.objects.filter(mes=mes, anio=anio).first()
    if periodo:
        periodo.asegurar_reglas_area()
    bonos = list(
        bonos_produccion_elegibles_queryset(
            BonoProduccionEmpleado.objects.filter(periodo=periodo)
        ).select_related("empleado").order_by("area", "empleado__nombre")
        if periodo
        else []
    )
    area_labels = dict(AREAS_PRODUCCION)
    area_rows = []
    for area, label in AREAS_PRODUCCION:
        rows = [bono for bono in bonos if bono.area == area]
        regla = periodo.get_regla_area(area) if periodo else None
        rule_values = {
            "pct_produccion": regla.pct_produccion,
            "pct_asistencia": regla.pct_asistencia,
            "pct_puntualidad": regla.pct_puntualidad,
            "pct_uniforme": regla.pct_uniforme,
            "limite_produccion": regla.limite_produccion,
            "limite_asistencia": regla.limite_asistencia,
            "limite_puntualidad": regla.limite_puntualidad,
            "limite_uniforme": regla.limite_uniforme,
            "cancela_por_asistencia": regla.cancela_por_asistencia,
            "limite_asistencia_cancelacion": regla.limite_asistencia_cancelacion,
            "cancela_por_puntualidad": regla.cancela_por_puntualidad,
            "limite_retardos_cancelacion": regla.limite_retardos_cancelacion,
            "usa_produccion": regla.usa_produccion,
        } if regla else ConfigBonoArea.defaults_for_area(area)
        area_rows.append(
            {
                "area": area,
                "label": label,
                "count": len(rows),
                "total": sum((bono.total_a_pagar for bono in rows), Decimal("0")),
                "monto": periodo.get_monto_area(area) if periodo else Decimal("0"),
                "regla": regla,
                **rule_values,
                "prefix": f"regla_{area.lower()}",
            }
        )

    total_bonos = sum((bono.total_a_pagar for bono in bonos), Decimal("0"))
    passing = sum(1 for bono in bonos if bono.pasa_uniforme and bono.pasa_asistencia and bono.pasa_puntualidad and bono.pasa_produccion)

    # Estado visual del período basado en fecha_fin vs hoy (sin cambio en BD)
    periodo_estado = None
    if periodo:
        if periodo.fecha_fin:
            dias_diff = (periodo.fecha_fin - today).days
            if dias_diff < 0:
                periodo_estado = "cerrado"
            elif dias_diff == 0:
                periodo_estado = "cierra_hoy"
            else:
                periodo_estado = "activo"
        else:
            periodo_estado = "activo"

    return render(
        request,
        "bonos_produccion/dashboard.html",
        {
            "periodo": periodo,
            "periodo_estado": periodo_estado,
            "mes": mes,
            "anio": anio,
            "bonos": bonos,
            "area_rows": area_rows,
            "area_labels": area_labels,
            "total_bonos": total_bonos,
            "passing": passing,
            "can_manage": can_manage,
            "defaults": {
                "dias_laborables": 23,
                "monto_hornos": Decimal("1000.00"),
                "monto_area_produccion": Decimal("850.00"),
                "monto_armado": Decimal("850.00"),
                "monto_logistica": Decimal("850.00"),
                "monto_crucero": Decimal("950.00"),
                "premio_embetunado": Decimal("400.00"),
            },
        },
    )


@login_required
@never_cache
@ensure_csrf_cookie
def bonos_produccion_pwa(request):
    if not (is_bonos_produccion_capture_only(request.user) or can_view_submodule(request.user, "produccion", "bonos")):
        return redirect("/seguimiento/" if can_view_module(request.user, "seguimiento") else "/dashboard/")
    force_capture = (request.GET.get("captura") or "").strip().lower() in {"1", "true", "si", "sí"}
    user_agent = (request.headers.get("User-Agent") or "").lower()
    is_mobile = any(token in user_agent for token in ("iphone", "ipad", "android", "mobile"))
    if is_bonos_produccion_capture_only(request.user):
        force_capture = True
    if not force_capture and not is_mobile:
        return redirect("bonos_produccion:bonos-produccion-dashboard")
    return render(request, "bonos_produccion/index.html")


def _static_file_path(relative_path: str) -> str:
    path = finders.find(relative_path)
    if not path:
        raise Http404(f"Static file not found: {relative_path}")
    return path


@never_cache
def bonos_produccion_manifest(request):
    with open(_static_file_path("bonos_produccion/manifest.json"), encoding="utf-8") as manifest:
        return JsonResponse(json.load(manifest), content_type="application/manifest+json")


@never_cache
def bonos_produccion_sw(request):
    with open(_static_file_path("bonos_produccion/sw.js"), encoding="utf-8") as service_worker:
        return HttpResponse(service_worker.read(), content_type="application/javascript")
