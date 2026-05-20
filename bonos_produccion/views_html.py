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
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie

from core.access import can_manage_submodule, can_view_submodule

from .models import AREAS_PRODUCCION, BonoProduccionEmpleado, ConfigBonoPeriodo


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
            periodo.monto_hornos = _parse_decimal(request.POST.get("monto_hornos"))
            periodo.monto_area_produccion = _parse_decimal(request.POST.get("monto_area_produccion"))
            periodo.monto_armado = _parse_decimal(request.POST.get("monto_armado"))
            periodo.monto_logistica = _parse_decimal(request.POST.get("monto_logistica"))
            periodo.monto_crucero = _parse_decimal(request.POST.get("monto_crucero"))
            periodo.pct_produccion = _parse_decimal(request.POST.get("pct_produccion"))
            periodo.pct_asistencia = _parse_decimal(request.POST.get("pct_asistencia"))
            periodo.pct_puntualidad = _parse_decimal(request.POST.get("pct_puntualidad"))
            periodo.pct_uniforme = _parse_decimal(request.POST.get("pct_uniforme"))
            periodo.premio_embetunado = _parse_decimal(request.POST.get("premio_embetunado"))
            periodo.limite_uniforme = _parse_int(request.POST.get("limite_uniforme"), periodo.limite_uniforme)
            periodo.limite_asistencia = _parse_int(request.POST.get("limite_asistencia"), periodo.limite_asistencia)
            periodo.limite_puntualidad = _parse_int(request.POST.get("limite_puntualidad"), periodo.limite_puntualidad)
            periodo.limite_produccion = _parse_int(request.POST.get("limite_produccion"), periodo.limite_produccion)
            periodo.creado_por = periodo.creado_por or request.user
            periodo.save()
            messages.success(request, "Configuración de bonos producción guardada.")
            return _dashboard_redirect(mes, anio)

        periodo = get_object_or_404(ConfigBonoPeriodo, mes=mes, anio=anio)
        if action == "recalcular":
            total = 0
            for bono in periodo.bonos.select_related("empleado"):
                bono.recalcular()
                bono.save()
                total += 1
            messages.success(request, f"Bonos recalculados: {total}.")
            return _dashboard_redirect(mes, anio)

        if action == "ajuste_bono":
            bono = get_object_or_404(BonoProduccionEmpleado, pk=request.POST.get("bono_id"), periodo=periodo)
            bono.dias_trabajados = _parse_int(request.POST.get("dias_trabajados"), bono.dias_trabajados)
            bono.dias_uniforme = _parse_int(request.POST.get("dias_uniforme"), bono.dias_uniforme)
            bono.dias_asistencia = _parse_int(request.POST.get("dias_asistencia"), bono.dias_asistencia)
            bono.dias_puntualidad = _parse_int(request.POST.get("dias_puntualidad"), bono.dias_puntualidad)
            bono.dias_produccion = _parse_int(request.POST.get("dias_produccion"), bono.dias_produccion)
            bono.ajuste_positivo = _parse_decimal(request.POST.get("ajuste_positivo"))
            bono.ajuste_negativo = _parse_decimal(request.POST.get("ajuste_negativo"))
            bono.bono_extra = _parse_decimal(request.POST.get("bono_extra"))
            bono.desc_ajuste_positivo = (request.POST.get("desc_ajuste_positivo") or "").strip()[:200]
            bono.desc_ajuste_negativo = (request.POST.get("desc_ajuste_negativo") or "").strip()[:200]
            bono.desc_bono_extra = (request.POST.get("desc_bono_extra") or "").strip()[:200]
            bono.observaciones = (request.POST.get("observaciones") or "").strip()
            bono.recalcular()
            bono.save()
            messages.success(request, f"Ajuste guardado para {bono.empleado.nombre}.")
            return _dashboard_redirect(mes, anio)

    periodo = ConfigBonoPeriodo.objects.filter(mes=mes, anio=anio).first()
    bonos = list(
        BonoProduccionEmpleado.objects.filter(periodo=periodo).select_related("empleado").order_by("area", "empleado__nombre")
        if periodo
        else []
    )
    area_labels = dict(AREAS_PRODUCCION)
    area_rows = []
    for area, label in AREAS_PRODUCCION:
        rows = [bono for bono in bonos if bono.area == area]
        area_rows.append(
            {
                "area": area,
                "label": label,
                "count": len(rows),
                "total": sum((bono.total_a_pagar for bono in rows), Decimal("0")),
            }
        )

    total_bonos = sum((bono.total_a_pagar for bono in bonos), Decimal("0"))
    passing = sum(1 for bono in bonos if bono.pasa_uniforme and bono.pasa_asistencia and bono.pasa_puntualidad and bono.pasa_produccion)

    return render(
        request,
        "bonos_produccion/dashboard.html",
        {
            "periodo": periodo,
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
                "pct_produccion": Decimal("65.00"),
                "pct_asistencia": Decimal("15.00"),
                "pct_puntualidad": Decimal("15.00"),
                "pct_uniforme": Decimal("5.00"),
                "premio_embetunado": Decimal("400.00"),
                "limite_uniforme": 1,
                "limite_asistencia": 2,
                "limite_puntualidad": 2,
                "limite_produccion": 2,
            },
        },
    )


@login_required
@ensure_csrf_cookie
def bonos_produccion_pwa(request):
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
