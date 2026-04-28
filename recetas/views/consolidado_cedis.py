from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.access import can_view_recetas
from recetas.models import PlanProduccionItem
from recetas.services.consolidado_service import ConsolidadoNocturnoCedisService


def _parse_date(value: str | None):
    if not value:
        return timezone.localdate()
    try:
        return date.fromisoformat(value)
    except ValueError:
        return timezone.localdate()


@login_required
def consolidado_cedis_revision(request):
    if not can_view_recetas(request.user):
        raise PermissionDenied
    fecha_operacion = _parse_date(request.GET.get("fecha"))
    resumen = ConsolidadoNocturnoCedisService().get_resumen(fecha_operacion=fecha_operacion)
    return render(
        request,
        "recetas/consolidado_cedis_revision.html",
        {
            **resumen,
            "fecha_prev": fecha_operacion - timedelta(days=1),
            "fecha_next": fecha_operacion + timedelta(days=1),
        },
    )


@login_required
@require_POST
def consolidado_cedis_generar(request):
    if not can_view_recetas(request.user):
        raise PermissionDenied
    fecha_operacion = _parse_date(request.POST.get("fecha_operacion"))
    consolidado = ConsolidadoNocturnoCedisService().consolidar(
        fecha_operacion=fecha_operacion,
        usuario=request.user,
        sincronizar_point=request.POST.get("sincronizar_point", "1") == "1",
    )
    messages.success(
        request,
        f"Consolidado CEDIS generado para {fecha_operacion:%Y-%m-%d}. Plan #{consolidado.plan_produccion_id or '-'} listo.",
    )
    return redirect(f"{request.POST.get('next') or '/recetas/consolidado-cedis/'}?fecha={fecha_operacion.isoformat()}")


@login_required
@require_POST
def consolidado_cedis_autorizar_plan(request):
    if not can_view_recetas(request.user):
        raise PermissionDenied
    fecha_operacion = _parse_date(request.POST.get("fecha_operacion"))
    resumen = ConsolidadoNocturnoCedisService().get_resumen(fecha_operacion=fecha_operacion)
    plan = resumen.get("plan")
    if plan is None:
        messages.error(request, "No existe plan de producción para autorizar.")
        return redirect(f"/recetas/consolidado-cedis/?fecha={fecha_operacion.isoformat()}")

    for item in PlanProduccionItem.objects.filter(plan=plan):
        raw_qty = request.POST.get(f"cantidad_autorizada_{item.id}")
        if raw_qty is None:
            continue
        try:
            qty = Decimal(str(raw_qty or "0"))
        except Exception:
            qty = item.cantidad_autorizada or item.cantidad
        item.cantidad_autorizada = max(Decimal("0"), qty)
        item.cantidad = item.cantidad_autorizada
        item.save(update_fields=["cantidad", "cantidad_autorizada"])
    plan.autorizado = True
    plan.autorizado_en = timezone.now()
    plan.autorizado_por = request.user
    plan.save(update_fields=["autorizado", "autorizado_en", "autorizado_por", "actualizado_en"])
    messages.success(request, "Plan de producción autorizado.")
    return redirect(f"/recetas/consolidado-cedis/?fecha={fecha_operacion.isoformat()}")
