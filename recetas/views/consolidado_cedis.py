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
from recetas.models import ConsolidadoNocturnoCEDIS, PlanProduccionItem
from recetas.services.consolidado_service import ConsolidadoNocturnoCedisService
from recetas.tasks.consolidado_nocturno import consolidado_nocturno_cedis


def _redirect_revision(fecha_operacion: date):
    return redirect(f"/recetas/consolidado-cedis/?fecha={fecha_operacion.isoformat()}")


def _parse_date(value: str | None):
    if not value:
        return timezone.localdate()
    try:
        return date.fromisoformat(value)
    except ValueError:
        return timezone.localdate()


def _parse_decimal(value, default: Decimal) -> Decimal:
    try:
        return max(Decimal("0"), Decimal(str(value or "0")))
    except Exception:
        return default


def _autorizar_item(item: PlanProduccionItem, *, usuario, now) -> None:
    item_metadata = item.metadata if isinstance(item.metadata, dict) else {}
    item.metadata = {
        **item_metadata,
        "autorizado": True,
        "autorizado_en": now.isoformat(),
        "autorizado_por_id": usuario.id if usuario and usuario.is_authenticated else None,
    }
    item.save(update_fields=["cantidad", "cantidad_autorizada", "metadata"])


def _plan_completamente_autorizado(plan) -> bool:
    for item in PlanProduccionItem.objects.filter(plan=plan).only("metadata"):
        if not (isinstance(item.metadata, dict) and item.metadata.get("autorizado")):
            return False
    return True


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
    consolidado = ConsolidadoNocturnoCEDIS.objects.filter(fecha_operacion=fecha_operacion).first()
    if consolidado:
        messages.info(
            request,
            f"Ya existe consolidado CEDIS para {fecha_operacion:%Y-%m-%d}. No se recalculó desde la pantalla.",
        )
    else:
        consolidado_nocturno_cedis.delay(
            fecha_operacion=fecha_operacion.isoformat(),
            sincronizar_point=request.POST.get("sincronizar_point", "1") == "1",
            sincronizar_inventario_cedis=True,
            forzar_recalculo=False,
        )
        messages.success(
            request,
            f"Consolidado CEDIS para {fecha_operacion:%Y-%m-%d} encolado. Actualiza la pantalla en unos minutos.",
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
        return _redirect_revision(fecha_operacion)

    autorizar_item_id = request.POST.get("autorizar_item_id")
    autorizar_todo = request.POST.get("autorizar_todo") == "1"
    now = timezone.now()
    items = list(PlanProduccionItem.objects.filter(plan=plan))

    if autorizar_item_id:
        items = [item for item in items if str(item.id) == str(autorizar_item_id)]
        if not items:
            messages.error(request, "No se encontró el renglón del plan para autorizar.")
            return _redirect_revision(fecha_operacion)

    for item in items:
        raw_qty = request.POST.get(f"cantidad_autorizada_{item.id}")
        if raw_qty is None:
            continue
        item.cantidad_autorizada = _parse_decimal(raw_qty, item.cantidad_autorizada or item.cantidad)
        item.cantidad = item.cantidad_autorizada
        _autorizar_item(item, usuario=request.user, now=now)

    if autorizar_todo or _plan_completamente_autorizado(plan):
        plan.autorizado = True
        plan.autorizado_en = now
        plan.autorizado_por = request.user
        plan.save(update_fields=["autorizado", "autorizado_en", "autorizado_por", "actualizado_en"])
        messages.success(request, "Plan de producción autorizado completo.")
    elif autorizar_item_id:
        messages.success(request, "Renglón autorizado.")
    else:
        messages.success(request, "Cambios guardados.")
    return _redirect_revision(fecha_operacion)
