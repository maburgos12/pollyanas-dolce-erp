from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from io import BytesIO

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.access import can_view_recetas
from pos_bridge.services.daily_inventory_close_service import DailyInventoryCloseService
from recetas.models import ConsolidadoNocturnoCEDIS, PlanProduccionItem, Receta
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


def _inventory_close_payload(fecha_operacion: date) -> dict:
    payload = DailyInventoryCloseService().build_close(fecha_operacion=fecha_operacion)
    branch_codes = [branch["code"] for branch in payload["branches"]]
    table_rows = []
    for row in payload["rows"]:
        table_rows.append(
            {
                **row,
                "stock_cells": [row["stocks"].get(code, Decimal("0.000")) for code in branch_codes],
            }
        )
    payload["table_rows"] = table_rows
    return payload


@login_required
def consolidado_cedis_revision(request):
    if not can_view_recetas(request.user):
        raise PermissionDenied
    fecha_operacion = _parse_date(request.GET.get("fecha"))
    active_tab = "inventario" if request.GET.get("tab") == "inventario" else "productos"
    resumen = ConsolidadoNocturnoCedisService().get_resumen(fecha_operacion=fecha_operacion) if active_tab == "productos" else {}
    inventario_cierre = _inventory_close_payload(fecha_operacion) if active_tab == "inventario" else None
    return render(
        request,
        "recetas/consolidado_cedis_revision.html",
        {
            **resumen,
            "active_tab": active_tab,
            "inventario_cierre": inventario_cierre,
            "fecha_prev": fecha_operacion - timedelta(days=1),
            "fecha_next": fecha_operacion + timedelta(days=1),
            "recetas_disponibles": Receta.objects.order_by("nombre").only("id", "nombre", "codigo_point")[:700]
            if active_tab == "productos"
            else [],
        },
    )


@login_required
def consolidado_cedis_inventario_cierre_export(request):
    if not can_view_recetas(request.user):
        raise PermissionDenied
    fecha_operacion = _parse_date(request.GET.get("fecha"))
    export_format = (request.GET.get("format") or "xlsx").strip().lower()
    service = DailyInventoryCloseService()
    payload = service.build_close(fecha_operacion=fecha_operacion)
    filename_base = f"inventario_final_cierre_{fecha_operacion.isoformat()}"

    if export_format == "pdf":
        response = HttpResponse(service.build_pdf_bytes(payload), content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="{filename_base}.pdf"'
        return response

    workbook = service.build_workbook(payload)
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename_base}.xlsx"'
    return response


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
    eliminar_item_id = request.POST.get("eliminar_item_id")
    agregar_receta_id = request.POST.get("agregar_receta_id")
    autorizar_todo = request.POST.get("autorizar_todo") == "1"
    now = timezone.now()
    items = list(PlanProduccionItem.objects.filter(plan=plan))

    if agregar_receta_id:
        receta = Receta.objects.filter(pk=agregar_receta_id).first()
        if not receta:
            messages.error(request, "No se encontró el producto para agregar.")
            return _redirect_revision(fecha_operacion)
        qty = _parse_decimal(request.POST.get("agregar_cantidad"), Decimal("0"))
        item, created = PlanProduccionItem.objects.get_or_create(
            plan=plan,
            receta=receta,
            defaults={
                "cantidad": qty,
                "cantidad_autorizada": qty,
                "cantidad_sugerida": qty,
                "notas": "AGREGADO_MANUAL_CONSOLIDADO_CEDIS",
                "metadata": {
                    "agregado_manual": True,
                    "agregado_en": now.isoformat(),
                    "agregado_por_id": request.user.id if request.user.is_authenticated else None,
                },
            },
        )
        if not created:
            item.cantidad = qty
            item.cantidad_autorizada = qty
            item.cantidad_sugerida = qty
            item_metadata = item.metadata if isinstance(item.metadata, dict) else {}
            item.metadata = {
                **item_metadata,
                "agregado_manual": True,
                "eliminado": False,
                "actualizado_en": now.isoformat(),
                "actualizado_por_id": request.user.id if request.user.is_authenticated else None,
            }
            item.save(update_fields=["cantidad", "cantidad_autorizada", "cantidad_sugerida", "metadata"])
        messages.success(request, "Producto agregado al plan.")
        return _redirect_revision(fecha_operacion)

    if eliminar_item_id:
        item = PlanProduccionItem.objects.filter(plan=plan, pk=eliminar_item_id).first()
        if not item:
            messages.error(request, "No se encontró el renglón para eliminar.")
            return _redirect_revision(fecha_operacion)
        item_metadata = item.metadata if isinstance(item.metadata, dict) else {}
        item.cantidad = Decimal("0")
        item.cantidad_autorizada = Decimal("0")
        item.metadata = {
            **item_metadata,
            "eliminado": True,
            "eliminado_en": now.isoformat(),
            "eliminado_por_id": request.user.id if request.user.is_authenticated else None,
        }
        item.save(update_fields=["cantidad", "cantidad_autorizada", "metadata"])
        messages.success(request, "Producto eliminado del plan.")
        return _redirect_revision(fecha_operacion)

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
