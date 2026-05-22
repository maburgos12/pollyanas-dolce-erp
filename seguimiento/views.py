from __future__ import annotations

from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.audit import log_event

from .models import SeguimientoChecklistItem, SeguimientoComentario, SeguimientoEvidencia, SeguimientoItem
from .services import empleado_de_usuario


def _items_del_usuario(user):
    empleado = empleado_de_usuario(user)
    filters = Q(responsable_user=user)
    if empleado:
        filters |= Q(responsable_empleado=empleado)
    return (
        SeguimientoItem.objects.filter(filters)
        .select_related("responsable_user", "responsable_empleado")
        .prefetch_related("checklist", "comentarios", "evidencias")
        .distinct()
    )


def _get_item_para_usuario(user, pk):
    empleado = empleado_de_usuario(user)
    filters = Q(pk=pk, responsable_user=user)
    if empleado:
        filters |= Q(pk=pk, responsable_empleado=empleado)
    return get_object_or_404(SeguimientoItem.objects.filter(filters).distinct(), pk=pk)


@login_required
def mi_seguimiento(request):
    now = timezone.now()
    empleado = empleado_de_usuario(request.user)
    items = list(_items_del_usuario(request.user))

    for item in items:
        checks = list(item.checklist.all())
        actividad = [item.updated_at]
        actividad.extend(check.updated_at for check in checks if check.updated_at)
        actividad.extend(comentario.created_at for comentario in item.comentarios.all())
        actividad.extend(evidencia.created_at for evidencia in item.evidencias.all())

        item.checklist_total = len(checks)
        item.checklist_done = sum(1 for check in checks if check.completado)
        item.progreso_pct = round((item.checklist_done / item.checklist_total) * 100) if item.checklist_total else 0
        item.ultima_actividad = max(actividad) if actividad else item.updated_at
        item.origen_display = item.origen or "ERP"
        if item.esta_vencido:
            item.prioridad_label = "Vencido"
            item.prioridad_tone = "danger"
        elif item.fecha_limite and item.fecha_limite <= now + timedelta(days=2) and not item.esta_cerrado:
            item.prioridad_label = "Alta"
            item.prioridad_tone = "warn"
        elif item.estatus == SeguimientoItem.ESTATUS_EN_REVISION:
            item.prioridad_label = "Revisión"
            item.prioridad_tone = "warn"
        else:
            item.prioridad_label = "Normal"
            item.prioridad_tone = ""

    metrics = {
        "total": len(items),
        "abiertos": sum(1 for item in items if not item.esta_cerrado),
        "vencidos": sum(1 for item in items if item.esta_vencido),
        "en_revision": sum(1 for item in items if item.estatus == SeguimientoItem.ESTATUS_EN_REVISION),
        "completados": sum(1 for item in items if item.estatus == SeguimientoItem.ESTATUS_COMPLETADO),
    }
    by_type = SeguimientoItem.objects.filter(pk__in=[item.pk for item in items]).values("tipo").annotate(total=Count("id"))

    return render(
        request,
        "seguimiento/mi_seguimiento.html",
        {
            "empleado": empleado,
            "items": items,
            "metrics": metrics,
            "by_type": by_type,
            "estatus_en_revision": SeguimientoItem.ESTATUS_EN_REVISION,
        },
    )


@login_required
@require_POST
def toggle_checklist(request, pk, check_id):
    item = _get_item_para_usuario(request.user, pk)
    check = get_object_or_404(SeguimientoChecklistItem, pk=check_id, seguimiento=item)
    check.completado = not check.completado
    if check.completado:
        check.completado_por = request.user
        check.completado_at = timezone.now()
        if item.estatus == SeguimientoItem.ESTATUS_PENDIENTE:
            item.estatus = SeguimientoItem.ESTATUS_EN_PROCESO
            item.save(update_fields=["estatus", "updated_at"])
    else:
        check.completado_por = None
        check.completado_at = None
    check.save(update_fields=["completado", "completado_por", "completado_at", "updated_at"])
    log_event(request.user, "seguimiento.checklist", "SeguimientoChecklistItem", check.pk, {"seguimiento_id": item.pk})
    messages.success(request, "Checklist actualizado.")
    return redirect("seguimiento:mi_seguimiento")


@login_required
@require_POST
def registrar_feedback(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    comentario = (request.POST.get("comentario") or "").strip()
    if not comentario:
        messages.error(request, "Escribe la retroalimentación antes de enviarla.")
        return redirect("seguimiento:mi_seguimiento")
    SeguimientoComentario.objects.create(
        seguimiento=item,
        usuario=request.user,
        tipo=SeguimientoComentario.TIPO_FEEDBACK,
        comentario=comentario,
    )
    if item.estatus in {SeguimientoItem.ESTATUS_PENDIENTE, SeguimientoItem.ESTATUS_EN_PROCESO}:
        item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
        item.save(update_fields=["estatus", "updated_at"])
    log_event(request.user, "seguimiento.retroalimentacion", "SeguimientoItem", item.pk, {"comentario": True})
    messages.success(request, "Retroalimentación enviada para revisión.")
    return redirect("seguimiento:mi_seguimiento")


@login_required
@require_POST
def subir_evidencia(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    archivo = request.FILES.get("archivo")
    if not archivo:
        messages.error(request, "Selecciona un archivo de evidencia.")
        return redirect("seguimiento:mi_seguimiento")
    SeguimientoEvidencia.objects.create(
        seguimiento=item,
        usuario=request.user,
        archivo=archivo,
        nombre_original=archivo.name,
        comentario=(request.POST.get("comentario") or "").strip(),
    )
    if item.estatus in {SeguimientoItem.ESTATUS_PENDIENTE, SeguimientoItem.ESTATUS_EN_PROCESO}:
        item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
        item.save(update_fields=["estatus", "updated_at"])
    log_event(request.user, "seguimiento.evidencia", "SeguimientoItem", item.pk, {"archivo": archivo.name})
    messages.success(request, "Evidencia enviada para revisión.")
    return redirect("seguimiento:mi_seguimiento")
