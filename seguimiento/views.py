from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils.dateparse import parse_date
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.access import ROLE_DG, ROLE_ADMIN, has_any_role
from core.audit import log_event

from .models import (
    SeguimientoChecklistItem,
    SeguimientoComentario,
    SeguimientoEvidencia,
    SeguimientoItem,
    SeguimientoProrrogaSolicitud,
)
from .services import empleado_de_usuario


EVIDENCIA_ALLOWED_EXTENSIONS = {
    ".csv",
    ".doc",
    ".docx",
    ".gif",
    ".heic",
    ".heif",
    ".jpeg",
    ".jpg",
    ".ods",
    ".odt",
    ".pdf",
    ".png",
    ".rtf",
    ".tif",
    ".tiff",
    ".txt",
    ".webp",
    ".xls",
    ".xlsb",
    ".xlsm",
    ".xlsx",
}
EVIDENCIA_BLOCKED_CONTENT_TYPES = {
    "application/javascript",
    "image/svg+xml",
    "text/html",
    "text/javascript",
}
DEFAULT_EVIDENCIA_MAX_UPLOAD_BYTES = 15 * 1024 * 1024


def _items_del_usuario(user):
    empleado = empleado_de_usuario(user)
    filters = Q(responsable_user=user) | Q(participantes_user=user)
    if empleado:
        filters |= Q(responsable_empleado=empleado) | Q(participantes_empleado=empleado)
    return (
        SeguimientoItem.objects.filter(filters)
        .select_related("responsable_user", "responsable_empleado")
        .prefetch_related(
            "checklist",
            "comentarios",
            "evidencias",
            "prorrogas",
            "participantes_user",
            "participantes_empleado",
        )
        .distinct()
    )


def _get_item_para_usuario(user, pk):
    empleado = empleado_de_usuario(user)
    filters = Q(pk=pk, responsable_user=user) | Q(pk=pk, participantes_user=user)
    if empleado:
        filters |= Q(pk=pk, responsable_empleado=empleado) | Q(pk=pk, participantes_empleado=empleado)
    return get_object_or_404(SeguimientoItem.objects.filter(filters).distinct(), pk=pk)


def _validar_archivo_evidencia(archivo) -> str | None:
    max_bytes = int(getattr(settings, "SEGUIMIENTO_EVIDENCIA_MAX_UPLOAD_BYTES", DEFAULT_EVIDENCIA_MAX_UPLOAD_BYTES))
    if archivo.size and archivo.size > max_bytes:
        max_mb = max_bytes / (1024 * 1024)
        return f"El archivo excede el máximo permitido de {max_mb:g} MB."

    extension = Path(archivo.name or "").suffix.lower()
    if extension not in EVIDENCIA_ALLOWED_EXTENSIONS:
        return "Tipo de archivo no permitido para evidencia."

    content_type = (getattr(archivo, "content_type", "") or "").split(";", 1)[0].strip().lower()
    if content_type in EVIDENCIA_BLOCKED_CONTENT_TYPES:
        return "Tipo de archivo no permitido para evidencia."
    return None


@login_required
def mi_seguimiento(request, tipo: str | None = None):
    now = timezone.now()
    empleado = empleado_de_usuario(request.user)
    items = list(_items_del_usuario(request.user))
    tabs = [
        {
            "label": "Minutas",
            "url_name": "seguimiento:minutas",
            "tipo": SeguimientoItem.TIPO_MINUTA,
        },
        {
            "label": "Proyectos",
            "url_name": "seguimiento:proyectos",
            "tipo": SeguimientoItem.TIPO_PROYECTO,
        },
        {
            "label": "Compromisos",
            "url_name": "seguimiento:compromisos",
            "tipo": SeguimientoItem.TIPO_COMPROMISO,
        },
    ]

    for item in items:
        checks = list(item.checklist.all())
        actividad = [item.updated_at]
        actividad.extend(check.updated_at for check in checks if check.updated_at)
        actividad.extend(comentario.created_at for comentario in item.comentarios.all())
        actividad.extend(evidencia.created_at for evidencia in item.evidencias.all())
        actividad.extend(prorroga.created_at for prorroga in item.prorrogas.all())

        item.checklist_total = len(checks)
        item.checklist_done = sum(1 for check in checks if check.completado)
        item.progreso_pct = round((item.checklist_done / item.checklist_total) * 100) if item.checklist_total else 0
        item.ultima_actividad = max(actividad) if actividad else item.updated_at
        item.origen_display = item.origen or "ERP"
        item.es_compartido = item.participantes_user.exists() or item.participantes_empleado.exists()
        item.prorroga_pendiente = next(
            (
                prorroga
                for prorroga in item.prorrogas.all()
                if prorroga.estatus == SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE
            ),
            None,
        )
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
        "por_vencer_24h": sum(
            1
            for item in items
            if item.fecha_limite and now <= item.fecha_limite <= now + timedelta(hours=24) and not item.esta_cerrado
        ),
        "vencidos": sum(1 for item in items if item.esta_vencido),
        "en_revision": sum(1 for item in items if item.estatus == SeguimientoItem.ESTATUS_EN_REVISION),
        "prorrogas_pendientes": sum(
            1
            for item in items
            for prorroga in item.prorrogas.all()
            if prorroga.estatus == SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE
        ),
        "completados": sum(1 for item in items if item.estatus == SeguimientoItem.ESTATUS_COMPLETADO),
        "cumplidos_a_tiempo": sum(
            1
            for item in items
            if item.estatus == SeguimientoItem.ESTATUS_COMPLETADO
            and (not item.fecha_limite or (item.aprobado_at or item.ultima_actividad) <= item.fecha_limite)
        ),
    }
    section_config = [
        {
            "tipo": SeguimientoItem.TIPO_COMPROMISO,
            "title": "Compromisos",
            "subtitle": "Desempeño",
            "tone": "commitment",
        },
        {
            "tipo": SeguimientoItem.TIPO_MINUTA,
            "title": "Minutas",
            "subtitle": "Acuerdos derivados de juntas y revisiones",
            "tone": "minute",
        },
        {
            "tipo": SeguimientoItem.TIPO_PROYECTO,
            "title": "Proyectos",
            "subtitle": "Iniciativas con pasos y dependencias",
            "tone": "project",
        },
    ]
    if tipo:
        section_config = [config for config in section_config if config["tipo"] == tipo]
    sections = []
    for config in section_config:
        section_items = [item for item in items if item.tipo == config["tipo"]]
        total_checks = sum(item.checklist_total for item in section_items)
        done_checks = sum(item.checklist_done for item in section_items)
        sections.append(
            {
                **config,
                "items": section_items,
                "total": len(section_items),
                "abiertos": sum(1 for item in section_items if not item.esta_cerrado),
                "vencidos": sum(1 for item in section_items if item.esta_vencido),
                "en_revision": sum(1 for item in section_items if item.estatus == SeguimientoItem.ESTATUS_EN_REVISION),
                "completados": sum(1 for item in section_items if item.estatus == SeguimientoItem.ESTATUS_COMPLETADO),
                "progreso_pct": round((done_checks / total_checks) * 100) if total_checks else 0,
            }
        )

    return render(
        request,
        "seguimiento/mi_seguimiento.html",
        {
            "empleado": empleado,
            "items": items,
            "sections": sections,
            "metrics": metrics,
            "estatus_en_revision": SeguimientoItem.ESTATUS_EN_REVISION,
            "tabs": tabs,
            "active_tipo": tipo,
            "modo_detalle": bool(tipo),
        },
    )


def seguimiento_minutas(request):
    return mi_seguimiento(request, SeguimientoItem.TIPO_MINUTA)


def seguimiento_proyectos(request):
    return mi_seguimiento(request, SeguimientoItem.TIPO_PROYECTO)


def seguimiento_compromisos(request):
    return mi_seguimiento(request, SeguimientoItem.TIPO_COMPROMISO)


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
    error_archivo = _validar_archivo_evidencia(archivo)
    if error_archivo:
        messages.error(request, error_archivo)
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


@login_required
@require_POST
def solicitar_prorroga(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    fecha_solicitada = parse_date((request.POST.get("fecha_solicitada") or "").strip())
    motivo = (request.POST.get("motivo") or "").strip()
    if not fecha_solicitada:
        messages.error(request, "Selecciona la nueva fecha solicitada.")
        return redirect("seguimiento:mi_seguimiento")
    if fecha_solicitada <= timezone.localdate():
        messages.error(request, "La fecha solicitada debe ser posterior a hoy.")
        return redirect("seguimiento:mi_seguimiento")
    if not motivo:
        messages.error(request, "Escribe el motivo para solicitar más tiempo.")
        return redirect("seguimiento:mi_seguimiento")

    SeguimientoProrrogaSolicitud.objects.create(
        seguimiento=item,
        usuario=request.user,
        fecha_solicitada=fecha_solicitada,
        motivo=motivo,
    )
    if item.estatus in {
        SeguimientoItem.ESTATUS_PENDIENTE,
        SeguimientoItem.ESTATUS_EN_PROCESO,
        SeguimientoItem.ESTATUS_BLOQUEADO,
    }:
        item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
        item.save(update_fields=["estatus", "updated_at"])
    log_event(
        request.user,
        "seguimiento.prorroga",
        "SeguimientoItem",
        item.pk,
        {"fecha_solicitada": fecha_solicitada.isoformat()},
    )
    messages.success(request, "Solicitud de más tiempo enviada para revisión.")
    return redirect("seguimiento:mi_seguimiento")


@login_required
def panel_dg(request):
    if not (request.user.is_staff or request.user.is_superuser or has_any_role(request.user, ROLE_DG, ROLE_ADMIN)):
        messages.error(request, "Acceso restringido a Dirección General.")
        return redirect("seguimiento:mi_seguimiento")

    now = timezone.now()

    # Filtros desde GET
    filtro_tipo = (request.GET.get("tipo") or "").strip().upper()
    filtro_estatus = (request.GET.get("estatus") or "").strip().upper()
    filtro_colaborador = (request.GET.get("colaborador") or "").strip()
    filtro_vencidos = request.GET.get("vencidos") == "1"

    qs = (
        SeguimientoItem.objects.select_related("responsable_user", "responsable_empleado", "aprobado_por")
        .prefetch_related("checklist", "prorrogas", "comentarios", "evidencias")
        .order_by("estatus", "fecha_limite", "-updated_at")
    )

    if filtro_tipo and filtro_tipo in dict(SeguimientoItem.TIPO_CHOICES):
        qs = qs.filter(tipo=filtro_tipo)
    if filtro_estatus and filtro_estatus in dict(SeguimientoItem.ESTATUS_CHOICES):
        qs = qs.filter(estatus=filtro_estatus)
    if filtro_colaborador:
        qs = qs.filter(
            Q(responsable_user__first_name__icontains=filtro_colaborador)
            | Q(responsable_user__last_name__icontains=filtro_colaborador)
            | Q(responsable_user__username__icontains=filtro_colaborador)
            | Q(responsable_empleado__nombre__icontains=filtro_colaborador)
        )
    if filtro_vencidos:
        qs = qs.filter(fecha_limite__lt=now).exclude(estatus__in=[SeguimientoItem.ESTATUS_COMPLETADO, SeguimientoItem.ESTATUS_CANCELADO])

    items = list(qs)

    for item in items:
        checks = list(item.checklist.all())
        item.checklist_total = len(checks)
        item.checklist_done = sum(1 for c in checks if c.completado)
        item.progreso_pct = round((item.checklist_done / item.checklist_total) * 100) if item.checklist_total else 0
        item.prorroga_pendiente = next(
            (p for p in item.prorrogas.all() if p.estatus == SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE), None
        )
        item.actividad_count = item.comentarios.count() + item.evidencias.count()
        item.responsable_nombre = (
            item.responsable_user.get_full_name() or item.responsable_user.username
            if item.responsable_user
            else (item.responsable_empleado.nombre if item.responsable_empleado else "Sin asignar")
        )
        if item.esta_vencido:
            item.urgencia = "danger"
        elif item.fecha_limite and item.fecha_limite <= now + timedelta(days=2) and not item.esta_cerrado:
            item.urgencia = "warn"
        else:
            item.urgencia = ""

    # KPIs globales
    total = len(items)
    abiertos = sum(1 for i in items if not i.esta_cerrado)
    vencidos = sum(1 for i in items if i.esta_vencido)
    en_revision = sum(1 for i in items if i.estatus == SeguimientoItem.ESTATUS_EN_REVISION)
    completados = sum(1 for i in items if i.estatus == SeguimientoItem.ESTATUS_COMPLETADO)
    prorrogas_pendientes = sum(1 for i in items if i.prorroga_pendiente)
    por_vencer_24h = sum(
        1 for i in items
        if i.fecha_limite and now <= i.fecha_limite <= now + timedelta(hours=24) and not i.esta_cerrado
    )

    # Agrupado por colaborador para la vista resumen
    from collections import defaultdict
    por_colaborador = defaultdict(lambda: {"items": [], "nombre": "", "abiertos": 0, "vencidos": 0, "en_revision": 0, "completados": 0})
    for item in items:
        key = item.responsable_nombre
        por_colaborador[key]["nombre"] = key
        por_colaborador[key]["items"].append(item)
        if not item.esta_cerrado:
            por_colaborador[key]["abiertos"] += 1
        if item.esta_vencido:
            por_colaborador[key]["vencidos"] += 1
        if item.estatus == SeguimientoItem.ESTATUS_EN_REVISION:
            por_colaborador[key]["en_revision"] += 1
        if item.estatus == SeguimientoItem.ESTATUS_COMPLETADO:
            por_colaborador[key]["completados"] += 1

    colaboradores_resumen = sorted(
        por_colaborador.values(),
        key=lambda c: (-c["vencidos"], -c["en_revision"], -c["abiertos"]),
    )

    vista = request.GET.get("vista", "tabla")  # "tabla" | "colaborador"

    return render(request, "seguimiento/panel_dg.html", {
        "items": items,
        "colaboradores_resumen": colaboradores_resumen,
        "vista": vista,
        "total": total,
        "abiertos": abiertos,
        "vencidos": vencidos,
        "en_revision": en_revision,
        "completados": completados,
        "prorrogas_pendientes": prorrogas_pendientes,
        "por_vencer_24h": por_vencer_24h,
        "filtro_tipo": filtro_tipo,
        "filtro_estatus": filtro_estatus,
        "filtro_colaborador": filtro_colaborador,
        "filtro_vencidos": filtro_vencidos,
        "tipo_choices": SeguimientoItem.TIPO_CHOICES,
        "estatus_choices": SeguimientoItem.ESTATUS_CHOICES,
        "ESTATUS_EN_REVISION": SeguimientoItem.ESTATUS_EN_REVISION,
        "ESTATUS_COMPLETADO": SeguimientoItem.ESTATUS_COMPLETADO,
    })
