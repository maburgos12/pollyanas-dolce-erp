from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.mail import send_mail
from django.db.models import Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils.dateparse import parse_date
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.access import ROLE_DG, ROLE_ADMIN, has_any_role
from core.audit import log_event
from core.notificaciones import (
    notificar_seguimiento_avance,
    notificar_seguimiento_completado,
    notificar_seguimiento_entrega,
    notificar_seguimiento_prorroga,
)

from .models import (
    SeguimientoChecklistItem,
    SeguimientoComentario,
    SeguimientoEvidencia,
    SeguimientoItem,
    SeguimientoProrrogaSolicitud,
)
from .services import empleado_de_usuario


def _notificar_dg_revision(item: SeguimientoItem, accion: str, usuario_nombre: str) -> None:
    director_email = (getattr(settings, "DIRECTOR_EMAIL", "") or "").strip()
    if not director_email:
        return
    from_email = getattr(settings, "DEFAULT_FROM_EMAIL", "")
    titulo = item.titulo[:80]
    asunto = f"[Seguimiento] {accion}: {titulo}"
    cuerpo = (
        f"{usuario_nombre} marcó el acuerdo '{titulo}' como listo para revisión.\n\n"
        f"Tipo: {item.get_tipo_display()}\n"
        f"Estatus: {item.get_estatus_display()}\n"
        f"Área: {item.area or '—'}\n"
        f"Fecha límite: {item.fecha_limite.strftime('%d/%m/%Y %H:%M') if item.fecha_limite else 'Sin fecha'}\n\n"
        f"Revisa en el ERP: /seguimiento/{item.pk}/\n"
        f"Bandeja de revisión: /seguimiento/revision/"
    )
    try:
        send_mail(asunto, cuerpo, from_email, [director_email], fail_silently=True)
    except Exception:
        pass


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
    notificar_seguimiento_avance(item, actor=request.user, mensaje_extra=f"Check: {'✓ ' if check.completado else '○ '}{check.titulo}")
    messages.success(request, "Checklist actualizado.")
    return redirect("seguimiento:detalle", pk=item.pk)


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
    notificar_seguimiento_avance(item, actor=request.user, mensaje_extra=comentario[:160])
    messages.success(request, "Retroalimentación enviada.")
    return redirect("seguimiento:detalle", pk=item.pk)


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
    notificar_seguimiento_avance(item, actor=request.user, mensaje_extra=f"Archivo: {archivo.name}")
    messages.success(request, "Evidencia subida.")
    return redirect("seguimiento:detalle", pk=item.pk)


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
    notificar_seguimiento_prorroga(item, fecha_solicitada, motivo, actor=request.user)
    messages.success(request, "Solicitud de prórroga enviada.")
    return redirect("seguimiento:detalle", pk=item.pk)


def _registrar_cierre_opcional(request, item, prefijo: str) -> bool:
    """Guarda comentario y/o evidencia opcionales. Devuelve False si el archivo falla."""
    comentario_texto = (request.POST.get("comentario") or "").strip()
    archivo = request.FILES.get("archivo")
    if archivo:
        error_archivo = _validar_archivo_evidencia(archivo)
        if error_archivo:
            messages.error(request, error_archivo)
            return False
        SeguimientoEvidencia.objects.create(
            seguimiento=item,
            usuario=request.user,
            archivo=archivo,
            nombre_original=archivo.name,
            comentario=comentario_texto,
        )
    if comentario_texto:
        SeguimientoComentario.objects.create(
            seguimiento=item,
            usuario=request.user,
            tipo=SeguimientoComentario.TIPO_FEEDBACK,
            comentario=f"[{prefijo}] {comentario_texto}" if comentario_texto else comentario_texto,
        )
    return True


@login_required
@require_POST
def entregar_para_revision(request, pk):
    """Flujo para acuerdos que sí requieren aprobación del DG."""
    item = _get_item_para_usuario(request.user, pk)
    if item.esta_cerrado:
        messages.error(request, "Este acuerdo ya está cerrado.")
        return redirect("seguimiento:detalle", pk=pk)
    if not _registrar_cierre_opcional(request, item, "ENTREGA"):
        return redirect("seguimiento:detalle", pk=pk)
    item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
    item.save(update_fields=["estatus", "updated_at"])
    log_event(request.user, "seguimiento.entrega", "SeguimientoItem", item.pk, {"entrega": True})
    notificar_seguimiento_entrega(item, actor=request.user)
    nombre_usuario = request.user.get_full_name() or request.user.username
    _notificar_dg_revision(item, "Entregado para revisión", nombre_usuario)
    messages.success(request, "Acuerdo enviado a revisión. El Director General será notificado.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
@require_POST
def completar_directamente(request, pk):
    """Cierre directo para acuerdos sin aprobación requerida o con checklist 100%."""
    item = _get_item_para_usuario(request.user, pk)
    if item.esta_cerrado:
        messages.error(request, "Este acuerdo ya está cerrado.")
        return redirect("seguimiento:detalle", pk=pk)
    checks = list(item.checklist.all())
    checklist_completo = checks and all(c.completado for c in checks)
    puede_cerrar_directo = not item.requiere_aprobacion or checklist_completo
    if not puede_cerrar_directo:
        messages.error(request, "Este acuerdo requiere aprobación del Director General.")
        return redirect("seguimiento:detalle", pk=pk)
    if not _registrar_cierre_opcional(request, item, "COMPLETADO"):
        return redirect("seguimiento:detalle", pk=pk)
    item.estatus = SeguimientoItem.ESTATUS_COMPLETADO
    item.aprobado_at = timezone.now()
    item.save(update_fields=["estatus", "aprobado_at", "updated_at"])
    log_event(request.user, "seguimiento.completar", "SeguimientoItem", item.pk, {"directo": True})
    notificar_seguimiento_completado(item, actor=request.user)
    messages.success(request, "Acuerdo marcado como completado.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
def detalle_item(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    checks = list(item.checklist.all())
    checklist_total = len(checks)
    checklist_done = sum(1 for c in checks if c.completado)
    progreso_pct = round((checklist_done / checklist_total) * 100) if checklist_total else 0
    now = timezone.now()
    if item.esta_vencido:
        prioridad_label, prioridad_tone = "Vencido", "danger"
    elif item.fecha_limite and item.fecha_limite <= now + timedelta(days=2) and not item.esta_cerrado:
        prioridad_label, prioridad_tone = "Alta", "warn"
    else:
        prioridad_label, prioridad_tone = "Normal", ""
    prorroga_pendiente = item.prorrogas.filter(estatus=SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE).first()
    comentarios = item.comentarios.select_related("usuario").order_by("created_at")
    evidencias = item.evidencias.select_related("usuario", "revisado_por").order_by("created_at")
    checklist_completo = bool(checks) and all(c.completado for c in checks)
    puede_cerrar_directo = not item.requiere_aprobacion or checklist_completo
    puede_entregar = not item.esta_cerrado and item.requiere_aprobacion and not checklist_completo
    return render(request, "seguimiento/detalle_item.html", {
        "item": item,
        "checks": checks,
        "checklist_total": checklist_total,
        "checklist_done": checklist_done,
        "progreso_pct": progreso_pct,
        "prioridad_label": prioridad_label,
        "prioridad_tone": prioridad_tone,
        "prorroga_pendiente": prorroga_pendiente,
        "comentarios": comentarios,
        "evidencias": evidencias,
        "puede_cerrar_directo": puede_cerrar_directo and not item.esta_cerrado,
        "puede_entregar": puede_entregar,
        "checklist_completo": checklist_completo,
        "estatus_en_revision": SeguimientoItem.ESTATUS_EN_REVISION,
    })


@login_required
def bandeja_revision(request):
    if not (request.user.is_staff or request.user.is_superuser or has_any_role(request.user, ROLE_DG, ROLE_ADMIN)):
        messages.error(request, "No tienes acceso a la bandeja de revisión.")
        return redirect("seguimiento:mi_seguimiento")
    items = (
        SeguimientoItem.objects.filter(estatus=SeguimientoItem.ESTATUS_EN_REVISION)
        .select_related("responsable_user", "responsable_empleado")
        .prefetch_related("comentarios", "evidencias__usuario", "prorrogas", "checklist")
        .order_by("fecha_limite", "-updated_at")
    )
    for item in items:
        checks = list(item.checklist.all())
        item.checklist_total = len(checks)
        item.checklist_done = sum(1 for c in checks if c.completado)
        item.progreso_pct = round((item.checklist_done / item.checklist_total) * 100) if item.checklist_total else 0
        item.ultima_evidencia = item.evidencias.order_by("-created_at").first()
        item.ultimo_comentario = item.comentarios.order_by("-created_at").first()
        item.prorroga_pendiente = item.prorrogas.filter(estatus=SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE).first()
    return render(request, "seguimiento/bandeja_revision.html", {"items": items, "total": items.count()})


@login_required
@require_POST
def resolver_revision(request, pk):
    if not (request.user.is_staff or request.user.is_superuser or has_any_role(request.user, ROLE_DG, ROLE_ADMIN)):
        messages.error(request, "No tienes permiso para resolver revisiones.")
        return redirect("seguimiento:bandeja_revision")
    item = get_object_or_404(SeguimientoItem, pk=pk)
    accion = (request.POST.get("accion") or "").strip()
    comentario_texto = (request.POST.get("comentario") or "").strip()
    if accion == "aprobar":
        item.estatus = SeguimientoItem.ESTATUS_COMPLETADO
        item.aprobado_por = request.user
        item.aprobado_at = timezone.now()
        item.save(update_fields=["estatus", "aprobado_por", "aprobado_at", "updated_at"])
        if comentario_texto:
            SeguimientoComentario.objects.create(
                seguimiento=item, usuario=request.user,
                tipo=SeguimientoComentario.TIPO_REVISION_DG,
                comentario=f"[APROBADO] {comentario_texto}",
            )
        log_event(request.user, "seguimiento.aprobar", "SeguimientoItem", item.pk, {})
        messages.success(request, f"'{item.titulo[:60]}' marcado como completado.")
    elif accion == "devolver":
        if not comentario_texto:
            messages.error(request, "Escribe el motivo para devolver el acuerdo.")
            return redirect("seguimiento:detalle", pk=pk)
        item.estatus = SeguimientoItem.ESTATUS_EN_PROCESO
        item.save(update_fields=["estatus", "updated_at"])
        SeguimientoComentario.objects.create(
            seguimiento=item, usuario=request.user,
            tipo=SeguimientoComentario.TIPO_REVISION_DG,
            comentario=f"[DEVUELTO] {comentario_texto}",
        )
        log_event(request.user, "seguimiento.devolver", "SeguimientoItem", item.pk, {})
        messages.warning(request, f"'{item.titulo[:60]}' devuelto para corrección.")
    else:
        messages.error(request, "Acción no reconocida.")
    return redirect("seguimiento:bandeja_revision")


@login_required
@require_POST
def resolver_prorroga(request, pk, prorroga_id):
    if not (request.user.is_staff or request.user.is_superuser or has_any_role(request.user, ROLE_DG, ROLE_ADMIN)):
        messages.error(request, "No tienes permiso para resolver solicitudes de prórroga.")
        return redirect("seguimiento:bandeja_revision")
    prorroga = get_object_or_404(SeguimientoProrrogaSolicitud, pk=prorroga_id, seguimiento_id=pk)
    accion = (request.POST.get("accion") or "").strip()
    if accion == "aprobar":
        item = prorroga.seguimiento
        item.fecha_limite = timezone.make_aware(
            timezone.datetime.combine(prorroga.fecha_solicitada, timezone.datetime.min.time().replace(hour=18)),
            timezone.get_current_timezone(),
        )
        item.estatus = SeguimientoItem.ESTATUS_EN_PROCESO
        item.save(update_fields=["fecha_limite", "estatus", "updated_at"])
        prorroga.estatus = SeguimientoProrrogaSolicitud.ESTATUS_APROBADA
        prorroga.resuelto_por = request.user
        prorroga.resuelto_at = timezone.now()
        prorroga.save()
        log_event(request.user, "seguimiento.prorroga.aprobar", "SeguimientoProrrogaSolicitud", prorroga.pk, {})
        messages.success(request, f"Prórroga aprobada hasta {prorroga.fecha_solicitada.strftime('%d/%m/%Y')}.")
    elif accion == "rechazar":
        prorroga.estatus = SeguimientoProrrogaSolicitud.ESTATUS_RECHAZADA
        prorroga.resuelto_por = request.user
        prorroga.resuelto_at = timezone.now()
        prorroga.save()
        log_event(request.user, "seguimiento.prorroga.rechazar", "SeguimientoProrrogaSolicitud", prorroga.pk, {})
        messages.warning(request, "Solicitud de prórroga rechazada.")
    return redirect("seguimiento:bandeja_revision")


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

    _ESTATUS_PROGRESO = {
        SeguimientoItem.ESTATUS_COMPLETADO: 100,
        SeguimientoItem.ESTATUS_EN_REVISION: 80,
        SeguimientoItem.ESTATUS_EN_PROCESO: 50,
        SeguimientoItem.ESTATUS_BLOQUEADO: 30,
        SeguimientoItem.ESTATUS_PENDIENTE: 10,
        SeguimientoItem.ESTATUS_CANCELADO: 0,
    }

    for item in items:
        checks = list(item.checklist.all())
        item.checklist_total = len(checks)
        item.checklist_done = sum(1 for c in checks if c.completado)
        if item.checklist_total:
            item.progreso_pct = round((item.checklist_done / item.checklist_total) * 100)
        else:
            # Sin checklist: progreso inferido del estatus
            item.progreso_pct = _ESTATUS_PROGRESO.get(item.estatus, 0)
        item.prorroga_pendiente = next(
            (p for p in item.prorrogas.all() if p.estatus == SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE), None
        )
        item.ultima_actualizacion = item.updated_at
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

    vista = request.GET.get("vista", "tabla")

    # Tab activo por tipo (independiente del filtro_tipo del formulario)
    active_tab = (request.GET.get("tab") or "").strip().upper()
    if active_tab and active_tab in dict(SeguimientoItem.TIPO_CHOICES):
        items = [i for i in items if i.tipo == active_tab]
    else:
        active_tab = ""

    # Conteos para las tabs (sobre la lista completa sin filtro de tab)
    all_items_for_counts = list(qs) if active_tab else items
    count_compromisos = sum(1 for i in all_items_for_counts if i.tipo == SeguimientoItem.TIPO_COMPROMISO)
    count_minutas = sum(1 for i in all_items_for_counts if i.tipo == SeguimientoItem.TIPO_MINUTA)
    count_proyectos = sum(1 for i in all_items_for_counts if i.tipo == SeguimientoItem.TIPO_PROYECTO)

    # Recalcular totales sobre la lista final (ya filtrada por tab)
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

    return render(request, "seguimiento/panel_dg.html", {
        "items": items,
        "colaboradores_resumen": colaboradores_resumen,
        "vista": vista,
        "active_tab": active_tab,
        "count_compromisos": count_compromisos,
        "count_minutas": count_minutas,
        "count_proyectos": count_proyectos,
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


def _registrar_cierre_opcional(request, item, prefijo: str) -> bool:
    """Guarda comentario y/o evidencia opcionales. Devuelve False si el archivo falla."""
    comentario_texto = (request.POST.get("comentario") or "").strip()
    archivo = request.FILES.get("archivo")
    if archivo:
        error_archivo = _validar_archivo_evidencia(archivo)
        if error_archivo:
            messages.error(request, error_archivo)
            return False
        SeguimientoEvidencia.objects.create(
            seguimiento=item,
            usuario=request.user,
            archivo=archivo,
            nombre_original=archivo.name,
            comentario=comentario_texto,
        )
    if comentario_texto:
        SeguimientoComentario.objects.create(
            seguimiento=item,
            usuario=request.user,
            tipo=SeguimientoComentario.TIPO_FEEDBACK,
            comentario=f"[{prefijo}] {comentario_texto}" if comentario_texto else comentario_texto,
        )
    return True


@login_required
@require_POST
def entregar_para_revision(request, pk):
    """Flujo para acuerdos que sí requieren aprobación del DG."""
    item = _get_item_para_usuario(request.user, pk)
    if item.esta_cerrado:
        messages.error(request, "Este acuerdo ya está cerrado.")
        return redirect("seguimiento:detalle", pk=pk)

    if not _registrar_cierre_opcional(request, item, "ENTREGA"):
        return redirect("seguimiento:detalle", pk=pk)

    item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
    item.save(update_fields=["estatus", "updated_at"])
    log_event(request.user, "seguimiento.entrega", "SeguimientoItem", item.pk, {"entrega": True})
    notificar_seguimiento_entrega(item, actor=request.user)
    nombre_usuario = request.user.get_full_name() or request.user.username
    _notificar_dg_revision(item, "Entregado para revisión", nombre_usuario)
    messages.success(request, "Acuerdo enviado a revisión. El Director General será notificado.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
@require_POST
def completar_directamente(request, pk):
    """Cierre directo para acuerdos sin aprobación requerida o con checklist 100%."""
    item = _get_item_para_usuario(request.user, pk)
    if item.esta_cerrado:
        messages.error(request, "Este acuerdo ya está cerrado.")
        return redirect("seguimiento:detalle", pk=pk)

    checks = list(item.checklist.all())
    checklist_completo = checks and all(c.completado for c in checks)
    puede_cerrar_directo = not item.requiere_aprobacion or checklist_completo

    if not puede_cerrar_directo:
        messages.error(request, "Este acuerdo requiere aprobación del Director General.")
        return redirect("seguimiento:detalle", pk=pk)

    if not _registrar_cierre_opcional(request, item, "COMPLETADO"):
        return redirect("seguimiento:detalle", pk=pk)

    item.estatus = SeguimientoItem.ESTATUS_COMPLETADO
    item.aprobado_at = timezone.now()
    item.save(update_fields=["estatus", "aprobado_at", "updated_at"])
    log_event(request.user, "seguimiento.completar", "SeguimientoItem", item.pk, {"directo": True})
    notificar_seguimiento_completado(item, actor=request.user)
    messages.success(request, "Acuerdo marcado como completado.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
def detalle_item(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    checks = list(item.checklist.all())
    checklist_total = len(checks)
    checklist_done = sum(1 for c in checks if c.completado)
    progreso_pct = round((checklist_done / checklist_total) * 100) if checklist_total else 0

    now = timezone.now()
    if item.esta_vencido:
        prioridad_label, prioridad_tone = "Vencido", "danger"
    elif item.fecha_limite and item.fecha_limite <= now + timedelta(days=2) and not item.esta_cerrado:
        prioridad_label, prioridad_tone = "Alta", "warn"
    else:
        prioridad_label, prioridad_tone = "Normal", ""

    prorroga_pendiente = item.prorrogas.filter(estatus=SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE).first()
    comentarios = item.comentarios.select_related("usuario").order_by("created_at")
    evidencias = item.evidencias.select_related("usuario", "revisado_por").order_by("created_at")

    checklist_completo = bool(checks) and all(c.completado for c in checks)
    puede_cerrar_directo = not item.requiere_aprobacion or checklist_completo
    puede_entregar = not item.esta_cerrado and item.requiere_aprobacion and not checklist_completo

    tiene_revision_dg = comentarios.filter(tipo=SeguimientoComentario.TIPO_REVISION_DG).exists()
    puede_retractar = (
        item.estatus == SeguimientoItem.ESTATUS_EN_REVISION
        and not tiene_revision_dg
    )

    return render(
        request,
        "seguimiento/detalle_item.html",
        {
            "item": item,
            "checks": checks,
            "checklist_total": checklist_total,
            "checklist_done": checklist_done,
            "progreso_pct": progreso_pct,
            "prioridad_label": prioridad_label,
            "prioridad_tone": prioridad_tone,
            "prorroga_pendiente": prorroga_pendiente,
            "comentarios": comentarios,
            "evidencias": evidencias,
            "puede_cerrar_directo": puede_cerrar_directo and not item.esta_cerrado,
            "puede_entregar": puede_entregar,
            "checklist_completo": checklist_completo,
            "estatus_en_revision": SeguimientoItem.ESTATUS_EN_REVISION,
            "puede_retractar": puede_retractar,
            "current_user": request.user,
        },
    )


@login_required
@require_POST
def eliminar_evidencia_propia(request, pk, evidencia_id):
    item = _get_item_para_usuario(request.user, pk)
    if item.esta_cerrado:
        messages.error(request, "No puedes eliminar archivos de un acuerdo cerrado.")
        return redirect("seguimiento:detalle", pk=pk)
    evidencia = get_object_or_404(
        SeguimientoEvidencia,
        pk=evidencia_id,
        seguimiento=item,
        usuario=request.user,
    )
    nombre = evidencia.nombre_original
    if evidencia.archivo:
        evidencia.archivo.delete(save=False)
    evidencia.delete()
    log_event(request.user, "seguimiento.evidencia.eliminar", "SeguimientoItem", item.pk, {"archivo": nombre})
    messages.success(request, f"Archivo '{nombre}' eliminado. Ahora puedes subir el correcto.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
@require_POST
def editar_comentario_propio(request, pk, comentario_id):
    item = _get_item_para_usuario(request.user, pk)
    comentario = get_object_or_404(
        SeguimientoComentario,
        pk=comentario_id,
        seguimiento=item,
        usuario=request.user,
        tipo=SeguimientoComentario.TIPO_FEEDBACK,
    )
    nuevo_texto = (request.POST.get("comentario") or "").strip()
    if not nuevo_texto:
        messages.error(request, "El comentario no puede quedar vacío.")
        return redirect("seguimiento:detalle", pk=pk)
    comentario.comentario = nuevo_texto
    comentario.save(update_fields=["comentario"])
    log_event(request.user, "seguimiento.comentario.editar", "SeguimientoComentario", comentario.pk, {"seguimiento_id": item.pk})
    messages.success(request, "Comentario actualizado.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
@require_POST
def editar_nota_evidencia(request, pk, evidencia_id):
    item = _get_item_para_usuario(request.user, pk)
    evidencia = get_object_or_404(
        SeguimientoEvidencia,
        pk=evidencia_id,
        seguimiento=item,
        usuario=request.user,
    )
    nueva_nota = (request.POST.get("comentario") or "").strip()
    evidencia.comentario = nueva_nota
    evidencia.save(update_fields=["comentario"])
    log_event(request.user, "seguimiento.evidencia.editar_nota", "SeguimientoEvidencia", evidencia.pk, {"seguimiento_id": item.pk})
    messages.success(request, "Nota de evidencia actualizada.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
@require_POST
def retractar_entrega(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    if item.estatus != SeguimientoItem.ESTATUS_EN_REVISION:
        messages.error(request, "Solo puedes retractar cuando el acuerdo está en revisión.")
        return redirect("seguimiento:detalle", pk=pk)
    tiene_revision_dg = item.comentarios.filter(tipo=SeguimientoComentario.TIPO_REVISION_DG).exists()
    if tiene_revision_dg:
        messages.error(request, "El Director General ya revisó este acuerdo. No es posible retractarlo.")
        return redirect("seguimiento:detalle", pk=pk)
    item.estatus = SeguimientoItem.ESTATUS_EN_PROCESO
    item.save(update_fields=["estatus", "updated_at"])
    log_event(request.user, "seguimiento.retractar", "SeguimientoItem", item.pk, {})
    messages.success(request, "Acuerdo regresado a 'En proceso'. Ya puedes hacer cambios y volver a enviarlo.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
def detalle_item_dg(request, pk):
    """Detalle de cualquier acuerdo para DG — sin restricción de responsable."""
    if not (request.user.is_staff or request.user.is_superuser or has_any_role(request.user, ROLE_DG, ROLE_ADMIN)):
        messages.error(request, "Acceso restringido a Dirección General.")
        return redirect("seguimiento:mi_seguimiento")
    item = get_object_or_404(
        SeguimientoItem.objects.select_related("responsable_user", "responsable_empleado", "aprobado_por")
        .prefetch_related("checklist", "comentarios__usuario", "evidencias__usuario", "prorrogas"),
        pk=pk,
    )
    checks = list(item.checklist.all())
    checklist_total = len(checks)
    checklist_done = sum(1 for c in checks if c.completado)
    progreso_pct = round((checklist_done / checklist_total) * 100) if checklist_total else 0
    now = timezone.now()
    if item.esta_vencido:
        prioridad_label, prioridad_tone = "Vencido", "danger"
    elif item.fecha_limite and item.fecha_limite <= now + timedelta(days=2) and not item.esta_cerrado:
        prioridad_label, prioridad_tone = "Alta", "warn"
    else:
        prioridad_label, prioridad_tone = "Normal", ""
    prorroga_pendiente = item.prorrogas.filter(estatus=SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE).first()
    comentarios = item.comentarios.select_related("usuario").order_by("created_at")
    evidencias = item.evidencias.select_related("usuario", "revisado_por").order_by("created_at")
    return render(request, "seguimiento/detalle_item.html", {
        "item": item,
        "checks": checks,
        "checklist_total": checklist_total,
        "checklist_done": checklist_done,
        "progreso_pct": progreso_pct,
        "prioridad_label": prioridad_label,
        "prioridad_tone": prioridad_tone,
        "prorroga_pendiente": prorroga_pendiente,
        "comentarios": comentarios,
        "evidencias": evidencias,
        "puede_cerrar_directo": False,
        "puede_entregar": False,
        "checklist_completo": bool(checks) and all(c.completado for c in checks),
        "estatus_en_revision": SeguimientoItem.ESTATUS_EN_REVISION,
        "es_vista_dg": True,
        "puede_resolver_dg": item.estatus == SeguimientoItem.ESTATUS_EN_REVISION,
    })
