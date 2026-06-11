from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.db.models import Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils.dateparse import parse_date, parse_time
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.access import can_review_seguimiento_global
from core.audit import log_event
from core.notificaciones import (
    notificar_paso_aprobado_por_colaborador,
    notificar_paso_devuelto_por_colaborador,
    notificar_seguimiento_aprobado,
    notificar_seguimiento_avance,
    notificar_seguimiento_completado,
    notificar_seguimiento_devuelto,
    notificar_seguimiento_entrega,
    notificar_seguimiento_feedback_responsable,
    notificar_seguimiento_prorroga,
)

from .models import (
    ActividadCalendario,
    SeguimientoChecklistItem,
    SeguimientoComentario,
    SeguimientoEvidencia,
    SeguimientoItem,
    SeguimientoProrrogaSolicitud,
)
from .services import empleado_de_usuario, upsert_agente_dg_payload

logger = logging.getLogger(__name__)


def _writeback_activo() -> bool:
    return (os.getenv("AGENTE_DG_WRITEBACK_ENABLED", "") or "").strip().lower() in {"1", "true", "yes", "on"}


def _agente_dg_source(item: SeguimientoItem) -> tuple[str, int] | None:
    metadata = item.metadata or {}
    source_table = str(metadata.get("source_table") or "").strip()
    source_id = metadata.get("source_id")
    if not source_table and item.referencia_externa and ":" in item.referencia_externa:
        source_table, source_id = item.referencia_externa.split(":", 1)
    if str(metadata.get("source") or "").strip() != "agente_dg" and not source_table:
        return None
    try:
        return source_table, int(source_id)
    except (TypeError, ValueError):
        return None


def _writeback_agente_dg_item(item: SeguimientoItem, *, accion: str, comentario: str = "") -> bool:
    """Sincroniza cierres/devoluciones al Agente DG cuando el item viene de esa app.

    Devuelve True si hizo write-back. Si el item es de Agente DG y el write-back
    está activo pero falla, lanza AgenteDGError para impedir divergencia local.
    """
    source = _agente_dg_source(item)
    if not source or not _writeback_activo():
        return False

    from .agente_dg_client import (
        AgenteDGError,
        is_configured,
        patch_commitment_status,
        patch_minute_agreement,
    )

    if not is_configured():
        raise AgenteDGError("La API del Agente DG no está configurada para write-back.")

    source_table, source_id = source
    accion = accion.strip().lower()
    comentario = comentario.strip()

    if source_table == "minute_agreements":
        if accion in {"aprobar", "completar"}:
            payload = {"status": "COMPLETED"}
            if comentario:
                payload["completion_note"] = comentario
            patch_minute_agreement(source_id, **payload)
            return True
        if accion == "devolver":
            patch_minute_agreement(source_id, status="IN_PROGRESS")
            return True
        if accion == "feedback" and comentario:
            patch_minute_agreement(source_id, completion_note=comentario)
            return True
        return False

    if source_table == "commitments":
        if accion == "aprobar":
            patch_commitment_status(source_id, status="CLOSED", comment=comentario)
            return True
        if accion == "devolver":
            patch_commitment_status(source_id, status="PENDING", comment=comentario)
            return True
        if accion == "completar":
            patch_commitment_status(source_id, status="CLOSED", comment=comentario)
            return True
        return False

    return False


def _conversacion_context(comentarios):
    comentarios_list = list(comentarios)
    comentarios_dg = [comentario for comentario in comentarios_list if comentario.tipo == SeguimientoComentario.TIPO_REVISION_DG]
    return {
        "comentarios": comentarios_list,
        "comentarios_dg": comentarios_dg,
        "ultimo_comentario_dg": comentarios_dg[-1] if comentarios_dg else None,
    }


ACTIVE_AGENTE_DG_STATUSES = {"OPEN", "IN_PROGRESS", "WAITING_THIRD_PARTY", "POSTPONED", "BLOCKED", "PENDING", "OVERDUE", "DUE_SOON", "AT_RISK", "ACTIVE"}
CLOSED_AGENTE_DG_STATUSES = {"REVIEWED", "CLOSED", "COMPLETED", "APPROVED", "ENTREGADO_A_TIEMPO", "ENTREGADO_TARDE"}
CANCELLED_AGENTE_DG_STATUSES = {"CANCELLED", "CANCELED"}
CLOSED_OR_CANCELLED_AGENTE_DG_STATUSES = CLOSED_AGENTE_DG_STATUSES | CANCELLED_AGENTE_DG_STATUSES
PANEL_BUCKETS = {
    "activos": "Activos",
    "revision": "En revisión",
    "desfases": "Desfases",
    "historico": "Histórico finalizado",
}


def _source_status(item: SeguimientoItem) -> str:
    return str((item.metadata or {}).get("source_status") or "").strip().upper()


def _source_archived_at(item: SeguimientoItem) -> str:
    return str((item.metadata or {}).get("source_archived_at") or "").strip()


def _source_synced_at(item: SeguimientoItem) -> str:
    return str((item.metadata or {}).get("synced_at") or "").strip()


def _tiene_desfase_agente_dg(item: SeguimientoItem) -> bool:
    source_status = _source_status(item)
    if str((item.metadata or {}).get("source") or "").strip() != "agente_dg":
        return False
    if source_status in ACTIVE_AGENTE_DG_STATUSES and item.aprobado_at:
        return True
    if source_status in ACTIVE_AGENTE_DG_STATUSES and item.estatus == SeguimientoItem.ESTATUS_COMPLETADO:
        return True
    if source_status in CLOSED_OR_CANCELLED_AGENTE_DG_STATUSES and not item.esta_cerrado:
        return True
    return False


def _bucket_panel_seguimiento(item: SeguimientoItem) -> str:
    if item.tiene_desfase_agente_dg:
        return "desfases"
    if item.esta_cerrado or _source_status(item) in CLOSED_OR_CANCELLED_AGENTE_DG_STATUSES or _source_archived_at(item):
        return "historico"
    if item.estatus == SeguimientoItem.ESTATUS_EN_REVISION or getattr(item, "prorroga_pendiente", None):
        return "revision"
    return "activos"


def _aplicar_estado_visual_seguimiento(item: SeguimientoItem, checks=None) -> None:
    checks = list(checks if checks is not None else item.checklist.all())
    item.checklist_total = len(checks)
    item.checklist_done = sum(1 for c in checks if c.completado)
    item.tiene_desfase_agente_dg = _tiene_desfase_agente_dg(item)
    item.source_status = _source_status(item)
    item.source_archived_at = _source_archived_at(item)
    item.source_synced_at = _source_synced_at(item)
    item.estado_app_label = item.source_status or ("Archivado" if item.source_archived_at else "Sin estado app")
    if item.source_archived_at and "Archivado" not in item.estado_app_label:
        item.estado_app_label = f"{item.estado_app_label} · Archivado"
    item.es_historico_agente_dg = bool(item.source_archived_at or item.source_status in CLOSED_OR_CANCELLED_AGENTE_DG_STATUSES)
    item.es_vencido_visual = bool(item.esta_vencido and not item.tiene_desfase_agente_dg and not item.es_historico_agente_dg)
    if item.checklist_total:
        item.progreso_pct = round((item.checklist_done / item.checklist_total) * 100)
        item.avance_label = f"{item.progreso_pct}%"
        item.avance_detalle = f"{item.checklist_done}/{item.checklist_total} checks"
    elif item.esta_cerrado:
        item.progreso_pct = 100
        item.avance_label = "Cerrado"
        item.avance_detalle = "Sin checklist"
    else:
        item.progreso_pct = 0
        item.avance_label = "Sin checklist"
        item.avance_detalle = "Avance no medible"
    item.visual_bucket = _bucket_panel_seguimiento(item)
    item.visual_bucket_label = PANEL_BUCKETS[item.visual_bucket]


def _agente_dg_user_id_para_erp_user(user) -> int | None:
    metadata_match = (
        SeguimientoItem.objects.filter(responsable_user=user, metadata__source="agente_dg")
        .exclude(metadata__source_user_id__isnull=True)
        .values_list("metadata__source_user_id", flat=True)
        .first()
    )
    if metadata_match:
        try:
            return int(metadata_match)
        except (TypeError, ValueError):
            pass
    if not user.email:
        return None
    from .agente_dg_client import get_users

    for agente_user in get_users():
        if str(agente_user.get("email") or "").strip().lower() == user.email.strip().lower():
            try:
                return int(agente_user.get("id"))
            except (TypeError, ValueError):
                return None
    return None


def _record_minuta_agente_dg(payload: dict, responsable_user=None) -> dict:
    checklist_items = payload.get("checklist_items") or []
    checklist_items_json = json_dumps_checklist(checklist_items)
    return {
        "id": payload.get("id"),
        "titulo": payload.get("title") or "",
        "descripcion": payload.get("agreement_text") or "",
        "checklist_items_json": checklist_items_json,
        "status": payload.get("status") or "OPEN",
        "due_at": payload.get("due_at"),
        "user_id": payload.get("collaborator_user_id"),
        "user_email": getattr(responsable_user, "email", "") or "",
        "user_name": getattr(responsable_user, "get_full_name", lambda: "")() or getattr(responsable_user, "username", "") or "",
        "area_name": payload.get("meeting_label") or "",
    }


def json_dumps_checklist(checklist_items) -> str:
    import json

    if not checklist_items:
        return ""
    return json.dumps(checklist_items, ensure_ascii=False)


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
        _aplicar_estado_visual_seguimiento(item, checks)
        comentarios_list = list(item.comentarios.all())  # prefetched, orden -created_at
        actividad = [item.updated_at]
        actividad.extend(check.updated_at for check in checks if check.updated_at)
        actividad.extend(c.created_at for c in comentarios_list)
        actividad.extend(evidencia.created_at for evidencia in item.evidencias.all())
        actividad.extend(prorroga.created_at for prorroga in item.prorrogas.all())

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
        ultimo_comentario_dg = next(
            (c for c in comentarios_list if c.tipo == SeguimientoComentario.TIPO_REVISION_DG),
            None,
        )
        ultimo_feedback_propio = next(
            (
                c for c in comentarios_list
                if c.tipo == SeguimientoComentario.TIPO_FEEDBACK
                and c.usuario_id == item.responsable_user_id
            ),
            None,
        )
        item.ultimo_comentario_dg = ultimo_comentario_dg
        item.respuesta_dg_nueva = ultimo_comentario_dg is not None and (
            ultimo_feedback_propio is None
            or ultimo_comentario_dg.created_at > ultimo_feedback_propio.created_at
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

    # Pasos de proyectos donde el usuario es aprobador y están esperando su aprobación
    mis_aprobaciones = list(
        SeguimientoChecklistItem.objects.filter(
            aprobador_user=request.user,
            requiere_aprobacion=True,
            completado=False,
            estatus_origen="SUBMITTED",
        )
        .select_related("seguimiento", "seguimiento__responsable_user", "seguimiento__responsable_empleado")
        .order_by("vence", "id")
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
            "mis_aprobaciones": mis_aprobaciones,
            "writeback_activo": _writeback_activo(),
            "puede_revisar_seguimiento_global": can_review_seguimiento_global(request.user),
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
    checks = list(item.checklist.all())  # ordenado por (orden, id) según Meta

    # Orden secuencial: los pasos se completan en orden y se deshacen en orden inverso.
    if not check.completado:
        # Marcar: todos los pasos anteriores deben estar completados.
        anteriores = [c for c in checks if (c.orden, c.id) < (check.orden, check.id)]
        if any(not c.completado for c in anteriores):
            messages.error(request, "Completa primero los pasos anteriores, en orden.")
            return redirect("seguimiento:detalle", pk=item.pk)
    else:
        # Desmarcar: ningún paso posterior debe estar completado.
        posteriores = [c for c in checks if (c.orden, c.id) > (check.orden, check.id)]
        if any(c.completado for c in posteriores):
            messages.error(request, "Desmarca primero los pasos posteriores, en orden inverso.")
            return redirect("seguimiento:detalle", pk=item.pk)

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
    notificar_seguimiento_avance(
        item,
        actor=request.user,
        mensaje_extra=f"Check: {'✓ ' if check.completado else '○ '}{check.titulo}",
        enviar_correo=False,
    )
    messages.success(request, "Checklist actualizado.")
    return redirect("seguimiento:detalle", pk=item.pk)


@login_required
@require_POST
def marcar_paso(request, pk, check_id):
    """Marca un paso o sub-punto desde el ERP y lo sincroniza al Agente DG (Fase 2b).

    Solo el responsable/participante (el DG queda en solo lectura). Requiere write-back
    activo y configurado, y que el paso tenga origen_step_id. Si la API falla, NO se
    cambia el estado local (la fuente de verdad sigue siendo el Agente DG).
    """
    from .agente_dg_client import AgenteDGError, is_configured, patch_step

    item = _get_item_para_usuario(request.user, pk)
    check = get_object_or_404(SeguimientoChecklistItem, pk=check_id, seguimiento=item)

    if not (_writeback_activo() and is_configured()):
        messages.error(request, "La sincronización con el Agente DG no está activa.")
        return redirect("seguimiento:detalle", pk=item.pk)
    if not check.origen_step_id:
        messages.error(request, "Este paso no está vinculado con el Agente DG.")
        return redirect("seguimiento:detalle", pk=item.pk)

    accion = (request.POST.get("accion") or "").strip()
    try:
        if accion == "subpunto":
            try:
                idx = int(request.POST.get("sub_index") or -1)
            except ValueError:
                idx = -1
            subs = list(check.sub_checklist or [])
            if not (0 <= idx < len(subs)):
                messages.error(request, "Sub-punto inválido.")
                return redirect("seguimiento:detalle", pk=item.pk)
            subs[idx]["completado"] = not bool(subs[idx].get("completado"))
            items_api = [{"text": s.get("titulo", ""), "completed": bool(s.get("completado"))} for s in subs]
            patch_step(check.origen_step_id, checklist_items=items_api)
            check.sub_checklist = subs
            check.save(update_fields=["sub_checklist", "updated_at"])
            messages.success(request, "Sub-punto actualizado y sincronizado.")
        elif accion in {"iniciar", "enviar", "completar"}:
            status_map = {"iniciar": "IN_PROGRESS", "enviar": "SUBMITTED", "completar": "COMPLETED"}
            nuevo = status_map[accion]
            patch_step(check.origen_step_id, status=nuevo)
            check.estatus_origen = nuevo
            check.completado = nuevo == "COMPLETED"
            check.completado_por = request.user if check.completado else None
            check.completado_at = timezone.now() if check.completado else None
            check.save(update_fields=["estatus_origen", "completado", "completado_por", "completado_at", "updated_at"])
            messages.success(request, "Paso sincronizado con el Agente DG.")
        else:
            messages.error(request, "Acción no reconocida.")
            return redirect("seguimiento:detalle", pk=item.pk)
    except AgenteDGError as exc:
        logger.warning("Write-back Agente DG falló (step %s): %s", check.origen_step_id, exc)
        messages.error(request, "No se pudo sincronizar con el Agente DG. El cambio no se aplicó; intenta de nuevo.")
        return redirect("seguimiento:detalle", pk=item.pk)

    log_event(request.user, "seguimiento.writeback", "SeguimientoChecklistItem", check.pk, {"accion": accion})
    return redirect("seguimiento:detalle", pk=item.pk)


@login_required
@require_POST
def registrar_feedback(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    comentario = (request.POST.get("comentario") or "").strip()
    if not comentario:
        messages.error(request, "Escribe la retroalimentación antes de enviarla.")
        return redirect("seguimiento:detalle", pk=pk)
    es_revision_dg = can_review_seguimiento_global(request.user)
    SeguimientoComentario.objects.create(
        seguimiento=item,
        usuario=request.user,
        tipo=SeguimientoComentario.TIPO_REVISION_DG if es_revision_dg else SeguimientoComentario.TIPO_FEEDBACK,
        comentario=comentario,
    )
    if es_revision_dg:
        try:
            _writeback_agente_dg_item(item, accion="feedback", comentario=comentario)
        except Exception:
            logger.exception("Write-back feedback Agente DG falló (item %s)", item.pk)
    elif item.requiere_aprobacion:
        if item.estatus in {SeguimientoItem.ESTATUS_PENDIENTE, SeguimientoItem.ESTATUS_EN_PROCESO}:
            item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
            item.save(update_fields=["estatus", "updated_at"])
    elif item.estatus == SeguimientoItem.ESTATUS_PENDIENTE:
        item.estatus = SeguimientoItem.ESTATUS_EN_PROCESO
        item.save(update_fields=["estatus", "updated_at"])
    log_event(request.user, "seguimiento.retroalimentacion", "SeguimientoItem", item.pk, {"comentario": True})
    notificar_seguimiento_avance(item, actor=request.user, mensaje_extra=comentario[:160])
    if can_review_seguimiento_global(request.user):
        notificar_seguimiento_feedback_responsable(item, comentario=comentario, actor=request.user)
    messages.success(request, "Retroalimentación enviada.")
    return redirect("seguimiento:detalle", pk=item.pk)


@login_required
@require_POST
def subir_evidencia(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    archivo = request.FILES.get("archivo")
    if not archivo:
        messages.error(request, "Selecciona un archivo de evidencia.")
        return redirect("seguimiento:detalle", pk=pk)
    error_archivo = _validar_archivo_evidencia(archivo)
    if error_archivo:
        messages.error(request, error_archivo)
        return redirect("seguimiento:detalle", pk=pk)
    SeguimientoEvidencia.objects.create(
        seguimiento=item,
        usuario=request.user,
        archivo=archivo,
        nombre_original=archivo.name,
        comentario=(request.POST.get("comentario") or "").strip(),
    )
    if item.requiere_aprobacion:
        if item.estatus in {SeguimientoItem.ESTATUS_PENDIENTE, SeguimientoItem.ESTATUS_EN_PROCESO}:
            item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
            item.save(update_fields=["estatus", "updated_at"])
    elif item.estatus == SeguimientoItem.ESTATUS_PENDIENTE:
        item.estatus = SeguimientoItem.ESTATUS_EN_PROCESO
        item.save(update_fields=["estatus", "updated_at"])
    log_event(request.user, "seguimiento.evidencia", "SeguimientoItem", item.pk, {"archivo": archivo.name})
    notificar_seguimiento_avance(item, actor=request.user, mensaje_extra=f"Archivo: {archivo.name}")
    messages.success(request, "Evidencia subida.")
    return redirect("seguimiento:detalle", pk=item.pk)


@login_required
@require_POST
def solicitar_prorroga(request, pk):
    item = _get_item_para_usuario(request.user, pk)
    if not item.requiere_aprobacion:
        messages.error(request, "Las prórrogas aplican solo a minutas y proyectos.")
        return redirect("seguimiento:detalle", pk=item.pk)
    fecha_solicitada = parse_date((request.POST.get("fecha_solicitada") or "").strip())
    motivo = (request.POST.get("motivo") or "").strip()
    if not fecha_solicitada:
        messages.error(request, "Selecciona la nueva fecha solicitada.")
        return redirect("seguimiento:detalle", pk=pk)
    if fecha_solicitada <= timezone.localdate():
        messages.error(request, "La fecha solicitada debe ser posterior a hoy.")
        return redirect("seguimiento:detalle", pk=pk)
    if not motivo:
        messages.error(request, "Escribe el motivo para solicitar más tiempo.")
        return redirect("seguimiento:detalle", pk=pk)

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


@login_required
def bandeja_revision(request):
    if not can_review_seguimiento_global(request.user):
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


def _redirect_post_resolucion(request, pk):
    """Devuelve al DG al lugar de donde resolvió (panel, detalle, dashboard o bandeja)."""
    destino = (request.POST.get("next") or "").strip()
    if destino == "panel":
        return redirect("seguimiento:panel_dg")
    if destino == "detalle":
        return redirect("seguimiento:detalle_dg", pk=pk)
    if destino == "dashboard":
        return redirect("dashboard")
    return redirect("seguimiento:bandeja_revision")


@login_required
@require_POST
def resolver_revision(request, pk):
    from .agente_dg_client import AgenteDGError

    if not can_review_seguimiento_global(request.user):
        messages.error(request, "No tienes permiso para resolver revisiones.")
        return redirect("seguimiento:bandeja_revision")
    item = get_object_or_404(SeguimientoItem, pk=pk)
    accion = (request.POST.get("accion") or "").strip()
    comentario_texto = (request.POST.get("comentario") or "").strip()
    if accion == "aprobar":
        try:
            _writeback_agente_dg_item(item, accion="aprobar", comentario=comentario_texto)
        except AgenteDGError as exc:
            logger.warning("Write-back cierre Agente DG falló (item %s): %s", item.pk, exc)
            messages.error(request, "No se pudo cerrar en app.pollyanasdolce.com. El acuerdo no se marcó como completado en ERP; intenta de nuevo.")
            return _redirect_post_resolucion(request, pk)
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
        notificar_seguimiento_aprobado(item, comentario=comentario_texto, actor=request.user)
        messages.success(request, f"'{item.titulo[:60]}' marcado como completado.")
    elif accion == "devolver":
        if not comentario_texto:
            messages.error(request, "Escribe el motivo para devolver el acuerdo.")
            return _redirect_post_resolucion(request, pk)
        try:
            _writeback_agente_dg_item(item, accion="devolver", comentario=comentario_texto)
        except AgenteDGError as exc:
            logger.warning("Write-back devolucion Agente DG falló (item %s): %s", item.pk, exc)
            messages.error(request, "No se pudo devolver en app.pollyanasdolce.com. El acuerdo no se modificó en ERP; intenta de nuevo.")
            return _redirect_post_resolucion(request, pk)
        item.estatus = SeguimientoItem.ESTATUS_EN_PROCESO
        item.save(update_fields=["estatus", "updated_at"])
        SeguimientoComentario.objects.create(
            seguimiento=item, usuario=request.user,
            tipo=SeguimientoComentario.TIPO_REVISION_DG,
            comentario=f"[DEVUELTO] {comentario_texto}",
        )
        log_event(request.user, "seguimiento.devolver", "SeguimientoItem", item.pk, {})
        notificar_seguimiento_devuelto(item, comentario=comentario_texto, actor=request.user)
        messages.warning(request, f"'{item.titulo[:60]}' devuelto para corrección.")
    else:
        messages.error(request, "Acción no reconocida.")
    return _redirect_post_resolucion(request, pk)


@login_required
@require_POST
def resolver_prorroga(request, pk, prorroga_id):
    if not can_review_seguimiento_global(request.user):
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
    return _redirect_post_resolucion(request, pk)


@login_required
@require_POST
def crear_acuerdo_agente_dg(request):
    """Crea una minuta en app.pollyanasdolce.com y la espeja al ERP."""
    if not can_review_seguimiento_global(request.user):
        messages.error(request, "Acceso restringido a Dirección General.")
        return redirect("seguimiento:mi_seguimiento")

    from .agente_dg_client import AgenteDGError, create_minute_agreement, is_configured

    if not _writeback_activo() or not is_configured():
        messages.error(request, "La integración con app.pollyanasdolce.com no está activa para crear acuerdos.")
        return redirect("seguimiento:panel_dg")

    User = get_user_model()
    responsable = get_object_or_404(User, pk=request.POST.get("responsable_user_id"), is_active=True)
    titulo = (request.POST.get("titulo") or "").strip()
    descripcion = (request.POST.get("descripcion") or "").strip()
    fecha_raw = (request.POST.get("fecha_limite") or "").strip()
    hora_raw = (request.POST.get("hora_limite") or "18:00").strip() or "18:00"
    checklist_raw = (request.POST.get("checklist") or "").strip()

    if not titulo:
        messages.error(request, "El acuerdo necesita un título.")
        return redirect("seguimiento:panel_dg")
    fecha = parse_date(fecha_raw)
    if not fecha:
        messages.error(request, "La fecha límite no es válida.")
        return redirect("seguimiento:panel_dg")
    try:
        hora = datetime.strptime(hora_raw, "%H:%M").time()
    except ValueError:
        messages.error(request, "La hora límite no es válida.")
        return redirect("seguimiento:panel_dg")

    try:
        agente_user_id = _agente_dg_user_id_para_erp_user(responsable)
    except AgenteDGError as exc:
        logger.warning("No se pudo resolver usuario Agente DG para %s: %s", responsable.pk, exc)
        messages.error(request, "No se pudo consultar usuarios de app.pollyanasdolce.com para asignar el acuerdo.")
        return redirect("seguimiento:panel_dg")
    if not agente_user_id:
        messages.error(request, "Ese colaborador no tiene usuario ligado en app.pollyanasdolce.com; primero hay que crearlo o vincularlo.")
        return redirect("seguimiento:panel_dg")

    due_at = timezone.make_aware(datetime.combine(fecha, hora), timezone.get_current_timezone())
    checklist_items = [
        {"text": line.strip(), "completed": False}
        for line in checklist_raw.splitlines()
        if line.strip()
    ]
    payload = {
        "collaborator_user_id": agente_user_id,
        "title": titulo,
        "meeting_label": "ERP Seguimiento",
        "agreement_text": descripcion,
        "checklist_items": checklist_items or None,
        "due_at": due_at.isoformat(),
        "send_email_on_create": False,
        "send_whatsapp_on_create": False,
        "send_calendar_invite_on_create": False,
        "create_commitment": False,
    }
    try:
        created = create_minute_agreement(**payload)
    except AgenteDGError as exc:
        logger.warning("Crear minuta Agente DG falló: %s", exc)
        messages.error(request, "No se pudo crear el acuerdo en app.pollyanasdolce.com. No se creó en ERP para evitar desfase.")
        return redirect("seguimiento:panel_dg")

    record = _record_minuta_agente_dg(created, responsable_user=responsable)
    try:
        counters = upsert_agente_dg_payload({
            "source_table": "minute_agreements",
            "source_id": created.get("id"),
            "record": record,
        })
    except Exception:
        logger.exception("Minuta creada en Agente DG pero no pudo espejarse al ERP: %s", created.get("id"))
        messages.warning(
            request,
            "El acuerdo se creó en app.pollyanasdolce.com, pero no se pudo reflejar inmediatamente en ERP. "
            "El sincronizador debe importarlo en el siguiente ciclo.",
        )
        return redirect("seguimiento:panel_dg")
    item = SeguimientoItem.objects.filter(
        metadata__source="agente_dg",
        metadata__source_table="minute_agreements",
        metadata__source_id=created.get("id"),
    ).first()
    log_event(request.user, "seguimiento.agente_dg.crear", "SeguimientoItem", item.pk if item else 0, counters)
    messages.success(request, "Acuerdo creado en app.pollyanasdolce.com y reflejado en el ERP.")
    if item:
        return redirect("seguimiento:detalle_dg", pk=item.pk)
    return redirect("seguimiento:panel_dg")


@login_required
def panel_dg(request):
    if not can_review_seguimiento_global(request.user):
        messages.error(request, "Acceso restringido a Dirección General.")
        return redirect("seguimiento:mi_seguimiento")

    now = timezone.now()

    # Filtros desde GET
    filtro_tipo = (request.GET.get("tipo") or "").strip().upper()
    filtro_estatus = (request.GET.get("estatus") or "").strip().upper()
    filtro_colaborador = (request.GET.get("colaborador") or "").strip()
    filtro_vencidos = request.GET.get("vencidos") == "1"
    active_bucket = (request.GET.get("bucket") or "activos").strip().lower()
    if active_bucket not in PANEL_BUCKETS:
        active_bucket = "activos"

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

    items_base = list(qs)

    for item in items_base:
        checks = list(item.checklist.all())
        item.prorroga_pendiente = next(
            (p for p in item.prorrogas.all() if p.estatus == SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE), None
        )
        _aplicar_estado_visual_seguimiento(item, checks)
        item.ultima_actualizacion = item.updated_at
        item.responsable_nombre = (
            item.responsable_user.get_full_name() or item.responsable_user.username
            if item.responsable_user
            else (item.responsable_empleado.nombre if item.responsable_empleado else "Sin asignar")
        )
        if item.es_vencido_visual:
            item.urgencia = "danger"
        elif item.fecha_limite and item.fecha_limite <= now + timedelta(days=2) and not item.esta_cerrado:
            item.urgencia = "warn"
        else:
            item.urgencia = ""

    vista = request.GET.get("vista", "tabla")

    bucket_counts = {bucket: sum(1 for i in items_base if i.visual_bucket == bucket) for bucket in PANEL_BUCKETS}
    bucket_nav = [
        {"key": bucket, "label": label, "count": bucket_counts[bucket]}
        for bucket, label in PANEL_BUCKETS.items()
    ]
    items = [i for i in items_base if i.visual_bucket == active_bucket]

    active_tab = (request.GET.get("tab") or "").strip().upper()
    items_for_type_counts = list(items)
    if active_tab and active_tab in dict(SeguimientoItem.TIPO_CHOICES):
        items = [i for i in items if i.tipo == active_tab]
    else:
        active_tab = ""

    count_compromisos = sum(1 for i in items_for_type_counts if i.tipo == SeguimientoItem.TIPO_COMPROMISO)
    count_minutas = sum(1 for i in items_for_type_counts if i.tipo == SeguimientoItem.TIPO_MINUTA)
    count_proyectos = sum(1 for i in items_for_type_counts if i.tipo == SeguimientoItem.TIPO_PROYECTO)
    count_todos_tipo = len(items_for_type_counts)

    total = len(items)
    abiertos = sum(1 for i in items if not i.esta_cerrado)
    vencidos = sum(1 for i in items if i.es_vencido_visual)
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
        if item.es_vencido_visual:
            por_colaborador[key]["vencidos"] += 1
        if item.estatus == SeguimientoItem.ESTATUS_EN_REVISION:
            por_colaborador[key]["en_revision"] += 1
        if item.estatus == SeguimientoItem.ESTATUS_COMPLETADO:
            por_colaborador[key]["completados"] += 1

    colaboradores_resumen = sorted(
        por_colaborador.values(),
        key=lambda c: (-c["vencidos"], -c["en_revision"], -c["abiertos"]),
    )

    # Sección "Requiere tu acción": en revisión + prórrogas pendientes (sin importar filtros).
    from .services import items_pendientes_revision_dg, _responsable_nombre

    pendientes_accion = list(items_pendientes_revision_dg())
    for item in pendientes_accion:
        checks = list(item.checklist.all())
        _aplicar_estado_visual_seguimiento(item, checks)
        item.responsable_nombre = _responsable_nombre(item)
        item.ultima_evidencia = item.evidencias.order_by("-created_at").first()
        item.ultimo_comentario = item.comentarios.order_by("-created_at").first()
        item.prorroga_pendiente = next(
            (p for p in item.prorrogas.all() if p.estatus == SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE), None
        )
        item.es_en_revision = item.estatus == SeguimientoItem.ESTATUS_EN_REVISION

    usuarios_crear_acuerdo = (
        get_user_model().objects.filter(is_active=True)
        .exclude(username__startswith="service")
        .order_by("first_name", "last_name", "username")
    )

    return render(request, "seguimiento/panel_dg.html", {
        "items": items,
        "pendientes_accion": pendientes_accion,
        "pendientes_accion_total": len(pendientes_accion),
        "colaboradores_resumen": colaboradores_resumen,
        "vista": vista,
        "active_tab": active_tab,
        "active_bucket": active_bucket,
        "bucket_nav": bucket_nav,
        "bucket_counts": bucket_counts,
        "count_compromisos": count_compromisos,
        "count_minutas": count_minutas,
        "count_proyectos": count_proyectos,
        "count_todos_tipo": count_todos_tipo,
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
        "usuarios_crear_acuerdo": usuarios_crear_acuerdo,
        "writeback_activo": _writeback_activo(),
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
    if not item.requiere_aprobacion:
        messages.error(request, "Este tipo de acuerdo no requiere aprobación; márcalo como completado.")
        return redirect("seguimiento:detalle", pk=pk)

    if not _registrar_cierre_opcional(request, item, "ENTREGA"):
        return redirect("seguimiento:detalle", pk=pk)

    item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
    item.save(update_fields=["estatus", "updated_at"])
    log_event(request.user, "seguimiento.entrega", "SeguimientoItem", item.pk, {"entrega": True})
    notificar_seguimiento_entrega(item, actor=request.user)
    messages.success(request, "Acuerdo enviado a revisión. El Director General será notificado.")
    return redirect("seguimiento:detalle", pk=pk)


@login_required
@require_POST
def completar_directamente(request, pk):
    """Cierre directo para acuerdos sin aprobación requerida o con checklist 100%."""
    from .agente_dg_client import AgenteDGError

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

    comentario_texto = (request.POST.get("comentario") or "").strip()
    try:
        _writeback_agente_dg_item(item, accion="completar", comentario=comentario_texto)
    except AgenteDGError as exc:
        logger.warning("Write-back completar Agente DG falló (item %s): %s", item.pk, exc)
        messages.error(request, "No se pudo cerrar en app.pollyanasdolce.com. El acuerdo no se marcó como completado en ERP; intenta de nuevo.")
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
    _aplicar_estado_visual_seguimiento(item, checks)
    checklist_total = item.checklist_total
    checklist_done = item.checklist_done
    progreso_pct = item.progreso_pct

    now = timezone.now()
    if item.esta_vencido:
        prioridad_label, prioridad_tone = "Vencido", "danger"
    elif item.fecha_limite and item.fecha_limite <= now + timedelta(days=2) and not item.esta_cerrado:
        prioridad_label, prioridad_tone = "Alta", "warn"
    else:
        prioridad_label, prioridad_tone = "Normal", ""

    prorroga_pendiente = item.prorrogas.filter(estatus=SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE).first()
    comentarios = item.comentarios.select_related("usuario").order_by("created_at")
    conversacion = _conversacion_context(comentarios)
    evidencias = item.evidencias.select_related("usuario", "revisado_por").order_by("created_at")

    checklist_completo = bool(checks) and all(c.completado for c in checks)
    puede_cerrar_directo = not item.requiere_aprobacion or checklist_completo
    puede_entregar = not item.esta_cerrado and item.requiere_aprobacion and not checklist_completo

    tiene_revision_dg = comentarios.filter(tipo=SeguimientoComentario.TIPO_REVISION_DG).exists()
    puede_retractar = (
        item.estatus == SeguimientoItem.ESTATUS_EN_REVISION
        and not tiene_revision_dg
    )

    # Orden secuencial: solo el primer paso incompleto se puede marcar y solo el último
    # completado se puede deshacer. El resto queda bloqueado en la interfaz.
    siguiente_check_id = next((c.id for c in checks if not c.completado), None)
    ultimo_completado_id = next((c.id for c in reversed(checks) if c.completado), None)

    # Pasos de ESTE ítem donde el usuario logeado es aprobador y están en espera
    pasos_a_aprobar = [
        c for c in checks
        if c.aprobador_user_id == request.user.pk
        and c.requiere_aprobacion
        and not c.completado
        and c.estatus_origen == "SUBMITTED"
    ]

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
            "evidencias": evidencias,
            **conversacion,
            "puede_cerrar_directo": puede_cerrar_directo and not item.esta_cerrado,
            "puede_entregar": puede_entregar,
            "checklist_completo": checklist_completo,
            "estatus_en_revision": SeguimientoItem.ESTATUS_EN_REVISION,
            "puede_retractar": puede_retractar,
            "current_user": request.user,
            "siguiente_check_id": siguiente_check_id,
            "ultimo_completado_id": ultimo_completado_id,
            "writeback_activo": _writeback_activo(),
            "pasos_a_aprobar": pasos_a_aprobar,
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
    if not can_review_seguimiento_global(request.user):
        messages.error(request, "Acceso restringido a Dirección General.")
        return redirect("seguimiento:mi_seguimiento")
    item = get_object_or_404(
        SeguimientoItem.objects.select_related("responsable_user", "responsable_empleado", "aprobado_por")
        .prefetch_related("checklist__completado_por", "comentarios__usuario", "evidencias__usuario", "prorrogas"),
        pk=pk,
    )
    checks = list(item.checklist.all())
    _aplicar_estado_visual_seguimiento(item, checks)
    checklist_total = item.checklist_total
    checklist_done = item.checklist_done
    progreso_pct = item.progreso_pct
    now = timezone.now()
    if item.esta_vencido:
        prioridad_label, prioridad_tone = "Vencido", "danger"
    elif item.fecha_limite and item.fecha_limite <= now + timedelta(days=2) and not item.esta_cerrado:
        prioridad_label, prioridad_tone = "Alta", "warn"
    else:
        prioridad_label, prioridad_tone = "Normal", ""
    prorroga_pendiente = item.prorrogas.filter(estatus=SeguimientoProrrogaSolicitud.ESTATUS_PENDIENTE).first()
    comentarios = item.comentarios.select_related("usuario").order_by("created_at")
    conversacion = _conversacion_context(comentarios)
    evidencias = item.evidencias.select_related("usuario", "revisado_por").order_by("created_at")
    checklist_completo = bool(checks) and all(c.completado for c in checks)
    siguiente_check_id = next((c.id for c in checks if not c.completado), None)
    return render(request, "seguimiento/detalle_item.html", {
        "item": item,
        "checks": checks,
        "checklist_total": checklist_total,
        "checklist_done": checklist_done,
        "progreso_pct": progreso_pct,
        "prioridad_label": prioridad_label,
        "prioridad_tone": prioridad_tone,
        "prorroga_pendiente": prorroga_pendiente,
        "evidencias": evidencias,
        **conversacion,
        "puede_cerrar_directo": False,
        "puede_entregar": False,
        "checklist_completo": checklist_completo,
        "estatus_en_revision": SeguimientoItem.ESTATUS_EN_REVISION,
        "puede_retractar": False,
        "current_user": request.user,
        "siguiente_check_id": siguiente_check_id,
        "ultimo_completado_id": None,
        "writeback_activo": False,  # DG es solo lectura — nunca activa write-back
        "pasos_a_aprobar": [],      # DG no aprueba pasos individuales
        "es_vista_dg": True,
        "puede_resolver_dg": item.estatus == SeguimientoItem.ESTATUS_EN_REVISION,
    })


@login_required
@require_POST
def aprobar_paso_colaborador(request, pk, check_id):
    """Un colaborador aprueba o devuelve un paso en el que es el aprobador designado.

    El paso debe tener estatus_origen="SUBMITTED" (el responsable lo envió a revisión).
    Si write-back está activo, sincroniza contra el Agente DG. Si no, actualiza solo el ERP.
    El DG NO usa esta vista — los ítems completos los aprueba vía resolver_revision.
    """
    from .agente_dg_client import AgenteDGError, is_configured, patch_step

    # Solo quien es aprobador del paso puede actuar; 404 para cualquier otro
    check = get_object_or_404(
        SeguimientoChecklistItem,
        pk=check_id,
        seguimiento_id=pk,
        aprobador_user=request.user,
        requiere_aprobacion=True,
    )
    item = check.seguimiento

    if item.esta_cerrado:
        messages.error(request, "El proyecto está cerrado; no se pueden aprobar pasos.")
        return redirect("seguimiento:mi_seguimiento")

    accion = (request.POST.get("accion") or "").strip()
    if accion not in {"aprobar", "devolver"}:
        messages.error(request, "Acción no reconocida.")
        return redirect("seguimiento:mi_seguimiento")
    if check.estatus_origen != "SUBMITTED":
        messages.error(request, "Este paso todavía no está enviado a revisión.")
        return redirect("seguimiento:mi_seguimiento")

    motivo = (request.POST.get("motivo") or "").strip()

    wb_activo = _writeback_activo() and is_configured() and bool(check.origen_step_id)

    if accion == "aprobar":
        nuevo_estatus = "COMPLETED"
        completado = True
        msg_ok = "Paso aprobado."
        msg_wb = "Paso aprobado y sincronizado con el Agente DG."
    else:  # devolver
        nuevo_estatus = "IN_PROGRESS"
        completado = False
        msg_ok = "Paso devuelto al responsable para corrección."
        msg_wb = "Paso devuelto al responsable y sincronizado con el Agente DG."

    if wb_activo:
        try:
            patch_step(check.origen_step_id, status=nuevo_estatus)
        except AgenteDGError as exc:
            logger.warning("Write-back aprobacion-paso falló (step %s): %s", check.origen_step_id, exc)
            messages.error(request, "No se pudo sincronizar con el Agente DG. Intenta de nuevo.")
            return redirect("seguimiento:mi_seguimiento")
        messages.success(request, msg_wb)
    else:
        messages.success(request, msg_ok)

    check.estatus_origen = nuevo_estatus
    check.completado = completado
    check.completado_por = request.user if completado else None
    check.completado_at = timezone.now() if completado else None
    check.save(update_fields=["estatus_origen", "completado", "completado_por", "completado_at", "updated_at"])

    # Si hay motivo al devolver, guardarlo como comentario en el item
    if accion == "devolver" and motivo:
        texto = f"Paso devuelto por {request.user.get_full_name() or request.user.username}: {motivo}"
        SeguimientoComentario.objects.create(
            seguimiento=item,
            usuario=request.user,
            comentario=texto[:1000],
            tipo=SeguimientoComentario.TIPO_FEEDBACK,
        )

    # Notificar al responsable del proyecto
    try:
        if accion == "aprobar":
            notificar_paso_aprobado_por_colaborador(check, actor=request.user)
        else:
            notificar_paso_devuelto_por_colaborador(check, motivo=motivo, actor=request.user)
    except Exception:
        logger.exception("Error al enviar notificación de aprobación de paso colaborador (step %s)", check.pk)

    log_event(
        request.user,
        f"seguimiento.aprobacion_paso.{accion}",
        "SeguimientoChecklistItem",
        check.pk,
        {"seguimiento_id": item.pk, "writeback": wb_activo, "tiene_motivo": bool(motivo)},
    )
    return redirect("seguimiento:mi_seguimiento")


def _nombre_usuario_legible(user) -> str:
    if not user:
        return ""
    return user.get_full_name() or user.username


def _fecha_hora_local(value):
    if not value:
        return None, None
    local_value = timezone.localtime(value)
    return local_value.date(), local_value.strftime("%H:%M")


def _actividad_evento(actividad: ActividadCalendario, request_user) -> dict:
    completada = actividad.estatus == ActividadCalendario.ESTATUS_COMPLETADA
    return {
        "id": f"act-{actividad.pk}",
        "fuente": "actividad",
        "tipo": "ACTIVIDAD",
        "source_label": "Actividad personal",
        "titulo": actividad.titulo,
        "fecha": actividad.fecha.isoformat(),
        "hora": actividad.hora_inicio.strftime("%H:%M") if actividad.hora_inicio else None,
        "hora_fin": actividad.hora_fin.strftime("%H:%M") if actividad.hora_fin else None,
        "estatus": actividad.estatus,
        "finalizado": completada,
        "vencido": actividad.fecha < timezone.localdate() and not completada,
        "url": None,
        "accion_label": "Editar actividad",
        "responsable": _nombre_usuario_legible(actividad.usuario),
        "descripcion": actividad.descripcion,
        "editable": actividad.usuario_id == request_user.id,
    }


def _seguimiento_url_calendario(item: SeguimientoItem, request_user) -> str:
    if can_review_seguimiento_global(request_user):
        return f"/seguimiento/panel/{item.pk}/"
    return f"/seguimiento/{item.pk}/"


def _item_evento(item: SeguimientoItem, request_user) -> dict:
    fecha, hora = _fecha_hora_local(item.fecha_limite)
    responsable = _nombre_usuario_legible(item.responsable_user) or getattr(item.responsable_empleado, "nombre", "")
    completado = item.estatus in {SeguimientoItem.ESTATUS_COMPLETADO, SeguimientoItem.ESTATUS_CANCELADO}
    source = _agente_dg_source(item)
    return {
        "id": f"item-{item.pk}",
        "fuente": "seguimiento",
        "tipo": item.tipo,
        "source_label": item.get_tipo_display(),
        "titulo": item.titulo,
        "fecha": fecha.isoformat() if fecha else "",
        "hora": hora,
        "hora_fin": None,
        "estatus": item.estatus,
        "finalizado": completado,
        "vencido": bool(fecha and fecha < timezone.localdate() and not completado),
        "url": _seguimiento_url_calendario(item, request_user),
        "accion_label": "Ver seguimiento",
        "responsable": responsable or None,
        "descripcion": item.entregable_esperado or item.descripcion,
        "source_table": source[0] if source else "",
        "source_id": source[1] if source else None,
        "editable": False,
    }


def _checklist_evento(check: SeguimientoChecklistItem, request_user) -> dict:
    fecha, hora = _fecha_hora_local(check.vence)
    item = check.seguimiento
    responsable = _nombre_usuario_legible(item.responsable_user) or getattr(item.responsable_empleado, "nombre", "")
    source = _agente_dg_source(item)
    return {
        "id": f"paso-{check.pk}",
        "fuente": "checklist",
        "tipo": "PASO",
        "source_label": "Paso",
        "titulo": check.titulo,
        "fecha": fecha.isoformat() if fecha else "",
        "hora": hora,
        "hora_fin": None,
        "estatus": "COMPLETADO" if check.completado else (check.estatus_origen or "PENDIENTE"),
        "finalizado": check.completado,
        "vencido": bool(fecha and fecha < timezone.localdate() and not check.completado),
        "url": _seguimiento_url_calendario(item, request_user),
        "accion_label": "Ver seguimiento",
        "responsable": responsable or None,
        "descripcion": check.entregable or item.entregable_esperado,
        "source_table": source[0] if source else "",
        "source_id": source[1] if source else None,
        "editable": False,
    }


def _validar_rango_calendario(request):
    start = parse_date((request.GET.get("start") or "").strip())
    end = parse_date((request.GET.get("end") or "").strip())
    if not start or not end:
        return None, None, JsonResponse({"error": "Parámetros start y end requeridos en formato YYYY-MM-DD."}, status=400)
    if end < start:
        return None, None, JsonResponse({"error": "El rango de fechas no es válido."}, status=400)
    if (end - start).days > 62:
        return None, None, JsonResponse({"error": "El rango máximo permitido es de 62 días."}, status=400)
    return start, end, None


def _day_bounds(start, end):
    tz = timezone.get_current_timezone()
    start_dt = timezone.make_aware(datetime.combine(start, datetime.min.time()), tz)
    end_dt = timezone.make_aware(datetime.combine(end, datetime.max.time()), tz)
    return start_dt, end_dt


def _parse_hora_calendario(raw: str, campo: str):
    raw = (raw or "").strip()
    if not raw:
        return None, None
    hora = parse_time(raw)
    if hora is None:
        return None, f"La hora de {campo} no es válida."
    return hora, None


def _datos_actividad_post(request):
    titulo = (request.POST.get("titulo") or "").strip()
    descripcion = (request.POST.get("descripcion") or "").strip()
    fecha = parse_date((request.POST.get("fecha") or "").strip())
    hora_inicio, error_inicio = _parse_hora_calendario(request.POST.get("hora_inicio") or "", "inicio")
    hora_fin, error_fin = _parse_hora_calendario(request.POST.get("hora_fin") or "", "fin")

    if not titulo:
        return None, "El título es obligatorio."
    if not fecha:
        return None, "La fecha es obligatoria."
    if error_inicio:
        return None, error_inicio
    if error_fin:
        return None, error_fin
    if hora_inicio and hora_fin and hora_fin <= hora_inicio:
        return None, "La hora de fin debe ser posterior a la hora de inicio."
    return {
        "titulo": titulo,
        "descripcion": descripcion,
        "fecha": fecha,
        "hora_inicio": hora_inicio,
        "hora_fin": hora_fin,
    }, None


@login_required
def calendario(request):
    es_dg = can_review_seguimiento_global(request.user)
    contexto = {"es_dg": es_dg}
    if es_dg:
        User = get_user_model()
        contexto["colaboradores"] = (
            User.objects.filter(is_active=True)
            .filter(
                Q(seguimiento_items__isnull=False)
                | Q(seguimiento_participaciones__isnull=False)
                | Q(actividades_calendario__isnull=False)
            )
            .distinct()
            .order_by("first_name", "last_name", "username")
        )
    return render(request, "seguimiento/calendario.html", contexto)


@login_required
def calendario_eventos(request):
    start, end, error_response = _validar_rango_calendario(request)
    if error_response:
        return error_response

    es_dg = can_review_seguimiento_global(request.user)
    usuario_objetivo = None
    if es_dg and (request.GET.get("usuario") or "").strip():
        usuario_objetivo = get_object_or_404(get_user_model(), pk=request.GET.get("usuario"), is_active=True)
    elif not es_dg:
        usuario_objetivo = request.user

    start_dt, end_dt = _day_bounds(start, end)
    if usuario_objetivo:
        items_qs = _items_del_usuario(usuario_objetivo)
    else:
        items_qs = SeguimientoItem.objects.select_related("responsable_user", "responsable_empleado").prefetch_related("checklist")

    items_qs = (
        items_qs.filter(fecha_limite__gte=start_dt, fecha_limite__lte=end_dt)
        .exclude(estatus=SeguimientoItem.ESTATUS_CANCELADO)
        .distinct()
    )
    checklist_qs = (
        SeguimientoChecklistItem.objects.filter(
            seguimiento__in=items_qs.values("pk"),
            vence__gte=start_dt,
            vence__lte=end_dt,
        )
        .select_related("seguimiento__responsable_user", "seguimiento__responsable_empleado")
        .distinct()
    )
    actividades_qs = (
        ActividadCalendario.objects.filter(activo=True, fecha__gte=start, fecha__lte=end)
        .select_related("usuario")
        .order_by("fecha", "hora_inicio", "id")
    )
    if usuario_objetivo:
        actividades_qs = actividades_qs.filter(usuario=usuario_objetivo)
    else:
        actividades_qs = actividades_qs.filter(usuario__is_active=True)

    eventos = []
    eventos.extend(_item_evento(item, request.user) for item in items_qs)
    eventos.extend(_checklist_evento(check, request.user) for check in checklist_qs)
    eventos.extend(_actividad_evento(actividad, request.user) for actividad in actividades_qs)
    eventos = [evento for evento in eventos if evento.get("fecha")]
    eventos.sort(key=lambda event: (event["fecha"], event.get("hora") or "99:99", event["titulo"].lower()))
    return JsonResponse({"eventos": eventos})


@login_required
@require_POST
def actividad_crear(request):
    datos, error = _datos_actividad_post(request)
    if error:
        return JsonResponse({"error": error}, status=400)
    actividad = ActividadCalendario.objects.create(usuario=request.user, **datos)
    log_event(
        request.user,
        "seguimiento.calendario.crear",
        "ActividadCalendario",
        actividad.pk,
        {"fecha": actividad.fecha.isoformat()},
    )
    return JsonResponse({"evento": _actividad_evento(actividad, request.user)})


@login_required
@require_POST
def actividad_editar(request, pk):
    actividad = get_object_or_404(ActividadCalendario, pk=pk, usuario=request.user, activo=True)
    datos, error = _datos_actividad_post(request)
    if error:
        return JsonResponse({"error": error}, status=400)
    for campo, valor in datos.items():
        setattr(actividad, campo, valor)
    actividad.save(update_fields=["titulo", "descripcion", "fecha", "hora_inicio", "hora_fin", "updated_at"])
    return JsonResponse({"evento": _actividad_evento(actividad, request.user)})


@login_required
@require_POST
def actividad_completar(request, pk):
    actividad = get_object_or_404(ActividadCalendario, pk=pk, usuario=request.user, activo=True)
    if actividad.estatus == ActividadCalendario.ESTATUS_COMPLETADA:
        actividad.estatus = ActividadCalendario.ESTATUS_PENDIENTE
    else:
        actividad.estatus = ActividadCalendario.ESTATUS_COMPLETADA
    actividad.save(update_fields=["estatus", "updated_at"])
    return JsonResponse({"evento": _actividad_evento(actividad, request.user)})


@login_required
@require_POST
def actividad_eliminar(request, pk):
    actividad = get_object_or_404(ActividadCalendario, pk=pk, usuario=request.user, activo=True)
    actividad.activo = False
    actividad.save(update_fields=["activo", "updated_at"])
    log_event(
        request.user,
        "seguimiento.calendario.eliminar",
        "ActividadCalendario",
        actividad.pk,
        {"fecha": actividad.fecha.isoformat()},
    )
    return JsonResponse({"ok": True})
