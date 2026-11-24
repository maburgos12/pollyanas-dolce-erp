"""Solicitudes durables de carga: mismo worker y monitor para ERP y app."""
import logging
from datetime import timedelta

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from core.access import can_manage_submodule
from pos_bridge.models import PointSyncJob
from .models import RutaEntrega
from .services_carga_ruta import (
    ERROR_SYNC_CARGA_VENCIDA, MODO_SYNC_CARGA_REPARTIDOR,
    VIGENCIA_SYNC_CARGA_REPARTIDOR, _tramo_carga_actual,
)

logger = logging.getLogger(__name__)


def contexto_carga_point(ruta):
    recarga, ordenes = _tramo_carga_actual(ruta)
    return {"fecha": str(ruta.fecha_ruta), "recarga": recarga,
            "ordenes": sorted(ordenes) if ordenes is not None else None}


def validar_contexto_carga_erp(*, ruta, user, contexto):
    if not user or not user.is_active or not can_manage_submodule(user, "logistica", "rutas"):
        raise PermissionDenied("No tienes permisos para actualizar la carga de esta ruta.")
    if ruta.estatus not in {RutaEntrega.ESTATUS_PLANEADA, RutaEntrega.ESTATUS_EN_RUTA}:
        raise ValidationError("La ruta está cerrada o cancelada; no se actualizará su carga.")
    if contexto != contexto_carga_point(ruta):
        raise ValidationError("El tramo o la fecha cambiaron. Solicita una nueva actualización.")


def respuesta_carga_point(job):
    if job.status == PointSyncJob.STATUS_SUCCESS:
        return {**job.result_summary, "solicitud_id": job.pk}, 200
    if job.status == PointSyncJob.STATUS_FAILED:
        return {"estado": "ERROR", "detail": job.error_message, "solicitud_id": job.pk}, 200
    if job.started_at <= timezone.now() - VIGENCIA_SYNC_CARGA_REPARTIDOR:
        return {"estado": "ERROR", "detail": ERROR_SYNC_CARGA_VENCIDA, "solicitud_id": job.pk}, 200
    return {"estado": job.status, "solicitud_id": job.pk}, 202


def solicitar_carga_point_erp(*, ruta, user):
    # La transacción sólo reclama la solicitud; nunca espera Point con filas bloqueadas.
    with transaction.atomic():
        ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
        contexto = contexto_carga_point(ruta)
        validar_contexto_carga_erp(ruta=ruta, user=user, contexto=contexto)
        solicitudes = PointSyncJob.objects.filter(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS, triggered_by=user,
            parameters__mode=MODO_SYNC_CARGA_REPARTIDOR, parameters__origen="erp",
            parameters__ruta_id=ruta.pk, parameters__contexto=contexto,
        )
        ultima = solicitudes.first()
        ahora = timezone.now()
        if ultima and (
            (ultima.finished_at and ultima.finished_at > ahora - timedelta(seconds=20))
            or (not ultima.finished_at and ultima.started_at > ahora - VIGENCIA_SYNC_CARGA_REPARTIDOR)
        ):
            return ultima
        job = solicitudes.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS, triggered_by=user,
            parameters={"mode": MODO_SYNC_CARGA_REPARTIDOR, "origen": "erp",
                        "ruta_id": ruta.pk, "recarga_cedis_id": contexto["recarga"], "contexto": contexto},
        )
    from .tasks import actualizar_carga_point_repartidor
    try:
        actualizar_carga_point_repartidor.apply_async(
            kwargs={"job_id": job.pk}, expires=VIGENCIA_SYNC_CARGA_REPARTIDOR.total_seconds(),
            retry=True, retry_policy={"max_retries": 0},
        )
    except Exception:
        logger.exception("No se pudo encolar carga Point ERP %s", job.pk)
        PointSyncJob.objects.filter(pk=job.pk, status=PointSyncJob.STATUS_PENDING).update(
            status=PointSyncJob.STATUS_FAILED, finished_at=timezone.now(),
            error_message="No se pudo solicitar la actualización. Vuelve a intentarlo.",
        )
        job.refresh_from_db()
    return job
