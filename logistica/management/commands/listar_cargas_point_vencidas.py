import json
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from logistica.models import RutaEntrega
from logistica.services_carga_ruta import (
    ERROR_SYNC_CARGA_VENCIDA,
    MODO_SYNC_CARGA_REPARTIDOR,
    VIGENCIA_SYNC_CARGA_REPARTIDOR,
)
from pos_bridge.models import PointSyncJob


class Command(BaseCommand):
    help = "Lista solicitudes de carga Point vencidas en las últimas 24 horas sin modificar rutas."
    requires_system_checks = []

    def handle(self, *args, **options):
        ahora = timezone.now()
        solicitudes_recientes = PointSyncJob.objects.filter(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            parameters__mode=MODO_SYNC_CARGA_REPARTIDOR,
            started_at__gte=ahora - timedelta(days=1),
        )
        solicitudes = list(solicitudes_recientes.filter(
            Q(status__in=[PointSyncJob.STATUS_PENDING, PointSyncJob.STATUS_RUNNING])
            | Q(status=PointSyncJob.STATUS_FAILED, error_message=ERROR_SYNC_CARGA_VENCIDA),
            started_at__lte=ahora - VIGENCIA_SYNC_CARGA_REPARTIDOR,
        ).order_by("id").values_list("id", "parameters", "started_at"))
        if not solicitudes:
            return "[]"
        exitos = {
            (parametros["ruta_id"], parametros.get("recarga_cedis_id")): final
            for parametros, final in solicitudes_recientes.filter(
                status=PointSyncJob.STATUS_SUCCESS, finished_at__isnull=False,
            ).order_by("finished_at").values_list("parameters", "finished_at")
        }
        rutas = {
            pk: (folio, sincronizado_en)
            for pk, folio, sincronizado_en in RutaEntrega.objects.filter(
                pk__in={parametros["ruta_id"] for _, parametros, _ in solicitudes},
            ).values_list("pk", "folio", "checklist_carga__sincronizado_en")
        }
        alertas = []
        for job_id, parametros, inicio in solicitudes:
            ruta_id = parametros["ruta_id"]
            recarga_id = parametros.get("recarga_cedis_id")
            folio, sincronizado_en = rutas.get(ruta_id, (None, None))
            posterior = exitos.get((ruta_id, recarga_id))
            sync_posterior = bool(posterior and posterior > inicio)
            if recarga_id is None and sincronizado_en and sincronizado_en > inicio:
                sync_posterior = True
            alertas.append({
                "id": job_id,
                "ruta_id": ruta_id,
                "folio": folio,
                "recarga_id": recarga_id,
                "inicio": timezone.localtime(inicio).strftime("%d/%m %H:%M"),
                "sync_posterior": sync_posterior,
            })
        return json.dumps(alertas)
