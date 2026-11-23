import json
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from logistica.models import RutaEntrega
from logistica.services_carga_ruta import (
    MODO_SYNC_CARGA_REPARTIDOR,
    VIGENCIA_SYNC_CARGA_REPARTIDOR,
)
from pos_bridge.models import PointSyncJob


class Command(BaseCommand):
    help = "Lista fallos técnicos y bloqueos operativos de carga Point de las últimas 24 horas."
    requires_system_checks = []

    def handle(self, *args, **options):
        ahora = timezone.now()
        solicitudes_recientes = PointSyncJob.objects.filter(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            parameters__mode=MODO_SYNC_CARGA_REPARTIDOR,
            started_at__gte=ahora - timedelta(days=1),
        )
        solicitudes = list(solicitudes_recientes.filter(
            Q(
                status__in=[PointSyncJob.STATUS_PENDING, PointSyncJob.STATUS_RUNNING],
                started_at__lte=ahora - VIGENCIA_SYNC_CARGA_REPARTIDOR,
            )
            | Q(status=PointSyncJob.STATUS_FAILED)
            | Q(status=PointSyncJob.STATUS_SUCCESS, result_summary__estado__in=["SIN_CARGA", "PENDIENTE_ENVIO"]),
        ).order_by("id").values_list("id", "parameters", "started_at", "status", "result_summary"))
        if not solicitudes:
            return "[]"
        exitos = {
            (parametros["ruta_id"], parametros.get("recarga_cedis_id")): final
            for parametros, final in solicitudes_recientes.filter(
                status=PointSyncJob.STATUS_SUCCESS, result_summary__estado="ACTUALIZADA", finished_at__isnull=False,
            ).order_by("finished_at").values_list("parameters", "finished_at")
        }
        rutas = {
            pk: folio
            for pk, folio in RutaEntrega.objects.filter(
                pk__in={parametros["ruta_id"] for _, parametros, _, _, _ in solicitudes},
            ).values_list("pk", "folio")
        }
        alertas = []
        for job_id, parametros, inicio, status, result in solicitudes:
            ruta_id = parametros["ruta_id"]
            recarga_id = parametros.get("recarga_cedis_id")
            folio = rutas.get(ruta_id)
            posterior = exitos.get((ruta_id, recarga_id))
            sync_posterior = bool(posterior and posterior > inicio)
            estado = "VENCIDA"
            if status == PointSyncJob.STATUS_FAILED:
                estado = "ERROR"
            elif status == PointSyncJob.STATUS_SUCCESS:
                estado = result["estado"]
            alertas.append({
                "id": job_id,
                "estado": estado,
                "clave": f"{ruta_id}:{recarga_id or 0}:{timezone.localtime(inicio).date()}:{estado}",
                "ruta_id": ruta_id,
                "folio": folio,
                "recarga_id": recarga_id,
                "inicio": timezone.localtime(inicio).strftime("%d/%m %H:%M"),
                "sync_posterior": sync_posterior,
            })
        return json.dumps(alertas)
