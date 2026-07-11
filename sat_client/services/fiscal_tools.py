from __future__ import annotations

from typing import Any

from django.conf import settings

from sat_client.models import CfdiDescargado, LogDescargaSat, SolicitudDescarga, SolicitudDocumentoSat


def estado_sat() -> dict[str, Any]:
    ultimo_log = LogDescargaSat.objects.order_by("-creado_en").first()
    solicitudes_abiertas = SolicitudDescarga.objects.exclude(
        estado__in=[
            SolicitudDescarga.ESTADO_TERMINADA,
            SolicitudDescarga.ESTADO_ERROR,
            SolicitudDescarga.ESTADO_RECHAZADA,
            SolicitudDescarga.ESTADO_VENCIDA,
        ]
    ).count()
    configured = all(
        [
            getattr(settings, "SAT_EFIRMA_CER_PATH", ""),
            getattr(settings, "SAT_EFIRMA_KEY_PATH", ""),
            getattr(settings, "SAT_EFIRMA_PASSWORD", ""),
            getattr(settings, "SAT_RFC", ""),
        ]
    )
    return {
        "enabled": getattr(settings, "SAT_DESCARGA_ENABLED", False),
        "configured": configured,
        "ultimo_log": ultimo_log,
        "solicitudes_abiertas": solicitudes_abiertas,
        "cfdis_total": CfdiDescargado.objects.count(),
    }


def documentos_sat_estado() -> list[dict[str, Any]]:
    recientes = {}
    for item in SolicitudDocumentoSat.objects.order_by("-creado_en"):
        recientes.setdefault(item.tipo, item)
    return [
        {
            "tipo": tipo,
            "nombre": nombre,
            "solicitud": recientes.get(tipo),
        }
        for tipo, nombre in SolicitudDocumentoSat.TIPO_CHOICES
    ]
