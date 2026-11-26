"""Estados firmados de Meta para avisos de clima; no recibe respuestas de encuesta."""

import hashlib
import hmac
import logging
from datetime import datetime, timedelta, timezone as utc_timezone

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .models import AvisoClima, EstadoMetaClimaPendiente

logger = logging.getLogger(__name__)
ESTADOS = {"sent": "ENVIADO", "delivered": "ENTREGADO", "read": "LEIDO", "failed": "FALLIDO"}
PRIORIDAD = {"ENVIADO": 1, "FALLIDO": 2, "ENTREGADO": 3, "LEIDO": 4}


def _digest_referencia(referencia):
    return hmac.new(settings.SECRET_KEY.encode(), f"clima-meta:{referencia}".encode(), hashlib.sha256).hexdigest()


def _aplicar(aviso, estado, evento_en):
    anterior = PRIORIDAD.get(aviso.entrega_estado, 0)
    nueva = PRIORIDAD[estado]
    if nueva < anterior or (nueva == anterior and aviso.entrega_evento_en and evento_en <= aviso.entrega_evento_en):
        return False
    aviso.entrega_estado = estado
    aviso.entrega_evento_en = evento_en
    aviso.save(update_fields=["entrega_estado", "entrega_evento_en"])
    return True


def registrar_estado(referencia, estado_meta, timestamp):
    """Idempotente ante reenvíos y eventos fuera de orden; conserva un race temprano."""
    estado = ESTADOS.get(estado_meta)
    if not estado or not isinstance(referencia, str) or not referencia or len(referencia) > 180:
        return False
    try:
        evento_en = datetime.fromtimestamp(int(timestamp), tz=utc_timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return False
    if evento_en > timezone.now() + timedelta(days=1):
        return False
    with transaction.atomic():
        avisos = list(AvisoClima.objects.select_for_update().filter(referencia_externa=referencia)[:2])
        if len(avisos) > 1:
            logger.error("Referencia de Meta duplicada entre avisos de clima")
            return False
        if avisos:
            return _aplicar(avisos[0], estado, evento_en)
        if not AvisoClima.objects.filter(estado="INCIERTO", intentado_en__gte=timezone.now() - timedelta(minutes=5)).exists():
            return False
        EstadoMetaClimaPendiente.objects.get_or_create(
            referencia_digest=_digest_referencia(referencia), estado=estado, evento_en=evento_en,
        )
    return True


def conciliar_estado(aviso_id):
    """Aplica eventos recibidos antes de que la respuesta de envío guardara el wamid."""
    with transaction.atomic():
        aviso = AvisoClima.objects.select_for_update().filter(pk=aviso_id).first()
        if not aviso or not aviso.referencia_externa:
            return 0
        pendientes = list(EstadoMetaClimaPendiente.objects.select_for_update().filter(
            referencia_digest=_digest_referencia(aviso.referencia_externa)
        ).order_by("evento_en", "id"))
        for pendiente in pendientes:
            _aplicar(aviso, pendiente.estado, pendiente.evento_en)
        if pendientes:
            EstadoMetaClimaPendiente.objects.filter(pk__in=[p.pk for p in pendientes]).delete()
        return len(pendientes)


def limpiar_pendientes():
    """Descarta identificadores no vinculados después de dos días."""
    borrados, _ = EstadoMetaClimaPendiente.objects.filter(
        recibido_en__lt=timezone.now() - timedelta(days=2)
    ).delete()
    return borrados


def conciliar_pendientes_recientes():
    """Recupera la carrera entre el webhook y la persistencia de la respuesta API."""
    if not EstadoMetaClimaPendiente.objects.exists():
        return 0
    conciliados = 0
    ids = AvisoClima.objects.filter(
        referencia_externa__gt="", intentado_en__gte=timezone.now() - timedelta(days=2)
    ).values_list("pk", flat=True)
    for aviso_id in ids.iterator():
        conciliados += conciliar_estado(aviso_id)
    return conciliados
