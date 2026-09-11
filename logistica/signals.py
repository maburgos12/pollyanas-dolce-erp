import logging

from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import ReporteUnidad
from .tasks import notificar_reporte_nuevo

logger = logging.getLogger(__name__)


@receiver(post_save, sender=ReporteUnidad)
def reporte_unidad_post_save(sender, instance, created, **kwargs):
    if not created:
        return
    try:
        notificar_reporte_nuevo.delay(instance.id)
    except Exception as exc:
        # El aviso es accesorio. Sin este guardado, un broker caído impedía
        # cerrar la inspección diaria y el repartidor no podía salir a ruta.
        logger.warning("[logistica] No se pudo encolar el aviso del reporte %s: %s", instance.id, exc)
