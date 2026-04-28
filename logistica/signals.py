from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import ReporteUnidad
from .tasks import notificar_reporte_nuevo


@receiver(post_save, sender=ReporteUnidad)
def reporte_unidad_post_save(sender, instance, created, **kwargs):
    if created:
        notificar_reporte_nuevo.delay(instance.id)
