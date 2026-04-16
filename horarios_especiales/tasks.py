from __future__ import annotations

from celery import shared_task

from horarios_especiales.models import SolicitudHorarioEspecial
from horarios_especiales.services.execution import execute_request


@shared_task(
    name="horarios_especiales.execute_request",
    bind=True,
    max_retries=2,
    default_retry_delay=300,
    acks_late=True,
)
def execute_special_hours_request_task(self, *, request_id: int, actor_id: int | None = None):
    del self
    actor = None
    if actor_id:
        from django.contrib.auth import get_user_model

        actor = get_user_model().objects.filter(id=actor_id).first()
    request_obj = SolicitudHorarioEspecial.objects.get(id=request_id)
    return execute_request(request_obj=request_obj, actor=actor)

