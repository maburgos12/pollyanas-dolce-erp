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
    if request_obj.telegramhoursupdate_set.exists():
        from horarios_especiales.services.telegram_bot import require_approver
        require_approver(actor)
        if request_obj.approved_by_id != actor.pk:
            from django.core.exceptions import PermissionDenied
            raise PermissionDenied("La ejecución requiere al aprobador Telegram original.")
    return execute_request(request_obj=request_obj, actor=actor)


@shared_task(name="horarios_especiales.telegram_poll", acks_late=True, soft_time_limit=50, time_limit=60)
def telegram_hours_poll_task(config_dir=None):
    from horarios_especiales.services.telegram_bot import TelegramUnavailable, load_config, poll_once
    try:
        return poll_once(load_config(config_dir))
    except TelegramUnavailable:
        return {"telegram": "unavailable", "retry": "next_tick"}


@shared_task(name="horarios_especiales.reconcile_approved")
def reconcile_approved_hours_task():
    from io import StringIO
    from django.core.management import call_command
    # Existing bounded reconciler owns publication and its audit trail.
    call_command("reconcile_special_hours", stdout=StringIO(), stderr=StringIO())
