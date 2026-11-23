from __future__ import annotations

from celery import current_app, shared_task

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


@shared_task(name="horarios_especiales.reconcile_approved", soft_time_limit=20, time_limit=30)
def reconcile_approved_hours_task():
    from datetime import timedelta
    from time import monotonic

    from django.db import transaction
    from django.db.models import DateTimeField, Q, Value
    from django.db.models.fields.json import KeyTextTransform
    from django.db.models.functions import Cast, NullIf
    from django.utils import timezone

    now = timezone.now()
    due = SolicitudHorarioEspecial.objects.filter(
        canonical_payload__schema_version=2,
        status__in=["APROBADO", "EJECUTADO", "FALLIDO"],
        approved_at__isnull=False, cancelled_at__isnull=True,
    ).exclude(execution_summary_json__contains={"projection_expired": True}).annotate(
        reconcile_at=Cast(NullIf(KeyTextTransform("next_reconcile_at", "execution_summary_json"), Value("")), DateTimeField()),
    ).filter(Q(reconcile_at__isnull=True) | Q(reconcile_at__lte=now))
    deadline = monotonic() + 10
    queued = 0
    try:
        with current_app.connection_for_write(connect_timeout=2, transport_options={
            **current_app.conf.broker_transport_options,
            "socket_connect_timeout": 2, "socket_timeout": 2, "retry_on_timeout": False,
        }) as broker:
            broker.ensure_connection(max_retries=0)
            # Redis sums all priority shards; an empty queue must be allowed to declare.
            depth = broker.default_channel.queue_declare(queue="hours_publish").message_count
            for request_id in due.order_by("pk").values_list("pk", flat=True)[:max(0, 100 - depth)]:
                if monotonic() >= deadline:
                    break
                with transaction.atomic():
                    obj = due.select_for_update(skip_locked=True).filter(pk=request_id).first()
                    if obj is None:
                        continue
                    # Expiry precedes the reservation; queue depth also bounds undrained expired messages.
                    obj.execution_summary_json = {**obj.execution_summary_json,
                        "next_reconcile_at": (now + timedelta(seconds=900)).isoformat()}
                    obj.save(update_fields=["execution_summary_json"])
                    execute_special_hours_request_task.apply_async(
                        kwargs={"request_id": obj.pk, "actor_id": obj.approved_by_id},
                        expires=840, priority=9, retry=False, connection=broker,
                    )
                queued += 1
    except Exception:
        # Broker/DB failure rolls back the current reservation; retry on the next beat tick.
        return {"queued": queued, "failed": 1}
    return {"queued": queued, "failed": 0}
