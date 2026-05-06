from __future__ import annotations

from celery import shared_task

from orquestacion.services.rule_runners import run_rule_by_code


def _resolve_user(user_id: int | None):
    if not user_id:
        return None
    from django.contrib.auth import get_user_model

    user_model = get_user_model()
    try:
        return user_model.objects.get(id=user_id)
    except user_model.DoesNotExist:
        return None


@shared_task(
    name="orquestacion.run_rule",
    bind=True,
    acks_late=True,
    max_retries=1,
    default_retry_delay=300,
    time_limit=900,
    soft_time_limit=840,
)
def task_run_rule(
    self,
    *,
    rule_code: str,
    force: bool = False,
    triggered_by_id: int | None = None,
    event_id: int | None = None,
):
    result = run_rule_by_code(
        rule_code,
        created_by=_resolve_user(triggered_by_id),
        force=force,
        trigger_source="celery",
        event_id=event_id,
    )
    return {
        "created": result.created,
        "status": result.status,
        "message": result.message,
        "run_id": result.run_id,
        "task_id": result.task_id,
        "suggestion_id": result.suggestion_id,
    }
