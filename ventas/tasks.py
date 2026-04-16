from __future__ import annotations

from celery import shared_task

from ventas.models import EventoVenta
from ventas.services.financials import build_financials
from ventas.services.forecasting import generate_event_forecast
from ventas.services.monitoring import monitor_active_events
from ventas.services.notifications import create_unique_notification
from ventas.services.postmortem import build_postmortem
from ventas.services.production import generate_production_plan
from ventas.services.requirements import build_input_requirements, build_purchase_requirements
from ventas.services.substitution_learning import rebuild_substitution_weights


def _load_event(event_id: int) -> EventoVenta:
    return EventoVenta.objects.get(pk=event_id)


@shared_task(name="ventas.generate_event_forecast", bind=True, acks_late=True)
def generate_event_forecast_task(self, event_id: int) -> dict:
    return generate_event_forecast(_load_event(event_id))


@shared_task(name="ventas.run_event_projection_pipeline", bind=True, acks_late=True)
def run_event_projection_pipeline_task(self, event_id: int) -> dict:
    event = _load_event(event_id)
    result = generate_event_forecast(event)
    artifact_count = 0
    if result.get("created"):
        from ventas.views import _persist_projection_artifacts, _refresh_event_support_outputs

        event.refresh_from_db()
        artifacts = _persist_projection_artifacts(event, event.created_by, force=True)
        artifact_count = len(artifacts)
        _refresh_event_support_outputs(event)
    create_unique_notification(
        event,
        (
            f"Recalculo del evento finalizado. Forecast {result.get('created', 0)} filas, "
            f"archivos {artifact_count}."
        ),
    )
    return {
        "created": result.get("created", 0),
        "warnings": result.get("warnings", []),
        "artifacts": artifact_count,
    }


@shared_task(name="ventas.generate_production_plan", bind=True, acks_late=True)
def generate_production_plan_task(self, event_id: int) -> dict:
    return generate_production_plan(_load_event(event_id))


@shared_task(name="ventas.generate_input_requirements", bind=True, acks_late=True)
def generate_input_requirements_task(self, event_id: int) -> dict:
    return build_input_requirements(_load_event(event_id))


@shared_task(name="ventas.generate_purchase_requirements", bind=True, acks_late=True)
def generate_purchase_requirements_task(self, event_id: int) -> dict:
    event = _load_event(event_id)
    build_input_requirements(event)
    return build_purchase_requirements(event)


@shared_task(name="ventas.build_financials", bind=True, acks_late=True)
def build_financials_task(self, event_id: int) -> dict:
    return build_financials(_load_event(event_id))


@shared_task(name="ventas.build_postmortem", bind=True, acks_late=True)
def build_postmortem_task(self, event_id: int) -> dict:
    return build_postmortem(_load_event(event_id))


@shared_task(name="ventas.monitor_active_events", bind=True, acks_late=True)
def monitor_active_events_task(self) -> dict:
    return monitor_active_events()


@shared_task(name="ventas.rebuild_substitution_weights", bind=True, acks_late=True)
def rebuild_substitution_weights_task(
    self,
    *,
    lookback_days: int = 180,
    window_days: int = 7,
    branch_ids: list[int] | None = None,
    family: str | None = None,
    category: str | None = None,
    version: str = "v7.2-learned",
) -> dict:
    return rebuild_substitution_weights(
        lookback_days=lookback_days,
        window_days=window_days,
        branch_ids=branch_ids,
        family=family,
        category=category,
        version=version,
    )
