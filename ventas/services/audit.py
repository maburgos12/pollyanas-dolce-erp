from __future__ import annotations

from django.utils import timezone

from core.access import primary_role
from ventas.models import EventoVenta, EventoVentaAuditLog


def log_evento_change(event: EventoVenta, entity_name: str, entity_id: str, action: str, old_data=None, new_data=None, actor=None):
    EventoVentaAuditLog.objects.create(
        sales_event=event,
        entity_name=entity_name,
        entity_id=str(entity_id or ""),
        action_type=action,
        old_data_json=old_data or {},
        new_data_json=new_data or {},
        actor_user=actor,
        actor_role=primary_role(actor) if actor else "",
        created_at=timezone.now(),
    )
