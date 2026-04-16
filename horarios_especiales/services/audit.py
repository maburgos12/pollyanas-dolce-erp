from __future__ import annotations

from core.access import primary_role
from core.audit import log_event

from horarios_especiales.models import HorarioEspecialBitacora, SolicitudHorarioEspecial


def log_special_hours_event(
    *,
    request_obj: SolicitudHorarioEspecial,
    action: str,
    actor=None,
    payload: dict | None = None,
    detail=None,
) -> None:
    HorarioEspecialBitacora.objects.create(
        request=request_obj,
        detail=detail,
        action=action,
        actor_user=actor if getattr(actor, "is_authenticated", False) else None,
        actor_role=primary_role(actor) if actor else "",
        payload_json=payload or {},
    )
    log_event(
        actor,
        f"HORARIOS_ESPECIALES_{action}",
        "horarios_especiales.SolicitudHorarioEspecial",
        request_obj.request_code,
        payload=payload or {},
    )

