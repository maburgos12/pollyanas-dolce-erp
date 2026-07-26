from typing import Any

from django.contrib.auth.models import AbstractBaseUser

from core.models import AuditLog


def log_event(user: AbstractBaseUser | None, action: str, model: str, object_id: str, payload: dict[str, Any] | None = None) -> None:
    # object_id es varchar(64); un identificador largo (p.ej. una ruta de
    # archivo en imports) no debe tumbar el flujo de negocio con DataError.
    max_length = AuditLog._meta.get_field("object_id").max_length
    AuditLog.objects.create(
        user=user if getattr(user, "is_authenticated", False) else None,
        action=action,
        model=model,
        object_id=str(object_id)[:max_length],
        payload=payload or {},
    )
