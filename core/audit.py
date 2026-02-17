from typing import Any

from django.contrib.auth.models import AbstractBaseUser

from core.models import AuditLog


def log_event(user: AbstractBaseUser | None, action: str, model: str, object_id: str, payload: dict[str, Any] | None = None) -> None:
    AuditLog.objects.create(
        user=user if getattr(user, "is_authenticated", False) else None,
        action=action,
        model=model,
        object_id=str(object_id),
        payload=payload or {},
    )
