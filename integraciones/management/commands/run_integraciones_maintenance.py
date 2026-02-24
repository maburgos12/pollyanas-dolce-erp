from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from django.contrib.auth import get_user_model
from django.db import OperationalError, ProgrammingError
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from core.audit import log_event
from integraciones.models import PublicApiAccessLog, PublicApiClient
from integraciones.views import _deactivate_idle_api_clients, _purge_api_logs


def _bound_int(raw_value: Any, *, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = default
    return max(min_value, min(value, max_value))


def _preview_deactivate_idle_clients(*, idle_days: int, limit: int) -> dict[str, Any]:
    cutoff = timezone.now() - timedelta(days=idle_days)
    recent_client_ids = set(
        PublicApiAccessLog.objects.filter(created_at__gte=cutoff)
        .values_list("client_id", flat=True)
        .distinct()
    )
    candidates = list(
        PublicApiClient.objects.filter(activo=True)
        .exclude(id__in=recent_client_ids)
        .order_by("id")
        .values("id", "nombre")[:limit]
    )
    return {
        "idle_days": idle_days,
        "limit": limit,
        "candidates": len(candidates),
        "deactivated": 0,
        "cutoff": cutoff.isoformat(),
        "candidate_clients": candidates,
        "dry_run": True,
    }


def _preview_purge_api_logs(*, retain_days: int, max_delete: int) -> dict[str, Any]:
    cutoff = timezone.now() - timedelta(days=retain_days)
    total_candidates = PublicApiAccessLog.objects.filter(created_at__lt=cutoff).count()
    return {
        "retain_days": retain_days,
        "max_delete": max_delete,
        "cutoff": cutoff.isoformat(),
        "candidates": int(total_candidates),
        "deleted": 0,
        "remaining_candidates": int(total_candidates),
        "would_delete": min(int(total_candidates), max_delete),
        "dry_run": True,
    }


class Command(BaseCommand):
    help = "Ejecuta mantenimiento operativo de integraciones Point (CLI), con soporte dry-run."

    def add_arguments(self, parser):
        parser.add_argument("--idle-days", type=int, default=30, help="Días de inactividad para desactivar clientes API.")
        parser.add_argument("--idle-limit", type=int, default=100, help="Máximo de clientes API a desactivar por corrida.")
        parser.add_argument("--retain-days", type=int, default=90, help="Retención de logs API (días).")
        parser.add_argument("--max-delete", type=int, default=5000, help="Máximo de logs API a purgar por corrida.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="No aplica cambios; solo previsualiza y registra bitácora PREVIEW.",
        )
        parser.add_argument(
            "--confirm-live",
            default="",
            help='Confirmación para live. Debe ser exactamente "YES" cuando no es dry-run.',
        )
        parser.add_argument(
            "--actor-username",
            default="",
            help="Usuario a registrar en bitácora (opcional). Si se omite, queda sin usuario.",
        )

    def _resolve_actor(self, actor_username: str):
        username = (actor_username or "").strip()
        if not username:
            return None
        User = get_user_model()
        actor = User.objects.filter(username=username).first()
        if not actor:
            raise CommandError(f"No existe actor_username '{username}'.")
        return actor

    def handle(self, *args, **options):
        idle_days = _bound_int(options.get("idle_days"), default=30, min_value=1, max_value=365)
        idle_limit = _bound_int(options.get("idle_limit"), default=100, min_value=1, max_value=500)
        retain_days = _bound_int(options.get("retain_days"), default=90, min_value=1, max_value=3650)
        max_delete = _bound_int(options.get("max_delete"), default=5000, min_value=1, max_value=50000)
        dry_run = bool(options.get("dry_run"))
        confirm_live = str(options.get("confirm_live") or "").strip().upper()
        actor = self._resolve_actor(str(options.get("actor_username") or ""))

        if not dry_run and confirm_live != "YES":
            raise CommandError('Para ejecución live debes confirmar con --confirm-live YES')

        try:
            deactivate_summary = (
                _preview_deactivate_idle_clients(idle_days=idle_days, limit=idle_limit)
                if dry_run
                else _deactivate_idle_api_clients(idle_days=idle_days, limit=idle_limit)
            )
            purge_summary = (
                _preview_purge_api_logs(retain_days=retain_days, max_delete=max_delete)
                if dry_run
                else _purge_api_logs(retain_days=retain_days, max_delete=max_delete)
            )
        except (OperationalError, ProgrammingError) as exc:
            raise CommandError(
                "No se pudo ejecutar mantenimiento: esquema de base de datos incompleto o no migrado. "
                "Ejecuta `python manage.py migrate` en el entorno objetivo."
            ) from exc

        payload = {
            "source": "CLI",
            "dry_run": dry_run,
            "deactivate_idle_clients": deactivate_summary,
            "purge_api_logs": purge_summary,
        }
        action = "PREVIEW_RUN_API_MAINTENANCE" if dry_run else "RUN_API_MAINTENANCE"
        log_event(actor, action, "integraciones.Operaciones", "", payload=payload)

        self.stdout.write(self.style.SUCCESS("Mantenimiento de integraciones ejecutado."))
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
