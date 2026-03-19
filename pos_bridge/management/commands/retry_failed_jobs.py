from __future__ import annotations

import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.tasks.retry_failed_jobs import retry_failed_jobs


class Command(BaseCommand):
    help = "Reintenta jobs fallidos de pos_bridge dentro del umbral configurado."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=5, help="Máximo de jobs fallidos a reintentar.")
        parser.add_argument("--max-attempts", type=int, default=None, help="Tope de intentos por job.")
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar el retry.")

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        jobs = retry_failed_jobs(
            triggered_by=actor,
            limit=int(options.get("limit") or 5),
            max_attempts=options.get("max_attempts"),
        )
        self.stdout.write(
            json.dumps(
                [{"job_id": job.id, "status": job.status, "summary": job.result_summary} for job in jobs],
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
