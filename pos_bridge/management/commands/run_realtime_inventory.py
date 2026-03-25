from __future__ import annotations

import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.realtime_inventory_service import run_realtime_inventory_sync


class Command(BaseCommand):
    help = "Ejecuta sincronización de inventario en alta frecuencia para sucursales prioritarias."

    def add_arguments(self, parser):
        parser.add_argument("--force", action="store_true", help="Ignora horario operativo.")
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar la ejecución.")

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        jobs = run_realtime_inventory_sync(force=bool(options.get("force")), triggered_by=actor)
        payload = [
            {
                "job_id": job.id,
                "status": job.status,
                "summary": job.result_summary,
                "error_message": job.error_message,
            }
            for job in jobs
        ]
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
