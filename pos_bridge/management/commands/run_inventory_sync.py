from __future__ import annotations

import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.tasks.run_inventory_sync import run_inventory_sync


class Command(BaseCommand):
    help = "Ejecuta sincronización manual de inventario Point vía browser automation."

    def add_arguments(self, parser):
        parser.add_argument("--branch", default="", help="Filtra por sucursal (external_id o label parcial).")
        parser.add_argument("--limit-branches", type=int, default=None, help="Límite de sucursales a recorrer.")
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar la ejecución.")

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        sync_job = run_inventory_sync(
            triggered_by=actor,
            branch_filter=(options.get("branch") or "").strip() or None,
            limit_branches=options.get("limit_branches"),
        )
        payload = {
            "job_id": sync_job.id,
            "status": sync_job.status,
            "summary": sync_job.result_summary,
            "error_message": sync_job.error_message,
            "artifacts": sync_job.artifacts,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
