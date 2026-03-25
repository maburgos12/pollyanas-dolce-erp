from __future__ import annotations

import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.tasks.run_recipe_gap_audit import run_recipe_gap_audit


class Command(BaseCommand):
    help = "Audita productos Point sin receta y busca corroboración en catálogos de insumos."

    def add_arguments(self, parser):
        parser.add_argument("--branch-hint", default="", help="Sucursal/workspace Point a usar para abrir sesión.")
        parser.add_argument(
            "--product-code",
            action="append",
            default=[],
            help="Código Point del producto a auditar. Se puede repetir.",
        )
        parser.add_argument("--limit", type=int, default=None, help="Límite de productos faltantes a auditar.")
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar la ejecución.")

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        sync_job = run_recipe_gap_audit(
            triggered_by=actor,
            branch_hint=(options.get("branch_hint") or "").strip() or None,
            product_codes=[code.strip() for code in (options.get("product_code") or []) if (code or "").strip()],
            limit=options.get("limit"),
        )
        payload = {
            "job_id": sync_job.id,
            "status": sync_job.status,
            "summary": sync_job.result_summary,
            "error_message": sync_job.error_message,
            "artifacts": sync_job.artifacts,
            "parameters": sync_job.parameters,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
