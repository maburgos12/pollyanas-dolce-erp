from __future__ import annotations

import json
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from pos_bridge.tasks.run_product_recipe_sync import run_product_recipe_sync


class DryRunRollback(Exception):
    pass


class Command(BaseCommand):
    help = "Sincroniza recetas/BOM de productos Point hacia Receta/LineaReceta del ERP."

    def add_arguments(self, parser):
        parser.add_argument("--branch-hint", default="", help="Sucursal/workspace Point a usar para abrir sesión.")
        parser.add_argument(
            "--product-code",
            action="append",
            default=[],
            help="Código Point del producto a sincronizar. Se puede repetir.",
        )
        parser.add_argument("--limit", type=int, default=None, help="Límite de productos a procesar.")
        parser.add_argument(
            "--include-without-recipe",
            action="store_true",
            help="Incluye productos sin receta en Point para auditarlos en el raw export.",
        )
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar la ejecución.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Ejecuta el sync dentro de una transacción y revierte todos los cambios al terminar.",
        )

    def handle(self, *args, **options):
        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        sync_kwargs = {
            "triggered_by": actor,
            "branch_hint": (options.get("branch_hint") or "").strip() or None,
            "product_codes": [code.strip() for code in (options.get("product_code") or []) if (code or "").strip()],
            "limit": options.get("limit"),
            "include_without_recipe": bool(options.get("include_without_recipe")),
        }
        if options.get("dry_run"):
            payload = None
            artifact_paths = []
            try:
                with transaction.atomic():
                    sync_job = run_product_recipe_sync(**sync_kwargs)
                    payload = self._build_payload(sync_job, dry_run=True)
                    artifact_paths = self._artifact_paths(payload)
                    raise DryRunRollback()
            except DryRunRollback:
                if payload is not None:
                    self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
                removed_artifacts = self._remove_artifacts(artifact_paths)
                self.stdout.write(f"[DRY-RUN] No se persistió nada. Artifacts removidos: {removed_artifacts}")
                return

        sync_job = run_product_recipe_sync(**sync_kwargs)
        payload = self._build_payload(sync_job, dry_run=False)
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))

    def _build_payload(self, sync_job, *, dry_run: bool) -> dict:
        payload = {
            "job_id": sync_job.id,
            "status": sync_job.status,
            "dry_run": dry_run,
            "summary": sync_job.result_summary,
            "error_message": sync_job.error_message,
            "artifacts": sync_job.artifacts,
            "parameters": sync_job.parameters,
            "preview": self._build_preview(sync_job),
        }
        return payload

    def _build_preview(self, sync_job) -> list[dict]:
        preview = []
        runs = sync_job.recipe_runs.prefetch_related("nodes__lines").order_by("-id")
        for run in runs:
            for node in run.nodes.filter(depth=0).prefetch_related("lines").order_by("point_name", "id"):
                preview.append(
                    {
                        "codigo_point": node.point_code,
                        "nombre": node.point_name,
                        "ingredientes": [
                            {
                                "codigo_point": line.point_code,
                                "nombre": line.point_name,
                                "cantidad": line.quantity,
                                "unidad": line.unit.codigo if line.unit_id else line.unit_text,
                                "clasificacion": line.classification,
                                "match_method": line.match_method,
                                "match_score": line.match_score,
                            }
                            for line in node.lines.all().order_by("position", "id")
                        ],
                    }
                )
        return preview

    def _artifact_paths(self, payload: dict) -> list[Path]:
        paths = []
        for raw_path in (payload.get("summary") or {}).get("raw_exports") or []:
            if raw_path:
                paths.append(Path(raw_path))
        return paths

    def _remove_artifacts(self, paths: list[Path]) -> int:
        removed = 0
        for path in paths:
            try:
                path.unlink(missing_ok=False)
            except FileNotFoundError:
                continue
            removed += 1
        return removed
