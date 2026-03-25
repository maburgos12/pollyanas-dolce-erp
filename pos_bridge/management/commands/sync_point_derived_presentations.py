from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.derived_presentation_sync_service import PointDerivedPresentationSyncService


class Command(BaseCommand):
    help = "Sincroniza relaciones padre->derivado para rebanadas desde el reporte de auditoría Point."

    def add_arguments(self, parser):
        parser.add_argument("--report-path", default="", help="Ruta al JSON de auditoría de recetas faltantes.")
        parser.add_argument(
            "--no-create-missing-recipes",
            action="store_true",
            help="No crear recetas derivadas placeholder cuando no existan en ERP.",
        )

    def handle(self, *args, **options):
        settings = load_point_bridge_settings()
        service = PointDerivedPresentationSyncService(storage_root=settings.storage_root)
        report_path = (options.get("report_path") or "").strip() or None
        try:
            result = service.sync(
                report_path=report_path,
                create_missing_recipes=not bool(options.get("no_create_missing_recipes")),
            )
        except FileNotFoundError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            json.dumps(
                {
                    "summary": result.summary,
                    "report_path": result.report_path,
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
