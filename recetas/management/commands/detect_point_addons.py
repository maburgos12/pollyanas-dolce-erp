from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from recetas.utils.addon_detection_service import PointAddonDetectionService


class Command(BaseCommand):
    help = "Detecta códigos add-on/sabor/topping de Point, sincroniza su receta y deja reglas DETECTED por coocurrencia."

    def add_arguments(self, parser):
        parser.add_argument("--branch-hint", default="MATRIZ")
        parser.add_argument("--limit", type=int, default=0)
        parser.add_argument("--top-per-addon", type=int, default=3)
        parser.add_argument("--no-auto-sync", action="store_true")

    def handle(self, *args, **options):
        service = PointAddonDetectionService()
        report = service.detect_and_stage(
            branch_hint=(options.get("branch_hint") or "MATRIZ").strip(),
            limit=(options.get("limit") or 0) or None,
            auto_sync_missing=not bool(options.get("no_auto_sync")),
            top_per_addon=max(int(options.get("top_per_addon") or 3), 1),
        )
        self.stdout.write(json.dumps(report, ensure_ascii=False, indent=2))
