from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from reportes.services_budget_detail_import import BudgetGeneralAuditService


class Command(BaseCommand):
    help = "Audita si las hojas GENERAL cuadran contra el detalle visible de ventas y nómina."

    def add_arguments(self, parser):
        parser.add_argument("--dir", required=True, help="Ruta de la carpeta con archivos XLSX.")

    def handle(self, *args, **options):
        folder_path = str(options["dir"]).strip()
        try:
            payload = BudgetGeneralAuditService().audit_folder(folder_path)
        except FileNotFoundError as exc:
            raise CommandError(f"Carpeta o archivo no encontrado: {folder_path}") from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
