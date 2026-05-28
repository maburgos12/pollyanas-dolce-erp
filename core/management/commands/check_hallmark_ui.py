from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from core.hallmark_ui_audit import new_issues_against_baseline, scan_hallmark_ui, write_baseline


class Command(BaseCommand):
    help = "Audita guardrails visuales Hallmark y falla si hay regresiones nuevas."
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument(
            "--update-baseline",
            action="store_true",
            help="Reescribe docs/hallmark_ui_audit_baseline.json con el estado actual.",
        )
        parser.add_argument(
            "--list-all",
            action="store_true",
            help="Muestra todos los issues detectados, no solo los nuevos.",
        )

    def handle(self, *args, **options):
        base_dir = Path(settings.BASE_DIR)
        if options["update_baseline"]:
            issues = scan_hallmark_ui(base_dir)
            write_baseline(base_dir, issues)
            self.stdout.write(self.style.WARNING(f"Baseline Hallmark actualizado con {len(issues)} issues aceptados."))
            return

        if options["list_all"]:
            issues = scan_hallmark_ui(base_dir)
        else:
            issues = new_issues_against_baseline(base_dir)

        if issues:
            for issue in issues[:50]:
                self.stdout.write(
                    f"{issue.rule}: {issue.path} :: {issue.snippet}\n"
                    f"  {issue.message}"
                )
            if len(issues) > 50:
                self.stdout.write(f"... {len(issues) - 50} issues adicionales.")
            raise CommandError(f"Hallmark UI detecto {len(issues)} regresion(es) visual(es) nuevas.")

        self.stdout.write(self.style.SUCCESS("Hallmark UI sin regresiones nuevas."))
