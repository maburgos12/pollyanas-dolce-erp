from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import OperationalError, ProgrammingError
from django.utils.dateparse import parse_date

from orquestacion.services.quality_guard_runner import run_quality_guards, sync_quality_guards


class Command(BaseCommand):
    help = "Ejecuta guards arquitectonicos y sincroniza hallazgos persistentes."

    def add_arguments(self, parser):
        parser.add_argument(
            "--base-dir",
            default=str(settings.BASE_DIR),
            help="Ruta raíz del repo que se va a revisar.",
        )
        parser.add_argument(
            "--no-persist",
            action="store_true",
            help="Solo ejecuta el guard sin registrar hallazgos en BD.",
        )
        parser.add_argument(
            "--reference-date",
            help="Fecha de referencia YYYY-MM-DD para revisar gaps de publicación visibles.",
        )

    def handle(self, *args, **options):
        if not getattr(settings, "ORQUESTACION_POINTDAILYSALE_GUARD_ENABLED", True):
            self.stdout.write(self.style.WARNING("PointDailySale guard desactivado por settings."))
            return

        base_dir = Path(options["base_dir"]).resolve()
        reference_date_raw = options.get("reference_date") or None
        reference_date = None
        if reference_date_raw:
            reference_date = parse_date(reference_date_raw)
            if reference_date is None:
                raise CommandError("`--reference-date` debe usar formato YYYY-MM-DD.")
        run_result = run_quality_guards(base_dir=base_dir, reference_date=reference_date)

        if options["no_persist"]:
            self.stdout.write(
                "Quality guards auditados: "
                f"PointDailySale={len(run_result.point_scan.violations)} violaciones, "
                f"ProtectedReaders={len(run_result.protected_scan.violations)} violaciones, "
                f"PublicationGap={'deferred' if run_result.publication_gap_scan.deferred_by_active_sync else ('sí' if run_result.publication_gap_scan.has_gap else 'no')}."
            )
        else:
            try:
                summary = sync_quality_guards(run_result)
            except (ProgrammingError, OperationalError) as exc:
                raise CommandError(
                    "Las tablas de QualityFinding/RemediationProposal todavía no están disponibles. "
                    "Corre `./.venv/bin/python manage.py migrate --settings=config.settings_test` "
                    "o usa `--no-persist` para ejecutar solo el detector."
                ) from exc
            self.stdout.write(
                "Quality guards sincronizados: "
                f"PointDailySale={summary['pointdailysale']['violations']} violaciones, "
                f"ProtectedReaders={summary['protected_sales_reader']['violations']} violaciones, "
                f"PublicationGap={'deferred' if run_result.publication_gap_scan.deferred_by_active_sync else summary['sales_publication_gap']['violations']}."
            )

        if run_result.point_scan.has_violations:
            for violation in run_result.point_scan.violations:
                self.stdout.write(
                    self.style.ERROR(
                        f"- {violation.relative_path}:{violation.line_number} | "
                        f"{violation.reason} | {violation.suggestion}"
                    )
                )
        if run_result.protected_scan.has_violations:
            for violation in run_result.protected_scan.violations:
                self.stdout.write(
                    self.style.ERROR(
                        f"- {violation.relative_path}:{violation.line_number} [{violation.symbol}] | "
                        f"{violation.reason} | {violation.suggestion}"
                    )
                )
        if run_result.publication_gap_scan.deferred_by_active_sync:
            self.stdout.write(
                self.style.WARNING(
                    "Sales publication gap deferred: "
                    f"sync_status={run_result.publication_gap_scan.sync_job_status or 'unknown'} | "
                    f"{run_result.publication_gap_scan.suggestion}"
                )
            )
        elif run_result.publication_gap_scan.has_gap:
            self.stdout.write(
                self.style.WARNING(
                    "Sales publication gap: "
                    f"target={run_result.publication_gap_scan.target_date} | "
                    f"fact_lag={run_result.publication_gap_scan.fact_lag_days} | "
                    f"visible_lag={run_result.publication_gap_scan.visible_lag_days} | "
                    f"{run_result.publication_gap_scan.suggestion}"
                )
            )

        if run_result.has_blocking_violations:
            raise CommandError("Quality guards encontraron violaciones arquitectónicas bloqueantes.")

        self.stdout.write(self.style.SUCCESS("Quality guards OK."))
