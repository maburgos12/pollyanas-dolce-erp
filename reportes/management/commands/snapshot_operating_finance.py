from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError

from reportes.services_operating_finance import OperatingFinanceSnapshotService


class Command(BaseCommand):
    help = "Genera el snapshot mensual de costo operativo, contribución por sucursal y pricing."

    def add_arguments(self, parser):
        parser.add_argument("--period", required=True, help="Periodo YYYY-MM.")
        parser.add_argument("--gross-margin-target", default="0.65", help="Objetivo de margen bruto contra venta.")
        parser.add_argument("--contribution-margin-target", default="0.18", help="Objetivo de margen de contribución.")

    def handle(self, *args, **options):
        raw_period = str(options["period"]).strip()
        try:
            period_start = date.fromisoformat(f"{raw_period}-01")
        except ValueError as exc:
            raise CommandError("period debe venir en formato YYYY-MM.") from exc

        summary = OperatingFinanceSnapshotService().build_snapshot(
            period_start=period_start,
            gross_margin_target=Decimal(str(options["gross_margin_target"])),
            contribution_margin_target=Decimal(str(options["contribution_margin_target"])),
        )
        self.stdout.write(
            json.dumps(
                {
                    "period_start": summary.period_start.isoformat(),
                    "product_cost_rows": summary.product_cost_rows,
                    "branch_contribution_rows": summary.branch_contribution_rows,
                    "pricing_rows": summary.pricing_rows,
                    "company_result_created": summary.company_result_created,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
