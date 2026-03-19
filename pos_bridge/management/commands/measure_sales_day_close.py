from __future__ import annotations

import json
import time
from datetime import date, datetime

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from pos_bridge.services.sales_cutoff_service import PointSalesCutoffService
from pos_bridge.tasks.run_sales_history_sync import run_sales_history_sync


def _parse_date(value: str, *, label: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise CommandError(f"{label} debe tener formato YYYY-MM-DD.") from exc


class Command(BaseCommand):
    help = "Mide la estabilización nocturna de ventas Point para fijar una hora de corte confiable."

    def add_arguments(self, parser):
        parser.add_argument(
            "--sale-date",
            default="",
            help="Fecha de venta a medir YYYY-MM-DD. Por default usa hoy local.",
        )
        parser.add_argument("--branch", default="", help="Filtra por sucursal Point (id o nombre parcial).")
        parser.add_argument("--probes", type=int, default=1, help="Número de mediciones consecutivas.")
        parser.add_argument(
            "--interval-minutes",
            type=int,
            default=60,
            help="Minutos entre mediciones cuando probes > 1.",
        )
        parser.add_argument(
            "--stable-after",
            type=int,
            default=2,
            help="Cuántas mediciones consecutivas sin cambios se consideran estabilidad.",
        )
        parser.add_argument(
            "--skip-sync",
            action="store_true",
            help="No consulta Point; solo resume lo ya cargado en PointDailySale.",
        )

    def handle(self, *args, **options):
        sale_date_raw = (options.get("sale_date") or "").strip()
        sale_date = _parse_date(sale_date_raw, label="sale-date") if sale_date_raw else timezone.localdate()
        branch_filter = (options.get("branch") or "").strip()
        probes = max(int(options.get("probes") or 1), 1)
        interval_minutes = max(int(options.get("interval_minutes") or 0), 0)
        stable_after = max(int(options.get("stable_after") or 1), 1)
        if probes > 1 and interval_minutes <= 0:
            raise CommandError("interval-minutes debe ser mayor a 0 cuando probes > 1.")

        cutoff_service = PointSalesCutoffService()
        report_path = cutoff_service.build_report_path(sale_date=sale_date, branch_filter=branch_filter)
        report = cutoff_service.load_report(report_path)

        for probe_index in range(1, probes + 1):
            sync_job = None
            if not options.get("skip_sync"):
                sync_job = run_sales_history_sync(
                    start_date=sale_date,
                    end_date=sale_date,
                    excluded_ranges=[],
                    branch_filter=branch_filter or None,
                )

            snapshot = cutoff_service.summarize_sales(sale_date=sale_date, branch_filter=branch_filter)
            report = cutoff_service.append_probe(
                report=report,
                sale_date=sale_date,
                branch_filter=branch_filter,
                snapshot=snapshot,
                sync_job=sync_job,
                stable_after=stable_after,
            )
            cutoff_service.save_report(report_path, report)

            self.stdout.write(
                json.dumps(
                    {
                        "probe_index": probe_index,
                        "sale_date": sale_date.isoformat(),
                        "branch_filter": branch_filter,
                        "report_path": str(report_path),
                        "analysis": report["analysis"],
                        "latest_probe": report["probes"][-1],
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            )

            if probe_index >= probes:
                continue
            time.sleep(interval_minutes * 60)
