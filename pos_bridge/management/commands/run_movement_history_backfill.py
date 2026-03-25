from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.utils.dates import iter_business_dates, timestamp_token


@dataclass
class BackfillTotals:
    chunks_processed: int = 0
    chunks_failed: int = 0
    waste_jobs: int = 0
    production_jobs: int = 0
    transfer_jobs: int = 0
    waste_lines_seen: int = 0
    production_lines_seen: int = 0
    transfer_lines_seen: int = 0
    inventory_entries_created: int = 0
    cedis_entries_created: int = 0
    mermas_created: int = 0
    unmatched_items: int = 0


@dataclass(frozen=True)
class DateChunk:
    start_date: date
    end_date: date


def _last_day_of_month(value: date) -> date:
    if value.month == 12:
        return date(value.year, 12, 31)
    return date(value.year, value.month + 1, 1) - timedelta(days=1)


def _first_day_of_quarter(value: date) -> date:
    first_month = ((value.month - 1) // 3) * 3 + 1
    return date(value.year, first_month, 1)


def iter_date_chunks(start_date: date, end_date: date, *, chunk_mode: str = "day", chunk_size: int = 1) -> Iterable[DateChunk]:
    effective_chunk_size = max(int(chunk_size or 1), 1)
    if chunk_mode == "day":
        for work_date in iter_business_dates(start_date, end_date):
            yield DateChunk(start_date=work_date, end_date=work_date)
        return

    cursor = start_date
    while cursor <= end_date:
        if chunk_mode == "month":
            month_cursor = cursor
            chunk_end = cursor
            for _ in range(effective_chunk_size):
                chunk_end = _last_day_of_month(month_cursor)
                next_day = chunk_end + timedelta(days=1)
                if next_day > end_date:
                    break
                month_cursor = next_day
            yield DateChunk(start_date=cursor, end_date=min(chunk_end, end_date))
        elif chunk_mode == "quarter":
            quarter_start = _first_day_of_quarter(cursor)
            quarter_cursor = quarter_start
            chunk_end = cursor
            for _ in range(effective_chunk_size):
                quarter_end_month = quarter_cursor.month + 2
                quarter_end = _last_day_of_month(date(quarter_cursor.year, quarter_end_month, 1))
                chunk_end = quarter_end
                next_day = quarter_end + timedelta(days=1)
                if next_day > end_date:
                    break
                quarter_cursor = next_day
            yield DateChunk(start_date=cursor, end_date=min(chunk_end, end_date))
        else:
            raise CommandError(f"chunk-mode no soportado: {chunk_mode}")
        cursor = min(chunk_end, end_date) + timedelta(days=1)


class Command(BaseCommand):
    help = "Backfill histórico por rango para mermas, producción y transferencias Point."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", required=True, help="Fecha inicio YYYY-MM-DD")
        parser.add_argument("--end-date", required=True, help="Fecha fin YYYY-MM-DD")
        parser.add_argument("--branch", dest="branch_filter", default="", help="Filtro opcional de sucursal")
        parser.add_argument("--waste", action="store_true", help="Incluye mermas")
        parser.add_argument("--production", action="store_true", help="Incluye producción")
        parser.add_argument("--transfers", action="store_true", help="Incluye transferencias")
        parser.add_argument(
            "--chunk-mode",
            choices=["day", "month", "quarter"],
            default="day",
            help="Agrupa el backfill por día, mes o trimestre.",
        )
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=1,
            help="Cantidad de meses o trimestres por chunk. Ignorado en modo day.",
        )
        parser.add_argument(
            "--stop-on-error",
            action="store_true",
            help="Detiene el proceso en el primer chunk fallido.",
        )

    def handle(self, *args, **options):
        try:
            start_date = date.fromisoformat(options["start_date"])
            end_date = date.fromisoformat(options["end_date"])
        except ValueError as exc:
            raise CommandError("Las fechas deben usar formato YYYY-MM-DD.") from exc
        if end_date < start_date:
            raise CommandError("end-date no puede ser menor que start-date.")

        include_waste = bool(options.get("waste"))
        include_production = bool(options.get("production"))
        include_transfers = bool(options.get("transfers"))
        if not any([include_waste, include_production, include_transfers]):
            include_waste = include_production = include_transfers = True

        branch_filter = (options.get("branch_filter") or "").strip() or None
        chunk_mode = options["chunk_mode"]
        chunk_size = max(int(options.get("chunk_size") or 1), 1)
        stop_on_error = bool(options.get("stop_on_error"))
        service = PointMovementSyncService()
        totals = BackfillTotals()
        chunk_results: list[dict] = []

        for chunk in iter_date_chunks(start_date, end_date, chunk_mode=chunk_mode, chunk_size=chunk_size):
            chunk_result = {
                "start_date": chunk.start_date.isoformat(),
                "end_date": chunk.end_date.isoformat(),
                "jobs": [],
                "has_failure": False,
            }
            if include_waste:
                job = service.run_waste_sync(start_date=chunk.start_date, end_date=chunk.end_date, branch_filter=branch_filter)
                totals.waste_jobs += 1
                summary = job.result_summary or {}
                totals.waste_lines_seen += int(summary.get("waste_lines_seen") or 0)
                totals.mermas_created += int(summary.get("mermas_created") or 0)
                totals.unmatched_items += int(summary.get("unmatched_items") or 0)
                chunk_result["jobs"].append(
                    {
                        "type": "waste",
                        "job_id": job.id,
                        "status": job.status,
                        "summary": summary,
                        "error_message": job.error_message,
                    }
                )
                chunk_result["has_failure"] = chunk_result["has_failure"] or job.status != job.STATUS_SUCCESS
            if include_production:
                job = service.run_production_sync(start_date=chunk.start_date, end_date=chunk.end_date, branch_filter=branch_filter)
                totals.production_jobs += 1
                summary = job.result_summary or {}
                totals.production_lines_seen += int(summary.get("production_lines_seen") or 0)
                totals.inventory_entries_created += int(summary.get("inventory_entries_created") or 0)
                totals.cedis_entries_created += int(summary.get("cedis_entries_created") or 0)
                totals.unmatched_items += int(summary.get("unmatched_items") or 0)
                chunk_result["jobs"].append(
                    {
                        "type": "production",
                        "job_id": job.id,
                        "status": job.status,
                        "summary": summary,
                        "error_message": job.error_message,
                    }
                )
                chunk_result["has_failure"] = chunk_result["has_failure"] or job.status != job.STATUS_SUCCESS
            if include_transfers:
                job = service.run_transfer_sync(start_date=chunk.start_date, end_date=chunk.end_date, branch_filter=branch_filter)
                totals.transfer_jobs += 1
                summary = job.result_summary or {}
                totals.transfer_lines_seen += int(summary.get("transfer_lines_seen") or 0)
                totals.inventory_entries_created += int(summary.get("inventory_entries_created") or 0)
                totals.cedis_entries_created += int(summary.get("cedis_entries_created") or 0)
                totals.unmatched_items += int(summary.get("unmatched_items") or 0)
                chunk_result["jobs"].append(
                    {
                        "type": "transfers",
                        "job_id": job.id,
                        "status": job.status,
                        "summary": summary,
                        "error_message": job.error_message,
                    }
                )
                chunk_result["has_failure"] = chunk_result["has_failure"] or job.status != job.STATUS_SUCCESS
            totals.chunks_processed += 1
            if chunk_result["has_failure"]:
                totals.chunks_failed += 1
            chunk_results.append(chunk_result)
            if chunk_result["has_failure"] and stop_on_error:
                break

        settings = load_point_bridge_settings()
        reports_dir = Path(settings.storage_root) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = reports_dir / f"{timestamp_token()}_movement_backfill_{start_date.isoformat()}_{end_date.isoformat()}.json"
        report_payload = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch_filter": branch_filter or "",
            "chunk_mode": chunk_mode,
            "chunk_size": chunk_size,
            "stop_on_error": stop_on_error,
            "includes": {
                "waste": include_waste,
                "production": include_production,
                "transfers": include_transfers,
            },
            "totals": asdict(totals),
            "chunks": chunk_results,
        }
        report_path.write_text(json.dumps(report_payload, indent=2, ensure_ascii=True), encoding="utf-8")

        self.stdout.write(self.style.SUCCESS("Backfill de movimientos Point finalizado"))
        self.stdout.write(f"  - chunks procesados: {totals.chunks_processed}")
        self.stdout.write(f"  - chunks fallidos: {totals.chunks_failed}")
        self.stdout.write(f"  - waste lines: {totals.waste_lines_seen}")
        self.stdout.write(f"  - production lines: {totals.production_lines_seen}")
        self.stdout.write(f"  - transfer lines: {totals.transfer_lines_seen}")
        self.stdout.write(f"  - inventory entries created: {totals.inventory_entries_created}")
        self.stdout.write(f"  - cedis entries created: {totals.cedis_entries_created}")
        self.stdout.write(f"  - mermas created: {totals.mermas_created}")
        self.stdout.write(f"  - unmatched items: {totals.unmatched_items}")
        self.stdout.write(f"  - report: {report_path}")
