from __future__ import annotations

import json
from datetime import date

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from pos_bridge.services.product_month_closure_service import ProductMonthClosureError, ProductMonthClosureService


def _month_cursor(value: str) -> date:
    try:
        year_text, month_text = value.strip().split("-", 1)
        return date(int(year_text), int(month_text), 1)
    except Exception as exc:  # noqa: BLE001
        raise CommandError(f"Mes invalido '{value}'. Usa formato YYYY-MM.") from exc


def _iter_months(from_month: date, to_month: date):
    cursor = date(from_month.year, from_month.month, 1)
    target = date(to_month.year, to_month.month, 1)
    while cursor <= target:
        yield cursor
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)


class Command(BaseCommand):
    help = "Construye o simula el backfill de cierres mensuales de producto Point en un rango YYYY-MM."

    def add_arguments(self, parser):
        parser.add_argument("--from-month", required=True, help="Mes inicial en formato YYYY-MM.")
        parser.add_argument("--to-month", required=True, help="Mes final en formato YYYY-MM.")
        parser.add_argument("--dry-run", action="store_true", help="Simula el backfill sin persistir cierres.")
        parser.add_argument("--rebuild", action="store_true", help="Permite rebuild si el mes ya existe y no esta bloqueado.")
        parser.add_argument("--lock-after-build", action="store_true", help="Bloquea cada mes construido si pasa guardas.")
        parser.add_argument("--actor-username", default="", help="Usuario ERP para registrar la ejecucion.")
        parser.add_argument("--approval-note", default="", help="Nota de aprobacion o contexto de corrida.")
        parser.add_argument("--approval-reason", default="", help="Motivo corto para backfill/build.")

    def handle(self, *args, **options):
        from_month = _month_cursor(options["from_month"])
        to_month = _month_cursor(options["to_month"])
        if from_month > to_month:
            raise CommandError("--from-month no puede ser mayor que --to-month.")

        actor = None
        actor_username = (options.get("actor_username") or "").strip()
        if actor_username:
            actor = get_user_model().objects.filter(username=actor_username).first()
            if actor is None:
                raise CommandError(f"No existe actor_username '{actor_username}'.")

        service = ProductMonthClosureService()
        results: list[dict[str, object]] = []
        built_count = 0
        error_count = 0

        def run_months() -> None:
            nonlocal built_count, error_count
            for month_start in _iter_months(from_month, to_month):
                month_label = month_start.strftime("%Y-%m")
                try:
                    closure = service.build(
                        month=month_start,
                        rebuild=bool(options.get("rebuild")),
                        lock_after_build=bool(options.get("lock_after_build")),
                        built_by=actor,
                        approval_note=options.get("approval_note") or "",
                        approval_reason=options.get("approval_reason") or "backfill",
                        approval_channel="command_backfill" if not options.get("dry_run") else "command_backfill_dry_run",
                    )
                    if not options.get("dry_run"):
                        built_count += 1
                    results.append(
                        {
                            "month": month_label,
                            "mode": "dry_run" if options.get("dry_run") else "build",
                            "status": closure.status if not options.get("dry_run") else ("ready" if (closure.metadata or {}).get("validation", {}).get("lock_ready") else "warning"),
                            "opening_source": closure.opening_source,
                            "line_count": closure.lines.count(),
                            "total_ending_inventory": str(
                                sum((line.inventario_final_teorico for line in closure.lines.all()), start=0)
                            ),
                            "validation": (closure.metadata or {}).get("validation", {}),
                        }
                    )
                except ProductMonthClosureError as exc:
                    error_count += 1
                    results.append(
                        {
                            "month": month_label,
                            "mode": "dry_run" if options.get("dry_run") else "build",
                            "status": "error",
                            "detail": str(exc),
                        }
                    )

        if options.get("dry_run"):
            with transaction.atomic():
                run_months()
                transaction.set_rollback(True)
        else:
            run_months()

        payload = {
            "from_month": from_month.strftime("%Y-%m"),
            "to_month": to_month.strftime("%Y-%m"),
            "dry_run": bool(options.get("dry_run")),
            "built_count": built_count,
            "error_count": error_count,
            "results": results,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
