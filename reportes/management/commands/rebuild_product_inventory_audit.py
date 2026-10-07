from __future__ import annotations

import re
from datetime import date

from django.core.management.base import BaseCommand, CommandError

from reportes.services_inventory_traceability import InventoryAuditMaterializer


_MONTH_PATTERN = re.compile(r"^(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])$")


def _parse_month(value: str) -> date:
    match = _MONTH_PATTERN.fullmatch(str(value))
    if match is None:
        raise CommandError("--month debe usar el formato estricto YYYY-MM.")
    return date(int(match.group("year")), int(match.group("month")), 1)


class Command(BaseCommand):
    help = "Reconstruye la auditoría mensual de inventario por producto y ubicación."

    def add_arguments(self, parser):
        parser.add_argument("--month", required=True, type=_parse_month)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--allow-partial", action="store_true", help="Publica solo productos con evidencia identificada; no acredita el cierre mensual.")

    def handle(self, *args, **options):
        month = options["month"]
        if not isinstance(month, date):
            month = _parse_month(month)
        counts = InventoryAuditMaterializer().rebuild(
            month,
            dry_run=bool(options["dry_run"]),
            allow_partial=bool(options["allow_partial"]),
        )
        self.stdout.write(
            " ".join(f"{name}={value}" for name, value in counts.items())
        )
        if not counts.required_sources_available and not counts.partial_published:
            raise CommandError(
                "No están disponibles todas las fuentes requeridas para reconstruir el mes."
            )
