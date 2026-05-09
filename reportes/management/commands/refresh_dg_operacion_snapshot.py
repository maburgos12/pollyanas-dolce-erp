from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from reportes.services_dg_operacion_snapshot import (
    get_dg_operacion_snapshot_payload,
    refresh_dg_operacion_snapshot,
)


class Command(BaseCommand):
    help = "Genera o actualiza el snapshot persistido de Operación DG."

    def add_arguments(self, parser):
        parser.add_argument("--fecha-operacion", dest="fecha_operacion", help="Fecha operativa YYYY-MM-DD.")
        parser.add_argument("--dg-start-date", dest="start_date", help="Inicio del filtro de planes YYYY-MM-DD.")
        parser.add_argument("--dg-end-date", dest="end_date", help="Fin del filtro de planes YYYY-MM-DD.")
        parser.add_argument(
            "--dg-group-by",
            dest="group_by",
            default="day",
            choices=["day", "week", "month"],
            help="Agrupación del bloque de planes.",
        )

    def handle(self, *args, **options):
        fecha_operacion = self._parse_date(options.get("fecha_operacion"), "fecha-operacion")
        start_date = self._parse_date(options.get("start_date"), "dg-start-date")
        end_date = self._parse_date(options.get("end_date"), "dg-end-date")
        if start_date and end_date and start_date > end_date:
            raise CommandError("dg-start-date no puede ser mayor que dg-end-date.")

        started_at = timezone.now()
        snapshot = refresh_dg_operacion_snapshot(
            fecha_operacion=fecha_operacion,
            start_date=start_date,
            end_date=end_date,
            group_by=options["group_by"],
        )
        elapsed = (timezone.now() - started_at).total_seconds()
        payload = get_dg_operacion_snapshot_payload(snapshot.fecha_operacion) or {}
        sections = snapshot.metadata.get("payload_sections") or sorted(payload.keys())
        self.stdout.write(self.style.SUCCESS("Snapshot DG guardado"))
        self.stdout.write(f"id={snapshot.id}")
        self.stdout.write(f"fecha_operacion={snapshot.fecha_operacion.isoformat()}")
        self.stdout.write(f"status={snapshot.status}")
        self.stdout.write(f"elapsed_seconds={elapsed:.3f}")
        self.stdout.write(f"sections={len(sections)}")
        self.stdout.write(f"snapshot_source={payload.get('dg_snapshot_source', 'database-ready')}")

    def _parse_date(self, raw: str | None, label: str) -> date | None:
        if not raw:
            return None
        try:
            return date.fromisoformat(raw[:10])
        except ValueError as exc:
            raise CommandError(f"{label} debe venir en formato YYYY-MM-DD.") from exc
