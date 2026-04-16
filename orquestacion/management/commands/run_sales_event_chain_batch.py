from __future__ import annotations

from datetime import datetime

from django.core.management.base import BaseCommand, CommandError

from orquestacion.services.event_chain_scheduler import run_sales_event_chain_batch
from orquestacion.services.rule_runners import resolve_created_by


class Command(BaseCommand):
    help = "Ejecuta la cadena multi-agente sobre eventos comerciales candidatos o una lista explícita de event_ids."

    def add_arguments(self, parser):
        parser.add_argument(
            "--event-ids",
            default="",
            help="Lista separada por comas de event_ids. Si se omite, se seleccionan candidatos automáticamente.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=25,
            help="Máximo de eventos candidatos a revisar cuando no se pasan event_ids explícitos.",
        )
        parser.add_argument(
            "--reference-datetime",
            default="",
            help="Fecha y hora local de referencia en formato ISO, por ejemplo 2026-04-10T09:30:00.",
        )
        parser.add_argument(
            "--username",
            default="",
            help="Usuario que dispara la corrida para dejar trazabilidad.",
        )

    def handle(self, *args, **options):
        reference_dt = self._parse_reference_datetime(str(options.get("reference_datetime") or "").strip())
        created_by = resolve_created_by(str(options.get("username") or "").strip())
        if options.get("username") and created_by is None:
            raise CommandError(f"No existe el usuario '{options['username']}'.")

        raw_event_ids = str(options.get("event_ids") or "").strip()
        event_ids = [int(part.strip()) for part in raw_event_ids.split(",") if part.strip()] if raw_event_ids else None
        results = run_sales_event_chain_batch(
            event_ids=event_ids,
            reference_dt=reference_dt,
            created_by=created_by,
            trigger_source="management_command_batch",
            limit=int(options.get("limit") or 25),
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Eventos procesados={len(results)} :: "
                + ", ".join(
                    f"run_id={result.run_id}|status={result.status}|task_id={result.task_id}|suggestion_id={result.suggestion_id}"
                    for result in results
                )
            )
        )

    def _parse_reference_datetime(self, value: str) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise CommandError("reference-datetime debe venir en formato ISO, ejemplo 2026-04-10T09:30:00.") from exc
