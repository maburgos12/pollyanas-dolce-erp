from __future__ import annotations

from datetime import datetime

from django.core.management.base import BaseCommand, CommandError

from orquestacion.services.rule_runners import (
    resolve_created_by,
    run_rule_by_code,
)


class Command(BaseCommand):
    help = "Ejecuta una regla puntual del orquestador y registra la corrida, tarea y sugerencia si aplica."

    def add_arguments(self, parser):
        parser.add_argument("--rule", required=True, help="Codigo de la regla a ejecutar.")
        parser.add_argument(
            "--reference-datetime",
            default="",
            help="Fecha y hora local de referencia en formato ISO, por ejemplo 2026-03-30T09:30:00.",
        )
        parser.add_argument(
            "--username",
            default="",
            help="Usuario que dispara la corrida para dejar trazabilidad en la bitacora.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Ejecuta aunque todavia no se haya alcanzado el cutoff de la regla.",
        )
        parser.add_argument(
            "--event-id",
            type=int,
            default=None,
            help="ID de entidad opcional para reglas que lo requieran.",
        )

    def handle(self, *args, **options):
        rule_code = str(options["rule"]).strip()
        reference_dt = self._parse_reference_datetime(str(options.get("reference_datetime") or "").strip())
        created_by = resolve_created_by(str(options.get("username") or "").strip())

        if options.get("username") and created_by is None:
            raise CommandError(f"No existe el usuario '{options['username']}'.")

        try:
            result = run_rule_by_code(
                rule_code,
                reference_dt=reference_dt,
                created_by=created_by,
                force=bool(options.get("force")),
                trigger_source="management_command",
                event_id=options.get("event_id"),
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(
            self.style.SUCCESS(
                f"{result.status}: {result.message} "
                f"(run_id={result.run_id}, task_id={result.task_id}, suggestion_id={result.suggestion_id})"
            )
        )

    def _parse_reference_datetime(self, value: str) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise CommandError("reference-datetime debe venir en formato ISO, ejemplo 2026-03-30T09:30:00.") from exc
