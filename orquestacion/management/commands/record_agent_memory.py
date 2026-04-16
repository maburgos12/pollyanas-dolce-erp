from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from orquestacion.memory_control import append_controlled_memory_entry
from orquestacion.services.agent_runtime import resolve_runtime_actor


class Command(BaseCommand):
    help = "Escribe una entrada controlada en memory.md con evidencia y auditoría."

    def add_arguments(self, parser):
        parser.add_argument("--section", required=True, choices=["fact", "error", "gap"])
        parser.add_argument("--text", required=True, help="Texto estable y reusable que se agregará a memory.md.")
        parser.add_argument("--source", required=True, help="Origen explícito del hallazgo, por ejemplo runtime, test o revisión.")
        parser.add_argument(
            "--evidence",
            action="append",
            default=[],
            help="Referencia de evidencia verificable. Repetir la bandera para múltiples refs.",
        )
        parser.add_argument("--username", default="", help="Usuario a registrar en AuditLog.")
        parser.add_argument(
            "--base-dir",
            default=".",
            help="Directorio base donde vive memory.md. Por defecto usa el repo actual.",
        )

    def handle(self, *args, **options):
        actor = resolve_runtime_actor(str(options.get("username") or "").strip())
        if options.get("username") and actor is None:
            raise CommandError(f"No existe el usuario '{options['username']}'.")

        try:
            result = append_controlled_memory_entry(
                section=str(options["section"]).strip(),
                text=str(options["text"]).strip(),
                evidence_refs=options.get("evidence") or [],
                source=str(options["source"]).strip(),
                actor=actor,
                base_dir=str(options.get("base_dir") or ".").strip(),
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(json.dumps(result.as_dict(), ensure_ascii=False, indent=2)))
