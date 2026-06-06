from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from rrhh.services_personnel_normalization import build_personnel_normalization_plan


class Command(BaseCommand):
    help = "Genera una tabla dry-run de propuestas para normalizar personal, usuarios y catalogos."

    def add_arguments(self, parser):
        parser.add_argument(
            "--json",
            action="store_true",
            dest="as_json",
            help="Imprime el reporte completo en JSON.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=80,
            help="Maximo de filas a mostrar. Usa 0 para mostrar todas. Default: 80.",
        )
        parser.add_argument(
            "--fail-on-risk",
            action="store_true",
            help="Termina con error si hay propuestas de severidad risk.",
        )

    def handle(self, *args, **options):
        report = build_personnel_normalization_plan(limit=options["limit"])
        if options["as_json"]:
            self.stdout.write(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            self._print_markdown(report)

        if options["fail_on_risk"] and report["summary"]["by_severity"].get("risk", 0):
            raise CommandError("El plan contiene propuestas de riesgo.")

    def _print_markdown(self, report: dict) -> None:
        summary = report["summary"]
        self.stdout.write("# Plan de normalizacion de personal (dry-run)")
        self.stdout.write("")
        self.stdout.write(
            f"- usuarios: {summary['users']} ({summary['active_users']} activos)\n"
            f"- empleados: {summary['employees']} ({summary['active_employees']} activos)\n"
            f"- propuestas: {summary['proposals']} (mostrando {summary['shown']})\n"
            f"- por severidad: {_render_counter(summary['by_severity'])}\n"
            f"- escrituras: no"
        )
        self.stdout.write("")
        if not report["proposals"]:
            self.stdout.write("Sin propuestas de normalizacion.")
            return

        columns = [
            "Sev",
            "Plano",
            "Accion",
            "Entidad",
            "Actual",
            "Propuesto",
            "Auto",
            "Razon",
        ]
        self.stdout.write("| " + " | ".join(columns) + " |")
        self.stdout.write("| " + " | ".join("---" for _ in columns) + " |")
        for item in report["proposals"]:
            row = [
                item["severity"],
                item["plane"],
                item["action"],
                f"{item['entity_type']} {item['entity_id']} {item['display']}".strip(),
                item["current_value"],
                item["proposed_value"],
                "si" if item["auto_apply"] else "no",
                item["reason"],
            ]
            self.stdout.write("| " + " | ".join(_cell(value) for value in row) + " |")


def _render_counter(counter: dict) -> str:
    if not counter:
        return "-"
    return ", ".join(f"{key}={value}" for key, value in sorted(counter.items()))


def _cell(value: object, *, max_length: int = 120) -> str:
    text = str(value or "").replace("\n", " ").replace("|", "\\|").strip()
    if len(text) > max_length:
        return text[: max_length - 1] + "…"
    return text
