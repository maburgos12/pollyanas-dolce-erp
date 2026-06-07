from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from rrhh.services_personnel_identity_sync import build_personnel_identity_projection_plan


class Command(BaseCommand):
    help = "Sincroniza proyecciones seguras desde rrhh.Empleado hacia usuarios ya vinculados."

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica solo acciones seguras. Sin esta bandera el comando es dry-run.",
        )
        parser.add_argument(
            "--include-repartidores",
            action="store_true",
            help="Tambien crea/asegura logistica.Repartidor para empleados ya vinculados.",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            dest="as_json",
            help="Imprime el reporte en JSON.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=80,
            help="Maximo de filas a mostrar. Usa 0 para mostrar todas. Default: 80.",
        )

    def handle(self, *args, **options):
        report = build_personnel_identity_projection_plan(
            apply=options["apply"],
            include_repartidores=options["include_repartidores"],
            limit=options["limit"],
        )
        if options["as_json"]:
            self.stdout.write(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
            return
        self._print_markdown(report)

    def _print_markdown(self, report: dict) -> None:
        summary = report["summary"]
        mode = "apply" if report["writes"] else "dry-run"
        self.stdout.write(f"# Proyecciones seguras de identidad ({mode})")
        self.stdout.write("")
        self.stdout.write(
            f"- empleados activos vinculados: {summary['linked_active_employees']}\n"
            f"- acciones: {summary['actions']} (mostrando {summary['shown']})\n"
            f"- aplicadas: {summary['applied']}\n"
            f"- pendientes seguras: {summary['safe_pending']}\n"
            f"- pendientes manuales: {summary['manual_pending']}\n"
            f"- repartidores incluidos: {'si' if report['include_repartidores'] else 'no'}\n"
            f"- guardrails: {', '.join(report['guardrails'])}"
        )
        self.stdout.write("")
        if not report["actions"]:
            self.stdout.write("Sin proyecciones pendientes.")
            return

        columns = ["Aplicada", "Accion", "Empleado", "Usuario", "Actual", "Propuesto", "Razon"]
        self.stdout.write("| " + " | ".join(columns) + " |")
        self.stdout.write("| " + " | ".join("---" for _ in columns) + " |")
        for item in report["actions"]:
            row = [
                "si" if item["applied"] else "no",
                item["action"],
                f"{item['employee_id']} {item['employee_name']}",
                f"{item['user_id']} {item['username']}",
                item["current_value"],
                item["proposed_value"],
                item["reason"],
            ]
            self.stdout.write("| " + " | ".join(_cell(value) for value in row) + " |")


def _cell(value: object, *, max_length: int = 120) -> str:
    text = str(value or "").replace("\n", " ").replace("|", "\\|").strip()
    if len(text) > max_length:
        return text[: max_length - 1] + "…"
    return text
