from __future__ import annotations

import json

from django.core.management.base import BaseCommand, CommandError

from rrhh.services_personnel_audit import build_personnel_identity_audit


class Command(BaseCommand):
    help = "Audita en dry-run la identidad de personal, usuarios ERP, grupos, sucursales y autorizadores."

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
            default=25,
            help="Maximo de ejemplos por categoria. Default: 25.",
        )
        parser.add_argument(
            "--fail-on-risk",
            action="store_true",
            help="Termina con error si hay hallazgos de severidad risk.",
        )

    def handle(self, *args, **options):
        report = build_personnel_identity_audit(limit=options["limit"])
        if options["as_json"]:
            self.stdout.write(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            self._print_text(report)

        if options["fail_on_risk"] and any(item["severity"] == "risk" for item in report["findings"]):
            raise CommandError("Auditoria encontro hallazgos de riesgo.")

    def _print_text(self, report: dict) -> None:
        summary = report["summary"]
        self.stdout.write("Auditoria identidad personal ERP (dry-run)")
        self.stdout.write(
            "Resumen: "
            f"usuarios={summary['users']} activos={summary['active_users']} | "
            f"empleados={summary['employees']} activos={summary['active_employees']} | "
            f"perfiles={summary['profiles']} | grupos={summary['groups']} | "
            f"accesos_modulo={summary['module_access']} | "
            f"hallazgos={summary['finding_count']} en {summary['finding_categories']} categorias"
        )
        if not report["findings"]:
            self.stdout.write(self.style.SUCCESS("Sin hallazgos."))
            return

        for finding in report["findings"]:
            style = self.style.ERROR if finding["severity"] == "risk" else self.style.WARNING
            if finding["severity"] == "info":
                style = self.style.NOTICE
            self.stdout.write(style(f"[{finding['severity']}] {finding['category']}: {finding['count']}"))
            self.stdout.write(f"  {finding['title']}")
            for example in finding["examples"]:
                rendered = ", ".join(f"{key}={value}" for key, value in example.items())
                self.stdout.write(f"  - {rendered}")
