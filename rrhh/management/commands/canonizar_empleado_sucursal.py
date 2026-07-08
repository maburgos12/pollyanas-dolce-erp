from __future__ import annotations

import json

from django.core.management.base import BaseCommand
from django.db import transaction

from core.branch_catalog import resolver_sucursal_por_texto
from core.models import Sucursal
from rrhh.models import Empleado


class Command(BaseCommand):
    help = "Canoniza rrhh.Empleado.sucursal al nombre real de core.Sucursal sin adivinar valores ambiguos."

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica los cambios. Sin esta bandera el comando corre en dry-run.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=200,
            help="Máximo de filas a mostrar. Usa 0 para mostrar todas. Default: 200.",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            dest="as_json",
            help="Imprime el reporte en JSON.",
        )

    def handle(self, *args, **options):
        report = self._build_report(apply=options["apply"], limit=options["limit"])
        if options["as_json"]:
            self.stdout.write(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
            return
        self._print_markdown(report)

    def _build_report(self, *, apply: bool, limit: int) -> dict:
        sucursales_qs = Sucursal.objects.filter(activa=True).order_by("nombre")
        empleados_qs = Empleado.objects.select_related("sucursal_ref").order_by("id")

        rows = []
        writes = 0
        matched = 0
        unresolved = 0

        for empleado in empleados_qs.iterator():
            current_text = (empleado.sucursal or "").strip()
            resolved = empleado.sucursal_ref or resolver_sucursal_por_texto(current_text, sucursal_qs=sucursales_qs)
            if resolved:
                matched += 1
                target_text = resolved.nombre
                needs_text = current_text != target_text
                needs_fk = empleado.sucursal_ref_id != resolved.id
                if not needs_text and not needs_fk:
                    continue
                applied = False
                if apply:
                    with transaction.atomic():
                        empleado.sucursal = target_text
                        empleado.sucursal_ref = resolved
                        empleado.save(update_fields=["sucursal", "sucursal_ref"])
                    writes += 1
                    applied = True
                rows.append(
                    {
                        "empleado_id": empleado.id,
                        "empleado": empleado.nombre,
                        "actual": current_text or "(vacío)",
                        "propuesto": target_text,
                        "sucursal_ref_actual": empleado.sucursal_ref.nombre if empleado.sucursal_ref_id else "",
                        "sucursal_ref_propuesta": resolved.nombre,
                        "applied": applied,
                        "reason": "FK canónico disponible" if empleado.sucursal_ref_id else "Texto resolvible al catálogo real",
                    }
                )
            elif current_text:
                unresolved += 1
                rows.append(
                    {
                        "empleado_id": empleado.id,
                        "empleado": empleado.nombre,
                        "actual": current_text,
                        "propuesto": "",
                        "sucursal_ref_actual": "",
                        "sucursal_ref_propuesta": "",
                        "applied": False,
                        "reason": "No resolvió contra core.Sucursal; revisión manual",
                    }
                )

        shown_rows = rows if limit == 0 else rows[:limit]
        return {
            "writes": apply,
            "summary": {
                "employees": empleados_qs.count(),
                "matched": matched,
                "actions": len(rows),
                "shown": len(shown_rows),
                "applied": writes,
                "unresolved": unresolved,
            },
            "rows": shown_rows,
        }

    def _print_markdown(self, report: dict) -> None:
        summary = report["summary"]
        mode = "apply" if report["writes"] else "dry-run"
        self.stdout.write(f"# Canonización de sucursal de empleados ({mode})")
        self.stdout.write("")
        self.stdout.write(
            f"- empleados: {summary['employees']}\n"
            f"- resolubles: {summary['matched']}\n"
            f"- acciones: {summary['actions']} (mostrando {summary['shown']})\n"
            f"- aplicadas: {summary['applied']}\n"
            f"- no resueltas: {summary['unresolved']}"
        )
        self.stdout.write("")
        if not report["rows"]:
            self.stdout.write("Sin cambios propuestos.")
            return

        columns = ["Aplicada", "Empleado", "Actual", "Propuesto", "FK actual", "FK propuesta", "Razón"]
        self.stdout.write("| " + " | ".join(columns) + " |")
        self.stdout.write("| " + " | ".join("---" for _ in columns) + " |")
        for row in report["rows"]:
            values = [
                "si" if row["applied"] else "no",
                f"{row['empleado_id']} {row['empleado']}",
                row["actual"],
                row["propuesto"],
                row["sucursal_ref_actual"],
                row["sucursal_ref_propuesta"],
                row["reason"],
            ]
            self.stdout.write("| " + " | ".join(_cell(v) for v in values) + " |")


def _cell(value: object, *, max_length: int = 120) -> str:
    text = str(value or "").replace("\n", " ").replace("|", "\\|").strip()
    if len(text) > max_length:
        return text[: max_length - 1] + "…"
    return text
