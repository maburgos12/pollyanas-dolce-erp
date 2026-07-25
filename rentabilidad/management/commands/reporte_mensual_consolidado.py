"""
Reporte mensual consolidado de los últimos N meses (default 12).

Uso:
    python manage.py reporte_mensual_consolidado
    python manage.py reporte_mensual_consolidado --hasta 2026-06 --meses 12
    python manage.py reporte_mensual_consolidado --formato csv > reporte.csv
    python manage.py reporte_mensual_consolidado --formato json --detalle
    python manage.py reporte_mensual_consolidado --sucursal "Centro"
"""

from __future__ import annotations

import csv
import io
import json
from datetime import date

from django.core.management.base import BaseCommand, CommandError

from rentabilidad.services_reporte_mensual import (
    build_reporte_mensual_consolidado,
    reporte_a_json,
)


def _parse_hasta(value: str) -> date:
    try:
        year, month = value.strip().split("-", 1)
        return date(int(year), int(month), 1)
    except (ValueError, AttributeError) as exc:
        raise CommandError(f"--hasta inválido '{value}'. Usa YYYY-MM.") from exc


def _fmt(value, suffix: str = "") -> str:
    if value is None:
        return "—"
    return f"{value:,.2f}{suffix}"


class Command(BaseCommand):
    help = "Reporte mensual consolidado: ingresos, nómina (+horas extra), % de ventas, utilidad neta y variación YoY."

    def add_arguments(self, parser):
        parser.add_argument("--hasta", help="Último mes del reporte (YYYY-MM). Default: último mes con P&L.")
        parser.add_argument("--meses", type=int, default=12, help="Cantidad de meses hacia atrás (default 12).")
        parser.add_argument("--sucursal", help="Filtrar por sucursal (id numérico o nombre exacto).")
        parser.add_argument(
            "--formato",
            choices=["tabla", "csv", "json"],
            default="tabla",
            help="Formato de salida (default tabla).",
        )
        parser.add_argument(
            "--detalle",
            action="store_true",
            help="Incluir desglose por sucursal (csv agrega filas por sucursal; tabla lo imprime por mes).",
        )

    def _resolve_sucursal(self, valor: str | None) -> int | None:
        if not valor:
            return None
        from core.models import Sucursal

        valor = valor.strip()
        if valor.isdigit():
            if Sucursal.objects.filter(pk=int(valor)).exists():
                return int(valor)
            raise CommandError(f"No existe sucursal con id {valor}.")
        sucursal = Sucursal.objects.filter(nombre__iexact=valor).first()
        if not sucursal:
            raise CommandError(f"No existe sucursal con nombre '{valor}'.")
        return sucursal.pk

    def handle(self, *args, **options):
        hasta = _parse_hasta(options["hasta"]) if options.get("hasta") else None
        sucursal_id = self._resolve_sucursal(options.get("sucursal"))
        reporte = build_reporte_mensual_consolidado(
            hasta=hasta,
            meses=options["meses"],
            sucursal_id=sucursal_id,
        )
        formato = options["formato"]
        if formato == "json":
            self.stdout.write(json.dumps(reporte_a_json(reporte), ensure_ascii=False, indent=2))
        elif formato == "csv":
            self.stdout.write(self._render_csv(reporte, detalle=options["detalle"]))
        else:
            self._render_tabla(reporte, detalle=options["detalle"])

    def _render_csv(self, reporte: dict, *, detalle: bool) -> str:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "periodo", "sucursal", "ingresos", "nomina", "horas_extra",
                "nomina_total", "nomina_pct_ventas", "utilidad_neta",
                "yoy_ingresos_pct", "yoy_nomina_pct", "yoy_utilidad_pct",
            ]
        )
        for fila in reporte["filas"]:
            writer.writerow(
                [
                    fila["periodo"].strftime("%Y-%m"), "TOTAL",
                    fila["ingresos"], fila["nomina"], fila["horas_extra"],
                    fila["nomina_total"],
                    fila["nomina_pct_ventas"] if fila["nomina_pct_ventas"] is not None else "",
                    fila["utilidad_neta"],
                    fila["yoy"]["ingresos"] if fila["yoy"]["ingresos"] is not None else "",
                    fila["yoy"]["nomina_total"] if fila["yoy"]["nomina_total"] is not None else "",
                    fila["yoy"]["utilidad_neta"] if fila["yoy"]["utilidad_neta"] is not None else "",
                ]
            )
            if detalle:
                for s in fila["sucursales"]:
                    writer.writerow(
                        [
                            fila["periodo"].strftime("%Y-%m"), s["sucursal"],
                            s["ingresos"], s["nomina"], s["horas_extra"],
                            s["nomina_total"],
                            s["nomina_pct_ventas"] if s["nomina_pct_ventas"] is not None else "",
                            s["utilidad_neta"], "", "", "",
                        ]
                    )
        return buffer.getvalue()

    def _render_tabla(self, reporte: dict, *, detalle: bool) -> None:
        encabezado = (
            f"{'Mes':<8} {'Ingresos':>14} {'Nómina':>13} {'Hrs extra':>11} "
            f"{'Nómina tot':>13} {'Nóm %vtas':>10} {'Utilidad':>14} "
            f"{'YoY ing':>9} {'YoY nóm':>9} {'YoY util':>9}"
        )
        self.stdout.write(self.style.MIGRATE_HEADING(encabezado))
        self.stdout.write("-" * len(encabezado))
        for fila in reporte["filas"]:
            self.stdout.write(
                f"{fila['periodo'].strftime('%Y-%m'):<8} "
                f"{_fmt(fila['ingresos']):>14} {_fmt(fila['nomina']):>13} "
                f"{_fmt(fila['horas_extra']):>11} {_fmt(fila['nomina_total']):>13} "
                f"{_fmt(fila['nomina_pct_ventas'], '%'):>10} {_fmt(fila['utilidad_neta']):>14} "
                f"{_fmt(fila['yoy']['ingresos'], '%'):>9} "
                f"{_fmt(fila['yoy']['nomina_total'], '%'):>9} "
                f"{_fmt(fila['yoy']['utilidad_neta'], '%'):>9}"
            )
            if detalle:
                for s in fila["sucursales"]:
                    self.stdout.write(
                        f"  · {s['sucursal']:<24} ing {_fmt(s['ingresos'])} · "
                        f"nóm {_fmt(s['nomina'])} + HE {_fmt(s['horas_extra'])} "
                        f"({_fmt(s['nomina_pct_ventas'], '%')} de vtas) · "
                        f"util {_fmt(s['utilidad_neta'])}"
                    )
        totales = reporte["totales"]
        self.stdout.write("-" * len(encabezado))
        self.stdout.write(
            f"{'TOTAL':<8} {_fmt(totales['ingresos']):>14} {_fmt(totales['nomina']):>13} "
            f"{_fmt(totales['horas_extra']):>11} {_fmt(totales['nomina_total']):>13} "
            f"{'':>10} {_fmt(totales['utilidad_neta']):>14}"
        )
