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
    CAMPOS_FINANCIEROS,
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
    help = "Reporte financiero mensual: ventas, costos, márgenes, gastos, resultado operativo y PE; personal informativo."

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
        try:
            reporte = build_reporte_mensual_consolidado(
                hasta=hasta,
                meses=options["meses"],
                sucursal_id=sucursal_id,
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
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
                "yoy_ingresos_pct", "yoy_nomina_pct", "yoy_utilidad_pct", "alcance", "central_registrado", "resultado_empresa_parcial",
            ] + ["fin_" + k for k in (*CAMPOS_FINANCIEROS, "punto_equilibrio", "margen_pct", "utilidad_pct")]
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
                    fila["alcance"], fila["central_registrado"], fila["resultado_empresa_parcial"],
                ] + [(fila.get("financiero") or {}).get(k, "") for k in (*CAMPOS_FINANCIEROS, "punto_equilibrio", "margen_pct", "utilidad_pct")]
            )
            if detalle:
                for s in fila["sucursales"]:
                    writer.writerow(
                        [
                            fila["periodo"].strftime("%Y-%m"), s["sucursal"],
                            s["ingresos"], s["nomina"], s["horas_extra"],
                            s["nomina_total"],
                            s["nomina_pct_ventas"] if s["nomina_pct_ventas"] is not None else "",
                            s["utilidad_neta"], "", "", "", s["alcance"], "", "",
                        ] + [(s.get("financiero") or {}).get(k, "") for k in (*CAMPOS_FINANCIEROS, "punto_equilibrio", "margen_pct", "utilidad_pct")]
                    )
        return buffer.getvalue()

    def _render_tabla(self, reporte: dict, *, detalle: bool) -> None:
        self.stdout.write("Resultado operativo por mes; — indica ausencia de datos. PE integral mensual.")
        for fila in reporte["filas"]:
            fin = fila.get("financiero") or {}
            self.stdout.write(
                f"{fila['periodo']:%Y-%m} · {fila['alcance']} · ventas {_fmt(fila['ingresos'])} · "
                f"costos y producción {_fmt(fin.get('costos_produccion'))} · "
                f"margen {_fmt(fin.get('margen'))} · gastos {_fmt(fin.get('gastos_operativos'))} · "
                f"resultado {_fmt(fila['utilidad_neta'])} · PE {_fmt(fin.get('punto_equilibrio'))}"
            )
            if detalle:
                for suc in fila["sucursales"]:
                    fin = suc.get("financiero") or {}
                    self.stdout.write(f"  {suc['sucursal']} · resultado {_fmt(suc['utilidad_neta'])} · "
                                      f"PE {_fmt(fin.get('punto_equilibrio'))} · personal directo {_fmt(suc['nomina'])} · "
                                      f"HE operativas informativas {_fmt(suc['horas_extra'])}")
