from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from django.core.management.base import BaseCommand

from recetas.views import (
    _build_dg_operacion_dashboard_payload,
    _export_dg_operacion_dashboard_csv,
    _export_dg_operacion_dashboard_xlsx,
    _parse_date_safe,
)


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Tipo no serializable: {type(value)!r}")


def _normalize_payload_for_json(payload: dict) -> dict:
    resumen = payload["resumen_cierre"]
    return {
        "fecha_operacion": payload["fecha_operacion"],
        "plan_status_dashboard": payload["plan_status_dashboard"],
        "ventas_historicas_summary": payload["ventas_historicas_summary"],
        "reabasto_stage": payload["reabasto_stage"],
        "reabasto_tone": payload["reabasto_tone"],
        "reabasto_detail": payload["reabasto_detail"],
        "demand_history_summary": payload["demand_history_summary"],
        "resumen_cierre": {
            "fecha_corte": resumen["fecha_corte"],
            "total": resumen["total"],
            "en_tiempo": resumen["en_tiempo"],
            "tardias": resumen["tardias"],
            "borrador": resumen["borrador"],
            "pendientes": resumen["pendientes"],
            "listo_8am": resumen["listo_8am"],
            "pendientes_codigos": resumen["pendientes_codigos"],
            "detalle": [
                {
                    "sucursal_codigo": row["sucursal"].codigo,
                    "sucursal_nombre": row["sucursal"].nombre,
                    "estado": row["estado"],
                    "estado_label": row["estado_label"],
                    "actualizado_en": row["actualizado_en"],
                    "semaforo": row["semaforo"],
                }
                for row in resumen["detalle"]
            ],
        },
    }


class Command(BaseCommand):
    help = "Genera snapshot ejecutivo DG de operación integrada: plan, reabasto CEDIS y ventas históricas."

    def add_arguments(self, parser):
        parser.add_argument("--fecha-operacion", type=str, default="", help="Fecha operativa CEDIS en YYYY-MM-DD.")
        parser.add_argument("--dg-start-date", type=str, default="", help="Fecha inicial del corte de planes.")
        parser.add_argument("--dg-end-date", type=str, default="", help="Fecha final del corte de planes.")
        parser.add_argument(
            "--dg-group-by",
            choices=["day", "week", "month"],
            default="day",
            help="Granularidad del corte de planes.",
        )
        parser.add_argument(
            "--format",
            choices=["json", "csv", "xlsx"],
            default="json",
            help="Formato de salida del snapshot.",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default="storage/dg_reports",
            help="Directorio local donde se guardará el snapshot.",
        )

    def handle(self, *args, **options):
        fecha_operacion = _parse_date_safe(options.get("fecha_operacion"))
        dg_start_date = _parse_date_safe(options.get("dg_start_date"))
        dg_end_date = _parse_date_safe(options.get("dg_end_date"))
        dg_group_by = (options.get("dg_group_by") or "day").strip().lower()
        export_format = (options.get("format") or "json").strip().lower()

        payload = _build_dg_operacion_dashboard_payload(
            start_date=dg_start_date,
            end_date=dg_end_date,
            group_by=dg_group_by,
            fecha_operacion=fecha_operacion,
        )

        output_dir = Path(options.get("output_dir") or "storage/dg_reports").expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = {
            "json": "json",
            "csv": "csv",
            "xlsx": "xlsx",
        }[export_format]
        output_path = output_dir / f"dg_operacion_snapshot_{stamp}.{suffix}"

        if export_format == "json":
            normalized = _normalize_payload_for_json(payload)
            output_path.write_text(
                json.dumps(normalized, ensure_ascii=False, indent=2, default=_json_default),
                encoding="utf-8",
            )
        elif export_format == "csv":
            response = _export_dg_operacion_dashboard_csv(payload)
            output_path.write_bytes(response.content)
        else:
            response = _export_dg_operacion_dashboard_xlsx(payload)
            output_path.write_bytes(response.content)

        self.stdout.write(self.style.SUCCESS("Snapshot DG generado"))
        self.stdout.write(f"  - archivo: {output_path}")
        self.stdout.write(f"  - fecha operacion: {payload['fecha_operacion']}")
        self.stdout.write(f"  - plan estatus: {payload['plan_status_dashboard']['status']}")
        self.stdout.write(f"  - reabasto: {payload['reabasto_stage']}")
        self.stdout.write(f"  - ventas historicas: {payload['ventas_historicas_summary']['status']}")
