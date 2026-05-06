from __future__ import annotations

import csv
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from core.branch_catalog import eligible_operational_branch_qs
from core.models import Sucursal
from recetas.views import _build_forecast_backtest_preview, _parse_date_safe


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Tipo no serializable: {type(value)!r}")


class Command(BaseCommand):
    help = (
        "Evalúa forecast base vs forecast con mix_adjuster sin activarlo en producción "
        "y guarda artefactos comparativos repetibles."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--fecha-base",
            type=str,
            default="",
            help="Fecha base para el backtest en YYYY-MM-DD. Default: hoy.",
        )
        parser.add_argument(
            "--alcance",
            choices=["dia", "mes", "semana", "fin_semana"],
            default="dia",
            help="Alcance del forecast a evaluar.",
        )
        parser.add_argument(
            "--periods",
            type=int,
            default=8,
            help="Número de ventanas de backtest a evaluar.",
        )
        parser.add_argument(
            "--sucursal-id",
            action="append",
            type=int,
            default=[],
            help="Sucursal específica a evaluar. Se puede repetir. Si se omite, corre agregado.",
        )
        parser.add_argument(
            "--all-sucursales",
            action="store_true",
            help="Evalúa una corrida separada para cada sucursal activa.",
        )
        parser.add_argument(
            "--incluir-preparaciones",
            action="store_true",
            help="Incluye preparaciones en la evaluación.",
        )
        parser.add_argument(
            "--safety-pct",
            type=str,
            default="0",
            help="Safety pct del forecast base.",
        )
        parser.add_argument(
            "--min-confianza-pct",
            type=str,
            default="0",
            help="Filtro mínimo de confianza.",
        )
        parser.add_argument(
            "--escenario",
            choices=["base", "bajo", "alto"],
            default="base",
            help="Escenario del forecast a comparar.",
        )
        parser.add_argument(
            "--top",
            type=int,
            default=10,
            help="Top errores por ventana.",
        )
        parser.add_argument(
            "--label",
            type=str,
            default="",
            help="Etiqueta opcional para el nombre de la corrida.",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default="storage/forecast_mix_eval",
            help="Directorio donde se guardan los artefactos de evaluación.",
        )

    def handle(self, *args, **options):
        fecha_base = _parse_date_safe(options.get("fecha_base")) or timezone.localdate()
        alcance = (options.get("alcance") or "dia").strip().lower()
        periods = max(1, min(int(options.get("periods") or 8), 24))
        incluir_preparaciones = bool(options.get("incluir_preparaciones"))
        safety_pct = Decimal(str(options.get("safety_pct") or "0"))
        min_confianza_pct = Decimal(str(options.get("min_confianza_pct") or "0"))
        escenario = (options.get("escenario") or "base").strip().lower()
        top = max(1, min(int(options.get("top") or 10), 50))
        label = (options.get("label") or "").strip().replace(" ", "_")

        sucursal_ids = [int(value) for value in (options.get("sucursal_id") or [])]
        if options.get("all_sucursales"):
            sucursales = list(eligible_operational_branch_qs())
        elif sucursal_ids:
            sucursales = list(eligible_operational_branch_qs().filter(id__in=sucursal_ids))
            found_ids = {s.id for s in sucursales}
            missing = [sid for sid in sucursal_ids if sid not in found_ids]
            if missing:
                raise CommandError(f"Sucursales no encontradas o inactivas: {missing}")
        else:
            sucursales = [None]

        output_dir = Path(options.get("output_dir") or "storage/forecast_mix_eval").expanduser()
        output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        label_suffix = f"_{label}" if label else ""
        run_dir = output_dir / f"mix_adjuster_eval_{stamp}{label_suffix}"
        run_dir.mkdir(parents=True, exist_ok=True)

        summary_rows: list[dict] = []
        window_rows: list[dict] = []
        case_rows: list[dict] = []
        raw_payload: list[dict] = []

        for sucursal in sucursales:
            payload = _build_forecast_backtest_preview(
                alcance=alcance,
                fecha_base=fecha_base,
                periods=periods,
                sucursal=sucursal,
                incluir_preparaciones=incluir_preparaciones,
                safety_pct=safety_pct,
                min_confianza_pct=min_confianza_pct,
                escenario=escenario,
                top=top,
                mix_adjustment_enabled=False,
                include_mix_compare=True,
            )
            scope_label = f"{sucursal.codigo} - {sucursal.nombre}" if sucursal else "Todas"
            scope_key = sucursal.codigo if sucursal else "ALL"
            if payload is None:
                summary_rows.append(
                    {
                        "scope_key": scope_key,
                        "sucursal_id": sucursal.id if sucursal else "",
                        "sucursal_nombre": scope_label,
                        "alcance": alcance,
                        "fecha_base": fecha_base.isoformat(),
                        "periods": periods,
                        "status": "sin_base",
                    }
                )
                continue

            compare = payload.get("mix_adjuster_compare") or {}
            summary_rows.append(
                {
                    "scope_key": scope_key,
                    "sucursal_id": sucursal.id if sucursal else "",
                    "sucursal_nombre": scope_label,
                    "alcance": alcance,
                    "fecha_base": fecha_base.isoformat(),
                    "periods": periods,
                    "status": "ok",
                    "base_mape": (compare.get("base") or {}).get("mape_promedio"),
                    "adjusted_mape": (compare.get("adjusted") or {}).get("mape_promedio"),
                    "base_mae": (compare.get("base") or {}).get("mae_promedio"),
                    "adjusted_mae": (compare.get("adjusted") or {}).get("mae_promedio"),
                    "base_bias_total": (compare.get("base") or {}).get("bias_total"),
                    "adjusted_bias_total": (compare.get("adjusted") or {}).get("bias_total"),
                    "improved_cases": compare.get("improved_cases"),
                    "worsened_cases": compare.get("worsened_cases"),
                    "tie_cases": compare.get("tie_cases"),
                    "improvement_rate_pct": compare.get("improvement_rate_pct"),
                    "mape_improvement_pct": compare.get("mape_improvement_pct"),
                    "base_fallback_rows": (compare.get("base") or {}).get("fallback_rows"),
                    "adjusted_fallback_rows": (compare.get("adjusted") or {}).get("fallback_rows"),
                    "base_portfolio_stability_pct": (compare.get("base") or {}).get("portfolio_stability_pct"),
                    "adjusted_portfolio_stability_pct": (compare.get("adjusted") or {}).get("portfolio_stability_pct"),
                    "base_effective_recent_samples": (compare.get("base") or {}).get("effective_recent_samples"),
                    "adjusted_effective_recent_samples": (compare.get("adjusted") or {}).get("effective_recent_samples"),
                    "recommendation": compare.get("recommendation"),
                    "activation_candidate": compare.get("activation_candidate"),
                }
            )
            for row in (compare.get("windows") or []):
                window_rows.append(
                    {
                        "scope_key": scope_key,
                        "sucursal_id": sucursal.id if sucursal else "",
                        "sucursal_nombre": scope_label,
                        **row,
                    }
                )
            for row in (compare.get("cases") or []):
                case_rows.append(
                    {
                        "scope_key": scope_key,
                        "sucursal_id": sucursal.id if sucursal else "",
                        "sucursal_nombre": scope_label,
                        **row,
                    }
                )
            raw_payload.append(
                {
                    "scope_key": scope_key,
                    "sucursal_id": sucursal.id if sucursal else None,
                    "sucursal_nombre": scope_label,
                    "payload": payload,
                }
            )

        summary_path = run_dir / "summary.csv"
        windows_path = run_dir / "windows.csv"
        cases_path = run_dir / "cases.csv"
        json_path = run_dir / "summary.json"

        self._write_csv(summary_path, summary_rows)
        self._write_csv(windows_path, window_rows)
        self._write_csv(cases_path, case_rows)
        json_path.write_text(
            json.dumps(
                {
                    "generated_at": datetime.now().isoformat(),
                    "run_params": {
                        "alcance": alcance,
                        "fecha_base": fecha_base.isoformat(),
                        "periods": periods,
                        "incluir_preparaciones": incluir_preparaciones,
                        "safety_pct": str(safety_pct),
                        "min_confianza_pct": str(min_confianza_pct),
                        "escenario": escenario,
                        "top": top,
                    },
                    "summary": summary_rows,
                    "windows": window_rows,
                    "cases": case_rows,
                },
                ensure_ascii=False,
                indent=2,
                default=_json_default,
            ),
            encoding="utf-8",
        )

        self.stdout.write(self.style.SUCCESS("Evaluación mix_adjuster generada"))
        self.stdout.write(f"  - run_dir: {run_dir}")
        self.stdout.write(f"  - summary_csv: {summary_path}")
        self.stdout.write(f"  - windows_csv: {windows_path}")
        self.stdout.write(f"  - cases_csv: {cases_path}")
        self.stdout.write(f"  - summary_json: {json_path}")
        self.stdout.write(f"  - scopes evaluados: {len(summary_rows)}")

    def _write_csv(self, path: Path, rows: list[dict]) -> None:
        fieldnames: list[str] = []
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames or ["status"])
            writer.writeheader()
            if not rows:
                writer.writerow({"status": "sin_datos"})
                return
            for row in rows:
                writer.writerow(row)
