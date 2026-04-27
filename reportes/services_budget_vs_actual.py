from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from django.db import transaction

from reportes.models import EmpresaResultadoMensual, PresupuestoImport, PresupuestoLineaMensual, PresupuestoResumenMensual


MONTH_COLUMNS = [
    ("enero", 1),
    ("febrero", 2),
    ("marzo", 3),
    ("abril", 4),
    ("mayo", 5),
    ("junio", 6),
    ("julio", 7),
    ("agosto", 8),
    ("septiembre", 9),
    ("octubre", 10),
    ("noviembre", 11),
    ("diciembre", 12),
]

REQUIRED_BUDGET_CONCEPTS = [
    "ventas",
    "costo_mp",
    "costo_reventa",
    "gasto_fijo",
    "mano_obra",
    "utilidad_operativa",
]

BUDGET_VS_ACTUAL_SOURCE = "PRESUPUESTO_VS_REAL"
BUDGET_CSV_SOURCE = "PRESUPUESTO_2026_CSV"


CONCEPT_LABELS = {
    "ventas": "Ventas",
    "costo_mp": "Costo materia prima",
    "costo_reventa": "Costo reventa",
    "gasto_fijo": "Gasto fijo",
    "mano_obra": "Mano de obra producción",
    "indirectos": "Indirectos producción",
    "utilidad_operativa": "Utilidad operativa",
}

CONCEPT_TYPES = {
    "ventas": "INGRESO",
    "utilidad_operativa": "INGRESO",
    "costo_mp": "COSTO",
    "costo_reventa": "COSTO",
    "gasto_fijo": "COSTO",
    "mano_obra": "COSTO",
    "indirectos": "COSTO",
}

CONCEPT_ALIASES = {
    "venta": "ventas",
    "ventas": "ventas",
    "ingresos": "ventas",
    "costo_mp": "costo_mp",
    "costo materia prima": "costo_mp",
    "materia prima": "costo_mp",
    "cmv": "costo_mp",
    "costo_reventa": "costo_reventa",
    "reventa": "costo_reventa",
    "costo reventa": "costo_reventa",
    "gasto_fijo": "gasto_fijo",
    "gasto fijo": "gasto_fijo",
    "gastos fijos": "gasto_fijo",
    "opex": "gasto_fijo",
    "mano_obra": "mano_obra",
    "mano obra": "mano_obra",
    "mano de obra": "mano_obra",
    "mano de obra produccion": "mano_obra",
    "indirectos": "indirectos",
    "indirecto": "indirectos",
    "indirectos produccion": "indirectos",
    "utilidad": "utilidad_operativa",
    "utilidad_operativa": "utilidad_operativa",
    "utilidad operativa": "utilidad_operativa",
}


@dataclass(frozen=True)
class BudgetCsvImportSummary:
    imports_created: int
    imports_updated: int
    lines_created: int
    lines_updated: int
    periods: list[str]
    missing_required_concepts: list[str]


@dataclass(frozen=True)
class BudgetVsActualSummary:
    period: date
    rows: list[dict[str, object]]
    total_budget: Decimal
    total_actual: Decimal
    total_variance: Decimal
    persisted: bool
    has_budget: bool
    has_actual: bool


def parse_period(value: str | date) -> date:
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    raw = str(value or "").strip()
    if len(raw) == 7:
        year, month = raw.split("-")
        return date(int(year), int(month), 1)
    parsed = date.fromisoformat(raw)
    return date(parsed.year, parsed.month, 1)


def normalize_budget_concept(value: str) -> str:
    raw = " ".join(str(value or "").strip().lower().replace("-", "_").split())
    raw = raw.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    return CONCEPT_ALIASES.get(raw, raw.replace(" ", "_"))


def money_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    raw = str(value).strip()
    if not raw:
        return Decimal("0")
    raw = raw.replace("$", "").replace(",", "").replace(" ", "")
    if raw.startswith("(") and raw.endswith(")"):
        raw = f"-{raw[1:-1]}"
    try:
        return Decimal(raw).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def variance_pct(variance: Decimal, budget: Decimal) -> Decimal:
    if budget == 0:
        return Decimal("0")
    return (variance / budget * Decimal("100")).quantize(Decimal("0.01"))


def budget_tone(concept_type: str, variance: Decimal) -> str:
    if variance == 0:
        return "neutral"
    if concept_type == "INGRESO":
        return "success" if variance > 0 else "danger"
    return "danger" if variance > 0 else "success"


def example_budget_csv_rows() -> list[dict[str, str]]:
    rows = []
    for concept in REQUIRED_BUDGET_CONCEPTS:
        row = {"concepto": concept}
        for month_name, _ in MONTH_COLUMNS:
            row[month_name] = "0.00"
        rows.append(row)
    return rows


def write_example_budget_csv(path: str | Path) -> Path:
    output_path = Path(path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["concepto", *[month_name for month_name, _ in MONTH_COLUMNS]]
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(example_budget_csv_rows())
    return output_path


class BudgetCsvImportService:
    def _file_hash(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _rows(self, path: Path) -> Iterable[dict[str, str]]:
        with path.open("r", newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            headers = {str(header or "").strip().lower() for header in (reader.fieldnames or [])}
            required_headers = {"concepto", *{month_name for month_name, _ in MONTH_COLUMNS}}
            missing_headers = sorted(required_headers - headers)
            if missing_headers:
                raise ValueError(f"CSV sin columnas requeridas: {', '.join(missing_headers)}")
            yield from reader

    @transaction.atomic
    def import_csv(self, csv_path: str | Path, *, year: int = 2026) -> BudgetCsvImportSummary:
        path = Path(csv_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(path)

        file_hash = self._file_hash(path)
        import_obj, created = PresupuestoImport.objects.update_or_create(
            tipo=PresupuestoImport.TIPO_GENERAL,
            fuente_nombre=BUDGET_CSV_SOURCE,
            sheet_name="CSV_2026",
            defaults={
                "archivo_ruta": str(path),
                "archivo_hash": file_hash,
                "titulo": "PRESUPUESTO 2026 CSV",
                "metadata": {
                    "year": year,
                    "source": "cargar_presupuesto_2026",
                    "filename": path.name,
                },
            },
        )

        lines_created = 0
        lines_updated = 0
        periods: set[str] = set()
        concepts_seen: set[str] = set()

        for row_index, row in enumerate(self._rows(path), start=2):
            concept_key = normalize_budget_concept(row.get("concepto", ""))
            if not concept_key:
                continue
            concepts_seen.add(concept_key)
            label = CONCEPT_LABELS.get(concept_key, str(row.get("concepto", "")).strip())
            for month_name, month_number in MONTH_COLUMNS:
                period = date(year, month_number, 1)
                amount = money_decimal(row.get(month_name))
                external_key = f"{BUDGET_CSV_SOURCE}:{year}:{concept_key}:{month_number:02d}"
                _, was_created = PresupuestoLineaMensual.objects.update_or_create(
                    external_key=external_key,
                    defaults={
                        "importacion": import_obj,
                        "period": period,
                        "account_code": concept_key,
                        "concept": label,
                        "annual_budget": Decimal("0"),
                        "annual_actual": Decimal("0"),
                        "annual_variance": Decimal("0"),
                        "monthly_budget": amount,
                        "monthly_actual": Decimal("0"),
                        "monthly_variance": Decimal("0"),
                        "row_index": row_index,
                        "audit_status": PresupuestoLineaMensual.AUDIT_OK,
                        "audit_source": "csv_budget_2026",
                        "metadata": {
                            "concept_key": concept_key,
                            "concept_type": CONCEPT_TYPES.get(concept_key, "COSTO"),
                            "source": "cargar_presupuesto_2026",
                        },
                    },
                )
                lines_created += int(was_created)
                lines_updated += int(not was_created)
                periods.add(period.isoformat())

        missing_required = [concept for concept in REQUIRED_BUDGET_CONCEPTS if concept not in concepts_seen]
        return BudgetCsvImportSummary(
            imports_created=int(created),
            imports_updated=int(not created),
            lines_created=lines_created,
            lines_updated=lines_updated,
            periods=sorted(periods),
            missing_required_concepts=missing_required,
        )


class BudgetVsActualSnapshotService:
    def _budget_by_concept(self, period_start: date) -> dict[str, Decimal]:
        budgets: dict[str, Decimal] = {}
        for line in PresupuestoLineaMensual.objects.filter(period=period_start).select_related("importacion"):
            concept_key = line.metadata.get("concept_key") or normalize_budget_concept(line.account_code or line.concept)
            budgets[concept_key] = budgets.get(concept_key, Decimal("0")) + money_decimal(line.monthly_budget)
        return budgets

    def _actual_by_concept(self, result: EmpresaResultadoMensual | None) -> dict[str, Decimal]:
        if result is None:
            return {}
        return {
            "ventas": money_decimal(result.venta_total),
            "costo_mp": money_decimal(result.costo_materia_prima_total),
            "costo_reventa": money_decimal(result.costo_reventa_total),
            "gasto_fijo": money_decimal(result.gasto_comercial_total) + money_decimal(result.gasto_corporativo_total),
            "mano_obra": money_decimal(result.mano_obra_prod_total),
            "indirectos": money_decimal(result.indirecto_prod_total) + money_decimal(result.empaque_prod_total),
            "utilidad_operativa": money_decimal(result.utilidad_operativa_total),
        }

    def build_snapshot(self, *, period_start: str | date, dry_run: bool = False) -> BudgetVsActualSummary:
        period = parse_period(period_start)
        result = EmpresaResultadoMensual.objects.filter(periodo=period).first()
        budgets = self._budget_by_concept(period)
        actuals = self._actual_by_concept(result)
        concept_keys = list(dict.fromkeys([*REQUIRED_BUDGET_CONCEPTS, "indirectos", *budgets.keys(), *actuals.keys()]))
        rows = []
        for concept_key in concept_keys:
            budget = money_decimal(budgets.get(concept_key, Decimal("0")))
            actual = money_decimal(actuals.get(concept_key, Decimal("0")))
            variance = actual - budget
            concept_type = CONCEPT_TYPES.get(concept_key, "COSTO")
            rows.append(
                {
                    "concept": concept_key,
                    "label": CONCEPT_LABELS.get(concept_key, concept_key.replace("_", " ").title()),
                    "type": concept_type,
                    "budget": budget,
                    "actual": actual,
                    "variance": variance,
                    "variance_pct": variance_pct(variance, budget),
                    "tone": budget_tone(concept_type, variance),
                }
            )

        utility_budget = money_decimal(budgets.get("utilidad_operativa", Decimal("0")))
        utility_actual = money_decimal(actuals.get("utilidad_operativa", Decimal("0")))
        utility_variance = utility_actual - utility_budget

        if not dry_run:
            PresupuestoResumenMensual.objects.update_or_create(
                period=period,
                tipo=PresupuestoResumenMensual.TIPO_FUENTE,
                fuente_nombre=BUDGET_VS_ACTUAL_SOURCE,
                defaults={
                    "total_budget": utility_budget,
                    "total_actual": utility_actual,
                    "total_variance": variance_pct(utility_variance, utility_budget),
                    "line_count": len(rows),
                    "metadata": {
                        "source": BUDGET_VS_ACTUAL_SOURCE,
                        "real_source_model": "reportes.EmpresaResultadoMensual",
                        "budget_source_model": "reportes.PresupuestoLineaMensual",
                        "empresa_resultado_id": result.id if result else None,
                        "empresa_resultado_financial_source": (result.metadata or {}).get("financial_totals_source") if result else "",
                        "rows": [
                            {
                                **row,
                                "budget": str(row["budget"]),
                                "actual": str(row["actual"]),
                                "variance": str(row["variance"]),
                                "variance_pct": str(row["variance_pct"]),
                            }
                            for row in rows
                        ],
                    },
                },
            )

        return BudgetVsActualSummary(
            period=period,
            rows=rows,
            total_budget=utility_budget,
            total_actual=utility_actual,
            total_variance=utility_variance,
            persisted=not dry_run,
            has_budget=any(value != 0 for value in budgets.values()),
            has_actual=result is not None,
        )
