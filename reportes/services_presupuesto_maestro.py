from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from django.db import transaction
from django.db.models import Sum
from django.utils.text import slugify
from openpyxl import load_workbook

from core.models import Sucursal
from reportes.models import (
    AreaPresupuesto,
    EmpresaResultadoMensual,
    LineaPresupuestoMensual,
    RubroPresupuesto,
)
from reportes.services_budget_vs_actual import MONTH_COLUMNS, money_decimal, parse_period, variance_pct


MONTH_NAME_TO_NUMBER = {month_name: month_number for month_name, month_number in MONTH_COLUMNS}

AREA_DEFINITIONS = [
    ("ventas", "Ventas", 10),
    ("produccion", "Producción", 20),
    ("gastos-venta", "Gastos de Venta", 30),
    ("administracion", "Administración", 40),
    ("nomina", "Nómina", 50),
    ("logistica", "Logística", 60),
    ("compras", "Compras / Costo", 70),
    ("capex", "CAPEX apertura", 80),
]

RUBRO_TYPES = {
    RubroPresupuesto.TIPO_INGRESO,
    RubroPresupuesto.TIPO_EGRESO,
    RubroPresupuesto.TIPO_COSTO,
    RubroPresupuesto.TIPO_CAPEX,
}

ACTUAL_ALIAS = {
    "ventas": "ventas",
    "venta": "ventas",
    "ingresos": "ventas",
    "costo_mp": "costo_mp",
    "costo materia prima": "costo_mp",
    "materia prima": "costo_mp",
    "compras": "costo_mp",
    "costo_reventa": "costo_reventa",
    "reventa": "costo_reventa",
    "gasto_fijo": "gasto_fijo",
    "gastos venta": "gasto_fijo",
    "gastos de venta": "gasto_fijo",
    "administracion": "gasto_fijo",
    "administración": "gasto_fijo",
    "mano_obra": "mano_obra",
    "mano de obra": "mano_obra",
    "nomina": "mano_obra",
    "nómina": "mano_obra",
    "indirectos": "indirectos",
    "produccion": "indirectos",
    "producción": "indirectos",
    "utilidad_operativa": "utilidad_operativa",
    "utilidad operativa": "utilidad_operativa",
}

ACTUAL_LABELS = {
    "ventas": "Venta total",
    "costo_mp": "Costo materia prima",
    "costo_reventa": "Costo reventa",
    "gasto_fijo": "Gasto fijo",
    "mano_obra": "Mano de obra producción",
    "indirectos": "Indirectos producción",
    "utilidad_operativa": "Utilidad operativa",
}

CAPEX_GUAMUCHIL_ROWS = [
    ("CAPEX Guamúchil local", 2026, 1, Decimal("101518.61")),
    ("CAPEX Guamúchil arquitecta", 2026, 1, Decimal("102300.00")),
    ("CAPEX Guamúchil local", 2026, 2, Decimal("83405.71")),
    ("CAPEX Guamúchil arquitecta", 2026, 2, Decimal("110000.00")),
    ("CAPEX Guamúchil local", 2026, 3, Decimal("294624.92")),
    ("CAPEX Guamúchil arquitecta", 2026, 3, Decimal("125000.00")),
    ("CAPEX Guamúchil equipo", 2026, 2, Decimal("159806.30")),
    ("CAPEX Guamúchil equipo", 2026, 3, Decimal("159806.29")),
]

TOTALIZER_KEYWORDS = (
    "total",
    "subtotal",
    "utilidad",
    "perdida",
    "pérdida",
    "presupuesto",
)

SECTION_HEADER_CONCEPTS = {
    "produccion",
    "producción",
    "logistica",
    "logística",
    "ingresos",
    "egresos",
    "ingreso",
    "egreso",
    "costos",
    "gastos",
    "administracion",
    "administración",
    "nomina",
    "nómina",
}


@dataclass(frozen=True)
class PresupuestoImportSummary:
    area: str
    version: str
    year: int
    rubros_created: int
    rubros_updated: int
    lines_created: int
    lines_updated: int
    skipped_rows: int


def normalize_area_code(value: str) -> str:
    raw = slugify(str(value or "").strip()).replace("_", "-")
    aliases = {
        "gastos-ventas": "gastos-venta",
        "gasto-venta": "gastos-venta",
        "compras-costo": "compras",
        "costos": "compras",
        "produccion": "produccion",
        "produccion-": "produccion",
    }
    return aliases.get(raw, raw)


def normalize_version(value: str | None) -> str:
    raw = str(value or "").strip().upper()
    if raw in {LineaPresupuestoMensual.VERSION_ORIGINAL, LineaPresupuestoMensual.VERSION_REVISADO}:
        return raw
    return LineaPresupuestoMensual.VERSION_ORIGINAL


def normalize_rubro_type(value: str | None, area_code: str) -> str:
    raw = str(value or "").strip().upper()
    if raw in RUBRO_TYPES:
        return raw
    if area_code == "ventas":
        return RubroPresupuesto.TIPO_INGRESO
    if area_code == "capex":
        return RubroPresupuesto.TIPO_CAPEX
    if area_code in {"compras", "produccion"}:
        return RubroPresupuesto.TIPO_COSTO
    return RubroPresupuesto.TIPO_EGRESO


def infer_rubro_type(value: str | None, area_code: str, concept: str) -> str:
    raw = str(value or "").strip().upper()
    if raw in RUBRO_TYPES:
        return raw
    normalized_concept = normalize_concept_text(concept)
    if area_code == "ventas":
        return RubroPresupuesto.TIPO_INGRESO
    if area_code == "capex":
        return RubroPresupuesto.TIPO_CAPEX
    if area_code == "compras":
        return RubroPresupuesto.TIPO_COSTO
    if area_code == "produccion":
        if "costo" in normalized_concept or "insumo" in normalized_concept or "producto" in normalized_concept:
            return RubroPresupuesto.TIPO_COSTO
        return RubroPresupuesto.TIPO_EGRESO
    return RubroPresupuesto.TIPO_EGRESO


def normalize_actual_key(*values: str) -> str:
    joined = " ".join(str(value or "") for value in values).strip().lower()
    compact = " ".join(joined.replace("-", " ").replace("_", " ").split())
    if compact in ACTUAL_ALIAS:
        return ACTUAL_ALIAS[compact]
    slug = slugify(compact).replace("-", "_")
    return ACTUAL_ALIAS.get(slug, "")


def money(value) -> Decimal:
    return money_decimal(value)


def normalize_concept_text(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def is_totalizer_budget_concept(concept: object, *, area_code: str = "") -> bool:
    normalized = normalize_concept_text(concept)
    if not normalized:
        return True
    if normalized in {"gasto", "cuenta", "concepto", "conceptos", "descripcion", "descripción"}:
        return True
    if any(keyword in normalized for keyword in TOTALIZER_KEYWORDS):
        return True
    if normalized in SECTION_HEADER_CONCEPTS:
        return True
    normalized_area = normalize_concept_text(area_code).replace("-", " ")
    if normalized_area and normalized == normalized_area:
        return True
    return False


def ensure_master_budget_areas() -> dict[str, AreaPresupuesto]:
    areas: dict[str, AreaPresupuesto] = {}
    for code, name, order in AREA_DEFINITIONS:
        area, _ = AreaPresupuesto.objects.update_or_create(
            codigo=code,
            defaults={"nombre": name, "orden": order, "activa": True},
        )
        areas[code] = area
    return areas


def month_periods(year: int) -> list[date]:
    return [date(year, month_number, 1) for _, month_number in MONTH_COLUMNS]


class PresupuestoMaestroImportService:
    def _normalize_cell(self, value: object) -> str:
        return normalize_concept_text(value)

    def _csv_rows(self, path: Path) -> Iterable[dict[str, object]]:
        with path.open("r", newline="", encoding="utf-8-sig") as fh:
            yield from csv.DictReader(fh)

    def _xlsx_rows(self, path: Path) -> Iterable[dict[str, object]]:
        workbook = load_workbook(path, read_only=True, data_only=True)
        for sheet in workbook.worksheets:
            yield from self._sales_projection_rows(sheet)
            yield from self._budget_table_rows(sheet)

    def _sales_projection_rows(self, sheet) -> Iterable[dict[str, object]]:
        rows = list(sheet.iter_rows(values_only=True))
        if len(rows) < 5:
            return
        month_row = rows[1]
        projection_row = rows[2]
        metric_row = rows[3]
        if not any("proyeccion 2026" in self._normalize_cell(value).replace("ó", "o") for value in projection_row):
            return

        month_starts: list[tuple[str, int]] = []
        for idx, value in enumerate(month_row):
            month_name = self._normalize_cell(value)
            if month_name in MONTH_NAME_TO_NUMBER:
                month_starts.append((month_name, idx))
        if not month_starts:
            return

        budget_columns: dict[str, int] = {}
        for pos, (month_name, start_idx) in enumerate(month_starts):
            end_idx = month_starts[pos + 1][1] if pos + 1 < len(month_starts) else len(month_row)
            for idx in range(start_idx, end_idx):
                projection_label = self._normalize_cell(projection_row[idx]).replace("ó", "o")
                if "proyeccion 2026" not in projection_label:
                    continue
                venta_idx = idx + 1
                if venta_idx < len(metric_row) and self._normalize_cell(metric_row[venta_idx]) == "venta":
                    budget_columns[month_name] = venta_idx
                    break
        if not budget_columns:
            return

        branch_name = "" if self._normalize_cell(sheet.title) == "general" else sheet.title
        for row in rows[4:]:
            concept = str(row[0] or "").strip() if row else ""
            if is_totalizer_budget_concept(concept, area_code="ventas"):
                continue
            output = {
                "concepto": concept,
                "tipo": RubroPresupuesto.TIPO_INGRESO,
                "sucursal": branch_name,
                "codigo_cuenta": "",
            }
            has_value = False
            for month_name, col_idx in budget_columns.items():
                value = row[col_idx] if col_idx < len(row) else None
                output[month_name] = value
                has_value = has_value or money(value) != 0
            if has_value:
                yield output

    def _budget_table_rows(self, sheet) -> Iterable[dict[str, object]]:
        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            return

        for header_idx in range(min(8, len(rows))):
            budget_columns = self._month_budget_columns(rows, header_idx)
            if not budget_columns:
                continue
            concept_col = self._concept_column(rows, header_idx)
            if concept_col is None:
                continue
            account_col = self._account_column(rows, header_idx)
            branch_name = self._branch_from_sheet(sheet.title)
            for row in rows[header_idx + 1 :]:
                concept = str(row[concept_col] or "").strip() if concept_col < len(row) else ""
                if is_totalizer_budget_concept(concept, area_code=sheet.title):
                    continue
                output = {
                    "concepto": concept,
                    "tipo": "",
                    "sucursal": branch_name,
                    "codigo_cuenta": str(row[account_col] or "").strip() if account_col is not None and account_col < len(row) else "",
                }
                has_value = False
                for month_name, col_idx in budget_columns.items():
                    value = row[col_idx] if col_idx < len(row) else None
                    output[month_name] = value
                    has_value = has_value or money(value) != 0
                if has_value:
                    yield output
            return

    def _month_budget_columns(self, rows: list[tuple[object, ...]], header_idx: int) -> dict[str, int]:
        header = rows[header_idx]
        next_row = rows[header_idx + 1] if header_idx + 1 < len(rows) else ()
        columns: dict[str, int] = {}

        for idx, value in enumerate(header):
            label = self._normalize_cell(value)
            for month_name in MONTH_NAME_TO_NUMBER:
                if label == month_name or label == f"{month_name} presupuestado":
                    if "presupuestado" in label:
                        columns[month_name] = idx
                    elif idx < len(next_row) and self._normalize_cell(next_row[idx]) == "presupuestado":
                        columns[month_name] = idx

        if columns:
            return columns

        for idx, value in enumerate(header):
            label = self._normalize_cell(value)
            if label in MONTH_NAME_TO_NUMBER:
                columns[label] = idx
        return columns if len(columns) >= 3 else {}

    def _concept_column(self, rows: list[tuple[object, ...]], header_idx: int) -> int | None:
        candidates = [rows[header_idx]]
        if header_idx + 1 < len(rows):
            candidates.append(rows[header_idx + 1])
        accepted = {"concepto", "conceptos", "descripcion", "descripción", "producto/insumo", "unidad de negocio"}
        for row in candidates:
            for idx, value in enumerate(row):
                if self._normalize_cell(value) in accepted:
                    return idx
        return 1 if len(rows[header_idx]) > 1 else 0

    def _account_column(self, rows: list[tuple[object, ...]], header_idx: int) -> int | None:
        candidates = [rows[header_idx]]
        if header_idx + 1 < len(rows):
            candidates.append(rows[header_idx + 1])
        for row in candidates:
            for idx, value in enumerate(row):
                if self._normalize_cell(value) in {"cuenta", "clave"}:
                    return idx
        return None

    def _branch_from_sheet(self, title: str) -> str:
        normalized = self._normalize_cell(title)
        if normalized in {
            "general",
            "admon",
            "administracion",
            "produccion",
            "producccion",
            "logistica",
            "logística",
            "mantenimiento",
            "costo de produccion",
            "costo de producción",
            "presupuesto produccion",
            "presupuesto producción",
        }:
            return ""
        return title

    def _rows(self, path: Path) -> Iterable[dict[str, object]]:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return self._csv_rows(path)
        if suffix in {".xlsx", ".xlsm"}:
            return self._xlsx_rows(path)
        raise ValueError("Formato no soportado. Usa CSV o XLSX.")

    def _resolve_branch(self, value: object) -> Sucursal | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        return (
            Sucursal.objects.filter(codigo__iexact=raw).first()
            or Sucursal.objects.filter(nombre__iexact=raw).first()
            or Sucursal.objects.filter(nombre__icontains=raw).first()
        )

    @transaction.atomic
    def import_file(
        self,
        *,
        archivo: str | Path,
        area_code: str,
        version: str = LineaPresupuestoMensual.VERSION_ORIGINAL,
        year: int = 2026,
        source_name: str = "",
    ) -> PresupuestoImportSummary:
        path = Path(archivo).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(path)

        areas = ensure_master_budget_areas()
        normalized_area = normalize_area_code(area_code)
        if normalized_area not in areas:
            raise ValueError(f"Área de presupuesto desconocida: {area_code}")
        area = areas[normalized_area]
        version = normalize_version(version)

        rubros_created = 0
        rubros_updated = 0
        lines_created = 0
        lines_updated = 0
        skipped_rows = 0
        source = source_name or path.stem.upper()

        for row in self._rows(path):
            concept = str(row.get("concepto") or row.get("concept") or "").strip()
            if is_totalizer_budget_concept(concept, area_code=normalized_area):
                skipped_rows += 1
                continue
            branch = self._resolve_branch(row.get("sucursal"))
            account_code = str(row.get("codigo_cuenta") or row.get("cuenta") or "").strip()
            rubro_type = infer_rubro_type(row.get("tipo"), normalized_area, concept)
            rubro, was_created = RubroPresupuesto.objects.update_or_create(
                area=area,
                concepto=concept,
                codigo_cuenta=account_code,
                sucursal=branch,
                defaults={
                    "tipo": rubro_type,
                    "activo": True,
                    "metadata": {
                        "source": source,
                        "source_file": path.name,
                        "actual_key": normalize_actual_key(concept, account_code, area.codigo),
                    },
                },
            )
            rubros_created += int(was_created)
            rubros_updated += int(not was_created)
            for month_name, month_number in MONTH_COLUMNS:
                period = date(year, month_number, 1)
                amount = money(row.get(month_name))
                _, line_created = LineaPresupuestoMensual.objects.update_or_create(
                    rubro=rubro,
                    periodo=period,
                    version=version,
                    defaults={
                        "monto_presupuesto": amount,
                        "metadata": {
                            "source": source,
                            "source_file": path.name,
                        },
                    },
                )
                lines_created += int(line_created)
                lines_updated += int(not line_created)

        return PresupuestoImportSummary(
            area=area.codigo,
            version=version,
            year=year,
            rubros_created=rubros_created,
            rubros_updated=rubros_updated,
            lines_created=lines_created,
            lines_updated=lines_updated,
            skipped_rows=skipped_rows,
        )


class PresupuestoMaestroService:
    def actuals_for_period(self, period: date) -> dict[str, Decimal]:
        result = EmpresaResultadoMensual.objects.filter(periodo=period).first()
        if result is None:
            return {}
        return {
            "ventas": money(result.venta_total),
            "costo_mp": money(result.costo_materia_prima_total),
            "costo_reventa": money(result.costo_reventa_total),
            "gasto_fijo": money(result.gasto_comercial_total) + money(result.gasto_corporativo_total),
            "mano_obra": money(result.mano_obra_prod_total),
            "indirectos": money(result.indirecto_prod_total) + money(result.empaque_prod_total),
            "utilidad_operativa": money(result.utilidad_operativa_total),
        }

    def _line_actual(self, line: LineaPresupuestoMensual, actuals: dict[str, Decimal], consumed: set[str]) -> tuple[Decimal | None, str]:
        rubro = line.rubro
        actual_key = str((rubro.metadata or {}).get("actual_key") or "")
        if not actual_key:
            actual_key = normalize_actual_key(rubro.codigo_cuenta, rubro.concepto, rubro.area.codigo)
        if rubro.sucursal_id:
            return None, ""
        if actual_key and actual_key in actuals and actual_key not in consumed:
            consumed.add(actual_key)
            return actuals[actual_key], "reportes.EmpresaResultadoMensual"
        return line.monto_real, line.fuente_real

    def build_consolidado(
        self,
        *,
        periodo: str | date,
        version: str = LineaPresupuestoMensual.VERSION_ORIGINAL,
        area: str | None = None,
    ) -> dict[str, object]:
        period = parse_period(periodo)
        version = normalize_version(version)
        ensure_master_budget_areas()
        actuals = self.actuals_for_period(period)
        areas_qs = AreaPresupuesto.objects.filter(activa=True).order_by("orden", "nombre")
        if area:
            areas_qs = areas_qs.filter(codigo=normalize_area_code(area))
        lines = (
            LineaPresupuestoMensual.objects.filter(periodo=period, version=version, rubro__area__in=areas_qs)
            .select_related("rubro", "rubro__area", "rubro__sucursal")
            .order_by("rubro__area__orden", "rubro__concepto", "rubro__sucursal__codigo")
        )

        consumed_actuals: set[str] = set()
        area_map: dict[int, dict[str, object]] = {}
        total_budget = Decimal("0")
        total_actual = Decimal("0")
        total_variance = Decimal("0")

        for line in lines:
            rubro = line.rubro
            area_payload = area_map.setdefault(
                rubro.area_id,
                {
                    "id": rubro.area_id,
                    "codigo": rubro.area.codigo,
                    "nombre": rubro.area.nombre,
                    "orden": rubro.area.orden,
                    "rubros": [],
                    "total_presupuesto": Decimal("0"),
                    "total_real": Decimal("0"),
                    "total_varianza": Decimal("0"),
                },
            )
            actual, fuente_real = self._line_actual(line, actuals, consumed_actuals)
            budget = money(line.monto_presupuesto)
            actual_value = money(actual) if actual is not None else Decimal("0")
            variance = actual_value - budget
            pct = variance_pct(variance, budget)
            tone = self._tone(rubro.tipo, variance, pct)
            row = {
                "linea_id": line.id,
                "rubro_id": rubro.id,
                "concepto": rubro.concepto,
                "codigo_cuenta": rubro.codigo_cuenta,
                "tipo": rubro.tipo,
                "sucursal": rubro.sucursal.codigo if rubro.sucursal_id else "",
                "presupuesto": budget,
                "real": actual_value if actual is not None else None,
                "fuente_real": fuente_real,
                "varianza": variance if actual is not None else None,
                "varianza_pct": pct if actual is not None else None,
                "tone": tone,
            }
            area_payload["rubros"].append(row)
            area_payload["total_presupuesto"] += budget
            total_budget += budget
            if actual is not None:
                area_payload["total_real"] += actual_value
                area_payload["total_varianza"] += variance
                total_actual += actual_value
                total_variance += variance

        areas = list(area_map.values())
        for area_payload in areas:
            area_payload["varianza_pct"] = variance_pct(area_payload["total_varianza"], area_payload["total_presupuesto"])
            area_payload["tone"] = "neutral"

        return {
            "periodo": period,
            "version": version,
            "areas": areas,
            "totales": {
                "presupuesto": total_budget,
                "real": total_actual,
                "varianza": total_variance,
                "varianza_pct": variance_pct(total_variance, total_budget),
            },
            "actual_source": "reportes.EmpresaResultadoMensual",
            "actuals": {key: value for key, value in actuals.items()},
            "actual_labels": ACTUAL_LABELS,
        }

    def annual_matrix(self, *, year: int, version: str, area: str | None = None) -> dict[str, object]:
        version = normalize_version(version)
        ensure_master_budget_areas()
        areas_qs = AreaPresupuesto.objects.filter(activa=True).order_by("orden", "nombre")
        if area:
            areas_qs = areas_qs.filter(codigo=normalize_area_code(area))
        rubros = RubroPresupuesto.objects.filter(area__in=areas_qs, activo=True).select_related("area", "sucursal").order_by(
            "area__orden", "area__nombre", "concepto", "sucursal__codigo"
        )
        lines_by_rubro_period = {
            (line.rubro_id, line.periodo.month): line
            for line in LineaPresupuestoMensual.objects.filter(periodo__year=year, version=version, rubro__in=rubros)
        }
        rows = []
        for rubro in rubros:
            month_cells = []
            total = Decimal("0")
            for month_name, month_number in MONTH_COLUMNS:
                line = lines_by_rubro_period.get((rubro.id, month_number))
                amount = money(line.monto_presupuesto if line else Decimal("0"))
                total += amount
                month_cells.append(
                    {
                        "name": month_name,
                        "number": month_number,
                        "linea_id": line.id if line else None,
                        "amount": amount,
                    }
                )
            rows.append(
                {
                    "rubro": rubro,
                    "area": rubro.area,
                    "months": month_cells,
                    "total": total,
                }
            )
        return {
            "year": year,
            "version": version,
            "area": area or "",
            "months": MONTH_COLUMNS,
            "areas": list(areas_qs),
            "rows": rows,
            "total_anual": sum((row["total"] for row in rows), Decimal("0")),
        }

    def update_line_amount(self, *, line_id: int, amount: Decimal) -> LineaPresupuestoMensual:
        line = LineaPresupuestoMensual.objects.select_related("rubro").get(pk=line_id)
        line.monto_presupuesto = money(amount)
        line.save(update_fields=["monto_presupuesto", "actualizado_en"])
        return line

    @transaction.atomic
    def create_rubro_with_empty_year(
        self,
        *,
        area_code: str,
        concepto: str,
        tipo: str,
        year: int,
        version: str,
        codigo_cuenta: str = "",
        sucursal_id: int | None = None,
    ) -> RubroPresupuesto:
        areas = ensure_master_budget_areas()
        area = areas[normalize_area_code(area_code)]
        branch = Sucursal.objects.filter(pk=sucursal_id).first() if sucursal_id else None
        rubro, _ = RubroPresupuesto.objects.update_or_create(
            area=area,
            concepto=concepto.strip(),
            codigo_cuenta=codigo_cuenta.strip(),
            sucursal=branch,
            defaults={
                "tipo": normalize_rubro_type(tipo, area.codigo),
                "activo": True,
                "metadata": {"source": "ui", "actual_key": normalize_actual_key(concepto, codigo_cuenta, area.codigo)},
            },
        )
        for period in month_periods(year):
            LineaPresupuestoMensual.objects.update_or_create(
                rubro=rubro,
                periodo=period,
                version=normalize_version(version),
                defaults={"monto_presupuesto": Decimal("0"), "metadata": {"source": "ui"}},
            )
        return rubro

    def _tone(self, rubro_type: str, variance: Decimal, pct: Decimal) -> str:
        abs_pct = abs(pct)
        if variance == 0 or pct == 0:
            return "neutral"
        favorable = variance > 0 if rubro_type == RubroPresupuesto.TIPO_INGRESO else variance < 0
        if favorable:
            return "success"
        if abs_pct > Decimal("10"):
            return "danger"
        if abs_pct >= Decimal("5"):
            return "warning"
        return "neutral"


def seed_capex_guamuchil_2026() -> dict[str, int]:
    areas = ensure_master_budget_areas()
    area = areas["capex"]
    created = 0
    updated = 0
    for concept, year, month, amount in CAPEX_GUAMUCHIL_ROWS:
        rubro, rubro_created = RubroPresupuesto.objects.update_or_create(
            area=area,
            concepto=concept,
            codigo_cuenta="CAPEX_GUAMUCHIL",
            sucursal=None,
            defaults={
                "tipo": RubroPresupuesto.TIPO_CAPEX,
                "activo": True,
                "metadata": {"source": "seed_presupuesto_2026", "confirmed_real": True},
            },
        )
        _, line_created = LineaPresupuestoMensual.objects.update_or_create(
            rubro=rubro,
            periodo=date(year, month, 1),
            version=LineaPresupuestoMensual.VERSION_ORIGINAL,
            defaults={
                "monto_presupuesto": amount,
                "monto_real": amount,
                "fuente_real": "CAPEX_GUAMUCHIL_CONFIRMADO",
                "metadata": {"source": "seed_presupuesto_2026", "confirmed_real": True},
            },
        )
        created += int(rubro_created or line_created)
        updated += int(not line_created)
    return {
        "areas": AreaPresupuesto.objects.count(),
        "created": created,
        "updated": updated,
        "capex_total": int(
            LineaPresupuestoMensual.objects.filter(
                rubro__area=area,
                periodo__year=2026,
                version=LineaPresupuestoMensual.VERSION_ORIGINAL,
            ).aggregate(total=Sum("monto_presupuesto"))["total"]
            or 0
        ),
    }
