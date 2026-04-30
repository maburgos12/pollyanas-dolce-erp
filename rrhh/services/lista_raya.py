from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd


PERCEPCION = "PERCEPCION"
DEDUCCION = "DEDUCCION"


@dataclass(frozen=True)
class ConceptoListaRaya:
    tipo: str
    codigo: str
    nombre: str
    valor: Decimal
    importe: Decimal


@dataclass(frozen=True)
class EmpleadoListaRaya:
    codigo: str
    nombre: str
    area: str
    rfc: str
    nss: str
    curp: str
    fecha_ingreso: date | None
    salario_diario: Decimal
    sdi: Decimal
    sbc: Decimal
    dias_pagados: Decimal
    horas_trabajadas: Decimal
    horas_dia: Decimal
    horas_extra: Decimal
    ausencias: Decimal
    incapacidades: Decimal
    total_percepciones: Decimal
    total_deducciones: Decimal
    neto: Decimal
    conceptos: list[ConceptoListaRaya] = field(default_factory=list)


@dataclass(frozen=True)
class ListaRayaParseResult:
    source_path: str
    source_hash: str
    empresa: str
    fecha_inicio: date | None
    fecha_fin: date | None
    periodo_numero: str
    empleados: list[EmpleadoListaRaya]
    total_empleados_reportado: int
    total_percepciones_reportado: Decimal
    total_deducciones_reportado: Decimal
    total_neto_reportado: Decimal

    @property
    def total_percepciones_calculado(self) -> Decimal:
        return sum((row.total_percepciones for row in self.empleados), Decimal("0"))

    @property
    def total_deducciones_calculado(self) -> Decimal:
        return sum((row.total_deducciones for row in self.empleados), Decimal("0"))

    @property
    def total_neto_calculado(self) -> Decimal:
        return sum((row.neto for row in self.empleados), Decimal("0"))

    def validation_summary(self) -> dict[str, Any]:
        return {
            "empleados_detectados": len(self.empleados),
            "empleados_reportados": self.total_empleados_reportado,
            "total_percepciones_calculado": str(self.total_percepciones_calculado),
            "total_percepciones_reportado": str(self.total_percepciones_reportado),
            "total_deducciones_calculado": str(self.total_deducciones_calculado),
            "total_deducciones_reportado": str(self.total_deducciones_reportado),
            "total_neto_calculado": str(self.total_neto_calculado),
            "total_neto_reportado": str(self.total_neto_reportado),
            "cuadra_empleados": len(self.empleados) == self.total_empleados_reportado,
            "cuadra_percepciones": self.total_percepciones_calculado == self.total_percepciones_reportado,
            "cuadra_deducciones": self.total_deducciones_calculado == self.total_deducciones_reportado,
            "cuadra_neto": self.total_neto_calculado == self.total_neto_reportado,
        }


def parse_lista_raya_xls(path: str | Path) -> ListaRayaParseResult:
    source = Path(path)
    file_bytes = source.read_bytes()
    source_hash = hashlib.sha256(file_bytes).hexdigest()
    df = pd.read_excel(source, sheet_name="Lista de raya", header=None, engine="xlrd")

    starts = _employee_start_rows(df)
    total_general_row = _find_row_with_exact_text(df, "Total General")
    if total_general_row is None:
        raise ValueError("No se encontró la sección Total General en la lista de raya.")
    if not starts:
        raise ValueError("No se detectaron empleados en la lista de raya.")

    empleados: list[EmpleadoListaRaya] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else total_general_row
        empleados.append(_parse_employee_block(df, start, end))

    return ListaRayaParseResult(
        source_path=str(source),
        source_hash=source_hash,
        empresa=_clean(df.iat[1, 4]),
        fecha_inicio=_parse_period_dates(_clean(df.iat[4, 4]))[0],
        fecha_fin=_parse_period_dates(_clean(df.iat[4, 4]))[1],
        periodo_numero=_clean(df.iat[5, 4]),
        empleados=empleados,
        total_empleados_reportado=int(_find_value_by_label(df, "Total de empleados general") or 0),
        total_percepciones_reportado=_find_total_general_amount(df, "Total Percepciones"),
        total_deducciones_reportado=_find_total_general_amount(df, "Total Deducciones"),
        total_neto_reportado=_find_value_by_label(df, "Neto general"),
    )


def _parse_employee_block(df: pd.DataFrame, start: int, end: int) -> EmpleadoListaRaya:
    row_name = df.iloc[start]
    row_identity = df.iloc[start + 1]
    row_salary = df.iloc[start + 2]
    row_time = df.iloc[start + 3]

    concepts: list[ConceptoListaRaya] = []
    total_percepciones = Decimal("0")
    total_deducciones = Decimal("0")
    neto = Decimal("0")
    ausencias = Decimal("0")
    incapacidades = Decimal("0")

    for row_index in range(start + 4, end):
        row = df.iloc[row_index]
        if any(_clean(value).startswith("Total Departamento") for value in row):
            break
        left_label = _clean(row.iloc[1])
        right_label = _clean(row.iloc[6])
        if left_label == "Total Percepciones":
            total_percepciones = _decimal(row.iloc[3])
            total_deducciones = _decimal(row.iloc[8])
            continue
        if left_label == "Neto a pagar":
            neto = _decimal(row.iloc[3])
            continue
        if left_label == "Ausencias":
            ausencias = _decimal(row.iloc[2])
            continue
        if left_label == "Incapacidades":
            incapacidades = _decimal(row.iloc[2])
            continue
        if left_label and _is_number(row.iloc[0]):
            concepts.append(
                ConceptoListaRaya(
                    tipo=PERCEPCION,
                    codigo=str(int(row.iloc[0])),
                    nombre=left_label,
                    valor=_decimal(row.iloc[2]),
                    importe=_decimal(row.iloc[3]),
                )
            )
        if right_label and _is_number(row.iloc[5]):
            concepts.append(
                ConceptoListaRaya(
                    tipo=DEDUCCION,
                    codigo=str(int(row.iloc[5])),
                    nombre=right_label,
                    valor=_decimal(row.iloc[7]),
                    importe=_decimal(row.iloc[8]),
                )
            )

    return EmpleadoListaRaya(
        codigo=str(int(row_name.iloc[0])),
        nombre=_clean(row_name.iloc[1]),
        area=_clean(row_identity.iloc[1]),
        rfc=_after_colon(row_identity.iloc[2]),
        nss=_after_colon(row_identity.iloc[3]),
        curp=_after_colon(row_time.iloc[7]),
        fecha_ingreso=_parse_date(_after_colon(row_salary.iloc[1])),
        salario_diario=_first_decimal(row_salary.iloc[2]),
        sdi=_first_decimal(row_salary.iloc[3]),
        sbc=_first_decimal(row_salary.iloc[4]),
        dias_pagados=_decimal(row_time.iloc[2]),
        horas_trabajadas=_first_decimal(row_time.iloc[3]),
        horas_dia=_first_decimal(row_time.iloc[4]),
        horas_extra=_decimal(row_time.iloc[6]),
        ausencias=ausencias,
        incapacidades=incapacidades,
        total_percepciones=total_percepciones,
        total_deducciones=total_deducciones,
        neto=neto,
        conceptos=concepts,
    )


def _employee_start_rows(df: pd.DataFrame) -> list[int]:
    starts: list[int] = []
    for index, row in df.iterrows():
        number = row.iloc[0]
        name = row.iloc[1]
        if not _is_number(number) or not isinstance(name, str):
            continue
        stripped = name.strip()
        if stripped and stripped.upper() == stripped and "TOTAL" not in stripped:
            starts.append(index)
    return starts


def _find_row_with_exact_text(df: pd.DataFrame, text: str) -> int | None:
    for index, row in df.iterrows():
        if any(_clean(value) == text for value in row):
            return int(index)
    return None


def _find_value_by_label(df: pd.DataFrame, label: str) -> Decimal:
    for _, row in df.iterrows():
        for col_index, value in enumerate(row):
            if _clean(value) != label:
                continue
            for right_index in range(col_index + 1, len(row)):
                if _decimal(row.iloc[right_index]) != Decimal("0"):
                    return _decimal(row.iloc[right_index])
    return Decimal("0")


def _find_total_general_amount(df: pd.DataFrame, label: str) -> Decimal:
    total_general_row = _find_row_with_exact_text(df, "Total General") or 0
    for _, row in df.iloc[total_general_row:].iterrows():
        for col_index, value in enumerate(row):
            if _clean(value) != label:
                continue
            for right_index in range(col_index + 1, len(row)):
                if _decimal(row.iloc[right_index]) != Decimal("0"):
                    return _decimal(row.iloc[right_index])
    return Decimal("0")


def _parse_period_dates(text: str) -> tuple[date | None, date | None]:
    matches = re.findall(r"(\d{1,2}/[A-Za-zÁÉÍÓÚáéíóú]{3}/\d{4})", text)
    if len(matches) < 2:
        return None, None
    return _parse_date(matches[0]), _parse_date(matches[1])


def _parse_date(value: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    month_map = {
        "ene": 1,
        "feb": 2,
        "mar": 3,
        "abr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dic": 12,
    }
    parts = value.split("/")
    try:
        if len(parts) == 3 and parts[1].lower()[:3] in month_map:
            return date(int(parts[2]), month_map[parts[1].lower()[:3]], int(parts[0]))
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError:
        return None


def _after_colon(value: Any) -> str:
    text = _clean(value)
    return text.split(":", 1)[1].strip() if ":" in text else ""


def _first_decimal(value: Any) -> Decimal:
    match = re.search(r"-?\d+(?:\.\d+)?", _clean(value).replace(",", ""))
    return _decimal(match.group(0)) if match else Decimal("0")


def _decimal(value: Any) -> Decimal:
    text = _clean(value).replace(",", "")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _clean(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not math.isnan(value)
