from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from openpyxl import load_workbook
from django.db import transaction

from maestros.models import CostoInsumo, Insumo, UnidadMedida
from recetas.models import LineaReceta, Receta
from recetas.utils.matching import clasificar_match, match_insumo
from recetas.utils.normalizacion import normalizar_nombre


REQUIRED_HEADERS = {"receta", "ingrediente", "cantidad"}


@dataclass
class TemplateImportResult:
    total_rows: int = 0
    recetas_creadas: int = 0
    recetas_actualizadas: int = 0
    recetas_omitidas: int = 0
    lineas_creadas: int = 0
    matches_pendientes: int = 0
    errores: list[str] | None = None

    def __post_init__(self):
        if self.errores is None:
            self.errores = []


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value is None:
            return Decimal(default)
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        raw = str(value).strip().replace(",", ".")
        if raw == "":
            return Decimal(default)
        return Decimal(raw)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _map_header(name: str) -> str:
    n = normalizar_nombre(name)
    aliases = {
        "receta": "receta",
        "nombre receta": "receta",
        "subreceta": "subreceta",
        "producto final": "producto_final",
        "producto": "producto_final",
        "tipo": "tipo",
        "tipo_linea": "tipo_linea",
        "etapa": "etapa",
        "ingrediente": "ingrediente",
        "insumo": "ingrediente",
        "cantidad": "cantidad",
        "unidad": "unidad",
        "costo linea": "costo_linea",
        "costo": "costo_linea",
        "orden": "orden",
        "notas": "notas",
    }
    return aliases.get(n, n)


def _resolve_recipe_type(raw: Any) -> str:
    n = normalizar_nombre(raw)
    if n in {"producto final", "productofinal", "producto_final"}:
        return Receta.TIPO_PRODUCTO_FINAL
    if n in {"preparacion", "base", "subreceta"}:
        return Receta.TIPO_PREPARACION
    if isinstance(raw, str):
        r = raw.strip().upper()
        if r in {Receta.TIPO_PREPARACION, Receta.TIPO_PRODUCTO_FINAL}:
            return r
    return Receta.TIPO_PREPARACION


def _resolve_line_type(raw: Any) -> str:
    n = normalizar_nombre(raw)
    if n in {"subseccion", "sub seccion", "sub-seccion"}:
        return LineaReceta.TIPO_SUBSECCION
    return LineaReceta.TIPO_NORMAL


def _unit_from_text(unit_text: str) -> UnidadMedida | None:
    if not unit_text:
        return None
    u = normalizar_nombre(unit_text)
    convert = {
        "gr": "g",
        "gramo": "g",
        "kg": "kg",
        "ml": "ml",
        "lt": "lt",
        "l": "lt",
        "pza": "pza",
        "pz": "pza",
        "pieza": "pza",
        "unidad": "pza",
    }
    code = convert.get(u, u)
    return UnidadMedida.objects.filter(codigo=code).first()


def _latest_cost_by_insumo(insumo_id: int) -> Decimal | None:
    cost = (
        CostoInsumo.objects.filter(insumo_id=insumo_id)
        .order_by("-fecha", "-id")
        .values_list("costo_unitario", flat=True)
        .first()
    )
    return Decimal(str(cost)) if cost is not None else None


def _should_autocreate_component(
    recipe_type: str,
    tipo_linea: str,
    ingrediente: str,
    costo_linea_value: Decimal | None,
) -> bool:
    if recipe_type != Receta.TIPO_PRODUCTO_FINAL:
        return False
    if tipo_linea != LineaReceta.TIPO_NORMAL:
        return False
    if costo_linea_value is None or costo_linea_value <= 0:
        return False
    ingrediente_norm = normalizar_nombre(ingrediente)
    if not ingrediente_norm:
        return False
    if ingrediente_norm in {"armado", "presentacion", "presentación"}:
        return False
    return ("-" in ingrediente) or (len(ingrediente_norm.split()) >= 2)


def _get_or_create_component_insumo(nombre: str, unidad: UnidadMedida | None) -> Insumo:
    nombre_norm = normalizar_nombre(nombre)
    insumo = Insumo.objects.filter(nombre_normalizado=nombre_norm).order_by("id").first()
    if insumo:
        if insumo.unidad_base is None and unidad is not None:
            insumo.unidad_base = unidad
            insumo.save(update_fields=["unidad_base"])
        return insumo
    return Insumo.objects.create(nombre=nombre[:250], unidad_base=unidad)


def _build_hash(receta_name: str, sheet_name: str, rows: list[dict[str, Any]]) -> str:
    payload = [
        (
            normalizar_nombre(r.get("ingrediente", "")),
            str(_to_decimal(r.get("cantidad"), "0")),
            normalizar_nombre(r.get("unidad", "")),
            str(_to_decimal(r.get("costo_linea"), "0")),
        )
        for r in rows
    ]
    raw = f"{normalizar_nombre(receta_name)}|{sheet_name}|{payload}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def read_template_rows(filepath: str) -> list[dict[str, Any]]:
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {filepath}")

    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                normalized = {_map_header(k): v for k, v in row.items() if k}
                rows.append(normalized)
            return rows

    if path.suffix.lower() in {".xlsx", ".xlsm"}:
        wb = load_workbook(path, data_only=True, read_only=True)
        ws = wb.active
        values = list(ws.values)
        if not values:
            return []

        headers = [_map_header(str(h or "")) for h in values[0]]
        rows = []
        for r in values[1:]:
            row = {}
            for i, header in enumerate(headers):
                if not header:
                    continue
                row[header] = r[i] if i < len(r) else None
            rows.append(row)
        return rows

    raise ValueError("Formato no soportado. Usa .csv, .xlsx o .xlsm")


@transaction.atomic
def import_template(filepath: str, replace_existing: bool = False) -> TemplateImportResult:
    result = TemplateImportResult()
    rows = read_template_rows(filepath)
    result.total_rows = len(rows)
    if not rows:
        result.errores.append("La plantilla no contiene filas de datos.")
        return result

    headers_found = set()
    for r in rows:
        headers_found.update(r.keys())
    missing = REQUIRED_HEADERS - headers_found
    if missing:
        result.errores.append(f"Faltan columnas requeridas: {', '.join(sorted(missing))}")
        return result

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        receta_name = str(row.get("receta") or "").strip()
        ingrediente = str(row.get("ingrediente") or "").strip()
        if not receta_name or not ingrediente:
            continue
        grouped.setdefault(receta_name, []).append(row)

    # Reduce roundtrips in bulk imports (especially over public DB connection).
    unit_cache: dict[str, UnidadMedida | None] = {}
    match_cache: dict[str, tuple[Any, float, str]] = {}
    cost_cache: dict[int, Decimal | None] = {}

    for receta_name, recipe_rows in grouped.items():
        receta_norm = normalizar_nombre(receta_name)
        existing = Receta.objects.filter(nombre_normalizado=receta_norm).order_by("id").first()
        if existing and not replace_existing:
            result.recetas_omitidas += 1
            continue

        sheet_name = str(recipe_rows[0].get("subreceta") or recipe_rows[0].get("producto_final") or "PLANTILLA").strip()[:120]
        recipe_type = _resolve_recipe_type(recipe_rows[0].get("tipo"))
        hash_contenido = _build_hash(receta_name, sheet_name, recipe_rows)

        if existing:
            receta = existing
            receta.nombre = receta_name[:250]
            receta.sheet_name = sheet_name
            receta.tipo = recipe_type
            receta.hash_contenido = hash_contenido
            receta.save()
            receta.lineas.all().delete()
            result.recetas_actualizadas += 1
        else:
            receta = Receta.objects.create(
                nombre=receta_name[:250],
                sheet_name=sheet_name,
                tipo=recipe_type,
                hash_contenido=hash_contenido,
            )
            result.recetas_creadas += 1

        sorted_rows = sorted(
            recipe_rows,
            key=lambda r: (_to_decimal(r.get("orden"), "999999"), str(r.get("ingrediente") or "")),
        )
        pos = 1
        for row in sorted_rows:
            ingrediente = str(row.get("ingrediente") or "").strip()
            if not ingrediente:
                continue
            cantidad = _to_decimal(row.get("cantidad"), "0")
            unidad_texto = str(row.get("unidad") or "").strip()
            costo_linea = _to_decimal(row.get("costo_linea"), "0")
            costo_linea_value = costo_linea if costo_linea > 0 else None
            etapa = str(row.get("etapa") or row.get("notas") or "").strip()[:120]
            tipo_linea = _resolve_line_type(row.get("tipo_linea"))

            if ingrediente in match_cache:
                insumo, score, method = match_cache[ingrediente]
            else:
                insumo, score, method = match_insumo(ingrediente)
                match_cache[ingrediente] = (insumo, score, method)
            status = clasificar_match(score)
            if unidad_texto in unit_cache:
                unidad = unit_cache[unidad_texto]
            else:
                unidad = _unit_from_text(unidad_texto)
                unit_cache[unidad_texto] = unidad

            if insumo is None and _should_autocreate_component(
                recipe_type=recipe_type,
                tipo_linea=tipo_linea,
                ingrediente=ingrediente,
                costo_linea_value=costo_linea_value,
            ):
                insumo = _get_or_create_component_insumo(ingrediente, unidad)
                score = 100.0
                method = "AUTO_COMPONENTE"
                status = LineaReceta.STATUS_AUTO
                match_cache[ingrediente] = (insumo, score, method)

            costo_snapshot = None
            if insumo:
                if insumo.id in cost_cache:
                    costo_snapshot = cost_cache[insumo.id]
                else:
                    costo_snapshot = _latest_cost_by_insumo(insumo.id)
                    cost_cache[insumo.id] = costo_snapshot

            if tipo_linea == LineaReceta.TIPO_SUBSECCION and insumo is None:
                status = LineaReceta.STATUS_AUTO
                method = LineaReceta.MATCH_SUBSECTION
                score = 100.0

            LineaReceta.objects.create(
                receta=receta,
                posicion=pos,
                tipo_linea=tipo_linea,
                etapa=etapa,
                insumo=insumo if status != LineaReceta.STATUS_REJECTED else None,
                insumo_texto=ingrediente[:250],
                cantidad=cantidad if cantidad > 0 else None,
                unidad_texto=unidad_texto[:40],
                unidad=unidad,
                costo_linea_excel=costo_linea_value,
                costo_unitario_snapshot=costo_snapshot,
                match_score=score,
                match_method=method,
                match_status=status,
            )
            result.lineas_creadas += 1
            if status == LineaReceta.STATUS_NEEDS_REVIEW:
                result.matches_pendientes += 1
            pos += 1

    return result
