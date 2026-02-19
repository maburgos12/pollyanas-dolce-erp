from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
from rapidfuzz import fuzz, process
from django.db import transaction
from django.utils import timezone

from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import Insumo, InsumoAlias, UnidadMedida
from recetas.utils.normalizacion import normalizar_nombre


INVENTARIO_FILE = "1. CONTROL DE INVENTARIO ALMACEN ENERO 2026.xlsx"
ENTRADAS_FILE = "2. CONTROL DE ENTRADAS ALMACEN ENERO 2026.xlsx"
SALIDAS_FILE = "3. CONTROL DE SALIDAS ALMACEN ENERO 2026.xlsx"
MERMA_FILE = "4. CONTROL DE MERMA DE ALMACEN ENERO 2026.xlsx"


IGNORED_NORMALIZED_NAMES = {
    "",
    "-",
    "insumos",
    "producto insumo",
    "almacen 1",
    "almacen 2",
    "almacen casa 1",
    "almacen casa 2",
    "almacen de limpieza",
    "almacen de velas",
    "cuarto frio",
}


@dataclass
class MatchResult:
    insumo: Insumo | None
    metodo: str
    score: float
    nombre_normalizado: str
    sugerencia: str | None = None


@dataclass
class StockRow:
    source: str
    row_index: int
    nombre_origen: str
    unidad_texto: str
    stock_actual: Decimal
    stock_minimo: Decimal
    stock_maximo: Decimal
    inventario_promedio: Decimal
    punto_reorden: Decimal
    dias_llegada_pedido: int
    consumo_diario_promedio: Decimal


@dataclass
class MovementRow:
    source: str
    row_index: int
    tipo: str
    fecha: datetime
    nombre_origen: str
    unidad_texto: str
    cantidad: Decimal
    referencia: str


@dataclass
class ImportSummary:
    rows_stock_read: int = 0
    rows_mov_read: int = 0
    rows_skipped_invalid: int = 0
    matched: int = 0
    unmatched: int = 0
    aliases_created: int = 0
    insumos_created: int = 0
    existencias_updated: int = 0
    movimientos_created: int = 0
    movimientos_skipped_duplicate: int = 0
    errores: list[str] | None = None
    pendientes: list[dict[str, Any]] | None = None

    def __post_init__(self):
        if self.errores is None:
            self.errores = []
        if self.pendientes is None:
            self.pendientes = []


class CatalogMatcher:
    def __init__(self):
        self.by_norm: dict[str, Insumo] = {}
        self.by_alias: dict[str, Insumo] = {}
        self.norm_choices: list[str] = []
        self._load()

    def _load(self) -> None:
        for insumo in Insumo.objects.all().order_by("id"):
            self.by_norm.setdefault(insumo.nombre_normalizado, insumo)
        self.norm_choices = list(self.by_norm.keys())

        aliases = InsumoAlias.objects.select_related("insumo").all().order_by("id")
        for alias in aliases:
            self.by_alias.setdefault(alias.nombre_normalizado, alias.insumo)

    def resolve(self, raw_name: str, fuzzy_threshold: int = 96) -> MatchResult:
        norm = normalizar_nombre(raw_name)
        if _is_ignored_name(norm):
            return MatchResult(insumo=None, metodo="IGNORED", score=0, nombre_normalizado=norm)

        exact = self.by_norm.get(norm)
        if exact:
            return MatchResult(insumo=exact, metodo="EXACT", score=100.0, nombre_normalizado=norm)

        aliased = self.by_alias.get(norm)
        if aliased:
            return MatchResult(insumo=aliased, metodo="ALIAS", score=100.0, nombre_normalizado=norm)

        if not self.norm_choices:
            return MatchResult(insumo=None, metodo="NO_CATALOG", score=0, nombre_normalizado=norm)

        best = process.extractOne(norm, self.norm_choices, scorer=fuzz.WRatio)
        if not best:
            return MatchResult(insumo=None, metodo="NO_MATCH", score=0, nombre_normalizado=norm)

        best_norm, score, _ = best
        candidate = self.by_norm.get(best_norm)
        if candidate and score >= fuzzy_threshold:
            return MatchResult(
                insumo=candidate,
                metodo="FUZZY",
                score=float(score),
                nombre_normalizado=norm,
                sugerencia=candidate.nombre,
            )

        return MatchResult(
            insumo=None,
            metodo="NO_MATCH",
            score=float(score),
            nombre_normalizado=norm,
            sugerencia=candidate.nombre if candidate else None,
        )


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value is None:
            return Decimal(default)
        if isinstance(value, Decimal):
            return value if value.is_finite() else Decimal(default)
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return Decimal(default)
            return Decimal(str(value))
        raw = str(value).strip().replace(",", ".")
        if raw == "" or raw.lower() == "nan":
            return Decimal(default)
        d = Decimal(raw)
        return d if d.is_finite() else Decimal(default)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _to_datetime(fecha_raw: Any, hora_raw: Any = None) -> datetime:
    if fecha_raw is None:
        return timezone.now()

    dt = None
    try:
        dt = pd.to_datetime(fecha_raw, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            dt = None
    except Exception:
        dt = None

    if dt is None:
        return timezone.now()

    if hora_raw is not None:
        hora_text = _as_text(hora_raw)
        if hora_text and hora_text != "-":
            parsed_time = pd.to_datetime(hora_text, errors="coerce")
            if not pd.isna(parsed_time):
                dt = dt.replace(hour=parsed_time.hour, minute=parsed_time.minute, second=0, microsecond=0)

    dt_py = dt.to_pydatetime()
    if timezone.is_naive(dt_py):
        dt_py = timezone.make_aware(dt_py, timezone.get_current_timezone())
    return dt_py


def _is_ignored_name(norm: str) -> bool:
    if not norm:
        return True
    if norm in IGNORED_NORMALIZED_NAMES:
        return True
    if norm.startswith("almacen "):
        return True
    return False


def _unit_from_text(unit_text: str) -> UnidadMedida | None:
    u = normalizar_nombre(unit_text or "")
    if not u:
        return UnidadMedida.objects.filter(codigo="pza").first()

    mapped = {
        "pza": "pza",
        "pieza": "pza",
        "pz": "pza",
        "unidad": "pza",
        "paquete": "pza",
        "rollo": "pza",
        "caja": "pza",
        "costal": "pza",
        "lata": "pza",
        "metro": "pza",
        "kg": "kg",
        "kilogramo": "kg",
        "g": "g",
        "gramo": "g",
        "lt": "lt",
        "l": "lt",
        "litro": "lt",
        "ml": "ml",
        "mililitro": "ml",
        "galon": "lt",
        "garrafon": "lt",
    }
    code = mapped.get(u, "pza")
    return UnidadMedida.objects.filter(codigo=code).first()


def _get_or_create_missing_insumo(raw_name: str, unidad_texto: str) -> tuple[Insumo, bool]:
    norm = normalizar_nombre(raw_name)
    insumo = Insumo.objects.filter(nombre_normalizado=norm).order_by("id").first()
    if insumo:
        if insumo.unidad_base_id is None:
            unit = _unit_from_text(unidad_texto)
            if unit:
                insumo.unidad_base = unit
                insumo.save(update_fields=["unidad_base"])
        return insumo, False

    return (
        Insumo.objects.create(
            nombre=raw_name[:250],
            unidad_base=_unit_from_text(unidad_texto),
        ),
        True,
    )


def _read_inventory_rows(folder: Path) -> list[StockRow]:
    path = folder / INVENTARIO_FILE
    if not path.exists():
        return []

    df = pd.read_excel(path, sheet_name="INVENTARIO", header=None)
    rows: list[StockRow] = []
    for idx in range(4, len(df)):
        name = _as_text(df.iat[idx, 1] if df.shape[1] > 1 else None)
        norm = normalizar_nombre(name)
        if _is_ignored_name(norm):
            continue

        unidad = _as_text(df.iat[idx, 3] if df.shape[1] > 3 else None)
        stock = _to_decimal(df.iat[idx, 8] if df.shape[1] > 8 else None, default="0")
        stock_minimo = _to_decimal(df.iat[idx, 12] if df.shape[1] > 12 else None, default="0")
        stock_maximo = _to_decimal(df.iat[idx, 13] if df.shape[1] > 13 else None, default="0")
        inventario_promedio = _to_decimal(df.iat[idx, 15] if df.shape[1] > 15 else None, default="0")
        reorden = _to_decimal(df.iat[idx, 12] if df.shape[1] > 12 else None, default="0")
        punto_retorno = _to_decimal(df.iat[idx, 17] if df.shape[1] > 17 else None, default=str(reorden))
        dias_llegada = int(_to_decimal(df.iat[idx, 18] if df.shape[1] > 18 else None, default="0"))
        consumo_diario = _to_decimal(df.iat[idx, 19] if df.shape[1] > 19 else None, default="0")

        rows.append(
            StockRow(
                source="inventario",
                row_index=idx + 1,
                nombre_origen=name,
                unidad_texto=unidad,
                stock_actual=stock,
                stock_minimo=stock_minimo,
                stock_maximo=stock_maximo,
                inventario_promedio=inventario_promedio,
                punto_reorden=punto_retorno,
                dias_llegada_pedido=max(dias_llegada, 0),
                consumo_diario_promedio=consumo_diario,
            )
        )

    return rows


def _read_entradas_rows(folder: Path) -> list[MovementRow]:
    path = folder / ENTRADAS_FILE
    if not path.exists():
        return []

    df = pd.read_excel(path, sheet_name="CONTROL DE ENTRADAS DE ALMACEN", header=None)
    rows: list[MovementRow] = []
    for idx in range(3, len(df)):
        nombre = _as_text(df.iat[idx, 7] if df.shape[1] > 7 else None)
        norm = normalizar_nombre(nombre)
        if _is_ignored_name(norm):
            continue

        cantidad = _to_decimal(df.iat[idx, 5] if df.shape[1] > 5 else None, default="0")
        if cantidad <= 0:
            continue

        fecha = _to_datetime(
            df.iat[idx, 0] if df.shape[1] > 0 else None,
            df.iat[idx, 8] if df.shape[1] > 8 else None,
        )
        factura = _as_text(df.iat[idx, 1] if df.shape[1] > 1 else None)
        oc = _as_text(df.iat[idx, 2] if df.shape[1] > 2 else None)
        proveedor = _as_text(df.iat[idx, 3] if df.shape[1] > 3 else None)
        referencia = f"ENTRADA|{factura}|{oc}|{proveedor}|r{idx + 1}".strip("|")

        rows.append(
            MovementRow(
                source="entradas",
                row_index=idx + 1,
                tipo=MovimientoInventario.TIPO_ENTRADA,
                fecha=fecha,
                nombre_origen=nombre,
                unidad_texto=_as_text(df.iat[idx, 6] if df.shape[1] > 6 else None),
                cantidad=cantidad,
                referencia=referencia,
            )
        )

    return rows


def _read_salidas_rows(folder: Path) -> list[MovementRow]:
    path = folder / SALIDAS_FILE
    if not path.exists():
        return []

    df = pd.read_excel(path, sheet_name="Hoja1", header=None)
    rows: list[MovementRow] = []
    for idx in range(3, len(df)):
        nombre = _as_text(df.iat[idx, 2] if df.shape[1] > 2 else None)
        norm = normalizar_nombre(nombre)
        if _is_ignored_name(norm):
            continue

        cantidad = _to_decimal(df.iat[idx, 4] if df.shape[1] > 4 else None, default="0")
        if cantidad <= 0:
            continue

        fecha = _to_datetime(df.iat[idx, 1] if df.shape[1] > 1 else None)
        vale = _as_text(df.iat[idx, 0] if df.shape[1] > 0 else None)
        area = _as_text(df.iat[idx, 5] if df.shape[1] > 5 else None)
        referencia = f"SALIDA|{vale}|{area}|r{idx + 1}".strip("|")

        rows.append(
            MovementRow(
                source="salidas",
                row_index=idx + 1,
                tipo=MovimientoInventario.TIPO_SALIDA,
                fecha=fecha,
                nombre_origen=nombre,
                unidad_texto=_as_text(df.iat[idx, 3] if df.shape[1] > 3 else None),
                cantidad=cantidad,
                referencia=referencia,
            )
        )

    return rows


def _read_merma_rows(folder: Path) -> list[MovementRow]:
    path = folder / MERMA_FILE
    if not path.exists():
        return []

    df = pd.read_excel(path, sheet_name="INVENTARIO", header=None)
    period_end = df.iat[2, 3] if len(df) > 2 and df.shape[1] > 3 else None
    fecha_base = _to_datetime(period_end)

    rows: list[MovementRow] = []
    for idx in range(4, len(df)):
        nombre = _as_text(df.iat[idx, 1] if df.shape[1] > 1 else None)
        norm = normalizar_nombre(nombre)
        if _is_ignored_name(norm):
            continue

        cantidad = _to_decimal(df.iat[idx, 4] if df.shape[1] > 4 else None, default="0")
        if cantidad <= 0:
            continue

        referencia = f"MERMA|{fecha_base.date().isoformat()}|r{idx + 1}"
        rows.append(
            MovementRow(
                source="merma",
                row_index=idx + 1,
                tipo=MovimientoInventario.TIPO_CONSUMO,
                fecha=fecha_base,
                nombre_origen=nombre,
                unidad_texto=_as_text(df.iat[idx, 2] if df.shape[1] > 2 else None),
                cantidad=cantidad,
                referencia=referencia,
            )
        )

    return rows


def collect_rows(folderpath: str) -> tuple[list[StockRow], list[MovementRow]]:
    folder = Path(folderpath)
    stock_rows = _read_inventory_rows(folder)
    movement_rows = []
    movement_rows.extend(_read_entradas_rows(folder))
    movement_rows.extend(_read_salidas_rows(folder))
    movement_rows.extend(_read_merma_rows(folder))
    return stock_rows, movement_rows


def audit_folder(folderpath: str, fuzzy_threshold: int = 90) -> dict[str, Any]:
    stock_rows, movement_rows = collect_rows(folderpath)
    matcher = CatalogMatcher()

    grouped: dict[str, dict[str, Any]] = {}

    def push(source: str, raw_name: str) -> None:
        norm = normalizar_nombre(raw_name)
        if _is_ignored_name(norm):
            return
        bucket = grouped.setdefault(
            norm,
            {
                "nombre_origen": raw_name,
                "nombre_normalizado": norm,
                "frecuencia_total": 0,
                "fuentes": set(),
            },
        )
        bucket["frecuencia_total"] += 1
        bucket["fuentes"].add(source)

    for row in stock_rows:
        push(row.source, row.nombre_origen)

    for row in movement_rows:
        push(row.source, row.nombre_origen)

    rows = []
    matched = 0
    unmatched = 0
    for _, bucket in sorted(grouped.items(), key=lambda kv: (-kv[1]["frecuencia_total"], kv[0])):
        result = matcher.resolve(bucket["nombre_origen"], fuzzy_threshold=fuzzy_threshold)
        status = "MATCH" if result.insumo else "UNMATCHED"
        if status == "MATCH":
            matched += 1
        else:
            unmatched += 1

        rows.append(
            {
                "nombre_origen": bucket["nombre_origen"],
                "nombre_normalizado": bucket["nombre_normalizado"],
                "frecuencia_total": bucket["frecuencia_total"],
                "fuentes": ",".join(sorted(bucket["fuentes"])),
                "match_status": status,
                "metodo_match": result.metodo,
                "score": f"{result.score:.1f}",
                "insumo_id": result.insumo.id if result.insumo else "",
                "insumo_nombre": result.insumo.nombre if result.insumo else "",
                "sugerencia": result.sugerencia or "",
            }
        )

    return {
        "stock_rows": len(stock_rows),
        "movement_rows": len(movement_rows),
        "unique_names": len(rows),
        "matched": matched,
        "unmatched": unmatched,
        "rows": rows,
    }


def _ensure_alias(raw_name: str, insumo: Insumo) -> bool:
    norm = normalizar_nombre(raw_name)
    if _is_ignored_name(norm):
        return False

    _, created = InsumoAlias.objects.get_or_create(
        nombre_normalizado=norm,
        defaults={"nombre": raw_name[:250], "insumo": insumo},
    )
    if not created:
        alias = InsumoAlias.objects.get(nombre_normalizado=norm)
        if alias.insumo_id != insumo.id:
            alias.insumo = insumo
            alias.nombre = raw_name[:250]
            alias.save(update_fields=["insumo", "nombre"])
            return True
    return created


def _apply_movimiento(movimiento: MovimientoInventario) -> None:
    existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=movimiento.insumo)
    if movimiento.tipo == MovimientoInventario.TIPO_ENTRADA:
        existencia.stock_actual += movimiento.cantidad
    else:
        existencia.stock_actual -= movimiento.cantidad
    existencia.actualizado_en = timezone.now()
    existencia.save(update_fields=["stock_actual", "actualizado_en"])


def _build_source_hash(
    source: str,
    row_index: int,
    tipo: str,
    fecha: datetime,
    nombre_norm: str,
    cantidad: Decimal,
    referencia: str,
) -> str:
    raw = "|".join(
        [
            source,
            str(row_index),
            tipo,
            fecha.isoformat(),
            nombre_norm,
            f"{cantidad:.6f}",
            referencia,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@transaction.atomic
def import_folder(
    folderpath: str,
    include_sources: set[str] | None = None,
    fuzzy_threshold: int = 96,
    create_aliases: bool = False,
    alias_threshold: int = 95,
    create_missing_insumos: bool = False,
    dry_run: bool = False,
) -> ImportSummary:
    include_sources = include_sources or {"inventario", "entradas", "salidas", "merma"}
    summary = ImportSummary()
    matcher = CatalogMatcher()

    stock_rows, movement_rows = collect_rows(folderpath)

    if "inventario" in include_sources:
        for row in stock_rows:
            summary.rows_stock_read += 1
            match = matcher.resolve(row.nombre_origen, fuzzy_threshold=fuzzy_threshold)
            if not match.insumo:
                if create_missing_insumos:
                    created_insumo, was_created = _get_or_create_missing_insumo(row.nombre_origen, row.unidad_texto)
                    if was_created:
                        summary.insumos_created += 1
                    match = MatchResult(
                        insumo=created_insumo,
                        metodo="CREATED",
                        score=100.0,
                        nombre_normalizado=normalizar_nombre(row.nombre_origen),
                    )
                else:
                    summary.unmatched += 1
                    summary.pendientes.append(
                        {
                            "source": row.source,
                            "row": row.row_index,
                            "nombre_origen": row.nombre_origen,
                            "nombre_normalizado": match.nombre_normalizado,
                            "score": f"{match.score:.1f}",
                            "sugerencia": match.sugerencia or "",
                        }
                    )
                    continue

            if not match.insumo:
                summary.unmatched += 1
                summary.pendientes.append(
                    {
                        "source": row.source,
                        "row": row.row_index,
                        "nombre_origen": row.nombre_origen,
                        "nombre_normalizado": match.nombre_normalizado,
                        "score": f"{match.score:.1f}",
                        "sugerencia": match.sugerencia or "",
                    }
                )
                continue
            summary.matched += 1
            if create_aliases and match.metodo == "FUZZY" and match.score >= alias_threshold and not dry_run:
                if _ensure_alias(row.nombre_origen, match.insumo):
                    summary.aliases_created += 1

            if dry_run:
                continue

            existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=match.insumo)
            existencia.stock_actual = row.stock_actual
            if row.punto_reorden >= 0:
                existencia.punto_reorden = row.punto_reorden
            if row.stock_minimo >= 0:
                existencia.stock_minimo = row.stock_minimo
            if row.stock_maximo >= 0:
                existencia.stock_maximo = row.stock_maximo
            if row.inventario_promedio >= 0:
                existencia.inventario_promedio = row.inventario_promedio
            if row.consumo_diario_promedio >= 0:
                existencia.consumo_diario_promedio = row.consumo_diario_promedio
            existencia.dias_llegada_pedido = max(int(row.dias_llegada_pedido), 0)
            existencia.actualizado_en = timezone.now()
            existencia.save(
                update_fields=[
                    "stock_actual",
                    "stock_minimo",
                    "stock_maximo",
                    "inventario_promedio",
                    "punto_reorden",
                    "dias_llegada_pedido",
                    "consumo_diario_promedio",
                    "actualizado_en",
                ]
            )
            summary.existencias_updated += 1

    valid_movement_sources = include_sources.intersection({"entradas", "salidas", "merma"})
    for row in movement_rows:
        if row.source not in valid_movement_sources:
            continue
        summary.rows_mov_read += 1

        match = matcher.resolve(row.nombre_origen, fuzzy_threshold=fuzzy_threshold)
        if not match.insumo:
            if create_missing_insumos:
                created_insumo, was_created = _get_or_create_missing_insumo(row.nombre_origen, row.unidad_texto)
                if was_created:
                    summary.insumos_created += 1
                match = MatchResult(
                    insumo=created_insumo,
                    metodo="CREATED",
                    score=100.0,
                    nombre_normalizado=normalizar_nombre(row.nombre_origen),
                )
            else:
                summary.unmatched += 1
                summary.pendientes.append(
                    {
                        "source": row.source,
                        "row": row.row_index,
                        "nombre_origen": row.nombre_origen,
                        "nombre_normalizado": match.nombre_normalizado,
                        "score": f"{match.score:.1f}",
                        "sugerencia": match.sugerencia or "",
                    }
                )
                continue

        summary.matched += 1
        if create_aliases and match.metodo == "FUZZY" and match.score >= alias_threshold and not dry_run:
            if _ensure_alias(row.nombre_origen, match.insumo):
                summary.aliases_created += 1

        source_hash = _build_source_hash(
            source=row.source,
            row_index=row.row_index,
            tipo=row.tipo,
            fecha=row.fecha,
            nombre_norm=match.nombre_normalizado,
            cantidad=row.cantidad,
            referencia=row.referencia,
        )

        if MovimientoInventario.objects.filter(source_hash=source_hash).exists():
            summary.movimientos_skipped_duplicate += 1
            continue

        if dry_run:
            continue

        movimiento = MovimientoInventario.objects.create(
            fecha=row.fecha,
            tipo=row.tipo,
            insumo=match.insumo,
            cantidad=row.cantidad,
            referencia=row.referencia[:120],
            source_hash=source_hash,
        )
        _apply_movimiento(movimiento)
        summary.movimientos_created += 1

    if dry_run:
        transaction.set_rollback(True)

    return summary
