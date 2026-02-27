from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from rapidfuzz import fuzz
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Sucursal
from recetas.models import Receta, RecetaCodigoPointAlias, VentaHistorica, normalizar_codigo_point
from recetas.utils.normalizacion import normalizar_nombre

NON_RECIPE_FAMILIES = {
    "wilton",
    "accesorios",
    "velas",
    "velas granmark",
    "plasticos",
    "regalos",
    "bebidas",
    "hielo",
}

NON_RECIPE_CATEGORIES = {
    "accesorios de reposteria",
    "granmark",
    "alegria",
    "plasticos",
    "regalos",
    "letreros",
    "velas sparklers",
    "industrias lec",
    "te",
    "hielo y agua mar de cortez",
}

NON_RECIPE_TOKENS = (
    "manga",
    "duya",
    "molde",
    "set ",
    "juego ",
    "cepillo",
    "batidora",
    "tarjeta",
    "pluma",
    "libreta",
    "vela",
    "bolsa",
    "caja ",
    "encendedor",
    "servicio",
    "decoracion",
    "deco ",
    "topping",
    "sabor ",
    "extra ",
    "contenedor",
    "aderezo",
    "gragea",
    "pirotecnia",
    "coca",
    "lonchera",
    "plato",
    "servilleta",
    "tenedor",
    "sticker",
    "taza",
    "solido",
)


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    raw = str(value).strip()
    if raw == "":
        return Decimal("0")
    raw = raw.replace("$", "").replace(" ", "").replace(",", ".")
    try:
        return Decimal(raw)
    except Exception:
        return Decimal("0")


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    raw = str(value).strip()
    if raw == "":
        return default
    try:
        return int(float(raw))
    except Exception:
        return default


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    if raw == "":
        return None
    parsed = pd.to_datetime(raw, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.date()


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _map_ventas_header(header: str) -> str:
    key = normalizar_nombre(header).replace("_", " ")
    if key in {"receta", "producto", "nombre receta", "nombre producto", "nombre"}:
        return "receta"
    if key in {"codigo point", "codigo", "sku", "codigo producto"}:
        return "codigo_point"
    if key in {"fecha", "dia", "date"}:
        return "fecha"
    if key in {"cantidad", "cantidad vendida", "unidades", "qty", "ventas"}:
        return "cantidad"
    if key in {"sucursal", "tienda", "store"}:
        return "sucursal"
    if key in {"sucursal codigo", "codigo sucursal", "store code"}:
        return "sucursal_codigo"
    if key in {"tickets", "ticket count", "num tickets"}:
        return "tickets"
    if key in {"monto", "total", "monto total", "importe"}:
        return "monto_total"
    return key


def _read_csv_rows(filepath: Path) -> list[dict[str, Any]]:
    encodings = ["utf-8-sig", "latin-1"]
    last_error: Exception | None = None
    for enc in encodings:
        try:
            with filepath.open("r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                rows: list[dict[str, Any]] = []
                for row in reader:
                    parsed: dict[str, Any] = {}
                    for key, value in (row or {}).items():
                        if not key:
                            continue
                        parsed[_map_ventas_header(str(key))] = value
                    rows.append(parsed)
                return rows
        except Exception as exc:  # pragma: no cover - fallback path
            last_error = exc
    if last_error:
        raise last_error
    return []


def _read_excel_rows(filepath: Path, *, all_sheets: bool) -> list[dict[str, Any]]:
    try:
        sheets = pd.read_excel(filepath, sheet_name=None if all_sheets else 0, dtype=object, header=None)
    except Exception as exc:
        raise CommandError(f"No se pudo leer Excel {filepath.name}: {exc}") from exc

    if isinstance(sheets, pd.DataFrame):
        sheets = {"Sheet1": sheets}

    rows: list[dict[str, Any]] = []
    for sheet_name, frame in (sheets or {}).items():
        if frame is None or frame.empty:
            continue
        legacy_rows = _parse_legacy_point_sheet(frame, sheet_name=sheet_name)
        if legacy_rows:
            rows.extend(legacy_rows)
            continue

        fallback_rows = _parse_generic_sheet(frame)
        rows.extend(fallback_rows)
    return rows


def _parse_generic_sheet(frame: pd.DataFrame) -> list[dict[str, Any]]:
    values = frame.values.tolist()
    if not values:
        return []
    header_idx = _find_header_row(values)
    if header_idx < 0:
        return []

    headers = [_map_ventas_header(str(h or "")) for h in values[header_idx]]
    rows: list[dict[str, Any]] = []
    for raw in values[header_idx + 1 :]:
        parsed: dict[str, Any] = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            parsed[header] = raw[idx] if idx < len(raw) else None
        rows.append(parsed)
    return rows


def _find_header_row(values: list[list[Any]], max_scan: int = 20) -> int:
    scan = min(len(values), max_scan)
    for idx in range(scan):
        row = values[idx] or []
        tokens = {normalizar_nombre(str(c or "")).replace("_", " ") for c in row}
        if {"categoria", "codigo", "producto"}.issubset(tokens):
            return idx
    return -1


def _extract_period_end(values: list[list[Any]], max_scan_rows: int = 6, max_scan_cols: int = 10) -> date | None:
    date_hits: list[date] = []
    for r in range(min(len(values), max_scan_rows)):
        row = values[r] or []
        for c in range(min(len(row), max_scan_cols)):
            cell = row[c]
            if cell is None:
                continue
            txt = str(cell).strip()
            if not txt:
                continue

            for hit in re.findall(r"(\d{1,2}/\d{1,2}/\d{4})", txt):
                try:
                    date_hits.append(datetime.strptime(hit, "%m/%d/%Y").date())
                except Exception:
                    pass
            for hit in re.findall(r"(\d{4}-\d{2}-\d{2})", txt):
                try:
                    date_hits.append(datetime.strptime(hit, "%Y-%m-%d").date())
                except Exception:
                    pass

    if not date_hits:
        return None
    return max(date_hits)


def _parse_legacy_point_sheet(frame: pd.DataFrame, *, sheet_name: str) -> list[dict[str, Any]]:
    values = frame.values.tolist()
    if not values:
        return []

    header_idx = _find_header_row(values)
    if header_idx < 0:
        return []
    header_tokens = [normalizar_nombre(str(c or "")).replace("_", " ") for c in (values[header_idx] or [])]
    if not {"sucursal", "categoria", "codigo", "producto", "cantidad"}.issubset(set(header_tokens)):
        return []

    period_end = _extract_period_end(values)
    rows: list[dict[str, Any]] = []
    sucursal_actual = ""
    for raw in values[header_idx + 1 :]:
        if raw is None:
            continue

        sucursal_cells = [raw[idx] if len(raw) > idx else None for idx in (0, 1, 2)]
        categoria_cell = raw[3] if len(raw) > 3 else None
        codigo_cell = raw[4] if len(raw) > 4 else None
        producto_cell = raw[5] if len(raw) > 5 else None
        cantidad_a = raw[6] if len(raw) > 6 else None
        cantidad_b = raw[7] if len(raw) > 7 else None
        venta_bruta_cell = raw[8] if len(raw) > 8 else None
        venta_neta_cell = raw[11] if len(raw) > 11 else None

        sucursal_tokens = [_text(cell) for cell in sucursal_cells if _text(cell)]
        if sucursal_tokens:
            sucursal_txt = " ".join(sucursal_tokens)
            if normalizar_nombre(sucursal_txt) not in {"sucursal"}:
                sucursal_actual = sucursal_txt

        categoria_txt = _text(categoria_cell)
        codigo_txt = _text(codigo_cell)
        producto_txt = _text(producto_cell)

        canon = normalizar_nombre(" ".join([codigo_txt, producto_txt, categoria_txt]))
        code_norm = normalizar_nombre(codigo_txt)
        product_norm = normalizar_nombre(producto_txt)
        if (
            "total por categoria" in canon
            or "total por sucursal" in canon
            or code_norm.startswith("total por")
            or product_norm.startswith("total por")
        ):
            continue
        if not codigo_txt and not producto_txt:
            continue

        cantidad = _to_decimal(cantidad_a)
        if cantidad == 0:
            cantidad = _to_decimal(cantidad_b)
        if cantidad <= 0:
            continue
        monto_total = _to_decimal(venta_neta_cell)
        if monto_total <= 0:
            monto_total = _to_decimal(venta_bruta_cell)

        rows.append(
            {
                "fecha": period_end,
                "receta": producto_txt,
                "codigo_point": codigo_txt,
                "categoria": categoria_txt,
                "familia": "",
                "cantidad": cantidad,
                "sucursal": sucursal_actual or sheet_name,
                "sucursal_codigo": "",
                "monto_total": monto_total,
            }
        )
    return rows


def _load_rows(filepath: Path, *, all_sheets: bool) -> list[dict[str, Any]]:
    suffix = filepath.suffix.lower()
    if suffix == ".csv":
        return _read_csv_rows(filepath)
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return _read_excel_rows(filepath, all_sheets=all_sheets)
    raise CommandError(f"Formato no soportado para {filepath.name}. Usa CSV/XLSX/XLSM/XLS.")


@dataclass
class ImportCounters:
    read: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    unresolved_receta: int = 0
    unresolved_sucursal: int = 0
    invalid_fecha: int = 0
    point_identity_synced: int = 0
    skipped_non_recipe: int = 0
    created_recetas: int = 0
    incompatible: bool = False


class Command(BaseCommand):
    help = "Importa historial de ventas desde exportes Point (CSV/XLSX/XLSM/XLS) por carpeta o archivo."

    def add_arguments(self, parser):
        parser.add_argument("path", type=str, help="Ruta de archivo o carpeta de exportes Point.")
        parser.add_argument(
            "--pattern",
            type=str,
            default="*",
            help="Patrón de nombre si path es carpeta (ej. '*venta*').",
        )
        parser.add_argument(
            "--recursive",
            action="store_true",
            help="Busca archivos recursivamente dentro de la carpeta.",
        )
        parser.add_argument(
            "--modo",
            choices=["replace", "accumulate"],
            default="replace",
            help="replace: reemplaza receta+fecha+sucursal. accumulate: acumula.",
        )
        parser.add_argument(
            "--fuente",
            type=str,
            default="POINT_EXPORT",
            help="Etiqueta de fuente a guardar en VentaHistorica.fuente.",
        )
        parser.add_argument(
            "--sucursal-default",
            type=str,
            default="",
            help="Sucursal default (id, código o nombre) cuando el archivo no trae sucursal.",
        )
        parser.add_argument(
            "--all-sheets",
            dest="all_sheets",
            action="store_true",
            default=True,
            help="Procesa todas las hojas de cada Excel (default recomendado para exportes Point).",
        )
        parser.add_argument(
            "--first-sheet",
            dest="all_sheets",
            action="store_false",
            help="Procesa solo la primera hoja del Excel.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Solo previsualiza conteos sin escribir en DB.",
        )
        parser.add_argument(
            "--strict-sucursal",
            action="store_true",
            help="Si una sucursal no existe en catálogo, omite fila (default: conserva fila sin sucursal).",
        )
        parser.add_argument(
            "--skip-non-recipe",
            dest="skip_non_recipe",
            action="store_true",
            default=True,
            help="Omite filas de accesorios/no-receta (velas, cajas, toppings, extras, etc.).",
        )
        parser.add_argument(
            "--include-non-recipe",
            dest="skip_non_recipe",
            action="store_false",
            help="Incluye filas no-receta en matching de recetas.",
        )
        parser.add_argument(
            "--create-missing-product-recipes",
            action="store_true",
            help="Crea recetas placeholder (tipo PRODUCTO_FINAL) para productos vendibles sin match.",
        )
        parser.add_argument(
            "--max-files",
            type=int,
            default=0,
            help="Límite máximo de archivos a procesar (0 = sin límite).",
        )
        parser.add_argument(
            "--top-unresolved",
            type=int,
            default=20,
            help="Máximo de ejemplos de receta/sucursal no resuelta a mostrar.",
        )

    def handle(self, *args, **options):
        root = Path(options["path"]).expanduser()
        if not root.exists():
            raise CommandError(f"No existe ruta: {root}")

        default_sucursal = self._resolve_default_sucursal((options.get("sucursal_default") or "").strip())
        files = self._collect_files(
            root=root,
            pattern=(options.get("pattern") or "*").strip() or "*",
            recursive=bool(options.get("recursive")),
            max_files=int(options.get("max_files") or 0),
        )
        if not files:
            raise CommandError("No se encontraron archivos válidos (csv/xlsx/xlsm/xls).")

        modo = str(options.get("modo") or "replace").strip().lower()
        fuente = (str(options.get("fuente") or "POINT_EXPORT").strip()[:40] or "POINT_EXPORT")
        dry_run = bool(options.get("dry_run"))
        all_sheets = bool(options.get("all_sheets"))
        strict_sucursal = bool(options.get("strict_sucursal"))
        skip_non_recipe = bool(options.get("skip_non_recipe"))
        create_missing_product_recipes = bool(options.get("create_missing_product_recipes"))
        top_unresolved = max(0, int(options.get("top_unresolved") or 20))
        self._fuzzy_index_built = False
        self._fuzzy_candidates_producto_final: list[tuple[Receta, str]] = []
        self._fuzzy_candidates_all: list[tuple[Receta, str]] = []
        self._point_name_index_built = False
        self._point_name_to_receta: dict[str, Receta] = {}

        receta_cache: dict[tuple[str, str], Receta | None] = {}
        sucursal_cache: dict[tuple[str, str], Sucursal | None] = {}
        unresolved_recetas: dict[str, int] = {}
        unresolved_sucursales: dict[str, int] = {}
        grand = ImportCounters()
        skipped_incompatible = 0

        self.stdout.write(self.style.NOTICE(f"Procesando {len(files)} archivo(s)..."))
        for filepath in files:
            counters = self._process_file(
                filepath=filepath,
                modo=modo,
                fuente=fuente,
                dry_run=dry_run,
                all_sheets=all_sheets,
                strict_sucursal=strict_sucursal,
                skip_non_recipe=skip_non_recipe,
                create_missing_product_recipes=create_missing_product_recipes,
                default_sucursal=default_sucursal,
                receta_cache=receta_cache,
                sucursal_cache=sucursal_cache,
                unresolved_recetas=unresolved_recetas,
                unresolved_sucursales=unresolved_sucursales,
            )
            if counters.incompatible:
                skipped_incompatible += 1
                self.stdout.write(f"- {filepath.name}: omitido (archivo no compatible con historial de ventas)")
                continue
            grand.read += counters.read
            grand.created += counters.created
            grand.updated += counters.updated
            grand.skipped += counters.skipped
            grand.unresolved_receta += counters.unresolved_receta
            grand.unresolved_sucursal += counters.unresolved_sucursal
            grand.invalid_fecha += counters.invalid_fecha
            grand.point_identity_synced += counters.point_identity_synced
            grand.skipped_non_recipe += counters.skipped_non_recipe
            grand.created_recetas += counters.created_recetas

            self.stdout.write(
                f"- {filepath.name}: leidas={counters.read} creadas={counters.created} actualizadas={counters.updated} omitidas={counters.skipped}"
            )

        title = "Dry-run importación ventas Point" if dry_run else "Importación ventas Point completada"
        self.stdout.write(self.style.SUCCESS(title))
        self.stdout.write(f"  - archivos procesados: {len(files)}")
        self.stdout.write(f"  - archivos no compatibles (omitidos): {skipped_incompatible}")
        self.stdout.write(f"  - filas leídas: {grand.read}")
        self.stdout.write(f"  - creadas: {grand.created}")
        self.stdout.write(f"  - actualizadas: {grand.updated}")
        self.stdout.write(f"  - omitidas: {grand.skipped}")
        self.stdout.write(f"  - recetas no resueltas: {grand.unresolved_receta}")
        self.stdout.write(f"  - sucursales no resueltas: {grand.unresolved_sucursal}")
        self.stdout.write(f"  - filas con fecha inválida: {grand.invalid_fecha}")
        self.stdout.write(f"  - identidades Point sincronizadas: {grand.point_identity_synced}")
        self.stdout.write(f"  - filas omitidas no-receta: {grand.skipped_non_recipe}")
        self.stdout.write(f"  - recetas placeholder creadas: {grand.created_recetas}")

        if unresolved_recetas and top_unresolved > 0:
            self.stdout.write("  - ejemplos receta no resuelta:")
            for name, count in sorted(unresolved_recetas.items(), key=lambda item: item[1], reverse=True)[:top_unresolved]:
                self.stdout.write(f"    * {name}: {count}")

        if unresolved_sucursales and top_unresolved > 0:
            self.stdout.write("  - ejemplos sucursal no resuelta:")
            for name, count in sorted(unresolved_sucursales.items(), key=lambda item: item[1], reverse=True)[:top_unresolved]:
                self.stdout.write(f"    * {name}: {count}")

    def _collect_files(self, *, root: Path, pattern: str, recursive: bool, max_files: int) -> list[Path]:
        allowed = {".csv", ".xlsx", ".xlsm", ".xls"}
        if root.is_file():
            if root.suffix.lower() not in allowed:
                raise CommandError("Archivo no soportado. Usa CSV/XLSX/XLSM/XLS.")
            return [root]

        if recursive:
            matches = [p for p in root.rglob(pattern) if p.is_file() and p.suffix.lower() in allowed]
        else:
            matches = [p for p in root.glob(pattern) if p.is_file() and p.suffix.lower() in allowed]
        matches = sorted(matches, key=lambda p: str(p).lower())
        if max_files > 0:
            return matches[:max_files]
        return matches

    def _resolve_default_sucursal(self, raw: str) -> Sucursal | None:
        if not raw:
            return None
        sucursal = None
        if raw.isdigit():
            sucursal = Sucursal.objects.filter(pk=int(raw), activa=True).first()
        if sucursal is None:
            sucursal = Sucursal.objects.filter(codigo__iexact=raw, activa=True).order_by("id").first()
        if sucursal is None:
            sucursal = Sucursal.objects.filter(nombre__iexact=raw, activa=True).order_by("id").first()
        if sucursal is None:
            raise CommandError(f"Sucursal default no encontrada: {raw}")
        return sucursal

    def _resolve_receta(
        self,
        *,
        receta_name: str,
        codigo_point: str,
        cache: dict[tuple[str, str], Receta | None],
    ) -> Receta | None:
        key = (normalizar_nombre(receta_name), normalizar_codigo_point(codigo_point))
        if key in cache:
            return cache[key]

        receta = None
        if codigo_point:
            receta = Receta.objects.filter(codigo_point__iexact=codigo_point).order_by("id").first()
            if receta is None:
                code_norm = normalizar_codigo_point(codigo_point)
                if code_norm:
                    alias = (
                        RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm, activo=True)
                        .select_related("receta")
                        .first()
                    )
                    if alias and alias.receta_id:
                        receta = alias.receta
        if receta is None and receta_name:
            receta = self._resolve_receta_by_point_name(receta_name)
        if receta is None and receta_name:
            receta = Receta.objects.filter(nombre_normalizado=normalizar_nombre(receta_name)).order_by("id").first()
            if receta is None:
                code_norm = normalizar_codigo_point(receta_name)
                if code_norm:
                    receta = Receta.objects.filter(codigo_point__iexact=receta_name).order_by("id").first()
                    if receta is None:
                        alias = (
                            RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm, activo=True)
                            .select_related("receta")
                            .first()
                        )
                        if alias and alias.receta_id:
                            receta = alias.receta
        if receta is None and receta_name:
            receta = self._resolve_receta_fuzzy(receta_name)
        if receta is not None and receta_name:
            self._point_name_to_receta.setdefault(normalizar_nombre(receta_name), receta)
        cache[key] = receta
        return receta

    def _is_non_recipe_sale_row(self, row: dict[str, Any], *, receta_name: str = "", codigo_point: str = "") -> bool:
        familia = normalizar_nombre(_text(row.get("familia")))
        categoria = normalizar_nombre(_text(row.get("categoria")))
        nombre = normalizar_nombre(receta_name or _text(row.get("receta")))
        alias = normalizar_nombre(_text(row.get("alias")))
        code = normalizar_nombre(codigo_point or _text(row.get("codigo_point")))

        if familia in NON_RECIPE_FAMILIES or categoria in NON_RECIPE_CATEGORIES:
            return True

        joined = f"{nombre} {alias} {code}".strip()
        return any(token in joined for token in NON_RECIPE_TOKENS)

    def _create_missing_product_recipe(
        self,
        *,
        receta_name: str,
        codigo_point: str,
        dry_run: bool,
    ) -> Receta | None:
        base_name = (receta_name or codigo_point or "").strip()
        if not base_name:
            return None

        code_raw = (codigo_point or "").strip()[:80]
        code_norm = normalizar_codigo_point(code_raw)
        if code_norm:
            alias = (
                RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm, activo=True)
                .select_related("receta")
                .first()
            )
            if alias and alias.receta_id:
                return alias.receta

        norm_name = normalizar_nombre(base_name)
        seed = f"auto-point-sales|{norm_name}|{code_norm}"
        salt = 0
        hash_value = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        while Receta.objects.filter(hash_contenido=hash_value).exists():
            salt += 1
            hash_value = hashlib.sha256(f"{seed}|{salt}".encode("utf-8")).hexdigest()

        receta = Receta(
            nombre=base_name[:250],
            codigo_point=code_raw,
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            sheet_name="AUTO_POINT_SALES",
            hash_contenido=hash_value,
        )
        if not dry_run:
            receta.save()
            if code_norm:
                RecetaCodigoPointAlias.objects.get_or_create(
                    codigo_point_normalizado=code_norm,
                    defaults={
                        "receta": receta,
                        "codigo_point": code_raw,
                        "nombre_point": base_name[:250],
                        "activo": True,
                    },
                )
        else:
            receta.id = -(salt + 1)

        self._point_name_to_receta.setdefault(norm_name, receta)
        self._fuzzy_index_built = False
        return receta

    def _build_point_name_index(self) -> None:
        if self._point_name_index_built:
            return
        recetas_qs = Receta.objects.exclude(codigo_point="").exclude(codigo_point__isnull=True).only("id", "nombre", "nombre_normalizado")
        for receta in recetas_qs:
            key = receta.nombre_normalizado or normalizar_nombre(receta.nombre)
            if key and key not in self._point_name_to_receta:
                self._point_name_to_receta[key] = receta
        alias_qs = (
            RecetaCodigoPointAlias.objects.filter(activo=True)
            .exclude(nombre_point__isnull=True)
            .exclude(nombre_point="")
            .select_related("receta")
            .order_by("id")
        )
        for alias in alias_qs:
            if not alias.receta_id:
                continue
            key = normalizar_nombre(alias.nombre_point or "")
            if key and key not in self._point_name_to_receta:
                self._point_name_to_receta[key] = alias.receta
        self._point_name_index_built = True

    def _resolve_receta_by_point_name(self, receta_name: str) -> Receta | None:
        self._build_point_name_index()
        key = normalizar_nombre(receta_name)
        if not key:
            return None
        return self._point_name_to_receta.get(key)

    def _sync_point_identity(
        self,
        *,
        receta: Receta,
        codigo_point: str,
        nombre_point: str,
        dry_run: bool,
    ) -> int:
        changed = 0
        code_raw = (codigo_point or "").strip()
        name_raw = (nombre_point or "").strip()
        code_norm = normalizar_codigo_point(code_raw)
        if not code_norm:
            return 0

        if not receta.codigo_point:
            if not dry_run:
                receta.codigo_point = code_raw[:80]
                receta.save(update_fields=["codigo_point"])
            changed += 1

        alias = RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm).select_related("receta").first()
        if alias is None:
            if not dry_run:
                RecetaCodigoPointAlias.objects.create(
                    receta=receta,
                    codigo_point=code_raw[:80],
                    nombre_point=name_raw[:250] if name_raw else "",
                    activo=True,
                )
            changed += 1
            return changed

        # Si el código ya existe ligado a otra receta, no reasignamos automático por seguridad.
        if alias.receta_id != receta.id:
            return changed

        update_fields: list[str] = []
        if code_raw and alias.codigo_point != code_raw[:80]:
            alias.codigo_point = code_raw[:80]
            update_fields.append("codigo_point")
        if name_raw and alias.nombre_point != name_raw[:250]:
            alias.nombre_point = name_raw[:250]
            update_fields.append("nombre_point")
        if not alias.activo:
            alias.activo = True
            update_fields.append("activo")
        if update_fields:
            changed += 1
            if not dry_run:
                alias.save(update_fields=update_fields + ["actualizado_en"])
        return changed

    def _build_fuzzy_index(self) -> None:
        if self._fuzzy_index_built:
            return
        qs = Receta.objects.only("id", "nombre", "nombre_normalizado", "tipo").order_by("id")
        for receta in qs:
            norm = receta.nombre_normalizado or normalizar_nombre(receta.nombre)
            if not norm:
                continue
            if receta.tipo == Receta.TIPO_PRODUCTO_FINAL:
                self._fuzzy_candidates_producto_final.append((receta, norm))
            self._fuzzy_candidates_all.append((receta, norm))
        self._fuzzy_index_built = True

    def _resolve_receta_fuzzy(self, receta_name: str) -> Receta | None:
        self._build_fuzzy_index()
        needle = normalizar_nombre(receta_name)
        if len(needle) < 4:
            return None

        token_count = len([tok for tok in needle.split(" ") if tok])
        threshold = 90 if token_count <= 2 else 86
        best_score = 0.0
        best_receta: Receta | None = None

        candidates = self._fuzzy_candidates_producto_final or self._fuzzy_candidates_all
        for receta, cand in candidates:
            score = float(fuzz.token_set_ratio(needle, cand))
            if score > best_score:
                best_score = score
                best_receta = receta

        if best_receta and best_score >= threshold:
            return best_receta
        return None

    def _resolve_sucursal(
        self,
        *,
        sucursal_name: str,
        sucursal_code: str,
        default_sucursal: Sucursal | None,
        cache: dict[tuple[str, str], Sucursal | None],
    ) -> Sucursal | None:
        key = (normalizar_nombre(sucursal_name), normalizar_nombre(sucursal_code))
        if key in cache:
            return cache[key]
        sucursal = None
        if sucursal_code:
            sucursal = Sucursal.objects.filter(codigo__iexact=sucursal_code, activa=True).order_by("id").first()
        if sucursal is None and sucursal_name:
            sucursal = Sucursal.objects.filter(nombre__iexact=sucursal_name, activa=True).order_by("id").first()
        if sucursal is None and sucursal_name:
            objetivo = normalizar_nombre(sucursal_name)
            for row in Sucursal.objects.filter(activa=True).only("id", "codigo", "nombre").order_by("id"):
                if normalizar_nombre(row.nombre) == objetivo or normalizar_nombre(row.codigo) == objetivo:
                    sucursal = row
                    break
        sucursal = sucursal or default_sucursal
        cache[key] = sucursal
        return sucursal

    def _process_file(
        self,
        *,
        filepath: Path,
        modo: str,
        fuente: str,
        dry_run: bool,
        all_sheets: bool,
        strict_sucursal: bool,
        skip_non_recipe: bool,
        create_missing_product_recipes: bool,
        default_sucursal: Sucursal | None,
        receta_cache: dict[tuple[str, str], Receta | None],
        sucursal_cache: dict[tuple[str, str], Sucursal | None],
        unresolved_recetas: dict[str, int],
        unresolved_sucursales: dict[str, int],
    ) -> ImportCounters:
        counters = ImportCounters()
        rows = _load_rows(filepath, all_sheets=all_sheets)
        if not rows:
            return counters
        if not self._rows_look_like_sales(rows):
            counters.incompatible = True
            return counters

        tx_cm = transaction.atomic() if not dry_run else transaction.atomic()
        with tx_cm:
            for row in rows:
                counters.read += 1
                receta_name = _text(row.get("receta"))
                codigo_point = _text(row.get("codigo_point"))
                is_non_recipe = skip_non_recipe and self._is_non_recipe_sale_row(
                    row,
                    receta_name=receta_name,
                    codigo_point=codigo_point,
                )
                if is_non_recipe:
                    counters.skipped += 1
                    counters.skipped_non_recipe += 1
                    continue
                receta = self._resolve_receta(
                    receta_name=receta_name,
                    codigo_point=codigo_point,
                    cache=receta_cache,
                )
                if receta is None:
                    if create_missing_product_recipes:
                        receta = self._create_missing_product_recipe(
                            receta_name=receta_name,
                            codigo_point=codigo_point,
                            dry_run=dry_run,
                        )
                        if receta is not None:
                            receta_cache[(normalizar_nombre(receta_name), normalizar_codigo_point(codigo_point))] = receta
                            counters.created_recetas += 1
                if receta is None:
                    counters.skipped += 1
                    counters.unresolved_receta += 1
                    unresolved_key = receta_name or codigo_point or "sin_identificador"
                    unresolved_recetas[unresolved_key] = unresolved_recetas.get(unresolved_key, 0) + 1
                    continue
                counters.point_identity_synced += self._sync_point_identity(
                    receta=receta,
                    codigo_point=codigo_point,
                    nombre_point=receta_name,
                    dry_run=dry_run,
                )

                fecha = _to_date(row.get("fecha"))
                if not fecha:
                    counters.skipped += 1
                    counters.invalid_fecha += 1
                    continue

                cantidad = _to_decimal(row.get("cantidad"))
                if cantidad < 0:
                    counters.skipped += 1
                    continue

                sucursal_name = _text(row.get("sucursal"))
                sucursal_code = _text(row.get("sucursal_codigo"))
                sucursal = self._resolve_sucursal(
                    sucursal_name=sucursal_name,
                    sucursal_code=sucursal_code,
                    default_sucursal=default_sucursal,
                    cache=sucursal_cache,
                )
                if (sucursal_name or sucursal_code) and sucursal is None:
                    counters.unresolved_sucursal += 1
                    unresolved_key = sucursal_code or sucursal_name
                    unresolved_sucursales[unresolved_key] = unresolved_sucursales.get(unresolved_key, 0) + 1
                    if strict_sucursal:
                        counters.skipped += 1
                        continue

                tickets = max(0, _to_int(row.get("tickets"), default=0))
                monto_total = _to_decimal(row.get("monto_total"))
                monto_store = monto_total if monto_total > 0 else None

                existing_qs = VentaHistorica.objects.filter(receta=receta, fecha=fecha)
                if sucursal:
                    existing_qs = existing_qs.filter(sucursal=sucursal)
                else:
                    existing_qs = existing_qs.filter(sucursal__isnull=True)
                existing = existing_qs.order_by("id").first()

                if existing:
                    if modo == "accumulate":
                        existing.cantidad = Decimal(str(existing.cantidad or 0)) + cantidad
                        existing.tickets = int(existing.tickets or 0) + tickets
                        if monto_store is not None:
                            existing.monto_total = Decimal(str(existing.monto_total or 0)) + monto_store
                    else:
                        existing.cantidad = cantidad
                        existing.tickets = tickets
                        existing.monto_total = monto_store
                    existing.fuente = fuente
                    if not dry_run:
                        existing.save(update_fields=["cantidad", "tickets", "monto_total", "fuente", "actualizado_en"])
                    counters.updated += 1
                else:
                    if not dry_run:
                        VentaHistorica.objects.create(
                            receta=receta,
                            sucursal=sucursal,
                            fecha=fecha,
                            cantidad=cantidad,
                            tickets=tickets,
                            monto_total=monto_store,
                            fuente=fuente,
                        )
                    counters.created += 1

            if dry_run:
                transaction.set_rollback(True)

        return counters

    def _rows_look_like_sales(self, rows: list[dict[str, Any]]) -> bool:
        if not rows:
            return False
        seen_fecha = False
        seen_cantidad = False
        seen_producto_ref = False
        sample = rows[:50]
        for row in sample:
            keys = {str(k or "").strip().lower() for k in (row or {}).keys()}
            if "fecha" in keys:
                seen_fecha = True
            if "cantidad" in keys:
                seen_cantidad = True
            if "receta" in keys or "codigo_point" in keys:
                seen_producto_ref = True
            if seen_fecha and seen_cantidad and seen_producto_ref:
                return True
        return False
