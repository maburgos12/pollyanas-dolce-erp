from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from rapidfuzz import fuzz, process
from unidecode import unidecode

from maestros.models import Insumo, InsumoAlias, PointPendingMatch, Proveedor, UnidadMedida
from recetas.models import Receta
from recetas.utils.normalizacion import normalizar_nombre


def _norm_text(value: str | None) -> str:
    return normalizar_nombre(value or "")


def _norm_header(value: str | None) -> str:
    return _norm_text(unidecode((value or "").replace("_", " "))).upper()


def _safe(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _pick_col(columns: Iterable[str], *candidates: str) -> str | None:
    normalized = {col: _norm_header(col) for col in columns}
    wanted = [_norm_header(x) for x in candidates]

    for col, col_norm in normalized.items():
        if col_norm in wanted:
            return col
    for col, col_norm in normalized.items():
        for want in wanted:
            if want and want in col_norm:
                return col
    return None


def _header_index(filepath: Path, required: list[str], max_scan_rows: int = 30) -> int:
    df = pd.read_excel(filepath, header=None, dtype=str, engine="xlrd")
    required_norm = [_norm_header(h) for h in required]
    limit = min(max_scan_rows, len(df))
    for idx in range(limit):
        vals = [_norm_header(_safe(v)) for v in df.iloc[idx].tolist()]
        vals = [v for v in vals if v]
        if all(any(req in cell for cell in vals) for req in required_norm):
            return idx
    raise CommandError(f"No se encontró header con columnas requeridas en: {filepath.name}")


def _read_with_header(filepath: Path, required: list[str]) -> pd.DataFrame:
    idx = _header_index(filepath, required=required)
    df = pd.read_excel(filepath, header=idx, dtype=str, engine="xlrd").fillna("")
    df.columns = [_safe(c) for c in df.columns]
    return df


def _find_file(point_dir: Path, includes: list[str]) -> Path:
    files = sorted(point_dir.glob("*.xls"))
    if not files:
        raise CommandError(f"No hay archivos .xls en {point_dir}")
    for f in files:
        norm = _norm_text(f.name)
        if all(k in norm for k in includes):
            return f
    raise CommandError(f"No se encontró archivo con patrón {includes} en {point_dir}")


def _unit_from_point_text(text: str) -> UnidadMedida | None:
    norm = _norm_text(text)
    map_code = {
        "kilogramo": "kg",
        "kg": "kg",
        "gramo": "g",
        "g": "g",
        "litro": "lt",
        "l": "lt",
        "lt": "lt",
        "mililitro": "ml",
        "ml": "ml",
        "pieza": "pza",
        "pza": "pza",
        "pz": "pza",
        "unidad": "pza",
    }
    code = map_code.get(norm, "pza")
    return UnidadMedida.objects.filter(codigo=code).first()


@dataclass
class MatchRow:
    point_codigo: str
    point_nombre: str
    matched: int
    method: str
    erp_id: int | None
    erp_name: str
    fuzzy_score: float
    fuzzy_sugerencia: str


class Command(BaseCommand):
    help = (
        "Sincroniza catálogos Point (proveedores, insumos y productos) contra ERP. "
        "Genera propuestas CSV y opcionalmente aplica homologación controlada."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "point_dir",
            nargs="?",
            default="/Users/mauricioburgos/Downloads/INFORMACION POINT",
            help="Carpeta con exports .xls de Point",
        )
        parser.add_argument("--output-dir", default="logs", help="Carpeta destino de reportes CSV")
        parser.add_argument("--fuzzy-threshold", type=int, default=90, help="Umbral fuzzy (0-100)")
        parser.add_argument("--apply-proveedores", action="store_true", help="Crear proveedores faltantes")
        parser.add_argument("--apply-insumos", action="store_true", help="Aplicar homologación de códigos Point en insumos")
        parser.add_argument("--apply-productos", action="store_true", help="Aplicar homologación de códigos Point en recetas")
        parser.add_argument("--create-aliases", action="store_true", help="Crear alias de insumo en matches por nombre/fuzzy")
        parser.add_argument("--create-missing-insumos", action="store_true", help="Crear insumos nuevos si no tienen match")
        parser.add_argument("--dry-run", action="store_true", help="Simula cambios y hace rollback")

    @transaction.atomic
    def handle(self, *args, **options):
        point_dir = Path(options["point_dir"]).expanduser().resolve()
        out_dir = Path(options["output_dir"]).expanduser().resolve()
        threshold = int(options["fuzzy_threshold"])
        out_dir.mkdir(parents=True, exist_ok=True)

        if not point_dir.exists():
            raise CommandError(f"No existe la carpeta: {point_dir}")

        f_insumos = _find_file(point_dir, ["catalogo", "insumos"])
        f_productos = _find_file(point_dir, ["catalogo", "productos"])
        f_proveedores = _find_file(point_dir, ["proveedores"])

        self.stdout.write(self.style.SUCCESS("Archivos Point detectados:"))
        self.stdout.write(f"  - Insumos: {f_insumos.name}")
        self.stdout.write(f"  - Productos: {f_productos.name}")
        self.stdout.write(f"  - Proveedores: {f_proveedores.name}")

        point_insumos = self._load_point_insumos(f_insumos)
        point_productos = self._load_point_productos(f_productos)
        point_proveedores = self._load_point_proveedores(f_proveedores)

        providers_report, providers_stats = self._sync_proveedores(
            point_proveedores,
            threshold=threshold,
            apply=bool(options["apply_proveedores"]),
        )
        insumos_report, insumos_stats = self._sync_insumos(
            point_insumos,
            threshold=threshold,
            apply=bool(options["apply_insumos"]),
            create_aliases=bool(options["create_aliases"]),
            create_missing_insumos=bool(options["create_missing_insumos"]),
        )
        productos_report, productos_stats = self._sync_productos(
            point_productos,
            threshold=threshold,
            apply=bool(options["apply_productos"]),
        )

        p_prov = out_dir / "point_vs_erp_proveedores_missing.csv"
        p_ins = out_dir / "point_vs_erp_insumos_missing.csv"
        p_prod = out_dir / "point_vs_erp_productos_vs_recetas_missing.csv"
        self._write_csv(
            p_prov,
            providers_report,
            ["point_proveedor", "rfc", "correo", "matched", "erp_proveedor", "fuzzy_score", "fuzzy_sugerencia", "method"],
        )
        self._write_csv(
            p_ins,
            insumos_report,
            [
                "point_codigo",
                "point_nombre",
                "unidad",
                "point_categoria",
                "matched",
                "method",
                "erp_insumo",
                "erp_codigo_point",
                "fuzzy_score",
                "fuzzy_sugerencia",
            ],
        )
        self._write_csv(
            p_prod,
            productos_report,
            [
                "point_codigo_producto",
                "point_nombre_producto",
                "precio_default",
                "matched_receta",
                "erp_receta",
                "fuzzy_score",
                "fuzzy_sugerencia",
                "method",
            ],
        )

        if not options["dry_run"]:
            self._replace_point_pending(
                PointPendingMatch.TIPO_PROVEEDOR,
                [
                    {
                        "point_codigo": "",
                        "point_nombre": row["point_proveedor"],
                        "payload": {"rfc": row.get("rfc", ""), "correo": row.get("correo", "")},
                        "method": row.get("method", ""),
                        "fuzzy_score": row.get("fuzzy_score", 0),
                        "fuzzy_sugerencia": row.get("fuzzy_sugerencia", ""),
                    }
                    for row in providers_report
                    if int(row.get("matched") or 0) == 0
                ],
            )
            self._replace_point_pending(
                PointPendingMatch.TIPO_INSUMO,
                [
                    {
                        "point_codigo": row.get("point_codigo", ""),
                        "point_nombre": row.get("point_nombre", ""),
                        "payload": {
                            "unidad": row.get("unidad", ""),
                            "categoria": row.get("point_categoria", ""),
                        },
                        "method": row.get("method", ""),
                        "fuzzy_score": row.get("fuzzy_score", 0),
                        "fuzzy_sugerencia": row.get("fuzzy_sugerencia", ""),
                    }
                    for row in insumos_report
                    if int(row.get("matched") or 0) == 0
                ],
            )
            self._replace_point_pending(
                PointPendingMatch.TIPO_PRODUCTO,
                [
                    {
                        "point_codigo": row.get("point_codigo_producto", ""),
                        "point_nombre": row.get("point_nombre_producto", ""),
                        "payload": {"precio_default": row.get("precio_default", "")},
                        "method": row.get("method", ""),
                        "fuzzy_score": row.get("fuzzy_score", 0),
                        "fuzzy_sugerencia": row.get("fuzzy_sugerencia", ""),
                    }
                    for row in productos_report
                    if int(row.get("matched_receta") or 0) == 0
                ],
            )

        if options["dry_run"]:
            transaction.set_rollback(True)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Resumen sync Point vs ERP"))
        self.stdout.write(
            f"  - proveedores: point={providers_stats['point_total']} "
            f"match={providers_stats['matched']} no_match={providers_stats['unmatched']} "
            f"creados={providers_stats['created']}"
        )
        self.stdout.write(
            f"  - insumos: point={insumos_stats['point_total']} "
            f"match={insumos_stats['matched']} no_match={insumos_stats['unmatched']} "
            f"actualizados={insumos_stats['updated_codes']} creados={insumos_stats['created']} aliases={insumos_stats['aliases_created']}"
        )
        self.stdout.write(
            f"  - productos/recetas: point={productos_stats['point_total']} "
            f"match={productos_stats['matched']} no_match={productos_stats['unmatched']} "
            f"recetas_actualizadas={productos_stats['updated_codes']}"
        )
        self.stdout.write(f"  - reportes: {p_prov}, {p_ins}, {p_prod}")
        self.stdout.write(f"  - modo: {'DRY-RUN (rollback)' if options['dry_run'] else 'APLICADO'}")

    def _replace_point_pending(self, tipo: str, entries: list[dict]) -> None:
        PointPendingMatch.objects.filter(tipo=tipo).delete()
        if not entries:
            return

        objs = []
        for entry in entries:
            score_raw = entry.get("fuzzy_score", 0)
            try:
                score = float(score_raw)
            except (TypeError, ValueError):
                score = 0.0
            objs.append(
                PointPendingMatch(
                    tipo=tipo,
                    point_codigo=(entry.get("point_codigo") or "")[:80],
                    point_nombre=(entry.get("point_nombre") or "")[:250],
                    payload=entry.get("payload") or {},
                    method=(entry.get("method") or "")[:32],
                    fuzzy_score=score,
                    fuzzy_sugerencia=(entry.get("fuzzy_sugerencia") or "")[:250],
                )
            )
        PointPendingMatch.objects.bulk_create(objs, batch_size=500)

    def _load_point_insumos(self, filepath: Path) -> list[dict]:
        df = _read_with_header(filepath, required=["CÓDIGO", "NOMBRE"])
        c_codigo = _pick_col(df.columns, "CÓDIGO")
        c_nombre = _pick_col(df.columns, "NOMBRE")
        c_unidad = _pick_col(df.columns, "UNIDAD")
        c_categoria = _pick_col(df.columns, "CATEGORÍA", "CATEGORIA")
        if not c_nombre:
            raise CommandError(f"No se encontró columna NOMBRE en {filepath.name}")
        rows = []
        for _, row in df.iterrows():
            nombre = _safe(row.get(c_nombre))
            if not nombre:
                continue
            rows.append(
                {
                    "codigo": _safe(row.get(c_codigo)),
                    "nombre": nombre,
                    "unidad": _safe(row.get(c_unidad)),
                    "categoria": _safe(row.get(c_categoria)),
                }
            )
        return rows

    def _load_point_productos(self, filepath: Path) -> list[dict]:
        df = _read_with_header(filepath, required=["CÓDIGO", "NOMBRE"])
        c_codigo = _pick_col(df.columns, "CÓDIGO")
        c_nombre = _pick_col(df.columns, "NOMBRE")
        c_precio = _pick_col(df.columns, "PRECIO DEFAULT", "PRECIO")
        if not c_nombre:
            raise CommandError(f"No se encontró columna NOMBRE en {filepath.name}")
        rows = []
        for _, row in df.iterrows():
            nombre = _safe(row.get(c_nombre))
            if not nombre:
                continue
            rows.append(
                {
                    "codigo": _safe(row.get(c_codigo)),
                    "nombre": nombre,
                    "precio_default": _safe(row.get(c_precio)),
                }
            )
        return rows

    def _load_point_proveedores(self, filepath: Path) -> list[dict]:
        df = _read_with_header(filepath, required=["PROVEEDOR", "RFC"])
        c_nombre = _pick_col(df.columns, "PROVEEDOR")
        c_rfc = _pick_col(df.columns, "RFC")
        c_correo = _pick_col(df.columns, "CORREO")
        if not c_nombre:
            raise CommandError(f"No se encontró columna PROVEEDOR en {filepath.name}")
        rows = []
        for _, row in df.iterrows():
            nombre = _safe(row.get(c_nombre))
            if not nombre:
                continue
            rows.append(
                {
                    "nombre": nombre,
                    "rfc": _safe(row.get(c_rfc)),
                    "correo": _safe(row.get(c_correo)),
                }
            )
        return rows

    def _sync_proveedores(self, rows: list[dict], threshold: int, apply: bool) -> tuple[list[dict], dict]:
        erp_proveedores = list(Proveedor.objects.all())
        by_norm: dict[str, Proveedor] = {}
        for p in erp_proveedores:
            key = _norm_text(p.nombre)
            if key and key not in by_norm:
                by_norm[key] = p
        choices = list(by_norm.keys())

        report: list[dict] = []
        created = 0
        matched = 0
        unmatched = 0

        for row in rows:
            point_name = row["nombre"]
            point_norm = _norm_text(point_name)
            direct = by_norm.get(point_norm)

            method = "NO_MATCH"
            target: Proveedor | None = None
            score = 0.0
            sugerencia = ""

            if direct:
                target = direct
                method = "EXACT"
                score = 100.0
            elif choices:
                best = process.extractOne(point_norm, choices, scorer=fuzz.WRatio)
                if best:
                    best_norm, best_score, _ = best
                    sugerencia = by_norm[best_norm].nombre
                    score = float(best_score)
                    if score >= threshold:
                        target = by_norm[best_norm]
                        method = "FUZZY"

            if target:
                matched += 1
            else:
                unmatched += 1
                if apply and point_name:
                    exists = Proveedor.objects.filter(nombre=point_name).exists()
                    if not exists:
                        Proveedor.objects.create(nombre=point_name[:200], activo=True)
                        created += 1

            report.append(
                {
                    "point_proveedor": point_name,
                    "rfc": row["rfc"],
                    "correo": row["correo"],
                    "matched": 1 if target else 0,
                    "erp_proveedor": target.nombre if target else "",
                    "fuzzy_score": f"{score:.1f}",
                    "fuzzy_sugerencia": sugerencia,
                    "method": method,
                }
            )

        return report, {
            "point_total": len(rows),
            "matched": matched,
            "unmatched": unmatched,
            "created": created,
        }

    def _sync_insumos(
        self,
        rows: list[dict],
        threshold: int,
        apply: bool,
        create_aliases: bool,
        create_missing_insumos: bool,
    ) -> tuple[list[dict], dict]:
        insumos = list(Insumo.objects.select_related("unidad_base").all())
        by_norm: dict[str, Insumo] = {}
        by_code: dict[str, Insumo] = {}
        for i in insumos:
            norm = _norm_text(i.nombre)
            if norm and norm not in by_norm:
                by_norm[norm] = i
            if i.codigo_point:
                code_key = _norm_text(i.codigo_point)
                if code_key and code_key not in by_code:
                    by_code[code_key] = i
        choices = list(by_norm.keys())

        report: list[dict] = []
        assignments: dict[int, set[str]] = {}
        created = 0
        updated_codes = 0
        aliases_created = 0
        matched = 0
        unmatched = 0

        matched_rows: list[tuple[MatchRow, str, str, str]] = []

        for row in rows:
            point_code = _safe(row["codigo"])
            point_name = row["nombre"]
            point_norm = _norm_text(point_name)
            point_code_key = _norm_text(point_code)

            target: Insumo | None = None
            method = "NO_MATCH"
            score = 0.0
            sugerencia = ""

            if point_code_key and point_code_key in by_code:
                target = by_code[point_code_key]
                method = "EXACT_CODE"
                score = 100.0
            elif point_norm in by_norm:
                target = by_norm[point_norm]
                method = "EXACT_NAME"
                score = 100.0
            elif choices:
                best = process.extractOne(point_norm, choices, scorer=fuzz.WRatio)
                if best:
                    best_norm, best_score, _ = best
                    score = float(best_score)
                    sugerencia = by_norm[best_norm].nombre
                    if score >= threshold:
                        target = by_norm[best_norm]
                        method = "FUZZY"

            if target:
                matched += 1
                if target.id not in assignments:
                    assignments[target.id] = set()
                if point_code:
                    assignments[target.id].add(point_code)
                matched_rows.append(
                    (
                        MatchRow(
                            point_codigo=point_code,
                            point_nombre=point_name,
                            matched=1,
                            method=method,
                            erp_id=target.id,
                            erp_name=target.nombre,
                            fuzzy_score=score,
                            fuzzy_sugerencia=sugerencia,
                        ),
                        row["unidad"],
                        row["categoria"],
                        point_norm,
                    )
                )
            else:
                unmatched += 1
                if apply and create_missing_insumos and point_name:
                    unit = _unit_from_point_text(row["unidad"])
                    new_insumo = Insumo.objects.create(
                        nombre=point_name[:250],
                        codigo_point=point_code[:80],
                        nombre_point=point_name[:250],
                        unidad_base=unit,
                        activo=True,
                    )
                    created += 1
                    by_norm[_norm_text(new_insumo.nombre)] = new_insumo
                    if new_insumo.codigo_point:
                        by_code[_norm_text(new_insumo.codigo_point)] = new_insumo

            report.append(
                {
                    "point_codigo": point_code,
                    "point_nombre": point_name,
                    "unidad": row["unidad"],
                    "point_categoria": row["categoria"],
                    "matched": 1 if target else 0,
                    "method": method,
                    "erp_insumo": target.nombre if target else "",
                    "erp_codigo_point": target.codigo_point if target else "",
                    "fuzzy_score": f"{score:.1f}",
                    "fuzzy_sugerencia": sugerencia,
                }
            )

        if apply:
            for match, unidad_txt, categoria_txt, point_norm in matched_rows:
                if not match.erp_id:
                    continue
                target = Insumo.objects.filter(pk=match.erp_id).first()
                if not target:
                    continue

                # Evita sobrescribir cuando un mismo insumo recibió varios códigos Point distintos.
                conflicting_codes = {c for c in assignments.get(target.id, set()) if c}
                if len(conflicting_codes) > 1:
                    continue

                changed_fields: list[str] = []
                if match.point_codigo and target.codigo_point != match.point_codigo[:80]:
                    target.codigo_point = match.point_codigo[:80]
                    changed_fields.append("codigo_point")
                if target.nombre_point != match.point_nombre[:250]:
                    target.nombre_point = match.point_nombre[:250]
                    changed_fields.append("nombre_point")
                if not target.unidad_base_id:
                    unit = _unit_from_point_text(unidad_txt)
                    if unit:
                        target.unidad_base = unit
                        changed_fields.append("unidad_base")
                if changed_fields:
                    target.save(update_fields=changed_fields)
                    updated_codes += 1

                if create_aliases and point_norm and point_norm != target.nombre_normalizado:
                    alias, was_created = InsumoAlias.objects.get_or_create(
                        nombre_normalizado=point_norm,
                        defaults={"nombre": match.point_nombre[:250], "insumo": target},
                    )
                    if not was_created and alias.insumo_id != target.id:
                        alias.insumo = target
                        alias.save(update_fields=["insumo"])
                    if was_created:
                        aliases_created += 1

        return report, {
            "point_total": len(rows),
            "matched": matched,
            "unmatched": unmatched,
            "updated_codes": updated_codes,
            "created": created,
            "aliases_created": aliases_created,
        }

    def _sync_productos(self, rows: list[dict], threshold: int, apply: bool) -> tuple[list[dict], dict]:
        recetas = list(Receta.objects.all())
        by_norm: dict[str, Receta] = {}
        by_code: dict[str, Receta] = {}
        for r in recetas:
            norm = _norm_text(r.nombre)
            if norm and norm not in by_norm:
                by_norm[norm] = r
            if r.codigo_point:
                code_key = _norm_text(r.codigo_point)
                if code_key and code_key not in by_code:
                    by_code[code_key] = r
        choices = list(by_norm.keys())

        report: list[dict] = []
        updated_codes = 0
        matched = 0
        unmatched = 0

        for row in rows:
            point_code = _safe(row["codigo"])
            point_name = row["nombre"]
            point_norm = _norm_text(point_name)
            point_code_key = _norm_text(point_code)

            target: Receta | None = None
            method = "NO_MATCH"
            score = 0.0
            sugerencia = ""

            if point_code_key and point_code_key in by_code:
                target = by_code[point_code_key]
                method = "EXACT_CODE"
                score = 100.0
            elif point_norm in by_norm:
                target = by_norm[point_norm]
                method = "EXACT_NAME"
                score = 100.0
            elif choices:
                best = process.extractOne(point_norm, choices, scorer=fuzz.WRatio)
                if best:
                    best_norm, best_score, _ = best
                    score = float(best_score)
                    sugerencia = by_norm[best_norm].nombre
                    if score >= threshold:
                        target = by_norm[best_norm]
                        method = "FUZZY"

            if target:
                matched += 1
                if apply and point_code and target.codigo_point != point_code[:80]:
                    target.codigo_point = point_code[:80]
                    target.save(update_fields=["codigo_point"])
                    updated_codes += 1
            else:
                unmatched += 1

            report.append(
                {
                    "point_codigo_producto": point_code,
                    "point_nombre_producto": point_name,
                    "precio_default": row["precio_default"],
                    "matched_receta": 1 if target else 0,
                    "erp_receta": target.nombre if target else "",
                    "fuzzy_score": f"{score:.1f}",
                    "fuzzy_sugerencia": sugerencia,
                    "method": method,
                }
            )

        return report, {
            "point_total": len(rows),
            "matched": matched,
            "unmatched": unmatched,
            "updated_codes": updated_codes,
        }

    def _write_csv(self, filepath: Path, rows: list[dict], headers: list[str]) -> None:
        with filepath.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow({h: row.get(h, "") for h in headers})
