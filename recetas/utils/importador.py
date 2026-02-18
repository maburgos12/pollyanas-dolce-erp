import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import openpyxl
from django.db import transaction
from django.utils import timezone

from maestros.models import Insumo, CostoInsumo, Proveedor, UnidadMedida, seed_unidades_basicas
from .matching import match_insumo, clasificar_match
from .normalizacion import normalizar_nombre
from recetas.models import Receta, LineaReceta

log = logging.getLogger(__name__)

@dataclass
class ImportResultado:
    catalogo_importado: int = 0
    insumos_creados: int = 0
    costos_creados: int = 0
    recetas_creadas: int = 0
    recetas_actualizadas: int = 0
    lineas_creadas: int = 0
    errores: List[Dict[str, Any]] = field(default_factory=list)
    matches_pendientes: List[Dict[str, Any]] = field(default_factory=list)

def _sha256(texto: str) -> str:
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()

def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def _to_date(x) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    try:
        return datetime.fromisoformat(str(x)).date()
    except Exception:
        return None

def _normalize_header(val: Any) -> str:
    return normalizar_nombre(val)

def _find_header_row(values: List[List[Any]], required_tokens: List[str], max_scan: int = 30) -> Optional[int]:
    required_tokens = [normalizar_nombre(t) for t in required_tokens]
    for i, row in enumerate(values[:max_scan]):
        row_tokens = " | ".join([_normalize_header(v) for v in row if v is not None])
        if all(t in row_tokens for t in required_tokens):
            return i
    return None

def _map_columns(header_row: List[Any]) -> Dict[str, int]:
    # maps known fields to column index
    mapping = {}
    for idx, cell in enumerate(header_row):
        h = _normalize_header(cell)
        if not h:
            continue
        if h in ("proveedor",):
            mapping["proveedor"] = idx
        elif "producto" in h:
            mapping["producto"] = idx
        elif "descripcion" in h:
            mapping["descripcion"] = idx
        elif "cantidad" == h:
            mapping["cantidad"] = idx
        elif "unidad" == h:
            mapping["unidad"] = idx
        elif h in ("costo", "costounitario", "costo unitario"):
            mapping["costo"] = idx
        elif "$/unidad" in str(cell).lower() or "unidad" == h and "costo" not in mapping:
            # no-op; handled by unidad
            pass
        elif "fecha" in h:
            mapping["fecha"] = idx
        elif "codigo" in h and "barras" in h:
            mapping["codigo_barras"] = idx
        elif "precio" in h:
            mapping["precio"] = idx
    return mapping

def _get_unit(code: str) -> Optional[UnidadMedida]:
    if not code:
        return None
    code_norm = normalizar_nombre(code)
    # unify
    repl = {
        "pz": "pza",
        "pieza": "pza",
        "pzas": "pza",
        "kg": "kg",
        "g": "g",
        "gr": "g",
        "gramo": "g",
        "lt": "lt",
        "l": "lt",
        "litro": "lt",
        "ml": "ml",
    }
    code2 = repl.get(code_norm, code_norm)
    return UnidadMedida.objects.filter(codigo=code2).first()

def _latest_cost_unitario(insumo: Insumo) -> Optional[float]:
    ci = CostoInsumo.objects.filter(insumo=insumo).order_by("-fecha", "-id").first()
    return float(ci.costo_unitario) if ci else None


def _is_presentation_header(text: Any) -> bool:
    n = normalizar_nombre(text)
    return n in {
        "mini",
        "chico",
        "mediano",
        "grande",
        "individual",
        "rebanada",
        "bollos",
        "bollito",
        "media plancha",
        "1/2 plancha",
        "1 2 plancha",
    }


class ImportadorCosteo:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.wb = openpyxl.load_workbook(filepath, data_only=True)
        self.resultado = ImportResultado()

    @transaction.atomic
    def procesar_completo(self) -> ImportResultado:
        seed_unidades_basicas()
        self.importar_catalogo_costos()
        self.importar_recetas()
        return self.resultado

    def importar_catalogo_costos(self):
        if "Costo Materia Prima" not in self.wb.sheetnames:
            self.resultado.errores.append({"sheet": "Costo Materia Prima", "error": "No existe la hoja"})
            return

        ws = self.wb["Costo Materia Prima"]
        values = list(ws.values)
        header_i = _find_header_row(values, ["Proveedor", "Producto", "Costo"])
        if header_i is None:
            header_i = 1  # fallback
        header = list(values[header_i])
        colmap = _map_columns(header)

        # data rows
        for row in values[header_i + 1 :]:
            try:
                producto = row[colmap.get("producto", 3)] if len(row) > colmap.get("producto", 3) else None
                desc = row[colmap.get("descripcion", 4)] if len(row) > colmap.get("descripcion", 4) else None
                nombre = (producto or desc)
                if not nombre:
                    continue

                proveedor_nombre = None
                if "proveedor" in colmap and len(row) > colmap["proveedor"]:
                    proveedor_nombre = row[colmap["proveedor"]]
                proveedor = None
                if proveedor_nombre:
                    proveedor, _ = Proveedor.objects.get_or_create(nombre=str(proveedor_nombre).strip()[:200])

                unidad_code = None
                if "unidad" in colmap and len(row) > colmap["unidad"]:
                    unidad_code = row[colmap["unidad"]]
                # en este excel, la unidad real puede estar duplicada; preferimos Unidad
                unidad = _get_unit(str(unidad_code)) if unidad_code else None

                costo = None
                if "costo" in colmap and len(row) > colmap["costo"]:
                    costo = _to_float(row[colmap["costo"]])

                cantidad = None
                if "cantidad" in colmap and len(row) > colmap["cantidad"]:
                    cantidad = _to_float(row[colmap["cantidad"]])

                precio = None
                if "precio" in colmap and len(row) > colmap["precio"]:
                    precio = _to_float(row[colmap["precio"]])

                fecha = None
                if "fecha" in colmap and len(row) > colmap["fecha"]:
                    fecha = _to_date(row[colmap["fecha"]])

                # Si costo no existe pero precio y cantidad sí, intentar calcular
                costo_unitario = costo
                if costo_unitario is None and precio is not None and cantidad and cantidad > 0:
                    costo_unitario = precio / cantidad

                if costo_unitario is None:
                    continue

                insumo, created = Insumo.objects.get_or_create(
                    nombre_normalizado=normalizar_nombre(nombre),
                    defaults={
                        "nombre": str(nombre).strip()[:250],
                        "unidad_base": unidad,
                        "proveedor_principal": proveedor,
                    },
                )
                if created:
                    self.resultado.insumos_creados += 1
                else:
                    # actualizar unidad/proveedor si no tiene
                    changed = False
                    if insumo.unidad_base is None and unidad is not None:
                        insumo.unidad_base = unidad
                        changed = True
                    if insumo.proveedor_principal is None and proveedor is not None:
                        insumo.proveedor_principal = proveedor
                        changed = True
                    if changed:
                        insumo.save()

                raw = {
                    "producto": str(producto) if producto is not None else "",
                    "descripcion": str(desc) if desc is not None else "",
                    "precio": precio,
                    "cantidad": cantidad,
                    "unidad": str(unidad_code) if unidad_code is not None else "",
                    "fecha": str(fecha) if fecha else "",
                    "proveedor": str(proveedor_nombre) if proveedor_nombre else "",
                }
                source_hash = _sha256(json_dumps_sorted({"n": insumo.nombre_normalizado, "c": costo_unitario, "f": str(fecha), "p": raw.get("proveedor","")}))
                if not CostoInsumo.objects.filter(source_hash=source_hash).exists():
                    CostoInsumo.objects.create(
                        insumo=insumo,
                        proveedor=proveedor,
                        fecha=fecha or timezone.now().date(),
                        costo_unitario=costo_unitario,
                        source_hash=source_hash,
                        raw=raw,
                    )
                    self.resultado.costos_creados += 1
            except Exception as e:
                self.resultado.errores.append({"sheet": "Costo Materia Prima", "error": str(e)})

        self.resultado.catalogo_importado = 1

    def importar_recetas(self):
        # detecta hojas con recetas tradicionales y/o matrices por presentación.
        for sheet_name in self.wb.sheetnames:
            ws = self.wb[sheet_name]
            has_ingredientes = self._sheet_has_ingredientes(ws)
            has_matrix = self._sheet_has_presentacion_matrix(ws)
            if not has_ingredientes and not has_matrix:
                continue
            try:
                if has_ingredientes:
                    self._importar_recetas_de_hoja(ws, sheet_name)
                if has_matrix:
                    self._importar_productos_finales_de_hoja(ws, sheet_name)
            except Exception as e:
                self.resultado.errores.append({"sheet": sheet_name, "error": str(e)})

    def _sheet_has_ingredientes(self, ws) -> bool:
        for r in range(1, 80):
            for c in range(1, 8):
                v = ws.cell(row=r, column=c).value
                if isinstance(v, str) and "ingrediente" in v.lower():
                    return True
        return False

    def _sheet_has_presentacion_matrix(self, ws) -> bool:
        if "pastel" not in normalizar_nombre(ws.title):
            return False
        max_row = min(ws.max_row, 120)
        max_col = min(ws.max_column, 24)
        for r in range(1, max_row + 1):
            for c in range(1, max_col + 1):
                if normalizar_nombre(ws.cell(row=r, column=c).value) != "elemento":
                    continue
                for cc in range(c + 1, min(c + 8, max_col + 1)):
                    if _is_presentation_header(ws.cell(row=r, column=cc).value):
                        return True
        return False

    def _importar_recetas_de_hoja(self, ws, sheet_name: str):
        max_row = min(ws.max_row, 400)
        r = 1
        while r <= max_row - 2:
            a = ws.cell(row=r, column=1).value
            b = ws.cell(row=r+1, column=1).value
            if isinstance(a, str) and a.strip() and isinstance(b, str) and "ingrediente" in b.lower():
                receta_nombre = a.strip()
                header_row = r + 1
                # detect columns based on header row
                header_vals = [ws.cell(row=header_row, column=c).value for c in range(1, 10)]
                col_ing = 1
                col_qty = 2
                col_unit = 3
                col_cost = 4
                for idx, hv in enumerate(header_vals, start=1):
                    hvn = normalizar_nombre(hv)
                    if hvn in ("ingredientes", "insumo", "ingrediente"):
                        col_ing = idx
                    elif hvn.startswith("cantidad"):
                        col_qty = idx
                    elif hvn.startswith("unidad"):
                        col_unit = idx
                    elif "costo" in hvn or hvn == "$":
                        col_cost = idx

                lineas = []
                rr = header_row + 1
                pos = 1
                while rr <= max_row:
                    ing = ws.cell(row=rr, column=col_ing).value
                    qty = ws.cell(row=rr, column=col_qty).value if col_qty else None
                    unit = ws.cell(row=rr, column=col_unit).value if col_unit else None
                    cost = ws.cell(row=rr, column=col_cost).value if col_cost else None

                    # stop conditions
                    if isinstance(ing, str) and normalizar_nombre(ing) in ("total", "costo total"):
                        break
                    if isinstance(qty, str) and "costo total" in qty.lower():
                        break
                    if ing is None and (qty is None or (isinstance(qty, str) and qty.strip()=="")):
                        # blank row => end block
                        break
                    if isinstance(ing, str) and "costo total" in ing.lower():
                        break
                    if isinstance(qty, str) and normalizar_nombre(qty) == "total":
                        break

                    if ing is not None and str(ing).strip():
                        lineas.append({
                            "pos": pos,
                            "tipo_linea": LineaReceta.TIPO_NORMAL,
                            "etapa": "",
                            "ingrediente": str(ing).strip(),
                            "cantidad": _to_float(qty),
                            "unidad": str(unit).strip() if unit is not None else "",
                            "costo": _to_float(cost),
                        })
                        pos += 1
                    rr += 1

                if lineas:
                    self._upsert_receta(
                        receta_nombre,
                        sheet_name,
                        lineas,
                        recipe_type=Receta.TIPO_PREPARACION,
                    )
                    r = rr + 1
                    continue

            r += 1

    def _importar_productos_finales_de_hoja(self, ws, sheet_name: str):
        if "pastel" not in normalizar_nombre(sheet_name):
            return
        max_row = min(ws.max_row, 250)
        max_col = min(ws.max_column, 30)
        recetas_por_presentacion: Dict[str, List[Dict[str, Any]]] = {}
        size_cols: Dict[str, Tuple[int, str]] = {}

        # Bloque principal de costos por componente (columna A + tamaños en la fila de encabezado).
        for r in range(1, max_row + 1):
            head = normalizar_nombre(ws.cell(row=r, column=1).value)
            if head not in {"insumo", "ingrediente", "insumos"}:
                continue
            for c in range(2, max_col + 1):
                hv = ws.cell(row=r, column=c).value
                if _is_presentation_header(hv):
                    size_cols[normalizar_nombre(hv)] = (c, str(hv).strip())

            if size_cols:
                rr = r + 1
                while rr <= max_row:
                    componente = ws.cell(row=rr, column=1).value
                    if componente is None or str(componente).strip() == "":
                        rr += 1
                        continue
                    componente_txt = str(componente).strip()
                    componente_norm = normalizar_nombre(componente_txt)
                    if componente_norm.startswith("subtotal") or componente_norm.startswith("costo"):
                        break

                    for _, (col, presentacion) in size_cols.items():
                        costo = _to_float(ws.cell(row=rr, column=col).value)
                        if costo is None:
                            continue
                        receta_nombre = f"{sheet_name} - {presentacion}"[:250]
                        lineas = recetas_por_presentacion.setdefault(receta_nombre, [])
                        lineas.append(
                            {
                                "pos": len(lineas) + 1,
                                "tipo_linea": LineaReceta.TIPO_NORMAL,
                                "etapa": "",
                                "ingrediente": componente_txt[:250],
                                "cantidad": None,
                                "unidad": "",
                                "costo": costo,
                            }
                        )
                    rr += 1
            break

        for r in range(1, max_row + 1):
            for c in range(1, max_col + 1):
                if normalizar_nombre(ws.cell(row=r, column=c).value) != "elemento":
                    continue

                headers: List[Tuple[int, str]] = []
                cc = c + 1
                while cc <= max_col:
                    hv = ws.cell(row=r, column=cc).value
                    if hv is None or str(hv).strip() == "":
                        if headers:
                            break
                        cc += 1
                        continue
                    if isinstance(hv, (int, float)):
                        break
                    if _is_presentation_header(hv):
                        headers.append((cc, str(hv).strip()))
                    cc += 1

                if not headers:
                    continue

                section = ""
                if r > 1:
                    prev = ws.cell(row=r - 1, column=c).value
                    if isinstance(prev, str) and prev.strip():
                        section = prev.strip()[:120]

                rr = r + 1
                while rr <= max_row:
                    elemento = ws.cell(row=rr, column=c).value
                    if elemento is None or str(elemento).strip() == "":
                        break
                    elemento_txt = str(elemento).strip()
                    elemento_norm = normalizar_nombre(elemento_txt)
                    if elemento_norm == "elemento" or elemento_norm.startswith("total"):
                        break

                    for col, presentacion in headers:
                        cantidad = _to_float(ws.cell(row=rr, column=col).value)
                        if cantidad is None or cantidad <= 0:
                            continue

                        receta_nombre = f"{sheet_name} - {presentacion}"[:250]
                        lineas = recetas_por_presentacion.setdefault(receta_nombre, [])
                        lineas.append(
                            {
                                "pos": len(lineas) + 1,
                                "tipo_linea": LineaReceta.TIPO_SUBSECCION,
                                "etapa": section,
                                "ingrediente": elemento_txt[:250],
                                "cantidad": cantidad,
                                "unidad": "kg",
                                "costo": None,
                            }
                        )
                    rr += 1

        for receta_nombre, lineas in recetas_por_presentacion.items():
            if not lineas:
                continue
            self._upsert_receta(
                receta_nombre,
                sheet_name,
                lineas,
                recipe_type=Receta.TIPO_PRODUCTO_FINAL,
            )

    def _upsert_receta(
        self,
        nombre: str,
        sheet_name: str,
        lineas: List[Dict[str, Any]],
        recipe_type: str = Receta.TIPO_PREPARACION,
    ):
        # Hash idempotente por contenido
        nombre_norm = normalizar_nombre(nombre)
        h_payload = {
            "nombre": nombre_norm,
            "sheet": sheet_name,
            "lineas": [
                (
                    normalizar_nombre(l["ingrediente"]),
                    l.get("cantidad"),
                    normalizar_nombre(l.get("unidad", "")),
                    l.get("tipo_linea", LineaReceta.TIPO_NORMAL),
                    normalizar_nombre(l.get("etapa", "")),
                )
                for l in lineas
            ],
        }
        hash_contenido = _sha256(json_dumps_sorted(h_payload))

        # Consolidar por nombre normalizado para evitar duplicados entre corridas.
        receta = Receta.objects.filter(nombre_normalizado=nombre_norm).order_by("-id").first()
        if not receta:
            receta = Receta.objects.filter(hash_contenido=hash_contenido).first()
        if receta:
            self.resultado.recetas_actualizadas += 1
            # asegurar nombre/sheet/hash y recrear lineas
            if (
                receta.nombre != nombre
                or receta.sheet_name != sheet_name
                or receta.hash_contenido != hash_contenido
                or receta.tipo != recipe_type
            ):
                receta.nombre = nombre
                receta.sheet_name = sheet_name
                receta.hash_contenido = hash_contenido
                receta.tipo = recipe_type
                receta.save()
            receta.lineas.all().delete()
        else:
            receta = Receta.objects.create(
                nombre=nombre,
                sheet_name=sheet_name,
                hash_contenido=hash_contenido,
                tipo=recipe_type,
            )
            self.resultado.recetas_creadas += 1

        for l in lineas:
            insumo, score, method = match_insumo(l["ingrediente"])
            status = clasificar_match(score)
            unidad = _get_unit(l.get("unidad",""))
            costo_unitario = _latest_cost_unitario(insumo) if insumo else None
            tipo_linea = l.get("tipo_linea", LineaReceta.TIPO_NORMAL)
            etapa = str(l.get("etapa") or "")[:120]
            if tipo_linea == LineaReceta.TIPO_SUBSECCION and insumo is None:
                status = LineaReceta.STATUS_AUTO
                method = LineaReceta.MATCH_SUBSECTION
                score = 100.0
            lr = LineaReceta.objects.create(
                receta=receta,
                posicion=l["pos"],
                tipo_linea=tipo_linea,
                etapa=etapa,
                insumo=insumo if status != "REJECTED" else None,
                insumo_texto=l["ingrediente"],
                cantidad=l.get("cantidad"),
                unidad_texto=l.get("unidad",""),
                unidad=unidad,
                costo_linea_excel=l.get("costo"),
                costo_unitario_snapshot=costo_unitario,
                match_score=score,
                match_method=method,
                match_status=status,
            )
            self.resultado.lineas_creadas += 1
            if status == "NEEDS_REVIEW":
                self.resultado.matches_pendientes.append({
                    "receta": receta.nombre,
                    "ingrediente": l["ingrediente"],
                    "score": round(score, 2),
                    "method": method,
                    "status": status,
                })

def json_dumps_sorted(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
