from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

from django.core.management.base import BaseCommand

from maestros.models import UnidadMedida, seed_unidades_basicas
from recetas.models import Receta, RecetaPresentacion
from recetas.utils.importador import ImportadorCosteo, _to_float
from recetas.utils.normalizacion import normalizar_nombre


class Command(BaseCommand):
    help = "Sincroniza metadatos de recetas base desde COSTEO.xlsx (F/G rendimiento y bloque I:Q presentaciones)."

    def add_arguments(self, parser):
        parser.add_argument("filepath", type=str, help="Ruta al archivo COSTEO.xlsx")
        parser.add_argument(
            "--receta",
            dest="receta",
            default="",
            help="Filtra por nombre de receta (contains, opcional).",
        )
        parser.add_argument(
            "--all-sheets",
            action="store_true",
            help="Procesa todas las hojas con bloques de ingredientes (no solo Insumos 1/2).",
        )

    def handle(self, *args, **options):
        filepath = str(options["filepath"])
        if not Path(filepath).exists():
            self.stdout.write(self.style.ERROR(f"Archivo no encontrado: {filepath}"))
            return

        seed_unidades_basicas()
        importador = ImportadorCosteo(filepath)
        receta_filter = (options.get("receta") or "").strip().lower()
        if options.get("all_sheets"):
            sheets = list(importador.wb.sheetnames)
        else:
            sheets = [
                s for s in importador.wb.sheetnames if normalizar_nombre(s) in {"insumos 1", "insumos 2"}
            ]
        if not sheets:
            self.stdout.write(self.style.ERROR("No se encontraron hojas 'Insumos 1' o 'Insumos 2'."))
            return

        recetas_actualizadas = 0
        recetas_no_encontradas = 0
        presentaciones_upsert = 0

        for sheet_name in sheets:
            ws = importador.wb[sheet_name]
            max_row = min(ws.max_row, 500)
            r = 1
            while r <= max_row - 2:
                a = ws.cell(row=r, column=1).value
                b = ws.cell(row=r + 1, column=1).value
                if not (isinstance(a, str) and a.strip() and isinstance(b, str) and "ingrediente" in b.lower()):
                    r += 1
                    continue

                receta_nombre = a.strip()
                if receta_filter and receta_filter not in receta_nombre.lower():
                    r += 1
                    continue
                header_row = r + 1
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

                lineas: List[Dict[str, Any]] = []
                rr = header_row + 1
                while rr <= max_row:
                    ing = ws.cell(row=rr, column=col_ing).value
                    qty = ws.cell(row=rr, column=col_qty).value if col_qty else None
                    unit = ws.cell(row=rr, column=col_unit).value if col_unit else None
                    cost = ws.cell(row=rr, column=col_cost).value if col_cost else None

                    if isinstance(ing, str) and normalizar_nombre(ing) in ("total", "costo total"):
                        break
                    if isinstance(qty, str) and "costo total" in qty.lower():
                        break
                    if ing is None and (qty is None or (isinstance(qty, str) and qty.strip() == "")):
                        break
                    if isinstance(ing, str) and "costo total" in ing.lower():
                        break
                    if isinstance(qty, str) and normalizar_nombre(qty) == "total":
                        break

                    if ing is not None and str(ing).strip():
                        lineas.append(
                            {
                                "ingrediente": str(ing).strip(),
                                "cantidad": _to_float(qty),
                                "unidad": str(unit).strip() if unit is not None else "",
                                "costo": _to_float(cost),
                            }
                        )
                    rr += 1

                receta = Receta.objects.filter(nombre_normalizado=normalizar_nombre(receta_nombre)).order_by("-id").first()
                if receta is None:
                    recetas_no_encontradas += 1
                    r = rr + 1
                    continue
                if receta.tipo != Receta.TIPO_PREPARACION:
                    r = rr + 1
                    continue

                rendimiento_cantidad, rendimiento_unidad, presentaciones = importador._extract_preparacion_metadata(
                    ws, r, rr, lineas
                )

                changed_fields: List[str] = []
                if rendimiento_cantidad is not None and rendimiento_cantidad > 0:
                    qty = Decimal(str(rendimiento_cantidad))
                    if receta.rendimiento_cantidad != qty:
                        receta.rendimiento_cantidad = qty
                        changed_fields.append("rendimiento_cantidad")
                if isinstance(rendimiento_unidad, UnidadMedida):
                    if receta.rendimiento_unidad_id != rendimiento_unidad.id:
                        receta.rendimiento_unidad = rendimiento_unidad
                        changed_fields.append("rendimiento_unidad")

                usa_presentaciones = bool(presentaciones)
                if receta.usa_presentaciones != usa_presentaciones:
                    receta.usa_presentaciones = usa_presentaciones
                    changed_fields.append("usa_presentaciones")

                if changed_fields:
                    receta.save(update_fields=changed_fields)
                    recetas_actualizadas += 1

                presentaciones_upsert += self._sync_presentaciones(receta, presentaciones)
                r = rr + 1

        self.stdout.write(self.style.SUCCESS("Sincronización de metadatos completada"))
        self.stdout.write(f"  - recetas_actualizadas: {recetas_actualizadas}")
        self.stdout.write(f"  - recetas_no_encontradas: {recetas_no_encontradas}")
        self.stdout.write(f"  - presentaciones_upsert: {presentaciones_upsert}")

    def _sync_presentaciones(self, receta: Receta, presentaciones: List[Dict[str, Any]]) -> int:
        incoming: Dict[str, Dict[str, Any]] = {}
        for p in presentaciones:
            nombre_p = str(p.get("nombre") or "").strip()[:80]
            peso = _to_float(p.get("peso_por_unidad_kg"))
            if not nombre_p or peso is None or peso <= 0:
                continue
            incoming[normalizar_nombre(nombre_p)] = {
                "nombre": nombre_p,
                "peso_por_unidad_kg": Decimal(str(peso)),
                "unidades_por_batch": p.get("unidades_por_batch"),
                "unidades_por_pastel": p.get("unidades_por_pastel"),
            }

        existing = {normalizar_nombre(x.nombre): x for x in receta.presentaciones.all()}
        writes = 0

        for key, data in incoming.items():
            obj = existing.get(key)
            if obj:
                changed_fields: List[str] = []
                if obj.nombre != data["nombre"]:
                    obj.nombre = data["nombre"]
                    changed_fields.append("nombre")
                if obj.peso_por_unidad_kg != data["peso_por_unidad_kg"]:
                    obj.peso_por_unidad_kg = data["peso_por_unidad_kg"]
                    changed_fields.append("peso_por_unidad_kg")
                if obj.unidades_por_batch != data["unidades_por_batch"]:
                    obj.unidades_por_batch = data["unidades_por_batch"]
                    changed_fields.append("unidades_por_batch")
                if obj.unidades_por_pastel != data["unidades_por_pastel"]:
                    obj.unidades_por_pastel = data["unidades_por_pastel"]
                    changed_fields.append("unidades_por_pastel")
                if not obj.activo:
                    obj.activo = True
                    changed_fields.append("activo")
                if changed_fields:
                    obj.save(update_fields=changed_fields)
                    writes += 1
            else:
                RecetaPresentacion.objects.create(
                    receta=receta,
                    nombre=data["nombre"],
                    peso_por_unidad_kg=data["peso_por_unidad_kg"],
                    unidades_por_batch=data["unidades_por_batch"],
                    unidades_por_pastel=data["unidades_por_pastel"],
                    activo=True,
                )
                writes += 1

        for key, obj in existing.items():
            if key not in incoming:
                obj.delete()
                writes += 1
        return writes
