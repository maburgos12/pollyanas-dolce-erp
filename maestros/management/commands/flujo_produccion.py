"""
Muestra el flujo completo de producción de insumos preparados (INSUMO_INTERNO).

Para cada insumo producido muestra:
  - Qué materias primas lo componen (BOM de 1er nivel)
  - En qué recetas de productos finales se usa
  - Orden sugerido de producción (por dependencias)

Por defecto imprime el árbol en consola. Usa --csv para exportar.
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict, deque

from django.core.management.base import BaseCommand

from maestros.models import Insumo
from recetas.models import LineaReceta, Receta


def _build_graphs(internos: list[Insumo]) -> tuple[dict, dict, dict]:
    """
    Devuelve:
      bom       : insumo_id → [{"nombre", "cantidad", "unidad", "tipo"}]
      usado_en  : insumo_id → [{"receta_nombre", "tipo_receta", "cantidad"}]
      deps      : insumo_id → set(insumo_ids que necesita antes de producirse)
    """
    interno_ids = {i.id for i in internos}

    bom: dict[int, list[dict]] = defaultdict(list)
    usado_en: dict[int, list[dict]] = defaultdict(list)
    deps: dict[int, set[int]] = defaultdict(set)

    for linea in (
        LineaReceta.objects.select_related("insumo", "insumo__unidad_base", "receta")
        .filter(receta__insumo_resultado__in=interno_ids)
        .order_by("receta_id", "id")
    ):
        receta_insumo_id = linea.receta.insumo_resultado_id
        componente = linea.insumo
        unidad = componente.unidad_base.codigo if componente.unidad_base_id else "?"
        bom[receta_insumo_id].append({
            "nombre": componente.nombre,
            "tipo": componente.tipo_item,
            "cantidad": float(linea.cantidad),
            "unidad": unidad,
            "componente_id": componente.id,
        })
        if componente.id in interno_ids:
            deps[receta_insumo_id].add(componente.id)

    for linea in (
        LineaReceta.objects.select_related("receta", "insumo")
        .filter(insumo__in=interno_ids)
        .exclude(receta__insumo_resultado__in=interno_ids)
        .order_by("receta_id")
    ):
        usado_en[linea.insumo_id].append({
            "receta_nombre": linea.receta.nombre,
            "tipo_receta": linea.receta.tipo,
            "cantidad": float(linea.cantidad),
        })

    return dict(bom), dict(usado_en), dict(deps)


def _topo_sort(internos: list[Insumo], deps: dict[int, set[int]]) -> list[Insumo]:
    """Orden topológico: primero los que no dependen de otros internos."""
    id_to_insumo = {i.id: i for i in internos}
    in_degree: dict[int, int] = {i.id: 0 for i in internos}
    reverse: dict[int, list[int]] = defaultdict(list)

    for nodo, predecesores in deps.items():
        for pred in predecesores:
            if pred in in_degree:
                in_degree[nodo] += 1
                reverse[pred].append(nodo)

    queue = deque(iid for iid, deg in in_degree.items() if deg == 0)
    ordered: list[Insumo] = []
    while queue:
        iid = queue.popleft()
        ordered.append(id_to_insumo[iid])
        for sucesor in reverse[iid]:
            in_degree[sucesor] -= 1
            if in_degree[sucesor] == 0:
                queue.append(sucesor)

    # Agregar cualquier nodo no alcanzado (ciclos o sin deps registradas)
    seen = {i.id for i in ordered}
    for i in internos:
        if i.id not in seen:
            ordered.append(i)

    return ordered


class Command(BaseCommand):
    help = (
        "Muestra el flujo de producción de insumos internos (INSUMO_INTERNO): "
        "qué los compone, en qué productos finales se usan y el orden de producción sugerido."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--categoria",
            default="",
            help="Filtrar por categoría (ej. MASAS, RELLENOS Y CREMAS, LACTEOS).",
        )
        parser.add_argument(
            "--nombre",
            default="",
            help="Filtrar por nombre parcial del insumo interno.",
        )
        parser.add_argument(
            "--csv",
            dest="csv_output",
            default="",
            help="Exportar a CSV en la ruta indicada (ej. flujo.csv).",
        )
        parser.add_argument(
            "--solo-orden",
            action="store_true",
            help="Imprime solo el orden sugerido de producción, sin el detalle de BOM.",
        )

    def handle(self, *args, **options):
        categoria = (options["categoria"] or "").strip().upper()
        nombre_filtro = (options["nombre"] or "").strip().lower()
        csv_path = (options["csv_output"] or "").strip()
        solo_orden = bool(options["solo_orden"])

        qs = Insumo.objects.filter(activo=True, tipo_item=Insumo.TIPO_INTERNO).select_related("unidad_base")
        if categoria:
            qs = qs.filter(categoria__iexact=categoria)
        if nombre_filtro:
            qs = qs.filter(nombre__icontains=nombre_filtro)
        internos = list(qs.order_by("nombre"))

        if not internos:
            self.stdout.write(self.style.WARNING("No hay insumos internos activos con ese filtro."))
            return

        bom, usado_en, deps = _build_graphs(internos)
        ordenados = _topo_sort(internos, deps)

        if csv_path:
            self._export_csv(ordenados, bom, usado_en, csv_path)
            self.stdout.write(self.style.SUCCESS(f"Exportado: {csv_path}"))
            return

        if solo_orden:
            self._print_orden(ordenados, deps)
            return

        self._print_flujo(ordenados, bom, usado_en, deps)

    def _print_orden(self, ordenados: list[Insumo], deps: dict[int, set[int]]):
        self.stdout.write("\n── Orden sugerido de producción (menor a mayor dependencia) ──\n")
        for idx, insumo in enumerate(ordenados, 1):
            dep_count = len(deps.get(insumo.id, set()))
            dep_label = f"  ← depende de {dep_count} preparado(s) previo(s)" if dep_count else ""
            self.stdout.write(f"  {idx:3d}. {insumo.nombre} [{insumo.categoria or '—'}]{dep_label}")
        self.stdout.write(f"\n  Total: {len(ordenados)} insumos preparados\n")

    def _print_flujo(self, ordenados: list[Insumo], bom: dict, usado_en: dict, deps: dict):
        self.stdout.write(f"\n{'═'*70}")
        self.stdout.write("  FLUJO DE PRODUCCIÓN — INSUMOS PREPARADOS (INSUMO_INTERNO)")
        self.stdout.write(f"{'═'*70}\n")

        for idx, insumo in enumerate(ordenados, 1):
            cat = insumo.categoria or "—"
            unidad = insumo.unidad_base.codigo if insumo.unidad_base_id else "?"
            dep_ids = deps.get(insumo.id, set())

            self.stdout.write(self.style.SUCCESS(
                f"\n[{idx:03d}] {insumo.nombre}"
            ))
            self.stdout.write(f"      Categoría : {cat}  |  Unidad : {unidad}")

            componentes = bom.get(insumo.id, [])
            if componentes:
                self.stdout.write("      ── Componentes (BOM) ──")
                for c in componentes:
                    tipo_tag = "⚙ preparado" if c["tipo"] == Insumo.TIPO_INTERNO else "🛒 compra"
                    self.stdout.write(
                        f"         {c['cantidad']:>8.3f} {c['unidad']:<4}  {c['nombre']}  [{tipo_tag}]"
                    )
            else:
                self.stdout.write(self.style.WARNING("      ── Sin BOM registrado en ERP ──"))

            productos = usado_en.get(insumo.id, [])
            if productos:
                self.stdout.write("      ── Usado en productos finales ──")
                for p in productos[:10]:
                    self.stdout.write(f"         · {p['receta_nombre']}  ({p['cantidad']:.3f})")
                if len(productos) > 10:
                    self.stdout.write(f"         ... y {len(productos)-10} más")
            else:
                self.stdout.write(self.style.WARNING("      ── No vinculado a productos finales aún ──"))

            if dep_ids:
                self.stdout.write(f"      ── Requiere preparar antes : {len(dep_ids)} insumo(s) interno(s) ──")

        self.stdout.write(f"\n{'─'*70}")
        self.stdout.write(f"  Total insumos preparados: {len(ordenados)}")
        no_bom = sum(1 for i in ordenados if not bom.get(i.id))
        no_uso = sum(1 for i in ordenados if not usado_en.get(i.id))
        if no_bom:
            self.stdout.write(self.style.WARNING(f"  Sin BOM: {no_bom}  ← agregar receta en ERP"))
        if no_uso:
            self.stdout.write(self.style.WARNING(f"  Sin uso en productos finales: {no_uso}"))
        self.stdout.write("")

    def _export_csv(self, ordenados: list[Insumo], bom: dict, usado_en: dict, path: str):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "orden_produccion", "insumo_preparado", "categoria", "unidad",
                "componente", "componente_tipo", "cantidad_componente", "unidad_componente",
                "usado_en_producto", "tipo_producto",
            ])
            for idx, insumo in enumerate(ordenados, 1):
                unidad = insumo.unidad_base.codigo if insumo.unidad_base_id else ""
                componentes = bom.get(insumo.id, [{"nombre": "", "tipo": "", "cantidad": "", "unidad": ""}])
                productos = usado_en.get(insumo.id, [{"receta_nombre": "", "tipo_receta": ""}])
                for c in componentes:
                    for p in productos:
                        writer.writerow([
                            idx, insumo.nombre, insumo.categoria or "", unidad,
                            c["nombre"], c.get("tipo", ""), c.get("cantidad", ""), c.get("unidad", ""),
                            p["receta_nombre"], p.get("tipo_receta", ""),
                        ])
