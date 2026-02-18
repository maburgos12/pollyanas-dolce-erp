from __future__ import annotations

from decimal import Decimal

from django.core.management.base import BaseCommand

from recetas.models import LineaReceta, Receta
from recetas.utils.subsection_costing import find_parent_cost_for_stage


class Command(BaseCommand):
    help = "Recalcula costo_linea_excel de subsecciones en productos finales usando prorrateo por etapa."

    def add_arguments(self, parser):
        parser.add_argument(
            "--receta",
            dest="receta",
            default="",
            help="Filtra por nombre de receta (contains).",
        )

    def handle(self, *args, **options):
        receta_filter = (options.get("receta") or "").strip()
        recetas = Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).order_by("id")
        if receta_filter:
            recetas = recetas.filter(nombre__icontains=receta_filter)

        updated_lines = 0
        touched_recipes = 0
        skipped_stages = 0

        for receta in recetas:
            lineas = list(receta.lineas.order_by("posicion"))
            main_costs: list[tuple[str, float]] = []
            for l in lineas:
                if l.tipo_linea != LineaReceta.TIPO_NORMAL:
                    continue
                if l.costo_linea_excel is None or l.costo_linea_excel <= 0:
                    continue
                main_costs.append((l.insumo_texto or "", float(l.costo_linea_excel)))

            if not main_costs:
                continue

            groups: dict[str, list[LineaReceta]] = {}
            for l in lineas:
                if l.tipo_linea != LineaReceta.TIPO_SUBSECCION:
                    continue
                if l.cantidad is None or l.cantidad <= 0:
                    continue
                if l.costo_linea_excel is not None and l.costo_linea_excel > 0:
                    continue
                stage = (l.etapa or "").strip()
                if not stage:
                    continue
                groups.setdefault(stage, []).append(l)

            changed_any = False
            for stage, stage_lines in groups.items():
                parent_cost = find_parent_cost_for_stage(stage, main_costs)
                if parent_cost is None or parent_cost <= 0:
                    skipped_stages += 1
                    continue
                total_qty = sum(Decimal(l.cantidad) for l in stage_lines if l.cantidad is not None)
                if total_qty <= 0:
                    skipped_stages += 1
                    continue
                parent_dec = Decimal(str(parent_cost))
                for l in stage_lines:
                    qty = Decimal(l.cantidad or 0)
                    if qty <= 0:
                        continue
                    l.costo_linea_excel = parent_dec * (qty / total_qty)
                    updated_lines += 1
                    changed_any = True

            if changed_any:
                LineaReceta.objects.bulk_update(groups_flat(groups), ["costo_linea_excel"])
                touched_recipes += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Subsecciones recalculadas: {updated_lines} lineas | recetas tocadas: {touched_recipes} | etapas sin parent: {skipped_stages}"
            )
        )


def groups_flat(groups: dict[str, list[LineaReceta]]) -> list[LineaReceta]:
    items: list[LineaReceta] = []
    for lines in groups.values():
        items.extend(lines)
    return items
