from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db.models import Count, Q
from rapidfuzz import fuzz

from maestros.models import CostoInsumo, Insumo
from recetas.models import LineaReceta


@dataclass
class Suggestion:
    nombre: str
    score: float


class Command(BaseCommand):
    help = (
        "Audita insumos ligados en recetas que no tienen costo base y sugiere "
        "homologaciones contra insumos con costo."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--top",
            type=int,
            default=3,
            help="Cantidad de sugerencias por insumo (default: 3).",
        )
        parser.add_argument(
            "--min-score",
            type=float,
            default=70.0,
            help="Score mínimo para reportar sugerencias (default: 70).",
        )
        parser.add_argument(
            "--output",
            type=str,
            default="",
            help="Ruta opcional CSV de salida.",
        )

    def handle(self, *args, **options):
        top_n = max(1, int(options["top"]))
        min_score = float(options["min_score"])

        costed_ids = set(CostoInsumo.objects.values_list("insumo_id", flat=True).distinct())
        costed_insumos = list(
            Insumo.objects.filter(id__in=costed_ids)
            .values_list("id", "nombre", "nombre_normalizado")
            .order_by("nombre")
        )

        missing_qs = (
            LineaReceta.objects.filter(insumo__isnull=False)
            .filter(Q(costo_unitario_snapshot__isnull=True) | Q(costo_unitario_snapshot__lte=0))
            .values("insumo_id", "insumo__nombre")
            .annotate(lineas_afectadas=Count("id"))
            .order_by("-lineas_afectadas", "insumo__nombre")
        )

        rows: list[dict] = []
        for item in missing_qs:
            insumo_id = item["insumo_id"]
            nombre = item["insumo__nombre"]
            lineas_afectadas = int(item["lineas_afectadas"])
            has_cost = insumo_id in costed_ids
            suggestions = self._suggest(
                source=nombre,
                candidates=costed_insumos,
                top_n=top_n,
                min_score=min_score,
            )
            rows.append(
                {
                    "insumo": nombre,
                    "lineas_afectadas": lineas_afectadas,
                    "has_cost": has_cost,
                    "sugerencias": " | ".join(f"{s.nombre} ({s.score:.1f})" for s in suggestions) or "-",
                }
            )

        self.stdout.write("Auditoría de insumos sin costo en recetas")
        self.stdout.write(f"  - insumos costeados en catálogo: {len(costed_ids)}")
        self.stdout.write(f"  - insumos sin costo afectando recetas: {len(rows)}")
        for row in rows:
            self.stdout.write(
                f"    * {row['insumo']} | lineas={row['lineas_afectadas']} | "
                f"has_cost={row['has_cost']} | sugerencias={row['sugerencias']}"
            )

        output = (options.get("output") or "").strip()
        if output:
            out_path = Path(output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("w", newline="", encoding="utf-8") as fp:
                writer = csv.DictWriter(
                    fp,
                    fieldnames=["insumo", "lineas_afectadas", "has_cost", "sugerencias"],
                )
                writer.writeheader()
                writer.writerows(rows)
            self.stdout.write(self.style.SUCCESS(f"CSV generado: {out_path}"))

    def _suggest(
        self,
        source: str,
        candidates: list[tuple[int, str, str]],
        top_n: int,
        min_score: float,
    ) -> list[Suggestion]:
        source_norm = " ".join((source or "").lower().split())
        scored: list[Suggestion] = []
        for _, name, norm in candidates:
            score = float(fuzz.WRatio(source_norm, norm))
            if score >= min_score:
                scored.append(Suggestion(nombre=name, score=score))
        scored.sort(key=lambda x: (-x.score, x.nombre))
        return scored[:top_n]
