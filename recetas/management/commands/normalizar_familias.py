from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from unidecode import unidecode

from recetas.models import Receta


NORMALIZACION = {
    "GALLETAS": "Galletas",
    "PAN": "Pan",
    "MASAS": "Masas",
    "Betún, Cremas, Rellenos (INSUMO PRODUCIDO)": "Betún y Rellenos",
}

REGLAS = [
    ("Pastel", ["pastel"]),
    ("Pay", ["pay", "sabor fresa", "sabor guayaba", "sabor galleta"]),
    ("Galletas", ["galleta", "alfajor", "cookie"]),
    ("Vasos Preparados", ["vaso"]),
    ("Bollo", ["bollo"]),
    ("Empanadas", ["empanada"]),
    ("Pan", ["pan ", "dona", "croissant"]),
    (
        "Bebidas",
        [
            "café",
            "cafe",
            "capuchino",
            "moka",
            "americano",
            "chocola",
            "agua ",
            "litro",
            "frappé",
        ],
    ),
    ("Cheesecake", ["cheesecake"]),
    ("Otros postres", []),
]


@dataclass
class FamilyNormalizationSummary:
    normalizaciones: Counter[str] = field(default_factory=Counter)
    asignaciones: Counter[str] = field(default_factory=Counter)
    fallback_rows: list[tuple[int, str, str]] = field(default_factory=list)


class Command(BaseCommand):
    help = "Normaliza familias duplicadas y clasifica productos finales sin familia con reglas por nombre."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Simula cambios sin escribir en BD.")
        parser.add_argument("--ejecutar", action="store_true", help="Aplica cambios en BD.")

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        ejecutar = bool(options["ejecutar"])
        if dry_run and ejecutar:
            raise CommandError("Usa solo una opcion: --dry-run o --ejecutar.")
        if not dry_run and not ejecutar:
            dry_run = True

        summary = self._process(dry_run=dry_run)
        self.stdout.write(f"normalizar_familias · dry_run={dry_run} · ejecutar={ejecutar}")
        self.stdout.write("")
        self.stdout.write("Normalizaciones de familias existentes:")
        if summary.normalizaciones:
            for family, count in sorted(summary.normalizaciones.items()):
                self.stdout.write(f"  {family}: {count}")
        else:
            self.stdout.write("  0")

        self.stdout.write("")
        self.stdout.write("Asignaciones nuevas por familia:")
        if summary.asignaciones:
            for family, count in sorted(summary.asignaciones.items()):
                self.stdout.write(f"  {family}: {count}")
        else:
            self.stdout.write("  0")

        self.stdout.write("")
        self.stdout.write(f"Fallback Otros postres: {len(summary.fallback_rows)}")
        for receta_id, codigo_point, nombre in summary.fallback_rows[:40]:
            self.stdout.write(f"  - {receta_id} | {codigo_point or ''} | {nombre}")

    def _process(self, *, dry_run: bool) -> FamilyNormalizationSummary:
        summary = FamilyNormalizationSummary()
        with transaction.atomic():
            for source, target in NORMALIZACION.items():
                qs = Receta.objects.filter(familia=source)
                count = qs.count()
                if count:
                    summary.normalizaciones[target] += count
                    if not dry_run:
                        qs.update(familia=target)

            sin_familia = Receta.objects.filter(
                tipo=Receta.TIPO_PRODUCTO_FINAL,
                familia="",
            ).order_by("nombre", "id")
            for receta in sin_familia:
                family = self._family_for_name(receta.nombre)
                summary.asignaciones[family] += 1
                if family == "Otros postres":
                    summary.fallback_rows.append((int(receta.id), receta.codigo_point or "", receta.nombre))
                if not dry_run:
                    receta.familia = family
                    receta.save(update_fields=["familia"])

            if dry_run:
                transaction.set_rollback(True)
        return summary

    def _family_for_name(self, name: str) -> str:
        normalized = self._normalize_name(name)
        for family, tokens in REGLAS:
            if not tokens:
                continue
            for token in tokens:
                if self._normalize_name(token) in normalized:
                    return family
        return "Otros postres"

    def _normalize_name(self, value: str) -> str:
        return " ".join(unidecode(value or "").casefold().strip().split())
