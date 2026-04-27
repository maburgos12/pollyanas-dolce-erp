from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from unidecode import unidecode

from recetas.models import Receta, RecetaEquivalencia


CONFIRMED_VALUES = {"SI", "SÍ", "YES", "TRUE", "1", "CONFIRMADO", "CONFIRMADA"}
EXCLUDE_VALUE = "EXCLUIR"


@dataclass
class ImportStats:
    loaded: int = 0
    direct: int = 0
    parent: int = 0
    excluded: int = 0
    not_found: int = 0
    skipped: int = 0
    warnings: list[str] | None = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class Command(BaseCommand):
    help = "Carga equivalencias de porciones para el cierre operativo mensual."

    def add_arguments(self, parser):
        parser.add_argument("--archivo", required=True, help="CSV con receta_porcion, receta_padre_confirmada, factor_conversion, confirmado.")
        parser.add_argument("--dry-run", action="store_true", help="Simula la carga sin escribir en BD.")
        parser.add_argument("--ejecutar", action="store_true", help="Aplica cambios en BD.")

    def handle(self, *args, **options):
        dry_run = bool(options["dry_run"])
        ejecutar = bool(options["ejecutar"])
        if dry_run and ejecutar:
            raise CommandError("Usa solo una opcion: --dry-run o --ejecutar.")
        if not dry_run and not ejecutar:
            dry_run = True

        path = Path(options["archivo"])
        if not path.exists():
            raise CommandError(f"No existe el archivo: {path}")

        rows = self._read_rows(path)
        stats = self._process_rows(rows=rows, dry_run=dry_run)
        payload = {
            "archivo": str(path),
            "dry_run": dry_run,
            "ejecutar": ejecutar,
            "equivalencias_cargadas": stats.loaded,
            "directas_1a1": stats.direct,
            "con_padre_distinto": stats.parent,
            "excluidas": stats.excluded,
            "no_encontradas": stats.not_found,
            "omitidas": stats.skipped,
            "warnings": stats.warnings[:50],
        }
        for key, value in payload.items():
            if key == "warnings":
                self.stdout.write("warnings:")
                for warning in value:
                    self.stdout.write(f"  - {warning}")
            else:
                self.stdout.write(f"{key}: {value}")

    def _read_rows(self, path: Path) -> list[dict[str, str]]:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            required = {"receta_porcion", "receta_padre_confirmada", "factor_conversion", "confirmado"}
            missing = required - set(reader.fieldnames or [])
            if missing:
                raise CommandError(f"Faltan columnas requeridas: {', '.join(sorted(missing))}")
            return list(reader)

    def _process_rows(self, *, rows: list[dict[str, str]], dry_run: bool) -> ImportStats:
        stats = ImportStats()
        recipe_by_name = self._recipe_index()
        with transaction.atomic():
            for index, row in enumerate(rows, start=2):
                self._process_row(row=row, row_number=index, stats=stats, recipe_by_name=recipe_by_name, dry_run=dry_run)
            if dry_run:
                transaction.set_rollback(True)
        return stats

    def _process_row(
        self,
        *,
        row: dict[str, str],
        row_number: int,
        stats: ImportStats,
        recipe_by_name: dict[str, Receta],
        dry_run: bool,
    ) -> None:
        porcion_name = (row.get("receta_porcion") or "").strip()
        parent_name = (row.get("receta_padre_confirmada") or "").strip()
        confirmed = (row.get("confirmado") or "").strip().upper()
        if not porcion_name:
            stats.skipped += 1
            stats.warnings.append(f"Fila {row_number}: receta_porcion vacia.")
            return

        porcion = recipe_by_name.get(self._normalize(porcion_name))
        if porcion is None:
            stats.not_found += 1
            stats.warnings.append(f"Fila {row_number}: no se encontro receta_porcion '{porcion_name}'.")
            return

        if confirmed == EXCLUDE_VALUE:
            stats.excluded += 1
            if not dry_run and not porcion.excluir_cierre:
                porcion.excluir_cierre = True
                porcion.save(update_fields=["excluir_cierre"])
            return

        if confirmed not in CONFIRMED_VALUES:
            stats.skipped += 1
            stats.warnings.append(f"Fila {row_number}: confirmado='{confirmed}' no esta aprobado; se omite.")
            return

        if not parent_name:
            stats.not_found += 1
            stats.warnings.append(f"Fila {row_number}: receta_padre_confirmada vacia para '{porcion_name}'.")
            return

        parent = recipe_by_name.get(self._normalize(parent_name))
        if parent is None:
            stats.not_found += 1
            stats.warnings.append(f"Fila {row_number}: no se encontro receta_padre_confirmada '{parent_name}'.")
            return

        factor = self._parse_factor(row.get("factor_conversion"), row_number=row_number, stats=stats)
        if factor is None:
            return

        stats.loaded += 1
        if parent.id == porcion.id and factor == Decimal("1"):
            stats.direct += 1
        else:
            stats.parent += 1

        if not dry_run:
            if porcion.excluir_cierre:
                porcion.excluir_cierre = False
                porcion.save(update_fields=["excluir_cierre"])
            RecetaEquivalencia.objects.update_or_create(
                receta_porcion=porcion,
                defaults={
                    "receta_padre": parent,
                    "factor_conversion": factor,
                    "activo": True,
                    "fuente": "equivalencias_porciones_final",
                    "metadata": {
                        "csv_receta_porcion": porcion_name,
                        "csv_receta_padre_confirmada": parent_name,
                        "csv_row_number": row_number,
                    },
                },
            )

    def _parse_factor(self, raw_value: str | None, *, row_number: int, stats: ImportStats) -> Decimal | None:
        value = (raw_value or "").strip().replace(",", "")
        if not value:
            stats.skipped += 1
            stats.warnings.append(f"Fila {row_number}: factor_conversion vacio.")
            return None
        try:
            factor = Decimal(value)
        except InvalidOperation:
            stats.skipped += 1
            stats.warnings.append(f"Fila {row_number}: factor_conversion invalido '{raw_value}'.")
            return None
        if factor <= 0:
            stats.skipped += 1
            stats.warnings.append(f"Fila {row_number}: factor_conversion debe ser mayor a cero.")
            return None
        return factor

    def _recipe_index(self) -> dict[str, Receta]:
        recipes = Receta.objects.all().order_by("id")
        index: dict[str, Receta] = {}
        duplicates: set[str] = set()
        for receta in recipes:
            key = self._normalize(receta.nombre)
            if not key:
                continue
            if key in index:
                duplicates.add(key)
                continue
            index[key] = receta
        for key in duplicates:
            index.pop(key, None)
        return index

    def _normalize(self, value: str) -> str:
        return " ".join(unidecode(value or "").casefold().strip().split())
