from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from inventario.utils.almacen_import import import_folder


VALID_SOURCES = {"inventario", "entradas", "salidas", "merma"}


class Command(BaseCommand):
    help = "Importa inventario/movimientos desde los 4 archivos de almacén y unifica nombres con matching."

    def add_arguments(self, parser):
        parser.add_argument("folderpath", type=str, help="Carpeta donde están los 4 archivos de almacén")
        parser.add_argument(
            "--sources",
            type=str,
            default="inventario,entradas,salidas,merma",
            help="Fuentes a importar (coma separada). Ejemplo: inventario,entradas",
        )
        parser.add_argument(
            "--fuzzy-threshold",
            type=int,
            default=96,
            help="Score mínimo para aceptar match FUZZY (default: 96)",
        )
        parser.add_argument(
            "--create-aliases",
            action="store_true",
            help="Crear/actualizar alias automáticamente cuando un match FUZZY sea confiable.",
        )
        parser.add_argument(
            "--create-missing-insumos",
            action="store_true",
            help="Crea insumos faltantes en catálogo cuando no hay match (recomendado para empaque/limpieza).",
        )
        parser.add_argument(
            "--alias-threshold",
            type=int,
            default=95,
            help="Score mínimo para crear alias automático (default: 95)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Simula importación sin persistir cambios.",
        )
        parser.add_argument(
            "--pending-output",
            type=str,
            default="",
            help="Ruta CSV para pendientes de match (default: logs/almacen_pendientes_YYYYMMDD_HHMMSS.csv)",
        )

    def handle(self, *args, **options):
        folder = Path(options["folderpath"]).expanduser()
        if not folder.exists() or not folder.is_dir():
            raise CommandError(f"Carpeta inválida: {folder}")

        requested = {x.strip().lower() for x in options["sources"].split(",") if x.strip()}
        invalid = sorted(requested - VALID_SOURCES)
        if invalid:
            raise CommandError(f"Fuentes inválidas: {', '.join(invalid)}")
        if not requested:
            raise CommandError("Debes indicar al menos una fuente en --sources")

        summary = import_folder(
            folderpath=str(folder),
            include_sources=requested,
            fuzzy_threshold=int(options["fuzzy_threshold"]),
            create_aliases=bool(options["create_aliases"]),
            alias_threshold=int(options["alias_threshold"]),
            create_missing_insumos=bool(options["create_missing_insumos"]),
            dry_run=bool(options["dry_run"]),
        )

        pending_output = options["pending_output"].strip()
        if summary.pendientes:
            if not pending_output:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                pending_output = f"logs/almacen_pendientes_{ts}.csv"
            p = Path(pending_output)
            p.parent.mkdir(parents=True, exist_ok=True)
            headers = ["source", "row", "nombre_origen", "nombre_normalizado", "score", "sugerencia"]
            with p.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(summary.pendientes)

        mode = "DRY-RUN" if options["dry_run"] else "APLICADO"
        self.stdout.write(self.style.SUCCESS(f"Importación de almacén completada ({mode})"))
        self.stdout.write(f"  - filas inventario leídas: {summary.rows_stock_read}")
        self.stdout.write(f"  - filas movimientos leídas: {summary.rows_mov_read}")
        self.stdout.write(f"  - matches: {summary.matched}")
        self.stdout.write(f"  - sin match: {summary.unmatched}")
        self.stdout.write(f"  - insumos creados: {summary.insumos_created}")
        self.stdout.write(f"  - existencias actualizadas: {summary.existencias_updated}")
        self.stdout.write(f"  - movimientos creados: {summary.movimientos_created}")
        self.stdout.write(f"  - movimientos omitidos (duplicado): {summary.movimientos_skipped_duplicate}")
        self.stdout.write(f"  - aliases creados/actualizados: {summary.aliases_created}")
        self.stdout.write(f"  - errores: {len(summary.errores)}")
        if summary.pendientes and pending_output:
            self.stdout.write(f"  - pendientes: {len(summary.pendientes)} ({pending_output})")
        elif summary.pendientes:
            self.stdout.write(f"  - pendientes: {len(summary.pendientes)}")
