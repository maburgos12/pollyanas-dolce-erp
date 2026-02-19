from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from inventario.utils.almacen_import import audit_folder


class Command(BaseCommand):
    help = "Audita nombres de insumos en archivos de almacén (inventario, entradas, salidas, merma)."

    def add_arguments(self, parser):
        parser.add_argument("folderpath", type=str, help="Carpeta donde están los 4 archivos de almacén")
        parser.add_argument(
            "--fuzzy-threshold",
            type=int,
            default=90,
            help="Score mínimo para considerar match FUZZY (default: 90)",
        )
        parser.add_argument(
            "--output",
            type=str,
            default="",
            help="Ruta CSV de salida (default: logs/almacen_auditoria_YYYYMMDD_HHMMSS.csv)",
        )

    def handle(self, *args, **options):
        folder = Path(options["folderpath"]).expanduser()
        if not folder.exists() or not folder.is_dir():
            raise CommandError(f"Carpeta inválida: {folder}")

        result = audit_folder(str(folder), fuzzy_threshold=int(options["fuzzy_threshold"]))

        output = options["output"].strip()
        if not output:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output = f"logs/almacen_auditoria_{ts}.csv"

        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        rows = result["rows"]
        headers = [
            "nombre_origen",
            "nombre_normalizado",
            "frecuencia_total",
            "fuentes",
            "match_status",
            "metodo_match",
            "score",
            "insumo_id",
            "insumo_nombre",
            "sugerencia",
        ]
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

        self.stdout.write(self.style.SUCCESS("Auditoría de almacén completada"))
        self.stdout.write(f"  - filas inventario: {result['stock_rows']}")
        self.stdout.write(f"  - filas movimientos: {result['movement_rows']}")
        self.stdout.write(f"  - nombres únicos: {result['unique_names']}")
        self.stdout.write(f"  - match: {result['matched']}")
        self.stdout.write(f"  - sin match: {result['unmatched']}")
        self.stdout.write(f"  - reporte: {out_path}")
