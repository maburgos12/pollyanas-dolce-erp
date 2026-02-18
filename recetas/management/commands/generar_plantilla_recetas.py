from __future__ import annotations

import csv
from pathlib import Path

from django.core.management.base import BaseCommand
from openpyxl import Workbook


HEADERS = [
    "receta",
    "subreceta",
    "producto_final",
    "ingrediente",
    "cantidad",
    "unidad",
    "costo_linea",
    "orden",
    "notas",
]

EXAMPLE_ROWS = [
    ["Pastel 3 Leches", "Pan Base", "Pastel 3 Leches Mediano", "Harina", "1.250", "kg", "", "1", ""],
    ["Pastel 3 Leches", "Pan Base", "Pastel 3 Leches Mediano", "Huevo", "18", "pza", "", "2", ""],
    ["Pastel 3 Leches", "Betun", "Pastel 3 Leches Mediano", "Queso crema", "0.850", "kg", "", "3", ""],
    ["Pastel 3 Leches", "Betun", "Pastel 3 Leches Mediano", "Leche evaporada", "0.600", "lt", "", "4", ""],
]


class Command(BaseCommand):
    help = "Genera plantilla oficial para carga masiva de recetas (CSV/XLSX)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--format",
            dest="fmt",
            choices=["csv", "xlsx"],
            default="xlsx",
            help="Formato de salida (default: xlsx)",
        )
        parser.add_argument(
            "--output",
            dest="output",
            default="plantilla_recetas_importacion.xlsx",
            help="Ruta de salida del archivo.",
        )

    def handle(self, *args, **options):
        fmt = options["fmt"]
        output_path = Path(options["output"])
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "csv":
            with output_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(HEADERS)
                writer.writerows(EXAMPLE_ROWS)
        else:
            wb = Workbook()
            ws = wb.active
            ws.title = "recetas"
            ws.append(HEADERS)
            for row in EXAMPLE_ROWS:
                ws.append(row)
            ws2 = wb.create_sheet("instrucciones")
            ws2.append(["Campo", "Descripción"])
            ws2.append(["receta", "Nombre de la receta final (obligatorio)."])
            ws2.append(["subreceta", "Bloque o etapa: pan, betún, mermelada, etc. (opcional)."])
            ws2.append(["producto_final", "Presentación/sabor final (opcional)."])
            ws2.append(["ingrediente", "Nombre del insumo/ingrediente (obligatorio)."])
            ws2.append(["cantidad", "Cantidad numérica por receta. Ejemplo: 1.250"])
            ws2.append(["unidad", "Unidad: kg, g, lt, ml, pza (recomendado)."])
            ws2.append(["costo_linea", "Costo directo de línea si viene en Excel (opcional)."])
            ws2.append(["orden", "Orden de la línea en receta (opcional)."])
            ws2.append(["notas", "Campo libre (opcional)."])
            wb.save(output_path)

        self.stdout.write(self.style.SUCCESS(f"Plantilla generada: {output_path}"))

