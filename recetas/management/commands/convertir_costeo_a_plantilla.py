from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand

from recetas.utils.template_converter import (
    convert_costeo_to_template,
    write_template_csv,
    write_template_xlsx,
)


class Command(BaseCommand):
    help = "Convierte COSTEO.xlsx a plantilla oficial de importación de recetas."

    def add_arguments(self, parser):
        parser.add_argument("input", type=str, help="Ruta a COSTEO.xlsx")
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
            default="/tmp/plantilla_recetas_desde_costeo.xlsx",
            help="Archivo de salida",
        )

    def handle(self, *args, **options):
        input_path = Path(options["input"])
        output_path = Path(options["output"])
        fmt = options["fmt"]

        if not input_path.exists():
            self.stdout.write(self.style.ERROR(f"No existe archivo: {input_path}"))
            return

        rows, result = convert_costeo_to_template(str(input_path))
        if fmt == "csv":
            write_template_csv(rows, str(output_path))
        else:
            write_template_xlsx(rows, str(output_path))

        self.stdout.write(self.style.SUCCESS("Conversión completada"))
        self.stdout.write(f"  - hojas escaneadas: {result.hojas_escaneadas}")
        self.stdout.write(f"  - hojas con recetas: {result.hojas_con_recetas}")
        self.stdout.write(f"  - recetas detectadas: {result.recetas_detectadas}")
        self.stdout.write(f"  - líneas detectadas: {result.lineas_detectadas}")
        self.stdout.write(f"  - archivo salida: {output_path}")

