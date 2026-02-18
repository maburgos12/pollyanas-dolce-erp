from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand

from recetas.utils.template_loader import import_template


class Command(BaseCommand):
    help = "Importa recetas desde plantilla oficial CSV/XLSX."

    def add_arguments(self, parser):
        parser.add_argument("filepath", type=str, help="Ruta de plantilla CSV/XLSX")
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Reemplaza recetas existentes con el mismo nombre normalizado.",
        )

    def handle(self, *args, **options):
        filepath = Path(options["filepath"])
        if not filepath.exists():
            self.stdout.write(self.style.ERROR(f"Archivo no encontrado: {filepath}"))
            return

        result = import_template(str(filepath), replace_existing=bool(options["replace"]))

        if result.errores:
            self.stdout.write(self.style.WARNING("Importación con observaciones:"))
            for err in result.errores:
                self.stdout.write(f"  - {err}")

        self.stdout.write(self.style.SUCCESS("Resumen importación de plantilla:"))
        self.stdout.write(f"  - filas leídas: {result.total_rows}")
        self.stdout.write(f"  - recetas creadas: {result.recetas_creadas}")
        self.stdout.write(f"  - recetas actualizadas: {result.recetas_actualizadas}")
        self.stdout.write(f"  - recetas omitidas: {result.recetas_omitidas}")
        self.stdout.write(f"  - líneas creadas: {result.lineas_creadas}")
        self.stdout.write(f"  - matches pendientes: {result.matches_pendientes}")

