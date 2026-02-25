from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from activos.utils.bitacora_import import import_bitacora


class Command(BaseCommand):
    help = "Importa activos y servicios desde bitácora en XLSX o CSV"

    def add_arguments(self, parser):
        parser.add_argument("archivo", type=str, help="Ruta al archivo XLSX/CSV")
        parser.add_argument("--sheet", type=str, default="", help="Nombre de hoja (solo XLSX; por defecto: primera)")
        parser.add_argument("--dry-run", action="store_true", help="Simula importación sin guardar")
        parser.add_argument(
            "--skip-servicios",
            action="store_true",
            help="Solo crea/actualiza activos, sin generar historial de órdenes",
        )

    def handle(self, *args, **options):
        archivo = Path(options["archivo"]).expanduser().resolve()
        if not archivo.exists():
            raise CommandError(f"No existe el archivo: {archivo}")

        dry_run = bool(options["dry_run"])
        skip_servicios = bool(options["skip_servicios"])
        try:
            stats = import_bitacora(
                str(archivo),
                sheet_name=options["sheet"] or "",
                dry_run=dry_run,
                skip_servicios=skip_servicios,
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("Importación bitácora completada"))
        self.stdout.write(f"  - archivo: {archivo}")
        self.stdout.write(f"  - hoja: {stats['sheet_name']}")
        self.stdout.write(f"  - modo: {'DRY-RUN (sin persistir)' if dry_run else 'APLICADO'}")
        self.stdout.write(f"  - filas leídas: {stats['filas_leidas']}")
        self.stdout.write(f"  - filas válidas: {stats['filas_validas']}")
        self.stdout.write(f"  - activos creados: {stats['activos_creados']}")
        self.stdout.write(f"  - activos actualizados: {stats['activos_actualizados']}")
        if skip_servicios:
            self.stdout.write("  - servicios: omitido por --skip-servicios")
        else:
            self.stdout.write(f"  - servicios creados: {stats['servicios_creados']}")
            self.stdout.write(f"  - servicios omitidos (duplicados): {stats['servicios_omitidos']}")
