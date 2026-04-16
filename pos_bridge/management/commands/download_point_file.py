from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.point_file_download_service import PointFileDownloadService


class Command(BaseCommand):
    help = "Descarga un archivo autenticado desde Point al raw export local sin depender de screenshots."

    def add_arguments(self, parser):
        parser.add_argument(
            "--path",
            required=True,
            help="Ruta Point (por ejemplo /Report/PrintReportes/) o URL completa del mismo host configurado.",
        )
        parser.add_argument("--branch", default="", help="ID externo de sucursal Point.")
        parser.add_argument("--branch-name", default="", help="Nombre visible de sucursal Point.")
        parser.add_argument("--output-name", default="", help="Nombre opcional del archivo de salida.")
        parser.add_argument(
            "--param",
            action="append",
            default=[],
            help="Parámetro query en formato clave=valor. Repetible.",
        )

    def handle(self, *args, **options):
        params: dict[str, str] = {}
        for raw_item in options["param"]:
            if "=" not in raw_item:
                raise CommandError(f"Parámetro inválido: {raw_item}. Usa clave=valor.")
            key, value = raw_item.split("=", 1)
            key = key.strip()
            if not key:
                raise CommandError(f"Parámetro inválido: {raw_item}. La clave no puede ir vacía.")
            params[key] = value.strip()

        result = PointFileDownloadService().download(
            path_or_url=options["path"].strip(),
            params=params,
            branch_external_id=options["branch"].strip() or None,
            branch_display_name=options["branch_name"].strip() or None,
            output_name=options["output_name"].strip() or None,
        )

        self.stdout.write("Archivo Point descargado")
        self.stdout.write(f"Recurso: {result.resource_path}")
        self.stdout.write(f"URL: {result.request_url}")
        self.stdout.write(f"Archivo: {result.output_path}")
        self.stdout.write(f"Tamaño: {result.size_bytes} bytes")
        self.stdout.write(f"Content-Type: {result.content_type}")
