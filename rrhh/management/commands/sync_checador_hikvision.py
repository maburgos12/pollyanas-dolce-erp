from __future__ import annotations

import os
from datetime import date, timedelta

from django.core.files import File
from django.core.management.base import BaseCommand, CommandError

from rrhh.importers import importar_excel_hikconnect
from rrhh.services_hikvision import importar_asistencia_isapi


class Command(BaseCommand):
    help = "Sincroniza asistencia desde checador Hikvision por ISAPI/IP; Excel Hik-Connect queda como respaldo."

    def add_arguments(self, parser):
        parser.add_argument("--desde", help="Fecha inicial YYYY-MM-DD. Default: ayer.")
        parser.add_argument("--hasta", help="Fecha final YYYY-MM-DD. Default: hoy.")
        parser.add_argument("--base-url", default=os.getenv("HIKVISION_ISAPI_URL", "http://127.0.0.1:28073"))
        parser.add_argument("--usuario", default=os.getenv("HIKVISION_ISAPI_USER", "admin"))
        parser.add_argument("--password", default=os.getenv("HIKVISION_ISAPI_PASSWORD", ""))
        parser.add_argument(
            "--fallback-excel",
            help="Ruta de Excel exportado desde Hik-Connect si ISAPI/IP no está disponible.",
        )

    def handle(self, *args, **options):
        hoy = date.today()
        desde = date.fromisoformat(options["desde"]) if options["desde"] else hoy - timedelta(days=1)
        hasta = date.fromisoformat(options["hasta"]) if options["hasta"] else hoy
        password = options["password"]
        if not password:
            raise CommandError("Configura HIKVISION_ISAPI_PASSWORD o pasa --password.")

        try:
            resultado = importar_asistencia_isapi(
                fecha_inicio=desde,
                fecha_fin=hasta,
                base_url=options["base_url"],
                username=options["usuario"],
                password=password,
            )
            self.stdout.write(
                self.style.SUCCESS(
                    "ISAPI/IP OK: "
                    f"{resultado['procesados']} procesados, "
                    f"{resultado['errores']} errores, "
                    f"{resultado['duplicados']} duplicados."
                )
            )
            return
        except Exception as exc:
            self.stderr.write(self.style.WARNING(f"ISAPI/IP no disponible: {exc}"))

        fallback_excel = options.get("fallback_excel")
        if not fallback_excel:
            raise CommandError(
                "No se aplicó respaldo. Exporta Excel desde Hik-Connect y reintenta con --fallback-excel."
            )

        with open(fallback_excel, "rb") as fh:
            resultado = importar_excel_hikconnect(File(fh, name=os.path.basename(fallback_excel)), None, desde, hasta)
        self.stdout.write(
            self.style.SUCCESS(
                "Respaldo Hik-Connect Excel OK: "
                f"{resultado['procesados']} procesados, {resultado['errores']} errores."
            )
        )
