from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from rrhh.services.lista_raya import (
    importar_lista_raya_nomina,
    parse_lista_raya_xls,
    validar_lista_raya_cuadre,
)


class Command(BaseCommand):
    help = "Importa una lista de raya de CONTPAQi Nóminas en formato .xls al módulo RRHH."

    def add_arguments(self, parser):
        parser.add_argument("archivo", type=str, help="Ruta al archivo .xls de lista de raya.")
        parser.add_argument(
            "--commit",
            action="store_true",
            help="Guarda empleados, periodo, líneas y conceptos. Sin esta bandera solo valida.",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Si el periodo ya existe, elimina sus líneas y conceptos antes de importar.",
        )

    def handle(self, *args, **options):
        archivo = Path(options["archivo"])
        if not archivo.exists():
            raise CommandError(f"No existe el archivo: {archivo}")

        try:
            result = parse_lista_raya_xls(archivo)
            summary = validar_lista_raya_cuadre(result)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self._print_summary(result, summary)
        if not options["commit"]:
            self.stdout.write(self.style.WARNING("Validación OK. Ejecuta con --commit para importar."))
            return

        try:
            import_result = importar_lista_raya_nomina(archivo, replace=options["replace"], archivo_nombre=archivo.name)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            self.style.SUCCESS(
                "Importación "
                f"{import_result['importacion'].id} lista: "
                f"{len(import_result['result'].empleados)} empleados, "
                f"periodo {import_result['periodo'].folio}."
            )
        )

    def _print_summary(self, result, summary):
        self.stdout.write(f"Archivo: {Path(result.source_path).name}")
        self.stdout.write(f"Empresa: {result.empresa}")
        self.stdout.write(f"Periodo: {result.fecha_inicio} a {result.fecha_fin} ({result.periodo_numero})")
        self.stdout.write(f"Empleados: {summary['empleados_detectados']} / {summary['empleados_reportados']}")
        self.stdout.write(
            "Totales: "
            f"percepciones {summary['total_percepciones_calculado']} / {summary['total_percepciones_reportado']}, "
            f"deducciones {summary['total_deducciones_calculado']} / {summary['total_deducciones_reportado']}, "
            f"neto {summary['total_neto_calculado']} / {summary['total_neto_reportado']}"
        )
