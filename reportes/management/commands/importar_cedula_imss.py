"""Importa una cédula IMSS/SIPARE (.xls) al presupuesto real.

Uso: manage.py importar_cedula_imss <ruta.xls> [--dry-run]
La mensual llena los rubros IMSS por área (cuota patronal); la bimestral
llena Infonavit/RCV partido 50/50 entre los dos meses del bimestre.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from reportes.services_cedula_imss import aplicar_cedula, cargar_filas_xls, parsear_cedula


class Command(BaseCommand):
    help = "Importa una cédula IMSS/SIPARE (.xls) al presupuesto real."

    def add_arguments(self, parser):
        parser.add_argument("ruta")
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        try:
            parseada = parsear_cedula(cargar_filas_xls(options["ruta"]))
        except (OSError, ValueError) as exc:
            raise CommandError(f"No se pudo leer la cédula: {exc}")

        resumen = aplicar_cedula(parseada, dry_run=options["dry_run"])
        modo = "DRY-RUN" if options["dry_run"] else "APLICADO"
        self.stdout.write(
            f"[{modo}] {resumen.tipo} → meses {', '.join(resumen.meses)} | "
            f"cuota patronal total: ${resumen.total_patronal:,.2f} | "
            f"empleados cruzados: {resumen.empleados_cruzados} | "
            f"líneas: {resumen.lineas_actualizadas} | manual protegidas: {resumen.protegidas_manual}"
        )
        for nss in resumen.nss_sin_cruce:
            self.stdout.write(self.style.WARNING(f"  NSS sin cruce: {nss}"))
        for aviso in resumen.avisos:
            self.stdout.write(self.style.WARNING(f"  AVISO: {aviso}"))
