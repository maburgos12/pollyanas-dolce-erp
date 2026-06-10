from __future__ import annotations

from datetime import date, timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from rrhh.models import Empleado
from rrhh.services_asistencia_reglas import evaluar_rango_asistencia


class Command(BaseCommand):
    help = "Evalua reglas internas de asistencia RRHH y genera incidencias auditables."

    def add_arguments(self, parser):
        parser.add_argument("--desde", help="Fecha inicial YYYY-MM-DD. Default: ayer.")
        parser.add_argument("--hasta", help="Fecha final YYYY-MM-DD. Default: hoy.")
        parser.add_argument(
            "--empleado",
            dest="empleado_codigo",
            help="Codigo de empleado / ID checador para evaluar una sola persona.",
        )
        parser.add_argument(
            "--solo-con-asistencia",
            action="store_true",
            help="No generar faltas por ausencia de registro; evalua solo empleados con asistencia en el rango.",
        )

    def handle(self, *args, **options):
        hoy = timezone.localdate()
        desde = date.fromisoformat(options["desde"]) if options["desde"] else hoy - timedelta(days=1)
        hasta = date.fromisoformat(options["hasta"]) if options["hasta"] else hoy
        if hasta < desde:
            raise CommandError("--hasta no puede ser anterior a --desde.")

        empleados = None
        codigo = (options.get("empleado_codigo") or "").strip()
        if codigo:
            try:
                empleados = [Empleado.objects.get(codigo=codigo, activo=True)]
            except Empleado.DoesNotExist as exc:
                raise CommandError(f"No existe empleado activo con codigo {codigo}.") from exc

        resultado = evaluar_rango_asistencia(
            desde,
            hasta,
            incluir_sin_asistencia=not options["solo_con_asistencia"],
            empleados=empleados,
        )
        self.stdout.write(
            self.style.SUCCESS(
                "Evaluacion asistencia RRHH OK: "
                f"{resultado.evaluados} empleado-dia evaluados, "
                f"{resultado.creados} incidencias nuevas, "
                f"{resultado.actualizados} actualizadas, "
                f"{resultado.resueltos} resueltas."
            )
        )

