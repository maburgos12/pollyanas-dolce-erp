from datetime import date

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from rrhh.services_vacaciones import reclasificar_solicitudes_futuras_consumidas


class Command(BaseCommand):
    help = "Reclasifica como reservadas las vacaciones futuras aprobadas que se consumieron anticipadamente."

    def add_arguments(self, parser):
        parser.add_argument(
            "--fecha-corte",
            default=None,
            help="Fecha local YYYY-MM-DD; por defecto usa el día actual.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica la reclasificación. Sin esta bandera solo reporta.",
        )

    def handle(self, *args, **options):
        fecha_corte = timezone.localdate()
        if options.get("fecha_corte"):
            try:
                fecha_corte = date.fromisoformat(options["fecha_corte"])
            except ValueError as exc:
                raise CommandError("--fecha-corte debe usar formato YYYY-MM-DD") from exc

        aplicar = bool(options.get("apply"))
        resultado = reclasificar_solicitudes_futuras_consumidas(
            fecha_corte=fecha_corte,
            aplicar=aplicar,
        )
        modo = "APLICADO" if aplicar else "DRY-RUN"
        self.stdout.write(
            f"{modo} fecha_corte={fecha_corte.isoformat()} "
            f"solicitudes={resultado['solicitudes']} "
            f"aplicaciones={resultado['aplicaciones']}"
        )
