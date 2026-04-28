from datetime import datetime
import json

from django.core.management.base import BaseCommand, CommandError

from core.models import Sucursal
from proyecciones.services import ProyeccionProduccionService


class Command(BaseCommand):
    help = "Genera proyecciones automáticas de producción por día o semana."

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--fecha", help="Fecha objetivo YYYY-MM-DD.")
        group.add_argument("--semana", help="Fecha dentro de la semana objetivo YYYY-MM-DD. Genera lunes-sábado.")
        parser.add_argument("--sucursal", help="Código o nombre de sucursal ERP opcional.")
        parser.add_argument("--dry-run", action="store_true", help="Calcula sin persistir cambios.")

    def handle(self, *args, **options):
        sucursal = self._get_sucursal(options.get("sucursal"))
        service = ProyeccionProduccionService()
        if options.get("fecha"):
            target_date = self._parse_date(options["fecha"], "--fecha")
            summary = service.proyectar_dia(target_date, sucursal=sucursal, dry_run=bool(options["dry_run"]))
        else:
            week_date = self._parse_date(options["semana"], "--semana")
            summary = service.proyectar_semana(week_date, sucursal=sucursal, dry_run=bool(options["dry_run"]))

        self.stdout.write(json.dumps(summary.as_dict(), ensure_ascii=False, indent=2))

    def _parse_date(self, value: str, option: str):
        try:
            return datetime.strptime(str(value or "").strip(), "%Y-%m-%d").date()
        except ValueError as exc:
            raise CommandError(f"{option} debe tener formato YYYY-MM-DD.") from exc

    def _get_sucursal(self, value: str | None):
        if not value:
            return None
        normalized = value.strip()
        try:
            return Sucursal.objects.get(codigo__iexact=normalized)
        except Sucursal.DoesNotExist:
            try:
                return Sucursal.objects.get(nombre__iexact=normalized)
            except Sucursal.DoesNotExist as exc:
                raise CommandError(f"No existe sucursal ERP: {value}") from exc
