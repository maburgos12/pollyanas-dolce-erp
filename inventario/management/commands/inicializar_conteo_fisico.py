from __future__ import annotations

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from inventario.services_conteo_fisico import ConteoFisicoError, ConteoFisicoService, parse_conteo_period


class Command(BaseCommand):
    help = "Inicializa el conteo físico mensual desde existencias de insumos y producto terminado CEDIS."

    def add_arguments(self, parser):
        parser.add_argument("--period", required=True, help="Periodo en formato YYYY-MM.")
        parser.add_argument("--responsable", default="", help="Username responsable. Default: primer superusuario/admin.")
        parser.add_argument("--dry-run", action="store_true", help="Muestra cuántas líneas crearía sin persistir.")

    def handle(self, *args, **options):
        try:
            periodo = parse_conteo_period(options["period"])
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        responsable = self._resolve_responsable(options.get("responsable") or "")
        try:
            summary = ConteoFisicoService().inicializar_conteo(
                periodo,
                responsable,
                fecha_conteo=timezone.localdate(),
                dry_run=options["dry_run"],
            )
        except ConteoFisicoError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            self.style.SUCCESS(
                f"Conteo físico · {periodo:%Y-%m} · dry_run={summary.dry_run}"
            )
        )
        self.stdout.write(f"Líneas insumos: {summary.insumos}")
        self.stdout.write(f"Líneas producto terminado: {summary.productos}")
        self.stdout.write(f"Total líneas: {summary.insumos + summary.productos}")

    def _resolve_responsable(self, username: str):
        User = get_user_model()
        if username:
            user = User.objects.filter(username=username).first()
            if user:
                return user
            raise CommandError(f"No existe usuario responsable: {username}")
        user = User.objects.filter(is_superuser=True).order_by("id").first()
        if user:
            return user
        user = User.objects.order_by("id").first()
        if user:
            return user
        raise CommandError("No hay usuarios para asignar responsable.")
