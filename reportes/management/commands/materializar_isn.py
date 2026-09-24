from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError

from reportes.services_isn import aplicar_expediente_isn, preparar_expediente_isn


def parse_period(value: str) -> date:
    try:
        parsed = date.fromisoformat(f"{value}-01")
    except (TypeError, ValueError) as exc:
        raise CommandError("--periodo debe usar el formato YYYY-MM.") from exc
    if parsed.strftime("%Y-%m") != value:
        raise CommandError("--periodo debe usar el formato YYYY-MM.")
    return parsed


class Command(BaseCommand):
    help = "Previsualiza o materializa la distribucion mensual del ISN."

    def add_arguments(self, parser):
        parser.add_argument("--periodo", required=True, help="Mes fiscal YYYY-MM")
        parser.add_argument(
            "--uuid",
            help="UUID exacto; si se omite debe existir un unico CFDI candidato",
        )
        parser.add_argument("--base-declarada", type=Decimal)
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        try:
            preview = preparar_expediente_isn(
                parse_period(options["periodo"]),
                uuid=options["uuid"],
                base_declarada=options["base_declarada"],
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(preview.render())
        if options["apply"]:
            try:
                expediente = aplicar_expediente_isn(
                    preview,
                )
            except ValueError as exc:
                raise CommandError(str(exc)) from exc
            self.stdout.write(
                self.style.SUCCESS(
                    f"{expediente.estado} expediente={expediente.pk}"
                )
            )
        else:
            self.stdout.write("DRY-RUN: sin cambios")
