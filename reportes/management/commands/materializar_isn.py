from argparse import ArgumentTypeError
from datetime import date
from decimal import Decimal, InvalidOperation

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


def parse_decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ArgumentTypeError(
            "--base-declarada debe ser un numero decimal."
        ) from exc


def parse_exenciones(values: list[str]) -> dict[str, Decimal]:
    politica = {}
    for value in values:
        if value.count("=") != 1:
            raise CommandError("--exencion debe usar CODIGO=PROPORCION.")
        codigo, proporcion_raw = (part.strip() for part in value.split("=", 1))
        if not codigo:
            raise CommandError("--exencion requiere un codigo no vacio.")
        if codigo in politica:
            raise CommandError(f"--exencion repite el codigo {codigo}.")
        try:
            proporcion = Decimal(proporcion_raw)
        except (InvalidOperation, ValueError) as exc:
            raise CommandError(
                f"--exencion {codigo} requiere una proporcion decimal."
            ) from exc
        if not proporcion.is_finite() or proporcion < 0 or proporcion > 1:
            raise CommandError(
                f"--exencion {codigo} debe estar entre 0 y 1."
            )
        politica[codigo] = proporcion
    return politica


class Command(BaseCommand):
    help = "Previsualiza o materializa la distribucion mensual del ISN."

    def add_arguments(self, parser):
        parser.add_argument("--periodo", required=True, help="Mes fiscal YYYY-MM")
        parser.add_argument(
            "--uuid",
            help="UUID exacto; si se omite debe existir un unico CFDI candidato",
        )
        parser.add_argument("--base-declarada", type=parse_decimal)
        parser.add_argument(
            "--exencion",
            action="append",
            default=[],
            metavar="CODIGO=PROPORCION",
            help="Sobrescribe una proporcion exenta; puede repetirse.",
        )
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        try:
            preview = preparar_expediente_isn(
                parse_period(options["periodo"]),
                uuid=options["uuid"],
                base_declarada=options["base_declarada"],
                politica_exenciones=parse_exenciones(options["exencion"]),
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
