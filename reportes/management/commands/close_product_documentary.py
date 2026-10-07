from datetime import date

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from reportes.services_product_documentary_close import ProductDocumentaryCloseService


class Command(BaseCommand):
    help = "Revisa y, con --apply, registra cierres documentales por producto y sucursal."

    def add_arguments(self, parser):
        parser.add_argument("month", help="Mes YYYY-MM")
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--actor-user-id", type=int)

    def handle(self, *args, **options):
        try:
            month = date.fromisoformat(f"{options['month']}-01")
        except ValueError as exc:
            raise CommandError("El mes debe tener formato YYYY-MM.") from exc
        service = ProductDocumentaryCloseService()
        if not options["apply"]:
            decisions = service.evaluate(month)
            self.stdout.write(f"Elegibles: {sum(bool(item['eligible']) for item in decisions.values())}; pendientes: {sum(not item['eligible'] for item in decisions.values())}.")
            return
        if not options["actor_user_id"]:
            raise CommandError("--actor-user-id es obligatorio con --apply.")
        actor = get_user_model().objects.filter(pk=options["actor_user_id"]).first()
        if actor is None:
            raise CommandError("No existe el usuario indicado.")
        counts = service.close_eligible(month, actor=actor)
        self.stdout.write(
            "Cerrados: {closed}; reabiertos: {reopened}; sin cambios: {unchanged}; pendientes: {pending}.".format(**counts)
        )
