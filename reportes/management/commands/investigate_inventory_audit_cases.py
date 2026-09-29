from datetime import datetime

from django.core.management.base import BaseCommand, CommandError

from reportes.services_inventory_audit_agent import InventoryAuditAgent


class Command(BaseCommand):
    help = "Investiga casos de auditoría ya guardados, sin descargar ni duplicar datos Point."

    def add_arguments(self, parser):
        parser.add_argument("--month", required=True, help="Mes en formato YYYY-MM.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Calcula resultados sin modificar casos ni crear notificaciones.",
        )

    def handle(self, *args, **options):
        try:
            month = datetime.strptime(options["month"], "%Y-%m").date().replace(day=1)
        except ValueError as exc:
            raise CommandError("--month debe usar el formato YYYY-MM.") from exc

        counters = InventoryAuditAgent().run_month(
            month,
            dry_run=options["dry_run"],
        )
        mode = "VISTA PREVIA" if options["dry_run"] else "APLICADO"
        self.stdout.write(f"{mode} · {month:%Y-%m}")
        self.stdout.write(
            " · ".join(f"{key}={value}" for key, value in counters.items())
        )
