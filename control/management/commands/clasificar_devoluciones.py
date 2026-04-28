import json

from django.core.management.base import BaseCommand

from control.services_mermas_devoluciones import MermaDevolucionAuditService


class Command(BaseCommand):
    help = "Clasifica devoluciones sucursal/devoluciones -> matriz desde PointTransferLine."

    def add_arguments(self, parser):
        parser.add_argument("--period", required=True, help="Periodo YYYY-MM.")
        parser.add_argument("--dry-run", action="store_true", help="No escribe en BD.")

    def handle(self, *args, **options):
        result = MermaDevolucionAuditService().clasificar_devoluciones(
            period=options["period"],
            dry_run=bool(options["dry_run"]),
        )
        self.stdout.write(json.dumps(result.as_dict(), ensure_ascii=False, indent=2))
