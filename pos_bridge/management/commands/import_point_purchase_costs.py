from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.point_purchase_cost_import_service import PointPurchaseCostImportService


class Command(BaseCommand):
    help = "Importa costos históricos de compras Point desde exportes JSON del browser helper."

    def add_arguments(self, parser):
        parser.add_argument("--summary-json", required=True, help="Ruta al JSON de resumen de compras.")
        parser.add_argument("--details-json", required=True, help="Ruta al JSON con detalles filtrados de compras.")

    def handle(self, *args, **options):
        service = PointPurchaseCostImportService()
        try:
            result = service.import_from_browser_exports(
                summary_path=options["summary_json"],
                details_path=options["details_json"],
            )
        except Exception as exc:  # noqa: BLE001
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            self.style.SUCCESS(
                f"created={result.created} existing={result.existing} "
                f"unresolved={result.unresolved}"
            )
        )
        if result.imported_articles:
            self.stdout.write("imported_articles=" + ", ".join(result.imported_articles))
        if result.unresolved_articles:
            self.stdout.write("unresolved_articles=" + ", ".join(result.unresolved_articles))
