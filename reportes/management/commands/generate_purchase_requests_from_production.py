from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand
from django.utils import timezone

from reportes.auto_purchase_service import generate_purchase_requests_from_production


class Command(BaseCommand):
    help = "Genera solicitudes de compra a partir de órdenes de producción aprobadas."

    def add_arguments(self, parser):
        parser.add_argument("--fecha", help="Fecha operativa en formato YYYY-MM-DD. Default: hoy.")
        parser.add_argument("--sucursal-id", type=int, help="Sucursal específica.")

    def handle(self, *args, **options):
        target_date = date.fromisoformat(options["fecha"]) if options.get("fecha") else timezone.localdate()
        result = generate_purchase_requests_from_production(
            target_date,
            sucursal_id=options.get("sucursal_id"),
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Auto purchase target={target_date} generated={result['generated']} "
                f"updated={result['updated']} deleted={result['deleted']} "
                f"skipped_locked={result['skipped_locked']} lines={result['lines']}"
            )
        )
