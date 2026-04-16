from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from reportes.services_operating_finance import OperatingFinanceBootstrapService


class Command(BaseCommand):
    help = "Crea el catálogo base de centros de costo, categorías de gasto y reglas de asignación."

    def handle(self, *args, **options):
        payload = OperatingFinanceBootstrapService().bootstrap()
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
