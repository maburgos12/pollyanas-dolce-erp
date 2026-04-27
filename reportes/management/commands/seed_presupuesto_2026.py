from __future__ import annotations

from django.core.management.base import BaseCommand

from reportes.services_presupuesto_maestro import ensure_master_budget_areas, seed_capex_guamuchil_2026


class Command(BaseCommand):
    help = "Inicializa áreas del presupuesto maestro y CAPEX Guamúchil confirmado 2026."

    def handle(self, *args, **options):
        areas = ensure_master_budget_areas()
        capex = seed_capex_guamuchil_2026()
        self.stdout.write(self.style.SUCCESS("Presupuesto maestro inicializado"))
        self.stdout.write(f"Áreas activas: {len(areas)}")
        self.stdout.write(f"CAPEX creado/actualizado: {capex['created']} / {capex['updated']}")
        self.stdout.write(f"CAPEX total entero: {capex['capex_total']}")
