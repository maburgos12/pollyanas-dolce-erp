from __future__ import annotations

from django.core.management.base import BaseCommand

from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import Receta


class Command(BaseCommand):
    help = "Backfill deterministico de modo_costeo para recetas generadas desde Point sales."

    def add_arguments(self, parser):
        parser.add_argument(
            "--all-productos-finales",
            action="store_true",
            help="Aplica la inferencia a todos los productos finales. Por defecto solo AUTO_POINT_SALES.",
        )

    def handle(self, *args, **options):
        matcher = PointSalesMatchingService()
        qs = Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL)
        if not options["all_productos_finales"]:
            qs = qs.filter(sheet_name="AUTO_POINT_SALES")

        updated = 0
        unchanged = 0
        for receta in qs.order_by("id"):
            expected_mode = matcher.infer_cost_mode(
                {
                    "family": receta.familia,
                    "category": receta.categoria,
                    "name": receta.nombre,
                    "sku": receta.codigo_point,
                }
            )
            if receta.modo_costeo == expected_mode:
                unchanged += 1
                continue
            receta.modo_costeo = expected_mode
            receta.save(update_fields=["modo_costeo"])
            updated += 1

        scope = "all_productos_finales" if options["all_productos_finales"] else "auto_point_sales"
        self.stdout.write(
            self.style.SUCCESS(
                f"backfill_receta_modo_costeo scope={scope} updated={updated} unchanged={unchanged}"
            )
        )
