from __future__ import annotations

from datetime import date, timedelta

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.point_purchase_resale_cost_service import PointPurchaseResaleCostSyncService


class Command(BaseCommand):
    help = "Extrae costos de adquisición de productos de reventa desde Compras Point."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Guarda ProductoReventaCosto. Por defecto es dry-run.")
        parser.add_argument("--dias", type=int, default=120, help="Días hacia atrás para consultar compras Point.")
        parser.add_argument("--desde", default="", help="Fecha inicio ISO YYYY-MM-DD. Sobrescribe --dias.")
        parser.add_argument("--hasta", default="", help="Fecha fin ISO YYYY-MM-DD. Default: hoy.")
        parser.add_argument("--max-compras", type=int, default=800, help="Máximo de compras recientes a revisar.")

    def handle(self, *args, **options):
        apply = bool(options["apply"])
        hasta = date.today()
        desde = hasta - timedelta(days=int(options["dias"]))

        if options["hasta"]:
            try:
                hasta = date.fromisoformat(options["hasta"].strip())
            except ValueError as exc:
                raise CommandError(f"Fecha --hasta inválida: {options['hasta']}") from exc

        if options["desde"]:
            try:
                desde = date.fromisoformat(options["desde"].strip())
            except ValueError as exc:
                raise CommandError(f"Fecha --desde inválida: {options['desde']}") from exc

        mode = "APPLY" if apply else "DRY-RUN"
        self.stdout.write(f"Modo: {mode}")
        self.stdout.write(f"Rango compras Point: {desde} → {hasta}")
        self.stdout.write(f"Máximo compras: {options['max_compras']}")

        result = PointPurchaseResaleCostSyncService().sync_from_point(
            desde=desde,
            hasta=hasta,
            max_compras=int(options["max_compras"]),
            apply=apply,
        )

        self.stdout.write(f"Compras revisadas        : {result.purchases_seen}")
        self.stdout.write(f"Detalles revisados       : {result.details_seen}")
        self.stdout.write(f"Matches producto Point   : {result.matched_products}")
        if apply:
            self.stdout.write(self.style.SUCCESS(f"Costos creados           : {result.created}"))
        else:
            self.stdout.write(self.style.WARNING(f"Costos que se crearían   : {result.dry_run_created}"))
        self.stdout.write(f"Costos existentes        : {result.existing}")
        self.stdout.write(f"Costos cero/invalidos    : {result.zero_or_invalid_cost}")
        self.stdout.write(f"Sin match PointProduct   : {result.unresolved}")

        if result.imported_products:
            self.stdout.write("Productos con costo:")
            for name in sorted(result.imported_products)[:80]:
                self.stdout.write(f"  - {name}")

        if result.unresolved_samples:
            self.stdout.write("Muestras sin match:")
            for sample in result.unresolved_samples:
                self.stdout.write(f"  - {sample}")
