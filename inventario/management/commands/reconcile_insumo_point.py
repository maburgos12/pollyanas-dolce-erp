from django.core.management.base import BaseCommand, CommandError

from inventario.services_point_reconciliation import reconcile_insumo_from_point
from maestros.models import Insumo


class Command(BaseCommand):
    help = "Previsualiza o concilia un insumo con Point, separando ALMACÉN y CEDIS."

    def add_arguments(self, parser):
        parser.add_argument("--insumo-id", type=int, required=True)
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica los dos ajustes. Sin esta opción el comando es solo lectura.",
        )

    def handle(self, *args, **options):
        try:
            insumo = Insumo.objects.select_related("unidad_base").get(pk=options["insumo_id"], activo=True)
        except Insumo.DoesNotExist as exc:
            raise CommandError("No existe un insumo activo con ese ID.") from exc

        results = reconcile_insumo_from_point(insumo=insumo, apply=options["apply"])
        mode = "APLICADO" if options["apply"] else "PREVISUALIZACIÓN"
        self.stdout.write(f"{mode}: {insumo.nombre} [{insumo.codigo_point}]")
        for row in results:
            movement = f" movimiento={row.movement_id}" if row.movement_id else ""
            self.stdout.write(
                f"{row.ledger}: Point {row.point_qty} {row.point_unit} "
                f"=> ERP {row.target_qty} {insumo.unidad_base.codigo}; "
                f"anterior={row.current_qty}; delta={row.delta}{movement}"
            )
