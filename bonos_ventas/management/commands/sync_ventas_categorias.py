from django.core.management.base import BaseCommand, CommandError

from bonos_ventas.models import ConfigBonoVentasPeriodo, VentaCategoriaSucursal
from bonos_ventas.services import sync_ventas_categorias


class Command(BaseCommand):
    help = "Sincroniza ventas por categoría para bonos desde pos_bridge_daily_sales."

    def add_arguments(self, parser):
        parser.add_argument("--mes", type=int, required=True)
        parser.add_argument("--anio", type=int, required=True)
        parser.add_argument("--sucursal", type=int, default=None)

    def handle(self, *args, **options):
        try:
            periodo = ConfigBonoVentasPeriodo.objects.get(mes=options["mes"], anio=options["anio"])
        except ConfigBonoVentasPeriodo.DoesNotExist as exc:
            raise CommandError("No existe ConfigBonoVentasPeriodo para el mes/anio indicado.") from exc

        updated = sync_ventas_categorias(periodo, sucursal_id=options.get("sucursal"))
        self.stdout.write(self.style.SUCCESS(f"Actualizados: {updated}"))
        rows = VentaCategoriaSucursal.objects.filter(periodo=periodo).select_related("sucursal").order_by("sucursal__nombre", "categoria")
        if options.get("sucursal"):
            rows = rows.filter(sucursal_id=options["sucursal"])
        for row in rows:
            self.stdout.write(
                f"{row.sucursal.nombre} / {row.categoria} / actual={row.cantidad_actual} / "
                f"anterior={row.cantidad_anterior} / pct={row.pct_crecimiento} / activo={row.activo_bono}"
            )
