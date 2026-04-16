from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from ventas.models import EventoVenta
from ventas.services.point_reconciliation import reconcile_event_point_sales


class Command(BaseCommand):
    help = "Conciliar Point vs VentaHistorica bridge para la ventana homóloga del evento y exportar diferencias por día/sucursal/SKU."

    def add_arguments(self, parser):
        parser.add_argument("--event-id", type=int, required=True, help="ID del evento comercial.")

    def handle(self, *args, **options):
        try:
            event = EventoVenta.objects.get(pk=options["event_id"])
        except EventoVenta.DoesNotExist as exc:
            raise CommandError("Evento no encontrado.") from exc

        summary = reconcile_event_point_sales(event)
        self.stdout.write("Conciliación Point vs ERP")
        self.stdout.write(f"Evento: {event.code} · {event.name}")
        self.stdout.write(f"Rango homólogo: {summary.start_date.isoformat()} -> {summary.end_date.isoformat()}")
        self.stdout.write(f"PointDailySale revisadas: {summary.scanned_rows}")
        self.stdout.write(f"Filas autoritativas aplicadas: {summary.authoritative_rows}")
        self.stdout.write(f"Filas no-receta: {summary.non_recipe_rows}")
        self.stdout.write(f"Filas sin match: {summary.unresolved_rows}")
        self.stdout.write(f"Filas bridge: {summary.bridge_rows}")
        self.stdout.write(f"Diferencias activas: {summary.mismatch_rows}")
        self.stdout.write(f"Diferencia total cantidad: {summary.qty_diff_total}")
        self.stdout.write(f"Diferencia total ventas: {summary.sales_diff_total}")
        self.stdout.write(f"Reporte: {summary.report_path}")
