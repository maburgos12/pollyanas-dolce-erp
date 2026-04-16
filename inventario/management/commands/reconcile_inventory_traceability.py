from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand
from django.utils import timezone

from core.audit import log_event
from inventario.models import AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
from inventario.stock_trace import infer_stock_trace
from reportes.analytics_service import rebuild_inventory_facts
from reportes.models import FactInventarioDiario


class Command(BaseCommand):
    help = "Reconcilia trazabilidad de existencias y reconstruye facts de inventario para una ventana histórica."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", default="2026-01-01", help="Fecha inicial YYYY-MM-DD")
        parser.add_argument("--end-date", default="", help="Fecha final YYYY-MM-DD. Default: hoy local")
        parser.add_argument("--execute", action="store_true", help="Aplica rebuild de facts y backfill de trazabilidad.")

    def handle(self, *args, **options):
        start_date = date.fromisoformat(options["start_date"])
        end_date = date.fromisoformat(options["end_date"]) if options["end_date"] else timezone.localdate()
        execute = bool(options["execute"])

        mov_days = set(
            MovimientoInventario.objects.filter(fecha__date__range=(start_date, end_date))
            .values_list("fecha__date", flat=True)
            .distinct()
        )
        fact_days_before = set(
            FactInventarioDiario.objects.filter(fecha__range=(start_date, end_date))
            .values_list("fecha", flat=True)
            .distinct()
        )
        missing_before = sorted(mov_days - fact_days_before)
        latest_sync_run = (
            AlmacenSyncRun.objects.filter(status=AlmacenSyncRun.STATUS_OK, started_at__date__range=(start_date, end_date))
            .order_by("-started_at", "-id")
            .first()
        )

        self.stdout.write(f"Ventana auditada: {start_date} -> {end_date}")
        self.stdout.write(f"Días con movimientos: {len(mov_days)}")
        self.stdout.write(f"Días con fact antes: {len(fact_days_before)}")
        self.stdout.write(f"Days missing fact before: {len(missing_before)}")

        rebuilt_rows = 0
        traced = 0
        untraced = 0
        if execute:
            rebuilt_rows = rebuild_inventory_facts(start_date=start_date, end_date=end_date)
            existencias = list(ExistenciaInsumo.objects.select_related("insumo").all())
            for existencia in existencias:
                existencia.trazabilidad_stock = infer_stock_trace(
                    existencia,
                    start_date=start_date,
                    end_date=end_date,
                    latest_sync_run=latest_sync_run,
                )
                if str((existencia.trazabilidad_stock or {}).get("source") or "") == "UNTRACED":
                    untraced += 1
                else:
                    traced += 1
            if existencias:
                ExistenciaInsumo.objects.bulk_update(existencias, ["trazabilidad_stock"], batch_size=500)
            log_event(
                None,
                "INVENTORY_TRACEABILITY_RECONCILED",
                "inventario.ExistenciaInsumo",
                f"{start_date}:{end_date}",
                payload={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "rebuilt_rows": rebuilt_rows,
                    "traceable_count": traced,
                    "untraceable_count": untraced,
                    "latest_sync_run_id": getattr(latest_sync_run, "id", None),
                },
            )

        fact_days_after = set(
            FactInventarioDiario.objects.filter(fecha__range=(start_date, end_date))
            .values_list("fecha", flat=True)
            .distinct()
        )
        missing_after = sorted(mov_days - fact_days_after)

        self.stdout.write(f"Fact rows rebuilt: {rebuilt_rows}")
        self.stdout.write(f"Días con fact después: {len(fact_days_after)}")
        self.stdout.write(f"Days missing fact after: {len(missing_after)}")
        if execute:
            self.stdout.write(f"Existencias con traza: {traced}")
            self.stdout.write(f"Existencias sin traza suficiente: {untraced}")
