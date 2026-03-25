from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Count, Sum

from pos_bridge.services.sync_service import PointSyncService
from recetas.models import VentaHistorica


def _parse_date(raw: str, label: str):
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise CommandError(f"{label} debe tener formato YYYY-MM-DD.") from exc


class Command(BaseCommand):
    help = "Reconstruye VentaHistorica fuente POINT_BRIDGE_SALES desde PointDailySale agrupando por fecha, sucursal ERP y receta."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", default="", help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", default="", help="Fecha final YYYY-MM-DD.")
        parser.add_argument("--dry-run", action="store_true", help="Solo calcula el resultado, no escribe cambios.")

    @transaction.atomic
    def handle(self, *args, **options):
        from pos_bridge.models import PointDailySale

        start_date = _parse_date((options.get("start_date") or "").strip(), "start-date")
        end_date = _parse_date((options.get("end_date") or "").strip(), "end-date")
        dry_run = bool(options.get("dry_run"))

        staged = PointDailySale.objects.filter(receta_id__isnull=False, branch__erp_branch_id__isnull=False)
        historical = VentaHistorica.objects.filter(fuente=PointSyncService.SALES_HISTORY_SOURCE)
        if start_date:
            staged = staged.filter(sale_date__gte=start_date)
            historical = historical.filter(fecha__gte=start_date)
        if end_date:
            staged = staged.filter(sale_date__lte=end_date)
            historical = historical.filter(fecha__lte=end_date)

        grouped = list(
            staged.values("sale_date", "branch__erp_branch_id", "receta_id")
            .annotate(
                cantidad=Sum("quantity"),
                tickets=Sum("tickets"),
                monto_total=Sum("total_amount"),
                source_rows=Count("id"),
            )
            .order_by("sale_date", "branch__erp_branch_id", "receta_id")
        )

        expected_count = len(grouped)
        deleted_count = historical.count()
        if dry_run:
            payload = {
                "dry_run": True,
                "expected_history_rows": expected_count,
                "existing_history_rows": deleted_count,
                "sample": grouped[:5],
            }
            self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
            return

        historical.delete()
        to_create = [
            VentaHistorica(
                receta_id=row["receta_id"],
                sucursal_id=row["branch__erp_branch_id"],
                fecha=row["sale_date"],
                cantidad=Decimal(str(row["cantidad"] or 0)),
                tickets=max(0, int(row["tickets"] or 0)),
                monto_total=Decimal(str(row["monto_total"] or 0)),
                fuente=PointSyncService.SALES_HISTORY_SOURCE,
            )
            for row in grouped
        ]
        VentaHistorica.objects.bulk_create(to_create, batch_size=1000)

        payload = {
            "dry_run": False,
            "deleted_history_rows": deleted_count,
            "created_history_rows": len(to_create),
            "expected_history_rows": expected_count,
        }
        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
