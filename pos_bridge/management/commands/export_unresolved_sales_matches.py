from __future__ import annotations

import csv
from datetime import date, datetime, timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointDailySale
from pos_bridge.services.sales_matching_service import PointSalesMatchingService


def _parse_date(value: str, *, label: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise CommandError(f"{label} debe tener formato YYYY-MM-DD.") from exc


class Command(BaseCommand):
    help = "Exporta a CSV las ventas Point sin match automático a receta."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", default="2022-01-01", help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", default="", help="Fecha final YYYY-MM-DD. Si se omite, usa ayer local.")

    def handle(self, *args, **options):
        start_date = _parse_date((options.get("start_date") or "").strip(), label="start-date")
        end_raw = (options.get("end_date") or "").strip()
        end_date = _parse_date(end_raw, label="end-date") if end_raw else (timezone.localdate() - timedelta(days=1))
        if end_date < start_date:
            raise CommandError("end-date no puede ser menor a start-date.")

        settings = load_point_bridge_settings()
        reports_dir = settings.storage_root / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        output_path = reports_dir / f"unresolved_sales_matches_{start_date.isoformat()}_{end_date.isoformat()}.csv"

        matcher = PointSalesMatchingService()
        buckets: dict[tuple[str, str], dict] = {}

        queryset = (
            PointDailySale.objects.filter(sale_date__gte=start_date, sale_date__lte=end_date, receta__isnull=True)
            .select_related("branch", "product")
            .order_by("sale_date", "branch__id", "product__id")
        )

        for sale in queryset.iterator(chunk_size=1000):
            if matcher.is_non_recipe_sale_row(
                {
                    "family": sale.product.metadata.get("family", ""),
                    "category": sale.product.category,
                    "name": sale.product.name,
                    "sku": sale.product.sku,
                }
            ):
                continue

            key = (str(sale.branch_id), str(sale.product_id))
            bucket = buckets.get(key)
            if bucket is None:
                bucket = {
                    "branch_external_id": sale.branch.external_id,
                    "branch_name": sale.branch.name,
                    "product_external_id": sale.product.external_id,
                    "sku": sale.product.sku,
                    "product_name": sale.product.name,
                    "category": sale.product.category,
                    "family": sale.product.metadata.get("family", ""),
                    "occurrences": 0,
                    "first_sale_date": sale.sale_date,
                    "last_sale_date": sale.sale_date,
                    "total_quantity": 0,
                    "total_amount": 0,
                    "tickets": 0,
                }
                buckets[key] = bucket

            bucket["occurrences"] += 1
            bucket["first_sale_date"] = min(bucket["first_sale_date"], sale.sale_date)
            bucket["last_sale_date"] = max(bucket["last_sale_date"], sale.sale_date)
            bucket["total_quantity"] += sale.quantity
            bucket["total_amount"] += sale.net_amount or sale.total_amount
            bucket["tickets"] += sale.tickets

        rows = sorted(
            buckets.values(),
            key=lambda item: (
                -item["occurrences"],
                -item["total_amount"],
                item["branch_name"],
                item["product_name"],
            ),
        )

        with output_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[
                    "branch_external_id",
                    "branch_name",
                    "product_external_id",
                    "sku",
                    "product_name",
                    "category",
                    "family",
                    "occurrences",
                    "first_sale_date",
                    "last_sale_date",
                    "total_quantity",
                    "total_amount",
                    "tickets",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        **row,
                        "first_sale_date": row["first_sale_date"].isoformat(),
                        "last_sale_date": row["last_sale_date"].isoformat(),
                        "total_quantity": row["total_quantity"],
                        "total_amount": row["total_amount"],
                    }
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"Reporte generado: {output_path} ({len(rows)} productos/sucursal sin match automático)"
            )
        )
