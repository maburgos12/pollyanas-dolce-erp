from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.models import PointDailySale
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from pos_bridge.services.sync_service import PointSyncService


def _parse_date(value: str, *, label: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise CommandError(f"{label} debe tener formato YYYY-MM-DD.") from exc


class Command(BaseCommand):
    help = "Reconcilia ventas Point sin match creando recetas placeholder seguras y materializando staging existente."

    def add_arguments(self, parser):
        parser.add_argument("--start-date", default="2022-01-01", help="Fecha inicial YYYY-MM-DD.")
        parser.add_argument("--end-date", default="", help="Fecha final YYYY-MM-DD. Si se omite, usa 2025-12-31.")
        parser.add_argument(
            "--create-missing-recipes",
            action="store_true",
            help="Crea recetas placeholder para productos descriptivos aún sin match.",
        )
        parser.add_argument("--dry-run", action="store_true", help="No persiste cambios; solo reporta.")

    def handle(self, *args, **options):
        start_date = _parse_date((options.get("start_date") or "").strip(), label="start-date")
        end_raw = (options.get("end_date") or "").strip()
        end_date = _parse_date(end_raw, label="end-date") if end_raw else date(2025, 12, 31)
        if end_date < start_date:
            raise CommandError("end-date no puede ser menor a start-date.")

        create_missing = bool(options.get("create_missing_recipes"))
        dry_run = bool(options.get("dry_run"))
        matcher = PointSalesMatchingService()
        sync_service = PointSyncService()

        summary = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "create_missing_recipes": create_missing,
            "dry_run": dry_run,
            "products_seen": 0,
            "products_non_recipe_skipped": 0,
            "products_matched_existing": 0,
            "products_created": 0,
            "products_ambiguous_skipped": 0,
            "sales_rows_updated": 0,
            "historical_sales_created": 0,
            "historical_sales_updated": 0,
            "branch_unresolved_rows": 0,
        }

        ambiguous_samples: list[dict] = []
        unresolved_products: dict[int, object] = {}

        sales_qs = (
            PointDailySale.objects.filter(sale_date__gte=start_date, sale_date__lte=end_date, receta__isnull=True)
            .select_related("branch", "product", "branch__erp_branch")
            .order_by("product_id", "sale_date", "id")
        )

        for sale in sales_qs.iterator(chunk_size=1000):
            if sale.product_id in unresolved_products:
                continue
            unresolved_products[sale.product_id] = sale.product

        for product in unresolved_products.values():
            summary["products_seen"] += 1
            row = {
                "family": product.metadata.get("family", ""),
                "category": product.category,
                "name": product.name,
                "sku": product.sku,
            }
            if matcher.is_non_recipe_sale_row(row):
                summary["products_non_recipe_skipped"] += 1
                continue

            receta = matcher.resolve_receta(codigo_point=product.sku, point_name=product.name)
            if receta is not None:
                summary["products_matched_existing"] += 1
                continue

            if not create_missing:
                summary["products_ambiguous_skipped"] += 1
                if len(ambiguous_samples) < 20:
                    ambiguous_samples.append(
                        {
                            "sku": product.sku,
                            "name": product.name,
                            "category": product.category,
                            "family": product.metadata.get("family", ""),
                            "reason": "missing_recipe_creation_disabled",
                        }
                    )
                continue

            if not matcher.is_descriptive_product_name(point_name=product.name, family=product.metadata.get("family", "")):
                summary["products_ambiguous_skipped"] += 1
                if len(ambiguous_samples) < 20:
                    ambiguous_samples.append(
                        {
                            "sku": product.sku,
                            "name": product.name,
                            "category": product.category,
                            "family": product.metadata.get("family", ""),
                            "reason": "non_descriptive_point_name",
                        }
                    )
                continue

            receta = matcher.create_missing_product_recipe(
                codigo_point=product.sku,
                point_name=product.name,
                category=product.category,
                family=product.metadata.get("family", ""),
                dry_run=dry_run,
            )
            if receta is not None:
                summary["products_created"] += 1

        if dry_run:
            summary["ambiguous_samples"] = ambiguous_samples
            self.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
            return

        for sale in sales_qs.iterator(chunk_size=1000):
            row = {
                "family": sale.product.metadata.get("family", ""),
                "category": sale.product.category,
                "name": sale.product.name,
                "sku": sale.product.sku,
            }
            if matcher.is_non_recipe_sale_row(row):
                continue

            receta = matcher.resolve_receta(codigo_point=sale.product.sku, point_name=sale.product.name)
            if receta is None:
                continue

            changed = False
            if sale.receta_id != receta.id:
                sale.receta = receta
                sale.save(update_fields=["receta", "updated_at"])
                summary["sales_rows_updated"] += 1
                changed = True

            if sale.branch.erp_branch is None:
                summary["branch_unresolved_rows"] += 1
                continue

            created_history, updated_history = sync_service._upsert_sales_materialization(
                receta=receta,
                sucursal=sale.branch.erp_branch,
                sale_date=sale.sale_date,
                quantity=sale.quantity,
                tickets=sale.tickets,
                total_amount=sale.net_amount or sale.total_amount,
            )
            summary["historical_sales_created"] += created_history
            summary["historical_sales_updated"] += updated_history

        summary["ambiguous_samples"] = ambiguous_samples
        self.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
