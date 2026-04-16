from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from django.db import transaction
from django.db.models import Q

from core.cache_versions import bump_cache_scopes
from pos_bridge.models import PointDailySale
from reportes.analytics_service import mark_analytics_dirty_for_range
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import VentaHistorica
from ventas.models import VentaAutoritativaPoint


@dataclass
class SalesMaterializationRepairResult:
    scanned_rows: int
    recipe_rows_updated: int
    recipe_rows_cleared: int
    bridge_history_deleted: int
    bridge_history_created: int
    unresolved_rows: int
    non_recipe_rows: int
    branchless_rows: int
    mismatched_recipe_rows: int


class BridgeSalesMaterializationRepairService:
    SALES_HISTORY_SOURCE = "POINT_BRIDGE_SALES"

    def __init__(self, matcher: PointSalesMatchingService | None = None):
        self.matcher = matcher or PointSalesMatchingService()

    def _resolved_recipe_for_sale(self, sale: PointDailySale):
        payload = {
            "sku": sale.product.sku,
            "name": sale.product.name,
            "category": sale.product.category,
            "family": (sale.product.metadata or {}).get("family", ""),
        }
        if self.matcher.is_non_recipe_sale_row(payload):
            return "NON_RECIPE", None
        receta = self.matcher.resolve_receta(codigo_point=sale.product.sku, point_name=sale.product.name)
        if receta is None:
            return "UNRESOLVED", None
        return "RESOLVED", receta

    @transaction.atomic
    def repair(self, *, start_date: date, end_date: date, branch_filter: str | None = None) -> SalesMaterializationRepairResult:
        sales_qs = (
            PointDailySale.objects.select_related("branch__erp_branch", "product", "receta")
            .filter(sale_date__gte=start_date, sale_date__lte=end_date)
            .order_by("sale_date", "branch_id", "product_id", "id")
        )
        if branch_filter:
            sales_qs = sales_qs.filter(Q(branch__external_id=branch_filter) | Q(branch__name__iexact=branch_filter))
        target_erp_branch_ids = list(
            sales_qs.exclude(branch__erp_branch__isnull=True).values_list("branch__erp_branch_id", flat=True).distinct()
        )

        scanned_rows = 0
        recipe_rows_updated = 0
        recipe_rows_cleared = 0
        unresolved_rows = 0
        non_recipe_rows = 0
        branchless_rows = 0
        mismatched_recipe_rows = 0

        buckets: dict[tuple[int, int, date], dict[str, object]] = {}

        for sale in sales_qs:
            scanned_rows += 1
            status, intended_receta = self._resolved_recipe_for_sale(sale)
            current_receta = sale.receta

            if status == "NON_RECIPE":
                non_recipe_rows += 1
            elif status == "UNRESOLVED":
                unresolved_rows += 1

            if current_receta_id := getattr(current_receta, "id", None):
                if intended_receta is None or current_receta_id != intended_receta.id:
                    mismatched_recipe_rows += 1

            if (current_receta is None and intended_receta is not None) or (
                current_receta is not None and intended_receta is not None and current_receta.id != intended_receta.id
            ):
                sale.receta = intended_receta
                sale.save(update_fields=["receta", "updated_at"])
                recipe_rows_updated += 1
            elif current_receta is not None and intended_receta is None:
                sale.receta = None
                sale.save(update_fields=["receta", "updated_at"])
                recipe_rows_cleared += 1

            if intended_receta is None:
                continue
            if sale.branch.erp_branch_id is None:
                branchless_rows += 1
                continue

            key = (intended_receta.id, sale.branch.erp_branch_id, sale.sale_date)
            bucket = buckets.setdefault(
                key,
                {
                    "receta": intended_receta,
                    "sucursal": sale.branch.erp_branch,
                    "fecha": sale.sale_date,
                    "cantidad": Decimal("0"),
                    "tickets": 0,
                    "monto_total": Decimal("0"),
                },
            )
            bucket["cantidad"] = Decimal(str(bucket["cantidad"])) + Decimal(str(sale.quantity or 0))
            bucket["tickets"] = int(bucket["tickets"] or 0) + max(0, int(sale.tickets or 0))
            bucket["monto_total"] = Decimal(str(bucket["monto_total"])) + Decimal(str(sale.total_amount or 0))

        deleted, _ = VentaHistorica.objects.filter(
            fuente=self.SALES_HISTORY_SOURCE,
            fecha__gte=start_date,
            fecha__lte=end_date,
            **({"sucursal_id__in": target_erp_branch_ids} if target_erp_branch_ids else {}),
        ).delete()

        rows_to_create = [
            VentaHistorica(
                receta=payload["receta"],
                sucursal=payload["sucursal"],
                fecha=payload["fecha"],
                cantidad=payload["cantidad"],
                tickets=payload["tickets"],
                monto_total=payload["monto_total"],
                fuente=self.SALES_HISTORY_SOURCE,
            )
            for payload in buckets.values()
        ]
        authoritative_rows = (
            VentaAutoritativaPoint.objects.select_related("branch", "product")
            .filter(
                sale_date__gte=start_date,
                sale_date__lte=end_date,
                branch__isnull=False,
                product__isnull=False,
                **({"branch_id__in": target_erp_branch_ids} if target_erp_branch_ids else {}),
            )
            .order_by("sale_date", "branch_id", "product_id", "id")
        )
        authoritative_keys: set[tuple[int, int, date]] = set()
        for row in authoritative_rows:
            if row.branch_id is None or row.product_id is None:
                continue
            key = (row.product_id, row.branch_id, row.sale_date)
            authoritative_keys.add(key)
            rows_to_create = [item for item in rows_to_create if not (
                item.receta_id == row.product_id and item.sucursal_id == row.branch_id and item.fecha == row.sale_date
            )]
            rows_to_create.append(
                VentaHistorica(
                    receta=row.product,
                    sucursal=row.branch,
                    fecha=row.sale_date,
                    cantidad=Decimal(str(row.quantity or 0)),
                    tickets=0,
                    monto_total=Decimal(str(row.total_amount or 0)),
                    fuente=self.SALES_HISTORY_SOURCE,
                )
        )
        if rows_to_create:
            VentaHistorica.objects.bulk_create(rows_to_create, batch_size=500)
            bump_cache_scopes("ventas", "dashboard")
            mark_analytics_dirty_for_range(
                start_date=min(row.fecha for row in rows_to_create),
                end_date=max(row.fecha for row in rows_to_create),
                include_sales=True,
                include_production=True,
                include_forecast=True,
                reason="sales_materialization_repair",
            )

        return SalesMaterializationRepairResult(
            scanned_rows=scanned_rows,
            recipe_rows_updated=recipe_rows_updated,
            recipe_rows_cleared=recipe_rows_cleared,
            bridge_history_deleted=deleted,
            bridge_history_created=len(rows_to_create),
            unresolved_rows=unresolved_rows,
            non_recipe_rows=non_recipe_rows,
            branchless_rows=branchless_rows,
            mismatched_recipe_rows=mismatched_recipe_rows,
        )
