from datetime import date

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointBranch, PointProduct, PointSyncJob
from pos_bridge.services.daily_inventory_close_service import DailyInventoryCloseService
from pos_bridge.services.historical_inventory_capture import (
    HistoricalInventoryCaptureError,
    HistoricalPointInventoryClosingCapture,
)
from pos_bridge.services.monthly_product_balance_service import MonthlyPointProductBalanceService
from pos_bridge.services.point_http_client import PointHttpSessionClient


def select_default_branches(operational_date: date) -> list[PointBranch]:
    erp_branch_ids = [
        branch.id
        for branch in DailyInventoryCloseService()._target_branches(operational_date)
    ]
    candidates = PointBranch.objects.filter(erp_branch_id__in=erp_branch_ids).order_by("erp_branch_id", "id")
    selected = {}
    for branch in candidates:
        if not str(branch.external_id).isdigit():
            continue
        selected.setdefault(branch.erp_branch_id, branch)
    return sorted(selected.values(), key=lambda branch: int(branch.external_id))


def select_default_products() -> list[PointProduct]:
    latest_job = (
        PointSyncJob.objects.filter(
            job_type=PointSyncJob.JOB_TYPE_INVENTORY,
            status=PointSyncJob.STATUS_SUCCESS,
            snapshots__isnull=False,
        )
        .distinct()
        .order_by("-started_at", "-id")
        .first()
    )
    if latest_job is None:
        raise HistoricalInventoryCaptureError("No existe un inventario Point exitoso para definir el catálogo.")
    products = list(PointProduct.objects.filter(snapshots__sync_job=latest_job).distinct())
    matcher = MonthlyPointProductBalanceService()
    matcher._build_match_cache = {}
    return sorted(
        [
            product
            for product in products
            if str(product.external_id).isdigit()
            and matcher._match_recipe(code=product.sku, name=product.name) is not None
        ],
        key=lambda product: int(product.external_id),
    )


class Command(BaseCommand):
    help = "Reconstruye un cierre exacto de inventario Point desde su historial oficial."

    def add_arguments(self, parser):
        parser.add_argument("operational_date", help="Fecha operativa del cierre (AAAA-MM-DD).")
        parser.add_argument("--branch-external-id", action="append", dest="branch_ids")
        parser.add_argument("--product-external-id", action="append", dest="product_ids")

    def handle(self, *args, **options):
        try:
            operational_date = date.fromisoformat(options["operational_date"])
        except ValueError as exc:
            raise CommandError("La fecha debe tener formato AAAA-MM-DD.") from exc

        try:
            branches = self._branches(operational_date, options.get("branch_ids"))
            products = self._products(options.get("product_ids"))
            self.stdout.write(
                f"Consultando Point para {operational_date}: "
                f"{len(branches)} sucursales x {len(products)} productos."
            )
            client = PointHttpSessionClient(load_point_bridge_settings())
            result = HistoricalPointInventoryClosingCapture(client=client).capture(
                operational_date=operational_date,
                branches=branches,
                products=products,
            )
        except HistoricalInventoryCaptureError as exc:
            raise CommandError(str(exc)) from exc

        message = (
            f"Cierre #{result.closing.id}: {result.closing.status}; "
            f"resueltos={result.resolved_count}; pendientes={result.unresolved_count}."
        )
        if result.unresolved_count:
            raise CommandError(message)
        self.stdout.write(self.style.SUCCESS(message))

    @staticmethod
    def _branches(operational_date: date, external_ids: list[str] | None):
        if not external_ids:
            branches = select_default_branches(operational_date)
        else:
            branches = list(PointBranch.objects.filter(external_id__in=external_ids).order_by("id"))
        if not branches:
            raise HistoricalInventoryCaptureError("No se encontraron sucursales Point para el cierre.")
        return branches

    @staticmethod
    def _products(external_ids: list[str] | None):
        if not external_ids:
            products = select_default_products()
        else:
            products = list(PointProduct.objects.filter(external_id__in=external_ids).order_by("id"))
        if not products:
            raise HistoricalInventoryCaptureError("No se encontraron productos Point para el cierre.")
        return products
