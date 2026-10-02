from __future__ import annotations

from copy import copy
from calendar import monthrange
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import MappingProxyType

from django.db.models import BooleanField, Case, Q, Value, When
from django.utils.dateparse import parse_datetime
from django.utils import timezone

from pos_bridge.models import (
    PointBranch,
    PointConversionLine,
    PointHistoricalInventoryClosing,
    PointHistoricalInventoryClosingLine,
    PointOpenTransferSnapshot,
    PointProduct,
    PointProductionLine,
    PointSyncJob,
    PointTransferLine,
    PointWasteLine,
)
from pos_bridge.models.product import _normalize_name, inventory_consumption_filter
from pos_bridge.services.monthly_product_balance_service import (
    MonthlyPointProductBalanceService,
)
from pos_bridge.services.open_transfer_sync_service import (
    OPEN_TRANSFER_MANIFEST_KEY,
    _payload_sha256,
    build_open_transfer_manifest,
    canonical_open_transfer_payload,
    open_transfer_close_window,
)
from pos_bridge.services.sales_branch_indicator_service import (
    point_branch_canonical_sort_key,
)
from recetas.models import Receta, RecetaEquivalencia, RecetaPresentacionDerivada
from ventas.services.sales_canonical_source import official_point_sales_rows_for_range

ZERO = Decimal("0")
TRACE_SOURCE_NAMES = (
    "opening",
    "closing",
    "sales",
    "production",
    "waste",
    "transfers",
    "conversions",
    "transfer_in",
    "transfer_out",
    "open_transfer_snapshot_in",
    "open_transfer_snapshot_out",
    "conversion_in",
    "conversion_out",
    "adjustments",
)
TRACE_VIEW_METADATA_NAMES = (
    "conversion_in_impacts",
    "conversion_out_impacts",
)


@dataclass(frozen=True)
class TraceSourceIssue:
    code: str
    message: str
    branch_id: int | None = None
    product_id: int | None = None
    source_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class BranchProductBalance:
    branch: PointBranch
    product: PointProduct
    opening: Decimal
    production: Decimal
    sales: Decimal
    waste: Decimal
    transfer_in: Decimal
    transfer_out: Decimal
    conversion_in: Decimal
    conversion_out: Decimal
    identified_adjustment: Decimal
    expected_closing: Decimal
    point_closing: Decimal
    difference: Decimal
    source_trace: Mapping[str, object]
    issues: tuple[TraceSourceIssue, ...]


@dataclass(frozen=True)
class BranchInventoryTraceability:
    month: date
    lines: tuple[BranchProductBalance, ...]
    global_issues: tuple[TraceSourceIssue, ...]
    company_difference: Decimal
    exception_count: int
    source_complete: bool


def canonical_point_branch_identity() -> tuple[
    dict[int, int], dict[int, PointBranch]
]:
    """Map Point aliases to one branch per linked ERP branch."""
    grouped: dict[int, list[PointBranch]] = {}
    branches = list(PointBranch.objects.all().order_by("id"))
    aliases = {branch.id: branch.id for branch in branches}
    branch_objects = {branch.id: branch for branch in branches}
    for branch in branches:
        if branch.erp_branch_id is not None:
            grouped.setdefault(branch.erp_branch_id, []).append(branch)
    for linked_branches in grouped.values():
        canonical = max(linked_branches, key=point_branch_canonical_sort_key)
        aliases.update(
            {branch.id: canonical.id for branch in linked_branches}
        )
    return aliases, branch_objects


class BranchInventoryTraceabilityService:
    def build(self, month: date) -> BranchInventoryTraceability:
        month_start = month.replace(day=1)
        opening_date = month_start - timedelta(days=1)
        closing_date = date(
            month_start.year,
            month_start.month,
            monthrange(month_start.year, month_start.month)[1],
        )

        opening_closing = self._select_closing(opening_date)
        point_closing = self._select_closing(closing_date)
        selected_closings = {
            opening_date: opening_closing,
            closing_date: point_closing,
        }
        missing_dates = [
            required_date
            for required_date, selected in selected_closings.items()
            if selected is None
        ]
        if missing_dates:
            issues = tuple(
                TraceSourceIssue(
                    code="SOURCE_INCOMPLETE",
                    message=f"Falta cierre Point verificado para {missing_date.isoformat()}.",
                )
                for missing_date in missing_dates
            )
            return BranchInventoryTraceability(
                month=month_start,
                lines=(),
                global_issues=issues,
                company_difference=ZERO,
                exception_count=0,
                source_complete=False,
            )

        opening = self._load_closing(opening_closing)
        closing = self._load_closing(point_closing)
        incomplete_manifests = [
            (required_date, selected, balances)
            for required_date, selected, balances in (
                (opening_date, opening_closing, opening),
                (closing_date, point_closing, closing),
            )
            if not self._coverage_complete(selected, balances)
        ]
        if incomplete_manifests:
            return BranchInventoryTraceability(
                month=month_start,
                lines=(),
                global_issues=tuple(
                    TraceSourceIssue(
                        code="SOURCE_INCOMPLETE",
                        message=(
                            "El cierre Point verificado tiene cobertura incompleta para "
                            f"{required_date.isoformat()}."
                        ),
                        source_ids=(selected.id,),
                    )
                    for required_date, selected, _balances in incomplete_manifests
                ),
                company_difference=ZERO,
                exception_count=0,
                source_complete=False,
            )

        products = list(PointProduct.objects.annotate(
            is_consumption=Case(When(inventory_consumption_filter(), then=Value(True)),
                default=Value(False), output_field=BooleanField()),
        ).order_by("id"))
        product_indexes = self._build_product_indexes(products)
        (
            sales,
            production,
            waste,
            transfer_in,
            transfer_out,
            open_transfer_snapshot_in,
            open_transfer_snapshot_out,
            conversion_in,
            conversion_out,
            conversion_in_impacts,
            conversion_out_impacts,
            movement_issues,
            authority_issues,
        ) = self._load_direct_movements(
            month_start=month_start,
            closing_date=closing_date,
            product_indexes=product_indexes,
        )
        if authority_issues:
            return BranchInventoryTraceability(
                month=month_start,
                lines=(),
                global_issues=tuple(authority_issues) + tuple(movement_issues),
                company_difference=ZERO,
                exception_count=0,
                source_complete=False,
            )
        branch_id_aliases, branch_objects = self.canonical_branch_identity()
        opening = self._canonicalize_balance_map(opening, branch_id_aliases)
        closing = self._canonicalize_balance_map(closing, branch_id_aliases)
        sales = self._canonicalize_balance_map(sales, branch_id_aliases)
        production = self._canonicalize_balance_map(production, branch_id_aliases)
        waste = self._canonicalize_balance_map(waste, branch_id_aliases)
        transfer_in = self._canonicalize_balance_map(transfer_in, branch_id_aliases)
        transfer_out = self._canonicalize_balance_map(transfer_out, branch_id_aliases)
        open_transfer_snapshot_in = self._canonicalize_balance_map(
            open_transfer_snapshot_in, branch_id_aliases
        )
        open_transfer_snapshot_out = self._canonicalize_balance_map(
            open_transfer_snapshot_out, branch_id_aliases
        )
        conversion_in = self._canonicalize_balance_map(
            conversion_in, branch_id_aliases
        )
        conversion_out = self._canonicalize_balance_map(
            conversion_out, branch_id_aliases
        )
        conversion_in_impacts = self._canonicalize_impact_map(
            conversion_in_impacts, branch_id_aliases
        )
        conversion_out_impacts = self._canonicalize_impact_map(
            conversion_out_impacts, branch_id_aliases
        )
        movement_issues = tuple(
            replace(
                issue,
                branch_id=branch_id_aliases.get(issue.branch_id, issue.branch_id),
            )
            if issue.branch_id is not None
            else issue
            for issue in movement_issues
        )
        movement_sources = (
            sales,
            production,
            waste,
            transfer_in,
            transfer_out,
            open_transfer_snapshot_in,
            open_transfer_snapshot_out,
            conversion_in,
            conversion_out,
        )
        consumption_ids = {product.id for product in products if product.is_consumption}
        for source in (opening, closing, *movement_sources):
            for key in list(source):
                if key[1] in consumption_ids:
                    del source[key]
        movement_issues = [issue for issue in movement_issues if issue.product_id not in consumption_ids]
        movement_keys = set().union(*(source.keys() for source in movement_sources))
        uncovered_movement_issues = []
        for branch_id, product_id in sorted(movement_keys):
            missing_manifests = []
            if (branch_id, product_id) not in opening:
                missing_manifests.append("apertura")
            if (branch_id, product_id) not in closing:
                missing_manifests.append("cierre")
            if not missing_manifests:
                continue
            source_ids = tuple(
                dict.fromkeys(
                    source_id
                    for source in movement_sources
                    for source_id in source.get(
                        (branch_id, product_id), (ZERO, ())
                    )[1]
                )
            )
            uncovered_movement_issues.append(
                TraceSourceIssue(
                    code="SOURCE_INCOMPLETE",
                    message=(
                        "El movimiento no tiene evidencia de "
                        f"{' y '.join(missing_manifests)} para la misma "
                        "ubicación y producto."
                    ),
                    branch_id=branch_id,
                    product_id=product_id,
                    source_ids=source_ids,
                )
            )
        movement_issues = [*movement_issues, *uncovered_movement_issues]
        keys = sorted(
            opening.keys()
            | closing.keys()
            | sales.keys()
            | production.keys()
            | waste.keys()
            | transfer_in.keys()
            | transfer_out.keys()
            | open_transfer_snapshot_in.keys()
            | open_transfer_snapshot_out.keys()
            | conversion_in.keys()
            | conversion_out.keys()
        )
        branches = {
            branch_id: branch_objects[branch_id]
            for branch_id, _product_id in keys
        }
        products_by_id = {product.id: product for product in products}

        global_issues = []
        issues_by_key: dict[tuple[int, int], list[TraceSourceIssue]] = {}
        for issue in movement_issues:
            if issue.branch_id is not None and issue.product_id is not None:
                issues_by_key.setdefault(
                    (issue.branch_id, issue.product_id), []
                ).append(issue)
            else:
                global_issues.append(issue)

        lines = []
        for branch_id, product_id in keys:
            opening_stock, opening_ids = opening.get(
                (branch_id, product_id), (ZERO, ())
            )
            point_closing, closing_ids = closing.get(
                (branch_id, product_id), (ZERO, ())
            )
            production_quantity, production_ids = production.get(
                (branch_id, product_id), (ZERO, ())
            )
            sales_quantity, sales_ids = sales.get((branch_id, product_id), (ZERO, ()))
            waste_quantity, waste_ids = waste.get((branch_id, product_id), (ZERO, ()))
            transfer_in_quantity, transfer_in_ids = transfer_in.get(
                (branch_id, product_id), (ZERO, ())
            )
            transfer_out_quantity, transfer_out_ids = transfer_out.get(
                (branch_id, product_id), (ZERO, ())
            )
            _snapshot_in_quantity, snapshot_in_ids = open_transfer_snapshot_in.get(
                (branch_id, product_id), (ZERO, ())
            )
            _snapshot_out_quantity, snapshot_out_ids = open_transfer_snapshot_out.get(
                (branch_id, product_id), (ZERO, ())
            )
            conversion_in_quantity, conversion_in_ids = conversion_in.get(
                (branch_id, product_id), (ZERO, ())
            )
            conversion_out_quantity, conversion_out_ids = conversion_out.get(
                (branch_id, product_id), (ZERO, ())
            )
            expected_closing = (
                opening_stock
                + production_quantity
                + transfer_in_quantity
                + conversion_in_quantity
                - sales_quantity
                - waste_quantity
                - transfer_out_quantity
                - conversion_out_quantity
            )
            difference = point_closing - expected_closing
            trace_values = {
                "opening": opening_ids,
                "closing": closing_ids,
                "sales": sales_ids,
                "production": production_ids,
                "waste": waste_ids,
                "transfers": tuple(
                    dict.fromkeys((*transfer_in_ids, *transfer_out_ids))
                ),
                "conversions": tuple(
                    dict.fromkeys((*conversion_in_ids, *conversion_out_ids))
                ),
                "transfer_in": transfer_in_ids,
                "transfer_out": transfer_out_ids,
                "open_transfer_snapshot_in": snapshot_in_ids,
                "open_transfer_snapshot_out": snapshot_out_ids,
                "conversion_in": conversion_in_ids,
                "conversion_out": conversion_out_ids,
                "adjustments": (),
                "conversion_in_impacts": conversion_in_impacts.get(
                    (branch_id, product_id), {}
                ),
                "conversion_out_impacts": conversion_out_impacts.get(
                    (branch_id, product_id), {}
                ),
            }
            lines.append(
                BranchProductBalance(
                    branch=branches[branch_id],
                    product=products_by_id[product_id],
                    opening=opening_stock,
                    production=production_quantity,
                    sales=sales_quantity,
                    waste=waste_quantity,
                    transfer_in=transfer_in_quantity,
                    transfer_out=transfer_out_quantity,
                    conversion_in=conversion_in_quantity,
                    conversion_out=conversion_out_quantity,
                    identified_adjustment=ZERO,
                    expected_closing=expected_closing,
                    point_closing=point_closing,
                    difference=difference,
                    source_trace=MappingProxyType(
                        {
                            source_name: (
                                MappingProxyType(dict(trace_values[source_name]))
                                if source_name in TRACE_VIEW_METADATA_NAMES
                                else tuple(trace_values[source_name])
                            )
                            for source_name in (
                                *TRACE_SOURCE_NAMES,
                                *TRACE_VIEW_METADATA_NAMES,
                            )
                        }
                    ),
                    issues=tuple(issues_by_key.get((branch_id, product_id), ())),
                )
            )

        frozen_lines = tuple(lines)
        return BranchInventoryTraceability(
            month=month_start,
            lines=frozen_lines,
            global_issues=tuple(global_issues),
            company_difference=sum((line.difference for line in frozen_lines), ZERO),
            exception_count=sum(line.difference != ZERO for line in frozen_lines),
            source_complete=True,
        )

    @staticmethod
    def _build_product_indexes(
        products: list[PointProduct],
    ) -> Mapping[str, Mapping[object, tuple[int, ...] | int]]:
        by_id = {product.id: product.id for product in products}
        by_external_id = {
            product.external_id.strip(): product.id
            for product in products
            if product.external_id.strip()
        }
        sku_candidates: dict[str, list[int]] = {}
        name_candidates: dict[str, list[int]] = {}
        for product in products:
            sku = product.sku.strip()
            if sku:
                sku_candidates.setdefault(sku, []).append(product.id)
            normalized_name = product.normalized_name.strip()
            if normalized_name:
                name_candidates.setdefault(normalized_name, []).append(product.id)
        return MappingProxyType(
            {
                "id": MappingProxyType(by_id),
                "external_id": MappingProxyType(by_external_id),
                "sku": MappingProxyType(
                    {key: tuple(value) for key, value in sku_candidates.items()}
                ),
                "normalized_name": MappingProxyType(
                    {key: tuple(value) for key, value in name_candidates.items()}
                ),
            }
        )

    def _load_direct_movements(self, *, month_start, closing_date, product_indexes):
        lower_bound, upper_bound = self._month_datetime_bounds(month_start)
        sales_rows = list(
            official_point_sales_rows_for_range(
                start_date=month_start, end_date=closing_date
            )
            .select_related("branch", "product", "sync_job")
            .only(
                "id",
                "branch_id",
                "branch__external_id",
                "branch__erp_branch_id",
                "product_id",
                "receta_id",
                "sync_job_id",
                "sale_date",
                "quantity",
            )
            .order_by("id")
        )
        production_rows = list(
            PointProductionLine.objects.filter(
                production_date__gte=month_start,
                production_date__lte=closing_date,
            )
            .select_related("branch", "sync_job")
            .only(
                "id",
                "branch_id",
                "item_code",
                "item_name",
                "produced_quantity",
                "receta_id",
                "sync_job_id",
                "is_insumo",
            )
            .order_by("id")
        )
        waste_rows = list(
            PointWasteLine.objects.filter(
                movement_at__gte=lower_bound,
                movement_at__lt=upper_bound,
            )
            .select_related("branch", "sync_job")
            .only(
                "id",
                "branch_id",
                "item_code",
                "item_name",
                "quantity",
                "receta_id",
                "insumo_id",
                "sync_job_id",
            )
            .order_by("id")
        )
        transfer_rows = list(
            PointTransferLine.objects.filter(
                Q(sent_at__gte=lower_bound, sent_at__lt=upper_bound)
                | Q(received_at__gte=lower_bound, received_at__lt=upper_bound)
                | Q(
                    sent_at__isnull=True,
                    is_finalized=True,
                    registered_at__gte=lower_bound,
                    registered_at__lt=upper_bound,
                )
            )
            .only(
                "id",
                "origin_branch_id",
                "destination_branch_id",
                "sync_job_id",
                "transfer_external_id",
                "detail_external_id",
                "registered_at",
                "sent_at",
                "received_at",
                "item_code",
                "item_name",
                "sent_quantity",
                "received_quantity",
                "is_insumo",
                "is_received",
                "is_cancelled",
                "is_finalized",
                "is_open",
                "is_current_snapshot",
            )
            .order_by("id")
        )
        conversion_rows = list(
            PointConversionLine.objects.filter(
                movement_at__gte=lower_bound,
                movement_at__lt=upper_bound,
            )
            .only(
                "id",
                "branch_id",
                "sync_job_id",
                "item_code",
                "item_name",
                "quantity",
                "source_item_code",
                "source_item_name",
            )
            .order_by("id")
        )

        authority_service = MonthlyPointProductBalanceService()
        sales_authoritative, sales_authority, _sales_authority_movements = (
            authority_service._validate_official_daily_sales_authority(
                month_start=month_start,
                month_end=closing_date,
                official_daily_row_count=len(sales_rows),
                daily_rows=sales_rows,
            )
        )
        production_authority = authority_service._validate_month_movement_job(
            family="production",
            month_start=month_start,
            month_end=closing_date,
            row_job_ids=[row.sync_job_id for row in production_rows],
        )
        waste_authority = authority_service._validate_month_movement_job(
            family="waste",
            month_start=month_start,
            month_end=closing_date,
            row_job_ids=[row.sync_job_id for row in waste_rows],
        )
        transfer_authority = self._validate_transfer_authority(
            month_start=month_start,
            month_end=closing_date,
            rows=transfer_rows,
        )
        conversion_authority = authority_service._validate_month_movement_job(
            family="conversions",
            month_start=month_start,
            month_end=closing_date,
            row_job_ids=[row.sync_job_id for row in conversion_rows],
        )
        authorities = {
            "sales": {
                **sales_authority,
                "authoritative": sales_authoritative,
                "selected_sync_job_ids": sales_authority.get(
                    "selected_row_job_ids", ()
                ),
            },
            "production": production_authority,
            "waste": waste_authority,
            "transfers": transfer_authority,
            "conversions": conversion_authority,
        }
        authority_issues = [
            TraceSourceIssue(
                code="SOURCE_INCOMPLETE",
                message=(
                    f"La fuente requerida {family} no es autoritativa: "
                    f"{', '.join(authority.get('authority_issues') or ())}."
                ),
                source_ids=tuple(
                    authority.get("source_ids")
                    or authority.get("selected_sync_job_ids")
                    or ()
                ),
            )
            for family, authority in authorities.items()
            if not authority.get("authoritative")
        ]

        sales: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        production: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        waste: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        transfer_in: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        transfer_out: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        open_transfer_snapshot_in: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        open_transfer_snapshot_out: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        conversion_in: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        conversion_out: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        conversion_in_impacts: dict[tuple[int, int], dict[int, Decimal]] = {}
        conversion_out_impacts: dict[tuple[int, int], dict[int, Decimal]] = {}
        issues: list[TraceSourceIssue] = []

        for row in sales_rows:
            product_id, issue_code = self._resolve_product(row, product_indexes)
            self._record_direct_row(
                balances=sales,
                issues=issues,
                source_name="sales",
                row=row,
                branch_id=row.branch_id,
                product_id=product_id,
                issue_code=issue_code,
                quantity=row.quantity,
            )
        for row in production_rows:
            if row.is_insumo:
                continue
            product_id, issue_code = self._resolve_product(row, product_indexes)
            self._record_direct_row(
                balances=production,
                issues=issues,
                source_name="production",
                row=row,
                branch_id=row.branch_id,
                product_id=product_id,
                issue_code=issue_code,
                quantity=row.produced_quantity,
            )
        for row in waste_rows:
            if row.receta_id is None and row.insumo_id is not None:
                continue
            product_id, issue_code = self._resolve_product(row, product_indexes)
            if (
                row.receta_id is not None
                and row.insumo_id is not None
                and product_id is None
            ):
                continue
            self._record_direct_row(
                balances=waste,
                issues=issues,
                source_name="waste",
                row=row,
                branch_id=row.branch_id,
                product_id=product_id,
                issue_code=issue_code,
                quantity=row.quantity,
            )
        self._apply_transfers(
            rows=transfer_authority.get("mutable_rows", transfer_rows),
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            product_indexes=product_indexes,
            transfer_in=transfer_in,
            transfer_out=transfer_out,
            issues=issues,
        )
        snapshot_transfer_in: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        snapshot_transfer_out: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        self._apply_transfers(
            rows=transfer_authority.get("snapshot_members", ()),
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            product_indexes=product_indexes,
            transfer_in=snapshot_transfer_in,
            transfer_out=snapshot_transfer_out,
            issues=issues,
        )
        self._merge_balance_maps(transfer_in, snapshot_transfer_in)
        self._merge_balance_maps(transfer_out, snapshot_transfer_out)
        open_transfer_snapshot_in.update(snapshot_transfer_in)
        open_transfer_snapshot_out.update(snapshot_transfer_out)
        self._apply_conversions(
            rows=conversion_rows,
            product_indexes=product_indexes,
            conversion_in=conversion_in,
            conversion_out=conversion_out,
            conversion_in_impacts=conversion_in_impacts,
            conversion_out_impacts=conversion_out_impacts,
            issues=issues,
        )
        return (
            sales,
            production,
            waste,
            transfer_in,
            transfer_out,
            open_transfer_snapshot_in,
            open_transfer_snapshot_out,
            conversion_in,
            conversion_out,
            conversion_in_impacts,
            conversion_out_impacts,
            tuple(issues),
            tuple(authority_issues),
        )

    @classmethod
    def _validate_transfer_authority(cls, *, month_start, month_end, rows):
        jobs = list(
            PointSyncJob.objects.filter(
                job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
                parameters__start_date=month_start.isoformat(),
                parameters__end_date=month_end.isoformat(),
            )
            .only(
                "id",
                "job_type",
                "status",
                "started_at",
                "finished_at",
                "parameters",
                "result_summary",
            )
            .order_by("-started_at", "-id")
        )
        if not jobs:
            return {
                "authoritative": False,
                "selected_sync_job_ids": (),
                "authority_issues": ("TRANSFER_SYNC_JOB_MISSING",),
            }
        unrestricted = [
            job
            for job in jobs
            if not str((job.parameters or {}).get("branch_filter") or "").strip()
        ]
        selected = unrestricted[0] if unrestricted else jobs[0]
        exact_job_issues = cls._transfer_job_contract_issues(
            selected,
            prefix="TRANSFER_SYNC",
        )
        issues = list(exact_job_issues)
        open_jobs = list(
            PointSyncJob.objects.filter(
                job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
                parameters__mode="open_transfers",
                parameters__fecha=month_end.isoformat(),
            )
            .select_related("open_transfer_snapshot")
            .prefetch_related("open_transfer_snapshot__members")
            .only(
                "id",
                "job_type",
                "status",
                "started_at",
                "finished_at",
                "parameters",
                "result_summary",
            )
            .order_by("-started_at", "-id")
        )
        unrestricted_open_jobs = [
            job
            for job in open_jobs
            if not str((job.parameters or {}).get("branch_filter") or "").strip()
        ]
        evaluated_open_jobs = [
            (
                job,
                cls._open_transfer_job_contract_issues(
                    job,
                    operational_date=month_end,
                ),
            )
            for job in unrestricted_open_jobs
        ]
        valid_open_job = next(
            (job for job, job_issues in evaluated_open_jobs if not job_issues),
            None,
        )
        legacy_open_job = next(
            (
                job
                for job in unrestricted_open_jobs
                if cls._uses_legacy_open_transfer_contract(job)
                and not cls._legacy_open_transfer_job_contract_issues(job)
            ),
            None,
        )
        legacy_open_reconstruction = False
        if valid_open_job is not None:
            selected_open_job = valid_open_job
            open_job_issues = []
        elif legacy_open_job is not None:
            selected_open_job = legacy_open_job
            open_job_issues = []
            legacy_open_reconstruction = True
        elif evaluated_open_jobs:
            selected_open_job, open_job_issues = evaluated_open_jobs[0]
        elif open_jobs:
            selected_open_job = open_jobs[0]
            open_job_issues = cls._open_transfer_job_contract_issues(
                selected_open_job,
                operational_date=month_end,
            )
        else:
            selected_open_job = None
            open_job_issues = ["OPEN_TRANSFER_SYNC_JOB_MISSING"]
        issues.extend(open_job_issues)
        snapshot_members = (
            list(selected_open_job.open_transfer_snapshot.members.all())
            if selected_open_job is not None
            and not open_job_issues
            and not legacy_open_reconstruction
            else []
        )
        frozen_references = {
            (row.transfer_external_id, row.detail_external_id)
            for row in snapshot_members
        }
        mutable_rows = [
            row
            for row in rows
            if (row.transfer_external_id, row.detail_external_id)
            not in frozen_references
        ]
        if legacy_open_reconstruction:
            mutable_rows, legacy_issues = cls._reconstruct_legacy_open_rows(
                mutable_rows,
                operational_date=month_end,
            )
            issues.extend(legacy_issues)
        relevant_rows = [
            row
            for row in mutable_rows
            if not row.is_cancelled
            and row.is_current_snapshot
            and not row.is_insumo
        ]
        unbound_row_ids = [row.id for row in relevant_rows if row.sync_job_id is None]
        if unbound_row_ids:
            issues.append("TRANSFER_ROW_PROVENANCE_MISSING")

        provenance_job_ids = {
            row.sync_job_id for row in relevant_rows if row.sync_job_id is not None
        }
        authority_jobs = [*jobs, *open_jobs]
        known_jobs = {
            job.id: job for job in authority_jobs if job.id in provenance_job_ids
        }
        missing_job_ids = provenance_job_ids - known_jobs.keys()
        if missing_job_ids:
            known_jobs.update(
                PointSyncJob.objects.filter(id__in=missing_job_ids)
                .only(
                    "id",
                    "job_type",
                    "status",
                    "started_at",
                    "finished_at",
                    "parameters",
                    "result_summary",
                )
                .in_bulk()
            )
        invalid_provenance_row_ids = []
        for row in relevant_rows:
            if row.sync_job_id is None:
                continue
            provenance_job = known_jobs.get(row.sync_job_id)
            if provenance_job is None:
                provenance_issues = ["TRANSFER_ROW_PROVENANCE_JOB_MISSING"]
            elif (provenance_job.parameters or {}).get("mode") == "open_transfers":
                if cls._uses_legacy_open_transfer_contract(provenance_job):
                    provenance_issues = cls._legacy_open_transfer_job_contract_issues(
                        provenance_job
                    )
                else:
                    provenance_issues = cls._open_transfer_job_contract_issues(
                        provenance_job,
                        operational_date=month_end,
                    )
            else:
                provenance_issues = cls._transfer_job_contract_issues(
                    provenance_job,
                    prefix="TRANSFER_ROW_PROVENANCE",
                )
            if (
                provenance_job is not None
                and provenance_job.job_type != PointSyncJob.JOB_TYPE_TRANSFERS
            ):
                provenance_issues.append("TRANSFER_ROW_PROVENANCE_JOB_TYPE_INVALID")
            if provenance_job is not None:
                provenance_issues.extend(cls._transfer_row_event_issues(row))
            if provenance_issues:
                invalid_provenance_row_ids.append(row.id)
                issues.extend(provenance_issues)
        source_ids = tuple(
            dict.fromkeys(
                (
                    *((selected.id,) if exact_job_issues else ()),
                    *(
                        (selected_open_job.id,)
                        if open_job_issues and selected_open_job
                        else ()
                    ),
                    *unbound_row_ids,
                    *invalid_provenance_row_ids,
                )
            )
        )
        return {
            "authoritative": not issues,
            "selected_sync_job_ids": (selected.id,),
            "authority_issues": tuple(dict.fromkeys(issues)),
            "source_ids": source_ids,
            "mutable_rows": tuple(mutable_rows),
            "snapshot_members": tuple(snapshot_members),
        }

    @staticmethod
    def _transfer_job_contract_issues(job, *, prefix):
        issues = []
        if job.status == PointSyncJob.STATUS_FAILED:
            issues.append(f"{prefix}_JOB_FAILED")
        elif job.status == PointSyncJob.STATUS_PARTIAL:
            issues.append(f"{prefix}_JOB_PARTIAL")
        elif job.status != PointSyncJob.STATUS_SUCCESS:
            issues.append(f"{prefix}_JOB_INCOMPLETE")
        if str((job.parameters or {}).get("branch_filter") or "").strip():
            issues.append(f"{prefix}_JOB_RESTRICTED")

        summary = job.result_summary or {}
        required_keys = (
            "transfer_lines_seen",
            "transfer_lines_created",
            "transfer_lines_updated",
        )
        if any(key not in summary for key in required_keys):
            issues.append(f"{prefix}_CONTRACT_INCOMPLETE")
            return issues
        try:
            seen, created, updated = (int(summary[key]) for key in required_keys)
        except (TypeError, ValueError):
            issues.append(f"{prefix}_CONTRACT_INCOMPLETE")
            return issues
        if min(seen, created, updated) < 0 or seen != created + updated:
            issues.append(f"{prefix}_COUNT_MISMATCH")
        return issues

    @classmethod
    def _legacy_open_transfer_job_contract_issues(cls, job):
        issues = cls._transfer_job_contract_issues(
            job,
            prefix="OPEN_TRANSFER_SYNC",
        )
        summary = job.result_summary or {}
        try:
            seen = int(summary["transfer_lines_seen"])
            new_rows = int(summary["lineas_nuevas"])
            updated_rows = int(summary["lineas_actualizadas"])
        except (KeyError, TypeError, ValueError):
            issues.append("OPEN_TRANSFER_SYNC_CONTRACT_INCOMPLETE")
            return list(dict.fromkeys(issues))
        if min(seen, new_rows, updated_rows) < 0 or seen != new_rows + updated_rows:
            issues.append("OPEN_TRANSFER_SYNC_COUNT_MISMATCH")
        return list(dict.fromkeys(issues))

    @staticmethod
    def _uses_legacy_open_transfer_contract(job):
        if OPEN_TRANSFER_MANIFEST_KEY in (job.result_summary or {}):
            return False
        try:
            job.open_transfer_snapshot
        except PointOpenTransferSnapshot.DoesNotExist:
            return True
        return False

    @staticmethod
    def _reconstruct_legacy_open_rows(rows, *, operational_date):
        cutoff, _window_end = open_transfer_close_window(operational_date)
        reconstructed = []
        issues = []
        for row in rows:
            was_open_at_close = (
                row.sent_at is not None
                and row.sent_at < cutoff
                and (row.received_at is None or row.received_at > cutoff)
            )
            if not was_open_at_close:
                reconstructed.append(row)
                continue
            if row.is_cancelled:
                issues.append("LEGACY_OPEN_TRANSFER_STATUS_AMBIGUOUS")
                continue
            historical_row = copy(row)
            historical_row.is_received = False
            historical_row.received_at = None
            historical_row.received_quantity = ZERO
            historical_row.is_open = True
            reconstructed.append(historical_row)
        return reconstructed, issues

    @classmethod
    def _open_transfer_job_contract_issues(cls, job, *, operational_date):
        issues = cls._transfer_job_contract_issues(
            job,
            prefix="OPEN_TRANSFER_SYNC",
        )
        summary = job.result_summary or {}
        extra_keys = ("lineas_nuevas", "lineas_actualizadas")
        if any(key not in summary for key in extra_keys):
            issues.append("OPEN_TRANSFER_SYNC_CONTRACT_INCOMPLETE")
            return list(dict.fromkeys(issues))
        try:
            new_rows, updated_rows = (int(summary[key]) for key in extra_keys)
            seen = int(summary["transfer_lines_seen"])
        except (KeyError, TypeError, ValueError):
            issues.append("OPEN_TRANSFER_SYNC_CONTRACT_INCOMPLETE")
            return list(dict.fromkeys(issues))
        if (
            min(new_rows, updated_rows) < 0
            or seen != new_rows + updated_rows
        ):
            issues.append("OPEN_TRANSFER_SYNC_COUNT_MISMATCH")

        manifest = summary.get(OPEN_TRANSFER_MANIFEST_KEY)
        if not isinstance(manifest, dict):
            issues.append("OPEN_TRANSFER_SYNC_MANIFEST_INCOMPLETE")
            return list(dict.fromkeys(issues))
        try:
            manifest_count = int(manifest["row_count"])
            manifest_hash = str(manifest["sha256"])
            manifest_snapshot_id = int(manifest["snapshot_id"])
            manifest_date = date.fromisoformat(str(manifest["operational_date"]))
            captured_at = parse_datetime(str(manifest["captured_at"]))
        except (KeyError, TypeError, ValueError):
            issues.append("OPEN_TRANSFER_SYNC_MANIFEST_INCOMPLETE")
            return list(dict.fromkeys(issues))
        if (
            manifest_count < 0
            or manifest_count != seen
            or len(manifest_hash) != 64
            or any(character not in "0123456789abcdef" for character in manifest_hash)
            or manifest_date != operational_date
            or captured_at is None
            or timezone.is_naive(captured_at)
        ):
            issues.append("OPEN_TRANSFER_SYNC_MANIFEST_INVALID")

        try:
            snapshot = job.open_transfer_snapshot
        except PointOpenTransferSnapshot.DoesNotExist:
            snapshot = None
        if snapshot is None:
            issues.append("OPEN_TRANSFER_SYNC_SNAPSHOT_MISSING")
        else:
            members = list(snapshot.members.all())
            if (
                snapshot.operational_date != operational_date
                or snapshot.captured_at != captured_at
                or snapshot.row_count != manifest_count
                or snapshot.sha256 != manifest_hash
                or snapshot.id != manifest_snapshot_id
            ):
                issues.append("OPEN_TRANSFER_SYNC_SNAPSHOT_MANIFEST_MISMATCH")
            if len(members) != snapshot.row_count:
                issues.append("OPEN_TRANSFER_SYNC_SNAPSHOT_COUNT_MISMATCH")
            if any(
                member.payload_sha256
                != _payload_sha256(canonical_open_transfer_payload(member))
                for member in members
            ):
                issues.append("OPEN_TRANSFER_SYNC_SNAPSHOT_MEMBER_HASH_MISMATCH")
            if any(not member.is_open or member.is_cancelled for member in members):
                issues.append("OPEN_TRANSFER_SYNC_SNAPSHOT_STATUS_INVALID")
            rebuilt_manifest = build_open_transfer_manifest(
                members,
                operational_date=snapshot.operational_date,
                captured_at=snapshot.captured_at,
            )
            if rebuilt_manifest["sha256"] != snapshot.sha256:
                issues.append("OPEN_TRANSFER_SYNC_SNAPSHOT_HASH_MISMATCH")

        cutoff, window_end = open_transfer_close_window(operational_date)
        started_at = job.started_at
        finished_at = job.finished_at
        if (
            started_at is None
            or finished_at is None
            or timezone.is_naive(started_at)
            or timezone.is_naive(finished_at)
            or captured_at is None
            or timezone.is_naive(captured_at)
            or not cutoff <= started_at <= finished_at <= window_end
            or not started_at <= captured_at <= finished_at
        ):
            issues.append("OPEN_TRANSFER_SYNC_CAPTURE_WINDOW_INVALID")
        return list(dict.fromkeys(issues))

    @staticmethod
    def _transfer_row_event_issues(row):
        if row.received_at is not None and row.sent_at is not None:
            if row.received_at < row.sent_at:
                return ["TRANSFER_ROW_EVENT_SEQUENCE_INVALID"]
        return []

    def _apply_transfers(
        self,
        *,
        rows,
        lower_bound,
        upper_bound,
        product_indexes,
        transfer_in,
        transfer_out,
        issues,
    ):
        for row in rows:
            if (
                row.is_cancelled
                or not getattr(row, "is_current_snapshot", True)
                or row.is_insumo
            ):
                continue
            product_id, issue_code = self._resolve_product(row, product_indexes)
            origin_at = row.sent_at
            used_fallback = False
            if origin_at is None and row.is_finalized:
                origin_at = row.registered_at
                used_fallback = True
            origin_in_month = origin_at is not None and lower_bound <= origin_at < upper_bound
            destination_in_month = (
                row.is_received
                and row.received_at is not None
                and lower_bound <= row.received_at < upper_bound
            )
            sent_quantity = Decimal(row.sent_quantity)
            received_quantity = Decimal(row.received_quantity)
            returned_quantity = (
                sent_quantity - received_quantity
                if destination_in_month
                and row.is_finalized
                and received_quantity < sent_quantity
                else ZERO
            )
            if product_id is None:
                issues.append(
                    TraceSourceIssue(
                        code=issue_code or "UNRESOLVED_PRODUCT",
                        message=(
                            f"No fue posible asignar la transferencia "
                            f"{row.transfer_external_id}/{row.detail_external_id} "
                            "a un único producto Point."
                        ),
                        branch_id=row.origin_branch_id,
                        source_ids=(row.id,),
                    )
                )
                continue
            if origin_in_month:
                self._add_balance(
                    transfer_out,
                    (row.origin_branch_id, product_id),
                    sent_quantity,
                    row.id,
                )
                if used_fallback:
                    issues.append(
                        self._transfer_issue(
                            row,
                            product_id,
                            "TRANSFER_DATE_FALLBACK",
                            "se fechó con registered_at porque no tiene sent_at",
                        )
                    )
                if not row.is_received or row.received_at is None:
                    issues.append(
                        self._transfer_issue(
                            row,
                            product_id,
                            "INCOMPLETE_TRANSFER",
                            "fue enviada pero aún no tiene recepción",
                        )
                    )
            if destination_in_month:
                self._add_balance(
                    transfer_in,
                    (row.destination_branch_id, product_id),
                    received_quantity,
                    row.id,
                )
            if returned_quantity > ZERO:
                self._add_balance(
                    transfer_in,
                    (row.origin_branch_id, product_id),
                    returned_quantity,
                    row.id,
                )
            affected_branches = []
            if origin_in_month or returned_quantity > ZERO:
                affected_branches.append(row.origin_branch_id)
            if destination_in_month:
                affected_branches.append(row.destination_branch_id)
            if issue_code:
                for affected_branch_id in dict.fromkeys(affected_branches):
                    issues.append(
                        TraceSourceIssue(
                            code=issue_code,
                            message=(
                                f"La transferencia {row.id} se asignó por "
                                "coincidencia secundaria."
                            ),
                            branch_id=affected_branch_id,
                            product_id=product_id,
                            source_ids=(row.id,),
                        )
                    )
            if (
                row.is_received
                and row.received_at is not None
                and sent_quantity != received_quantity
            ):
                return_detail = (
                    f" Point retornó {returned_quantity} al almacén origen."
                    if returned_quantity > ZERO
                    else ""
                )
                for affected_branch_id in dict.fromkeys(affected_branches):
                    issues.append(
                        TraceSourceIssue(
                            code="TRANSFER_QUANTITY_MISMATCH",
                            message=(
                                f"La transferencia {row.transfer_external_id}/"
                                f"{row.detail_external_id} registra "
                                f"{row.sent_quantity} enviadas y "
                                f"{row.received_quantity} recibidas."
                                f"{return_detail}"
                            ),
                            branch_id=affected_branch_id,
                            product_id=product_id,
                            source_ids=(row.id,),
                        )
                    )

    @staticmethod
    def _transfer_issue(row, product_id, code, detail):
        return TraceSourceIssue(
            code=code,
            message=(
                f"La transferencia {row.transfer_external_id}/"
                f"{row.detail_external_id} {detail}."
            ),
            branch_id=row.origin_branch_id,
            product_id=product_id,
            source_ids=(row.id,),
        )

    def _apply_conversions(
        self,
        *,
        rows,
        product_indexes,
        conversion_in,
        conversion_out,
        conversion_in_impacts,
        conversion_out_impacts,
        issues,
    ):
        recipe_indexes = self._build_recipe_indexes()
        relations = self._conversion_relations()
        for row in rows:
            destination_id, destination_issue = self._resolve_product(
                row, product_indexes
            )
            if destination_id is None:
                issues.append(
                    TraceSourceIssue(
                        code="MISSING_CONVERSION_DESTINATION",
                        message=f"La conversión {row.id} no tiene producto destino homologado.",
                        branch_id=row.branch_id,
                        source_ids=(row.id,),
                    )
                )
                continue
            if destination_issue:
                issues.append(
                    TraceSourceIssue(
                        code=destination_issue,
                        message=f"El destino de la conversión {row.id} se asignó por coincidencia secundaria.",
                        branch_id=row.branch_id,
                        product_id=destination_id,
                        source_ids=(row.id,),
                    )
                )
            destination_impact = Decimal(row.quantity)
            self._add_balance(
                conversion_in,
                (row.branch_id, destination_id),
                destination_impact,
                row.id,
            )
            conversion_in_impacts.setdefault(
                (row.branch_id, destination_id), {}
            )[row.id] = destination_impact
            destination_recipe_id = self._resolve_recipe_identity(
                row.item_code, row.item_name, recipe_indexes
            )
            relation = relations.get(destination_recipe_id)
            if relation is None:
                issues.append(
                    TraceSourceIssue(
                        code="NON_DERIVED_CONVERSION",
                        message=(
                            f"La fila Point {row.id} no tiene una relación derivada "
                            "activa; se conserva el destino sin asignar origen."
                        ),
                        branch_id=row.branch_id,
                        product_id=destination_id,
                        source_ids=(row.id,),
                    )
                )
                continue
            parent_recipe_id, factor = relation
            if factor <= ZERO:
                issues.append(
                    TraceSourceIssue(
                        code="CONVERSION_EQUIVALENCE_MISMATCH",
                        message=f"La conversión {row.id} tiene un factor canónico inválido.",
                        branch_id=row.branch_id,
                        product_id=destination_id,
                        source_ids=(row.id,),
                    )
                )
                continue

            supplied_origin = bool(
                str(row.source_item_code or "").strip()
                or str(row.source_item_name or "").strip()
            )
            if not supplied_origin:
                issues.append(
                    TraceSourceIssue(
                        code="MISSING_CONVERSION_ORIGIN",
                        message=(
                            f"La conversión {row.id} confirma el producto destino, "
                            "pero Point no informó el producto origen."
                        ),
                        branch_id=row.branch_id,
                        product_id=destination_id,
                        source_ids=(row.id,),
                    )
                )
                continue
            origin_code = row.source_item_code
            origin_name = row.source_item_name
            origin_id, origin_issue = self._resolve_product_identity(
                origin_code,
                origin_name,
                product_indexes,
            )
            if origin_id is None:
                if origin_issue:
                    issues.append(
                        TraceSourceIssue(
                            code=origin_issue,
                            message=(
                                f"El origen de la conversión {row.id} no pudo "
                                "resolverse de forma inequívoca."
                            ),
                            branch_id=row.branch_id,
                            product_id=destination_id,
                            source_ids=(row.id,),
                        )
                    )
                issues.append(
                    TraceSourceIssue(
                        code="MISSING_CONVERSION_ORIGIN",
                        message=f"La conversión {row.id} no tiene producto origen homologado.",
                        branch_id=row.branch_id,
                        product_id=destination_id,
                        source_ids=(row.id,),
                    )
                )
                continue
            origin_recipe_id = (
                self._resolve_recipe_identity(
                    row.source_item_code,
                    row.source_item_name,
                    recipe_indexes,
                )
                if supplied_origin
                else parent_recipe_id
            )
            if origin_recipe_id != parent_recipe_id:
                issues.append(
                    TraceSourceIssue(
                        code="CONVERSION_EQUIVALENCE_MISMATCH",
                        message=(
                            f"La conversión {row.id} no coincide con una equivalencia "
                            "activa entre su origen y destino."
                        ),
                        branch_id=row.branch_id,
                        product_id=destination_id,
                        source_ids=(row.id,),
                    )
                )
                continue
            origin_impact = destination_impact / factor
            self._add_balance(
                conversion_out,
                (row.branch_id, origin_id),
                origin_impact,
                row.id,
            )
            conversion_out_impacts.setdefault(
                (row.branch_id, origin_id), {}
            )[row.id] = origin_impact
            if origin_issue:
                issues.append(
                    TraceSourceIssue(
                        code=origin_issue,
                        message=(
                            f"El origen de la conversión {row.id} se asignó por "
                            "coincidencia secundaria."
                        ),
                        branch_id=row.branch_id,
                        product_id=origin_id,
                        source_ids=(row.id,),
                    )
                )

    @staticmethod
    def canonical_branch_identity() -> tuple[dict[int, int], dict[int, PointBranch]]:
        return canonical_point_branch_identity()

    @staticmethod
    def _canonicalize_balance_map(balances, branch_id_aliases):
        canonical = {}
        for (branch_id, product_id), (quantity, source_ids) in balances.items():
            key = (branch_id_aliases.get(branch_id, branch_id), product_id)
            current_quantity, current_ids = canonical.get(key, (ZERO, []))
            canonical[key] = (
                current_quantity + Decimal(quantity),
                list(dict.fromkeys((*current_ids, *source_ids))),
            )
        return canonical

    @staticmethod
    def _canonicalize_impact_map(impacts, branch_id_aliases):
        canonical = {}
        for (branch_id, product_id), source_impacts in impacts.items():
            key = (branch_id_aliases.get(branch_id, branch_id), product_id)
            canonical.setdefault(key, {}).update(source_impacts)
        return canonical

    @staticmethod
    def _add_balance(balances, key, quantity, source_id):
        current, source_ids = balances.get(key, (ZERO, []))
        source_ids.append(source_id)
        balances[key] = (current + Decimal(quantity), source_ids)

    @staticmethod
    def _merge_balance_maps(target, addition):
        for key, (quantity, _source_ids) in addition.items():
            current, current_source_ids = target.get(key, (ZERO, []))
            target[key] = (
                current + Decimal(quantity),
                list(current_source_ids),
            )

    @staticmethod
    def _build_recipe_indexes():
        code_candidates = {}
        name_candidates = {}
        by_id = {}
        for recipe in Receta.objects.only(
            "id",
            "codigo_point",
            "nombre",
            "nombre_normalizado",
        ):
            code = recipe.codigo_point.strip()
            if code:
                code_candidates.setdefault(code, []).append(recipe.id)
            name = recipe.nombre_normalizado.strip()
            if name:
                name_candidates.setdefault(name, []).append(recipe.id)
            by_id[recipe.id] = (recipe.codigo_point, recipe.nombre)
        return {
            "code": {key: tuple(value) for key, value in code_candidates.items()},
            "name": {key: tuple(value) for key, value in name_candidates.items()},
            "by_id": by_id,
        }

    @staticmethod
    def _conversion_relations():
        relations = {
            row.receta_porcion_id: (
                row.receta_padre_id,
                Decimal(row.factor_conversion),
            )
            for row in RecetaEquivalencia.objects.filter(
                activo=True,
                tipo_relacion=RecetaEquivalencia.TIPO_CONVERSION,
            ).only("receta_porcion_id", "receta_padre_id", "factor_conversion")
        }
        for row in RecetaPresentacionDerivada.objects.filter(
            activo=True,
            tipo_derivado=RecetaPresentacionDerivada.TIPO_REBANADA,
            receta_derivada_id__isnull=False,
        ).only("receta_derivada_id", "receta_padre_id", "unidades_por_padre"):
            relations.setdefault(
                row.receta_derivada_id,
                (row.receta_padre_id, Decimal(row.unidades_por_padre)),
            )
        return relations

    @staticmethod
    def _resolve_recipe_identity(code, name, indexes):
        code_matches = indexes["code"].get(str(code or "").strip(), ())
        if len(code_matches) == 1:
            return int(code_matches[0])
        name_matches = indexes["name"].get(_normalize_name(str(name or "")), ())
        if len(name_matches) == 1:
            return int(name_matches[0])
        return None

    @staticmethod
    def _resolve_product_identity(code, name, indexes):
        item_code = str(code or "").strip()
        if item_code:
            external_match = indexes["external_id"].get(item_code)
            if external_match is not None:
                return int(external_match), None
            sku_matches = indexes["sku"].get(item_code, ())
            if len(sku_matches) == 1:
                return int(sku_matches[0]), "PRODUCT_RESOLVED_BY_SKU"
            if len(sku_matches) > 1:
                return None, "AMBIGUOUS_PRODUCT"
        name_matches = indexes["normalized_name"].get(
            _normalize_name(str(name or "")), ()
        )
        if len(name_matches) == 1:
            return int(name_matches[0]), "PRODUCT_RESOLVED_BY_NAME"
        if len(name_matches) > 1:
            return None, "AMBIGUOUS_PRODUCT"
        return None, "UNRESOLVED_PRODUCT"

    @staticmethod
    def _resolve_product(row, indexes) -> tuple[int | None, str | None]:
        direct_product_id = getattr(row, "product_id", None)
        if direct_product_id in indexes["id"]:
            return int(direct_product_id), None

        item_code = str(getattr(row, "item_code", "") or "").strip()
        if item_code:
            external_match = indexes["external_id"].get(item_code)
            if external_match is not None:
                return int(external_match), None
            sku_matches = indexes["sku"].get(item_code, ())
            if len(sku_matches) == 1:
                return int(sku_matches[0]), "PRODUCT_RESOLVED_BY_SKU"
            if len(sku_matches) > 1:
                return None, "AMBIGUOUS_PRODUCT"

        normalized_name = _normalize_name(str(getattr(row, "item_name", "") or ""))
        name_matches = indexes["normalized_name"].get(normalized_name, ())
        if len(name_matches) == 1:
            return int(name_matches[0]), "PRODUCT_RESOLVED_BY_NAME"
        if len(name_matches) > 1:
            return None, "AMBIGUOUS_PRODUCT"
        return None, "UNRESOLVED_PRODUCT"

    @staticmethod
    def _record_direct_row(
        *,
        balances,
        issues,
        source_name,
        row,
        branch_id,
        product_id,
        issue_code,
        quantity,
    ) -> None:
        if product_id is None:
            issues.append(
                TraceSourceIssue(
                    code=issue_code or "UNRESOLVED_PRODUCT",
                    message=(
                        f"No fue posible asignar la fila {row.id} de {source_name} "
                        "a un único producto Point."
                    ),
                    branch_id=branch_id,
                    source_ids=(row.id,),
                )
            )
            return
        key = (branch_id, product_id)
        current_quantity, source_ids = balances.get(key, (ZERO, []))
        source_ids.append(row.id)
        balances[key] = (current_quantity + Decimal(quantity), source_ids)
        if issue_code:
            issues.append(
                TraceSourceIssue(
                    code=issue_code,
                    message=(
                        f"La fila {row.id} de {source_name} se asignó por una "
                        "coincidencia secundaria que requiere auditoría."
                    ),
                    branch_id=branch_id,
                    product_id=product_id,
                    source_ids=(row.id,),
                )
            )

    @staticmethod
    def _month_datetime_bounds(month_start: date):
        next_month = (
            date(month_start.year + 1, 1, 1)
            if month_start.month == 12
            else date(month_start.year, month_start.month + 1, 1)
        )
        current_timezone = timezone.get_current_timezone()
        return (
            timezone.make_aware(
                datetime.combine(month_start, time.min),
                current_timezone,
            ),
            timezone.make_aware(
                datetime.combine(next_month, time.min),
                current_timezone,
            ),
        )

    @staticmethod
    def _coverage_complete(
        closing: PointHistoricalInventoryClosing,
        balances: dict[tuple[int, int], tuple[Decimal, list[int]]],
    ) -> bool:
        expected_branch_ids = {
            int(value) for value in (closing.expected_branch_ids or [])
        }
        expected_product_ids = {
            int(value) for value in (closing.expected_product_ids or [])
        }
        expected_keys = {
            (branch_id, product_id)
            for branch_id in expected_branch_ids
            for product_id in expected_product_ids
        }
        return bool(
            expected_branch_ids
            and expected_product_ids
            and balances.keys() == expected_keys
        )

    @staticmethod
    def _select_closing(
        operational_date: date,
    ) -> PointHistoricalInventoryClosing | None:
        return (
            PointHistoricalInventoryClosing.objects.filter(
                operational_date=operational_date,
                status=PointHistoricalInventoryClosing.STATUS_VERIFIED,
            )
            .order_by("-id")
            .first()
        )

    @staticmethod
    def _load_closing(
        closing: PointHistoricalInventoryClosing,
    ) -> dict[tuple[int, int], tuple[Decimal, list[int]]]:
        balances: dict[tuple[int, int], tuple[Decimal, list[int]]] = {}
        lines = PointHistoricalInventoryClosingLine.objects.filter(
            closing=closing
        ).values_list("id", "branch_id", "product_id", "stock")
        for line_id, branch_id, product_id, line_stock in lines:
            key = (branch_id, product_id)
            stock, source_ids = balances.get(key, (ZERO, []))
            source_ids.append(line_id)
            balances[key] = (stock + line_stock, source_ids)
        return balances
