from __future__ import annotations

from decimal import Decimal

from django.db.models import F, OuterRef, Q, Subquery
from django_filters import rest_framework as filters
from rest_framework import filters as drf_filters
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from pos_bridge.api.pagination import LargePagination, StandardPagination
from pos_bridge.api.serializers.inventory import (
    CurrentStockSerializer,
    InventoryAvailabilitySerializer,
    LowStockAlertSerializer,
    PointInventorySnapshotSerializer,
)
from pos_bridge.models import PointInventorySnapshot

ZERO = Decimal("0")


class InventoryFilter(filters.FilterSet):
    branch = filters.CharFilter(method="filter_branch")
    product = filters.CharFilter(method="filter_product")
    category = filters.CharFilter(field_name="product__category", lookup_expr="icontains")
    captured_after = filters.DateTimeFilter(field_name="captured_at", lookup_expr="gte")
    captured_before = filters.DateTimeFilter(field_name="captured_at", lookup_expr="lte")

    class Meta:
        model = PointInventorySnapshot
        fields = []

    def filter_branch(self, queryset, name, value):
        return queryset.filter(
            Q(branch__name__icontains=value)
            | Q(branch__external_id__iexact=value)
            | Q(branch__erp_branch__codigo__iexact=value)
        )

    def filter_product(self, queryset, name, value):
        return queryset.filter(
            Q(product__name__icontains=value)
            | Q(product__sku__iexact=value)
            | Q(product__external_id__iexact=value)
        )


class InventoryViewSet(ReadOnlyModelViewSet):
    serializer_class = PointInventorySnapshotSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardPagination
    filterset_class = InventoryFilter
    filter_backends = [
        filters.DjangoFilterBackend,
        drf_filters.SearchFilter,
        drf_filters.OrderingFilter,
    ]
    search_fields = ["branch__name", "branch__external_id", "product__name", "product__sku", "product__category"]
    ordering_fields = ["captured_at", "stock", "branch__name", "product__name"]
    ordering = ["-captured_at", "-id"]

    def get_queryset(self):
        return PointInventorySnapshot.objects.select_related("branch", "branch__erp_branch", "product")

    def _latest_snapshot_qs(self, branch_filter: str | None = None):
        latest_snapshot_id = (
            PointInventorySnapshot.objects.filter(
                branch_id=OuterRef("branch_id"),
                product_id=OuterRef("product_id"),
            )
            .order_by("-captured_at", "-id")
            .values("id")[:1]
        )
        qs = self.get_queryset().filter(id=Subquery(latest_snapshot_id))
        if branch_filter:
            qs = qs.filter(
                Q(branch__name__icontains=branch_filter)
                | Q(branch__external_id__iexact=branch_filter)
                | Q(branch__erp_branch__codigo__iexact=branch_filter)
            )
        return qs

    @action(detail=False, methods=["get"], pagination_class=LargePagination)
    def current(self, request):
        branch_filter = request.query_params.get("branch", "").strip()
        product_filter = request.query_params.get("product", "").strip()
        category_filter = request.query_params.get("category", "").strip()

        qs = self._latest_snapshot_qs(branch_filter or None)
        if product_filter:
            qs = qs.filter(
                Q(product__name__icontains=product_filter)
                | Q(product__sku__iexact=product_filter)
                | Q(product__external_id__iexact=product_filter)
            )
        if category_filter:
            qs = qs.filter(product__category__icontains=category_filter)

        product_map: dict[int, dict] = {}
        for snap in qs.order_by("product__name", "branch__name", "id"):
            product_entry = product_map.setdefault(
                snap.product_id,
                {
                    "product_sku": snap.product.sku,
                    "product_name": snap.product.name,
                    "product_category": snap.product.category,
                    "branches": [],
                    "total_stock": ZERO,
                },
            )
            product_entry["branches"].append(
                {
                    "branch": snap.branch.name,
                    "branch_external_id": snap.branch.external_id,
                    "erp_branch_code": snap.branch.erp_branch.codigo if snap.branch.erp_branch_id else "",
                    "stock": snap.stock,
                    "min_stock": snap.min_stock,
                    "max_stock": snap.max_stock,
                    "captured_at": snap.captured_at.isoformat(),
                }
            )
            product_entry["total_stock"] += snap.stock

        page = self.paginate_queryset(list(product_map.values()))
        serializer = CurrentStockSerializer(page, many=True)
        return self.get_paginated_response(serializer.data)

    @action(detail=False, methods=["get"], pagination_class=LargePagination)
    def availability(self, request):
        branch_filter = request.query_params.get("branch", "").strip()
        category_filter = request.query_params.get("category", "").strip()
        sku_filter = request.query_params.get("sku", "").strip()

        qs = self._latest_snapshot_qs(branch_filter or None).filter(product__active=True)
        if category_filter:
            qs = qs.filter(product__category__icontains=category_filter)
        if sku_filter:
            qs = qs.filter(product__sku__iexact=sku_filter)

        product_map: dict[int, dict] = {}
        for snap in qs.order_by("product__name", "branch__name", "id"):
            product_entry = product_map.setdefault(
                snap.product_id,
                {
                    "sku": snap.product.sku,
                    "name": snap.product.name,
                    "category": snap.product.category,
                    "branches": [],
                    "total_stock": ZERO,
                    "last_updated": snap.captured_at,
                },
            )
            product_entry["branches"].append(
                {
                    "branch": snap.branch.name,
                    "branch_external_id": snap.branch.external_id,
                    "erp_branch_code": snap.branch.erp_branch.codigo if snap.branch.erp_branch_id else "",
                    "stock": float(snap.stock),
                    "available": snap.stock > ZERO,
                }
            )
            product_entry["total_stock"] += snap.stock
            if snap.captured_at > product_entry["last_updated"]:
                product_entry["last_updated"] = snap.captured_at

        results = []
        for item in sorted(product_map.values(), key=lambda row: row["name"]):
            item["available"] = item["total_stock"] > ZERO
            results.append(item)

        page = self.paginate_queryset(results)
        serializer = InventoryAvailabilitySerializer(page, many=True)
        return self.get_paginated_response(serializer.data)

    @action(detail=False, methods=["get"], url_path="low-stock", pagination_class=LargePagination)
    def low_stock(self, request):
        branch_filter = request.query_params.get("branch", "").strip()
        qs = (
            self._latest_snapshot_qs(branch_filter or None)
            .filter(stock__lt=F("min_stock"), min_stock__gt=0)
            .order_by("branch__name", "product__name", "id")
        )
        rows = [
            {
                "branch_name": snap.branch.name,
                "branch_external_id": snap.branch.external_id,
                "erp_branch_code": snap.branch.erp_branch.codigo if snap.branch.erp_branch_id else "",
                "product_sku": snap.product.sku,
                "product_name": snap.product.name,
                "current_stock": snap.stock,
                "min_stock": snap.min_stock,
                "deficit": snap.min_stock - snap.stock,
            }
            for snap in qs
        ]
        page = self.paginate_queryset(rows)
        serializer = LowStockAlertSerializer(page, many=True)
        return self.get_paginated_response(serializer.data)
