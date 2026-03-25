from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Count, Q, Sum
from django.db.models.functions import Coalesce, TruncMonth
from django_filters import rest_framework as filters
from rest_framework import filters as drf_filters
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from pos_bridge.api.pagination import StandardPagination
from pos_bridge.api.serializers.sales import (
    PointDailySaleSerializer,
    SalesByGroupSerializer,
    SalesSummarySerializer,
    SalesTrendSerializer,
)
from pos_bridge.models import PointDailySale

ZERO = Decimal("0")


class SalesFilter(filters.FilterSet):
    start_date = filters.DateFilter(field_name="sale_date", lookup_expr="gte")
    end_date = filters.DateFilter(field_name="sale_date", lookup_expr="lte")
    branch = filters.CharFilter(method="filter_branch")
    product = filters.CharFilter(method="filter_product")
    category = filters.CharFilter(field_name="product__category", lookup_expr="icontains")
    has_receta = filters.BooleanFilter(method="filter_has_receta")

    class Meta:
        model = PointDailySale
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

    def filter_has_receta(self, queryset, name, value):
        return queryset.filter(receta__isnull=not value)


class SalesViewSet(ReadOnlyModelViewSet):
    serializer_class = PointDailySaleSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardPagination
    filterset_class = SalesFilter
    filter_backends = [
        filters.DjangoFilterBackend,
        drf_filters.SearchFilter,
        drf_filters.OrderingFilter,
    ]
    search_fields = ["branch__name", "product__name", "product__sku", "receta__nombre"]
    ordering_fields = ["sale_date", "total_amount", "quantity", "branch__name", "product__name"]
    ordering = ["-sale_date", "-total_amount", "-id"]

    def get_queryset(self):
        return PointDailySale.objects.select_related("branch", "branch__erp_branch", "product", "receta")

    def _get_filtered_qs(self):
        return self.filter_queryset(self.get_queryset())

    def _date_range(self):
        params = self.request.query_params
        end = date.today()
        start = end - timedelta(days=30)
        if params.get("start_date"):
            try:
                start = date.fromisoformat(params["start_date"])
            except ValueError:
                pass
        if params.get("end_date"):
            try:
                end = date.fromisoformat(params["end_date"])
            except ValueError:
                pass
        return start, end

    @action(detail=False, methods=["get"])
    def summary(self, request):
        qs = self._get_filtered_qs()
        start, end = self._date_range()
        totals = qs.aggregate(
            total_sales=Coalesce(Sum("total_amount"), ZERO),
            total_quantity=Coalesce(Sum("quantity"), ZERO),
            total_tickets=Coalesce(Sum("tickets"), 0),
            total_discount=Coalesce(Sum("discount_amount"), ZERO),
            total_tax=Coalesce(Sum("tax_amount"), ZERO),
            total_net=Coalesce(Sum("net_amount"), ZERO),
            branches_count=Count("branch", distinct=True),
            products_count=Count("product", distinct=True),
            days_count=Count("sale_date", distinct=True),
        )
        serializer = SalesSummarySerializer({"period_start": start, "period_end": end, **totals})
        return Response(serializer.data)

    @action(detail=False, methods=["get"], url_path="by-branch")
    def by_branch(self, request):
        qs = self._get_filtered_qs()
        grand_total = qs.aggregate(total=Coalesce(Sum("total_amount"), ZERO))["total"]
        rows = (
            qs.values("branch__external_id", "branch__name")
            .annotate(
                total_sales=Coalesce(Sum("total_amount"), ZERO),
                total_quantity=Coalesce(Sum("quantity"), ZERO),
                total_tickets=Coalesce(Sum("tickets"), 0),
            )
            .order_by("-total_sales", "branch__name")
        )
        payload = []
        for row in rows:
            pct = (row["total_sales"] / grand_total * 100) if grand_total else ZERO
            payload.append(
                {
                    "group_key": row["branch__external_id"],
                    "group_name": row["branch__name"],
                    "total_sales": row["total_sales"],
                    "total_quantity": row["total_quantity"],
                    "total_tickets": row["total_tickets"],
                    "percentage": round(pct, 2),
                }
            )
        return Response(SalesByGroupSerializer(payload, many=True).data)

    @action(detail=False, methods=["get"], url_path="by-product")
    def by_product(self, request):
        qs = self._get_filtered_qs()
        grand_total = qs.aggregate(total=Coalesce(Sum("total_amount"), ZERO))["total"]
        rows = (
            qs.values("product__sku", "product__name")
            .annotate(
                total_sales=Coalesce(Sum("total_amount"), ZERO),
                total_quantity=Coalesce(Sum("quantity"), ZERO),
                total_tickets=Coalesce(Sum("tickets"), 0),
            )
            .order_by("-total_sales", "product__name")[:100]
        )
        payload = []
        for row in rows:
            pct = (row["total_sales"] / grand_total * 100) if grand_total else ZERO
            payload.append(
                {
                    "group_key": row["product__sku"],
                    "group_name": row["product__name"],
                    "total_sales": row["total_sales"],
                    "total_quantity": row["total_quantity"],
                    "total_tickets": row["total_tickets"],
                    "percentage": round(pct, 2),
                }
            )
        return Response(SalesByGroupSerializer(payload, many=True).data)

    @action(detail=False, methods=["get"])
    def trends(self, request):
        qs = self._get_filtered_qs()
        rows = (
            qs.annotate(month=TruncMonth("sale_date"))
            .values("month")
            .annotate(
                total_sales=Coalesce(Sum("total_amount"), ZERO),
                total_quantity=Coalesce(Sum("quantity"), ZERO),
                total_tickets=Coalesce(Sum("tickets"), 0),
            )
            .order_by("month")
        )
        payload = []
        for row in rows:
            tickets = row["total_tickets"] or 1
            payload.append(
                {
                    "period": row["month"].strftime("%Y-%m"),
                    "total_sales": row["total_sales"],
                    "total_quantity": row["total_quantity"],
                    "total_tickets": row["total_tickets"],
                    "avg_ticket": round(row["total_sales"] / tickets, 2),
                }
            )
        return Response(SalesTrendSerializer(payload, many=True).data)
