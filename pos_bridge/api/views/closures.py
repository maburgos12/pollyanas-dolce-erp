from __future__ import annotations

from django_filters import rest_framework as filters
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from pos_bridge.api.pagination import StandardPagination
from pos_bridge.api.permissions import IsProductClosureUser
from pos_bridge.api.serializers.closures import (
    ProductMonthClosureBuildSerializer,
    ProductMonthClosureDetailSerializer,
    ProductMonthClosureLockSerializer,
    ProductMonthClosureSerializer,
)
from pos_bridge.services.product_month_closure_service import ProductMonthClosureError, ProductMonthClosureService
from recetas.models import ProductoMonthClosure


class ProductMonthClosureFilter(filters.FilterSet):
    month = filters.CharFilter(method="filter_month")
    status = filters.CharFilter(field_name="status", lookup_expr="iexact")
    locked = filters.BooleanFilter(field_name="is_locked")

    class Meta:
        model = ProductoMonthClosure
        fields = []

    def filter_month(self, queryset, name, value):
        normalized = (value or "").strip()
        if len(normalized) != 7 or "-" not in normalized:
            return queryset.none()
        return queryset.filter(month_start=f"{normalized}-01")


class ProductMonthClosureViewSet(ReadOnlyModelViewSet):
    permission_classes = [IsProductClosureUser]
    pagination_class = StandardPagination
    filterset_class = ProductMonthClosureFilter

    def get_queryset(self):
        return ProductoMonthClosure.objects.prefetch_related("lines", "lines__receta_padre").order_by("-month_start", "-id")

    def get_serializer_class(self):
        if self.action == "retrieve":
            return ProductMonthClosureDetailSerializer
        return ProductMonthClosureSerializer

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        return Response(serializer.data)

    @action(detail=False, methods=["post"])
    def build(self, request):
        serializer = ProductMonthClosureBuildSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data

        service = ProductMonthClosureService()
        try:
            closure = service.build(
                month=payload["month"],
                rebuild=payload.get("rebuild", False),
                lock_after_build=payload.get("lock_after_build", False),
                built_by=request.user,
                approval_note=payload.get("approval_note", ""),
                approval_reason=payload.get("approval_reason", ""),
                approval_channel="api",
            )
        except ProductMonthClosureError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        response = ProductMonthClosureDetailSerializer(closure)
        return Response(response.data, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["post"])
    def lock(self, request, pk=None):
        serializer = ProductMonthClosureLockSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        closure = self.get_object()
        service = ProductMonthClosureService()
        try:
            closure = service.lock(
                closure=closure,
                locked_by=request.user,
                reason=serializer.validated_data.get("approval_reason", "") or "api_lock",
                note=serializer.validated_data.get("approval_note", ""),
                channel="api",
            )
        except ProductMonthClosureError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        response = ProductMonthClosureDetailSerializer(closure)
        return Response(response.data, status=status.HTTP_200_OK)
