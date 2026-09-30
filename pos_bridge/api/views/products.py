from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
import time

from django.utils import timezone
import requests
from rest_framework import status
from rest_framework import filters as drf_filters
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from pos_bridge.api.pagination import StandardPagination
from pos_bridge.api.serializers.products import PointProductSerializer, ProductRecipeSerializer
from pos_bridge.models import PointProduct
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import LineaReceta


class ProductsViewSet(ReadOnlyModelViewSet):
    serializer_class = PointProductSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = StandardPagination
    filter_backends = [drf_filters.SearchFilter, drf_filters.OrderingFilter]
    search_fields = ["name", "sku", "external_id", "category"]
    ordering_fields = ["name", "sku", "category", "updated_at"]
    ordering = ["name", "id"]

    def get_queryset(self):
        return PointProduct.objects.all()

    @action(detail=False, methods=["get"], url_path="sale-price")
    def sale_price(self, request):
        from pos_bridge.config import load_point_bridge_settings
        from pos_bridge.services.catalog_recipe_execution import DEADLINE, remaining_seconds
        from pos_bridge.services.point_account_session_lock import point_account_session_lock
        from pos_bridge.services.point_http_client import PointHttpSessionClient
        from pos_bridge.utils.exceptions import AuthenticationError, ConfigurationError, ExtractionError

        code = request.query_params.get("product_code", "")
        payload = {
            "status": "UNKNOWN", "product_code": code if len(code) <= 120 else None,
            "point_product_id": None, "point_product_code": None, "point_product_name": None,
            "amount": None, "currency": "MXN", "currency_source": "ERP_POLICY",
            "source": "ERP_POS_BRIDGE_LIVE_POINT", "is_fresh": False, "checked_at": None,
        }
        if not code.strip() or len(code) > 120:
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        products = list(PointProduct.objects.filter(sku=code)[:2])
        if not products or (len(products) == 1 and not products[0].active):
            return Response(payload, status=status.HTTP_404_NOT_FOUND)
        if len(products) != 1 or not re.fullmatch(r"[1-9][0-9]*", products[0].external_id):
            return Response(payload, status=status.HTTP_409_CONFLICT)
        product = products[0]
        # ponytail: shared HTTP client's deadline bounds this read; no second timeout mechanism.
        deadline = time.monotonic() + 10
        outer_deadline = DEADLINE.get()
        token = DEADLINE.set(min(deadline, outer_deadline) if outer_deadline is not None else deadline)
        try:
            remaining_seconds()
            with point_account_session_lock(wait=False) as acquired:
                if not acquired:
                    return Response(payload, status=status.HTTP_503_SERVICE_UNAVAILABLE)
                with PointHttpSessionClient(load_point_bridge_settings()) as client:
                    client.login()
                    detail = client.get_product_detail(product.external_id)
                    remaining_seconds()
                    checked_at = timezone.now()
        except (AuthenticationError, ConfigurationError, ExtractionError, requests.RequestException, OSError, TimeoutError):
            return Response(payload, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        finally:
            DEADLINE.reset(token)

        if (
            not isinstance(detail, dict)
            or type(detail.get("PK")) not in (int, str)
            or str(detail["PK"]) != product.external_id
            or detail.get("Codigo") != product.sku
            or not product.name.strip()
            or detail.get("Nombre") != product.name
            or detail.get("Activo") is not True
        ):
            return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
        try:
            amount = Decimal(str(detail.get("Precio_default")))
        except (InvalidOperation, TypeError, ValueError):
            return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
        if not amount.is_finite() or amount <= 0:
            return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
        payload.update(
            status="VERIFIED", point_product_id=str(detail["PK"]),
            point_product_code=detail["Codigo"], point_product_name=detail["Nombre"],
            amount=str(amount), is_fresh=True, checked_at=checked_at.isoformat(),
        )
        return Response(payload)

    @action(detail=True, methods=["get"])
    def recipe(self, request, pk=None):
        product = self.get_object()
        matcher = PointSalesMatchingService()
        receta = matcher.resolve_receta(codigo_point=product.sku, point_name=product.name)
        if receta is None:
            return Response(
                {"detail": "Este producto no tiene receta vinculada en el ERP."},
                status=status.HTTP_404_NOT_FOUND,
            )

        bom = []
        for line in (
            LineaReceta.objects.filter(receta=receta)
            .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .select_related("insumo", "unidad")
            .order_by("posicion", "id")
        ):
            bom.append(
                {
                    "insumo": line.insumo.nombre if line.insumo_id else line.insumo_texto,
                    "cantidad": line.cantidad,
                    "unidad": line.unidad.codigo if line.unidad_id else line.unidad_texto,
                    "costo_unitario": line.costo_unitario_snapshot,
                    "match_status": line.match_status,
                }
            )

        serializer = ProductRecipeSerializer(
            {
                "product_sku": product.sku,
                "product_name": product.name,
                "receta_id": receta.id,
                "receta_nombre": receta.nombre,
                "receta_tipo": receta.tipo,
                "bom": bom,
            }
        )
        return Response(serializer.data)
