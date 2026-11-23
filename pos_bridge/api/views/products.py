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
        if len(products) == 1 and not products[0].active:
            return Response(payload, status=status.HTTP_404_NOT_FOUND)
        if len(products) > 1 or (products and not re.fullmatch(r"[1-9][0-9]*", products[0].external_id)):
            return Response(payload, status=status.HTTP_409_CONFLICT)
        product = products[0] if products else None
        expected_id = product.external_id if product else None
        expected_name = product.name if product else None
        # ponytail: shared deadline fits Maya's 8s HTTP timeout; no second timeout mechanism.
        deadline = time.monotonic() + 6
        outer_deadline = DEADLINE.get()
        token = DEADLINE.set(min(deadline, outer_deadline) if outer_deadline is not None else deadline)
        try:
            remaining_seconds()
            with point_account_session_lock(wait=True) as acquired:
                if not acquired:
                    return Response(payload, status=status.HTTP_503_SERVICE_UNAVAILABLE)
                remaining_seconds()
                with PointHttpSessionClient(load_point_bridge_settings()) as client:
                    client.login()
                    if product is None:
                        from pos_bridge.services.customer_catalog import customer_products
                        rows = client.get_products(text_art=code, activo=True)
                        if len(rows) >= client.CATALOG_PAGE_LIMIT:
                            return Response(payload, status=status.HTTP_409_CONFLICT)
                        candidates = [row for row in customer_products(rows)
                                      if row["product_code"] == code]
                        if not candidates:
                            return Response(payload, status=status.HTTP_404_NOT_FOUND)
                        if len(candidates) != 1:
                            return Response(payload, status=status.HTTP_409_CONFLICT)
                        expected_id = candidates[0]["point_product_id"]
                        expected_name = candidates[0]["product_name"]
                    detail = client.get_product_detail(expected_id)
                    remaining_seconds()
                    checked_at = timezone.now()
        except (AuthenticationError, ConfigurationError, ExtractionError, requests.RequestException, OSError, TimeoutError):
            return Response(payload, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        finally:
            DEADLINE.reset(token)

        point_ids = [detail[key] for key in ("PK_Producto", "PK") if key in detail] if isinstance(detail, dict) else []
        if (
            not isinstance(detail, dict)
            or not point_ids
            or any(type(value) not in (int, str) or str(value) != expected_id for value in point_ids)
            or detail.get("Codigo") != code
            or not expected_name.strip()
            or detail.get("Nombre") != expected_name
            or detail.get("Activo") is not True
        ):
            return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
        if product is None:
            try:
                if not customer_products([detail]):
                    return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
            except ExtractionError:
                return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
        try:
            amount = Decimal(str(detail.get("Precio_default")))
        except (InvalidOperation, TypeError, ValueError):
            return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
        if not amount.is_finite() or amount <= 0:
            return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
        payload.update(
            status="VERIFIED", point_product_id=str(point_ids[0]),
            point_product_code=detail["Codigo"], point_product_name=detail["Nombre"],
            amount=str(amount), is_fresh=True, checked_at=checked_at.isoformat(),
        )
        return Response(payload)

    @action(detail=False, methods=["get"], url_path="customer-catalog")
    def customer_catalog(self, request):
        from pos_bridge.config import load_point_bridge_settings
        from pos_bridge.services.catalog_recipe_execution import DEADLINE, remaining_seconds
        from pos_bridge.services.customer_catalog import customer_products
        from pos_bridge.services.point_account_session_lock import point_account_session_lock
        from pos_bridge.services.point_http_client import PointHttpSessionClient
        from pos_bridge.utils.exceptions import AuthenticationError, ConfigurationError, ExtractionError

        query = request.query_params.get("q", "")
        payload = {
            "status": "UNKNOWN", "source": "ERP_POS_BRIDGE_LIVE_POINT", "is_fresh": False,
            "checked_at": None, "complete": False, "query": query if len(query) <= 120 else None,
            "products": [],
        }
        if len(query) > 120:
            return Response(payload, status=status.HTTP_400_BAD_REQUEST)
        deadline = time.monotonic() + 6
        outer_deadline = DEADLINE.get()
        token = DEADLINE.set(min(deadline, outer_deadline) if outer_deadline is not None else deadline)
        try:
            remaining_seconds()
            with point_account_session_lock(wait=True) as acquired:
                if not acquired:
                    return Response(payload, status=status.HTTP_503_SERVICE_UNAVAILABLE)
                remaining_seconds()
                with PointHttpSessionClient(load_point_bridge_settings()) as client:
                    client.login()
                    rows = client.get_products(text_art=query, activo=True)
                    remaining_seconds()
                    products = customer_products(rows[:client.CATALOG_PAGE_LIMIT])
                    checked_at = timezone.now()
        except ExtractionError:
            return Response(payload, status=status.HTTP_502_BAD_GATEWAY)
        except (AuthenticationError, ConfigurationError, requests.RequestException, OSError, TimeoutError):
            return Response(payload, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        finally:
            DEADLINE.reset(token)
        payload.update(status="VERIFIED", is_fresh=True, checked_at=checked_at.isoformat(),
                       complete=len(rows) < PointHttpSessionClient.CATALOG_PAGE_LIMIT, products=products)
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
