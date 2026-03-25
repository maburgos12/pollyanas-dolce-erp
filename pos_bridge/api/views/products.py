from __future__ import annotations

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
