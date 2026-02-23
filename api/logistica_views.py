from __future__ import annotations

from decimal import Decimal

from django.db import transaction
from django.db.models import Q, Sum
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import can_manage_logistica, can_view_logistica
from core.audit import log_event
from crm.models import PedidoCliente
from logistica.models import EntregaRuta, RutaEntrega

from .logistica_serializers import (
    LogisticaEntregaCreateSerializer,
    LogisticaEntregaSerializer,
    LogisticaRutaSerializer,
)


class _LogisticaBaseView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _bounded_int(value, *, default: int, min_value: int, max_value: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(min_value, min(parsed, max_value))


class LogisticaRutasView(_LogisticaBaseView):
    def get(self, request):
        if not can_view_logistica(request.user):
            return Response({"detail": "No tienes permisos para consultar Logística."}, status=status.HTTP_403_FORBIDDEN)

        q = (request.query_params.get("q") or "").strip()
        estatus = (request.query_params.get("estatus") or "").strip().upper()
        limit = self._bounded_int(request.query_params.get("limit"), default=50, min_value=1, max_value=500)
        offset = self._bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)

        qs = RutaEntrega.objects.all()
        if q:
            qs = qs.filter(
                Q(folio__icontains=q)
                | Q(nombre__icontains=q)
                | Q(chofer__icontains=q)
                | Q(unidad__icontains=q)
            )
        if estatus:
            qs = qs.filter(estatus=estatus)

        total = qs.count()
        rows = list(qs.order_by("-fecha_ruta", "-id")[offset : offset + limit])
        return Response(
            {
                "count": total,
                "limit": limit,
                "offset": offset,
                "results": LogisticaRutaSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not can_manage_logistica(request.user):
            return Response({"detail": "No tienes permisos para crear rutas."}, status=status.HTTP_403_FORBIDDEN)

        serializer = LogisticaRutaSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        ruta = serializer.save(created_by=request.user)
        log_event(
            request.user,
            "CREATE",
            "logistica.RutaEntrega",
            str(ruta.id),
            {"folio": ruta.folio, "nombre": ruta.nombre},
        )
        return Response(LogisticaRutaSerializer(ruta).data, status=status.HTTP_201_CREATED)


class LogisticaRutaEntregasView(_LogisticaBaseView):
    def get(self, request, ruta_id: int):
        if not can_view_logistica(request.user):
            return Response({"detail": "No tienes permisos para consultar Logística."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        rows = list(ruta.entregas.select_related("pedido").order_by("secuencia", "id"))
        return Response(
            {
                "ruta": LogisticaRutaSerializer(ruta).data,
                "entregas": LogisticaEntregaSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request, ruta_id: int):
        if not can_manage_logistica(request.user):
            return Response({"detail": "No tienes permisos para crear entregas."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        serializer = LogisticaEntregaCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data

        pedido = None
        pedido_id = payload.get("pedido_id")
        if pedido_id is not None:
            pedido = get_object_or_404(PedidoCliente, pk=pedido_id)

        with transaction.atomic():
            entrega = EntregaRuta.objects.create(
                ruta=ruta,
                secuencia=payload["secuencia"],
                pedido=pedido,
                cliente_nombre=payload.get("cliente_nombre") or "",
                direccion=payload.get("direccion") or "",
                contacto=payload.get("contacto") or "",
                telefono=payload.get("telefono") or "",
                ventana_inicio=payload.get("ventana_inicio"),
                ventana_fin=payload.get("ventana_fin"),
                estatus=payload.get("estatus") or EntregaRuta.ESTATUS_PENDIENTE,
                monto_estimado=payload.get("monto_estimado") or Decimal("0"),
                comentario=payload.get("comentario") or "",
            )
            ruta.recompute_totals()
            ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])

        log_event(
            request.user,
            "CREATE",
            "logistica.EntregaRuta",
            str(entrega.id),
            {"ruta": ruta.folio, "secuencia": entrega.secuencia, "estatus": entrega.estatus},
        )
        return Response(
            {
                "ruta": LogisticaRutaSerializer(ruta).data,
                "entrega": LogisticaEntregaSerializer(entrega).data,
            },
            status=status.HTTP_201_CREATED,
        )


class LogisticaRutaStatusView(_LogisticaBaseView):
    def post(self, request, ruta_id: int):
        if not can_manage_logistica(request.user):
            return Response({"detail": "No tienes permisos para cambiar estatus de ruta."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        estatus_nuevo = (request.data.get("estatus") or "").strip().upper()
        valid = {choice[0] for choice in RutaEntrega.ESTATUS_CHOICES}
        if estatus_nuevo not in valid:
            return Response({"detail": "Estatus inválido."}, status=status.HTTP_400_BAD_REQUEST)

        from_status = ruta.estatus
        ruta.estatus = estatus_nuevo
        ruta.save(update_fields=["estatus", "updated_at"])
        log_event(
            request.user,
            "UPDATE",
            "logistica.RutaEntrega",
            str(ruta.id),
            {"folio": ruta.folio, "from": from_status, "to": estatus_nuevo},
        )
        return Response(LogisticaRutaSerializer(ruta).data, status=status.HTTP_200_OK)


class LogisticaDashboardView(_LogisticaBaseView):
    def get(self, request):
        if not can_view_logistica(request.user):
            return Response({"detail": "No tienes permisos para consultar Logística."}, status=status.HTTP_403_FORBIDDEN)

        return Response(
            {
                "rutas": {
                    "total": RutaEntrega.objects.count(),
                    "planeadas": RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_PLANEADA).count(),
                    "en_ruta": RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).count(),
                    "completadas": RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_COMPLETADA).count(),
                },
                "entregas": {
                    "total": EntregaRuta.objects.count(),
                    "pendientes": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_PENDIENTE).count(),
                    "en_camino": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_EN_CAMINO).count(),
                    "entregadas": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA).count(),
                    "incidencia": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_INCIDENCIA).count(),
                },
                "monto_estimado_total": str(
                    EntregaRuta.objects.aggregate(total=Sum("monto_estimado")).get("total") or Decimal("0")
                ),
            },
            status=status.HTTP_200_OK,
        )
