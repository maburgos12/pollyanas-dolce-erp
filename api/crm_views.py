from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.db import models, transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import can_manage_crm, can_view_crm
from core.audit import log_event
from crm.models import Cliente, PedidoCliente, SeguimientoPedido

from .crm_serializers import CRMClienteSerializer, CRMPedidoSerializer, CRMSeguimientoCreateSerializer


class _CRMBaseView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _bounded_int(value, *, default: int, min_value: int, max_value: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(min_value, min(parsed, max_value))

    @staticmethod
    def _to_decimal(value) -> Decimal:
        try:
            return Decimal(str(value or "0"))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal("0")


class CRMClientesView(_CRMBaseView):
    def get(self, request):
        if not can_view_crm(request.user):
            return Response({"detail": "No tienes permisos para consultar CRM."}, status=status.HTTP_403_FORBIDDEN)

        q = (request.query_params.get("q") or "").strip()
        activo_raw = (request.query_params.get("activo") or "").strip().lower()
        limit = self._bounded_int(request.query_params.get("limit"), default=50, min_value=1, max_value=500)
        offset = self._bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)

        qs = Cliente.objects.all()
        if q:
            qs = qs.filter(
                Q(nombre__icontains=q)
                | Q(codigo__icontains=q)
                | Q(telefono__icontains=q)
                | Q(email__icontains=q)
            )
        if activo_raw in {"1", "true", "yes"}:
            qs = qs.filter(activo=True)
        elif activo_raw in {"0", "false", "no"}:
            qs = qs.filter(activo=False)

        total = qs.count()
        rows = list(qs.order_by("nombre", "id")[offset : offset + limit])
        serializer = CRMClienteSerializer(rows, many=True)
        return Response(
            {
                "count": total,
                "limit": limit,
                "offset": offset,
                "results": serializer.data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not can_manage_crm(request.user):
            return Response({"detail": "No tienes permisos para crear clientes CRM."}, status=status.HTTP_403_FORBIDDEN)

        serializer = CRMClienteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        cliente = serializer.save()
        log_event(
            request.user,
            "CREATE",
            "crm.Cliente",
            str(cliente.id),
            {
                "codigo": cliente.codigo,
                "nombre": cliente.nombre,
            },
        )
        return Response(CRMClienteSerializer(cliente).data, status=status.HTTP_201_CREATED)


class CRMPedidosView(_CRMBaseView):
    def get(self, request):
        if not can_view_crm(request.user):
            return Response({"detail": "No tienes permisos para consultar CRM."}, status=status.HTTP_403_FORBIDDEN)

        q = (request.query_params.get("q") or "").strip()
        estatus = (request.query_params.get("estatus") or "").strip()
        prioridad = (request.query_params.get("prioridad") or "").strip()
        cliente_id = request.query_params.get("cliente_id")
        limit = self._bounded_int(request.query_params.get("limit"), default=50, min_value=1, max_value=500)
        offset = self._bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)

        qs = PedidoCliente.objects.select_related("cliente")
        if q:
            qs = qs.filter(
                Q(folio__icontains=q)
                | Q(cliente__nombre__icontains=q)
                | Q(descripcion__icontains=q)
                | Q(sucursal__icontains=q)
            )
        if estatus:
            qs = qs.filter(estatus=estatus)
        if prioridad:
            qs = qs.filter(prioridad=prioridad)
        if cliente_id:
            try:
                qs = qs.filter(cliente_id=int(cliente_id))
            except (TypeError, ValueError):
                return Response({"detail": "cliente_id inválido."}, status=status.HTTP_400_BAD_REQUEST)

        total = qs.count()
        rows = list(qs.order_by("-created_at", "-id")[offset : offset + limit])
        serializer = CRMPedidoSerializer(rows, many=True)
        return Response(
            {
                "count": total,
                "limit": limit,
                "offset": offset,
                "results": serializer.data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not can_manage_crm(request.user):
            return Response({"detail": "No tienes permisos para crear pedidos CRM."}, status=status.HTTP_403_FORBIDDEN)

        serializer = CRMPedidoSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        with transaction.atomic():
            pedido = serializer.save(created_by=request.user)
            SeguimientoPedido.objects.create(
                pedido=pedido,
                estatus_nuevo=pedido.estatus,
                comentario="Alta de pedido API",
                created_by=request.user,
            )

        log_event(
            request.user,
            "CREATE",
            "crm.PedidoCliente",
            str(pedido.id),
            {
                "folio": pedido.folio,
                "cliente_id": pedido.cliente_id,
                "estatus": pedido.estatus,
                "monto_estimado": str(pedido.monto_estimado),
            },
        )
        return Response(CRMPedidoSerializer(pedido).data, status=status.HTTP_201_CREATED)


class CRMPedidoSeguimientoView(_CRMBaseView):
    def post(self, request, pedido_id: int):
        if not can_manage_crm(request.user):
            return Response({"detail": "No tienes permisos para registrar seguimiento CRM."}, status=status.HTTP_403_FORBIDDEN)

        pedido = get_object_or_404(PedidoCliente, pk=pedido_id)
        serializer = CRMSeguimientoCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        comentario = serializer.validated_data.get("comentario", "")
        estatus_nuevo = serializer.validated_data.get("estatus_nuevo", "")

        with transaction.atomic():
            estatus_anterior = pedido.estatus
            if estatus_nuevo and estatus_nuevo != pedido.estatus:
                pedido.estatus = estatus_nuevo
                pedido.save(update_fields=["estatus", "updated_at"])
            seguimiento = SeguimientoPedido.objects.create(
                pedido=pedido,
                estatus_anterior=estatus_anterior if estatus_nuevo else "",
                estatus_nuevo=estatus_nuevo,
                comentario=comentario,
                created_by=request.user,
            )

        log_event(
            request.user,
            "UPDATE",
            "crm.PedidoCliente",
            str(pedido.id),
            {
                "folio": pedido.folio,
                "estatus_anterior": estatus_anterior,
                "estatus_nuevo": pedido.estatus,
                "comentario": comentario,
            },
        )
        return Response(
            {
                "pedido_id": pedido.id,
                "folio": pedido.folio,
                "estatus": pedido.estatus,
                "seguimiento_id": seguimiento.id,
            },
            status=status.HTTP_200_OK,
        )


class CRMDashboardView(_CRMBaseView):
    def get(self, request):
        if not can_view_crm(request.user):
            return Response({"detail": "No tienes permisos para consultar CRM."}, status=status.HTTP_403_FORBIDDEN)

        by_status = {
            key: PedidoCliente.objects.filter(estatus=key).count()
            for key, _ in PedidoCliente.ESTATUS_CHOICES
        }
        total_abiertos = PedidoCliente.objects.exclude(
            estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO]
        ).count()
        monto_abierto = (
            PedidoCliente.objects.exclude(estatus__in=[PedidoCliente.ESTATUS_ENTREGADO, PedidoCliente.ESTATUS_CANCELADO])
            .aggregate(total=models.Sum("monto_estimado"))
            .get("total")
            or Decimal("0")
        )
        return Response(
            {
                "clientes": {
                    "total": Cliente.objects.count(),
                    "activos": Cliente.objects.filter(activo=True).count(),
                },
                "pedidos": {
                    "total": PedidoCliente.objects.count(),
                    "abiertos": total_abiertos,
                    "monto_abierto": str(monto_abierto),
                    "by_status": by_status,
                },
            },
            status=status.HTTP_200_OK,
        )
