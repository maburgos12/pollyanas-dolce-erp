from __future__ import annotations

import hashlib
import re
from uuid import uuid4

from django.db import IntegrityError, connection, transaction
from django.db.models import Prefetch, Q
from django.db.models.functions import Lower
from django.urls import reverse
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from api.omnichannel_serializers import (
    OmnichannelCustomerOutputSerializer,
    OmnichannelOrderInputSerializer,
)
from api.public_views import _auth_public_client, _log_access
from crm.models import Cliente, DireccionCliente, PedidoCliente
from logistica.models import SolicitudDomicilio


IDEMPOTENCY_CONFLICT_CODE = "OMNICHANNEL_IDEMPOTENCY_CONFLICT"
OMNICHANNEL_CAPABILITY = "OMNICHANNEL"


def _normalize_phone(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_text(value: str) -> str:
    return (value or "").strip()


def _lock_external_key(external_source: str, external_id: str) -> None:
    if connection.vendor != "postgresql":
        raise RuntimeError("La API omnicanal requiere PostgreSQL")
    digest = hashlib.sha256(f"{external_source}\0{external_id}".encode()).digest()
    lock_id = int.from_bytes(digest[:8], byteorder="big", signed=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [lock_id])


def _lock_pedido_folio() -> None:
    digest = hashlib.sha256(b"crm-pedido-folio").digest()
    lock_id = int.from_bytes(digest[:8], byteorder="big", signed=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [lock_id])


def _authorize_omnichannel(api_client, request):
    if api_client.has_capability(OMNICHANNEL_CAPABILITY):
        return None
    _log_access(api_client, request, status.HTTP_403_FORBIDDEN)
    return Response(
        {
            "detail": "La API key no tiene autorización omnicanal.",
            "code": "OMNICHANNEL_CAPABILITY_REQUIRED",
        },
        status=status.HTTP_403_FORBIDDEN,
    )


def _find_customer(data: dict) -> Cliente | None:
    phone = _normalize_phone(data.get("telefono", ""))
    if phone:
        match = (
            Cliente.objects.extra(
                where=["regexp_replace(telefono, '[^0-9]', '', 'g') = %s"],
                params=[phone],
            )
            .order_by("id")
            .first()
        )
        if match:
            return match

    email = _normalize_email(data.get("email", ""))
    if email:
        return Cliente.objects.annotate(email_lower=Lower("email")).filter(
            email_lower=email,
        ).order_by("id").first()
    return None


def _get_or_create_customer(data: dict) -> Cliente:
    customer = _find_customer(data)
    if customer:
        return customer
    return Cliente.objects.create(
        nombre=_normalize_text(data["nombre"]),
        telefono=_normalize_phone(data.get("telefono", "")),
        email=_normalize_email(data.get("email", "")),
    )


def _get_or_create_address(customer: Cliente, data: dict) -> DireccionCliente:
    address_text = _normalize_text(data["direccion"])
    normalized = DireccionCliente.normalizar_direccion(address_text)
    address = DireccionCliente.objects.filter(
        cliente=customer,
        direccion_normalizada=normalized,
    ).first()
    if address:
        return address
    try:
        with transaction.atomic():
            return DireccionCliente.objects.create(
                cliente=customer,
                direccion=address_text,
                referencias=_normalize_text(data.get("referencias", "")),
                latitud=data.get("latitud"),
                longitud=data.get("longitud"),
                place_id=_normalize_text(data.get("place_id", "")),
            )
    except IntegrityError:
        return DireccionCliente.objects.get(
            cliente=customer,
            direccion_normalizada=normalized,
        )


def _canonical_payload(data: dict) -> dict:
    customer = data["cliente"]
    address = data["direccion"]
    detail = data["pedido"]
    return {
        "external_source": data["external_source"],
        "external_id": data["external_id"],
        "canal": data["canal"],
        "cliente": {
            "nombre": _normalize_text(customer["nombre"]),
            "telefono": _normalize_phone(customer.get("telefono", "")),
            "email": _normalize_email(customer.get("email", "")),
        },
        "direccion": {
            "direccion_normalizada": DireccionCliente.normalizar_direccion(address["direccion"]),
            "referencias": _normalize_text(address.get("referencias", "")),
            "latitud": str(address["latitud"]) if address.get("latitud") is not None else None,
            "longitud": str(address["longitud"]) if address.get("longitud") is not None else None,
            "place_id": _normalize_text(address.get("place_id", "")),
        },
        "pedido": {
            "descripcion": _normalize_text(detail["descripcion"]),
            "fecha_compromiso": (
                detail["fecha_compromiso"].isoformat()
                if detail.get("fecha_compromiso") is not None
                else None
            ),
            "monto_estimado": str(detail["monto_estimado"]),
        },
    }


def _snapshot_matches(order: PedidoCliente, data: dict) -> bool:
    return order.payload_snapshot == _canonical_payload(data)


def _conflict_response(api_client, request, *, detail: str, code: str):
    _log_access(api_client, request, status.HTTP_409_CONFLICT)
    return Response(
        {"detail": detail, "code": code},
        status=status.HTTP_409_CONFLICT,
    )


def _resolve_existing_order(api_client, request, order: PedidoCliente, data: dict):
    if (
        order.public_api_client_id is not None
        and order.public_api_client_id != api_client.id
    ):
        return None, _conflict_response(
            api_client,
            request,
            detail="La clave externa pertenece a otro cliente API.",
            code="OMNICHANNEL_ORDER_OWNERSHIP_CONFLICT",
        )
    if not order.payload_snapshot:
        return None, _conflict_response(
            api_client,
            request,
            detail="La clave externa pertenece a un pedido anterior sin snapshot verificable.",
            code="LEGACY_EXTERNAL_ORDER_CONFLICT",
        )
    if not _snapshot_matches(order, data):
        return None, _conflict_response(
            api_client,
            request,
            detail="La clave externa ya existe con contenido distinto.",
            code=IDEMPOTENCY_CONFLICT_CODE,
        )
    if not order.tracking_token:
        return None, _conflict_response(
            api_client,
            request,
            detail="El pedido omnicanal existe pero no tiene seguimiento seguro.",
            code="OMNICHANNEL_ORDER_INCOMPLETE",
        )
    delivery = SolicitudDomicilio.objects.filter(pedido_cliente=order).first()
    if not delivery:
        return None, _conflict_response(
            api_client,
            request,
            detail="El pedido omnicanal existe pero su solicitud de domicilio está incompleta.",
            code="OMNICHANNEL_ORDER_INCOMPLETE",
        )
    return delivery, None


def _response_payload(request, order: PedidoCliente, delivery: SolicitudDomicilio, *, created: bool):
    return {
        "cliente_id": order.cliente_id,
        "direccion_id": order.direccion_entrega_id,
        "pedido_id": order.id,
        "solicitud_domicilio_id": delivery.id,
        "created": created,
        "links": {
            "pedido_seguimiento": request.build_absolute_uri(
                reverse(
                    "api_public_omnichannel_order_status",
                    kwargs={"tracking_token": order.tracking_token},
                ),
            ),
        },
    }


class PublicOmnichannelOrdersView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error

        serializer = OmnichannelOrderInputSerializer(data=request.data)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        data = serializer.validated_data

        with transaction.atomic():
            _lock_external_key(data["external_source"], data["external_id"])
            order = (
                PedidoCliente.objects.select_related("cliente", "direccion_entrega")
                .filter(
                    external_source=data["external_source"],
                    external_id=data["external_id"],
                )
                .first()
            )
            if order:
                delivery, conflict = _resolve_existing_order(
                    api_client,
                    request,
                    order,
                    data,
                )
                if conflict:
                    return conflict
                payload = _response_payload(request, order, delivery, created=False)
                response_status = status.HTTP_200_OK
            else:
                customer = _get_or_create_customer(data["cliente"])
                address = _get_or_create_address(customer, data["direccion"])
                detail = data["pedido"]
                try:
                    with transaction.atomic():
                        _lock_pedido_folio()
                        order = PedidoCliente.objects.create(
                            cliente=customer,
                            direccion_entrega=address,
                            external_source=data["external_source"],
                            external_id=data["external_id"],
                            payload_snapshot=_canonical_payload(data),
                            tracking_token=uuid4(),
                            public_api_client=api_client,
                            canal=data["canal"],
                            descripcion=_normalize_text(detail["descripcion"]),
                            fecha_compromiso=detail.get("fecha_compromiso"),
                            monto_estimado=detail["monto_estimado"],
                        )
                except IntegrityError:
                    order = PedidoCliente.objects.select_related(
                        "cliente",
                        "direccion_entrega",
                    ).filter(
                        external_source=data["external_source"],
                        external_id=data["external_id"],
                    ).first()
                    if not order:
                        raise
                    delivery, conflict = _resolve_existing_order(
                        api_client,
                        request,
                        order,
                        data,
                    )
                    if conflict:
                        return conflict
                    payload = _response_payload(request, order, delivery, created=False)
                    response_status = status.HTTP_200_OK
                else:
                    delivery = SolicitudDomicilio.objects.create(
                        pedido_cliente=order,
                        cliente=customer,
                        direccion_cliente=address,
                        cliente_nombre=customer.nombre,
                        cliente_telefono=customer.telefono,
                        direccion=address.direccion,
                        canal_origen=order.canal,
                        canal_detalle=data["external_source"],
                        notas=address.referencias,
                    )
                    payload = _response_payload(request, order, delivery, created=True)
                    response_status = status.HTTP_201_CREATED

        _log_access(api_client, request, response_status)
        return Response(payload, status=response_status)


class PublicOmnichannelCustomersView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error

        query = _normalize_text(request.query_params.get("q", ""))
        if len(query) < 3:
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(
                {
                    "detail": "q debe contener al menos 3 caracteres.",
                    "code": "OMNICHANNEL_SEARCH_TERM_TOO_SHORT",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        customers = Cliente.objects.filter(activo=True)
        phone = _normalize_phone(query)
        if phone:
            customers = customers.extra(
                where=[
                    "(nombre ILIKE %s OR email ILIKE %s "
                    "OR regexp_replace(telefono, '[^0-9]', '', 'g') LIKE %s)"
                ],
                params=[f"%{query}%", f"%{query}%", f"%{phone}%"],
            )
        else:
            customers = customers.filter(
                Q(nombre__icontains=query) | Q(email__icontains=query),
            )
        customers = customers.prefetch_related(
            Prefetch(
                "direcciones",
                queryset=DireccionCliente.objects.filter(activa=True),
                to_attr="direcciones_activas",
            ),
        ).order_by("nombre", "id")[:20]

        data = OmnichannelCustomerOutputSerializer(customers, many=True).data
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response({"count": len(data), "results": data})


class PublicOmnichannelOrderStatusView(APIView):
    permission_classes = [AllowAny]

    def get(self, request, tracking_token):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error

        order = PedidoCliente.objects.filter(
            tracking_token=tracking_token,
            public_api_client=api_client,
        ).exclude(
            payload_snapshot={},
        ).first()
        if not order:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            return Response(
                {"detail": "Pedido omnicanal no encontrado."},
                status=status.HTTP_404_NOT_FOUND,
            )
        delivery = SolicitudDomicilio.objects.filter(pedido_cliente=order).first()
        if not delivery:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            return Response(
                {"detail": "Pedido omnicanal no encontrado."},
                status=status.HTTP_404_NOT_FOUND,
            )

        payload = {
            "pedido_id": order.id,
            "solicitud_domicilio_id": delivery.id,
            "canal": order.canal,
            "estatus": order.estatus,
            "estatus_domicilio": delivery.estatus,
            "created_at": order.created_at.isoformat(),
            "updated_at": order.updated_at.isoformat(),
        }
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload, status=status.HTTP_200_OK)
