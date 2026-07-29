from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timedelta
from uuid import uuid4

import requests
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.db import IntegrityError, connection, transaction
from django.db.models import Prefetch, Q
from django.db.models.functions import Lower
from django.http import Http404
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from api.omnichannel_serializers import (
    OmnichannelDeliveryPreparationStatusSerializer,
    OmnichannelDeliveryQuerySerializer,
    OmnichannelDeliveryStatusSerializer,
    OmnichannelCustomerOutputSerializer,
    OmnichannelOrderInputSerializer,
    PointLinkedOrderSerializer,
    PointNoteSearchSerializer,
)
from api.public_views import _auth_public_client, _log_access
from api.logistica_public_views import _authorize_logistica_assignment
from crm.services.point_order_link import (
    LinkPointOrderCommand,
    LinkPointOrderResult,
    link_point_note,
    verify_point_link_fingerprint,
)
from crm.models import Cliente, DireccionCliente, PedidoCliente
from logistica.models import SolicitudDomicilio
from logistica.services_domicilio_status import (
    DomicilioStatusError,
    prepare_domicilio_status,
    update_domicilio_status,
)
from pos_bridge.services.point_note_detail_service import (
    PointNoteContractError,
    PointNoteDetailError,
    PointNoteDetailService,
    PointNoteIntegrityError,
    PointNoteNotFoundError,
    PointNoteUnavailableError,
)
from pos_bridge.services.point_ticket_threshold_service import (
    PointTicketThresholdService,
)
from pos_bridge.utils.helpers import normalize_text
from pos_bridge.utils.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ExtractionError,
    PosBridgeError,
)


IDEMPOTENCY_CONFLICT_CODE = "OMNICHANNEL_IDEMPOTENCY_CONFLICT"
OMNICHANNEL_CAPABILITY = "OMNICHANNEL"
DOMICILIOS_PREPARATION_CAPABILITY = "DOMICILIOS_PREPARATION"
POINT_SEARCH_MAX_RESULTS = 20
POINT_SEARCH_DEFAULT_DAYS = 3


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


def _authorize_domicilios_preparation(api_client, request):
    if api_client.has_capability(DOMICILIOS_PREPARATION_CAPABILITY):
        return None
    _log_access(api_client, request, status.HTTP_403_FORBIDDEN)
    return Response(
        {
            "detail": "La API key no tiene autorización para preparar domicilios.",
            "code": "DOMICILIOS_PREPARATION_CAPABILITY_REQUIRED",
        },
        status=status.HTTP_403_FORBIDDEN,
    )


def _point_error_response(api_client, request, exc):
    if isinstance(exc, PointNoteNotFoundError):
        response_status = status.HTTP_404_NOT_FOUND
        code = "POINT_NOTE_NOT_FOUND"
        retryable = False
    elif isinstance(exc, PointNoteUnavailableError):
        response_status = status.HTTP_503_SERVICE_UNAVAILABLE
        code = "POINT_UNAVAILABLE"
        retryable = True
    else:
        response_status = status.HTTP_502_BAD_GATEWAY
        code = "POINT_CONTRACT_ERROR"
        retryable = False
    _log_access(api_client, request, response_status)
    return Response(
        {
            "detail": "No fue posible consultar la nota en Point.",
            "code": code,
            "retryable": retryable,
        },
        status=response_status,
    )


def _serialize_point_note(note):
    return {
        "pk_nota": note.pk_nota,
        "folio": note.folio,
        "sucursal": note.branch_name,
        "fecha_hora": note.sold_at.isoformat(),
        "total": note.total,
        "facturado": note.invoiced,
        "tipo_pago": note.payment_type,
        "canal_point": note.point_channel,
        "productos": [
            {
                "codigo": line.point_code,
                "descripcion": line.description,
                "cantidad": line.quantity,
                "precio_unitario": line.unit_price,
                "descuento": line.discount,
                "total": line.line_total,
            }
            for line in note.lines
        ],
    }


def _point_boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().casefold() in {"1", "true", "si", "sí"}


def _point_row_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    epoch_match = re.fullmatch(r"/Date\((-?\d+)(?:[+-]\d+)?\)/", raw)
    if epoch_match:
        try:
            return datetime.fromtimestamp(
                int(epoch_match.group(1)) / 1000,
                tz=timezone.get_current_timezone(),
            ).date()
        except (OverflowError, OSError, ValueError):
            return None
    for date_format in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw[:10], date_format).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _coerce_point_error(exc):
    if isinstance(exc, PointNoteDetailError):
        return exc
    if isinstance(exc, (ConfigurationError, ExtractionError)):
        return PointNoteContractError(
            "La integración Point no cumple el contrato requerido.",
        )
    if isinstance(exc, (AuthenticationError, PosBridgeError, requests.RequestException)):
        return PointNoteUnavailableError(
            "La sesión Point no está disponible.",
        )
    return PointNoteUnavailableError("Point no está disponible.")


def _fetch_point_note_candidates(*, folio, sucursal, fecha=None):
    """Query Point's confirmed note report and expose only operational fields."""
    try:
        service = PointTicketThresholdService()
    except (PosBridgeError, requests.RequestException) as exc:
        raise _coerce_point_error(exc) from exc
    end_date = fecha or timezone.localdate()
    start_date = fecha or (
        end_date - timedelta(days=POINT_SEARCH_DEFAULT_DAYS - 1)
    )
    params = service._build_params(start_date=start_date, end_date=end_date)
    request_url = (
        service.settings.base_url.rstrip("/")
        + service.NOTES_BY_PLAZA_PATH
    )
    try:
        auth_session = service.http_session_service.create()
    except (PosBridgeError, requests.RequestException) as exc:
        raise _coerce_point_error(exc) from exc
    try:
        try:
            response = auth_session.session.get(
                request_url,
                params=params,
                timeout=service.settings.timeout_ms / 1000,
            )
            response.raise_for_status()
        except Exception as exc:
            raise PointNoteUnavailableError(
                "Point no respondió la búsqueda de notas.",
            ) from exc
        try:
            payload = response.json() or []
        except (TypeError, ValueError) as exc:
            raise PointNoteContractError(
                "Point devolvió JSON inválido al buscar notas.",
            ) from exc
    finally:
        try:
            auth_session.session.close()
        except Exception:  # noqa: BLE001
            pass
    if not isinstance(payload, list):
        raise PointNoteContractError(
            "Point devolvió una estructura inválida al buscar notas.",
        )

    expected_folio = _normalize_text(folio).casefold()
    expected_branch = normalize_text(sucursal)
    candidates = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        row_folio = str(row.get("FOLIO") or row.get("Folio") or "").strip()
        row_branch = str(row.get("SUCURSAL") or row.get("Sucursal") or "").strip()
        if row_folio.casefold() != expected_folio:
            continue
        if normalize_text(row_branch) != expected_branch:
            continue
        pk_nota = str(row.get("PK_NOTA") or row.get("Pk_Nota") or "").strip()
        if not pk_nota:
            continue
        day = _point_row_date(row.get("DIA") or row.get("Dia"))
        # Point occasionally returns malformed report rows. A row without a
        # trustworthy date is never exposed as a candidate.
        if day is None or (fecha is not None and day != fecha):
            continue
        candidates.append(
            {
                "pk_nota": pk_nota,
                "folio": row_folio,
                "sucursal": row_branch,
                "fecha": day.isoformat(),
                "hora": str(row.get("HORA") or row.get("Hora") or "").strip(),
                "total": str(
                    row.get("MONTO")
                    or row.get("Monto")
                    or row.get("Importe")
                    or "0"
                ),
                "facturado": _point_boolean(
                    row.get("FACTURADO", row.get("Facturado", False))
                ),
                "canal_point": str(
                    row.get("CANAL_VENTA") or row.get("Canal_Venta") or ""
                ).strip(),
                "tipo_pago": str(
                    row.get("TIPO") or row.get("Tipo") or ""
                ).strip(),
            },
        )
        if len(candidates) >= POINT_SEARCH_MAX_RESULTS:
            break
    return candidates


def _consume_point_search_limit(api_client):
    limit = int(
        getattr(
            settings,
            "OMNICHANNEL_POINT_SEARCH_RATE_LIMIT_PER_MINUTE",
            10,
        ),
    )
    if limit <= 0:
        return True
    bucket = timezone.now().strftime("%Y%m%d%H%M")
    key = f"omnichannel_point_search:{api_client.id}:{bucket}"
    cache.add(key, 0, timeout=90)
    try:
        current = cache.incr(key)
    except ValueError:
        cache.set(key, 1, timeout=90)
        current = 1
    return current <= limit


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
        "instrucciones_entrega": _normalize_text(data.get("instrucciones_entrega", "")),
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
    snapshot = dict(order.payload_snapshot)
    if "instrucciones_entrega" not in snapshot:
        # Pre-fix snapshots did not record this field. Recover it only from the
        # canonical delivery, never by assuming it was blank.
        delivery = order.solicitudes_domicilio.only("instrucciones_entrega").first()
        snapshot["instrucciones_entrega"] = _normalize_text(
            delivery.instrucciones_entrega if delivery else ""
        )
    return snapshot == _canonical_payload(data)


def _conflict_response(api_client, request, *, detail: str, code: str):
    _log_access(api_client, request, status.HTTP_409_CONFLICT)
    return Response(
        {"detail": detail, "code": code},
        status=status.HTTP_409_CONFLICT,
    )


def _resolve_existing_order(api_client, request, order: PedidoCliente, data: dict):
    if order.public_api_client_id != api_client.id:
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
                    references = _normalize_text(address.referencias)
                    instructions = _normalize_text(data.get("instrucciones_entrega", ""))
                    notes = "\n".join(part for part in (
                        f"Referencias: {references}" if references else "",
                        f"Instrucciones: {instructions}" if instructions else "",
                    ) if part)
                    delivery = SolicitudDomicilio.objects.create(
                        pedido_cliente=order,
                        cliente=customer,
                        direccion_cliente=address,
                        cliente_nombre=customer.nombre,
                        cliente_telefono=customer.telefono,
                        direccion=address.direccion,
                        canal_origen=order.canal,
                        canal_detalle=data["external_source"],
                        estatus=(
                            SolicitudDomicilio.ESTATUS_CONFIRMADO
                            if (
                                data["external_source"] == "POLLYANAS_ECOMMERCE"
                                and data["canal"] == PedidoCliente.CANAL_WEB
                            )
                            else SolicitudDomicilio.ESTATUS_PENDIENTE_POINT
                        ),
                        notas=notes,
                        instrucciones_entrega=instructions,
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


class PublicOmnichannelPointNotesView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error
        if not _consume_point_search_limit(api_client):
            _log_access(api_client, request, status.HTTP_429_TOO_MANY_REQUESTS)
            return Response(
                {
                    "detail": "Límite de búsquedas Point por minuto excedido.",
                    "code": "POINT_SEARCH_RATE_LIMITED",
                },
                status=status.HTTP_429_TOO_MANY_REQUESTS,
            )
        serializer = PointNoteSearchSerializer(data=request.query_params)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        try:
            results = _fetch_point_note_candidates(**serializer.validated_data)
        except (
            PointNoteNotFoundError,
            PointNoteUnavailableError,
            PointNoteContractError,
            PointNoteIntegrityError,
        ) as exc:
            return _point_error_response(api_client, request, exc)
        except (PosBridgeError, requests.RequestException) as exc:
            return _point_error_response(
                api_client,
                request,
                _coerce_point_error(exc),
            )
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response({"count": len(results), "results": results})


class PublicOmnichannelPointNoteDetailView(APIView):
    permission_classes = [AllowAny]

    def get(self, request, pk_nota):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error
        if not str(pk_nota).strip() or len(str(pk_nota)) > 120:
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(
                {"detail": "pk_nota no es válido."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            note = PointNoteDetailService().fetch(pk_nota=str(pk_nota))
        except (
            PointNoteNotFoundError,
            PointNoteUnavailableError,
            PointNoteContractError,
            PointNoteIntegrityError,
        ) as exc:
            return _point_error_response(api_client, request, exc)
        except (PosBridgeError, requests.RequestException) as exc:
            return _point_error_response(
                api_client,
                request,
                _coerce_point_error(exc),
            )
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(_serialize_point_note(note))


def _point_replay_matches(order, delivery, command):
    persisted = order.point_link_fingerprint
    return bool(persisted) and verify_point_link_fingerprint(command, persisted)


class PublicOmnichannelPointOrdersView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error
        if not api_client.created_by_id or not api_client.created_by.is_active:
            _log_access(api_client, request, status.HTTP_409_CONFLICT)
            return Response(
                {
                    "detail": "La API key no tiene un usuario auditor activo.",
                    "code": "OMNICHANNEL_AUDIT_ACTOR_REQUIRED",
                },
                status=status.HTTP_409_CONFLICT,
            )
        serializer = PointLinkedOrderSerializer(data=request.data)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        data = serializer.validated_data
        customer = data["cliente"]
        address = data["direccion"]
        command = LinkPointOrderCommand(
            pk_nota=data["pk_nota"],
            channel=data["canal"],
            customer_name=customer["nombre"],
            customer_phone=customer.get("telefono", ""),
            customer_email=customer.get("email", ""),
            address=address["direccion"],
            references=address.get("referencias", ""),
            latitude=address.get("latitud"),
            longitude=address.get("longitud"),
            place_id=address.get("place_id", ""),
            social_reference=data.get("social_reference", ""),
            delivery_window_start=data.get("ventana_inicio"),
            delivery_window_end=data.get("ventana_fin"),
            instructions=data.get("instrucciones_entrega", ""),
        )
        try:
            with transaction.atomic():
                existing = (
                    PedidoCliente.objects.select_for_update(of=("self",))
                    .select_related("cliente", "direccion_entrega")
                    .filter(point_note_id=command.pk_nota)
                    .first()
                )
                if existing is not None:
                    delivery = (
                        SolicitudDomicilio.objects.select_for_update()
                        .filter(pedido_cliente=existing)
                        .first()
                    )
                    if (
                        existing.public_api_client_id != api_client.id
                        or delivery is None
                        or not _point_replay_matches(
                            existing,
                            delivery,
                            command,
                        )
                    ):
                        raise ValidationError(
                            {"pk_nota": "La nota Point ya está vinculada."},
                        )
                    result = LinkPointOrderResult(
                        order=existing,
                        delivery=delivery,
                        created=False,
                    )
                    order = existing
                else:
                    result = link_point_note(
                        command=command,
                        actor=api_client.created_by,
                    )
                    order = PedidoCliente.objects.select_for_update().get(
                        pk=result.order.pk,
                    )
                if (
                    not result.created
                    and not _point_replay_matches(
                        order,
                        result.delivery,
                        command,
                    )
                ):
                    raise ValidationError(
                        {"pk_nota": "La nota Point ya está vinculada."},
                    )
                if order.public_api_client_id not in (None, api_client.id):
                    raise ValidationError(
                        {"pk_nota": "La nota Point ya está vinculada."},
                    )
                if order.public_api_client_id is None:
                    if not result.created:
                        raise ValidationError(
                            {"pk_nota": "La nota Point ya está vinculada."},
                        )
                    order.public_api_client = api_client
                    order.save(update_fields=["public_api_client", "updated_at"])
        except (
            PointNoteNotFoundError,
            PointNoteUnavailableError,
            PointNoteContractError,
            PointNoteIntegrityError,
        ) as exc:
            return _point_error_response(api_client, request, exc)
        except (PosBridgeError, requests.RequestException) as exc:
            return _point_error_response(
                api_client,
                request,
                _coerce_point_error(exc),
            )
        except ValidationError:
            _log_access(api_client, request, status.HTTP_409_CONFLICT)
            return Response(
                {
                    "detail": "No fue posible vincular la nota Point.",
                    "code": "POINT_ORDER_CONFLICT",
                },
                status=status.HTTP_409_CONFLICT,
            )
        payload = {
            "pedido_id": order.id,
            "solicitud_domicilio_id": result.delivery.id,
            "cliente_id": order.cliente_id,
            "direccion_id": order.direccion_entrega_id,
            "created": result.created,
        }
        response_status = (
            status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
        )
        _log_access(api_client, request, response_status)
        return Response(payload, status=response_status)


def _owned_deliveries(api_client):
    return (
        SolicitudDomicilio.objects.filter(
            pedido_cliente__public_api_client=api_client,
            legacy_without_point=False,
        )
        .filter(
            (
                Q(pedido_cliente__external_source="POINT")
                & Q(pedido_cliente__point_note_id__gt="")
                & ~Q(pedido_cliente__canal=PedidoCliente.CANAL_WEB)
            )
            | (
                Q(pedido_cliente__external_source="POLLYANAS_ECOMMERCE")
                & Q(pedido_cliente__external_id__gt="")
                & Q(pedido_cliente__canal=PedidoCliente.CANAL_WEB)
            )
            | (
                Q(pedido_cliente__external_source="POLLYANAS_OPERACIONES")
                & Q(pedido_cliente__external_id__gt="")
                & ~Q(pedido_cliente__canal=PedidoCliente.CANAL_WEB)
            )
        )
        .select_related(
            "pedido_cliente",
            "pedido_cliente__cliente",
            "direccion_cliente",
            "repartidor",
            "repartidor__user",
            "unidad",
        )
    )


def _mutable_owned_deliveries(api_client):
    """Only Point-backed or trusted paid WEB orders may advance."""
    return _owned_deliveries(api_client).filter(
        Q(pedido_cliente__point_note_id__gt="")
        | (
            Q(pedido_cliente__external_source="POLLYANAS_ECOMMERCE")
            & Q(pedido_cliente__external_id__gt="")
            & Q(pedido_cliente__canal=PedidoCliente.CANAL_WEB)
        )
    )


def _serialize_delivery_summary(delivery):
    order = delivery.pedido_cliente
    return {
        "id": delivery.id,
        "pedido_id": order.id,
        "folio": order.folio or order.external_id,
        "folio_point": order.point_note_folio,
        "canal": order.canal,
        "estatus": delivery.estatus,
        "ventana_inicio": delivery.ventana_inicio,
        "ventana_fin": delivery.ventana_fin,
        "total": order.monto_estimado,
        "cliente_nombre": delivery.cliente_nombre,
        "direccion": delivery.direccion,
        "repartidor_id": delivery.repartidor_id,
        "created_at": delivery.created_at,
        "external_source": order.external_source,
        "external_id": order.external_id,
    }


def _serialize_delivery_source(order):
    if order.external_source == "POINT":
        return {
            "tipo": "POINT",
            "pk_nota": order.point_note_id,
            "folio": order.point_note_folio,
            "sucursal": order.sucursal,
            "fecha_consulta": order.point_note_fetched_at,
        }
    if order.external_source not in {
        "POLLYANAS_ECOMMERCE",
        "POLLYANAS_OPERACIONES",
    }:
        raise ValueError("Fuente omnicanal no soportada.")
    return {
        "tipo": order.external_source,
        "external_id": order.external_id,
        "folio": order.folio or order.external_id,
        "sucursal": order.sucursal,
        "fecha_consulta": order.created_at,
    }


def _serialize_delivery_detail(delivery):
    order = delivery.pedido_cliente
    customer = order.cliente
    address = delivery.direccion_cliente or order.direccion_entrega
    snapshot = order.point_note_snapshot or {}
    return {
        "id": delivery.id,
        "pedido_id": order.id,
        "fuente": _serialize_delivery_source(order),
        "cliente": {
            "id": customer.id,
            "codigo": customer.codigo,
            "nombre": customer.nombre,
            "telefono": customer.telefono,
            "email": customer.email,
        },
        "direccion": {
            "id": address.id if address else None,
            "direccion": address.direccion if address else delivery.direccion,
            "referencias": address.referencias if address else delivery.notas,
            "latitud": address.latitud if address else None,
            "longitud": address.longitud if address else None,
            "place_id": address.place_id if address else "",
        },
        "productos": [
            {
                "codigo": line.get("point_code", ""),
                "descripcion": line.get("description", ""),
                "cantidad": line.get("quantity", "0"),
                "precio_unitario": line.get("unit_price", "0"),
                "descuento": line.get("discount", "0"),
                "total": line.get("line_total", "0"),
            }
            for line in snapshot.get("lines", [])
            if isinstance(line, dict)
        ],
        "descripcion_pedido": order.descripcion,
        "total": order.monto_estimado,
        "canal": order.canal,
        "referencia_social": order.social_reference,
        "estatus": delivery.estatus,
        "ventana_inicio": delivery.ventana_inicio,
        "ventana_fin": delivery.ventana_fin,
        "instrucciones_entrega": delivery.instrucciones_entrega,
        "repartidor": (
            {
                "id": delivery.repartidor_id,
                "nombre": str(delivery.repartidor),
            }
            if delivery.repartidor_id
            else None
        ),
        "unidad": (
            {"id": delivery.unidad_id, "codigo": delivery.unidad.codigo}
            if delivery.unidad_id
            else None
        ),
        "auditoria": {
            "creado_en": delivery.created_at,
            "creado_por_id": delivery.created_by_id,
            "revision": delivery.revision,
            "operaciones_estado": [
                {
                    "operation_id": operation.operation_id,
                    "estatus_solicitado": operation.requested_status,
                    "estatus_final": operation.final_status,
                    "actor_id": operation.actor_id,
                    "actor_nombre": operation.actor_nombre,
                    "created_at": operation.created_at,
                }
                for operation in delivery.status_operations.all()
            ],
        },
    }


class PublicOmnichannelDeliveriesView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error
        serializer = OmnichannelDeliveryQuerySerializer(data=request.query_params)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        values = serializer.validated_data
        deliveries = _owned_deliveries(api_client)
        if values.get("estatus"):
            deliveries = deliveries.filter(estatus=values["estatus"])
        if values.get("canal"):
            deliveries = deliveries.filter(
                pedido_cliente__canal=values["canal"],
            )
        if values.get("repartidor_id"):
            deliveries = deliveries.filter(
                repartidor_id=values["repartidor_id"],
            )
        if values.get("sucursal"):
            deliveries = deliveries.filter(
                pedido_cliente__sucursal=values["sucursal"],
            )
        if values.get("fecha"):
            day_start = timezone.make_aware(
                datetime.combine(values["fecha"], datetime.min.time()),
                timezone.get_current_timezone(),
            )
            day_end = day_start + timedelta(days=1)
            deliveries = deliveries.filter(
                (
                    Q(pedido_cliente__external_source="POINT")
                    & Q(pedido_cliente__point_note_sold_at__gte=day_start)
                    & Q(pedido_cliente__point_note_sold_at__lt=day_end)
                )
                | (
                    ~Q(pedido_cliente__external_source="POINT")
                    & Q(pedido_cliente__created_at__gte=day_start)
                    & Q(pedido_cliente__created_at__lt=day_end)
                )
            )
        deliveries = deliveries.order_by(values["ordering"], "id")
        count = deliveries.count()
        start = (values["page"] - 1) * values["page_size"]
        page = deliveries[start:start + values["page_size"]]
        payload = {
            "count": count,
            "page": values["page"],
            "page_size": values["page_size"],
            "results": [_serialize_delivery_summary(item) for item in page],
        }
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload)


class PublicOmnichannelDeliveryIdentitiesView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error
        external_source = _normalize_text(request.query_params.get("external_source", ""))
        external_ids = [
            _normalize_text(value)
            for value in request.query_params.getlist("external_id")
            if _normalize_text(value)
        ]
        if (
            not external_source
            or len(external_source) > 40
            or not external_ids
            or len(external_ids) > 100
            or any(len(external_id) > 120 for external_id in external_ids)
        ):
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(
                {"detail": "La consulta de identidades no es válida."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        identities = list(
            _owned_deliveries(api_client)
            .filter(
                pedido_cliente__external_source=external_source,
                pedido_cliente__external_id__in=external_ids,
            )
            .values("pedido_cliente__external_source", "pedido_cliente__external_id")
            .distinct()
        )
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response({
            "identities": [
                {
                    "external_source": item["pedido_cliente__external_source"],
                    "external_id": item["pedido_cliente__external_id"],
                }
                for item in identities
            ],
        })


class PublicOmnichannelDeliveryDetailView(APIView):
    permission_classes = [AllowAny]

    def get(self, request, solicitud_id):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error
        delivery = (
            _owned_deliveries(api_client)
            .prefetch_related("status_operations")
            .filter(pk=solicitud_id)
            .first()
        )
        if not delivery:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            raise Http404
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(_serialize_delivery_detail(delivery))


class PublicOmnichannelDeliveryStatusView(APIView):
    permission_classes = [AllowAny]

    def patch(self, request, solicitud_id):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error
        logistics_error = _authorize_logistica_assignment(api_client, request)
        if logistics_error:
            return logistics_error
        serializer = OmnichannelDeliveryStatusSerializer(data=request.data)
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        values = serializer.validated_data
        try:
            with transaction.atomic():
                owned = (
                    _mutable_owned_deliveries(api_client)
                    .select_for_update()
                    .filter(pk=solicitud_id)
                    .exists()
                )
                if not owned:
                    raise Http404
                payload = update_domicilio_status(
                    solicitud_id=solicitud_id,
                    api_client=api_client,
                    repartidor_id=values["repartidor_id"],
                    requested_status=values["estatus"],
                    operation_id=values["operation_id"],
                    actor=values["actor"],
                )
        except Http404:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            raise
        except DomicilioStatusError as exc:
            _log_access(api_client, request, exc.status_code)
            return Response(
                {"detail": exc.detail, "code": "DOMICILIO_STATUS_CONFLICT"},
                status=exc.status_code,
            )
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload)


class PublicOmnichannelDeliveryPreparationStatusView(APIView):
    permission_classes = [AllowAny]

    def patch(self, request, solicitud_id):
        api_client, error = _auth_public_client(request)
        if error:
            return error
        capability_error = _authorize_omnichannel(api_client, request)
        if capability_error:
            return capability_error
        preparation_error = _authorize_domicilios_preparation(
            api_client, request
        )
        if preparation_error:
            return preparation_error
        serializer = OmnichannelDeliveryPreparationStatusSerializer(
            data=request.data
        )
        if not serializer.is_valid():
            _log_access(api_client, request, status.HTTP_400_BAD_REQUEST)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        values = serializer.validated_data
        try:
            with transaction.atomic():
                owned = (
                    _mutable_owned_deliveries(api_client)
                    .select_for_update()
                    .filter(pk=solicitud_id)
                    .exists()
                )
                if not owned:
                    raise Http404
                payload = prepare_domicilio_status(
                    solicitud_id=solicitud_id,
                    api_client=api_client,
                    requested_status=values["estatus"],
                    operation_id=values["operation_id"],
                    actor=values["actor"],
                )
        except Http404:
            _log_access(api_client, request, status.HTTP_404_NOT_FOUND)
            raise
        except DomicilioStatusError as exc:
            _log_access(api_client, request, exc.status_code)
            return Response(
                {"detail": exc.detail, "code": "DOMICILIO_STATUS_CONFLICT"},
                status=exc.status_code,
            )
        _log_access(api_client, request, status.HTTP_200_OK)
        return Response(payload)
