from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone as dt_timezone
from decimal import Decimal

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.core.validators import validate_email
from django.db import IntegrityError, connection, transaction
from django.db.models.functions import Lower
from django.utils import timezone
from django.utils.text import slugify

from crm.models import Cliente, DireccionCliente, PedidoCliente
from logistica.models import SolicitudDomicilio
from pos_bridge.services.point_note_detail_service import (
    PointNote,
    PointNoteDetailService,
)


@dataclass(frozen=True)
class LinkPointOrderCommand:
    pk_nota: str
    channel: str
    customer_name: str
    customer_phone: str
    customer_email: str
    address: str
    references: str
    latitude: Decimal | None
    longitude: Decimal | None
    place_id: str
    social_reference: str
    delivery_window_start: datetime | None
    delivery_window_end: datetime | None
    instructions: str


@dataclass(frozen=True)
class LinkPointOrderResult:
    order: PedidoCliente
    delivery: SolicitudDomicilio
    created: bool


POINT_PENDING_EXTERNAL_SOURCE = "POINT_PENDING"


def canonical_point_ticket_folio(folio: str) -> str:
    return re.sub(
        r"^nota(?:(?:\s+|[:#-]\s*)|$)",
        "",
        _text(str(folio or "")),
        flags=re.IGNORECASE,
    ).strip()


def point_pending_external_id(*, folio: str, branch: str, sale_date: date) -> str:
    canonical_folio = canonical_point_ticket_folio(folio)
    canonical_branch = slugify(_text(branch))
    if not canonical_folio or not canonical_branch:
        raise ValidationError(
            {"point": "Folio y sucursal son obligatorios para esperar la nota Point."},
        )
    identity_material = (
        f"{sale_date.isoformat()}\0{canonical_branch}\0{canonical_folio.casefold()}"
    )
    digest = hashlib.sha256(identity_material.encode("utf-8")).hexdigest()
    return f"POINT-PENDING:{sale_date.isoformat()}:{canonical_branch[:32]}:{digest[:32]}"


def _text(value: str) -> str:
    return (value or "").strip()


def _phone(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _email(value: str) -> str:
    return _text(value).lower()


def _canonical_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(dt_timezone.utc).isoformat()


def _fingerprint_key_bytes(value, *, setting_name: str) -> bytes:
    if isinstance(value, str):
        encoded = value.encode()
    elif isinstance(value, bytes):
        encoded = value
    else:
        raise ImproperlyConfigured(
            f"{setting_name} debe ser texto o bytes.",
        )
    if not encoded:
        raise ImproperlyConfigured(
            f"{setting_name} no puede estar vacío.",
        )
    return encoded


def _fingerprint_fallback_values(value, *, setting_name: str) -> list:
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        raise ImproperlyConfigured(
            f"{setting_name} debe ser una lista o tupla.",
        )
    return list(value)


def _point_link_fingerprint_keys() -> tuple[bytes, tuple[bytes, ...]]:
    dedicated_key = getattr(settings, "POINT_LINK_FINGERPRINT_KEY", None)
    uses_dedicated_key = dedicated_key is not None
    primary_value = dedicated_key if uses_dedicated_key else settings.SECRET_KEY
    primary = _fingerprint_key_bytes(
        primary_value,
        setting_name=(
            "POINT_LINK_FINGERPRINT_KEY"
            if uses_dedicated_key
            else "SECRET_KEY"
        ),
    )

    fallback_values = _fingerprint_fallback_values(
        getattr(settings, "POINT_LINK_FINGERPRINT_KEY_FALLBACKS", []),
        setting_name="POINT_LINK_FINGERPRINT_KEY_FALLBACKS",
    )
    if uses_dedicated_key:
        # Permite adoptar una clave dedicada sin invalidar huellas históricas
        # que se firmaron con SECRET_KEY.
        fallback_values.append(settings.SECRET_KEY)
    fallback_values.extend(
        _fingerprint_fallback_values(
            getattr(settings, "SECRET_KEY_FALLBACKS", []),
            setting_name="SECRET_KEY_FALLBACKS",
        ),
    )

    fallbacks = []
    seen = {primary}
    for index, value in enumerate(fallback_values):
        key = _fingerprint_key_bytes(
            value,
            setting_name=f"fallback de fingerprint #{index + 1}",
        )
        if key not in seen:
            seen.add(key)
            fallbacks.append(key)
    return primary, tuple(fallbacks)


def _point_link_fingerprint_message(command: LinkPointOrderCommand) -> bytes:
    payload = {
        "v": 1,
        "channel": command.channel,
        "social_reference": _text(command.social_reference),
        "customer": {
            "name": _text(command.customer_name),
            "phone": _phone(command.customer_phone),
            "email": _email(command.customer_email),
        },
        "address": {
            "value": DireccionCliente.normalizar_direccion(command.address),
            "references": _text(command.references),
            "latitude": (
                format(command.latitude, "f")
                if command.latitude is not None
                else None
            ),
            "longitude": (
                format(command.longitude, "f")
                if command.longitude is not None
                else None
            ),
            "place_id": _text(command.place_id),
        },
        "delivery_window": {
            "start": _canonical_datetime(command.delivery_window_start),
            "end": _canonical_datetime(command.delivery_window_end),
        },
        "instructions": _text(command.instructions),
    }
    return (
        "pollyanas:point-link:v1\0"
        + json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    ).encode()


def point_link_fingerprint(command: LinkPointOrderCommand) -> str:
    primary, _fallbacks = _point_link_fingerprint_keys()
    return hmac.new(
        primary,
        _point_link_fingerprint_message(command),
        digestmod=hashlib.sha256,
    ).hexdigest()


def verify_point_link_fingerprint(
    command: LinkPointOrderCommand,
    persisted_fingerprint: str,
) -> bool:
    primary, fallbacks = _point_link_fingerprint_keys()
    message = _point_link_fingerprint_message(command)
    matched = False
    for key in (primary, *fallbacks):
        candidate = hmac.new(
            key,
            message,
            digestmod=hashlib.sha256,
        ).hexdigest()
        matched |= hmac.compare_digest(persisted_fingerprint, candidate)
    return matched


def _validate_command(command: LinkPointOrderCommand, actor) -> None:
    errors: dict[str, str] = {}
    if not getattr(actor, "pk", None) or not getattr(actor, "is_active", False):
        errors["actor"] = "Se requiere un usuario activo para auditar la vinculación."
    if not _text(command.pk_nota):
        errors["pk_nota"] = "La nota Point es obligatoria."
    elif len(_text(command.pk_nota)) > 120:
        errors["pk_nota"] = "La nota Point excede la longitud permitida."
    if command.channel not in dict(PedidoCliente.CANAL_CHOICES):
        errors["channel"] = "El canal no es válido."
    if command.channel == PedidoCliente.CANAL_OTRO and not _text(command.social_reference):
        errors["social_reference"] = "El canal Otro requiere una descripción."
    if not _text(command.customer_name):
        errors["customer_name"] = "El nombre del cliente es obligatorio."
    elif len(_text(command.customer_name)) > 180:
        errors["customer_name"] = "El nombre excede la longitud permitida."
    if len(_phone(command.customer_phone)) > 40:
        errors["customer_phone"] = "El teléfono excede la longitud permitida."
    normalized_email = _email(command.customer_email)
    if len(normalized_email) > 254:
        errors["customer_email"] = "El correo excede la longitud permitida."
    elif normalized_email:
        try:
            validate_email(normalized_email)
        except ValidationError:
            errors["customer_email"] = "El correo no tiene un formato válido."
    if not _text(command.address):
        errors["address"] = "La dirección es obligatoria."
    elif len(_text(command.address)) > 300:
        errors["address"] = "La dirección excede la longitud permitida."
    if len(_text(command.references)) > 300:
        errors["references"] = "Las referencias exceden la longitud permitida."
    if len(_text(command.place_id)) > 255:
        errors["place_id"] = "El identificador del lugar excede la longitud permitida."
    if len(_text(command.social_reference)) > 180:
        errors["social_reference"] = "La referencia del canal excede la longitud permitida."
    if len(_text(command.instructions)) > 500:
        errors["instructions"] = "Las instrucciones exceden la longitud permitida."

    has_latitude = command.latitude is not None
    has_longitude = command.longitude is not None
    if has_latitude != has_longitude:
        errors["gps"] = "Latitud y longitud deben capturarse juntas."
    latitude_is_valid = (
        isinstance(command.latitude, Decimal)
        and command.latitude.is_finite()
    )
    longitude_is_valid = (
        isinstance(command.longitude, Decimal)
        and command.longitude.is_finite()
    )
    if has_latitude and not latitude_is_valid:
        errors["latitude"] = "La latitud debe ser un decimal finito."
    elif has_latitude and not Decimal("-90") <= command.latitude <= Decimal("90"):
        errors["latitude"] = "La latitud debe estar entre -90 y 90."
    if has_longitude and not longitude_is_valid:
        errors["longitude"] = "La longitud debe ser un decimal finito."
    elif has_longitude and not Decimal("-180") <= command.longitude <= Decimal("180"):
        errors["longitude"] = "La longitud debe estar entre -180 y 180."
    if latitude_is_valid and abs(command.latitude.as_tuple().exponent) > 6:
        errors["latitude"] = "La latitud admite máximo seis decimales."
    if longitude_is_valid and abs(command.longitude.as_tuple().exponent) > 6:
        errors["longitude"] = "La longitud admite máximo seis decimales."

    has_window_start = command.delivery_window_start is not None
    has_window_end = command.delivery_window_end is not None
    if has_window_start != has_window_end:
        errors["delivery_window"] = "La ventana de entrega requiere inicio y fin."
    window_types_valid = (
        (not has_window_start or isinstance(command.delivery_window_start, datetime))
        and (not has_window_end or isinstance(command.delivery_window_end, datetime))
    )
    if not window_types_valid:
        errors["delivery_window"] = "La ventana de entrega requiere fechas válidas."
    elif (
        has_window_start
        and has_window_end
        and (
            timezone.is_naive(command.delivery_window_start)
            or timezone.is_naive(command.delivery_window_end)
        )
    ):
        errors["delivery_window"] = "La ventana de entrega requiere zona horaria."
    elif has_window_start and has_window_end and (
        command.delivery_window_end < command.delivery_window_start
    ):
        errors["delivery_window_end"] = "El fin de la ventana no puede ser anterior al inicio."
    if errors:
        raise ValidationError(errors)


def _lock_key(namespace: str, value: str) -> None:
    if connection.vendor != "postgresql":
        raise RuntimeError("La vinculación de notas Point requiere PostgreSQL.")
    digest = hashlib.sha256(f"{namespace}\0{value}".encode()).digest()
    lock_id = int.from_bytes(digest[:8], byteorder="big", signed=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [lock_id])


def _find_or_create_customer(command: LinkPointOrderCommand) -> Cliente:
    normalized_phone = _phone(command.customer_phone)
    normalized_email = _email(command.customer_email)
    identity_keys = []
    if normalized_phone:
        identity_keys.append(f"phone:{normalized_phone}")
    if normalized_email:
        identity_keys.append(f"email:{normalized_email}")
    for identity_key in sorted(identity_keys):
        _lock_key("crm-customer-identity", identity_key)

    if normalized_phone:
        customer = (
            Cliente.objects.extra(
                where=["regexp_replace(telefono, '[^0-9]', '', 'g') = %s"],
                params=[normalized_phone],
            )
            .order_by("id")
            .first()
        )
        if customer:
            return customer

    if normalized_email:
        customer = (
            Cliente.objects.annotate(email_lower=Lower("email"))
            .filter(email_lower=normalized_email)
            .order_by("id")
            .first()
        )
        if customer:
            persisted_phone = _phone(customer.telefono)
            if normalized_phone and persisted_phone and persisted_phone != normalized_phone:
                raise ValidationError(
                    {
                        "customer": (
                            "El correo coincide con otro cliente que tiene un "
                            "teléfono diferente; seleccione el cliente explícitamente."
                        )
                    },
                )
            if normalized_phone and not persisted_phone:
                customer.telefono = normalized_phone
                customer.save(update_fields=["telefono", "updated_at"])
            return customer

    # Cliente.save() derives ``codigo`` from the current global maximum. This
    # lock serializes that legacy allocation even for unrelated identities.
    _lock_key("crm-customer-code", "global")
    for attempt in range(3):
        try:
            with transaction.atomic():
                return Cliente.objects.create(
                    nombre=_text(command.customer_name),
                    telefono=normalized_phone,
                    email=normalized_email,
                )
        except IntegrityError as exc:
            cause = getattr(exc, "__cause__", None)
            diagnostic = getattr(cause, "diag", None)
            constraint_name = getattr(diagnostic, "constraint_name", "")
            is_customer_code_collision = constraint_name == "crm_cliente_codigo_key"
            if not is_customer_code_collision or attempt == 2:
                raise
    raise AssertionError("unreachable")


def _find_or_create_address(
    customer: Cliente,
    command: LinkPointOrderCommand,
) -> DireccionCliente:
    address_text = _text(command.address)
    normalized = DireccionCliente.normalizar_direccion(address_text)
    address = (
        DireccionCliente.objects.select_for_update()
        .filter(cliente=customer, direccion_normalizada=normalized)
        .first()
    )
    if address:
        updates: list[str] = []
        conflicts: list[str] = []

        incoming_references = _text(command.references)
        if incoming_references:
            if address.referencias and address.referencias != incoming_references:
                conflicts.append("referencias")
            elif not address.referencias:
                address.referencias = incoming_references
                updates.append("referencias")

        incoming_place_id = _text(command.place_id)
        if incoming_place_id:
            if address.place_id and address.place_id != incoming_place_id:
                conflicts.append("place_id")
            elif not address.place_id:
                address.place_id = incoming_place_id
                updates.append("place_id")

        if command.latitude is not None and command.longitude is not None:
            has_saved_gps = address.latitud is not None or address.longitud is not None
            if has_saved_gps and (
                address.latitud != command.latitude
                or address.longitud != command.longitude
            ):
                conflicts.append("GPS")
            elif not has_saved_gps:
                address.latitud = command.latitude
                address.longitud = command.longitude
                updates.extend(("latitud", "longitud"))

        if conflicts:
            raise ValidationError(
                {
                    "address": (
                        "La dirección guardada contiene datos distintos en "
                        + ", ".join(conflicts)
                        + "; debe seleccionarse o corregirse explícitamente."
                    )
                },
            )
        if updates:
            address.save(update_fields=updates)
        return address

    try:
        with transaction.atomic():
            return DireccionCliente.objects.create(
                cliente=customer,
                direccion=address_text,
                referencias=_text(command.references),
                latitud=command.latitude,
                longitud=command.longitude,
                place_id=_text(command.place_id),
            )
    except IntegrityError:
        return DireccionCliente.objects.select_for_update().get(
            cliente=customer,
            direccion_normalizada=normalized,
        )


def _note_snapshot(note: PointNote) -> dict:
    return {
        "pk_nota": note.pk_nota,
        "folio": note.folio,
        "branch_name": note.branch_name,
        "sold_at": note.sold_at.isoformat(),
        "total": str(note.total),
        "invoiced": note.invoiced,
        "payment_type": note.payment_type,
        "point_channel": note.point_channel,
        "source_endpoint": note.source_endpoint,
        "customer_external_id": note.customer_external_id,
        "lines": [
            {
                "point_code": line.point_code,
                "description": line.description,
                "quantity": str(line.quantity),
                "unit_price": str(line.unit_price),
                "discount": str(line.discount),
                "line_total": str(line.line_total),
            }
            for line in note.lines
        ],
    }


def _description(note: PointNote) -> str:
    descriptions = ", ".join(line.description for line in note.lines)
    return f"Nota Point {note.folio}: {descriptions}"[:250]


def _delivery_for_existing(order: PedidoCliente) -> SolicitudDomicilio | None:
    deliveries = list(
        SolicitudDomicilio.objects.select_for_update()
        .filter(pedido_cliente=order)
        .order_by("id")[:2]
    )
    if len(deliveries) > 1:
        raise ValidationError(
            {"pedido": "La nota Point ya tiene más de una solicitud de domicilio."},
        )
    return deliveries[0] if deliveries else None


def _create_delivery(
    *,
    order: PedidoCliente,
    customer: Cliente,
    address: DireccionCliente,
    command: LinkPointOrderCommand,
    actor,
) -> SolicitudDomicilio:
    channel_detail = order.canal
    if order.canal == PedidoCliente.CANAL_OTRO:
        channel_detail = _text(command.social_reference)[:60]
    return SolicitudDomicilio.objects.create(
        pedido_cliente=order,
        cliente=customer,
        direccion_cliente=address,
        cliente_nombre=customer.nombre,
        cliente_telefono=customer.telefono,
        direccion=address.direccion,
        canal_origen=order.canal,
        canal_detalle=channel_detail,
        notas=_text(command.references),
        ventana_inicio=command.delivery_window_start,
        ventana_fin=command.delivery_window_end,
        instrucciones_entrega=_text(command.instructions),
        estatus=SolicitudDomicilio.ESTATUS_CONFIRMADO,
        created_by=actor,
    )


def _reconcile_pending_delivery(
    *,
    order: PedidoCliente,
    note: PointNote,
    command: LinkPointOrderCommand,
    fingerprint: str,
) -> LinkPointOrderResult:
    pending_snapshot = (
        order.payload_snapshot.get("point_pending", {})
        if isinstance(order.payload_snapshot, dict)
        else {}
    )
    pending_fingerprint = (
        pending_snapshot.get("capture_fingerprint", "")
        if isinstance(pending_snapshot, dict)
        else ""
    )
    if not pending_fingerprint or not verify_point_link_fingerprint(
        command, pending_fingerprint,
    ):
        raise ValidationError(
            {"pedido": "La captura pendiente de Point tiene datos distintos."},
        )
    delivery = _delivery_for_existing(order)
    if delivery is None:
        raise ValidationError(
            {"pedido": "La captura pendiente no tiene domicilio canónico."},
        )
    if delivery.estatus != SolicitudDomicilio.ESTATUS_PENDIENTE_POINT:
        raise ValidationError(
            {"pedido": "La captura pendiente ya avanzó y no puede conciliarse."},
        )

    order.reconcile_pending_point_provenance(
        point_note_id=note.pk_nota,
        point_note_folio=note.folio,
        point_note_snapshot=_note_snapshot(note),
        point_note_fetched_at=timezone.now(),
        point_note_sold_at=note.sold_at,
        point_link_fingerprint=fingerprint,
    )
    order.social_reference = _text(command.social_reference)
    order.canal = command.channel
    order.descripcion = _description(note)
    order.fecha_compromiso = (
        timezone.localtime(command.delivery_window_end).date()
        if command.delivery_window_end is not None
        else None
    )
    order.monto_estimado = note.total
    order.sucursal = note.branch_name
    order.save(
        update_fields=[
            "social_reference",
            "canal",
            "descripcion",
            "fecha_compromiso",
            "monto_estimado",
            "sucursal",
            "updated_at",
        ],
    )
    delivery.canal_origen = command.channel
    delivery.canal_detalle = (
        _text(command.social_reference)[:60]
        if command.channel == PedidoCliente.CANAL_OTRO
        else command.channel
    )
    delivery.estatus = SolicitudDomicilio.ESTATUS_CONFIRMADO
    delivery.save(update_fields=["canal_origen", "canal_detalle", "estatus"])
    return LinkPointOrderResult(order=order, delivery=delivery, created=False)


def link_point_note(
    *,
    command: LinkPointOrderCommand,
    actor,
    point_service: PointNoteDetailService | None = None,
) -> LinkPointOrderResult:
    """Create the canonical ERP order for one server-fetched Point note.

    Point I/O happens before acquiring database locks. PostgreSQL then serializes
    contenders for the same note, while the conditional unique constraint on
    ``PedidoCliente.point_note_id`` remains the final integrity boundary.
    """

    _validate_command(command, actor)
    fingerprint = point_link_fingerprint(command)
    note = (point_service or PointNoteDetailService()).fetch(
        pk_nota=_text(command.pk_nota),
    )
    if str(note.pk_nota).strip() != _text(command.pk_nota):
        raise ValidationError(
            {"pk_nota": "Point devolvió una nota distinta a la solicitada."},
        )

    with transaction.atomic():
        _lock_key("crm-point-note", note.pk_nota)
        existing = (
            PedidoCliente.objects.select_for_update()
            .filter(point_note_id=note.pk_nota)
            .first()
        )
        if existing:
            if not existing.point_link_fingerprint:
                raise ValidationError(
                    {
                        "pedido": (
                            "El pedido Point anterior requiere conciliación "
                            "antes de aceptar un replay."
                        )
                    },
                )
            if not verify_point_link_fingerprint(
                command,
                existing.point_link_fingerprint,
            ):
                raise ValidationError(
                    {"pedido": "La nota Point ya existe con otra captura."},
                )
            delivery = _delivery_for_existing(existing)
            if delivery is None:
                customer = existing.cliente
                address = existing.direccion_entrega
                if address is None:
                    address = _find_or_create_address(customer, command)
                    existing.direccion_entrega = address
                    existing.save(update_fields=["direccion_entrega", "updated_at"])
                delivery = _create_delivery(
                    order=existing,
                    customer=customer,
                    address=address,
                    command=command,
                    actor=actor,
                )
            return LinkPointOrderResult(
                order=existing,
                delivery=delivery,
                created=False,
            )

        sold_at = note.sold_at
        sale_date = (
            timezone.localtime(sold_at).date()
            if timezone.is_aware(sold_at)
            else sold_at.date()
        )
        pending_external_id = point_pending_external_id(
            folio=note.folio,
            branch=note.branch_name,
            sale_date=sale_date,
        )
        _lock_key(POINT_PENDING_EXTERNAL_SOURCE, pending_external_id)
        pending_order = (
            PedidoCliente.objects.select_for_update()
            .filter(
                external_source=POINT_PENDING_EXTERNAL_SOURCE,
                external_id=pending_external_id,
            )
            .first()
        )
        if pending_order is not None:
            return _reconcile_pending_delivery(
                order=pending_order,
                note=note,
                command=command,
                fingerprint=fingerprint,
            )

        customer = _find_or_create_customer(command)
        address = _find_or_create_address(customer, command)
        snapshot = _note_snapshot(note)
        try:
            with transaction.atomic():
                _lock_key("crm-pedido-folio", "global")
                order = PedidoCliente.objects.create(
                    cliente=customer,
                    direccion_entrega=address,
                    external_source="POINT",
                    external_id=note.pk_nota,
                    point_note_id=note.pk_nota,
                    point_note_folio=note.folio,
                    point_note_snapshot=snapshot,
                    point_note_fetched_at=timezone.now(),
                    point_note_sold_at=note.sold_at,
                    point_link_fingerprint=fingerprint,
                    social_reference=_text(command.social_reference),
                    canal=command.channel,
                    descripcion=_description(note),
                    fecha_compromiso=(
                        timezone.localtime(command.delivery_window_end).date()
                        if command.delivery_window_end is not None
                        else None
                    ),
                    monto_estimado=note.total,
                    sucursal=note.branch_name,
                    created_by=actor,
                )
        except IntegrityError:
            # The inner atomic block is a savepoint, so the outer transaction is
            # still usable and can safely load the committed winner.
            order = (
                PedidoCliente.objects.select_for_update()
                .filter(point_note_id=note.pk_nota)
                .first()
            )
            if order is None:
                raise
            delivery = _delivery_for_existing(order)
            if delivery is None:
                raise ValidationError(
                    {"pedido": "El pedido ganador aún no tiene domicilio canónico."},
                )
            return LinkPointOrderResult(order=order, delivery=delivery, created=False)

        delivery = _create_delivery(
            order=order,
            customer=customer,
            address=address,
            command=command,
            actor=actor,
        )
        return LinkPointOrderResult(order=order, delivery=delivery, created=True)
