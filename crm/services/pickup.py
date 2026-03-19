from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from django.conf import settings
from django.db import transaction
from django.db.models import Q, Sum
from django.db.models.functions import Coalesce
from django.utils import timezone

from core.audit import log_event
from core.models import Sucursal
from crm.models import Cliente, PedidoCliente, PickupReservation, SeguimientoPedido
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct
from recetas.models import Receta, RecetaCodigoPointAlias, normalizar_codigo_point
from recetas.utils.normalizacion import normalizar_nombre


ZERO = Decimal("0")


class PickupReservationError(Exception):
    def __init__(self, message: str, *, code: str, payload: dict | None = None):
        super().__init__(message)
        self.code = code
        self.payload = payload or {}


@dataclass(slots=True)
class PickupAvailability:
    receta: Receta
    sucursal: Sucursal
    point_branch: PointBranch | None
    point_product: PointProduct | None
    snapshot: PointInventorySnapshot | None
    snapshot_stock_qty: Decimal
    reserved_qty: Decimal
    buffer_qty: Decimal
    available_to_promise: Decimal
    requested_qty: Decimal
    is_fresh: bool
    freshness_seconds: int
    snapshot_age_seconds: int | None
    status: str

    @property
    def available(self) -> bool:
        return self.status in {"AVAILABLE", "LOW_STOCK"} and self.available_to_promise >= self.requested_qty

    def to_dict(self) -> dict:
        captured_at = self.snapshot.captured_at if self.snapshot else None
        return {
            "product_code": self.receta.codigo_point,
            "product_name": self.receta.nombre,
            "branch_code": self.sucursal.codigo,
            "branch_name": self.sucursal.nombre,
            "available": self.available,
            "stock_qty": str(self.snapshot_stock_qty),
            "reserved_qty": str(self.reserved_qty),
            "buffer_qty": str(self.buffer_qty),
            "available_to_promise": str(self.available_to_promise),
            "requested_qty": str(self.requested_qty),
            "status": self.status,
            "source": "ERP_POS_BRIDGE",
            "captured_at": captured_at.isoformat() if captured_at else None,
            "snapshot_age_seconds": self.snapshot_age_seconds,
            "freshness_seconds": self.freshness_seconds,
            "is_fresh": self.is_fresh,
        }


class PickupAvailabilityService:
    STATUS_AVAILABLE = "AVAILABLE"
    STATUS_LOW_STOCK = "LOW_STOCK"
    STATUS_OUT_OF_STOCK = "OUT_OF_STOCK"
    STATUS_UNKNOWN = "UNKNOWN"

    def __init__(self):
        self.freshness_minutes = max(int(getattr(settings, "PICKUP_AVAILABILITY_FRESHNESS_MINUTES", 20)), 1)
        self.default_buffer_qty = self._decimal(getattr(settings, "PICKUP_STOCK_BUFFER_DEFAULT", "1"))
        self.low_stock_threshold = self._decimal(getattr(settings, "PICKUP_LOW_STOCK_THRESHOLD", "3"))
        self.default_ttl_minutes = max(int(getattr(settings, "PICKUP_RESERVATION_TTL_MINUTES", 15)), 1)

    @staticmethod
    def _decimal(value, default: Decimal = ZERO) -> Decimal:
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return default

    def _resolve_receta(self, product_code: str) -> Receta:
        raw_code = (product_code or "").strip()
        if not raw_code:
            raise PickupReservationError("product_code es obligatorio.", code="missing_product_code")

        receta = Receta.objects.filter(codigo_point__iexact=raw_code).order_by("id").first()
        if receta is not None:
            return receta

        code_norm = normalizar_codigo_point(raw_code)
        alias = (
            RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm, activo=True)
            .select_related("receta")
            .order_by("id")
            .first()
        )
        if alias and alias.receta_id:
            return alias.receta

        raise PickupReservationError(
            "Producto no encontrado en catálogo ERP.",
            code="product_not_found",
            payload={"product_code": raw_code},
        )

    def _resolve_sucursal(self, branch_code: str) -> tuple[Sucursal, PointBranch | None]:
        raw_code = (branch_code or "").strip()
        if not raw_code:
            raise PickupReservationError("branch_code es obligatorio.", code="missing_branch_code")

        sucursal = Sucursal.objects.filter(codigo__iexact=raw_code, activa=True).first()
        if sucursal is None:
            sucursal = Sucursal.objects.filter(nombre__iexact=raw_code, activa=True).first()
        if sucursal is None:
            target = normalizar_nombre(raw_code)
            for row in Sucursal.objects.filter(activa=True).only("id", "codigo", "nombre"):
                if normalizar_nombre(row.nombre) == target:
                    sucursal = row
                    break
        if sucursal is None:
            raise PickupReservationError(
                "Sucursal no encontrada o inactiva.",
                code="branch_not_found",
                payload={"branch_code": raw_code},
            )

        point_branch = PointBranch.objects.filter(erp_branch=sucursal).order_by("id").first()
        return sucursal, point_branch

    def _candidate_codes(self, receta: Receta) -> list[str]:
        candidates: list[str] = []
        if (receta.codigo_point or "").strip():
            candidates.append(receta.codigo_point.strip())
        aliases = list(
            RecetaCodigoPointAlias.objects.filter(receta=receta, activo=True)
            .order_by("id")
            .values_list("codigo_point", flat=True)
        )
        for alias in aliases:
            code = (alias or "").strip()
            if code and code not in candidates:
                candidates.append(code)
        return candidates

    def _resolve_point_product(self, receta: Receta, point_branch: PointBranch | None) -> tuple[PointProduct | None, PointInventorySnapshot | None]:
        if point_branch is None:
            return None, None

        candidate_codes = self._candidate_codes(receta)
        products_qs = PointProduct.objects.all()
        if candidate_codes:
            code_query = Q()
            for code in candidate_codes:
                code_query |= Q(sku__iexact=code) | Q(external_id__iexact=code)
            products = list(products_qs.filter(code_query))
        else:
            products = []

        if not products:
            products = list(
                products_qs.filter(name__iexact=receta.nombre).order_by("-updated_at", "-id")[:5]
            )

        if not products and candidate_codes:
            target_norms = {normalizar_codigo_point(code) for code in candidate_codes if code}
            branch_product_ids = (
                PointInventorySnapshot.objects.filter(branch=point_branch)
                .values_list("product_id", flat=True)
                .distinct()
            )
            for product in PointProduct.objects.filter(id__in=branch_product_ids).only("id", "sku", "external_id", "name"):
                sku_norm = normalizar_codigo_point(product.sku or "")
                ext_norm = normalizar_codigo_point(product.external_id or "")
                if sku_norm in target_norms or ext_norm in target_norms:
                    products.append(product)

        if not products:
            return None, None

        snapshot = (
            PointInventorySnapshot.objects.filter(branch=point_branch, product_id__in=[product.id for product in products])
            .select_related("product")
            .order_by("-captured_at", "-id")
            .first()
        )
        if snapshot is None:
            return None, None
        return snapshot.product, snapshot

    def expire_stale_reservations(self) -> int:
        now = timezone.now()
        return PickupReservation.objects.filter(
            status=PickupReservation.STATUS_ACTIVE,
            expires_at__isnull=False,
            expires_at__lt=now,
        ).update(status=PickupReservation.STATUS_EXPIRED, released_at=now)

    def _reserved_qty(self, *, receta: Receta, sucursal: Sucursal) -> Decimal:
        self.expire_stale_reservations()
        return (
            PickupReservation.objects.filter(
                receta=receta,
                sucursal=sucursal,
                status__in=[PickupReservation.STATUS_ACTIVE, PickupReservation.STATUS_CONFIRMED],
            )
            .aggregate(total=Coalesce(Sum("quantity"), ZERO))
            .get("total")
            or ZERO
        )

    def get_availability(self, *, product_code: str, branch_code: str, quantity: Decimal | int | str = 1) -> PickupAvailability:
        requested_qty = max(self._decimal(quantity, Decimal("1")), Decimal("1"))
        receta = self._resolve_receta(product_code)
        sucursal, point_branch = self._resolve_sucursal(branch_code)
        point_product, snapshot = self._resolve_point_product(receta, point_branch)
        reserved_qty = self._reserved_qty(receta=receta, sucursal=sucursal)
        snapshot_stock_qty = snapshot.stock if snapshot else ZERO
        available_to_promise = max(snapshot_stock_qty - reserved_qty - self.default_buffer_qty, ZERO)

        now = timezone.now()
        freshness_seconds = self.freshness_minutes * 60
        snapshot_age_seconds = int((now - snapshot.captured_at).total_seconds()) if snapshot else None
        is_fresh = snapshot is not None and snapshot_age_seconds is not None and snapshot_age_seconds <= freshness_seconds

        if snapshot is None or not is_fresh:
            status = self.STATUS_UNKNOWN
        elif available_to_promise <= ZERO:
            status = self.STATUS_OUT_OF_STOCK
        elif available_to_promise <= self.low_stock_threshold:
            status = self.STATUS_LOW_STOCK
        else:
            status = self.STATUS_AVAILABLE

        return PickupAvailability(
            receta=receta,
            sucursal=sucursal,
            point_branch=point_branch,
            point_product=point_product,
            snapshot=snapshot,
            snapshot_stock_qty=snapshot_stock_qty,
            reserved_qty=reserved_qty,
            buffer_qty=self.default_buffer_qty,
            available_to_promise=available_to_promise,
            requested_qty=requested_qty,
            is_fresh=is_fresh,
            freshness_seconds=freshness_seconds,
            snapshot_age_seconds=snapshot_age_seconds,
            status=status,
        )

    @transaction.atomic
    def create_reservation(
        self,
        *,
        product_code: str,
        branch_code: str,
        quantity: Decimal | int | str = 1,
        client_name: str = "",
        source_client_prefix: str = "",
        external_reference: str = "",
        hold_minutes: int | None = None,
        metadata: dict | None = None,
    ) -> PickupReservation:
        requested_qty = max(self._decimal(quantity, Decimal("1")), Decimal("1"))
        receta = self._resolve_receta(product_code)
        sucursal, _point_branch = self._resolve_sucursal(branch_code)
        Receta.objects.select_for_update().filter(id=receta.id).first()
        Sucursal.objects.select_for_update().filter(id=sucursal.id).first()
        self.expire_stale_reservations()

        if external_reference:
            existing = (
                PickupReservation.objects.select_related("receta", "sucursal")
                .filter(
                    source_client_prefix=source_client_prefix[:12],
                    external_reference=external_reference[:120],
                    status__in=[PickupReservation.STATUS_ACTIVE, PickupReservation.STATUS_CONFIRMED],
                )
                .order_by("-created_at", "-id")
                .first()
            )
            if existing is not None:
                return existing

        availability = self.get_availability(product_code=product_code, branch_code=branch_code, quantity=requested_qty)
        if not availability.is_fresh:
            raise PickupReservationError(
                "Inventario sin confirmación reciente para esta sucursal.",
                code="inventory_not_fresh",
                payload=availability.to_dict(),
            )
        if availability.available_to_promise < requested_qty:
            raise PickupReservationError(
                "No hay inventario disponible para apartar en esta sucursal.",
                code="insufficient_stock",
                payload=availability.to_dict(),
            )

        ttl_minutes = max(int(hold_minutes or self.default_ttl_minutes), 1)
        reservation = PickupReservation.objects.create(
            token=uuid4().hex,
            receta=availability.receta,
            sucursal=availability.sucursal,
            quantity=requested_qty,
            status=PickupReservation.STATUS_ACTIVE,
            source=PickupReservation.SOURCE_WEB,
            source_client_prefix=source_client_prefix[:12],
            external_reference=(external_reference or "").strip()[:120],
            client_name=(client_name or "").strip()[:180],
            snapshot_stock_qty=availability.snapshot_stock_qty,
            reserved_qty_at_creation=availability.reserved_qty,
            buffer_qty=availability.buffer_qty,
            available_to_promise=availability.available_to_promise,
            snapshot_captured_at=availability.snapshot.captured_at if availability.snapshot else None,
            expires_at=timezone.now() + timedelta(minutes=ttl_minutes),
            metadata=metadata or {},
        )
        log_event(
            None,
            "CREATE",
            "crm.PickupReservation",
            str(reservation.id),
            {
                "token": reservation.token,
                "receta_id": reservation.receta_id,
                "sucursal_id": reservation.sucursal_id,
                "quantity": str(reservation.quantity),
                "status": reservation.status,
                "external_reference": reservation.external_reference,
            },
        )
        return reservation

    @transaction.atomic
    def confirm_reservation(
        self,
        *,
        token: str,
        cliente_nombre: str,
        descripcion: str,
        monto_estimado=ZERO,
        fecha_compromiso=None,
        prioridad: str = PedidoCliente.PRIORIDAD_MEDIA,
    ) -> tuple[PickupReservation, PedidoCliente]:
        reservation = (
            PickupReservation.objects.select_for_update()
            .select_related("receta", "sucursal")
            .filter(token=token)
            .first()
        )
        if reservation is None:
            raise PickupReservationError("Reserva no encontrada.", code="reservation_not_found")
        self.expire_stale_reservations()
        reservation.refresh_from_db()

        if reservation.status == PickupReservation.STATUS_CONFIRMED and hasattr(reservation, "pedido"):
            return reservation, reservation.pedido
        if reservation.status != PickupReservation.STATUS_ACTIVE:
            raise PickupReservationError(
                "La reserva ya no está activa.",
                code="reservation_not_active",
                payload={"status": reservation.status},
            )
        if reservation.expires_at and reservation.expires_at < timezone.now():
            reservation.status = PickupReservation.STATUS_EXPIRED
            reservation.released_at = timezone.now()
            reservation.save(update_fields=["status", "released_at", "updated_at"])
            raise PickupReservationError("La reserva expiró.", code="reservation_expired")

        cliente = Cliente.objects.filter(nombre_normalizado=normalizar_nombre(cliente_nombre)).first()
        if cliente is None:
            cliente = Cliente.objects.create(nombre=cliente_nombre)

        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion=descripcion,
            sucursal=reservation.sucursal.nombre,
            sucursal_ref=reservation.sucursal,
            pickup_reservation=reservation,
            canal=PedidoCliente.CANAL_WEB,
            prioridad=prioridad,
            estatus=PedidoCliente.ESTATUS_CONFIRMADO,
            monto_estimado=self._decimal(monto_estimado),
            fecha_compromiso=fecha_compromiso,
        )
        SeguimientoPedido.objects.create(
            pedido=pedido,
            estatus_nuevo=pedido.estatus,
            comentario=f"Reserva pickup confirmada {reservation.token}",
        )
        reservation.status = PickupReservation.STATUS_CONFIRMED
        reservation.save(update_fields=["status", "updated_at"])
        log_event(
            None,
            "CONFIRM",
            "crm.PickupReservation",
            str(reservation.id),
            {
                "token": reservation.token,
                "pedido_id": pedido.id,
                "folio": pedido.folio,
                "status": reservation.status,
            },
        )
        return reservation, pedido

    @transaction.atomic
    def release_reservation(self, *, token: str, reason: str = "") -> PickupReservation:
        reservation = (
            PickupReservation.objects.select_for_update()
            .select_related("sucursal", "receta")
            .filter(token=token)
            .first()
        )
        if reservation is None:
            raise PickupReservationError("Reserva no encontrada.", code="reservation_not_found")

        if reservation.status == PickupReservation.STATUS_ACTIVE:
            reservation.status = PickupReservation.STATUS_RELEASED
            reservation.released_at = timezone.now()
            reservation.metadata = {**(reservation.metadata or {}), "release_reason": reason}
            reservation.save(update_fields=["status", "released_at", "metadata", "updated_at"])
        elif reservation.status == PickupReservation.STATUS_CONFIRMED:
            reservation.status = PickupReservation.STATUS_CANCELED
            reservation.released_at = timezone.now()
            reservation.metadata = {**(reservation.metadata or {}), "cancel_reason": reason}
            reservation.save(update_fields=["status", "released_at", "metadata", "updated_at"])
            pedido = getattr(reservation, "pedido", None)
            if pedido is not None and pedido.estatus != PedidoCliente.ESTATUS_CANCELADO:
                previous_status = pedido.estatus
                pedido.estatus = PedidoCliente.ESTATUS_CANCELADO
                pedido.save(update_fields=["estatus", "updated_at"])
                SeguimientoPedido.objects.create(
                    pedido=pedido,
                    estatus_anterior=previous_status,
                    estatus_nuevo=pedido.estatus,
                    comentario=reason or "Reserva pickup cancelada",
                )
        log_event(
            None,
            "RELEASE",
            "crm.PickupReservation",
            str(reservation.id),
            {
                "token": reservation.token,
                "status": reservation.status,
                "reason": reason,
            },
        )
        return reservation
