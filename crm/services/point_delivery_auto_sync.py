from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from crm.models import PedidoCliente
from crm.services.point_order_link import (
    LinkPointOrderCommand,
    link_point_note,
    point_pending_external_id,
)
from integraciones.models import PublicApiClient
from pos_bridge.models import PointBranch, PointSyncJob
from pos_bridge.services.point_account_session_lock import POINT_ACCOUNT_SESSION_LOCK_ID
from pos_bridge.services.point_delivery_note_service import PointDeliveryNoteService
from pos_bridge.utils.exceptions import AuthenticationError, ConfigurationError


class PointDeliverySyncConfigurationError(RuntimeError):
    pass


class _PreloadedPointNoteService:
    def __init__(self, note):
        self.note = note

    def fetch(self, *, pk_nota: str):
        if str(pk_nota).strip() != self.note.pk_nota:
            raise ValueError("La nota precargada no coincide con la solicitada.")
        return self.note


class PointDeliveryAutoSyncService:
    # El mismo candado protege cualquier sesión larga de la cuenta Point. Si el
    # inventario está capturando, domicilios omite este minuto y reintenta en el
    # siguiente ciclo sin invalidar la sesión del navegador.
    LOCK_ID = POINT_ACCOUNT_SESSION_LOCK_ID
    DEFAULT_LOOKBACK_DAYS = 7
    MAX_LOOKBACK_DAYS = 31

    def __init__(self, *, delivery_service: PointDeliveryNoteService | None = None):
        self.delivery_service = delivery_service or PointDeliveryNoteService()

    def run(
        self,
        *,
        today: date | None = None,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    ) -> dict[str, Any]:
        if not self._try_lock():
            return self._result(
                status=PointSyncJob.STATUS_RUNNING,
                counts=self._empty_counts(),
                error_code="SYNC_IN_PROGRESS",
                job_id=None,
            )

        try:
            return self._run_locked(
                today=today or timezone.localdate(),
                lookback_days=lookback_days,
            )
        finally:
            self._unlock()

    def _run_locked(self, *, today: date, lookback_days: int) -> dict[str, Any]:
        days = max(
            self.DEFAULT_LOOKBACK_DAYS,
            min(int(lookback_days or 0), self.MAX_LOOKBACK_DAYS),
        )
        start_date = today - timedelta(days=days - 1)
        branches = self._canonical_branches()
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_DELIVERIES,
            status=PointSyncJob.STATUS_RUNNING,
            attempt_count=1,
            parameters={
                "start_date": start_date.isoformat(),
                "end_date": today.isoformat(),
                "lookback_days": days,
                "branch_count": len(branches),
            },
        )
        counts = self._empty_counts()
        error_codes: list[str] = []
        branch_error_codes: list[str] = []
        successful_branches = 0

        try:
            owner = self._resolve_owner()
        except PointDeliverySyncConfigurationError:
            return self._finish_job(
                job=job,
                status=PointSyncJob.STATUS_FAILED,
                counts=counts,
                error_code="OWNER_CONFIGURATION",
            )
        if not branches:
            return self._finish_job(
                job=job,
                status=PointSyncJob.STATUS_FAILED,
                counts=counts,
                error_code="BRANCH_CONFIGURATION",
            )

        existing_note_ids = set(
            PedidoCliente.objects.exclude(point_note_id="").values_list(
                "point_note_id",
                flat=True,
            ),
        )
        for branch in branches:
            try:
                batch = self.delivery_service.fetch_range(
                    start_date=start_date,
                    end_date=today,
                    branch_external_id=branch.external_id,
                    branch_display_name=branch.name,
                    exclude_note_ids=existing_note_ids,
                )
                successful_branches += 1
                counts["seen"] += batch.seen_count
                counts["existing"] += batch.seen_count - len(batch) - len(batch.failures)
                counts["failed"] += len(batch.failures)
                error_codes.extend(failure.error_code for failure in batch.failures)
                for delivery_note in batch:
                    pending_order = self._pending_capture(delivery_note.note)
                    if pending_order is not None:
                        if self._reconcile_pending_capture(
                            order=pending_order,
                            delivery_note=delivery_note,
                            owner=owner,
                        ):
                            existing_note_ids.add(delivery_note.note.pk_nota)
                            counts["existing"] += 1
                        else:
                            counts["failed"] += 1
                            error_codes.append("POINT_PENDING_REVIEW")
                        continue
                    try:
                        command = LinkPointOrderCommand(
                            pk_nota=delivery_note.note.pk_nota,
                            channel=PedidoCliente.CANAL_POR_CONFIRMAR,
                            customer_name=delivery_note.customer_name,
                            customer_phone=delivery_note.customer_phone,
                            customer_email=delivery_note.customer_email,
                            address=delivery_note.address,
                            references=delivery_note.references,
                            latitude=None,
                            longitude=None,
                            place_id="",
                            social_reference="",
                            delivery_window_start=None,
                            delivery_window_end=None,
                            instructions=delivery_note.references,
                            point_customer_id=delivery_note.customer_external_id,
                        )
                        with transaction.atomic():
                            linked = link_point_note(
                                command=command,
                                actor=owner.created_by,
                                point_service=_PreloadedPointNoteService(delivery_note.note),
                                allow_pending_channel=True,
                            )
                            if linked.order.public_api_client_id is None:
                                linked.order.public_api_client = owner
                                linked.order.save(update_fields=["public_api_client", "updated_at"])
                            elif linked.order.public_api_client_id != owner.id:
                                raise PointDeliverySyncConfigurationError(
                                    "La nota pertenece a otro cliente API.",
                                )
                        if linked.created:
                            counts["created"] += 1
                        else:
                            counts["existing"] += 1
                        existing_note_ids.add(delivery_note.note.pk_nota)
                    except Exception as exc:  # noqa: BLE001
                        counts["failed"] += 1
                        error_codes.append(self._error_code(exc))
            except Exception as exc:  # noqa: BLE001
                branch_error_codes.append(self._error_code(exc))

        all_error_codes = [*error_codes, *branch_error_codes]
        if not all_error_codes:
            final_status = PointSyncJob.STATUS_SUCCESS
            error_code = None
        elif successful_branches:
            final_status = PointSyncJob.STATUS_PARTIAL
            error_code = sorted(set(all_error_codes))[0]
        else:
            final_status = PointSyncJob.STATUS_FAILED
            error_code = sorted(set(all_error_codes))[0]
        return self._finish_job(
            job=job,
            status=final_status,
            counts=counts,
            error_code=error_code,
        )

    @staticmethod
    def _resolve_owner() -> PublicApiClient:
        eligible = [
            client
            for client in PublicApiClient.objects.select_related("created_by").filter(activo=True)
            if client.has_capability(PublicApiClient.CAPABILITY_OMNICHANNEL)
            and client.created_by_id is not None
            and client.created_by.is_active
        ]
        selected_prefix = str(
            getattr(settings, "POINT_DELIVERY_API_CLIENT_PREFIX", "") or "",
        ).strip()
        selected_id = getattr(settings, "POINT_DELIVERY_API_CLIENT_ID", None)
        if selected_prefix and selected_id not in (None, ""):
            raise PointDeliverySyncConfigurationError(
                "Solo puede configurarse un selector de propietario omnicanal.",
            )
        if selected_prefix:
            candidates = [
                client for client in eligible if client.clave_prefijo == selected_prefix
            ]
        elif selected_id not in (None, ""):
            try:
                client_id = int(selected_id)
            except (TypeError, ValueError):
                candidates = []
            else:
                candidates = [client for client in eligible if client.id == client_id]
        else:
            candidates = eligible
        if len(candidates) != 1:
            raise PointDeliverySyncConfigurationError(
                "Se requiere exactamente un propietario omnicanal activo.",
            )
        return candidates[0]

    @staticmethod
    def _canonical_branches() -> list[PointBranch]:
        candidates = PointBranch.objects.filter(
            status=PointBranch.STATUS_ACTIVE,
            erp_branch__isnull=False,
            external_id__regex=r"^[0-9]+$",
        ).order_by("id")
        by_numeric_id: dict[int, PointBranch] = {}
        for branch in candidates:
            numeric_id = int(branch.external_id)
            current = by_numeric_id.get(numeric_id)
            canonical_text = str(numeric_id)
            if current is None or (
                branch.external_id == canonical_text
                and current.external_id != canonical_text
            ):
                by_numeric_id[numeric_id] = branch
        return [by_numeric_id[key] for key in sorted(by_numeric_id)]

    @staticmethod
    def _pending_capture(note) -> PedidoCliente | None:
        sold_at = note.sold_at
        sale_date = (
            timezone.localtime(sold_at).date()
            if timezone.is_aware(sold_at)
            else sold_at.date()
        )
        external_id = point_pending_external_id(
            folio=note.folio,
            branch=note.branch_name,
            sale_date=sale_date,
        )
        return (
            PedidoCliente.objects.select_related(
                "cliente",
                "direccion_entrega",
                "public_api_client",
            )
            .filter(
                external_source="POINT_PENDING",
                external_id=external_id,
            )
            .first()
        )

    @staticmethod
    def _reconcile_pending_capture(*, order, delivery_note, owner) -> bool:
        pending = (
            order.payload_snapshot.get("point_pending", {})
            if isinstance(order.payload_snapshot, dict)
            else {}
        )
        capture = pending.get("capture") if isinstance(pending, dict) else None
        if not isinstance(capture, dict) or order.public_api_client_id != owner.id:
            return False

        def optional_decimal(value):
            return Decimal(str(value)) if value is not None else None

        def optional_datetime(value):
            return datetime.fromisoformat(str(value)) if value is not None else None

        try:
            command = LinkPointOrderCommand(
                pk_nota=delivery_note.note.pk_nota,
                channel=str(capture["channel"]),
                customer_name=str(capture["customer_name"]),
                customer_phone=str(capture.get("customer_phone", "")),
                customer_email=str(capture.get("customer_email", "")),
                address=str(capture["address"]),
                references=str(capture.get("references", "")),
                latitude=optional_decimal(capture.get("latitude")),
                longitude=optional_decimal(capture.get("longitude")),
                place_id=str(capture.get("place_id", "")),
                social_reference=str(capture.get("social_reference", "")),
                delivery_window_start=optional_datetime(
                    capture.get("delivery_window_start"),
                ),
                delivery_window_end=optional_datetime(
                    capture.get("delivery_window_end"),
                ),
                instructions=str(capture.get("instructions", "")),
                point_customer_id=delivery_note.customer_external_id,
            )
            linked = link_point_note(
                command=command,
                actor=owner.created_by,
                point_service=_PreloadedPointNoteService(delivery_note.note),
            )
        except Exception:  # noqa: BLE001 - queda pendiente sin duplicar ni persistir PII
            return False
        return (
            linked.order.id == order.id
            and linked.order.public_api_client_id == owner.id
        )

    def _try_lock(self) -> bool:
        if connection.vendor != "postgresql":
            raise RuntimeError("La sincronización de domicilios Point requiere PostgreSQL.")
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", [self.LOCK_ID])
            return bool(cursor.fetchone()[0])

    def _unlock(self) -> None:
        if connection.vendor != "postgresql":
            return
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", [self.LOCK_ID])

    @staticmethod
    def _error_code(exc: Exception) -> str:
        if isinstance(exc, AuthenticationError):
            return "POINT_AUTHENTICATION"
        if isinstance(exc, ConfigurationError):
            return "POINT_CONFIGURATION"
        if isinstance(exc, PointDeliverySyncConfigurationError):
            return "OWNER_CONFIGURATION"
        name = exc.__class__.__name__.upper()
        if "CONTRACT" in name:
            return "POINT_CONTRACT"
        if "UNAVAILABLE" in name or "TIMEOUT" in name or "CONNECTION" in name:
            return "POINT_UNAVAILABLE"
        return "NOTE_PROCESSING"

    @staticmethod
    def _empty_counts() -> dict[str, int]:
        return {"seen": 0, "created": 0, "existing": 0, "failed": 0}

    def _finish_job(
        self,
        *,
        job: PointSyncJob,
        status: str,
        counts: dict[str, int],
        error_code: str | None,
    ) -> dict[str, Any]:
        job.status = status
        job.finished_at = timezone.now()
        job.result_summary = dict(counts)
        job.error_message = error_code or ""
        job.save(
            update_fields=[
                "status",
                "finished_at",
                "result_summary",
                "error_message",
                "updated_at",
            ],
        )
        return self._result(
            status=status,
            counts=counts,
            error_code=error_code,
            job_id=job.id,
        )

    @staticmethod
    def _result(
        *,
        status: str,
        counts: dict[str, int],
        error_code: str | None,
        job_id: int | None,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "counts": dict(counts),
            "error_code": error_code,
            "job_id": job_id,
        }
