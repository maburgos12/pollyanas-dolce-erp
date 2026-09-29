from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from django.db import transaction
from django.utils import timezone

from core.models import sucursales_operativas
from pos_bridge.models import (
    PointExtractionLog,
    PointOpenTransferSnapshot,
    PointOpenTransferSnapshotMember,
    PointSyncJob,
    PointTransferLine,
)
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.utils.exceptions import PersistenceError, PosBridgeError
from pos_bridge.utils.helpers import normalize_text


CEDIS_TOKENS = {"cedis", "almacen", "almacen central", "produccion", "centro distribucion"}
OPEN_TRANSFER_MANIFEST_KEY = "open_transfer_manifest"
OPEN_TRANSFER_CLOSE_TIME = time(1, 1)
OPEN_TRANSFER_CLOSE_WINDOW = timedelta(hours=6)
OPEN_TRANSFER_TIME_ZONE = ZoneInfo("America/Mazatlan")


def open_transfer_close_window(operational_date: date) -> tuple[datetime, datetime]:
    next_day = operational_date + timedelta(days=1)
    cutoff = datetime.combine(next_day, OPEN_TRANSFER_CLOSE_TIME, tzinfo=OPEN_TRANSFER_TIME_ZONE)
    return cutoff, cutoff + OPEN_TRANSFER_CLOSE_WINDOW


def _canonical_decimal(value) -> str:
    try:
        decimal_value = Decimal(str(value or 0))
    except (InvalidOperation, TypeError, ValueError):
        return str(value or "")
    if not decimal_value:
        return "0"
    return format(decimal_value.normalize(), "f")


def _canonical_datetime(value) -> str:
    if value is None:
        return ""
    if timezone.is_naive(value):
        value = timezone.make_aware(value, OPEN_TRANSFER_TIME_ZONE)
    return value.astimezone(OPEN_TRANSFER_TIME_ZONE).isoformat()


def _branch_value(line, side: str, field: str):
    frozen_value = getattr(line, f"{side}_branch_{field}", None)
    if frozen_value is not None:
        return str(frozen_value or "")
    branch = getattr(line, f"{side}_branch", None)
    if isinstance(branch, dict):
        return str(branch.get(field) or "")
    return str(getattr(branch, field, "") or "")


def canonical_open_transfer_payload(line) -> dict:
    return {
        "source_hash": str(line.source_hash or ""),
        "transfer_external_id": str(line.transfer_external_id or ""),
        "detail_external_id": str(line.detail_external_id or ""),
        "origin_branch_external_id": _branch_value(line, "origin", "external_id"),
        "origin_branch_name": _branch_value(line, "origin", "name"),
        "destination_branch_external_id": _branch_value(
            line, "destination", "external_id"
        ),
        "destination_branch_name": _branch_value(line, "destination", "name"),
        "registered_at": _canonical_datetime(line.registered_at),
        "sent_at": _canonical_datetime(line.sent_at),
        "received_at": _canonical_datetime(line.received_at),
        "requested_by": str(getattr(line, "requested_by", "") or ""),
        "sent_by": str(getattr(line, "sent_by", "") or ""),
        "received_by": str(getattr(line, "received_by", "") or ""),
        "item_name": str(line.item_name or ""),
        "item_code": str(line.item_code or ""),
        "unit": str(getattr(line, "unit", "") or ""),
        "requested_quantity": _canonical_decimal(line.requested_quantity),
        "sent_quantity": _canonical_decimal(line.sent_quantity),
        "received_quantity": _canonical_decimal(line.received_quantity),
        "is_insumo": bool(line.is_insumo),
        "is_received": bool(line.is_received),
        "is_cancelled": bool(line.is_cancelled),
        "is_finalized": bool(line.is_finalized),
        "is_open": bool(line.is_open),
    }


def _payload_sha256(payload: dict) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_open_transfer_manifest(
    lines,
    *,
    operational_date: date,
    captured_at: datetime,
) -> dict:
    stable_rows = []
    for line in lines:
        stable_rows.append(canonical_open_transfer_payload(line))
    stable_rows.sort(
        key=lambda row: (
            row["source_hash"],
            row["transfer_external_id"],
            row["detail_external_id"],
            json.dumps(row, sort_keys=True, separators=(",", ":")),
        )
    )
    serialized = json.dumps(stable_rows, sort_keys=True, separators=(",", ":"))
    return {
        "sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
        "row_count": len(stable_rows),
        "operational_date": operational_date.isoformat(),
        "captured_at": captured_at.isoformat(),
    }


@transaction.atomic
def persist_open_transfer_snapshot(
    *,
    sync_job: PointSyncJob,
    lines,
    operational_date: date,
    captured_at: datetime,
) -> tuple[PointOpenTransferSnapshot, dict]:
    rows = list(lines)
    source_hashes = [str(row.source_hash or "") for row in rows]
    if any(not value for value in source_hashes) or len(source_hashes) != len(
        set(source_hashes)
    ):
        raise PersistenceError(
            "El snapshot de transferencias abiertas contiene identificadores "
            "duplicados o vacíos."
        )
    manifest = build_open_transfer_manifest(
        rows,
        operational_date=operational_date,
        captured_at=captured_at,
    )
    snapshot = PointOpenTransferSnapshot.objects.create(
        sync_job=sync_job,
        operational_date=operational_date,
        captured_at=captured_at,
        row_count=manifest["row_count"],
        sha256=manifest["sha256"],
    )
    members = []
    for row in rows:
        payload = canonical_open_transfer_payload(row)
        members.append(
            PointOpenTransferSnapshotMember(
                snapshot=snapshot,
                source_line_id=getattr(row, "id", None),
                source_hash=payload["source_hash"],
                payload_sha256=_payload_sha256(payload),
                transfer_external_id=payload["transfer_external_id"],
                detail_external_id=payload["detail_external_id"],
                origin_branch_id=row.origin_branch_id,
                destination_branch_id=row.destination_branch_id,
                origin_branch_external_id=payload["origin_branch_external_id"],
                origin_branch_name=payload["origin_branch_name"],
                destination_branch_external_id=payload["destination_branch_external_id"],
                destination_branch_name=payload["destination_branch_name"],
                registered_at=row.registered_at,
                sent_at=row.sent_at,
                received_at=row.received_at,
                requested_by=payload["requested_by"],
                sent_by=payload["sent_by"],
                received_by=payload["received_by"],
                item_name=payload["item_name"],
                item_code=payload["item_code"],
                unit=payload["unit"],
                requested_quantity=row.requested_quantity,
                sent_quantity=row.sent_quantity,
                received_quantity=row.received_quantity,
                is_insumo=row.is_insumo,
                is_received=row.is_received,
                is_cancelled=row.is_cancelled,
                is_finalized=row.is_finalized,
                is_open=row.is_open,
            )
        )
    PointOpenTransferSnapshotMember.objects.bulk_create(members)
    manifest["snapshot_id"] = snapshot.id
    return snapshot, manifest


def is_cedis_like_name(value: str) -> bool:
    normalized = normalize_text(value)
    return any(token in normalized for token in CEDIS_TOKENS)


def resolve_requesting_erp_branch(line: PointTransferLine):
    # El vínculo ERP es la fuente estructurada y debe prevalecer sobre el nombre
    # visible de Point. Algunas sucursales legítimas incluyen palabras como
    # "Almacén" o "Producción", que solo sirven como heurística cuando Point no
    # logró mapear el destino.
    if line.erp_destination_branch_id:
        return line.erp_destination_branch
    if line.destination_branch_id and not is_cedis_like_name(line.destination_branch.name):
        return line.erp_destination_branch
    return line.erp_origin_branch or line.erp_destination_branch


class OpenTransferSyncService:
    def __init__(self, movement_service: PointMovementSyncService | None = None):
        self.movement_service = movement_service or PointMovementSyncService()

    def sync_open_transfers(
        self,
        *,
        fecha: date | None = None,
        branch_filter: str | None = None,
        triggered_by=None,
    ) -> PointSyncJob:
        fecha = fecha or timezone.localdate()
        parameters = {
            "mode": "open_transfers",
            "fecha": fecha.isoformat(),
            "branch_filter": branch_filter or "",
        }
        sync_job = self.movement_service.create_job(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            triggered_by=triggered_by,
            parameters=parameters,
        )
        self.movement_service.record_log(
            sync_job,
            PointExtractionLog.LEVEL_INFO,
            "Inicio de sincronización Point transferencias abiertas.",
            context=parameters,
        )
        try:
            lines = self.movement_service.transfer_extractor.extract_open(
                start_date=fecha,
                end_date=fecha,
                branch_filter=branch_filter,
            )
            existing_hashes = set(
                PointTransferLine.objects.filter(source_hash__in=[line.source_hash for line in lines]).values_list(
                    "source_hash",
                    flat=True,
                )
            )
            # Flujo de logística (carga inicial): sólo snapshot, igual que la
            # recarga. Las líneas abiertas no están recibidas, así que esto no
            # cambia lo que se escribe hoy; hace explícita la regla de negocio.
            summary = self.movement_service.persist_transfer_lines(sync_job, lines, apply_inventory=False)
            transfer_ids = {line.transfer_external_id for line in lines if line.transfer_external_id}
            branch_ids = self._requesting_branch_ids(fecha=fecha, transfer_ids=transfer_ids, sync_job=sync_job)
            active_branch_ids = set(sucursales_operativas(fecha).values_list("id", flat=True))
            summary.update(
                {
                    "folios_encontrados": len(transfer_ids),
                    "lineas_nuevas": len([line for line in lines if line.source_hash not in existing_hashes]),
                    "lineas_actualizadas": len([line for line in lines if line.source_hash in existing_hashes]),
                    "sucursales_con_solicitud": len(branch_ids),
                    "sucursales_sin_solicitud": max(0, len(active_branch_ids - branch_ids)),
                }
            )
            if not str(branch_filter or "").strip():
                persisted_lines = list(
                    PointTransferLine.objects.filter(
                        source_hash__in=[line.source_hash for line in lines],
                        sync_job=sync_job,
                    ).select_related("origin_branch", "destination_branch")
                )
                if len(persisted_lines) != len(lines):
                    raise PersistenceError(
                        "No fue posible materializar todas las líneas del snapshot de cierre."
                    )
                _snapshot, manifest = persist_open_transfer_snapshot(
                    sync_job=sync_job,
                    lines=persisted_lines,
                    operational_date=fecha,
                    captured_at=timezone.now(),
                )
                summary[OPEN_TRANSFER_MANIFEST_KEY] = manifest
            return self.movement_service._mark_success(sync_job, summary)
        except PosBridgeError as exc:
            return self.movement_service._mark_failure(sync_job, exc)
        except Exception as exc:
            return self.movement_service._mark_failure(
                sync_job,
                PersistenceError(f"Error no controlado en sync de transferencias abiertas Point: {exc}"),
            )

    def _requesting_branch_ids(self, *, fecha: date, transfer_ids: set[str], sync_job: PointSyncJob | None = None) -> set[int]:
        if not transfer_ids:
            return set()
        filters = {
            "transfer_external_id__in": transfer_ids,
            "is_open": True,
            "is_cancelled": False,
        }
        if sync_job is not None:
            filters["sync_job"] = sync_job
        else:
            filters["registered_at__date"] = fecha
        lines = (
            PointTransferLine.objects.filter(**filters).select_related(
                "origin_branch",
                "destination_branch",
                "erp_origin_branch",
                "erp_destination_branch",
            )
        )
        branch_ids = set()
        for line in lines:
            branch = resolve_requesting_erp_branch(line)
            if branch is not None:
                branch_ids.add(branch.id)
        return branch_ids
