from __future__ import annotations

from datetime import date

from django.utils import timezone

from core.models import sucursales_operativas
from pos_bridge.models import PointExtractionLog, PointSyncJob, PointTransferLine
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from pos_bridge.utils.exceptions import PersistenceError, PosBridgeError
from pos_bridge.utils.helpers import normalize_text


CEDIS_TOKENS = {"cedis", "almacen", "almacen central", "produccion", "centro distribucion"}


def is_cedis_like_name(value: str) -> bool:
    normalized = normalize_text(value)
    return any(token in normalized for token in CEDIS_TOKENS)


def resolve_requesting_erp_branch(line: PointTransferLine):
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
            summary = self.movement_service.persist_transfer_lines(sync_job, lines)
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
