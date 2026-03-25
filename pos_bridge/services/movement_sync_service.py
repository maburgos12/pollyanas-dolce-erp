from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from django.db import transaction
from django.utils import timezone

from control.models import MermaPOS
from core.audit import log_event
from core.models import Sucursal
from inventario.models import ExistenciaInsumo, MovimientoInventario
from maestros.models import Insumo, PointPendingMatch
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import (
    PointBranch,
    PointExtractionLog,
    PointProductionLine,
    PointSyncJob,
    PointTransferLine,
    PointWasteLine,
)
from pos_bridge.services.alert_service import PointAlertService
from pos_bridge.services.movement_matching_service import PointMovementMatchingService
from pos_bridge.services.production_entry_extractor import PointProductionEntryExtractor
from pos_bridge.services.transfer_extractor import PointTransferExtractor
from pos_bridge.services.waste_extractor import PointWasteExtractor
from pos_bridge.utils.exceptions import PersistenceError, PosBridgeError
from pos_bridge.utils.helpers import normalize_text, sanitize_sensitive_data
from pos_bridge.utils.logger import get_job_logger, get_pos_bridge_logger
from recetas.models import InventarioCedisProducto, MovimientoProductoCedis


LOG_LEVELS = {
    PointExtractionLog.LEVEL_DEBUG: 10,
    PointExtractionLog.LEVEL_INFO: 20,
    PointExtractionLog.LEVEL_WARNING: 30,
    PointExtractionLog.LEVEL_ERROR: 40,
}


@dataclass
class MovementMaterializationResult:
    created: int = 0
    updated: int = 0
    skipped_unmatched: int = 0


class PointMovementSyncService:
    WASTE_SOURCE = "POINT_BRIDGE_WASTE"
    PRODUCTION_SOURCE = "POINT_BRIDGE_PRODUCTION"
    TRANSFER_SOURCE = "POINT_BRIDGE_TRANSFER"

    def __init__(
        self,
        waste_extractor: PointWasteExtractor | None = None,
        production_extractor: PointProductionEntryExtractor | None = None,
        transfer_extractor: PointTransferExtractor | None = None,
        matcher: PointMovementMatchingService | None = None,
    ):
        self.settings = load_point_bridge_settings()
        self.waste_extractor = waste_extractor or PointWasteExtractor(self.settings)
        self.production_extractor = production_extractor or PointProductionEntryExtractor(self.settings)
        self.transfer_extractor = transfer_extractor or PointTransferExtractor(self.settings)
        self.matcher = matcher or PointMovementMatchingService()
        self.logger = get_pos_bridge_logger()
        self.alert_service = PointAlertService()

    def _resolve_erp_branch(self, branch_payload: dict):
        external_id = str(branch_payload.get("external_id") or "").strip()
        name = str(branch_payload.get("name") or "").strip()
        match = None
        if external_id:
            match = Sucursal.objects.filter(codigo__iexact=external_id).first()
        if match is None and name:
            match = Sucursal.objects.filter(nombre__iexact=name).first()
        if match is None and name:
            normalized = normalize_text(name)
            for branch in Sucursal.objects.all().only("id", "nombre"):
                if normalize_text(branch.nombre) == normalized:
                    return branch
        return match

    def _upsert_branch(self, payload: dict) -> PointBranch:
        defaults = {
            "name": payload["name"],
            "status": payload.get("status") or PointBranch.STATUS_ACTIVE,
            "metadata": payload.get("metadata") or {},
            "erp_branch": self._resolve_erp_branch(payload),
            "last_seen_at": timezone.now(),
        }
        branch, _ = PointBranch.objects.update_or_create(external_id=payload["external_id"], defaults=defaults)
        return branch

    def record_log(self, sync_job: PointSyncJob, level: str, message: str, *, context: dict | None = None) -> None:
        context = sanitize_sensitive_data(context or {})
        PointExtractionLog.objects.create(sync_job=sync_job, level=level, message=message, context=context)
        get_job_logger(sync_job.id).log(LOG_LEVELS.get(level, 20), "%s | %s", message, context)

    def create_job(self, *, job_type: str, triggered_by=None, parameters: dict | None = None) -> PointSyncJob:
        return PointSyncJob.objects.create(
            job_type=job_type,
            status=PointSyncJob.STATUS_RUNNING,
            started_at=timezone.now(),
            triggered_by=triggered_by,
            parameters=parameters or {},
        )

    def _resolve_code_for_waste(self, *, receta, insumo) -> str:
        if receta is not None:
            return (receta.codigo_point or "").strip()
        if insumo is not None:
            return (insumo.codigo_point or insumo.codigo or "").strip()
        return ""

    def _operational_date(self, dt_value) -> date:
        if dt_value is None:
            return timezone.localdate()
        if timezone.is_aware(dt_value):
            return timezone.localtime(dt_value).date()
        return dt_value.date()

    def _upsert_pending_match(self, *, tipo: str, codigo: str, nombre: str, payload: dict) -> None:
        PointPendingMatch.objects.update_or_create(
            tipo=tipo,
            point_codigo=(codigo or "").strip(),
            point_nombre=(nombre or "").strip()[:250],
            defaults={
                "payload": payload,
                "method": "POINT_BRIDGE_MOVEMENTS",
            },
        )

    def _upsert_merma(self, *, line: PointWasteLine) -> bool:
        defaults = {
            "receta": line.receta,
            "sucursal": line.erp_branch,
            "fecha": self._operational_date(line.movement_at),
            "codigo_point": self._resolve_code_for_waste(receta=line.receta, insumo=line.insumo),
            "producto_texto": line.item_name,
            "cantidad": line.quantity,
            "motivo": line.justification[:160],
            "responsable_texto": line.responsible[:160],
            "fuente": self.WASTE_SOURCE,
        }
        _, created = MermaPOS.objects.update_or_create(source_hash=line.source_hash, defaults=defaults)
        return created

    def _apply_inventory_delta(self, *, insumo: Insumo, delta: Decimal) -> None:
        existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=insumo)
        existencia.stock_actual = Decimal(str(existencia.stock_actual or 0)) + delta
        existencia.actualizado_en = timezone.now()
        existencia.save(update_fields=["stock_actual", "actualizado_en"])

    def _apply_cedis_delta(self, *, receta, delta: Decimal) -> None:
        inventario, _ = InventarioCedisProducto.objects.get_or_create(receta=receta)
        inventario.stock_actual = Decimal(str(inventario.stock_actual or 0)) + delta
        inventario.save(update_fields=["stock_actual", "actualizado_en"])

    def _upsert_inventory_movement(self, *, line: PointProductionLine) -> bool:
        defaults = {
            "fecha": datetime.combine(line.production_date, datetime.min.time(), tzinfo=timezone.get_current_timezone()),
            "tipo": MovimientoInventario.TIPO_ENTRADA,
            "insumo": line.insumo,
            "cantidad": line.produced_quantity,
            "referencia": f"POINT-PROD:{line.production_external_id}",
        }
        existing = MovimientoInventario.objects.filter(source_hash=line.source_hash).first()
        if existing is None:
            MovimientoInventario.objects.create(source_hash=line.source_hash, **defaults)
            self._apply_inventory_delta(insumo=line.insumo, delta=Decimal(str(line.produced_quantity or 0)))
            return True
        new_qty = Decimal(str(line.produced_quantity or 0))
        old_qty = Decimal(str(existing.cantidad or 0))
        if existing.insumo_id == line.insumo_id and old_qty == new_qty:
            return False
        if existing.insumo_id == line.insumo_id:
            self._apply_inventory_delta(insumo=line.insumo, delta=new_qty - old_qty)
        else:
            self._apply_inventory_delta(insumo=existing.insumo, delta=-old_qty)
            self._apply_inventory_delta(insumo=line.insumo, delta=new_qty)
        for field, value in defaults.items():
            setattr(existing, field, value)
        existing.save(update_fields=["fecha", "tipo", "insumo", "cantidad", "referencia"])
        return False

    def _upsert_cedis_movement(self, *, line: PointProductionLine) -> bool:
        defaults = {
            "fecha": datetime.combine(line.production_date, datetime.min.time(), tzinfo=timezone.get_current_timezone()),
            "tipo": MovimientoProductoCedis.TIPO_ENTRADA,
            "receta": line.receta,
            "cantidad": line.produced_quantity,
            "referencia": f"POINT-PROD:{line.production_external_id}",
        }
        existing = MovimientoProductoCedis.objects.filter(source_hash=line.source_hash).first()
        if existing is None:
            MovimientoProductoCedis.objects.create(source_hash=line.source_hash, **defaults)
            self._apply_cedis_delta(receta=line.receta, delta=Decimal(str(line.produced_quantity or 0)))
            return True
        new_qty = Decimal(str(line.produced_quantity or 0))
        old_qty = Decimal(str(existing.cantidad or 0))
        if existing.receta_id == line.receta_id and old_qty == new_qty:
            return False
        if existing.receta_id == line.receta_id:
            self._apply_cedis_delta(receta=line.receta, delta=new_qty - old_qty)
        else:
            self._apply_cedis_delta(receta=existing.receta, delta=-old_qty)
            self._apply_cedis_delta(receta=line.receta, delta=new_qty)
        for field, value in defaults.items():
            setattr(existing, field, value)
        existing.save(update_fields=["fecha", "tipo", "receta", "cantidad", "referencia"])
        return False

    def _upsert_transfer_inventory_movement(self, *, line: PointTransferLine) -> bool:
        movement_at = line.received_at or line.sent_at or line.registered_at
        defaults = {
            "fecha": movement_at,
            "tipo": MovimientoInventario.TIPO_ENTRADA,
            "insumo": line.insumo,
            "cantidad": line.received_quantity,
            "referencia": f"POINT-TRANSFER:{line.transfer_external_id}",
        }
        existing = MovimientoInventario.objects.filter(source_hash=line.source_hash).first()
        if existing is None:
            MovimientoInventario.objects.create(source_hash=line.source_hash, **defaults)
            self._apply_inventory_delta(insumo=line.insumo, delta=Decimal(str(line.received_quantity or 0)))
            return True
        new_qty = Decimal(str(line.received_quantity or 0))
        old_qty = Decimal(str(existing.cantidad or 0))
        if existing.insumo_id == line.insumo_id and old_qty == new_qty:
            return False
        if existing.insumo_id == line.insumo_id:
            self._apply_inventory_delta(insumo=line.insumo, delta=new_qty - old_qty)
        else:
            self._apply_inventory_delta(insumo=existing.insumo, delta=-old_qty)
            self._apply_inventory_delta(insumo=line.insumo, delta=new_qty)
        for field, value in defaults.items():
            setattr(existing, field, value)
        existing.save(update_fields=["fecha", "tipo", "insumo", "cantidad", "referencia"])
        return False

    def _upsert_transfer_cedis_movement(self, *, line: PointTransferLine) -> bool:
        movement_at = line.received_at or line.sent_at or line.registered_at
        defaults = {
            "fecha": movement_at,
            "tipo": MovimientoProductoCedis.TIPO_ENTRADA,
            "receta": line.receta,
            "cantidad": line.received_quantity,
            "referencia": f"POINT-TRANSFER:{line.transfer_external_id}",
        }
        existing = MovimientoProductoCedis.objects.filter(source_hash=line.source_hash).first()
        if existing is None:
            MovimientoProductoCedis.objects.create(source_hash=line.source_hash, **defaults)
            self._apply_cedis_delta(receta=line.receta, delta=Decimal(str(line.received_quantity or 0)))
            return True
        new_qty = Decimal(str(line.received_quantity or 0))
        old_qty = Decimal(str(existing.cantidad or 0))
        if existing.receta_id == line.receta_id and old_qty == new_qty:
            return False
        if existing.receta_id == line.receta_id:
            self._apply_cedis_delta(receta=line.receta, delta=new_qty - old_qty)
        else:
            self._apply_cedis_delta(receta=existing.receta, delta=-old_qty)
            self._apply_cedis_delta(receta=line.receta, delta=new_qty)
        for field, value in defaults.items():
            setattr(existing, field, value)
        existing.save(update_fields=["fecha", "tipo", "receta", "cantidad", "referencia"])
        return False

    def _is_storage_branch(self, branch_name: str) -> bool:
        allowed = {normalize_text(value) for value in self.settings.production_storage_branches if value}
        return normalize_text(branch_name) in allowed

    def _is_transfer_storage_branch(self, branch_name: str) -> bool:
        allowed = {normalize_text(value) for value in self.settings.transfer_storage_branches if value}
        return normalize_text(branch_name) in allowed

    @transaction.atomic
    def persist_waste_lines(self, sync_job: PointSyncJob, extracted_lines: list) -> dict:
        staged_created = 0
        staged_updated = 0
        ledger_created = 0
        ledger_updated = 0
        unresolved = 0
        for item in extracted_lines:
            branch = self._upsert_branch(item.branch)
            receta = self.matcher.resolve_receta(point_name=item.item_name)
            insumo = self.matcher.resolve_insumo(point_name=item.item_name)
            movement_at = item.movement_at
            if timezone.is_naive(movement_at):
                movement_at = timezone.make_aware(movement_at, timezone.get_current_timezone())
            defaults = {
                "branch": branch,
                "erp_branch": branch.erp_branch,
                "receta": receta,
                "insumo": insumo,
                "sync_job": sync_job,
                "movement_external_id": item.movement_external_id,
                "movement_at": movement_at,
                "responsible": item.responsible,
                "item_name": item.item_name,
                "item_code": item.item_code,
                "quantity": item.quantity,
                "unit": item.unit,
                "unit_cost": item.unit_cost,
                "total_cost": item.total_cost,
                "justification": item.justification,
                "source_endpoint": "/Mermas/get_mermas",
                "raw_payload": item.raw_payload,
            }
            _, created = PointWasteLine.objects.update_or_create(source_hash=item.source_hash, defaults=defaults)
            if created:
                staged_created += 1
            else:
                staged_updated += 1
            line = PointWasteLine.objects.get(source_hash=item.source_hash)
            if self._upsert_merma(line=line):
                ledger_created += 1
            else:
                ledger_updated += 1
            if receta is None and insumo is None:
                unresolved += 1
                self._upsert_pending_match(
                    tipo=PointPendingMatch.TIPO_PRODUCTO,
                    codigo=item.item_code,
                    nombre=item.item_name,
                    payload=item.raw_payload,
                )
        return {
            "waste_lines_seen": len(extracted_lines),
            "waste_lines_created": staged_created,
            "waste_lines_updated": staged_updated,
            "mermas_created": ledger_created,
            "mermas_updated": ledger_updated,
            "unmatched_items": unresolved,
        }

    @transaction.atomic
    def persist_production_lines(self, sync_job: PointSyncJob, extracted_lines: list) -> dict:
        staged_created = 0
        staged_updated = 0
        inventory_entries_created = 0
        inventory_entries_updated = 0
        cedis_entries_created = 0
        cedis_entries_updated = 0
        skipped_non_storage = 0
        unresolved = 0

        for item in extracted_lines:
            branch = self._upsert_branch(item.branch)
            receta = self.matcher.resolve_receta(codigo_point=item.item_code, point_name=item.item_name)
            insumo = self.matcher.resolve_insumo(codigo_point=item.item_code, point_name=item.item_name)
            defaults = {
                "branch": branch,
                "erp_branch": branch.erp_branch,
                "receta": receta,
                "insumo": insumo,
                "sync_job": sync_job,
                "production_external_id": item.production_external_id,
                "detail_external_id": item.detail_external_id,
                "production_date": item.production_date,
                "responsible": item.responsible,
                "item_name": item.item_name,
                "item_code": item.item_code,
                "unit": item.unit,
                "unit_cost": item.unit_cost,
                "requested_quantity": item.requested_quantity,
                "produced_quantity": item.produced_quantity,
                "is_insumo": item.is_insumo,
                "source_endpoint": "/Produccion/getProduccionGeneral",
                "raw_payload": item.raw_payload,
            }
            _, created = PointProductionLine.objects.update_or_create(source_hash=item.source_hash, defaults=defaults)
            if created:
                staged_created += 1
            else:
                staged_updated += 1
            line = PointProductionLine.objects.get(source_hash=item.source_hash)
            if not self._is_storage_branch(branch.name):
                skipped_non_storage += 1
                continue
            if line.is_insumo and line.insumo is not None:
                created_entry = self._upsert_inventory_movement(line=line)
                if created_entry:
                    inventory_entries_created += 1
                else:
                    inventory_entries_updated += 1
                continue
            if (not line.is_insumo) and line.receta is not None:
                created_entry = self._upsert_cedis_movement(line=line)
                if created_entry:
                    cedis_entries_created += 1
                else:
                    cedis_entries_updated += 1
                continue
            unresolved += 1
            self._upsert_pending_match(
                tipo=PointPendingMatch.TIPO_INSUMO if line.is_insumo else PointPendingMatch.TIPO_PRODUCTO,
                codigo=line.item_code,
                nombre=line.item_name,
                payload=line.raw_payload,
            )

        return {
            "production_lines_seen": len(extracted_lines),
            "production_lines_created": staged_created,
            "production_lines_updated": staged_updated,
            "inventory_entries_created": inventory_entries_created,
            "inventory_entries_updated": inventory_entries_updated,
            "cedis_entries_created": cedis_entries_created,
            "cedis_entries_updated": cedis_entries_updated,
            "skipped_non_storage_branch": skipped_non_storage,
            "unmatched_items": unresolved,
        }

    @transaction.atomic
    def persist_transfer_lines(self, sync_job: PointSyncJob, extracted_lines: list) -> dict:
        staged_created = 0
        staged_updated = 0
        inventory_entries_created = 0
        inventory_entries_updated = 0
        cedis_entries_created = 0
        cedis_entries_updated = 0
        skipped_non_storage = 0
        unresolved = 0

        for item in extracted_lines:
            origin_branch = self._upsert_branch(item.origin_branch)
            destination_branch = self._upsert_branch(item.destination_branch)
            receta = self.matcher.resolve_receta(codigo_point=item.item_code, point_name=item.item_name)
            insumo = self.matcher.resolve_insumo(codigo_point=item.item_code, point_name=item.item_name)
            defaults = {
                "origin_branch": origin_branch,
                "destination_branch": destination_branch,
                "erp_origin_branch": origin_branch.erp_branch,
                "erp_destination_branch": destination_branch.erp_branch,
                "receta": receta,
                "insumo": insumo,
                "sync_job": sync_job,
                "transfer_external_id": item.transfer_external_id,
                "detail_external_id": item.detail_external_id,
                "registered_at": item.registered_at,
                "sent_at": item.sent_at,
                "received_at": item.received_at,
                "requested_by": item.requested_by,
                "sent_by": item.sent_by,
                "received_by": item.received_by,
                "item_name": item.item_name,
                "item_code": item.item_code,
                "unit": item.unit,
                "unit_cost": item.unit_cost,
                "requested_quantity": item.requested_quantity,
                "sent_quantity": item.sent_quantity,
                "received_quantity": item.received_quantity,
                "is_insumo": item.is_insumo,
                "is_received": item.is_received,
                "is_cancelled": item.is_cancelled,
                "is_finalized": item.is_finalized,
                "source_endpoint": "/Transfer/GetTransfer",
                "raw_payload": item.raw_payload,
            }
            _, created = PointTransferLine.objects.update_or_create(source_hash=item.source_hash, defaults=defaults)
            if created:
                staged_created += 1
            else:
                staged_updated += 1
            line = PointTransferLine.objects.get(source_hash=item.source_hash)
            if not line.is_received:
                continue
            if not self._is_transfer_storage_branch(destination_branch.name):
                skipped_non_storage += 1
                continue
            if line.is_insumo and line.insumo is not None:
                created_entry = self._upsert_transfer_inventory_movement(line=line)
                if created_entry:
                    inventory_entries_created += 1
                else:
                    inventory_entries_updated += 1
                continue
            if (not line.is_insumo) and line.receta is not None:
                created_entry = self._upsert_transfer_cedis_movement(line=line)
                if created_entry:
                    cedis_entries_created += 1
                else:
                    cedis_entries_updated += 1
                continue
            unresolved += 1
            self._upsert_pending_match(
                tipo=PointPendingMatch.TIPO_INSUMO if line.is_insumo else PointPendingMatch.TIPO_PRODUCTO,
                codigo=line.item_code,
                nombre=line.item_name,
                payload=line.raw_payload,
            )

        return {
            "transfer_lines_seen": len(extracted_lines),
            "transfer_lines_created": staged_created,
            "transfer_lines_updated": staged_updated,
            "inventory_entries_created": inventory_entries_created,
            "inventory_entries_updated": inventory_entries_updated,
            "cedis_entries_created": cedis_entries_created,
            "cedis_entries_updated": cedis_entries_updated,
            "skipped_non_storage_branch": skipped_non_storage,
            "unmatched_items": unresolved,
        }

    def _mark_success(self, sync_job: PointSyncJob, summary: dict) -> PointSyncJob:
        sync_job.status = PointSyncJob.STATUS_SUCCESS
        sync_job.finished_at = timezone.now()
        sync_job.result_summary = summary
        sync_job.error_message = ""
        sync_job.save(update_fields=["status", "finished_at", "result_summary", "error_message", "updated_at"])
        log_event(sync_job.triggered_by, "POS_BRIDGE_SYNC_SUCCESS", "pos_bridge.PointSyncJob", str(sync_job.id), payload=summary)
        return sync_job

    def _mark_failure(self, sync_job: PointSyncJob, exc: Exception) -> PointSyncJob:
        context = sanitize_sensitive_data(getattr(exc, "context", {}) or {})
        sync_job.status = PointSyncJob.STATUS_FAILED
        sync_job.finished_at = timezone.now()
        sync_job.error_message = str(exc)
        sync_job.artifacts = {**sync_job.artifacts, **context}
        sync_job.save(update_fields=["status", "finished_at", "error_message", "artifacts", "updated_at"])
        self.record_log(sync_job, PointExtractionLog.LEVEL_ERROR, str(exc), context=context)
        self.alert_service.emit_failure(job_id=sync_job.id, message=str(exc), context=context)
        return sync_job

    def run_waste_sync(self, *, start_date: date, end_date: date, branch_filter: str | None = None, triggered_by=None) -> PointSyncJob:
        parameters = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "branch_filter": branch_filter or ""}
        sync_job = self.create_job(job_type=PointSyncJob.JOB_TYPE_WASTE, triggered_by=triggered_by, parameters=parameters)
        self.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de sincronización Point mermas.", context=parameters)
        try:
            lines = self.waste_extractor.extract(start_date=start_date, end_date=end_date, branch_filter=branch_filter)
            summary = self.persist_waste_lines(sync_job, lines)
            return self._mark_success(sync_job, summary)
        except PosBridgeError as exc:
            return self._mark_failure(sync_job, exc)
        except Exception as exc:
            return self._mark_failure(sync_job, PersistenceError(f"Error no controlado en sync de mermas Point: {exc}"))

    def run_production_sync(self, *, start_date: date, end_date: date, branch_filter: str | None = None, triggered_by=None) -> PointSyncJob:
        parameters = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "branch_filter": branch_filter or ""}
        sync_job = self.create_job(job_type=PointSyncJob.JOB_TYPE_PRODUCTION, triggered_by=triggered_by, parameters=parameters)
        self.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de sincronización Point producción.", context=parameters)
        try:
            lines = self.production_extractor.extract(start_date=start_date, end_date=end_date, branch_filter=branch_filter)
            summary = self.persist_production_lines(sync_job, lines)
            return self._mark_success(sync_job, summary)
        except PosBridgeError as exc:
            return self._mark_failure(sync_job, exc)
        except Exception as exc:
            return self._mark_failure(sync_job, PersistenceError(f"Error no controlado en sync de producción Point: {exc}"))

    def run_transfer_sync(self, *, start_date: date, end_date: date, branch_filter: str | None = None, triggered_by=None) -> PointSyncJob:
        parameters = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "branch_filter": branch_filter or ""}
        sync_job = self.create_job(job_type=PointSyncJob.JOB_TYPE_TRANSFERS, triggered_by=triggered_by, parameters=parameters)
        self.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de sincronización Point transferencias.", context=parameters)
        try:
            lines = self.transfer_extractor.extract(start_date=start_date, end_date=end_date, branch_filter=branch_filter)
            summary = self.persist_transfer_lines(sync_job, lines)
            return self._mark_success(sync_job, summary)
        except PosBridgeError as exc:
            return self._mark_failure(sync_job, exc)
        except Exception as exc:
            return self._mark_failure(sync_job, PersistenceError(f"Error no controlado en sync de transferencias Point: {exc}"))
