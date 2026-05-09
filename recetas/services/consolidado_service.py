from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import Count, Max, Q
from django.utils import timezone

from core.models import sucursales_operativas
from pos_bridge.models import PointInventorySnapshot, PointSyncJob, PointTransferLine
from pos_bridge.services.open_transfer_sync_service import (
    OpenTransferSyncService,
    resolve_requesting_erp_branch,
)
from pos_bridge.tasks.run_inventory_sync import run_inventory_sync
from recetas.models import (
    ConsolidadoNocturnoCEDIS,
    PlanProduccion,
    PlanProduccionItem,
    SolicitudReabastoCedis,
    SolicitudReabastoCedisLinea,
)
from recetas.views.reabasto import _consolidado_reabasto_por_fecha, _upsert_plan_reabasto_cedis


logger = logging.getLogger(__name__)


def _consolidado_cache_key(fecha_operacion: date) -> str:
    return f"reabasto:consolidado:{fecha_operacion.isoformat()}"


class ConsolidadoNocturnoCedisService:
    def __init__(self, open_transfer_sync_service: OpenTransferSyncService | None = None):
        self.open_transfer_sync_service = open_transfer_sync_service or OpenTransferSyncService()

    def consolidar(
        self,
        *,
        fecha_operacion: date | None = None,
        usuario=None,
        sincronizar_point: bool = True,
        sincronizar_inventario_cedis: bool = True,
        forzar_recalculo: bool = False,
    ) -> ConsolidadoNocturnoCEDIS:
        fecha_operacion = fecha_operacion or timezone.localdate()
        consolidado_existente = ConsolidadoNocturnoCEDIS.objects.filter(fecha_operacion=fecha_operacion).first()
        if consolidado_existente and consolidado_existente.plan_produccion_id and not forzar_recalculo:
            return consolidado_existente
        if consolidado_existente and not consolidado_existente.plan_produccion_id and not forzar_recalculo:
            sincronizar_point = False

        inventory_sync_job = None
        inventory_sync_error = ""
        inventory_sync_skipped_reason = ""
        sync_job = None
        if sincronizar_point:
            if sincronizar_inventario_cedis:
                freshness = self._cedis_inventory_freshness()
                if freshness["fresh"]:
                    inventory_sync_skipped_reason = freshness["reason"]
                else:
                    try:
                        inventory_sync_job = self._sincronizar_inventario_cedis(usuario=usuario)
                    except Exception as exc:  # noqa: BLE001 - no debe bloquear solicitudes de sucursales
                        inventory_sync_error = str(exc)
                        logger.warning(
                            "No se pudo sincronizar inventario CEDIS antes del consolidado %s; se continuará con solicitudes: %s",
                            fecha_operacion,
                            exc,
                        )
            sync_job = self.open_transfer_sync_service.sync_open_transfers(
                fecha=fecha_operacion,
                triggered_by=usuario,
            )

        with transaction.atomic():
            if sincronizar_point:
                self._crear_solicitudes_desde_point(fecha_operacion=fecha_operacion, usuario=usuario, sync_job=sync_job)

            cache.delete(_consolidado_cache_key(fecha_operacion))
            rows = _consolidado_reabasto_por_fecha(fecha_operacion)
            plan, _, total_plan = _upsert_plan_reabasto_cedis(fecha_operacion, usuario)
            cache.delete(_consolidado_cache_key(fecha_operacion))
            cobertura = self._calcular_cobertura(fecha_operacion)
            total_sugerido = sum((Decimal(str(row.get("total_sugerido") or 0)) for row in rows), Decimal("0"))
            total_solicitado = sum((Decimal(str(row.get("total_solicitado") or 0)) for row in rows), Decimal("0"))
            consolidado, _ = ConsolidadoNocturnoCEDIS.objects.update_or_create(
                fecha_operacion=fecha_operacion,
                defaults={
                    "estado": ConsolidadoNocturnoCEDIS.ESTADO_PLAN_GENERADO,
                    "sync_job": sync_job,
                    "plan_produccion": plan,
                    "sucursales_esperadas": cobertura["sucursales_esperadas"],
                    "sucursales_con_solicitud": cobertura["sucursales_con_solicitud"],
                    "sucursales_sin_solicitud": cobertura["sucursales_sin_solicitud"],
                    "productos_consolidados": len(rows),
                    "total_sugerido": total_sugerido,
                    "total_solicitado": total_solicitado,
                    "total_plan_produccion": Decimal(str(total_plan or 0)),
                    "metadata": {
                        "inventory_sync_job_id": inventory_sync_job.id if inventory_sync_job else None,
                        "inventory_sync_error": inventory_sync_error,
                        "inventory_sync_skipped_reason": inventory_sync_skipped_reason,
                        "sync_job_id": sync_job.id if sync_job else None,
                        "coverage_branch_ids": cobertura["branch_ids"],
                        "source_order": [
                            "point_inventory_cedis",
                            "point_open_transfers",
                            "erp_solicitudes_reabasto_cedis",
                            "plan_produccion",
                        ],
                    },
                    "creado_por": usuario,
                },
            )
            self._marcar_plan_automatico(plan=plan, total_plan=total_plan)
            return consolidado

    def _cedis_inventory_freshness(self) -> dict[str, object]:
        freshness_minutes = int(getattr(settings, "CONSOLIDADO_CEDIS_INVENTORY_FRESHNESS_MINUTES", 180))
        cutoff = timezone.now() - timedelta(minutes=freshness_minutes)
        latest = (
            PointInventorySnapshot.objects.filter(
                Q(branch__name__iexact="CEDIS") | Q(branch__external_id__iexact="8")
            )
            .aggregate(captured_at=Max("captured_at"))
            .get("captured_at")
        )
        if latest and latest >= cutoff:
            return {
                "fresh": True,
                "latest": latest,
                "reason": f"snapshot_cedis_fresco:{latest.isoformat()}",
            }
        return {
            "fresh": False,
            "latest": latest,
            "reason": "snapshot_cedis_vencido_o_inexistente",
        }

    def _sincronizar_inventario_cedis(self, *, usuario=None) -> PointSyncJob:
        sync_job = run_inventory_sync(
            triggered_by=usuario,
            branch_filter="CEDIS",
            limit_branches=1,
            capture_costs=False,
        )
        if sync_job.status != PointSyncJob.STATUS_SUCCESS:
            raise RuntimeError(
                "No se pudo sincronizar inventario CEDIS desde PointMeUp antes del consolidado: "
                f"{sync_job.error_message or sync_job.status}"
            )
        summary = sync_job.result_summary or {}
        if int(summary.get("snapshots_created") or 0) <= 0:
            raise RuntimeError("La sincronización de inventario CEDIS desde PointMeUp no creó snapshots.")
        return sync_job

    def get_resumen(self, *, fecha_operacion: date | None = None) -> dict:
        fecha_operacion = fecha_operacion or timezone.localdate()
        consolidado = ConsolidadoNocturnoCEDIS.objects.filter(fecha_operacion=fecha_operacion).first()
        rows = _consolidado_reabasto_por_fecha(fecha_operacion)
        plan = getattr(consolidado, "plan_produccion", None) if consolidado else None
        if plan is None:
            plan = PlanProduccion.objects.filter(fecha_produccion=fecha_operacion).order_by("-id").first()
        plan_items = {}
        if plan:
            plan_items = {
                item.receta_id: item
                for item in plan.items.select_related("receta").all()
            }
        for row in rows:
            item = plan_items.get(row.get("receta_id"))
            item_metadata = item.metadata if item and isinstance(item.metadata, dict) else {}
            row["plan_item_id"] = item.id if item else None
            row["cantidad_autorizada"] = item.cantidad_autorizada if item else Decimal("0")
            row["cantidad_plan"] = item.cantidad if item else Decimal("0")
            row["item_autorizado"] = bool((plan and plan.autorizado) or item_metadata.get("autorizado"))
        return {
            "fecha_operacion": fecha_operacion,
            "consolidado": consolidado,
            "plan": plan,
            "rows": rows,
            "cobertura": self._calcular_cobertura(fecha_operacion),
        }

    def _crear_solicitudes_desde_point(self, *, fecha_operacion: date, usuario=None, sync_job=None) -> dict:
        filters = {
            "is_open": True,
            "is_cancelled": False,
            "receta__isnull": False,
        }
        if sync_job is not None:
            filters["sync_job"] = sync_job
        else:
            filters["registered_at__date"] = fecha_operacion
        transfer_lines = (
            PointTransferLine.objects.filter(**filters).select_related(
                "origin_branch",
                "destination_branch",
                "erp_origin_branch",
                "erp_destination_branch",
                "receta",
            )
        )
        grouped = defaultdict(lambda: defaultdict(Decimal))
        for line in transfer_lines:
            branch = resolve_requesting_erp_branch(line)
            if branch is None:
                continue
            grouped[branch][line.receta] += Decimal(str(line.requested_quantity or line.sent_quantity or 0))

        solicitudes_creadas = 0
        lineas_actualizadas = 0
        for branch, recetas in grouped.items():
            solicitud, created = SolicitudReabastoCedis.objects.get_or_create(
                fecha_operacion=fecha_operacion,
                sucursal=branch,
                defaults={
                    "estado": SolicitudReabastoCedis.ESTADO_ENVIADA,
                    "notas": "Generada automáticamente desde transferencias abiertas PointMeUp.",
                    "creado_por": usuario,
                },
            )
            if created:
                solicitudes_creadas += 1
            if solicitud.estado == SolicitudReabastoCedis.ESTADO_BORRADOR:
                solicitud.estado = SolicitudReabastoCedis.ESTADO_ENVIADA
                solicitud.notas = (solicitud.notas + "\n" if solicitud.notas else "") + (
                    "Marcada como enviada por consolidado nocturno PointMeUp."
                )
                solicitud.save(update_fields=["estado", "notas", "actualizado_en"])
            for receta, cantidad in recetas.items():
                SolicitudReabastoCedisLinea.objects.update_or_create(
                    solicitud=solicitud,
                    receta=receta,
                    defaults={
                        "solicitado": cantidad,
                        "sugerido": cantidad,
                        "justificacion": "Transferencia abierta PointMeUp.",
                    },
                )
                lineas_actualizadas += 1
        return {"solicitudes_creadas": solicitudes_creadas, "lineas_actualizadas": lineas_actualizadas}

    def _calcular_cobertura(self, fecha_operacion: date) -> dict:
        expected_ids = set(sucursales_operativas(fecha_operacion).values_list("id", flat=True))
        submitted = (
            SolicitudReabastoCedis.objects.filter(fecha_operacion=fecha_operacion)
            .exclude(estado=SolicitudReabastoCedis.ESTADO_CANCELADA)
            .values("sucursal_id")
            .annotate(lineas=Count("lineas"))
            .filter(lineas__gt=0)
        )
        submitted_ids = {row["sucursal_id"] for row in submitted if row["sucursal_id"]}
        missing_ids = expected_ids - submitted_ids
        return {
            "sucursales_esperadas": len(expected_ids),
            "sucursales_con_solicitud": len(submitted_ids & expected_ids),
            "sucursales_sin_solicitud": len(missing_ids),
            "branch_ids": sorted(submitted_ids),
            "missing_branch_ids": sorted(missing_ids),
        }

    def _marcar_plan_automatico(self, *, plan: PlanProduccion | None, total_plan) -> None:
        if plan is None:
            return
        PlanProduccion.objects.filter(pk=plan.pk).update(
            origen_automatizacion="CONSOLIDADO_NOCTURNO_CEDIS",
            metadata={**(plan.metadata or {}), "total_plan_reabasto_cedis": str(total_plan or 0)},
        )
        items = PlanProduccionItem.objects.filter(plan=plan)
        for item in items:
            if not item.cantidad_sugerida:
                item.cantidad_sugerida = item.cantidad
            if not item.cantidad_autorizada:
                item.cantidad_autorizada = item.cantidad
            item.save(update_fields=["cantidad_sugerida", "cantidad_autorizada"])
