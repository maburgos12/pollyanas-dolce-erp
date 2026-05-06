from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import calendar
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone
from unidecode import unidecode

from core.models import AuditLog
from compras.models import (
    PresupuestoCompraCategoria,
    PresupuestoCompraPeriodo,
    PresupuestoCompraProveedor,
    SolicitudCompra,
)
from inventario.models import AjusteInventario, ExistenciaInsumo
from maestros.utils.canonical_catalog import (
    canonical_insumo_by_id,
    canonicalized_active_insumos,
    enterprise_readiness_profile,
    latest_costo_canonico,
)
from orquestacion.models import AgentDefinition, AgentSuggestion, AgentTask, OrchestrationRule, OrchestrationRun
from orquestacion.services.agent_runtime import Goal, run_agent_goal
from recetas.models import LineaReceta, PlanProduccion, PronosticoVenta, SolicitudVenta, VentaHistorica


@dataclass(frozen=True)
class RuleRunResult:
    created: bool
    status: str
    message: str
    run_id: int | None = None
    task_id: int | None = None
    suggestion_id: int | None = None


SUPPORTED_RULE_CODES = (
    "daily_production_plan_missing",
    "plan_demand_production_purchase_chain",
    "purchase_exception_requires_dg_approval",
    "inventory_adjustment_authorization_guard",
    "near_expiry_or_low_rotation_review",
)


def run_daily_production_plan_missing(
    *,
    reference_dt: datetime | None = None,
    force: bool = False,
    created_by=None,
    trigger_source: str = "management_command",
) -> RuleRunResult:
    rule = OrchestrationRule.objects.select_related("primary_agent", "secondary_agent").get(
        code="daily_production_plan_missing"
    )
    local_dt = _coerce_local_datetime(reference_dt)
    production_date = local_dt.date()
    cutoff = _extract_cutoff(rule)

    if not force and local_dt.time() < cutoff:
        return RuleRunResult(
            created=False,
            status="skipped_before_cutoff",
            message=(
                f"Aun no aplica la regla: el corte es {cutoff.strftime('%H:%M')} y "
                f"la referencia es {local_dt.strftime('%H:%M')}."
            ),
        )

    if _has_recent_run(rule=rule, production_date=production_date, reference_dt=local_dt):
        return RuleRunResult(
            created=False,
            status="skipped_cooldown",
            message=f"Ya existe una corrida reciente para {production_date.isoformat()} dentro del cooldown configurado.",
        )

    plan = PlanProduccion.objects.filter(fecha_produccion=production_date).order_by("-creado_en").first()
    run_key = _build_run_key(rule.code, production_date.isoformat(), local_dt.strftime("%Y%m%d%H%M%S%f"))

    with transaction.atomic():
        run = OrchestrationRun.objects.create(
            run_key=run_key,
            trigger_source=trigger_source,
            rule=rule,
            status=OrchestrationRun.STATUS_SUCCESS,
            started_at=local_dt,
            finished_at=local_dt,
            context_json={
                "production_date": production_date.isoformat(),
                "cutoff_local_time": cutoff.strftime("%H:%M"),
                "force": force,
            },
            result_summary_json={
                "rule_code": rule.code,
                "plan_exists": bool(plan),
            },
            created_by=created_by,
        )
        _log_run_creation(run=run, rule=rule, created_by=created_by)

        if plan:
            run.result_summary_json = {
                **run.result_summary_json,
                "plan_id": plan.id,
                "plan_estado": plan.estado,
                "message": "Plan diario encontrado; no se genero sugerencia.",
            }
            run.save(update_fields=["result_summary_json"])
            return RuleRunResult(
                created=True,
                status="success_no_issue",
                message=f"Plan diario encontrado para {production_date.isoformat()} (plan_id={plan.id}).",
                run_id=run.id,
            )

        task = AgentTask.objects.create(
            run=run,
            agent=rule.primary_agent,
            title=f"Validar plan diario faltante para {production_date.isoformat()}",
            task_type="daily_production_plan_missing",
            priority=AgentTask.PRIORITY_CRITICAL,
            status=AgentTask.STATUS_PENDING,
            input_payload={
                "production_date": production_date.isoformat(),
                "cutoff_local_time": cutoff.strftime("%H:%M"),
                "rule_code": rule.code,
            },
            resolution_note="Pendiente de confirmar por Produccion o escalar a Direccion Operativa.",
        )
        suggestion = AgentSuggestion.objects.create(
            task=task,
            suggestion_type="missing_daily_production_plan",
            domain="produccion",
            severity=AgentSuggestion.SEVERITY_CRITICAL,
            summary=f"No hay plan de Produccion cargado para {production_date.isoformat()}",
            details_json={
                "production_date": production_date.isoformat(),
                "cutoff_local_time": cutoff.strftime("%H:%M"),
                "secondary_agent": rule.secondary_agent.code if rule.secondary_agent_id else "",
                "requires_human_approval": True,
            },
            recommended_action=(
                "Escalar a Director Operativo y capturar el plan diario antes de liberar compras, "
                "reabasto o compromisos de surtido."
            ),
            requires_approval=True,
        )
        _log_suggestion_creation(suggestion=suggestion, rule=rule, created_by=created_by)
        run.result_summary_json = {
            **run.result_summary_json,
            "task_id": task.id,
            "suggestion_id": suggestion.id,
            "message": "Se genero tarea critica por ausencia de plan diario.",
        }
        run.save(update_fields=["result_summary_json"])
        return RuleRunResult(
            created=True,
            status="success_issue_created",
            message=f"Se genero sugerencia critica para {production_date.isoformat()}.",
            run_id=run.id,
            task_id=task.id,
            suggestion_id=suggestion.id,
        )


def run_purchase_exception_requires_dg_approval(
    *,
    reference_dt: datetime | None = None,
    created_by=None,
    trigger_source: str = "management_command",
) -> RuleRunResult:
    rule = OrchestrationRule.objects.select_related("primary_agent", "secondary_agent").get(
        code="purchase_exception_requires_dg_approval"
    )
    local_dt = _coerce_local_datetime(reference_dt)
    recent_ids = _recent_purchase_exception_request_ids(rule=rule, reference_dt=local_dt)
    candidates = []
    solicitudes = (
        SolicitudCompra.objects.select_related(
            "insumo",
            "proveedor_sugerido",
            "insumo__proveedor_principal",
        )
        .filter(estatus__in=[SolicitudCompra.STATUS_BORRADOR, SolicitudCompra.STATUS_EN_REVISION])
        .order_by("fecha_requerida", "id")
    )
    budget_cache: dict[str, dict[str, object]] = {}
    for solicitud in solicitudes:
        if solicitud.id in recent_ids:
            continue
        assessment = _assess_purchase_request_exception(solicitud=solicitud, budget_cache=budget_cache)
        if assessment["reasons"]:
            candidates.append((solicitud, assessment))

    run_key = _build_run_key(rule.code, local_dt.strftime("%Y%m%d%H%M%S%f"))
    with transaction.atomic():
        run = OrchestrationRun.objects.create(
            run_key=run_key,
            trigger_source=trigger_source,
            rule=rule,
            status=OrchestrationRun.STATUS_SUCCESS,
            started_at=local_dt,
            finished_at=local_dt,
            context_json={
                "reference_date": local_dt.date().isoformat(),
                "candidate_ids": [solicitud.id for solicitud, _ in candidates],
            },
            result_summary_json={
                "rule_code": rule.code,
                "candidate_count": len(candidates),
                "cooldown_skipped_ids": sorted(recent_ids),
            },
            created_by=created_by,
        )
        _log_run_creation(run=run, rule=rule, created_by=created_by)

        if not candidates:
            run.result_summary_json = {
                **run.result_summary_json,
                "message": "No se detectaron solicitudes con excepcion cuantificable para DG.",
            }
            run.save(update_fields=["result_summary_json"])
            return RuleRunResult(
                created=True,
                status="success_no_issue",
                message="No se detectaron solicitudes de compra que requieran aprobacion DG con los datos actuales.",
                run_id=run.id,
            )

        created_suggestions = []
        created_tasks = []
        for solicitud, assessment in candidates:
            severity = (
                AgentSuggestion.SEVERITY_CRITICAL
                if (
                    "amount_gt_5000" in assessment["reason_codes"]
                    or "monthly_budget_exceeded" in assessment["reason_codes"]
                    or "out_of_catalog" in assessment["reason_codes"]
                )
                else AgentSuggestion.SEVERITY_HIGH
            )
            task = AgentTask.objects.create(
                run=run,
                agent=rule.primary_agent,
                title=f"Validar excepcion DG para solicitud {solicitud.folio}",
                task_type="purchase_exception_requires_dg_approval",
                priority=AgentTask.PRIORITY_HIGH,
                status=AgentTask.STATUS_PENDING,
                input_payload={
                    "solicitud_id": solicitud.id,
                    "solicitud_folio": solicitud.folio,
                    "fecha_requerida": solicitud.fecha_requerida.isoformat(),
                    "reasons": assessment["reasons"],
                },
                resolution_note="Pendiente de validar con Compras y escalar a Direccion General si procede.",
            )
            created_tasks.append(task.id)
            suggestion = AgentSuggestion.objects.create(
                task=task,
                suggestion_type="purchase_exception_requires_dg_approval",
                domain="compras",
                severity=severity,
                summary=f"Solicitud {solicitud.folio} requiere validacion DG",
                details_json={
                    "solicitud_id": solicitud.id,
                    "solicitud_folio": solicitud.folio,
                    "estimated_amount": str(assessment["estimated_amount"]),
                    "provider_name": assessment["provider_name"],
                    "category_name": assessment["category_name"],
                    "fuera_de_catalogo": solicitud.fuera_de_catalogo,
                    "cotizaciones_requeridas": assessment["quotes_required"],
                    "cotizaciones_recibidas": assessment["quotes_received"],
                    "justificacion_excepcion": solicitud.justificacion_excepcion,
                    "reason_codes": assessment["reason_codes"],
                    "reasons": assessment["reasons"],
                },
                recommended_action=(
                    "Revisar con Direccion General antes de aprobar o convertir a orden. "
                    "Verificar evidencia de cotizaciones y justificar excepciones fuera de catalogo dentro del expediente de compra."
                ),
                requires_approval=True,
            )
            created_suggestions.append(suggestion.id)
            _log_suggestion_creation(suggestion=suggestion, rule=rule, created_by=created_by)

        run.result_summary_json = {
            **run.result_summary_json,
            "suggestion_count": len(created_suggestions),
            "suggestion_ids": created_suggestions,
            "message": "Se detectaron solicitudes con excepcion de monto o presupuesto.",
        }
        run.save(update_fields=["result_summary_json"])
        return RuleRunResult(
            created=True,
            status="success_issue_created",
            message=f"Se generaron {len(created_suggestions)} sugerencias de excepcion de compra para DG.",
            run_id=run.id,
            task_id=created_tasks[0] if created_tasks else None,
            suggestion_id=created_suggestions[0] if created_suggestions else None,
        )


def run_plan_demand_production_purchase_chain(
    *,
    reference_dt: datetime | None = None,
    created_by=None,
    trigger_source: str = "management_command",
) -> RuleRunResult:
    rule = OrchestrationRule.objects.select_related("primary_agent", "secondary_agent").get(
        code="plan_demand_production_purchase_chain"
    )
    local_dt = _coerce_local_datetime(reference_dt)
    production_date = local_dt.date()
    plan = (
        PlanProduccion.objects.prefetch_related("items", "items__receta")
        .filter(fecha_produccion=production_date)
        .order_by("-creado_en", "-id")
        .first()
    )
    if not plan:
        return RuleRunResult(
            created=True,
            status="success_no_issue",
            message=(
                f"No existe plan para {production_date.isoformat()}. "
                "La ausencia de plan ya se controla con daily_production_plan_missing."
            ),
        )

    if _has_recent_plan_chain_run(rule=rule, plan_id=plan.id, reference_dt=local_dt):
        return RuleRunResult(
            created=False,
            status="skipped_cooldown",
            message=f"Ya existe una corrida reciente de cadena operativa para el plan {plan.id}.",
        )

    demand_summary = _build_chain_demand_summary(plan=plan, rule=rule)
    production_summary = _build_chain_production_summary(plan=plan)
    purchase_summary = _build_chain_purchase_summary(plan=plan)
    director_summary = _build_chain_director_summary(
        plan=plan,
        demand_summary=demand_summary,
        production_summary=production_summary,
        purchase_summary=purchase_summary,
    )

    if not director_summary["should_create_chain"]:
        run_key = _build_run_key(rule.code, str(plan.id), local_dt.strftime("%Y%m%d%H%M%S%f"))
        with transaction.atomic():
            run = OrchestrationRun.objects.create(
                run_key=run_key,
                trigger_source=trigger_source,
                rule=rule,
                status=OrchestrationRun.STATUS_SUCCESS,
                started_at=local_dt,
                finished_at=local_dt,
                context_json={
                    "plan_id": plan.id,
                    "production_date": production_date.isoformat(),
                },
                result_summary_json={
                    "rule_code": rule.code,
                    "plan_id": plan.id,
                    "plan_name": plan.nombre,
                    "message": "La cadena demanda-produccion-compras no detecto alertas para este plan.",
                    "director_tone": director_summary["tone"],
                },
                created_by=created_by,
            )
            _log_run_creation(run=run, rule=rule, created_by=created_by)
        return RuleRunResult(
            created=True,
            status="success_no_issue",
            message=f"El plan {plan.id} no presento alertas en la cadena demanda-produccion-compras.",
            run_id=run.id,
        )

    run_key = _build_run_key(rule.code, str(plan.id), local_dt.strftime("%Y%m%d%H%M%S%f"))
    with transaction.atomic():
        run = OrchestrationRun.objects.create(
            run_key=run_key,
            trigger_source=trigger_source,
            rule=rule,
            status=OrchestrationRun.STATUS_SUCCESS,
            started_at=local_dt,
            finished_at=local_dt,
            context_json={
                "plan_id": plan.id,
                "production_date": production_date.isoformat(),
                "known_gaps": list((rule.condition_json or {}).get("known_gaps") or []),
            },
            result_summary_json={
                "rule_code": rule.code,
                "plan_id": plan.id,
                "plan_name": plan.nombre,
                "director_tone": director_summary["tone"],
                "driver_codes": director_summary["driver_codes"],
            },
            created_by=created_by,
        )
        _log_run_creation(run=run, rule=rule, created_by=created_by)

        created_task_ids: list[int] = []
        created_suggestion_ids: list[int] = []
        chain_specs = [
            (
                "agente_demanda_ventas",
                demand_summary,
                "plan_demand_chain_review",
                "plan_demand_chain_review",
                "ventas",
                True,
            ),
            (
                "agente_produccion",
                production_summary,
                "plan_production_chain_review",
                "plan_production_chain_review",
                "produccion",
                True,
            ),
            (
                "agente_compras",
                purchase_summary,
                "plan_purchase_chain_review",
                "plan_purchase_chain_review",
                "compras",
                True,
            ),
            (
                "director_operativo",
                director_summary,
                "plan_director_chain_review",
                "plan_director_chain_review",
                "operaciones",
                True,
            ),
        ]

        agents = {agent.code: agent for agent in AgentDefinition.objects.filter(code__in=[spec[0] for spec in chain_specs])}

        for agent_code, summary, task_type, suggestion_type, domain, requires_approval in chain_specs:
            agent = agents[agent_code]
            task = AgentTask.objects.create(
                run=run,
                agent=agent,
                title=summary["task_title"],
                task_type=task_type,
                priority=summary["priority"],
                status=AgentTask.STATUS_PENDING,
                input_payload=summary["input_payload"],
                output_payload=summary["output_payload"],
                resolution_note=summary["resolution_note"],
            )
            created_task_ids.append(task.id)
            suggestion = AgentSuggestion.objects.create(
                task=task,
                suggestion_type=suggestion_type,
                domain=domain,
                severity=summary["severity"],
                summary=summary["summary"],
                details_json=summary["details"],
                recommended_action=summary["recommended_action"],
                requires_approval=requires_approval,
            )
            created_suggestion_ids.append(suggestion.id)
            _log_suggestion_creation(suggestion=suggestion, rule=rule, created_by=created_by)

        run.result_summary_json = {
            **run.result_summary_json,
            "task_ids": created_task_ids,
            "suggestion_ids": created_suggestion_ids,
            "message": "Se genero la cadena demanda-produccion-compras-direccion para seguimiento operativo.",
        }
        run.save(update_fields=["result_summary_json"])

        return RuleRunResult(
            created=True,
            status="success_issue_created",
            message=(
                f"Se genero la cadena operativa del plan {plan.id} con {len(created_suggestion_ids)} sugerencias "
                "para demanda, produccion, compras y direccion."
            ),
            run_id=run.id,
            task_id=created_task_ids[0] if created_task_ids else None,
            suggestion_id=created_suggestion_ids[0] if created_suggestion_ids else None,
        )

def run_inventory_adjustment_authorization_guard(
    *,
    reference_dt: datetime | None = None,
    created_by=None,
    trigger_source: str = "management_command",
) -> RuleRunResult:
    rule = OrchestrationRule.objects.select_related("primary_agent", "secondary_agent").get(
        code="inventory_adjustment_authorization_guard"
    )
    local_dt = _coerce_local_datetime(reference_dt)
    recent_ids = _recent_inventory_adjustment_guard_ids(rule=rule, reference_dt=local_dt)
    candidates = []
    ajustes = (
        AjusteInventario.objects.select_related("insumo", "solicitado_por", "aprobado_por")
        .order_by("creado_en", "id")
    )
    for ajuste in ajustes:
        if ajuste.id in recent_ids:
            continue
        assessment = _assess_inventory_adjustment_guard(ajuste=ajuste, rule=rule, reference_dt=local_dt)
        if assessment["reason_codes"]:
            candidates.append((ajuste, assessment))

    run_key = _build_run_key(rule.code, local_dt.strftime("%Y%m%d%H%M%S%f"))
    with transaction.atomic():
        run = OrchestrationRun.objects.create(
            run_key=run_key,
            trigger_source=trigger_source,
            rule=rule,
            status=OrchestrationRun.STATUS_SUCCESS,
            started_at=local_dt,
            finished_at=local_dt,
            context_json={
                "reference_datetime": local_dt.isoformat(),
                "candidate_ids": [ajuste.id for ajuste, _ in candidates],
            },
            result_summary_json={
                "rule_code": rule.code,
                "candidate_count": len(candidates),
                "cooldown_skipped_ids": sorted(recent_ids),
            },
            created_by=created_by,
        )
        _log_run_creation(run=run, rule=rule, created_by=created_by)

        if not candidates:
            run.result_summary_json = {
                **run.result_summary_json,
                "message": "No se detectaron ajustes con riesgo de autorizacion o auditoria.",
            }
            run.save(update_fields=["result_summary_json"])
            return RuleRunResult(
                created=True,
                status="success_no_issue",
                message="No se detectaron ajustes de inventario con guardas de autorizacion rotas.",
                run_id=run.id,
            )

        created_suggestions = []
        created_tasks = []
        for ajuste, assessment in candidates:
            severity = (
                AgentSuggestion.SEVERITY_CRITICAL
                if assessment["is_integrity_issue"]
                else AgentSuggestion.SEVERITY_HIGH
            )
            task = AgentTask.objects.create(
                run=run,
                agent=rule.primary_agent,
                title=f"Validar autorizacion de ajuste {ajuste.folio}",
                task_type="inventory_adjustment_authorization_guard",
                priority=AgentTask.PRIORITY_CRITICAL if assessment["is_integrity_issue"] else AgentTask.PRIORITY_HIGH,
                status=AgentTask.STATUS_PENDING,
                input_payload={
                    "ajuste_id": ajuste.id,
                    "ajuste_folio": ajuste.folio,
                    "estatus": ajuste.estatus,
                    "reasons": assessment["reasons"],
                },
                resolution_note="Pendiente de validar por Administracion antes de cerrar el hallazgo.",
            )
            created_tasks.append(task.id)
            suggestion = AgentSuggestion.objects.create(
                task=task,
                suggestion_type="inventory_adjustment_authorization_guard",
                domain="inventario",
                severity=severity,
                summary=f"Ajuste {ajuste.folio} requiere revision de autorizacion",
                details_json={
                    "ajuste_id": ajuste.id,
                    "ajuste_folio": ajuste.folio,
                    "estatus": ajuste.estatus,
                    "solicitado_por": ajuste.solicitado_por.username if ajuste.solicitado_por_id else "",
                    "aprobado_por": ajuste.aprobado_por.username if ajuste.aprobado_por_id else "",
                    "reason_codes": assessment["reason_codes"],
                    "reasons": assessment["reasons"],
                    "approval_owner": rule.condition_json.get("approval_owner") or "ADMIN",
                },
                recommended_action=(
                    "Revisar evidencia y autorizacion del ajuste antes de considerar el flujo cerrado. "
                    "Si ya fue aprobado por fuera del sistema, regularizar inmediatamente la bitacora."
                ),
                requires_approval=True,
            )
            created_suggestions.append(suggestion.id)
            _log_suggestion_creation(suggestion=suggestion, rule=rule, created_by=created_by)

        run.result_summary_json = {
            **run.result_summary_json,
            "suggestion_count": len(created_suggestions),
            "suggestion_ids": created_suggestions,
            "message": "Se detectaron ajustes con riesgo de autorizacion o trazabilidad.",
        }
        run.save(update_fields=["result_summary_json"])
        return RuleRunResult(
            created=True,
            status="success_issue_created",
            message=f"Se generaron {len(created_suggestions)} sugerencias para revisar autorizacion de ajustes.",
            run_id=run.id,
            task_id=created_tasks[0] if created_tasks else None,
            suggestion_id=created_suggestions[0] if created_suggestions else None,
        )


def run_near_expiry_or_low_rotation_review(
    *,
    reference_dt: datetime | None = None,
    created_by=None,
    trigger_source: str = "management_command",
) -> RuleRunResult:
    rule = OrchestrationRule.objects.select_related("primary_agent", "secondary_agent").get(
        code="near_expiry_or_low_rotation_review"
    )
    local_dt = _coerce_local_datetime(reference_dt)
    recent_insumo_ids = _recent_low_rotation_insumo_ids(rule=rule, reference_dt=local_dt)
    candidates = []
    for snapshot in _inventory_rotation_snapshots():
        if snapshot["canonical_insumo_id"] in recent_insumo_ids:
            continue
        assessment = _assess_inventory_rotation(snapshot=snapshot, rule=rule)
        if assessment["reason_codes"]:
            candidates.append((snapshot, assessment))

    run_key = _build_run_key(rule.code, local_dt.strftime("%Y%m%d%H%M%S%f"))
    with transaction.atomic():
        run = OrchestrationRun.objects.create(
            run_key=run_key,
            trigger_source=trigger_source,
            rule=rule,
            status=OrchestrationRun.STATUS_SUCCESS,
            started_at=local_dt,
            finished_at=local_dt,
            context_json={
                "reference_datetime": local_dt.isoformat(),
                "candidate_insumo_ids": [snapshot["canonical_insumo_id"] for snapshot, _ in candidates],
            },
            result_summary_json={
                "rule_code": rule.code,
                "candidate_count": len(candidates),
                "cooldown_skipped_insumo_ids": sorted(recent_insumo_ids),
                "expiry_evaluation_status": "missing_source_data",
            },
            created_by=created_by,
        )
        _log_run_creation(run=run, rule=rule, created_by=created_by)

        if not candidates:
            run.result_summary_json = {
                **run.result_summary_json,
                "message": "No se detectaron existencias con baja rotacion usando los datos actuales del ERP.",
            }
            run.save(update_fields=["result_summary_json"])
            return RuleRunResult(
                created=True,
                status="success_no_issue",
                message="No se detectaron existencias de baja rotacion con los datos actuales del ERP.",
                run_id=run.id,
            )

        created_suggestions = []
        created_tasks = []
        for snapshot, assessment in candidates:
            severity = (
                AgentSuggestion.SEVERITY_HIGH
                if "zero_consumption_with_stock" in assessment["reason_codes"]
                else AgentSuggestion.SEVERITY_WARNING
            )
            task = AgentTask.objects.create(
                run=run,
                agent=rule.primary_agent,
                title=f"Revisar baja rotacion de {snapshot['canonical_insumo_nombre']}",
                task_type="near_expiry_or_low_rotation_review",
                priority=AgentTask.PRIORITY_HIGH if severity == AgentSuggestion.SEVERITY_HIGH else AgentTask.PRIORITY_MEDIUM,
                status=AgentTask.STATUS_PENDING,
                input_payload={
                    "canonical_insumo_id": snapshot["canonical_insumo_id"],
                    "canonical_insumo_nombre": snapshot["canonical_insumo_nombre"],
                    "reason_codes": assessment["reason_codes"],
                    "days_of_cover": assessment["days_of_cover"],
                },
                resolution_note=(
                    "Pendiente de validar si existe riesgo operativo por baja rotacion. "
                    "La caducidad no se evalua automaticamente mientras no exista lote o fecha de vencimiento persistidos."
                ),
            )
            created_tasks.append(task.id)
            suggestion = AgentSuggestion.objects.create(
                task=task,
                suggestion_type="near_expiry_or_low_rotation_review",
                domain="inventario",
                severity=severity,
                summary=f"Existencia de baja rotacion detectada en {snapshot['canonical_insumo_nombre']}",
                details_json={
                    "canonical_insumo_id": snapshot["canonical_insumo_id"],
                    "canonical_insumo_nombre": snapshot["canonical_insumo_nombre"],
                    "member_ids": snapshot["member_ids"],
                    "stock_actual": str(snapshot["stock_actual"]),
                    "stock_minimo": str(snapshot["stock_minimo"]),
                    "inventario_promedio": str(snapshot["inventario_promedio"]),
                    "consumo_diario_promedio": str(snapshot["consumo_diario_promedio"]),
                    "rotation_index": str(assessment["rotation_index"]),
                    "days_of_cover": assessment["days_of_cover"],
                    "reason_codes": assessment["reason_codes"],
                    "reasons": assessment["reasons"],
                    "expiry_evaluation_status": "missing_source_data",
                    "secondary_agent": rule.secondary_agent.code if rule.secondary_agent_id else "",
                },
                recommended_action=(
                    "Revisar consumo real, plan de produccion y necesidad de recompra antes de seguir abasteciendo este insumo. "
                    "Validar por fuera del sistema si existe riesgo de caducidad mientras no se controle lote o fecha de vencimiento en ERP."
                ),
                requires_approval=False,
            )
            created_suggestions.append(suggestion.id)
            _log_suggestion_creation(suggestion=suggestion, rule=rule, created_by=created_by)

        run.result_summary_json = {
            **run.result_summary_json,
            "suggestion_count": len(created_suggestions),
            "suggestion_ids": created_suggestions,
            "message": "Se detectaron existencias con baja rotacion para revision operativa.",
        }
        run.save(update_fields=["result_summary_json"])
        return RuleRunResult(
            created=True,
            status="success_issue_created",
            message=f"Se generaron {len(created_suggestions)} sugerencias por baja rotacion.",
            run_id=run.id,
            task_id=created_tasks[0] if created_tasks else None,
            suggestion_id=created_suggestions[0] if created_suggestions else None,
        )


def resolve_created_by(username: str | None):
    if not username:
        return None
    user_model = get_user_model()
    return user_model.objects.filter(username=username).first()


def run_rule_by_code(
    rule_code: str,
    *,
    reference_dt: datetime | None = None,
    created_by=None,
    force: bool = False,
    trigger_source: str = "management_command",
    event_id: int | None = None,
) -> RuleRunResult:
    normalized_code = (rule_code or "").strip()
    if normalized_code == "daily_production_plan_missing":
        return run_daily_production_plan_missing(
            reference_dt=reference_dt,
            created_by=created_by,
            force=force,
            trigger_source=trigger_source,
        )
    if normalized_code == "plan_demand_production_purchase_chain":
        return run_plan_demand_production_purchase_chain(
            reference_dt=reference_dt,
            created_by=created_by,
            trigger_source=trigger_source,
        )
    if normalized_code == "purchase_exception_requires_dg_approval":
        return run_purchase_exception_requires_dg_approval(
            reference_dt=reference_dt,
            created_by=created_by,
            trigger_source=trigger_source,
        )
    if normalized_code == "inventory_adjustment_authorization_guard":
        return run_inventory_adjustment_authorization_guard(
            reference_dt=reference_dt,
            created_by=created_by,
            trigger_source=trigger_source,
        )
    if normalized_code == "near_expiry_or_low_rotation_review":
        return run_near_expiry_or_low_rotation_review(
            reference_dt=reference_dt,
            created_by=created_by,
            trigger_source=trigger_source,
        )
    raise ValueError(
        f"La regla '{rule_code}' todavia no tiene runner implementado. "
        f"Reglas soportadas: {', '.join(SUPPORTED_RULE_CODES)}."
    )


def _coerce_local_datetime(reference_dt: datetime | None) -> datetime:
    if reference_dt is None:
        return timezone.localtime()
    if timezone.is_naive(reference_dt):
        return timezone.make_aware(reference_dt, timezone.get_current_timezone())
    return timezone.localtime(reference_dt)


def _build_run_key(rule_code: str, *parts: str) -> str:
    clean_parts = [str(part).strip() for part in parts if str(part).strip()]
    clean_parts.append(uuid4().hex[:10])
    return ":".join([rule_code, *clean_parts])


def _extract_cutoff(rule: OrchestrationRule) -> time:
    schedule = rule.condition_json.get("schedule") or {}
    cutoff_raw = str(schedule.get("cutoff_local_time") or "09:00")
    hour, minute = cutoff_raw.split(":", 1)
    return time(hour=int(hour), minute=int(minute))



def _has_recent_run(*, rule: OrchestrationRule, production_date: date, reference_dt: datetime) -> bool:
    if not rule.cooldown_minutes:
        return False
    window_start = reference_dt - timedelta(minutes=rule.cooldown_minutes)
    runs = OrchestrationRun.objects.filter(rule=rule, started_at__gte=window_start)
    for run in runs.only("context_json"):
        if (run.context_json or {}).get("production_date") == production_date.isoformat():
            return True
    return False


def _has_recent_plan_chain_run(*, rule: OrchestrationRule, plan_id: int, reference_dt: datetime) -> bool:
    if not rule.cooldown_minutes:
        return False
    window_start = reference_dt - timedelta(minutes=rule.cooldown_minutes)
    runs = OrchestrationRun.objects.filter(rule=rule, started_at__gte=window_start)
    for run in runs.only("context_json"):
        if int((run.context_json or {}).get("plan_id") or 0) == int(plan_id):
            return True
    return False


def _recent_purchase_exception_request_ids(*, rule: OrchestrationRule, reference_dt: datetime) -> set[int]:
    if not rule.cooldown_minutes:
        return set()
    window_start = reference_dt - timedelta(minutes=rule.cooldown_minutes)
    suggestions = AgentSuggestion.objects.filter(
        task__run__rule=rule,
        created_at__gte=window_start,
        suggestion_type="purchase_exception_requires_dg_approval",
    ).only("details_json")
    request_ids: set[int] = set()
    for suggestion in suggestions:
        solicitud_id = (suggestion.details_json or {}).get("solicitud_id")
        if solicitud_id:
            request_ids.add(int(solicitud_id))
    return request_ids


def _recent_inventory_adjustment_guard_ids(*, rule: OrchestrationRule, reference_dt: datetime) -> set[int]:
    if not rule.cooldown_minutes:
        return set()
    window_start = reference_dt - timedelta(minutes=rule.cooldown_minutes)
    suggestions = AgentSuggestion.objects.filter(
        task__run__rule=rule,
        created_at__gte=window_start,
        suggestion_type="inventory_adjustment_authorization_guard",
    ).only("details_json")
    ajuste_ids: set[int] = set()
    for suggestion in suggestions:
        ajuste_id = (suggestion.details_json or {}).get("ajuste_id")
        if ajuste_id:
            ajuste_ids.add(int(ajuste_id))
    return ajuste_ids


def _recent_low_rotation_insumo_ids(*, rule: OrchestrationRule, reference_dt: datetime) -> set[int]:
    if not rule.cooldown_minutes:
        return set()
    window_start = reference_dt - timedelta(minutes=rule.cooldown_minutes)
    suggestions = AgentSuggestion.objects.filter(
        task__run__rule=rule,
        created_at__gte=window_start,
        suggestion_type="near_expiry_or_low_rotation_review",
    ).only("details_json")
    insumo_ids: set[int] = set()
    for suggestion in suggestions:
        insumo_id = (suggestion.details_json or {}).get("canonical_insumo_id")
        if insumo_id:
            insumo_ids.add(int(insumo_id))
    return insumo_ids


def _assess_purchase_request_exception(*, solicitud: SolicitudCompra, budget_cache: dict[str, dict[str, object]]) -> dict[str, object]:
    canonical = canonical_insumo_by_id(solicitud.insumo_id) or solicitud.insumo
    estimated_amount = _estimate_solicitud_amount(solicitud=solicitud, canonical=canonical)
    provider = solicitud.proveedor_sugerido or getattr(canonical, "proveedor_principal", None)
    provider_name = provider.nombre if provider else "Sin proveedor"
    category_name = _resolve_category_name(canonical)
    quotes_required = max(int(solicitud.cotizaciones_requeridas or 0), 0)
    quotes_received = max(int(solicitud.cotizaciones_recibidas or 0), 0)

    reasons: list[str] = []
    reason_codes: list[str] = []

    if estimated_amount > Decimal("5000"):
        reasons.append(f"Monto estimado {estimated_amount:.2f} mayor a 5000 MXN.")
        reason_codes.append("amount_gt_5000")
        quotes_required = max(quotes_required, 3)

    if solicitud.fuera_de_catalogo:
        reasons.append("Solicitud marcada fuera de catálogo operativo.")
        reason_codes.append("out_of_catalog")

    period_key = solicitud.fecha_requerida.strftime("%Y-%m")
    budget_snapshot = budget_cache.get(period_key)
    if budget_snapshot is None:
        budget_snapshot = _build_purchase_budget_snapshot(period_key=period_key)
        budget_cache[period_key] = budget_snapshot

    monthly_total = Decimal(str(budget_snapshot["monthly_total"] or Decimal("0")))
    monthly_target = Decimal(str(budget_snapshot["monthly_target"] or Decimal("0")))
    if monthly_target > 0 and monthly_total > monthly_target:
        reasons.append(
            f"El acumulado estimado del periodo {period_key} ({monthly_total:.2f}) supera el objetivo mensual ({monthly_target:.2f})."
        )
        reason_codes.append("monthly_budget_exceeded")

    provider_total = Decimal(str(budget_snapshot["provider_totals"].get(provider_name, Decimal("0"))))
    provider_target = Decimal(str(budget_snapshot["provider_targets"].get(provider_name, Decimal("0"))))
    if provider_target > 0 and provider_total > provider_target:
        reasons.append(
            f"El proveedor {provider_name} supera su objetivo del periodo ({provider_total:.2f} > {provider_target:.2f})."
        )
        reason_codes.append("provider_budget_exceeded")

    category_total = Decimal(str(budget_snapshot["category_totals"].get(category_name, Decimal("0"))))
    category_target = Decimal(str(budget_snapshot["category_targets"].get(_normalize_text(category_name), Decimal("0"))))
    if category_target > 0 and category_total > category_target:
        reasons.append(
            f"La categoria {category_name} supera su objetivo del periodo ({category_total:.2f} > {category_target:.2f})."
        )
        reason_codes.append("category_budget_exceeded")

    if quotes_required > quotes_received:
        reasons.append(
            f"Cotizaciones incompletas ({quotes_received}/{quotes_required}) para la politica vigente."
        )
        reason_codes.append("quotes_below_required_minimum")

    return {
        "estimated_amount": estimated_amount,
        "provider_name": provider_name,
        "category_name": category_name,
        "quotes_required": quotes_required,
        "quotes_received": quotes_received,
        "reasons": reasons,
        "reason_codes": reason_codes,
    }


def _build_purchase_budget_snapshot(*, period_key: str) -> dict[str, object]:
    year, month = period_key.split("-")
    start = date(int(year), int(month), 1)
    end = date(int(year), int(month), calendar.monthrange(int(year), int(month))[1])

    monthly_target_obj = PresupuestoCompraPeriodo.objects.filter(
        periodo_tipo=PresupuestoCompraPeriodo.TIPO_MES,
        periodo_mes=period_key,
    ).first()
    monthly_target = monthly_target_obj.monto_objetivo if monthly_target_obj else Decimal("0")

    provider_targets = {
        objetivo.proveedor.nombre: objetivo.monto_objetivo
        for objetivo in PresupuestoCompraProveedor.objects.select_related("proveedor").filter(
            presupuesto_periodo__periodo_tipo=PresupuestoCompraPeriodo.TIPO_MES,
            presupuesto_periodo__periodo_mes=period_key,
        )
    }
    category_targets = {
        objetivo.categoria_normalizada: objetivo.monto_objetivo
        for objetivo in PresupuestoCompraCategoria.objects.filter(
            presupuesto_periodo__periodo_tipo=PresupuestoCompraPeriodo.TIPO_MES,
            presupuesto_periodo__periodo_mes=period_key,
        )
    }

    monthly_total = Decimal("0")
    provider_totals: dict[str, Decimal] = {}
    category_totals: dict[str, Decimal] = {}
    solicitudes = (
        SolicitudCompra.objects.select_related(
            "insumo",
            "proveedor_sugerido",
            "insumo__proveedor_principal",
        )
        .exclude(estatus=SolicitudCompra.STATUS_RECHAZADA)
        .filter(fecha_requerida__range=(start, end))
    )
    for solicitud in solicitudes:
        canonical = canonical_insumo_by_id(solicitud.insumo_id) or solicitud.insumo
        estimated = _estimate_solicitud_amount(solicitud=solicitud, canonical=canonical)
        monthly_total += estimated

        provider = solicitud.proveedor_sugerido or getattr(canonical, "proveedor_principal", None)
        provider_name = provider.nombre if provider else "Sin proveedor"
        provider_totals[provider_name] = provider_totals.get(provider_name, Decimal("0")) + estimated

        category_name = _resolve_category_name(canonical)
        category_totals[category_name] = category_totals.get(category_name, Decimal("0")) + estimated

    return {
        "monthly_target": monthly_target,
        "monthly_total": monthly_total,
        "provider_targets": provider_targets,
        "provider_totals": provider_totals,
        "category_targets": category_targets,
        "category_totals": category_totals,
    }


def _assess_inventory_adjustment_guard(
    *,
    ajuste: AjusteInventario,
    rule: OrchestrationRule,
    reference_dt: datetime,
) -> dict[str, object]:
    reasons: list[str] = []
    reason_codes: list[str] = []
    stale_pending_hours = int((rule.condition_json or {}).get("stale_pending_hours") or 4)

    if ajuste.estatus == AjusteInventario.STATUS_PENDIENTE:
        age = reference_dt - _coerce_local_datetime(ajuste.creado_en)
        if age >= timedelta(hours=stale_pending_hours):
            reasons.append(
                f"Ajuste pendiente por {round(age.total_seconds() / 3600, 2)} horas sin autorizacion cerrada."
            )
            reason_codes.append("pending_approval_stale")

    if ajuste.estatus == AjusteInventario.STATUS_APLICADO and not ajuste.aprobado_por_id:
        reasons.append("Ajuste aplicado sin usuario aprobador registrado.")
        reason_codes.append("applied_without_approval_actor")
    if ajuste.estatus == AjusteInventario.STATUS_APLICADO and ajuste.aprobado_en is None:
        reasons.append("Ajuste aplicado sin fecha/hora de aprobacion registrada.")
        reason_codes.append("applied_without_approval_timestamp")
    if ajuste.estatus == AjusteInventario.STATUS_RECHAZADO and not ajuste.aprobado_por_id:
        reasons.append("Ajuste rechazado sin usuario revisor registrado.")
        reason_codes.append("rejected_without_approval_actor")
    if ajuste.estatus == AjusteInventario.STATUS_RECHAZADO and ajuste.aprobado_en is None:
        reasons.append("Ajuste rechazado sin fecha/hora de revision registrada.")
        reason_codes.append("rejected_without_approval_timestamp")

    return {
        "reasons": reasons,
        "reason_codes": reason_codes,
        "is_integrity_issue": any(code.startswith("applied_") or code.startswith("rejected_") for code in reason_codes),
    }


def _inventory_rotation_snapshots() -> list[dict[str, object]]:
    grouped: dict[int, dict[str, object]] = {}
    existencias_qs = (
        ExistenciaInsumo.objects.filter(insumo__activo=True)
        .select_related("insumo")
        .only(
            "insumo_id",
            "insumo__id",
            "insumo__nombre",
            "insumo__tipo_item",
            "insumo__categoria",
            "stock_actual",
            "stock_minimo",
            "inventario_promedio",
            "consumo_diario_promedio",
            "actualizado_en",
        )
    )
    for existencia in existencias_qs:
        canonical = canonical_insumo_by_id(existencia.insumo_id) or existencia.insumo
        if not canonical:
            continue
        snapshot = grouped.setdefault(
            canonical.id,
            {
                "canonical_insumo_id": canonical.id,
                "canonical_insumo_nombre": canonical.nombre,
                "tipo_item": canonical.tipo_item,
                "categoria": (canonical.categoria or "").strip(),
                "member_ids": set(),
                "stock_actual": Decimal("0"),
                "stock_minimo": Decimal("0"),
                "inventario_promedio": Decimal("0"),
                "consumo_diario_promedio": Decimal("0"),
                "actualizado_en": None,
            },
        )
        snapshot["member_ids"].add(existencia.insumo_id)
        snapshot["stock_actual"] += Decimal(str(existencia.stock_actual or 0))
        snapshot["stock_minimo"] = max(snapshot["stock_minimo"], Decimal(str(existencia.stock_minimo or 0)))
        snapshot["inventario_promedio"] = max(
            snapshot["inventario_promedio"],
            Decimal(str(existencia.inventario_promedio or 0)),
        )
        snapshot["consumo_diario_promedio"] += Decimal(str(existencia.consumo_diario_promedio or 0))
        if snapshot["actualizado_en"] is None or (
            existencia.actualizado_en and existencia.actualizado_en > snapshot["actualizado_en"]
        ):
            snapshot["actualizado_en"] = existencia.actualizado_en

    snapshots: list[dict[str, object]] = []
    for snapshot in grouped.values():
        snapshot["member_ids"] = sorted(snapshot["member_ids"])
        snapshots.append(snapshot)
    snapshots.sort(key=lambda row: str(row["canonical_insumo_nombre"]).lower())
    return snapshots


def _assess_inventory_rotation(*, snapshot: dict[str, object], rule: OrchestrationRule) -> dict[str, object]:
    scope = (rule.condition_json or {}).get("scope") or {}
    thresholds = (rule.condition_json or {}).get("thresholds") or {}
    allowed_item_types = set(scope.get("allowed_item_types") or [])
    excluded_category_keywords = [
        _normalize_text(keyword)
        for keyword in (scope.get("excluded_category_keywords") or [])
        if str(keyword).strip()
    ]
    tipo_item = str(snapshot.get("tipo_item") or "").strip()
    categoria = str(snapshot.get("categoria") or "").strip()
    categoria_norm = _normalize_text(categoria)

    if allowed_item_types and tipo_item not in allowed_item_types:
        return {
            "reasons": [],
            "reason_codes": [],
            "rotation_index": Decimal("0"),
            "days_of_cover": None,
        }
    if excluded_category_keywords and any(keyword in categoria_norm for keyword in excluded_category_keywords):
        return {
            "reasons": [],
            "reason_codes": [],
            "rotation_index": Decimal("0"),
            "days_of_cover": None,
        }

    stock_actual = Decimal(str(snapshot["stock_actual"] or 0))
    stock_minimo = Decimal(str(snapshot["stock_minimo"] or 0))
    inventario_promedio = Decimal(str(snapshot["inventario_promedio"] or 0))
    consumo_diario = Decimal(str(snapshot["consumo_diario_promedio"] or 0))

    min_stock_actual = Decimal(str(thresholds.get("min_stock_actual") or "1"))
    days_of_cover_gte = Decimal(str(thresholds.get("days_of_cover_gte") or "14"))
    stock_vs_min_factor_gte = Decimal(str(thresholds.get("stock_vs_min_factor_gte") or "1.25"))
    stock_vs_avg_factor_gte = Decimal(str(thresholds.get("stock_vs_avg_factor_gte") or "1.10"))
    zero_consumption_requires_review = bool(thresholds.get("zero_consumption_requires_review", True))
    zero_consumption_stock_vs_min_factor_gte = Decimal(str(thresholds.get("zero_consumption_stock_vs_min_factor_gte") or "3.00"))
    zero_consumption_stock_vs_avg_factor_gte = Decimal(str(thresholds.get("zero_consumption_stock_vs_avg_factor_gte") or "3.00"))
    zero_consumption_min_stock_absolute = Decimal(str(thresholds.get("zero_consumption_min_stock_absolute") or "10"))

    if stock_actual < min_stock_actual:
        return {
            "reasons": [],
            "reason_codes": [],
            "rotation_index": Decimal("0"),
            "days_of_cover": None,
        }

    reasons: list[str] = []
    reason_codes: list[str] = []
    rotation_index = Decimal("0")
    days_of_cover: float | None = None

    has_reference_metrics = stock_minimo > 0 or inventario_promedio > 0
    if not has_reference_metrics:
        return {
            "reasons": [],
            "reason_codes": [],
            "rotation_index": Decimal("0"),
            "days_of_cover": None,
        }

    exceeds_min_factor = stock_minimo > 0 and stock_actual >= (stock_minimo * stock_vs_min_factor_gte)
    exceeds_avg_factor = inventario_promedio > 0 and stock_actual >= (inventario_promedio * stock_vs_avg_factor_gte)
    exceeds_reference = exceeds_min_factor or exceeds_avg_factor

    if consumo_diario <= 0:
        exceeds_zero_min = stock_minimo > 0 and stock_actual >= (stock_minimo * zero_consumption_stock_vs_min_factor_gte)
        exceeds_zero_avg = inventario_promedio > 0 and stock_actual >= (inventario_promedio * zero_consumption_stock_vs_avg_factor_gte)
        exceeds_zero_reference = exceeds_zero_min or exceeds_zero_avg
        if zero_consumption_requires_review and stock_actual >= zero_consumption_min_stock_absolute and exceeds_zero_reference:
            reasons.append("Existe stock disponible sin consumo diario promedio registrado o con consumo en cero.")
            reason_codes.append("zero_consumption_with_stock")
    else:
        rotation_index = (consumo_diario / stock_actual).quantize(Decimal("0.0001"))
        days_of_cover = round(float(stock_actual / consumo_diario), 2)
        if Decimal(str(days_of_cover)) >= days_of_cover_gte and exceeds_reference:
            reasons.append(
                f"La cobertura estimada es de {days_of_cover} dias, por encima del umbral configurado ({days_of_cover_gte})."
            )
            reason_codes.append("days_of_cover_above_threshold")

    return {
        "reasons": reasons,
        "reason_codes": reason_codes,
        "rotation_index": rotation_index,
        "days_of_cover": days_of_cover,
    }


def _estimate_solicitud_amount(*, solicitud: SolicitudCompra, canonical) -> Decimal:
    latest_cost = latest_costo_canonico(canonical)
    if latest_cost is None:
        return Decimal("0")
    return (Decimal(str(solicitud.cantidad or 0)) * Decimal(str(latest_cost or 0))).quantize(Decimal("0.01"))


def _resolve_category_name(canonical) -> str:
    name = " ".join(str(getattr(canonical, "categoria", "") or "").strip().split())
    return name or "Sin categoria"


def _normalize_text(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


def _severity_to_priority(severity: str) -> str:
    mapping = {
        AgentSuggestion.SEVERITY_CRITICAL: AgentTask.PRIORITY_CRITICAL,
        AgentSuggestion.SEVERITY_HIGH: AgentTask.PRIORITY_HIGH,
        AgentSuggestion.SEVERITY_WARNING: AgentTask.PRIORITY_MEDIUM,
        AgentSuggestion.SEVERITY_INFO: AgentTask.PRIORITY_LOW,
    }
    return mapping.get(severity, AgentTask.PRIORITY_MEDIUM)


def _build_chain_demand_summary(*, plan: PlanProduccion, rule: OrchestrationRule) -> dict[str, object]:
    recipe_ids = list(plan.items.values_list("receta_id", flat=True).distinct())
    lookback_days = int(((rule.condition_json or {}).get("lookback_days") or 60))
    start_date = plan.fecha_produccion - timedelta(days=max(lookback_days, 1))
    historico_qs = VentaHistorica.objects.filter(
        receta_id__in=recipe_ids,
        fecha__gte=start_date,
        fecha__lt=plan.fecha_produccion,
    )
    historico_days = historico_qs.values("fecha").distinct().count()
    years_observed = historico_qs.dates("fecha", "year").count()
    comparable_years = historico_qs.filter(fecha__month=plan.fecha_produccion.month).dates("fecha", "year").count()
    historico_total = historico_qs.aggregate(total=Sum("cantidad")).get("total") or Decimal("0")
    top_historial = list(
        historico_qs.values("receta__nombre").annotate(total=Sum("cantidad")).order_by("-total", "receta__nombre")[:5]
    )
    periodo = plan.fecha_produccion.strftime("%Y-%m")
    pronosticos = list(
        PronosticoVenta.objects.filter(receta_id__in=recipe_ids, periodo=periodo)
        .select_related("receta")
        .order_by("-cantidad", "receta__nombre")
    )
    solicitudes = list(
        SolicitudVenta.objects.filter(
            receta_id__in=recipe_ids,
            fecha_inicio__lte=plan.fecha_produccion,
            fecha_fin__gte=plan.fecha_produccion,
        )
        .select_related("receta")
        .order_by("-cantidad", "receta__nombre")
    )
    warning_days = int((((rule.condition_json or {}).get("thresholds") or {}).get("demand_min_historico_days_warning") or 7))
    success_days = int((((rule.condition_json or {}).get("thresholds") or {}).get("demand_min_historico_days_success") or 28))

    if historico_days >= success_days and (pronosticos or comparable_years >= 2):
        severity = AgentSuggestion.SEVERITY_INFO
        tone = "success"
        summary = f"Demanda del plan {plan.id} con base comercial estable"
        recommended_action = "Usar la señal histórica y el forecast persistido como referencia para sostener producción y compras."
    elif historico_days >= warning_days or pronosticos or solicitudes:
        severity = AgentSuggestion.SEVERITY_WARNING
        tone = "warning"
        summary = f"Demanda del plan {plan.id} utilizable pero todavía en revisión"
        recommended_action = "Validar la señal comercial antes de cerrar abastecimiento, especialmente si el plan cubre una fecha fuerte."
    else:
        severity = AgentSuggestion.SEVERITY_HIGH
        tone = "danger"
        summary = f"Demanda del plan {plan.id} con base comercial frágil"
        recommended_action = "Reforzar histórico, solicitud comercial o forecast persistido antes de comprometer producción y compras."

    return {
        "tone": tone,
        "severity": severity,
        "priority": _severity_to_priority(severity),
        "summary": summary,
        "task_title": f"Validar señal comercial del plan {plan.id}",
        "recommended_action": recommended_action,
        "resolution_note": "Revisar si la base histórica, la solicitud comercial y el forecast persistido sostienen el plan actual.",
        "input_payload": {
            "plan_id": plan.id,
            "production_date": plan.fecha_produccion.isoformat(),
            "recipe_ids": recipe_ids,
        },
        "output_payload": {
            "historico_days": historico_days,
            "years_observed": years_observed,
            "comparable_years": comparable_years,
            "forecast_count": len(pronosticos),
            "sales_request_count": len(solicitudes),
        },
        "details": {
            "plan_id": plan.id,
            "production_date": plan.fecha_produccion.isoformat(),
            "historico_days": historico_days,
            "years_observed": years_observed,
            "comparable_years": comparable_years,
            "historico_total": str(Decimal(str(historico_total)).quantize(Decimal("0.1"))),
            "forecast_count": len(pronosticos),
            "sales_request_count": len(solicitudes),
            "top_historial": [
                {"receta": row["receta__nombre"], "total": str(Decimal(str(row["total"] or 0)).quantize(Decimal("0.1")))}
                for row in top_historial
            ],
            "top_forecast": [
                {"receta": item.receta.nombre, "cantidad": str(Decimal(str(item.cantidad or 0)).quantize(Decimal("0.1")))}
                for item in pronosticos[:5]
            ],
            "top_sales_requests": [
                {"receta": item.receta.nombre, "cantidad": str(Decimal(str(item.cantidad or 0)).quantize(Decimal("0.1")))}
                for item in solicitudes[:5]
            ],
            "known_gaps": ["branch_delivery_windows_not_modeled", "session_forecast_preview_not_available_in_background"],
        },
        "driver_codes": ["demand_signal_fragile"] if severity != AgentSuggestion.SEVERITY_INFO else [],
    }


def _build_chain_production_summary(*, plan: PlanProduccion) -> dict[str, object]:
    recipe_ids = list(plan.items.values_list("receta_id", flat=True).distinct())
    lineas_qs = (
        LineaReceta.objects.filter(receta_id__in=recipe_ids)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .select_related("insumo", "receta")
    )
    qty_by_recipe = {item.receta_id: Decimal(str(item.cantidad or 0)) for item in plan.items.all()}
    incomplete_recipes: set[int] = set()
    master_blockers: dict[int, dict[str, object]] = {}
    total_required_lines = 0
    for linea in lineas_qs:
        total_required_lines += 1
        if not linea.insumo_id or linea.cantidad is None:
            incomplete_recipes.add(linea.receta_id)
            continue
        canonical = canonical_insumo_by_id(linea.insumo_id) or linea.insumo
        readiness = enterprise_readiness_profile(canonical)
        if readiness["readiness_label"] != "Lista para operar":
            blocker = master_blockers.setdefault(
                canonical.id,
                {
                    "insumo_id": canonical.id,
                    "insumo_nombre": getattr(canonical, "display_name", canonical.nombre),
                    "missing": readiness["missing"][:3],
                    "required_qty": Decimal("0"),
                },
            )
            blocker["required_qty"] += qty_by_recipe.get(linea.receta_id, Decimal("0")) * Decimal(str(linea.cantidad or 0))

    if incomplete_recipes:
        severity = AgentSuggestion.SEVERITY_CRITICAL
        tone = "danger"
        summary = f"Producción del plan {plan.id} bloqueada por líneas incompletas de receta"
        recommended_action = "Cerrar primero recetas incompletas antes de liberar compras o ejecución productiva."
    elif master_blockers:
        severity = AgentSuggestion.SEVERITY_HIGH
        tone = "warning"
        summary = f"Producción del plan {plan.id} con bloqueos maestros aguas arriba"
        recommended_action = "Regularizar maestro e insumos críticos antes de ejecutar o ampliar el plan."
    else:
        severity = AgentSuggestion.SEVERITY_INFO
        tone = "success"
        summary = f"Producción del plan {plan.id} lista para seguimiento operativo"
        recommended_action = "Mantener seguimiento de capacidad y cierre de consumo del plan."

    top_blockers = sorted(
        master_blockers.values(),
        key=lambda item: Decimal(str(item["required_qty"] or 0)),
        reverse=True,
    )[:5]

    return {
        "tone": tone,
        "severity": severity,
        "priority": _severity_to_priority(severity),
        "summary": summary,
        "task_title": f"Validar cadena de producción del plan {plan.id}",
        "recommended_action": recommended_action,
        "resolution_note": "Validar estructura de receta, maestro e insumos críticos antes de seguir aguas abajo.",
        "input_payload": {"plan_id": plan.id, "recipe_ids": recipe_ids},
        "output_payload": {
            "recipes_in_plan": len(recipe_ids),
            "incomplete_recipe_count": len(incomplete_recipes),
            "master_blocker_count": len(master_blockers),
        },
        "details": {
            "plan_id": plan.id,
            "production_date": plan.fecha_produccion.isoformat(),
            "plan_status": plan.estado,
            "recipes_in_plan": len(recipe_ids),
            "line_items_reviewed": total_required_lines,
            "incomplete_recipe_ids": sorted(incomplete_recipes),
            "master_blockers": [
                {
                    "insumo_id": row["insumo_id"],
                    "insumo_nombre": row["insumo_nombre"],
                    "missing": row["missing"],
                    "required_qty": str(Decimal(str(row["required_qty"] or 0)).quantize(Decimal("0.001"))),
                }
                for row in top_blockers
            ],
        },
        "driver_codes": (
            ["recipe_lines_incomplete"] if incomplete_recipes else ["master_upstream_blockers"] if master_blockers else []
        ),
    }


def _build_chain_purchase_summary(*, plan: PlanProduccion) -> dict[str, object]:
    required_by_insumo: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))
    canonical_info: dict[int, dict[str, object]] = {}
    recipe_ids = list(plan.items.values_list("receta_id", flat=True).distinct())
    qty_by_recipe = {item.receta_id: Decimal(str(item.cantidad or 0)) for item in plan.items.all()}
    for linea in (
        LineaReceta.objects.filter(receta_id__in=recipe_ids, insumo_id__isnull=False)
        .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
        .select_related("insumo")
    ):
        canonical = canonical_insumo_by_id(linea.insumo_id) or linea.insumo
        if not canonical:
            continue
        member_ids = list(getattr(canonical, "member_ids", None) or [canonical.id])
        canonical_info[canonical.id] = {"canonical": canonical, "member_ids": member_ids}
        required_by_insumo[canonical.id] += qty_by_recipe.get(linea.receta_id, Decimal("0")) * Decimal(str(linea.cantidad or 0))

    canonical_member_ids = sorted(
        {
            int(member_id)
            for info in canonical_info.values()
            for member_id in info.get("member_ids") or []
        }
    )
    existencias = {
        row["insumo_id"]: Decimal(str(row["stock_actual"] or 0))
        for row in ExistenciaInsumo.objects.filter(insumo_id__in=canonical_member_ids).values("insumo_id", "stock_actual")
    }
    shortage_rows = []
    shortage_ratio_warning = Decimal("0.05")
    for canonical_id, required_qty in required_by_insumo.items():
        canonical = canonical_info.get(canonical_id, {}).get("canonical")
        member_ids = canonical_info.get(canonical_id, {}).get("member_ids") or [canonical_id]
        if not canonical:
            continue
        stock_actual = sum((existencias.get(member_id, Decimal("0")) for member_id in member_ids), Decimal("0"))
        shortage = max(required_qty - stock_actual, Decimal("0"))
        if shortage <= 0:
            continue
        provider = getattr(canonical, "proveedor_principal", None)
        latest_cost = latest_costo_canonico(canonical)
        shortage_rows.append(
            {
                "insumo_id": canonical.id,
                "insumo_nombre": getattr(canonical, "display_name", canonical.nombre),
                "required_qty": required_qty,
                "stock_actual": stock_actual,
                "shortage_qty": shortage,
                "shortage_ratio": (shortage / required_qty).quantize(Decimal("0.0001")) if required_qty > 0 else Decimal("1"),
                "provider_name": provider.nombre if provider else "Sin proveedor",
                "estimated_amount": (
                    (shortage * Decimal(str(latest_cost or 0))).quantize(Decimal("0.01"))
                    if latest_cost is not None
                    else Decimal("0")
                ),
                "has_provider": bool(provider),
            }
        )

    shortage_rows.sort(
        key=lambda item: (
            not item["has_provider"],
            Decimal(str(item["shortage_qty"] or 0)),
            Decimal(str(item["estimated_amount"] or 0)),
        ),
        reverse=True,
    )
    total_shortage_amount = sum((Decimal(str(item["estimated_amount"] or 0)) for item in shortage_rows), Decimal("0"))
    critical_shortages = [
        row
        for row in shortage_rows
        if (not row["has_provider"]) or Decimal(str(row["shortage_ratio"] or 0)) >= shortage_ratio_warning
    ]

    if critical_shortages:
        severity = AgentSuggestion.SEVERITY_CRITICAL
        tone = "danger"
        summary = f"Compras del plan {plan.id} con faltantes críticos de insumo"
        recommended_action = "Priorizar abastecimiento inmediato y escalar a Dirección si faltan proveedor o cobertura mínima."
    elif shortage_rows:
        severity = AgentSuggestion.SEVERITY_HIGH
        tone = "warning"
        summary = f"Compras del plan {plan.id} requieren reabasto preventivo"
        recommended_action = "Preparar solicitudes y validar cobertura de insumos antes de liberar producción."
    else:
        severity = AgentSuggestion.SEVERITY_INFO
        tone = "success"
        summary = f"Compras del plan {plan.id} sin faltantes relevantes"
        recommended_action = "Mantener monitoreo preventivo y validar lead times antes de cierres del día."

    return {
        "tone": tone,
        "severity": severity,
        "priority": _severity_to_priority(severity),
        "summary": summary,
        "task_title": f"Validar abastecimiento del plan {plan.id}",
        "recommended_action": recommended_action,
        "resolution_note": "Revisar faltantes, proveedor principal y monto estimado antes de convertirlo a flujo documental.",
        "input_payload": {"plan_id": plan.id, "production_date": plan.fecha_produccion.isoformat()},
        "output_payload": {
            "shortage_count": len(shortage_rows),
            "critical_shortage_count": len(critical_shortages),
            "estimated_shortage_amount": str(total_shortage_amount.quantize(Decimal("0.01"))),
        },
        "details": {
            "plan_id": plan.id,
            "production_date": plan.fecha_produccion.isoformat(),
            "shortage_count": len(shortage_rows),
            "critical_shortage_count": len(critical_shortages),
            "estimated_shortage_amount": str(total_shortage_amount.quantize(Decimal("0.01"))),
            "shortage_rows": [
                {
                    "insumo_id": row["insumo_id"],
                    "insumo_nombre": row["insumo_nombre"],
                    "required_qty": str(Decimal(str(row["required_qty"])).quantize(Decimal("0.001"))),
                    "stock_actual": str(Decimal(str(row["stock_actual"])).quantize(Decimal("0.001"))),
                    "shortage_qty": str(Decimal(str(row["shortage_qty"])).quantize(Decimal("0.001"))),
                    "shortage_ratio": str(Decimal(str(row["shortage_ratio"])).quantize(Decimal("0.0001"))),
                    "provider_name": row["provider_name"],
                    "estimated_amount": str(Decimal(str(row["estimated_amount"])).quantize(Decimal("0.01"))),
                }
                for row in shortage_rows[:8]
            ],
            "known_gaps": ["branch_delivery_windows_not_modeled"],
        },
        "driver_codes": ["purchase_shortage_detected"] if shortage_rows else [],
    }


def _build_chain_director_summary(
    *,
    plan: PlanProduccion,
    demand_summary: dict[str, object],
    production_summary: dict[str, object],
    purchase_summary: dict[str, object],
) -> dict[str, object]:
    severity_order = {
        AgentSuggestion.SEVERITY_INFO: 0,
        AgentSuggestion.SEVERITY_WARNING: 1,
        AgentSuggestion.SEVERITY_HIGH: 2,
        AgentSuggestion.SEVERITY_CRITICAL: 3,
    }
    step_summaries = {
        "demanda": demand_summary,
        "produccion": production_summary,
        "compras": purchase_summary,
    }
    driver_codes = [
        code
        for summary in step_summaries.values()
        for code in list(summary.get("driver_codes") or [])
    ]
    max_severity = max((summary["severity"] for summary in step_summaries.values()), key=lambda item: severity_order[item])
    should_create_chain = any(
        summary["severity"] != AgentSuggestion.SEVERITY_INFO for summary in step_summaries.values()
    )
    tone = "success" if max_severity == AgentSuggestion.SEVERITY_INFO else "warning" if max_severity == AgentSuggestion.SEVERITY_WARNING else "danger"

    return {
        "tone": tone,
        "severity": max_severity,
        "priority": _severity_to_priority(max_severity),
        "summary": (
            f"Dirección Operativa debe validar la cadena del plan {plan.id}"
            if should_create_chain
            else f"Cadena del plan {plan.id} sin alertas"
        ),
        "task_title": f"Consolidar cadena operativa del plan {plan.id}",
        "recommended_action": (
            "Revisar la cadena completa demanda -> producción -> compras y definir prioridad ejecutiva."
            if should_create_chain
            else "Sin acciones inmediatas; mantener seguimiento del plan."
        ),
        "resolution_note": "Consolidar riesgos, aprobar escalamiento y definir siguiente responsable operativo.",
        "input_payload": {"plan_id": plan.id, "production_date": plan.fecha_produccion.isoformat()},
        "output_payload": {"driver_codes": driver_codes, "step_tones": {key: value["tone"] for key, value in step_summaries.items()}},
        "details": {
            "plan_id": plan.id,
            "plan_name": plan.nombre,
            "production_date": plan.fecha_produccion.isoformat(),
            "driver_codes": driver_codes,
            "step_summaries": {
                key: {
                    "summary": value["summary"],
                    "severity": value["severity"],
                    "tone": value["tone"],
                    "recommended_action": value["recommended_action"],
                }
                for key, value in step_summaries.items()
            },
            "known_gaps": [
                "branch_delivery_windows_not_modeled",
                "session_forecast_preview_not_available_in_background",
            ],
        },
        "driver_codes": driver_codes,
        "should_create_chain": should_create_chain,
    }


def _log_run_creation(*, run: OrchestrationRun, rule: OrchestrationRule, created_by) -> AuditLog:
    return AuditLog.objects.create(
        user=created_by,
        action="CREATE",
        model="orquestacion.OrchestrationRun",
        object_id=str(run.id),
        payload={
            "rule_code": rule.code,
            "run_key": run.run_key,
            "context": run.context_json,
        },
    )


def _log_suggestion_creation(*, suggestion: AgentSuggestion, rule: OrchestrationRule, created_by) -> AuditLog:
    return AuditLog.objects.create(
        user=created_by,
        action="CREATE",
        model="orquestacion.AgentSuggestion",
        object_id=str(suggestion.id),
        payload={
            "rule_code": rule.code,
            "task_id": suggestion.task_id,
            "summary": suggestion.summary,
            "details": suggestion.details_json,
        },
    )
