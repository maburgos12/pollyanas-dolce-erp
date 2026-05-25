from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Callable
from uuid import uuid4

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.db import models, transaction
from django.db.models import Count, Max, Min, Q, Sum
from django.utils import timezone

from control.services import build_discrepancias_report, resolve_period_range
from compras.models import OrdenCompra, SolicitudCompra
from api.serializers import ComprasSolicitudCreateSerializer
from api.serializers import PlanProduccionCreateSerializer
from core.access import (
    ROLE_ADMIN,
    ROLE_COMPRAS,
    ROLE_DG,
    ROLE_LECTURA,
    ROLE_PRODUCCION,
    ROLE_VENTAS,
    can_view_audit,
    can_manage_compras,
    can_view_compras,
    can_view_inventario,
    can_view_recetas,
    can_view_reportes,
    has_any_role,
    primary_role,
)
from core.audit import log_event
from core.models import AuditLog
from orquestacion.models import AgentDefinition, AgentExecutionLink, AgentSuggestion, AgentTask, OrchestrationRun
from pos_bridge.models import PointBranch, PointDailyBranchIndicator, PointInventorySnapshot, PointProduct, PointSyncJob
from pos_bridge.services.point_ticket_threshold_service import PointTicketThresholdService
from pos_bridge.tasks.run_daily_sales_sync import run_daily_sales_sync
from pos_bridge.tasks.run_inventory_sync import run_inventory_sync
from pos_bridge.tasks.run_product_recipe_sync import run_product_recipe_sync
from pos_bridge.utils.helpers import safe_slug
from maestros.models import CostoInsumo, Insumo, InsumoAlias
from maestros.utils.canonical_catalog import canonical_insumo, canonical_insumo_by_id, canonical_member_ids
from recetas.models import PlanProduccion, PlanProduccionItem, Receta, RecetaCostoVersion
from recetas.utils.normalizacion import normalizar_nombre
from reportes.bi_utils import compute_bi_snapshot, serialize_bi_for_api
from ventas.services.sales_read_service import get_promotion_sales_totals, get_sales_range, get_sales_range_grouped

ZERO = Decimal("0")


def _safe_integration_name(value: Any, *, fallback: str) -> str:
    token = safe_slug(str(value or "").replace(".", "_"))
    return token or fallback


@dataclass(frozen=True)
class AIToolDefinition:
    key: str
    name: str
    description: str
    operation_type: str
    data_domain: str
    branch_scoped: bool
    requires_approval: bool
    access_check: Callable[[Any], bool]
    handler: Callable[[Any, dict[str, Any]], dict[str, Any]]
    execute_handler: Callable[[Any, dict[str, Any]], dict[str, Any]] | None = None
    argument_schema: dict[str, Any] = field(default_factory=dict)
    result_contract: dict[str, Any] = field(default_factory=dict)


def _resolve_sales_period(arguments: dict[str, Any]) -> tuple[date, date]:
    end_date = _parse_iso_date(arguments.get("end_date")) or timezone.localdate()
    start_date = _parse_iso_date(arguments.get("start_date")) or date(2022, 1, 1)
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    return start_date, end_date


def _parse_int(value: Any, *, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(min_value, min(parsed, max_value))


def _parse_decimal(value: Any, *, default: Decimal) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _parse_iso_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _build_scope(user, *, branch_scoped: bool, arguments: dict[str, Any]) -> dict[str, Any]:
    profile = getattr(user, "userprofile", None)
    branch_locked = bool(profile and getattr(profile, "modo_captura_sucursal", False) and getattr(profile, "sucursal_id", None))
    branch_code = profile.sucursal.codigo if branch_locked else ""
    branch_id = profile.sucursal_id if branch_locked else None

    scope = {
        "primary_role": primary_role(user),
        "branch_capture_only": branch_locked,
        "branch_code": branch_code,
        "branch_id": branch_id,
        "requested_branch": str(arguments.get("branch") or arguments.get("branch_code") or "").strip(),
        "requested_sucursal_id": arguments.get("sucursal_id"),
        "branch_scoped_tool": branch_scoped,
    }
    return scope


def _enforce_branch_scope(user, *, branch_scoped: bool, arguments: dict[str, Any]) -> dict[str, Any]:
    scope = _build_scope(user, branch_scoped=branch_scoped, arguments=arguments)
    if not scope["branch_capture_only"] or not branch_scoped:
        return scope

    requested_branch = str(arguments.get("branch") or arguments.get("branch_code") or "").strip()
    requested_sucursal_id = arguments.get("sucursal_id")
    forced_branch = scope["branch_code"]
    forced_sucursal_id = scope["branch_id"]

    if requested_branch and requested_branch.upper() != forced_branch.upper():
        raise PermissionDenied("La consulta excede el alcance de sucursal asignado al usuario.")
    if requested_sucursal_id not in {None, "", forced_sucursal_id}:
        raise PermissionDenied("La consulta excede el alcance de sucursal asignado al usuario.")

    if forced_branch and not requested_branch:
        arguments["branch"] = forced_branch
    if forced_sucursal_id and requested_sucursal_id in {None, ""}:
        arguments["sucursal_id"] = forced_sucursal_id
    return scope


def _tool_response(*, tool: AIToolDefinition, scope: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    return {
        "tool": {
            "key": tool.key,
            "name": _safe_integration_name(tool.key, fallback="erp_tool"),
            "display_name": tool.name,
            "operation_type": tool.operation_type,
            "data_domain": tool.data_domain,
            "branch_scoped": tool.branch_scoped,
            "requires_approval": tool.requires_approval,
        },
        "scope": scope,
        "result": result,
    }


def _serialize_tool_definition(tool: AIToolDefinition) -> dict[str, Any]:
    return {
        "key": tool.key,
        "name": _safe_integration_name(tool.key, fallback="erp_tool"),
        "display_name": tool.name,
        "description": tool.description,
        "operation_type": tool.operation_type,
        "data_domain": tool.data_domain,
        "branch_scoped": tool.branch_scoped,
        "requires_approval": tool.requires_approval,
        "argument_schema": tool.argument_schema,
        "result_contract": tool.result_contract,
    }


def _ensure_ai_gateway_agent() -> AgentDefinition:
    agent, _created = AgentDefinition.objects.get_or_create(
        code="ai_gateway",
        defaults={
            "name": "AI Gateway",
            "domain": "integrations",
            "status": AgentDefinition.STATUS_ACTIVE,
            "description": "Agente tecnico para solicitudes y ejecuciones controladas del ERP AI Gateway.",
            "allowed_tools_json": sorted(list(TOOLS.keys())) if "TOOLS" in globals() else [],
            "allowed_actions_json": ["request_approval", "execute_safe_action"],
            "requires_human_approval_default": True,
            "priority_order": 5,
        },
    )
    return agent


def _handle_dashboard(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    period_days = _parse_int(arguments.get("period_days"), default=90, min_value=7, max_value=365)
    months_window = _parse_int(arguments.get("months"), default=6, min_value=3, max_value=24)
    snapshot = compute_bi_snapshot(period_days=period_days, months_window=months_window)
    return {
        "status": "ok",
        "sources": ["reportes.bi_utils.compute_bi_snapshot"],
        "filters": {"period_days": period_days, "months": months_window},
        "payload": serialize_bi_for_api(snapshot),
    }


def _handle_audit_logs(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    q = str(arguments.get("q") or "").strip()
    action = str(arguments.get("action") or "").strip().upper()
    model_name = str(arguments.get("model") or "").strip()
    user_id_raw = arguments.get("user_id")
    limit = _parse_int(arguments.get("limit"), default=50, min_value=1, max_value=200)
    qs = AuditLog.objects.select_related("user").order_by("-timestamp", "-id")
    if q:
        qs = qs.filter(
            Q(action__icontains=q)
            | Q(model__icontains=q)
            | Q(object_id__icontains=q)
            | Q(payload__icontains=q)
            | Q(user__username__icontains=q)
        )
    if action:
        qs = qs.filter(action=action)
    if model_name:
        qs = qs.filter(model__icontains=model_name)
    if user_id_raw not in {None, ""}:
        try:
            qs = qs.filter(user_id=int(user_id_raw))
        except (TypeError, ValueError):
            raise PermissionDenied("user_id inválido para consulta de auditoría.")
    rows = []
    for log in qs[:limit]:
        rows.append(
            {
                "id": log.id,
                "timestamp": log.timestamp.isoformat(),
                "action": log.action,
                "model": log.model,
                "object_id": log.object_id,
                "user": log.user.username if log.user_id else "",
                "payload": log.payload or {},
            }
        )
    return {
        "status": "ok",
        "sources": ["core.AuditLog"],
        "filters": {"q": q, "action": action, "model": model_name, "user_id": user_id_raw, "limit": limit},
        "payload": {"items": rows, "returned": len(rows)},
    }


def _resolve_sales_branch_scope(arguments: dict[str, Any]) -> list[int] | None:
    branch = str(arguments.get("branch") or "").strip()
    if not branch:
        return None
    branch_ids = list(
        PointBranch.objects.filter(
            Q(name__icontains=branch)
            | Q(external_id__iexact=branch)
            | Q(erp_branch__codigo__iexact=branch)
            | Q(erp_branch__nombre__icontains=branch)
        )
        .exclude(erp_branch_id__isnull=True)
        .values_list("erp_branch_id", flat=True)
        .distinct()
    )
    return branch_ids or None


def _sales_source_label(selection: dict[str, Any]) -> tuple[str, str]:
    source = str(selection.get("source") or "none")
    detail = str(selection.get("source_detail") or "")
    if source == "authoritative":
        return "VentaAutoritativaPoint", "OFFICIAL"
    if source == "v2_fact":
        return "PointSalesDailyFact", "OFFICIAL"
    if source == "legacy" and detail == "point_daily_sale_official":
        return "PointDailySaleOfficial", "STAGING"
    if source == "legacy":
        return "PointDailySale", "STAGING"
    return "SinFuente", "EMPTY"


def _handle_sales_summary(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    start_date, end_date = _resolve_sales_period(arguments)
    branch_ids = _resolve_sales_branch_scope(arguments)
    selection = get_sales_range(
        start_date=start_date,
        end_date=end_date,
        sucursales=branch_ids,
        coverage_policy="prefer_complete",
    )
    grouped_branches = get_sales_range_grouped(
        start_date=start_date,
        end_date=end_date,
        dimension="branch",
        sucursales=branch_ids,
        coverage_policy="prefer_complete",
    )
    grouped_products = get_sales_range_grouped(
        start_date=start_date,
        end_date=end_date,
        dimension="product",
        sucursales=branch_ids,
        coverage_policy="prefer_complete",
    )
    source_label, source_status = _sales_source_label(selection)
    return {
        "status": "ok",
        "sources": [f"ventas.services.sales_read_service:{source_label}"],
        "filters": {
            "start_date": arguments.get("start_date"),
            "end_date": arguments.get("end_date"),
            "branch": arguments.get("branch"),
        },
        "payload": {
            "source": source_label,
            "source_status": source_status,
            "total_sales": float(selection["monto"]),
            "total_quantity": float(selection["cantidad"]),
            "total_tickets": sum(int(row.get("total_tickets") or 0) for row in grouped_branches["rows"]),
            "branches_count": len(grouped_branches["rows"]),
            "products_count": len(grouped_products["rows"]),
            "days_count": int(selection.get("coverage_days") or 0),
            "limitations": {
                "ticket_amount_distribution_available": False,
                "use_for_ticket_threshold_questions": False,
            },
        },
    }


def _handle_sales_by_branch(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    start_date, end_date = _resolve_sales_period(arguments)
    branch_ids = _resolve_sales_branch_scope(arguments)
    grouped = get_sales_range_grouped(
        start_date=start_date,
        end_date=end_date,
        dimension="branch",
        sucursales=branch_ids,
        coverage_policy="prefer_complete",
    )
    source_label, source_status = _sales_source_label(grouped)
    payload = []
    grand_total = sum((row["total_sales"] for row in grouped["rows"]), ZERO)
    for row in grouped["rows"]:
        pct = (row["total_sales"] / grand_total * 100) if grand_total else ZERO
        payload.append(
            {
                "branch_external_id": row["branch_code"],
                "branch_name": row["branch_name"],
                "total_sales": float(row["total_sales"]),
                "total_quantity": float(row["total_quantity"]),
                "total_tickets": int(row.get("total_tickets") or 0),
                "percentage": float(round(pct, 2)),
            }
        )
    return {
        "status": "ok",
        "sources": [f"ventas.services.sales_read_service:{source_label}"],
        "filters": {
            "start_date": arguments.get("start_date"),
            "end_date": arguments.get("end_date"),
            "branch": arguments.get("branch"),
        },
        "payload": {
            "source": source_label,
            "source_status": source_status,
            "items": payload,
            "returned": len(payload),
        },
    }


def _handle_sales_trends(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    start_date, end_date = _resolve_sales_period(arguments)
    branch_ids = _resolve_sales_branch_scope(arguments)
    grouped = get_sales_range_grouped(
        start_date=start_date,
        end_date=end_date,
        dimension="month",
        sucursales=branch_ids,
        coverage_policy="prefer_complete",
    )
    source_label, source_status = _sales_source_label(grouped)
    payload = []
    for row in grouped["rows"]:
        tickets = int(row.get("total_tickets") or 0) or 1
        payload.append(
            {
                "period": row["period"],
                "total_sales": float(row["total_sales"]),
                "total_quantity": float(row["total_quantity"]),
                "total_tickets": int(row.get("total_tickets") or 0),
                "avg_ticket": float(round(row["total_sales"] / tickets, 2)),
            }
        )
    return {
        "status": "ok",
        "sources": [f"ventas.services.sales_read_service:{source_label}"],
        "filters": {
            "start_date": arguments.get("start_date"),
            "end_date": arguments.get("end_date"),
            "branch": arguments.get("branch"),
        },
        "payload": {
            "source": source_label,
            "source_status": source_status,
            "items": payload,
            "returned": len(payload),
        },
    }


def _handle_ticket_amount_threshold(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    start_date, end_date = _resolve_sales_period(arguments)
    branch_ids = _resolve_sales_branch_scope(arguments)
    threshold = _parse_decimal(arguments.get("threshold_amount"), default=Decimal("500"))
    if threshold <= 0:
        threshold = Decimal("500")
    try:
        point_result = PointTicketThresholdService().fetch_threshold_count(
            start_date=start_date,
            end_date=end_date,
            threshold_amount=threshold,
            branch_ids=branch_ids,
        )
        return {
            "status": "ok",
            "sources": [f"Point:{point_result.source_endpoint}"],
            "filters": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "branch": arguments.get("branch"),
                "threshold_amount": float(threshold),
            },
            "payload": {
                "exact_count_available": True,
                "exact_count": point_result.exact_count,
                "source_system": "Point",
                "source_endpoint": point_result.source_endpoint,
                "method": "Conteo nota por nota con MONTO >= threshold_amount desde Point.",
                "total_notes": point_result.total_notes,
                "total_amount": _float_money(point_result.total_amount),
                "start_date": point_result.start_date.isoformat(),
                "end_date": point_result.end_date.isoformat(),
                "threshold_amount": _float_money(point_result.threshold_amount),
                "request_url": point_result.request_url,
                "items_by_branch": [
                    {
                        "branch_name": row.branch_name,
                        "exact_count": row.exact_count,
                        "total_notes": row.total_notes,
                        "total_amount": _float_money(row.total_amount),
                    }
                    for row in point_result.branch_results
                ],
            },
        }
    except Exception as exc:  # noqa: BLE001
        point_error = f"{type(exc).__name__}: {str(exc)[:240]}"

    qs = PointDailyBranchIndicator.objects.filter(indicator_date__range=(start_date, end_date))
    if branch_ids:
        qs = qs.filter(branch_id__in=branch_ids)

    totals = qs.aggregate(
        total_sales=Sum("total_amount"),
        total_tickets=Sum("total_tickets"),
        branch_days=Count("id"),
        first_date=Min("indicator_date"),
        last_date=Max("indicator_date"),
        branches_count=Count("branch_id", distinct=True),
    )
    total_sales = Decimal(str(totals.get("total_sales") or 0))
    total_tickets = int(totals.get("total_tickets") or 0)
    avg_ticket = (total_sales / Decimal(total_tickets)).quantize(Decimal("0.01")) if total_tickets else ZERO
    upper_bound = None
    if threshold > 0 and total_sales > 0:
        upper_bound = min(total_tickets, int((total_sales / threshold).to_integral_value(rounding=ROUND_FLOOR)))

    branch_payload = []
    branch_rows = (
        qs.values("branch__external_id", "branch__name", "branch__erp_branch__codigo", "branch__erp_branch__nombre")
        .annotate(total_sales=Sum("total_amount"), total_tickets=Sum("total_tickets"), branch_days=Count("id"))
        .order_by("-total_sales", "branch__name")
    )
    for row in branch_rows:
        row_sales = Decimal(str(row.get("total_sales") or 0))
        row_tickets = int(row.get("total_tickets") or 0)
        branch_payload.append(
            {
                "branch_external_id": row.get("branch__external_id") or "",
                "branch_code": row.get("branch__erp_branch__codigo") or "",
                "branch_name": row.get("branch__erp_branch__nombre") or row.get("branch__name") or "",
                "total_sales": _float_money(row_sales),
                "total_tickets": row_tickets,
                "avg_ticket": _float_money(row_sales / Decimal(row_tickets)) if row_tickets else 0.0,
                "branch_days": int(row.get("branch_days") or 0),
            }
        )

    return {
        "status": "not_available_exact" if total_tickets else "no_data",
        "sources": ["pos_bridge.PointDailyBranchIndicator"],
        "filters": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "branch": arguments.get("branch"),
            "threshold_amount": float(threshold),
        },
        "payload": {
            "exact_count_available": False,
            "exact_count": None,
            "reason": (
                "No se pudo consultar el conteo exacto nota por nota en Point. "
                "El fallback del ERP conserva ventas y tickets agregados por dia/sucursal; "
                "no conserva el monto individual de cada ticket en la capa analitica."
            ),
            "point_exact_query_available": False,
            "point_exact_query_error": point_error,
            "do_not_infer_zero": True,
            "total_sales": _float_money(total_sales),
            "total_tickets": total_tickets,
            "avg_ticket": _float_money(avg_ticket),
            "branch_days": int(totals.get("branch_days") or 0),
            "branches_count": int(totals.get("branches_count") or 0),
            "first_date": totals["first_date"].isoformat() if totals.get("first_date") else "",
            "last_date": totals["last_date"].isoformat() if totals.get("last_date") else "",
            "upper_bound_if_all_qualifying_tickets_were_at_least_threshold": upper_bound,
            "items_by_branch": branch_payload,
        },
    }


def _latest_inventory_qs(arguments: dict[str, Any]):
    from django.db.models import OuterRef, Subquery

    latest_snapshot_id = (
        PointInventorySnapshot.objects.filter(
            branch_id=OuterRef("branch_id"),
            product_id=OuterRef("product_id"),
        )
        .order_by("-captured_at", "-id")
        .values("id")[:1]
    )
    qs = PointInventorySnapshot.objects.select_related("branch", "branch__erp_branch", "product").filter(
        id=Subquery(latest_snapshot_id)
    )
    branch = str(arguments.get("branch") or "").strip()
    if branch:
        qs = qs.filter(
            Q(branch__name__icontains=branch)
            | Q(branch__external_id__iexact=branch)
            | Q(branch__erp_branch__codigo__iexact=branch)
        )
    return qs


def _handle_inventory_low_stock(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    qs = _latest_inventory_qs(arguments).filter(stock__lt=models.F("min_stock"), min_stock__gt=0).order_by(
        "branch__name",
        "product__name",
        "id",
    )
    limit = _parse_int(arguments.get("limit"), default=100, min_value=1, max_value=500)
    rows = []
    for snap in qs[:limit]:
        rows.append(
            {
                "branch_name": snap.branch.name,
                "branch_external_id": snap.branch.external_id,
                "erp_branch_code": snap.branch.erp_branch.codigo if snap.branch.erp_branch_id else "",
                "product_sku": snap.product.sku,
                "product_name": snap.product.name,
                "current_stock": float(snap.stock),
                "min_stock": float(snap.min_stock),
                "deficit": float(snap.min_stock - snap.stock),
                "captured_at": snap.captured_at.isoformat(),
            }
        )
    return {
        "status": "ok",
        "sources": ["pos_bridge.PointInventorySnapshot"],
        "filters": {"branch": arguments.get("branch"), "limit": limit},
        "payload": {"items": rows, "returned": len(rows)},
    }


def _handle_discrepancies(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    date_from, date_to, period_resolved = resolve_period_range(
        period_raw=arguments.get("periodo"),
        date_from_raw=arguments.get("from"),
        date_to_raw=arguments.get("to"),
    )
    threshold = _parse_decimal(arguments.get("threshold_pct"), default=Decimal("10"))
    top = _parse_int(arguments.get("top"), default=100, min_value=1, max_value=500)
    sucursal_id = arguments.get("sucursal_id")
    if sucursal_id not in {None, ""}:
        try:
            sucursal_id = int(sucursal_id)
        except (TypeError, ValueError):
            raise PermissionDenied("sucursal_id inválido para discrepancias.")
    else:
        sucursal_id = None
    payload = build_discrepancias_report(
        date_from=date_from,
        date_to=date_to,
        sucursal_id=sucursal_id,
        threshold_pct=threshold,
        top=top,
    )
    payload["scope"] = {
        "periodo": period_resolved,
        "sucursal_id": sucursal_id,
        "top": top,
    }
    return {
        "status": "ok",
        "sources": ["control.services.build_discrepancias_report"],
        "filters": {
            "periodo": arguments.get("periodo"),
            "from": arguments.get("from"),
            "to": arguments.get("to"),
            "sucursal_id": sucursal_id,
            "threshold_pct": str(threshold),
            "top": top,
        },
        "payload": payload,
    }


def _handle_sync_jobs(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    limit = _parse_int(arguments.get("limit"), default=20, min_value=1, max_value=100)
    qs = PointSyncJob.objects.all().order_by("-started_at", "-id")
    job_type = str(arguments.get("job_type") or "").strip().lower()
    status_filter = str(arguments.get("status") or "").strip().upper()
    if job_type:
        qs = qs.filter(job_type=job_type)
    if status_filter:
        qs = qs.filter(status=status_filter)
    items = []
    for job in qs[:limit]:
        items.append(
            {
                "id": job.id,
                "job_type": job.job_type,
                "status": job.status,
                "started_at": job.started_at.isoformat() if job.started_at else "",
                "finished_at": job.finished_at.isoformat() if job.finished_at else "",
                "attempt_count": job.attempt_count,
                "error_message": job.error_message,
                "triggered_by": job.triggered_by.username if job.triggered_by_id else "",
            }
        )
    return {
        "status": "ok",
        "sources": ["pos_bridge.PointSyncJob"],
        "filters": {"job_type": job_type, "status": status_filter, "limit": limit},
        "payload": {"items": items, "returned": len(items)},
    }


def _handle_purchase_requests(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    limit = _parse_int(arguments.get("limit"), default=50, min_value=1, max_value=200)
    estatus = str(arguments.get("estatus") or "").strip().upper()
    area = str(arguments.get("area") or "").strip()
    q = str(arguments.get("q") or "").strip()
    qs = SolicitudCompra.objects.select_related("insumo", "proveedor_sugerido").order_by("-creado_en", "-id")
    if estatus:
        qs = qs.filter(estatus=estatus)
    if area:
        qs = qs.filter(area__icontains=area)
    if q:
        qs = qs.filter(
            Q(folio__icontains=q)
            | Q(solicitante__icontains=q)
            | Q(insumo__nombre__icontains=q)
            | Q(proveedor_sugerido__nombre__icontains=q)
        )
    items = []
    for row in qs[:limit]:
        items.append(
            {
                "id": row.id,
                "folio": row.folio,
                "area": row.area,
                "solicitante": row.solicitante,
                "insumo": row.insumo.nombre,
                "proveedor_sugerido": row.proveedor_sugerido.nombre if row.proveedor_sugerido_id else "",
                "cantidad": float(row.cantidad),
                "fecha_requerida": row.fecha_requerida.isoformat(),
                "estatus": row.estatus,
                "fuera_de_catalogo": bool(row.fuera_de_catalogo),
                "cotizaciones_requeridas": row.cotizaciones_requeridas,
                "cotizaciones_recibidas": row.cotizaciones_recibidas,
                "justificacion_excepcion": row.justificacion_excepcion,
            }
        )
    return {
        "status": "ok",
        "sources": ["compras.SolicitudCompra"],
        "filters": {"estatus": estatus, "area": area, "q": q, "limit": limit},
        "payload": {"items": items, "returned": len(items)},
    }


def _handle_purchase_orders(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    limit = _parse_int(arguments.get("limit"), default=50, min_value=1, max_value=200)
    estatus = str(arguments.get("estatus") or "").strip().upper()
    q = str(arguments.get("q") or "").strip()
    qs = OrdenCompra.objects.select_related("proveedor", "solicitud").order_by("-creado_en", "-id")
    if estatus:
        qs = qs.filter(estatus=estatus)
    if q:
        qs = qs.filter(
            Q(folio__icontains=q)
            | Q(referencia__icontains=q)
            | Q(proveedor__nombre__icontains=q)
            | Q(solicitud__folio__icontains=q)
        )
    items = []
    for row in qs[:limit]:
        items.append(
            {
                "id": row.id,
                "folio": row.folio,
                "proveedor": row.proveedor.nombre,
                "solicitud_folio": row.solicitud.folio if row.solicitud_id else "",
                "fecha_emision": row.fecha_emision.isoformat(),
                "fecha_entrega_estimada": row.fecha_entrega_estimada.isoformat() if row.fecha_entrega_estimada else "",
                "monto_estimado": float(row.monto_estimado),
                "estatus": row.estatus,
                "referencia": row.referencia,
            }
        )
    return {
        "status": "ok",
        "sources": ["compras.OrdenCompra"],
        "filters": {"estatus": estatus, "q": q, "limit": limit},
        "payload": {"items": items, "returned": len(items)},
    }


INPUT_COST_STOPWORDS = {
    "actual",
    "al",
    "comprando",
    "compra",
    "compramos",
    "comprar",
    "costo",
    "cual",
    "de",
    "del",
    "el",
    "en",
    "es",
    "estamos",
    "kg",
    "kilo",
    "kilogramo",
    "la",
    "precio",
    "que",
}


def _input_cost_search_tokens(query: str) -> list[str]:
    normalized = normalizar_nombre(query)
    tokens = []
    for token in normalized.split():
        if token in INPUT_COST_STOPWORDS:
            continue
        if len(token) < 3:
            continue
        tokens.append(token)
    return tokens[:6]


def _score_input_candidate(*, insumo: Insumo, query: str, exact_alias_ids: set[int]) -> int:
    normalized_query = normalizar_nombre(query)
    normalized_name = insumo.nombre_normalizado or normalizar_nombre(insumo.nombre)
    normalized_point_name = normalizar_nombre(insumo.nombre_point or "")
    normalized_code = normalizar_nombre(insumo.codigo or "")
    normalized_point_code = normalizar_nombre(insumo.codigo_point or "")
    score = 0
    if insumo.id in exact_alias_ids:
        score = max(score, 980)
    if normalized_name == normalized_query:
        score = max(score, 1000)
    if normalized_point_name and normalized_point_name == normalized_query:
        score = max(score, 960)
    if normalized_code and normalized_code == normalized_query:
        score = max(score, 930)
    if normalized_point_code and normalized_point_code == normalized_query:
        score = max(score, 930)
    if normalized_query and normalized_name.startswith(normalized_query):
        score = max(score, 850)
    if normalized_query and normalized_query in normalized_name:
        score = max(score, 650)
    tokens = _input_cost_search_tokens(query)
    if tokens and all(token in normalized_name or token in normalized_point_name for token in tokens):
        score = max(score, 500 + (len(tokens) * 20))
    if insumo.tipo_item == Insumo.TIPO_MATERIA_PRIMA:
        score += 40
    if insumo.unidad_base_id:
        score += 10
    return score


def _resolve_input_cost_candidates(query: str, *, limit: int) -> list[Insumo]:
    normalized_query = normalizar_nombre(query)
    exact_alias_ids = set(
        InsumoAlias.objects.filter(nombre_normalizado=normalized_query, insumo__activo=True).values_list("insumo_id", flat=True)
    )
    q_filter = Q(pk__in=exact_alias_ids)
    if normalized_query:
        q_filter |= (
            Q(nombre_normalizado__icontains=normalized_query)
            | Q(nombre_point__icontains=query)
            | Q(codigo__iexact=query)
            | Q(codigo_point__iexact=query)
        )
    tokens = _input_cost_search_tokens(query)
    for token in tokens:
        q_filter |= Q(nombre_normalizado__icontains=token) | Q(nombre_point__icontains=token)
    candidates = list(
        Insumo.objects.select_related("unidad_base", "proveedor_principal")
        .filter(q_filter, activo=True)
        .distinct()[: max(limit * 8, 40)]
    )
    ranked = [
        (candidate, _score_input_candidate(insumo=candidate, query=query, exact_alias_ids=exact_alias_ids))
        for candidate in candidates
    ]
    ranked = [(candidate, score) for candidate, score in ranked if score > 0]
    ranked.sort(key=lambda item: (-item[1], item[0].nombre.lower(), item[0].id))
    return [candidate for candidate, _score in ranked[:limit]]


def _serialize_input_cost_candidate(insumo: Insumo) -> dict[str, Any]:
    return {
        "insumo_id": insumo.id,
        "insumo": insumo.nombre,
        "tipo_item": insumo.tipo_item,
        "unidad_base": insumo.unidad_base.codigo if insumo.unidad_base_id else "",
        "proveedor_principal": insumo.proveedor_principal.nombre if insumo.proveedor_principal_id else "",
    }


def _handle_current_input_cost(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    query = str(arguments.get("q") or arguments.get("insumo") or "").strip()
    insumo_id = arguments.get("insumo_id")
    limit = _parse_int(arguments.get("limit"), default=8, min_value=1, max_value=25)
    if insumo_id not in {None, ""}:
        insumo = canonical_insumo_by_id(insumo_id)
        candidates = [insumo] if insumo is not None else []
    elif query:
        candidates = _resolve_input_cost_candidates(query, limit=limit)
        insumo = canonical_insumo(candidates[0]) if candidates else None
    else:
        raise PermissionDenied("q o insumo_id es obligatorio para consultar costo vigente de insumo.")

    candidate_payload = [_serialize_input_cost_candidate(row) for row in candidates[:limit]]
    if insumo is None:
        return {
            "status": "not_found",
            "sources": ["maestros.CostoInsumo", "maestros.Insumo"],
            "filters": {"q": query, "insumo_id": insumo_id, "limit": limit},
            "payload": {"items": [], "returned": 0, "candidates": candidate_payload},
        }

    member_ids = canonical_member_ids(insumo)
    latest_cost = (
        CostoInsumo.objects.select_related("insumo", "proveedor")
        .filter(insumo_id__in=member_ids)
        .order_by("-fecha", "-id")
        .first()
    )
    if latest_cost is None:
        return {
            "status": "no_cost",
            "sources": ["maestros.CostoInsumo", "maestros.Insumo"],
            "filters": {"q": query, "insumo_id": insumo_id, "limit": limit},
            "payload": {
                **_serialize_input_cost_candidate(insumo),
                "member_ids": member_ids,
                "costo_unitario": None,
                "moneda": "",
                "fecha": "",
                "proveedor": "",
                "costo_source_insumo": "",
                "candidates": candidate_payload,
            },
        }

    return {
        "status": "ok",
        "sources": ["maestros.CostoInsumo", "maestros.Insumo"],
        "filters": {"q": query, "insumo_id": insumo_id, "limit": limit},
        "payload": {
            **_serialize_input_cost_candidate(insumo),
            "member_ids": member_ids,
            "costo_unitario": float(latest_cost.costo_unitario),
            "moneda": latest_cost.moneda,
            "fecha": latest_cost.fecha.isoformat(),
            "proveedor": latest_cost.proveedor.nombre if latest_cost.proveedor_id else "",
            "costo_source_insumo_id": latest_cost.insumo_id,
            "costo_source_insumo": latest_cost.insumo.nombre,
            "source_hash": latest_cost.source_hash,
            "candidates": candidate_payload,
        },
    }


PROMOTION_STOPWORDS = {
    "con",
    "crema",
    "de",
    "del",
    "dia",
    "día",
    "el",
    "en",
    "la",
    "las",
    "los",
    "pastel",
    "producto",
    "productos",
    "promocion",
    "promoción",
}


def _money(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


def _float_money(value: Decimal) -> float:
    return float(_money(value))


def _parse_promotion_queries(arguments: dict[str, Any]) -> list[str]:
    raw = arguments.get("product_queries")
    if isinstance(raw, list):
        queries = [str(item).strip() for item in raw if str(item or "").strip()]
    else:
        text = str(raw or arguments.get("q") or arguments.get("products") or "").strip()
        queries = [chunk.strip() for chunk in text.split(",") if chunk.strip()]
    return queries[:12]


PROMOTION_TOKEN_ALIASES = {
    "grnde": "grande",
    "grnade": "grande",
    "gde": "grande",
    "mediana": "mediano",
    "mdno": "mediano",
    "rebanadas": "rebanada",
    "vasos": "vaso",
}


def _normalize_promotion_text(value: str) -> str:
    tokens = []
    for token in normalizar_nombre(value).split():
        tokens.append(PROMOTION_TOKEN_ALIASES.get(token, token))
    return " ".join(tokens)


def _promotion_tokens(query: str) -> list[str]:
    tokens = []
    for token in _normalize_promotion_text(query).split():
        if token in PROMOTION_STOPWORDS or len(token) < 3:
            continue
        tokens.append(token)
    return tokens[:8]


def _promotion_product_text(product: PointProduct) -> str:
    return _normalize_promotion_text(" ".join([product.name or "", product.category or "", product.sku or ""]))


def _score_promotion_product(product: PointProduct, query: str) -> int:
    normalized_query = _normalize_promotion_text(query)
    normalized_name = _normalize_promotion_text(product.normalized_name or product.name)
    normalized_sku = _normalize_promotion_text(product.sku or "")
    product_text = _promotion_product_text(product)
    score = 0
    if normalized_name == normalized_query:
        score = 1000
    elif normalized_sku and normalized_sku == normalized_query:
        score = 980
    elif normalized_query and normalized_name.startswith(normalized_query):
        score = 850
    elif normalized_query and normalized_query in normalized_name:
        score = 700
    tokens = _promotion_tokens(query)
    if tokens:
        matched = sum(1 for token in tokens if token in product_text)
        score = max(score, 420 + matched * 60)
        if "vaso" in tokens:
            score += 260 if "vaso" in product_text else -260
        if "rebanada" in tokens:
            score += 160 if "rebanada" in product_text else -160
        for size_token in ("chico", "mini", "mediano", "grande"):
            if size_token in tokens:
                score += 180 if size_token in product_text else -120
    if product.precio and product.precio > 0:
        score += 25
    return score


def _resolve_promotion_product(query: str) -> PointProduct | None:
    normalized_query = _normalize_promotion_text(query)
    q_filter = Q(active=True)
    search = Q()
    if normalized_query:
        search |= Q(normalized_name__icontains=normalized_query)
    if query:
        search |= Q(name__icontains=query) | Q(sku__iexact=query) | Q(external_id__iexact=query)
    for token in _promotion_tokens(query):
        search |= Q(normalized_name__icontains=token)
    candidates = list(PointProduct.objects.filter(q_filter & search).distinct())
    ranked = [(candidate, _score_promotion_product(candidate, query)) for candidate in candidates]
    ranked = [(candidate, score) for candidate, score in ranked if score > 0]
    ranked.sort(key=lambda item: (-item[1], item[0].name.lower(), item[0].id))
    return ranked[0][0] if ranked else None


def _is_rebanada_mix_query(query: str) -> bool:
    normalized_query = _normalize_promotion_text(query)
    tokens = set(normalized_query.split())
    return "rebanada" in tokens and bool(tokens & {"revoltura", "surtido", "surtida", "mix", "mezcla"})


def _resolve_promotion_product_group(query: str) -> tuple[str, str, list[PointProduct]]:
    if not _is_rebanada_mix_query(query):
        return "", "", []
    products = list(
        PointProduct.objects.filter(active=True)
        .filter(Q(category__iexact="Rebanada") | Q(normalized_name__icontains="rebanada") | Q(normalized_name__endswith=" r"))
        .exclude(Q(normalized_name__startswith="sabor ") | Q(normalized_name__icontains=" topping") | Q(normalized_name__startswith="topping "))
        .order_by("sku", "name", "id")
    )
    return "GRUPO_REBANADAS", "Revoltura de rebanadas de pastel", products


def _resolve_product_recipe(product: PointProduct | None, query: str) -> Receta | None:
    if product is not None:
        receta = Receta.objects.filter(
            Q(codigo_point__iexact=product.sku)
            | Q(codigo_point__iexact=product.external_id)
            | Q(nombre_normalizado__icontains=product.normalized_name or normalizar_nombre(product.name))
        ).order_by("id").first()
        if receta is not None:
            return receta
    normalized_query = normalizar_nombre(query)
    if not normalized_query:
        return None
    return Receta.objects.filter(nombre_normalizado__icontains=normalized_query).order_by("id").first()


def _promotion_sales_totals(
    *,
    product: PointProduct | None,
    products: list[PointProduct] | None = None,
    receta: Receta | None,
    query: str,
    start: date,
    end: date,
):
    point_product_ids = []
    product_keys = set()
    if products:
        point_product_ids.extend(row.id for row in products)
        product_keys.update(key for row in products for key in (row.sku, row.external_id) if key)
    if product is not None:
        point_product_ids.append(product.id)
        product_keys.update(key for key in (product.sku, product.external_id) if key)
    normalized_query = _normalize_promotion_text(query)
    return get_promotion_sales_totals(
        start_date=start,
        end_date=end,
        point_product_ids=point_product_ids,
        product_keys=sorted(product_keys),
        receta_id=receta.id if receta is not None else None,
        receta_code=receta.codigo_point if receta is not None else None,
        product_name_query=query if normalized_query else None,
    )


def _latest_recipe_cost(receta: Receta | None) -> Decimal:
    if receta is None:
        return ZERO
    try:
        cost = Decimal(str(receta.costo_total_estimado_decimal or 0))
    except Exception:
        cost = ZERO
    if cost > 0:
        return cost
    version = (
        RecetaCostoVersion.objects.filter(receta=receta, costo_total__gt=0)
        .order_by("-version_num", "-creado_en", "-id")
        .values_list("costo_total", flat=True)
        .first()
    )
    return Decimal(str(version or 0))


def _ceil_decimal(value: Decimal) -> int | None:
    if value <= 0:
        return None
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _handle_promotion_profitability(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    promotion_type = str(arguments.get("promotion_type") or "3x2").strip().lower()
    if promotion_type not in {"3x2", "3 x 2", "3por2", "3_por_2"}:
        raise PermissionDenied("Solo se soporta simulación 3x2 en esta primera herramienta.")
    product_queries = _parse_promotion_queries(arguments)
    if not product_queries:
        raise PermissionDenied("product_queries es obligatorio para analizar una promoción.")
    event_name = str(arguments.get("event_name") or "Promoción").strip() or "Promoción"
    lookback_days = _parse_int(arguments.get("lookback_days"), default=30, min_value=1, max_value=365)
    expected_uplift_pct = _parse_decimal(arguments.get("expected_uplift_pct"), default=Decimal("50"))
    marketing_budget = _parse_decimal(arguments.get("marketing_budget"), default=ZERO)
    today = timezone.localdate()
    end_date = _parse_iso_date(arguments.get("end_date")) or today
    start_date = _parse_iso_date(arguments.get("start_date")) or (end_date - timedelta(days=lookback_days - 1))

    items = []
    data_gaps = []
    for query in product_queries:
        group_sku, group_name, group_products = _resolve_promotion_product_group(query)
        product = None if group_products else _resolve_promotion_product(query)
        receta = None if group_products else _resolve_product_recipe(product, query)
        totals = _promotion_sales_totals(
            product=product,
            products=group_products,
            receta=receta,
            query=query,
            start=start_date,
            end=end_date,
        )
        baseline_units = Decimal(str(totals.get("quantity") or 0))
        observed_sales = Decimal(str(totals.get("sales") or 0))
        observed_cost = Decimal(str(totals.get("cost") or 0))
        price = Decimal(str(product.precio or 0)) if product and product.precio else ZERO
        if price <= 0 and baseline_units > 0:
            price = observed_sales / baseline_units
        observed_unit_cost = observed_cost / baseline_units if baseline_units > 0 and observed_cost > 0 else ZERO
        recipe_unit_cost = _latest_recipe_cost(receta)
        if not group_products and recipe_unit_cost > 0:
            unit_cost = recipe_unit_cost
            cost_source = "receta_costo_vigente"
        elif observed_unit_cost > 0:
            unit_cost = observed_unit_cost
            cost_source = "venta_historica_observada"
        else:
            unit_cost = recipe_unit_cost
            cost_source = "receta_costo_vigente" if recipe_unit_cost > 0 else ""
        expected_units = (baseline_units * (Decimal("1") + (expected_uplift_pct / Decimal("100")))).quantize(Decimal("0.001"))
        promo_effective_price = price * Decimal("2") / Decimal("3")
        normal_margin = price - unit_cost
        promo_margin = promo_effective_price - unit_cost
        baseline_profit = baseline_units * normal_margin
        promo_profit = expected_units * promo_margin
        profit_delta = promo_profit - baseline_profit
        break_even_units = _ceil_decimal(baseline_profit / promo_margin) if promo_margin > 0 else None
        break_even_uplift = (
            ((Decimal(break_even_units) / baseline_units) - Decimal("1")) * Decimal("100")
            if break_even_units is not None and baseline_units > 0
            else None
        )
        if group_products and baseline_units <= 0:
            data_gaps.append(f"No encontré venta reciente para la mezcla '{query}'.")
        if not group_products and product is None:
            data_gaps.append(f"No encontré producto Point para '{query}'.")
        if price <= 0:
            data_gaps.append(f"Falta precio vigente para '{query}'.")
        if unit_cost <= 0:
            data_gaps.append(f"Falta costo unitario para '{query}'.")
        if baseline_units <= 0:
            data_gaps.append(f"Falta histórico reciente de venta para '{query}'.")

        if price <= 0 or unit_cost <= 0 or baseline_units <= 0:
            recommendation = "DATOS_INSUFICIENTES"
        elif promo_margin <= 0 or profit_delta < 0:
            recommendation = "NO_CONVIENE"
        elif break_even_uplift is not None and break_even_uplift > expected_uplift_pct + Decimal("20"):
            recommendation = "RIESGO_ALTO"
        else:
            recommendation = "CONVIENE"

        items.append(
            {
                "query": query,
                "item_type": "product_group" if group_products else "product",
                "product_id": product.id if product else None,
                "product_sku": group_sku or (product.sku if product else ""),
                "product_name": group_name or (product.name if product else ""),
                "product_count": len(group_products) if group_products else 1 if product else 0,
                "sample_products": [
                    {"sku": row.sku, "name": row.name}
                    for row in group_products[:10]
                ],
                "receta_id": receta.id if receta else None,
                "receta": receta.nombre if receta else "",
                "normal_unit_price": _float_money(price),
                "unit_cost": _float_money(unit_cost),
                "cost_source": cost_source,
                "recipe_unit_cost": _float_money(recipe_unit_cost),
                "observed_unit_cost": _float_money(observed_unit_cost),
                "promo_effective_unit_price": _float_money(promo_effective_price),
                "discount_pct": 33.33,
                "normal_margin_per_unit": _float_money(normal_margin),
                "promo_margin_per_unit": _float_money(promo_margin),
                "normal_margin_pct": float(((normal_margin / price) * 100).quantize(Decimal("0.01"))) if price > 0 else None,
                "promo_margin_pct": (
                    float(((promo_margin / promo_effective_price) * 100).quantize(Decimal("0.01")))
                    if promo_effective_price > 0
                    else None
                ),
                "baseline_units": float(baseline_units),
                "expected_units": float(expected_units),
                "baseline_profit": _float_money(baseline_profit),
                "promo_profit": _float_money(promo_profit),
                "profit_delta": _float_money(profit_delta),
                "break_even_units": break_even_units,
                "break_even_uplift_pct": float(break_even_uplift.quantize(Decimal("0.01"))) if break_even_uplift is not None else None,
                "recommendation": recommendation,
                "finance_note": "El 3x2 reduce el precio efectivo a 66.67% del precio normal.",
            }
        )

    total_baseline_profit = sum(Decimal(str(row["baseline_profit"])) for row in items)
    total_promo_profit = sum(Decimal(str(row["promo_profit"])) for row in items)
    total_profit_delta = total_promo_profit - total_baseline_profit - marketing_budget
    ranked = sorted(
        items,
        key=lambda row: (row["recommendation"] != "CONVIENE", -Decimal(str(row["profit_delta"]))),
    )
    return {
        "status": "ok",
        "sources": ["pos_bridge.PointProduct", "ventas.services.sales_read_service", "recetas.Receta"],
        "filters": {
            "promotion_type": "3x2",
            "event_name": event_name,
            "product_queries": product_queries,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "expected_uplift_pct": float(expected_uplift_pct),
            "marketing_budget": _float_money(marketing_budget),
        },
        "payload": {
            "promotion_type": "3x2",
            "event_name": event_name,
            "expected_uplift_pct": float(expected_uplift_pct),
            "marketing_budget": _float_money(marketing_budget),
            "items": items,
            "returned": len(items),
            "finance": {
                "baseline_profit": _float_money(total_baseline_profit),
                "promo_profit_before_marketing": _float_money(total_promo_profit),
                "promo_profit_after_marketing": _float_money(total_promo_profit - marketing_budget),
                "profit_delta_after_marketing": _float_money(total_profit_delta),
            },
            "marketing": {
                "message": "Usar el 3x2 solo como gancho de volumen; medir uplift real contra el punto de equilibrio por producto.",
                "suggested_controls": ["fechas limitadas", "stock objetivo por sucursal", "pieza extra visible en ticket", "no combinar con otros descuentos"],
            },
            "operations": {
                "requires_supply_plan": True,
                "notes": ["Validar fresa, crema, empaques, capacidad de producción y merma esperada antes de publicar."],
            },
            "accounting": {
                "discount_treatment": "Registrar el 3x2 como descuento/promoción contra venta bruta; no mezclarlo con costo de producción.",
                "audit_note": "La utilidad se calcula como venta neta promocional menos costo estimado; no incluye nómina fija ni gasto operativo general.",
            },
            "decision_summary": {
                "overall_recommendation": "CONVIENE" if total_profit_delta > 0 and not data_gaps else "REVISAR",
                "ranked_products": [
                    {
                        "product_sku": row["product_sku"],
                        "product_name": row["product_name"],
                        "recommendation": row["recommendation"],
                        "profit_delta": row["profit_delta"],
                        "break_even_units": row["break_even_units"],
                    }
                    for row in ranked
                ],
                "data_gaps": data_gaps,
            },
        },
    }


def _handle_recipe_cost_history(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    receta_id = arguments.get("receta_id")
    if receta_id in {None, ""}:
        raise PermissionDenied("receta_id es obligatorio para consultar costo historico.")
    try:
        receta_id = int(receta_id)
    except (TypeError, ValueError):
        raise PermissionDenied("receta_id invalido para costo historico.")
    limit = _parse_int(arguments.get("limit"), default=10, min_value=1, max_value=50)
    qs = RecetaCostoVersion.objects.select_related("receta").filter(receta_id=receta_id).order_by("-version_num", "-id")
    items = []
    receta_name = ""
    for row in qs[:limit]:
        receta_name = row.receta.nombre
        items.append(
            {
                "receta_id": row.receta_id,
                "receta": row.receta.nombre,
                "version_num": row.version_num,
                "costo_mp": float(row.costo_mp),
                "costo_mo": float(row.costo_mo),
                "costo_indirecto": float(row.costo_indirecto),
                "costo_total": float(row.costo_total),
                "rendimiento_cantidad": float(row.rendimiento_cantidad) if row.rendimiento_cantidad is not None else None,
                "rendimiento_unidad": row.rendimiento_unidad,
                "costo_por_unidad_rendimiento": (
                    float(row.costo_por_unidad_rendimiento) if row.costo_por_unidad_rendimiento is not None else None
                ),
                "fuente": row.fuente,
                "creado_en": row.creado_en.isoformat(),
            }
        )
    return {
        "status": "ok",
        "sources": ["recetas.RecetaCostoVersion"],
        "filters": {"receta_id": receta_id, "limit": limit},
        "payload": {"receta": receta_name, "items": items, "returned": len(items)},
    }


def _handle_trigger_sync_jobs(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "approval_required",
        "sources": ["policy.execute_safe_action"],
        "message": (
            "La herramienta existe pero en el piloto requiere aprobacion humana antes de ejecutar "
            "POST /api/pos-bridge/sync-jobs/trigger/."
        ),
        "requested_arguments": arguments,
    }


def _handle_create_purchase_request_draft(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "approval_required",
        "sources": ["policy.execute_safe_action"],
        "message": (
            "La creacion de borradores de solicitud de compra requiere aprobacion humana en el piloto. "
            "Tras aprobarse, el ERP generara una SolicitudCompra en estatus BORRADOR."
        ),
        "requested_arguments": arguments,
    }


def _handle_create_production_plan_draft(_user, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "approval_required",
        "sources": ["policy.execute_safe_action"],
        "message": (
            "La creacion de borradores de plan de produccion requiere aprobacion humana en el piloto. "
            "Tras aprobarse, el ERP generara un PlanProduccion en estado BORRADOR."
        ),
        "requested_arguments": arguments,
    }


def _execute_create_purchase_request_draft(user, arguments: dict[str, Any]) -> dict[str, Any]:
    normalized_payload = {
        "area": str(arguments.get("area") or "Compras").strip() or "Compras",
        "solicitante": (
            str(arguments.get("solicitante") or arguments.get("_approval_requested_by") or user.username or "").strip()
            or user.username
        )[:120],
        "insumo_id": arguments.get("insumo_id"),
        "cantidad": arguments.get("cantidad"),
        "fecha_requerida": arguments.get("fecha_requerida"),
        "estatus": SolicitudCompra.STATUS_BORRADOR,
        "auto_crear_orden": False,
    }
    serializer = ComprasSolicitudCreateSerializer(data=normalized_payload)
    if not serializer.is_valid():
        raise PermissionDenied(f"Solicitud de compra invalida para ejecucion segura: {serializer.errors}")
    data = serializer.validated_data

    insumo = canonical_insumo_by_id(data["insumo_id"])
    if insumo is None:
        raise PermissionDenied("insumo_id no encontrado o inactivo para crear solicitud de compra.")

    solicitud = SolicitudCompra.objects.create(
        area=(data.get("area") or "Compras").strip() or "Compras",
        solicitante=(data.get("solicitante") or user.username or "").strip()[:120] or user.username,
        insumo=insumo,
        proveedor_sugerido=insumo.proveedor_principal,
        cantidad=data["cantidad"],
        fecha_requerida=data.get("fecha_requerida") or timezone.localdate(),
        estatus=SolicitudCompra.STATUS_BORRADOR,
    )
    return {
        "status": "ok",
        "sources": ["compras.SolicitudCompra", "api.serializers.ComprasSolicitudCreateSerializer"],
        "payload": {
            "id": solicitud.id,
            "folio": solicitud.folio,
            "area": solicitud.area,
            "solicitante": solicitud.solicitante,
            "insumo_id": solicitud.insumo_id,
            "insumo": solicitud.insumo.nombre,
            "cantidad": float(solicitud.cantidad),
            "fecha_requerida": solicitud.fecha_requerida.isoformat(),
            "estatus": solicitud.estatus,
            "proveedor_sugerido_id": solicitud.proveedor_sugerido_id,
            "proveedor_sugerido": solicitud.proveedor_sugerido.nombre if solicitud.proveedor_sugerido_id else "",
        },
    }


def _execute_create_production_plan_draft(user, arguments: dict[str, Any]) -> dict[str, Any]:
    normalized_payload = {
        "nombre": str(arguments.get("nombre") or "").strip(),
        "fecha_produccion": arguments.get("fecha_produccion"),
        "notas": str(arguments.get("notas") or "").strip(),
        "items": arguments.get("items") or [],
    }
    serializer = PlanProduccionCreateSerializer(data=normalized_payload)
    if not serializer.is_valid():
        raise PermissionDenied(f"Plan de produccion invalido para ejecucion segura: {serializer.errors}")
    data = serializer.validated_data

    fecha_produccion = data.get("fecha_produccion") or timezone.localdate()
    nombre = (data.get("nombre") or "").strip()
    notas = (data.get("notas") or "").strip()
    rows = data["items"]

    receta_ids = sorted({int(row["receta_id"]) for row in rows})
    recetas = Receta.objects.filter(id__in=receta_ids).only("id", "nombre", "codigo_point")
    receta_map = {r.id: r for r in recetas}
    missing_ids = [rid for rid in receta_ids if rid not in receta_map]
    if missing_ids:
        raise PermissionDenied(f"Hay recetas inexistentes en items: {missing_ids}")

    with transaction.atomic():
        plan = PlanProduccion.objects.create(
            nombre=nombre or f"Plan {fecha_produccion} #{PlanProduccion.objects.count() + 1}",
            fecha_produccion=fecha_produccion,
            notas=notas,
            creado_por=user if user.is_authenticated else None,
            estado=PlanProduccion.ESTADO_BORRADOR,
        )
        for row in rows:
            receta = receta_map[int(row["receta_id"])]
            PlanProduccionItem.objects.create(
                plan=plan,
                receta=receta,
                cantidad=Decimal(str(row["cantidad"])),
                notas=(row.get("notas") or "").strip()[:160],
            )

    created_rows = list(plan.items.select_related("receta").all().order_by("id"))
    return {
        "status": "ok",
        "sources": ["recetas.PlanProduccion", "recetas.PlanProduccionItem", "api.serializers.PlanProduccionCreateSerializer"],
        "payload": {
            "id": plan.id,
            "nombre": plan.nombre,
            "fecha_produccion": plan.fecha_produccion.isoformat(),
            "estado": plan.estado,
            "notas": plan.notas or "",
            "items_count": len(created_rows),
            "items": [
                {
                    "id": row.id,
                    "receta_id": row.receta_id,
                    "receta": row.receta.nombre,
                    "codigo_point": row.receta.codigo_point,
                    "cantidad": float(row.cantidad),
                    "notas": row.notas or "",
                }
                for row in created_rows
            ],
        },
    }


def _execute_trigger_sync_jobs(user, arguments: dict[str, Any]) -> dict[str, Any]:
    job_type = str(arguments.get("job_type") or PointSyncJob.JOB_TYPE_INVENTORY).strip().lower()
    branch_filter = str(arguments.get("branch_filter") or arguments.get("branch") or "").strip() or None

    if job_type == PointSyncJob.JOB_TYPE_INVENTORY:
        sync_job = run_inventory_sync(triggered_by=user, branch_filter=branch_filter)
    elif job_type == PointSyncJob.JOB_TYPE_SALES:
        sync_job = run_daily_sales_sync(
            triggered_by=user,
            branch_filter=branch_filter,
            lookback_days=_parse_int(arguments.get("days"), default=3, min_value=1, max_value=30),
            lag_days=_parse_int(arguments.get("lag_days"), default=1, min_value=0, max_value=7),
        )
    elif job_type == PointSyncJob.JOB_TYPE_RECIPES:
        sync_job = run_product_recipe_sync(triggered_by=user, branch_hint=branch_filter)
    else:
        raise PermissionDenied("job_type no soportado para trigger seguro desde AI Gateway.")

    return {
        "status": "ok",
        "sources": ["pos_bridge.tasks.run_inventory_sync|run_daily_sales_sync|run_product_recipe_sync"],
        "payload": {
            "job_id": sync_job.id,
            "job_type": sync_job.job_type,
            "status": sync_job.status,
            "started_at": sync_job.started_at.isoformat() if sync_job.started_at else "",
            "finished_at": sync_job.finished_at.isoformat() if sync_job.finished_at else "",
            "attempt_count": sync_job.attempt_count,
            "triggered_by": sync_job.triggered_by.username if sync_job.triggered_by_id else "",
        },
    }


def _can_view_sync_jobs(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN)


def _can_manage_purchases(user) -> bool:
    return can_manage_compras(user) or has_any_role(user, ROLE_DG, ROLE_ADMIN)


def _can_manage_production(user) -> bool:
    return has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_PRODUCCION) and can_view_recetas(user)


def _can_view_sales(user) -> bool:
    return can_view_reportes(user)


def _can_view_dashboard(user) -> bool:
    return can_view_reportes(user)


def _can_view_inventory(user) -> bool:
    return can_view_inventario(user)


def _can_view_input_costs(user) -> bool:
    return can_view_recetas(user) or can_view_compras(user) or can_view_reportes(user)


def _can_analyze_promotions(user) -> bool:
    return can_view_reportes(user) or has_any_role(user, ROLE_DG, ROLE_ADMIN, ROLE_VENTAS)


def _can_view_purchases(user) -> bool:
    return can_view_compras(user)


def _can_view_recipe_costs(user) -> bool:
    return can_view_recetas(user)


TOOLS: dict[str, AIToolDefinition] = {
    "erp.get_dashboard": AIToolDefinition(
        key="erp.get_dashboard",
        name="Dashboard ejecutivo ERP",
        description="Consulta KPIs ejecutivos y snapshot BI del ERP.",
        operation_type="read",
        data_domain="reporting",
        branch_scoped=False,
        requires_approval=False,
        access_check=_can_view_dashboard,
        handler=_handle_dashboard,
        argument_schema={
            "type": "object",
            "properties": {
                "period_days": {"type": "integer", "minimum": 7, "maximum": 365, "default": 90},
                "months": {"type": "integer", "minimum": 3, "maximum": 24, "default": 6},
            },
        },
        result_contract={"status": "ok", "payload": "snapshot BI serializado para API"},
    ),
    "erp.get_audit_logs": AIToolDefinition(
        key="erp.get_audit_logs",
        name="Bitácora audit ERP",
        description="Consulta registros de auditoría del ERP.",
        operation_type="read",
        data_domain="audit",
        branch_scoped=False,
        requires_approval=False,
        access_check=can_view_audit,
        handler=_handle_audit_logs,
        argument_schema={
            "type": "object",
            "properties": {
                "q": {"type": "string"},
                "action": {"type": "string"},
                "model": {"type": "string"},
                "user_id": {"type": "integer"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
            },
        },
        result_contract={"status": "ok", "payload": {"items": "lista de eventos audit", "returned": "conteo"}},
    ),
    "erp.get_sales_summary": AIToolDefinition(
        key="erp.get_sales_summary",
        name="Resumen de ventas Point",
        description=(
            "Resume ventas agregadas del POS por rango y sucursal. "
            "No sirve para contar tickets por umbral de monto; para preguntas tipo tickets >= $500 usa erp_get_ticket_amount_threshold."
        ),
        operation_type="read",
        data_domain="sales",
        branch_scoped=True,
        requires_approval=False,
        access_check=_can_view_sales,
        handler=_handle_sales_summary,
        argument_schema={
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "branch": {"type": "string", "description": "codigo ERP o referencia Point de sucursal"},
            },
        },
        result_contract={"status": "ok", "payload": {"total_sales": "number", "total_quantity": "number"}},
    ),
    "erp.get_sales_by_branch": AIToolDefinition(
        key="erp.get_sales_by_branch",
        name="Ventas por sucursal",
        description="Analiza ventas agregadas por sucursal.",
        operation_type="analyze",
        data_domain="sales",
        branch_scoped=True,
        requires_approval=False,
        access_check=_can_view_sales,
        handler=_handle_sales_by_branch,
        argument_schema={
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "branch": {"type": "string"},
            },
        },
        result_contract={"status": "ok", "payload": "lista agregada por sucursal"},
    ),
    "erp.get_sales_trends": AIToolDefinition(
        key="erp.get_sales_trends",
        name="Tendencias de ventas",
        description="Analiza tendencia mensual de ventas del POS.",
        operation_type="analyze",
        data_domain="sales",
        branch_scoped=True,
        requires_approval=False,
        access_check=_can_view_sales,
        handler=_handle_sales_trends,
        argument_schema={
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "branch": {"type": "string"},
            },
        },
        result_contract={"status": "ok", "payload": "serie mensual agregada"},
    ),
    "erp.get_ticket_amount_threshold": AIToolDefinition(
        key="erp.get_ticket_amount_threshold",
        name="Tickets por umbral de monto",
        description=(
            "Evalúa preguntas como cuántos tickets fueron de $500 o más. "
            "Si el ERP no tiene monto por ticket individual, devuelve la limitación exacta y los agregados auditados; "
            "no debe inferir cero desde resumen de ventas."
        ),
        operation_type="analyze",
        data_domain="sales",
        branch_scoped=True,
        requires_approval=False,
        access_check=_can_view_sales,
        handler=_handle_ticket_amount_threshold,
        argument_schema={
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "branch": {"type": "string"},
                "threshold_amount": {"type": "number", "default": 500},
            },
        },
        result_contract={
            "status": "not_available_exact|no_data",
            "payload": {
                "exact_count_available": "boolean",
                "exact_count": "null",
                "total_sales": "number",
                "total_tickets": "number",
                "avg_ticket": "number",
            },
        },
    ),
    "erp.get_inventory_low_stock": AIToolDefinition(
        key="erp.get_inventory_low_stock",
        name="Alertas de stock bajo",
        description="Consulta productos por debajo del stock mínimo.",
        operation_type="read",
        data_domain="inventory",
        branch_scoped=True,
        requires_approval=False,
        access_check=_can_view_inventory,
        handler=_handle_inventory_low_stock,
        argument_schema={
            "type": "object",
            "properties": {
                "branch": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
            },
        },
        result_contract={"status": "ok", "payload": {"items": "stock bajo", "returned": "conteo"}},
    ),
    "erp.get_current_input_cost": AIToolDefinition(
        key="erp.get_current_input_cost",
        name="Costo actual de insumo",
        description=(
            "Consulta el costo unitario vigente de compra de un insumo o materia prima, "
            "por ejemplo kg de fresa fresca. No consulta recetas."
        ),
        operation_type="read",
        data_domain="costing",
        branch_scoped=False,
        requires_approval=False,
        access_check=_can_view_input_costs,
        handler=_handle_current_input_cost,
        argument_schema={
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "nombre, alias o codigo del insumo; ejemplo: fresa fresca"},
                "insumo_id": {"type": "integer"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 25, "default": 8},
            },
            "additionalProperties": False,
        },
        result_contract={
            "status": "ok|not_found|no_cost",
            "payload": {"insumo": "nombre canonico", "costo_unitario": "number|null", "unidad_base": "unidad"},
        },
    ),
    "erp.analyze_promotion_profitability": AIToolDefinition(
        key="erp.analyze_promotion_profitability",
        name="Análisis financiero de promoción",
        description=(
            "Simula una promoción 3x2 por producto con precio Point, costo observado, margen, utilidad, "
            "punto de equilibrio, notas de marketing, operación y contabilidad. Respeta presentaciones "
            "como vaso mediano/grande y soporta revoltura/surtido de rebanadas como grupo ponderado."
        ),
        operation_type="analyze",
        data_domain="finance",
        branch_scoped=False,
        requires_approval=False,
        access_check=_can_analyze_promotions,
        handler=_handle_promotion_profitability,
        argument_schema={
            "type": "object",
            "required": ["product_queries"],
            "properties": {
                "promotion_type": {"type": "string", "enum": ["3x2"], "default": "3x2"},
                "event_name": {"type": "string"},
                "product_queries": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Lista de productos o términos Point/ERP a comparar; ejemplo: vaso fresas con crema mediano, vaso fresas con crema grande, revoltura de rebanadas de pastel.",
                },
                "expected_uplift_pct": {"type": "number", "default": 50},
                "marketing_budget": {"type": "number", "default": 0},
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "lookback_days": {"type": "integer", "minimum": 1, "maximum": 365, "default": 30},
            },
        },
        result_contract={
            "status": "ok",
            "payload": {
                "items": "tabla comparativa por producto",
                "finance": "totales financieros",
                "marketing": "lectura comercial",
                "operations": "riesgos operativos",
                "accounting": "tratamiento contable",
            },
        },
    ),
    "erp.get_discrepancies": AIToolDefinition(
        key="erp.get_discrepancies",
        name="Discrepancias operativas",
        description="Analiza diferencias entre producción, ventas, mermas e inventario.",
        operation_type="analyze",
        data_domain="control",
        branch_scoped=True,
        requires_approval=False,
        access_check=can_view_reportes,
        handler=_handle_discrepancies,
        argument_schema={
            "type": "object",
            "properties": {
                "periodo": {"type": "string", "description": "alias de periodo soportado por control.services"},
                "from": {"type": "string", "format": "date"},
                "to": {"type": "string", "format": "date"},
                "sucursal_id": {"type": "integer"},
                "threshold_pct": {"type": "number", "default": 10},
                "top": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
            },
        },
        result_contract={"status": "ok", "payload": "reporte de discrepancias y scope resuelto"},
    ),
    "erp.get_sync_jobs": AIToolDefinition(
        key="erp.get_sync_jobs",
        name="Jobs de sincronización Point",
        description="Consulta estado de jobs de sincronización Point/ERP.",
        operation_type="read",
        data_domain="integrations",
        branch_scoped=False,
        requires_approval=False,
        access_check=_can_view_sync_jobs,
        handler=_handle_sync_jobs,
        argument_schema={
            "type": "object",
            "properties": {
                "job_type": {"type": "string", "enum": ["inventory", "sales", "recipes"]},
                "status": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
            },
        },
        result_contract={"status": "ok", "payload": {"items": "jobs", "returned": "conteo"}},
    ),
    "erp.get_purchase_requests": AIToolDefinition(
        key="erp.get_purchase_requests",
        name="Solicitudes de compra",
        description="Consulta solicitudes de compra del ERP.",
        operation_type="read",
        data_domain="purchasing",
        branch_scoped=False,
        requires_approval=False,
        access_check=_can_view_purchases,
        handler=_handle_purchase_requests,
        argument_schema={
            "type": "object",
            "properties": {
                "estatus": {"type": "string"},
                "area": {"type": "string"},
                "q": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
            },
        },
        result_contract={"status": "ok", "payload": {"items": "solicitudes", "returned": "conteo"}},
    ),
    "erp.get_purchase_orders": AIToolDefinition(
        key="erp.get_purchase_orders",
        name="Ordenes de compra",
        description="Consulta ordenes de compra del ERP.",
        operation_type="read",
        data_domain="purchasing",
        branch_scoped=False,
        requires_approval=False,
        access_check=_can_view_purchases,
        handler=_handle_purchase_orders,
        argument_schema={
            "type": "object",
            "properties": {
                "estatus": {"type": "string"},
                "q": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
            },
        },
        result_contract={"status": "ok", "payload": {"items": "ordenes", "returned": "conteo"}},
    ),
    "erp.get_recipe_cost_history": AIToolDefinition(
        key="erp.get_recipe_cost_history",
        name="Costo historico de receta",
        description="Consulta versiones historicas de costo de una receta.",
        operation_type="analyze",
        data_domain="costing",
        branch_scoped=False,
        requires_approval=False,
        access_check=_can_view_recipe_costs,
        handler=_handle_recipe_cost_history,
        argument_schema={
            "type": "object",
            "required": ["receta_id"],
            "properties": {
                "receta_id": {"type": "integer"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10},
            },
        },
        result_contract={"status": "ok", "payload": {"receta": "nombre", "items": "versiones", "returned": "conteo"}},
    ),
    "erp.trigger_sync_jobs": AIToolDefinition(
        key="erp.trigger_sync_jobs",
        name="Disparo seguro de sync jobs",
        description="Acción segura candidata para refresh de sync jobs; requiere aprobación humana en piloto.",
        operation_type="execute_safe_action",
        data_domain="integrations",
        branch_scoped=False,
        requires_approval=True,
        access_check=_can_view_sync_jobs,
        handler=_handle_trigger_sync_jobs,
        execute_handler=_execute_trigger_sync_jobs,
        argument_schema={
            "type": "object",
            "properties": {
                "job_type": {"type": "string", "enum": ["inventory", "sales", "recipes"], "default": "inventory"},
                "branch_filter": {"type": "string"},
                "branch": {"type": "string"},
                "days": {"type": "integer", "minimum": 1, "maximum": 30, "default": 3},
                "lag_days": {"type": "integer", "minimum": 0, "maximum": 7, "default": 1},
            },
        },
        result_contract={"status": "approval_required|ok", "payload": "job lanzado o solicitud de aprobacion"},
    ),
    "erp.create_purchase_request_draft": AIToolDefinition(
        key="erp.create_purchase_request_draft",
        name="Crear borrador de solicitud de compra",
        description="Genera una SolicitudCompra en BORRADOR tras aprobacion humana.",
        operation_type="execute_safe_action",
        data_domain="purchasing",
        branch_scoped=False,
        requires_approval=True,
        access_check=_can_manage_purchases,
        handler=_handle_create_purchase_request_draft,
        execute_handler=_execute_create_purchase_request_draft,
        argument_schema={
            "type": "object",
            "required": ["insumo_id", "cantidad"],
            "properties": {
                "area": {"type": "string", "default": "Compras"},
                "solicitante": {"type": "string"},
                "insumo_id": {"type": "integer"},
                "cantidad": {"type": "number", "exclusiveMinimum": 0},
                "fecha_requerida": {"type": "string", "format": "date"},
            },
        },
        result_contract={"status": "approval_required|ok", "payload": "solicitud de compra creada en borrador"},
    ),
    "erp.create_production_plan_draft": AIToolDefinition(
        key="erp.create_production_plan_draft",
        name="Crear borrador de plan de produccion",
        description="Genera un PlanProduccion en BORRADOR tras aprobacion humana.",
        operation_type="execute_safe_action",
        data_domain="production",
        branch_scoped=False,
        requires_approval=True,
        access_check=_can_manage_production,
        handler=_handle_create_production_plan_draft,
        execute_handler=_execute_create_production_plan_draft,
        argument_schema={
            "type": "object",
            "required": ["items"],
            "properties": {
                "nombre": {"type": "string"},
                "fecha_produccion": {"type": "string", "format": "date"},
                "notas": {"type": "string"},
                "items": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["receta_id", "cantidad"],
                        "properties": {
                            "receta_id": {"type": "integer"},
                            "cantidad": {"type": "number", "exclusiveMinimum": 0},
                            "notas": {"type": "string"},
                        },
                    },
                },
            },
        },
        result_contract={"status": "approval_required|ok", "payload": "plan de produccion creado en borrador"},
    ),
}


def list_allowed_tools(user) -> list[dict[str, Any]]:
    allowed = []
    for tool in TOOLS.values():
        if tool.access_check(user):
            allowed.append(_serialize_tool_definition(tool))
    return sorted(allowed, key=lambda item: (item["operation_type"], item["key"]))


def get_tool_definition(*, user, tool_key: str) -> dict[str, Any]:
    tool = TOOLS.get(tool_key)
    if tool is None:
        raise PermissionDenied("Herramienta no registrada en el ERP AI Gateway.")
    if not tool.access_check(user):
        raise PermissionDenied("No tienes permisos para consultar esta herramienta del ERP AI Gateway.")
    return _serialize_tool_definition(tool)


OPENAPI_TOOL_PROFILES: dict[str, dict[str, Any]] = {
    "dg": {
        "tool_keys": sorted(TOOLS.keys()),
        "include_approval_workflow": True,
    },
    "compras": {
        "tool_keys": [
            "erp.get_inventory_low_stock",
            "erp.get_current_input_cost",
            "erp.get_purchase_requests",
            "erp.get_purchase_orders",
            "erp.get_recipe_cost_history",
            "erp.create_purchase_request_draft",
        ],
        "include_approval_workflow": False,
    },
    "produccion": {
        "tool_keys": [
            "erp.get_sales_summary",
            "erp.get_sales_trends",
            "erp.get_inventory_low_stock",
            "erp.get_current_input_cost",
            "erp.analyze_promotion_profitability",
            "erp.get_recipe_cost_history",
            "erp.create_production_plan_draft",
        ],
        "include_approval_workflow": False,
    },
    "auditoria": {
        "tool_keys": [
            "erp.get_audit_logs",
            "erp.get_discrepancies",
            "erp.get_sync_jobs",
        ],
        "include_approval_workflow": False,
    },
}


def resolve_openapi_scope(
    *,
    user,
    profile: str = "",
    requested_tool_keys: set[str] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    profile_normalized = (profile or "").strip().lower()
    allowed_tools = list_allowed_tools(user)
    allowed_map = {tool["key"]: tool for tool in allowed_tools}

    selected_keys = set(allowed_map.keys())
    include_approval_workflow = has_any_role(user, ROLE_DG, ROLE_ADMIN)

    if profile_normalized:
        profile_config = OPENAPI_TOOL_PROFILES.get(profile_normalized)
        if profile_config is None:
            raise ValueError("profile inválido para OpenAPI del ERP AI Gateway.")
        selected_keys &= set(profile_config["tool_keys"])
        include_approval_workflow = bool(profile_config.get("include_approval_workflow", include_approval_workflow))

    if requested_tool_keys:
        selected_keys &= set(requested_tool_keys)

    tools = [allowed_map[key] for key in sorted(selected_keys) if key in allowed_map]
    return tools, include_approval_workflow


def build_gateway_manifest(*, user) -> dict[str, Any]:
    return {
        "gateway": {
            "name": "pollyana_erp_ai_gateway",
            "display_name": "Pollyana ERP AI Gateway",
            "version": "v1",
            "source_of_truth": "ERP Django/PostgreSQL",
            "approval_model": "human_in_the_loop",
            "branch_scope_mode": "enforced_by_user_profile",
        },
        "auth": {
            "type": "token",
            "required": True,
            "me_endpoint_name": "api_auth_me",
            "token_endpoint_name": "api_auth_token",
        },
        "approval_workflow": {
            "required_for_execute_safe_action": True,
            "states": ["PENDING", "APPROVED", "REJECTED", "EXECUTED"],
        },
        "tools": list_allowed_tools(user),
    }


def _build_openapi_request_schema(tool: dict[str, Any], *, approval: bool) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            "arguments": tool.get("argument_schema") or {"type": "object", "properties": {}},
        },
    }
    if approval:
        schema["properties"]["summary"] = {
            "type": "string",
            "description": "Resumen corto visible para aprobacion humana.",
        }
        schema["properties"]["rationale"] = {
            "type": "string",
            "description": "Motivo o justificacion operativa para la accion solicitada.",
        }
    return schema


def _build_openapi_operation(*, request, tool: dict[str, Any], approval: bool) -> dict[str, Any]:
    action_suffix = "request_approval" if approval else "invoke"
    operation_id = f"{tool['key'].replace('.', '_')}_{action_suffix}"
    response_status = "201" if approval else "200"
    description = tool["description"]
    if approval:
        description += " Esta operacion no ejecuta la accion final; crea una solicitud de aprobacion humana."
    elif tool.get("requires_approval"):
        description += " Si la politica exige aprobacion, usa primero el endpoint request-approval."

    return {
        "tags": [f"ai-gateway-{tool['data_domain']}"],
        "operationId": operation_id,
        "summary": tool["name"] if not approval else f"{tool['name']} (solicitar aprobacion)",
        "description": description,
        "security": [{"TokenAuth": []}],
        "requestBody": {
            "required": False,
            "content": {
                "application/json": {
                    "schema": _build_openapi_request_schema(tool, approval=approval),
                }
            },
        },
        "responses": {
            response_status: {
                "description": "Respuesta estandar del ERP AI Gateway.",
                "content": {"application/json": {"schema": {"type": "object"}}},
            },
            "403": {
                "description": "Usuario sin permisos para la herramienta o fuera de alcance RBAC/sucursal.",
                "content": {"application/json": {"schema": {"type": "object"}}},
            },
        },
    }


def build_gateway_openapi_spec(
    *,
    user,
    request,
    profile: str = "",
    requested_tool_keys: set[str] | None = None,
) -> dict[str, Any]:
    tools, include_approval_workflow = resolve_openapi_scope(
        user=user,
        profile=profile,
        requested_tool_keys=requested_tool_keys,
    )
    server_url = settings.AI_GATEWAY_OPENAPI_SERVER_URL or request.build_absolute_uri("/").rstrip("/")
    paths: dict[str, Any] = {
        "/api/ai-gateway/manifest/": {
            "get": {
                "tags": ["ai-gateway-meta"],
                "operationId": "erp_ai_gateway_manifest",
                "summary": "Manifest del ERP AI Gateway",
                "description": "Describe herramientas, auth y workflow de aprobacion disponibles para el usuario autenticado.",
                "security": [{"TokenAuth": []}],
                "responses": {
                    "200": {
                        "description": "Manifest del gateway.",
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    }
                },
            }
        },
    }

    if include_approval_workflow:
        paths["/api/ai-gateway/approvals/"] = {
            "get": {
                "tags": ["ai-gateway-approvals"],
                "operationId": "erp_ai_gateway_list_pending_approvals",
                "summary": "Listar aprobaciones pendientes",
                "description": "Devuelve solicitudes pendientes para acciones seguras. Requiere rol DG o admin.",
                "security": [{"TokenAuth": []}],
                "responses": {
                    "200": {
                        "description": "Listado de solicitudes pendientes.",
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "403": {
                        "description": "Sin permisos para revisar aprobaciones.",
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                },
            }
        }
        paths["/api/ai-gateway/approvals/{suggestion_id}/approve/"] = {
            "post": {
                "tags": ["ai-gateway-approvals"],
                "operationId": "erp_ai_gateway_approve",
                "summary": "Aprobar solicitud pendiente",
                "description": "Aprueba una solicitud de accion segura previamente creada.",
                "security": [{"TokenAuth": []}],
                "parameters": [
                    {
                        "name": "suggestion_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer"},
                    }
                ],
                "requestBody": {
                    "required": False,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"comment": {"type": "string"}},
                            }
                        }
                    },
                },
                "responses": {
                    "200": {"description": "Solicitud aprobada.", "content": {"application/json": {"schema": {"type": "object"}}}},
                    "403": {"description": "Sin permisos o estado invalido.", "content": {"application/json": {"schema": {"type": "object"}}}},
                },
            }
        }
        paths["/api/ai-gateway/approvals/{suggestion_id}/reject/"] = {
            "post": {
                "tags": ["ai-gateway-approvals"],
                "operationId": "erp_ai_gateway_reject",
                "summary": "Rechazar solicitud pendiente",
                "description": "Rechaza una solicitud de accion segura previamente creada.",
                "security": [{"TokenAuth": []}],
                "parameters": [
                    {
                        "name": "suggestion_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer"},
                    }
                ],
                "requestBody": {
                    "required": False,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"comment": {"type": "string"}},
                            }
                        }
                    },
                },
                "responses": {
                    "200": {"description": "Solicitud rechazada.", "content": {"application/json": {"schema": {"type": "object"}}}},
                    "403": {"description": "Sin permisos o estado invalido.", "content": {"application/json": {"schema": {"type": "object"}}}},
                },
            }
        }
        paths["/api/ai-gateway/approvals/{suggestion_id}/execute/"] = {
            "post": {
                "tags": ["ai-gateway-approvals"],
                "operationId": "erp_ai_gateway_execute_approved",
                "summary": "Ejecutar solicitud aprobada",
                "description": "Ejecuta una accion segura que ya fue aprobada por un humano.",
                "security": [{"TokenAuth": []}],
                "parameters": [
                    {
                        "name": "suggestion_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer"},
                    }
                ],
                "responses": {
                    "200": {"description": "Solicitud ejecutada.", "content": {"application/json": {"schema": {"type": "object"}}}},
                    "403": {"description": "Sin permisos o estado invalido.", "content": {"application/json": {"schema": {"type": "object"}}}},
                },
            }
        }

    for tool in tools:
        invoke_path = f"/api/ai-gateway/tools/{tool['key']}/invoke/"
        paths[invoke_path] = {"post": _build_openapi_operation(request=request, tool=tool, approval=False)}
        if tool.get("requires_approval"):
            approval_path = f"/api/ai-gateway/tools/{tool['key']}/request-approval/"
            paths[approval_path] = {"post": _build_openapi_operation(request=request, tool=tool, approval=True)}

    return {
        "openapi": "3.0.3",
        "info": {
            "title": "Pollyana ERP AI Gateway",
            "version": "1.0.0",
            "description": (
                "Contrato OpenAPI para importar acciones del ERP dentro del chat nativo. "
                "El ERP sigue siendo la unica fuente de verdad y las acciones seguras "
                "requieren aprobacion humana cuando aplique."
            ),
        },
        "servers": [{"url": server_url}],
        "components": {
            "securitySchemes": {
                "TokenAuth": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "Authorization",
                    "description": "Usa el header `Authorization: Token <api_token>`.",
                }
            }
        },
        "security": [{"TokenAuth": []}],
        "paths": paths,
    }


def invoke_tool(*, user, tool_key: str, arguments: dict[str, Any]) -> dict[str, Any]:
    tool = TOOLS.get(tool_key)
    if tool is None:
        raise PermissionDenied("Herramienta no registrada en el ERP AI Gateway.")
    if not tool.access_check(user):
        raise PermissionDenied("No tienes permisos para usar esta herramienta del ERP AI Gateway.")

    safe_arguments = dict(arguments or {})
    scope = _enforce_branch_scope(user, branch_scoped=tool.branch_scoped, arguments=safe_arguments)
    response = _tool_response(tool=tool, scope=scope, result=tool.handler(user, safe_arguments))
    log_event(
        user,
        "AI_GATEWAY_TOOL_INVOKE",
        "api.ai_gateway",
        tool.key,
        {
            "tool_key": tool.key,
            "operation_type": tool.operation_type,
            "data_domain": tool.data_domain,
            "requires_approval": tool.requires_approval,
            "scope": scope,
            "arguments": safe_arguments,
            "result_status": response["result"].get("status"),
        },
    )
    return response


def request_tool_approval(*, user, tool_key: str, arguments: dict[str, Any], summary: str = "", rationale: str = "") -> dict[str, Any]:
    tool = TOOLS.get(tool_key)
    if tool is None:
        raise PermissionDenied("Herramienta no registrada en el ERP AI Gateway.")
    if not tool.requires_approval:
        raise PermissionDenied("Esta herramienta no requiere aprobacion previa.")
    if not tool.access_check(user):
        raise PermissionDenied("No tienes permisos para solicitar aprobacion para esta herramienta.")

    safe_arguments = dict(arguments or {})
    scope = _enforce_branch_scope(user, branch_scoped=tool.branch_scoped, arguments=safe_arguments)
    agent = _ensure_ai_gateway_agent()
    run = OrchestrationRun.objects.create(
        run_key=f"ai-gateway-{uuid4().hex[:16]}",
        trigger_source="ai_gateway_approval_request",
        status=OrchestrationRun.STATUS_SUCCESS,
        context_json={
            "tool_key": tool.key,
            "arguments": safe_arguments,
            "scope": scope,
            "requested_by": user.username,
        },
        result_summary_json={"status": "approval_requested"},
        created_by=user,
    )
    task = AgentTask.objects.create(
        run=run,
        agent=agent,
        title=f"Aprobacion requerida: {tool.name}",
        task_type="ai_gateway_approval",
        priority=AgentTask.PRIORITY_HIGH,
        status=AgentTask.STATUS_PENDING,
        input_payload={"tool_key": tool.key, "arguments": safe_arguments, "scope": scope},
        assigned_branch_id=scope.get("branch_id"),
    )
    suggestion = AgentSuggestion.objects.create(
        task=task,
        suggestion_type="ai_gateway_execute_safe_action",
        domain=tool.data_domain,
        severity=AgentSuggestion.SEVERITY_WARNING,
        summary=(summary or f"Solicitud de aprobacion para {tool.name}")[:255],
        details_json={
            "tool_key": tool.key,
            "tool_name": _safe_integration_name(tool.key, fallback="erp_tool"),
            "tool_display_name": tool.name,
            "arguments": safe_arguments,
            "scope": scope,
            "rationale": rationale,
        },
        recommended_action=f"Aprobar o rechazar ejecucion segura de {tool.name}",
        requires_approval=True,
        decision_status=AgentSuggestion.DECISION_PENDING,
    )
    execution = AgentExecutionLink.objects.create(
        suggestion=suggestion,
        execution_mode="ai_gateway_tool",
        target_reference=tool.key,
        execution_status=AgentExecutionLink.STATUS_PENDING,
        execution_payload={"arguments": safe_arguments, "scope": scope},
    )
    log_event(
        user,
        "AI_GATEWAY_APPROVAL_REQUEST",
        "orquestacion.AgentSuggestion",
        suggestion.id,
        {
            "tool_key": tool.key,
            "task_id": task.id,
            "run_key": run.run_key,
            "execution_link_id": execution.id,
            "arguments": safe_arguments,
            "scope": scope,
        },
    )
    return {
        "status": "pending_approval",
        "approval": {
            "suggestion_id": suggestion.id,
            "execution_link_id": execution.id,
            "decision_status": suggestion.decision_status,
            "summary": suggestion.summary,
            "tool_key": tool.key,
            "tool_name": tool.name,
            "requires_approval": True,
        },
    }


def list_pending_approvals(*, user) -> dict[str, Any]:
    if not has_any_role(user, ROLE_DG, ROLE_ADMIN):
        raise PermissionDenied("No tienes permisos para consultar aprobaciones del AI Gateway.")
    rows = (
        AgentSuggestion.objects.select_related("task", "task__run")
        .filter(
            suggestion_type="ai_gateway_execute_safe_action",
            decision_status=AgentSuggestion.DECISION_PENDING,
        )
        .order_by("-created_at", "-id")
    )
    items = []
    for suggestion in rows[:100]:
        details = suggestion.details_json or {}
        items.append(
            {
                "suggestion_id": suggestion.id,
                "summary": suggestion.summary,
                "tool_key": details.get("tool_key", ""),
                "tool_name": details.get("tool_display_name", details.get("tool_name", "")),
                "arguments": details.get("arguments", {}),
                "scope": details.get("scope", {}),
                "rationale": details.get("rationale", ""),
                "requested_by": suggestion.task.run.created_by.username if suggestion.task.run.created_by_id else "",
                "created_at": suggestion.created_at.isoformat(),
            }
        )
    return {"status": "ok", "items": items, "count": len(items)}


def decide_tool_approval(*, user, suggestion_id: int, approve: bool, comment: str = "") -> dict[str, Any]:
    if not has_any_role(user, ROLE_DG, ROLE_ADMIN):
        raise PermissionDenied("No tienes permisos para decidir aprobaciones del AI Gateway.")
    suggestion = AgentSuggestion.objects.select_related("task", "task__run").filter(
        id=suggestion_id,
        suggestion_type="ai_gateway_execute_safe_action",
    ).first()
    if suggestion is None:
        raise PermissionDenied("Solicitud de aprobacion no encontrada.")
    if suggestion.decision_status != AgentSuggestion.DECISION_PENDING:
        raise PermissionDenied("La solicitud ya fue decidida y no puede modificarse.")

    now = timezone.now()
    details = dict(suggestion.details_json or {})
    details["decision_comment"] = comment
    if approve:
        suggestion.decision_status = AgentSuggestion.DECISION_APPROVED
        suggestion.approved_by = user
        suggestion.approved_at = now
    else:
        suggestion.decision_status = AgentSuggestion.DECISION_REJECTED
        suggestion.rejected_by = user
        suggestion.rejected_at = now
    suggestion.details_json = details
    suggestion.save(
        update_fields=[
            "decision_status",
            "approved_by",
            "approved_at",
            "rejected_by",
            "rejected_at",
            "details_json",
            "updated_at",
        ]
    )
    log_event(
        user,
        "AI_GATEWAY_APPROVAL_DECISION",
        "orquestacion.AgentSuggestion",
        suggestion.id,
        {
            "decision": suggestion.decision_status,
            "tool_key": details.get("tool_key"),
            "comment": comment,
        },
    )
    return {
        "status": "ok",
        "suggestion_id": suggestion.id,
        "decision_status": suggestion.decision_status,
        "approved_by": suggestion.approved_by.username if suggestion.approved_by_id else "",
        "rejected_by": suggestion.rejected_by.username if suggestion.rejected_by_id else "",
    }


def execute_approved_tool(*, user, suggestion_id: int) -> dict[str, Any]:
    if not has_any_role(user, ROLE_DG, ROLE_ADMIN):
        raise PermissionDenied("No tienes permisos para ejecutar acciones aprobadas del AI Gateway.")
    suggestion = AgentSuggestion.objects.select_related("task", "task__run").filter(
        id=suggestion_id,
        suggestion_type="ai_gateway_execute_safe_action",
    ).first()
    if suggestion is None:
        raise PermissionDenied("Solicitud aprobada no encontrada.")
    if suggestion.decision_status != AgentSuggestion.DECISION_APPROVED:
        raise PermissionDenied("La solicitud no esta aprobada para ejecucion.")

    tool_key = str((suggestion.details_json or {}).get("tool_key") or "").strip()
    tool = TOOLS.get(tool_key)
    if tool is None or tool.execute_handler is None:
        raise PermissionDenied("La herramienta aprobada no tiene ejecutor configurado.")

    executions = suggestion.executions.order_by("-created_at", "-id")
    execution = executions.first()
    if execution is None:
        execution = AgentExecutionLink.objects.create(
            suggestion=suggestion,
            execution_mode="ai_gateway_tool",
            target_reference=tool.key,
            execution_status=AgentExecutionLink.STATUS_PENDING,
            execution_payload={},
        )
    if execution.execution_status == AgentExecutionLink.STATUS_SUCCESS:
        raise PermissionDenied("La solicitud aprobada ya fue ejecutada.")

    arguments = dict((suggestion.details_json or {}).get("arguments") or {})
    if suggestion.task.run.created_by_id:
        arguments.setdefault("_approval_requested_by", suggestion.task.run.created_by.username)
    execution.execution_status = AgentExecutionLink.STATUS_RUNNING
    execution.executed_by = user
    execution.executed_at = timezone.now()
    execution.execution_payload = {"arguments": arguments}
    execution.save(update_fields=["execution_status", "executed_by", "executed_at", "execution_payload", "updated_at"])

    try:
        result = tool.execute_handler(user, arguments)
    except Exception as exc:
        execution.execution_status = AgentExecutionLink.STATUS_FAILED
        execution.execution_payload = {"arguments": arguments, "error": str(exc)}
        execution.save(update_fields=["execution_status", "execution_payload", "updated_at"])
        log_event(
            user,
            "AI_GATEWAY_APPROVAL_EXECUTE_FAILED",
            "orquestacion.AgentExecutionLink",
            execution.id,
            {"tool_key": tool.key, "suggestion_id": suggestion.id, "error": str(exc)},
        )
        raise

    suggestion.decision_status = AgentSuggestion.DECISION_EXECUTED
    suggestion.save(update_fields=["decision_status", "updated_at"])
    execution.execution_status = AgentExecutionLink.STATUS_SUCCESS
    execution.execution_payload = {"arguments": arguments, "result": result}
    execution.save(update_fields=["execution_status", "execution_payload", "updated_at"])
    log_event(
        user,
        "AI_GATEWAY_APPROVAL_EXECUTE",
        "orquestacion.AgentExecutionLink",
        execution.id,
        {"tool_key": tool.key, "suggestion_id": suggestion.id, "result_status": result.get("status")},
    )
    return {
        "status": "ok",
        "suggestion_id": suggestion.id,
        "decision_status": suggestion.decision_status,
        "execution_link_id": execution.id,
        "tool_key": tool.key,
        "result": result,
    }
