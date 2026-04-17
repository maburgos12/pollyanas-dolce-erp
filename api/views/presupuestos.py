import csv
import json
from collections import defaultdict
from contextlib import nullcontext
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any
from django.db import transaction, OperationalError, ProgrammingError
from django.db.models import Count, Max, Q, Sum
from django.db.models.functions import TruncDate
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone
from openpyxl import Workbook
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.views import ObtainAuthToken

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento
from control.models import MermaPOS, VentaPOS
from control.services import build_discrepancias_report, resolve_period_range
from compras.models import OrdenCompra, RecepcionCompra, SolicitudCompra
from core.access import (
    ROLE_ADMIN,
    ROLE_COMPRAS,
    ROLE_DG,
    can_manage_compras,
    can_manage_inventario,
    can_view_audit,
    can_view_compras,
    can_view_inventario,
    can_view_maestros,
    can_view_reportes,
    has_any_role,
)
from core.audit import log_event
from core.models import AuditLog, Sucursal
from compras.views import (
    _apply_recepcion_to_inventario,
    _active_solicitud_statuses,
    _build_budget_context,
    _build_budget_history,
    _build_category_dashboard,
    _can_transition_orden,
    _can_transition_recepcion,
    _can_transition_solicitud,
    _build_consumo_vs_plan_dashboard,
    _default_fecha_requerida,
    _parse_date_value,
    _build_provider_dashboard,
    _filtered_solicitudes,
    _resolve_proveedor_name,
    _sanitize_consumo_ref_filter,
)
from integraciones.models import PublicApiAccessLog, PublicApiClient
from integraciones.views import _deactivate_idle_api_clients, _purge_api_logs
from inventario.models import AjusteInventario, AlmacenSyncRun, ExistenciaInsumo
from inventario.views import (
    _apply_ajuste,
    _apply_cross_filters,
    _build_cross_unified_rows,
    _export_cross_pending_csv,
    _export_cross_pending_xlsx,
    _build_pending_grouped,
    _resolve_cross_source_with_alias,
)
from maestros.models import CostoInsumo, Insumo, InsumoAlias, PointPendingMatch, Proveedor
from maestros.utils.canonical_catalog import canonical_insumo, canonical_insumo_by_id, canonicalized_active_insumos, latest_costo_canonico
from recetas.models import (
    LineaReceta,
    PlanProduccion,
    PlanProduccionItem,
    PronosticoVenta,
    Receta,
    RecetaCodigoPointAlias,
    SolicitudVenta,
    VentaHistorica,
    normalizar_codigo_point,
)
from recetas.views import (
    _build_forecast_backtest_preview,
    _build_forecast_from_history,
    _filter_forecast_result_by_confianza,
    _forecast_session_payload,
    _forecast_vs_solicitud_preview,
    _normalize_periodo_mes,
    _resolve_receta_for_sales,
    _resolve_solicitud_window,
    _resolve_sucursal_for_sales,
    _ui_to_model_alcance,
)
from recetas.utils.normalizacion import normalizar_nombre
from recetas.utils.matching import match_insumo
from recetas.utils.costeo_versionado import asegurar_version_costeo, comparativo_versiones
from ..serializers import (
    ComprasSolicitudImportConfirmSerializer,
    ComprasSolicitudImportPreviewSerializer,
    ComprasCrearOrdenSerializer,
    ComprasOrdenStatusSerializer,
    ComprasRecepcionCreateSerializer,
    ComprasRecepcionStatusSerializer,
    ComprasSolicitudCreateSerializer,
    ComprasSolicitudStatusSerializer,
    ControlMermaPosBulkSerializer,
    ControlVentaPosBulkSerializer,
    ActivosOrdenCreateSerializer,
    ActivosOrdenStatusSerializer,
    ForecastBacktestRequestSerializer,
    ForecastEstadisticoGuardarSerializer,
    ForecastEstadisticoRequestSerializer,
    IntegracionesDeactivateIdleClientsSerializer,
    IntegracionesMaintenanceRunSerializer,
    IntegracionesOperationHistoryQuerySerializer,
    IntegracionesPurgeApiLogsSerializer,
    InventarioAjusteCreateSerializer,
    InventarioAjusteDecisionSerializer,
    InventarioAliasCreateSerializer,
    InventarioCrossPendientesResolveSerializer,
    InventarioAliasMassReassignSerializer,
    MasterDuplicatesSerializer,
    MasterNormalizeSerializer,
    InventarioPointPendingResolveSerializer,
    MRPRequestSerializer,
    MRPRequerimientosRequestSerializer,
    PlanProduccionCreateSerializer,
    PlanProduccionItemCreateSerializer,
    PlanProduccionItemUpdateSerializer,
    PlanProduccionUpdateSerializer,
    PlanDesdePronosticoRequestSerializer,
    PronosticoVentaBulkSerializer,
    RecetaCostoVersionSerializer,
    SolicitudVentaAplicarForecastSerializer,
    SolicitudVentaBulkSerializer,
    SolicitudVentaUpsertSerializer,
    VentaHistoricaBulkSerializer,
)


def _load_versiones_costeo(receta: Receta, limit: int):
    return list(receta.versiones_costo.order_by("-version_num")[:limit])


def _to_decimal(value, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _canonical_member_ids(insumo_id: int | str | None) -> tuple[Insumo | None, list[int]]:
    canonical = canonical_insumo_by_id(insumo_id)
    if canonical is None:
        return None, []
    for row in canonicalized_active_insumos(limit=5000):
        if canonical.id in row["member_ids"]:
            return canonical, list(row["member_ids"])
    return canonical, [canonical.id]


def _latest_cost_for_canonical(insumo_id: int | str | None, proveedor: Proveedor | None = None) -> tuple[Insumo | None, CostoInsumo | None]:
    canonical, member_ids = _canonical_member_ids(insumo_id)
    if canonical is None:
        return None, None
    costo_qs = CostoInsumo.objects.filter(insumo_id__in=member_ids).order_by("-fecha", "-id")
    if proveedor is not None:
        preferred = costo_qs.filter(proveedor=proveedor).first()
        if preferred is not None:
            return canonical, preferred
    return canonical, costo_qs.first()


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "si", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_period(period_raw: str | None) -> tuple[int, int] | None:
    if not period_raw:
        return None
    raw = str(period_raw).strip()
    parts = raw.split("-")
    if len(parts) != 2:
        return None
    try:
        year = int(parts[0])
        month = int(parts[1])
    except ValueError:
        return None
    if year < 2000 or year > 2200 or month < 1 or month > 12:
        return None
    return year, month


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _parse_bounded_int(raw_value, *, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = default
    return max(min_value, min(value, max_value))


def _to_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError, InvalidOperation):
        return default


def _pct_change(current: int, previous: int) -> float:
    current_i = int(current or 0)
    previous_i = int(previous or 0)
    if previous_i <= 0:
        return 100.0 if current_i > 0 else 0.0
    return round(((current_i - previous_i) * 100.0 / previous_i), 2)


def _build_public_api_daily_trend(days: int = 7) -> list[dict[str, Any]]:
    days = max(1, min(int(days or 7), 31))
    today = timezone.localdate()
    start_date = today - timedelta(days=days - 1)
    raw_rows = list(
        PublicApiAccessLog.objects.filter(created_at__date__gte=start_date)
        .annotate(day=TruncDate("created_at"))
        .values("day")
        .annotate(
            total=Count("id"),
            errors=Count("id", filter=Q(status_code__gte=400)),
        )
        .order_by("day")
    )
    by_day = {row["day"]: row for row in raw_rows}
    trend = []
    for index in range(days):
        day = start_date + timedelta(days=index)
        row = by_day.get(day, {})
        total = int(row.get("total") or 0)
        errors = int(row.get("errors") or 0)
        trend.append(
            {
                "day": day,
                "requests": total,
                "errors": errors,
                "error_rate_pct": round((errors * 100.0 / total), 2) if total else 0.0,
            }
        )
    return trend


def _preview_deactivate_idle_api_clients(idle_days: int, limit: int) -> dict[str, Any]:
    idle_days = max(1, min(int(idle_days or 30), 365))
    limit = max(1, min(int(limit or 100), 500))
    cutoff = timezone.now() - timedelta(days=idle_days)
    recent_client_ids = set(
        PublicApiAccessLog.objects.filter(created_at__gte=cutoff)
        .values_list("client_id", flat=True)
        .distinct()
    )
    candidates = list(
        PublicApiClient.objects.filter(activo=True)
        .exclude(id__in=recent_client_ids)
        .order_by("id")
        .values("id", "nombre")[:limit]
    )
    return {
        "idle_days": idle_days,
        "limit": limit,
        "candidates": len(candidates),
        "deactivated": 0,
        "cutoff": cutoff.isoformat(),
        "candidate_clients": candidates,
        "dry_run": True,
    }


def _preview_purge_api_logs(retain_days: int, max_delete: int) -> dict[str, Any]:
    retain_days = max(1, min(int(retain_days or 90), 3650))
    max_delete = max(1, min(int(max_delete or 5000), 50000))
    cutoff = timezone.now() - timedelta(days=retain_days)
    total_candidates = PublicApiAccessLog.objects.filter(created_at__lt=cutoff).count()
    return {
        "retain_days": retain_days,
        "max_delete": max_delete,
        "cutoff": cutoff.isoformat(),
        "candidates": int(total_candidates),
        "deleted": 0,
        "remaining_candidates": int(total_candidates),
        "would_delete": min(int(total_candidates), max_delete),
        "dry_run": True,
    }



class PresupuestosConsolidadoView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, periodo: str):
        if not can_view_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar presupuestos de compras."},
                status=status.HTTP_403_FORBIDDEN,
            )

        parsed = _parse_period(periodo)
        if not parsed:
            return Response(
                {"detail": "Periodo inválido. Usa formato YYYY-MM."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        year, month = parsed
        periodo_mes = f"{year:04d}-{month:02d}"
        periodo_tipo = (request.GET.get("periodo_tipo") or "mes").strip().lower()
        if periodo_tipo not in {"mes", "q1", "q2"}:
            periodo_tipo = "mes"

        source_raw = request.GET.get("source")
        plan_raw = request.GET.get("plan_id")
        categoria_raw = request.GET.get("categoria")
        reabasto_raw = request.GET.get("reabasto")

        (
            solicitudes,
            source_filter,
            plan_filter,
            categoria_filter,
            reabasto_filter,
            _,
            _periodo_tipo,
            _periodo_mes,
            periodo_label,
        ) = _filtered_solicitudes(
            source_raw,
            plan_raw,
            categoria_raw,
            reabasto_raw,
            periodo_tipo,
            periodo_mes,
        )
        consumo_ref_filter = _sanitize_consumo_ref_filter(request.GET.get("consumo_ref"))
        consumo_limit = _parse_bounded_int(request.GET.get("limit", 30), default=30, min_value=1, max_value=1000)
        consumo_offset = _parse_bounded_int(request.GET.get("offset", 0), default=0, min_value=0, max_value=200000)
        consumo_sort_by = (request.GET.get("sort_by") or "variacion_cost_abs").strip().lower()
        consumo_sort_dir = (request.GET.get("sort_dir") or "desc").strip().lower()
        consumo_allowed_sort = {
            "variacion_cost_abs",
            "variacion_cost",
            "costo_real",
            "costo_plan",
            "cantidad_real",
            "cantidad_plan",
            "consumo_pct",
            "insumo",
            "categoria",
            "estado",
            "semaforo",
        }
        if consumo_sort_by not in consumo_allowed_sort:
            return Response(
                {
                    "detail": (
                        "sort_by inválido. Usa variacion_cost_abs, variacion_cost, costo_real, costo_plan, "
                        "cantidad_real, cantidad_plan, consumo_pct, insumo, categoria, estado o semaforo."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        if consumo_sort_dir not in {"asc", "desc"}:
            return Response(
                {"detail": "sort_dir inválido. Usa asc o desc."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        budget_ctx = _build_budget_context(
            solicitudes,
            source_filter,
            plan_filter,
            categoria_filter,
            _periodo_tipo,
            _periodo_mes,
        )
        provider_dashboard = _build_provider_dashboard(
            _periodo_mes,
            source_filter,
            plan_filter,
            categoria_filter,
            budget_ctx["presupuesto_rows_proveedor"],
        )
        category_dashboard = _build_category_dashboard(
            _periodo_mes,
            source_filter,
            plan_filter,
            categoria_filter,
            budget_ctx.get("presupuesto_rows_categoria", []),
        )
        consumo_dashboard = _build_consumo_vs_plan_dashboard(
            _periodo_tipo,
            _periodo_mes,
            source_filter,
            plan_filter,
            categoria_filter,
            consumo_ref_filter,
            limit=consumo_limit,
            offset=consumo_offset,
            sort_by=consumo_sort_by,
            sort_dir=consumo_sort_dir,
        )
        historial = _build_budget_history(_periodo_mes, source_filter, plan_filter, categoria_filter)

        payload = {
            "periodo": {
                "path": periodo,
                "tipo": _periodo_tipo,
                "mes": _periodo_mes,
                "label": periodo_label,
            },
            "filters": {
                "source": source_filter,
                "plan_id": plan_filter or "",
                "categoria": categoria_filter or "",
                "reabasto": reabasto_filter,
                "consumo_ref": consumo_ref_filter,
                "limit": consumo_limit,
                "offset": consumo_offset,
                "sort_by": consumo_sort_by,
                "sort_dir": consumo_sort_dir,
            },
            "totals": {
                "solicitudes_count": len(solicitudes),
                "presupuesto_estimado_total": _to_float(budget_ctx["presupuesto_estimado_total"]),
                "presupuesto_ejecutado_total": _to_float(budget_ctx["presupuesto_ejecutado_total"]),
                "presupuesto_objetivo": _to_float(budget_ctx.get("presupuesto_objetivo")),
                "presupuesto_variacion_objetivo": _to_float(budget_ctx.get("presupuesto_variacion_objetivo")),
                "alertas_total": int(budget_ctx.get("presupuesto_alertas_total") or 0),
                "alertas_excedidas": int(budget_ctx.get("presupuesto_alertas_excedidas") or 0),
                "alertas_preventivas": int(budget_ctx.get("presupuesto_alertas_preventivas") or 0),
            },
            "alertas": [
                {
                    "nivel": row.get("nivel"),
                    "scope": row.get("scope"),
                    "nombre": row.get("nombre"),
                    "estimado": _to_float(row.get("estimado")),
                    "ejecutado": _to_float(row.get("ejecutado")),
                    "objetivo": _to_float(row.get("objetivo")),
                    "uso_objetivo_pct": _to_float(row.get("uso_objetivo_pct")) if row.get("uso_objetivo_pct") is not None else None,
                    "variacion": _to_float(row.get("variacion")),
                    "estado": row.get("estado"),
                }
                for row in budget_ctx.get("presupuesto_alertas", [])[:100]
            ],
            "proveedores": [
                {
                    "proveedor": row["proveedor"],
                    "estimado": _to_float(row["estimado"]),
                    "ejecutado": _to_float(row["ejecutado"]),
                    "variacion": _to_float(row["variacion"]),
                    "participacion_pct": _to_float(row["participacion_pct"]),
                    "objetivo_proveedor": _to_float(row.get("objetivo_proveedor")),
                    "uso_objetivo_pct": _to_float(row.get("uso_objetivo_pct")) if row.get("uso_objetivo_pct") is not None else None,
                    "objetivo_estado": row.get("objetivo_estado"),
                }
                for row in budget_ctx["presupuesto_rows_proveedor"][:50]
            ],
            "categorias": [
                {
                    "categoria": row["categoria"],
                    "estimado": _to_float(row["estimado"]),
                    "ejecutado": _to_float(row["ejecutado"]),
                    "variacion": _to_float(row["variacion"]),
                    "participacion_pct": _to_float(row["participacion_pct"]),
                    "objetivo_categoria": _to_float(row.get("objetivo_categoria")),
                    "uso_objetivo_pct": _to_float(row.get("uso_objetivo_pct")) if row.get("uso_objetivo_pct") is not None else None,
                    "objetivo_estado": row.get("objetivo_estado"),
                }
                for row in budget_ctx.get("presupuesto_rows_categoria", [])[:50]
            ],
            "historial_6m": [
                {
                    "periodo_mes": row["periodo_mes"],
                    "objetivo": _to_float(row["objetivo"]),
                    "estimado": _to_float(row["estimado"]),
                    "ejecutado": _to_float(row["ejecutado"]),
                    "ratio_pct": _to_float(row["ratio_pct"]) if row.get("ratio_pct") is not None else None,
                    "estado_label": row["estado_label"],
                }
                for row in historial
            ],
            "trend": {
                "proveedor_rows": [
                    {
                        "proveedor": row["proveedor"],
                        "mes": row["mes"],
                        "estimado": _to_float(row["estimado"]),
                        "ejecutado": _to_float(row["ejecutado"]),
                        "variacion": _to_float(row["variacion"]),
                    }
                    for row in provider_dashboard["trend_rows"]
                ],
                "categoria_rows": [
                    {
                        "categoria": row["categoria"],
                        "mes": row["mes"],
                        "estimado": _to_float(row["estimado"]),
                        "ejecutado": _to_float(row["ejecutado"]),
                        "variacion": _to_float(row["variacion"]),
                    }
                    for row in category_dashboard["trend_rows"]
                ],
            },
            "consumo_vs_plan": {
                "meta": {
                    "rows_total": int(consumo_dashboard.get("meta", {}).get("rows_total") or 0),
                    "rows_returned": int(consumo_dashboard.get("meta", {}).get("rows_returned") or 0),
                    "limit": int(consumo_dashboard.get("meta", {}).get("limit") or consumo_limit),
                    "offset": int(consumo_dashboard.get("meta", {}).get("offset") or consumo_offset),
                    "sort_by": consumo_dashboard.get("meta", {}).get("sort_by") or consumo_sort_by,
                    "sort_dir": consumo_dashboard.get("meta", {}).get("sort_dir") or consumo_sort_dir,
                },
                "totals": {
                    "plan_qty_total": _to_float(consumo_dashboard["totals"]["plan_qty_total"]),
                    "consumo_real_qty_total": _to_float(consumo_dashboard["totals"]["consumo_real_qty_total"]),
                    "plan_cost_total": _to_float(consumo_dashboard["totals"]["plan_cost_total"]),
                    "consumo_real_cost_total": _to_float(consumo_dashboard["totals"]["consumo_real_cost_total"]),
                    "variacion_cost_total": _to_float(consumo_dashboard["totals"]["variacion_cost_total"]),
                    "semaforo_rojo_count": int(consumo_dashboard["totals"]["semaforo_rojo_count"] or 0),
                    "semaforo_amarillo_count": int(consumo_dashboard["totals"]["semaforo_amarillo_count"] or 0),
                    "semaforo_verde_count": int(consumo_dashboard["totals"]["semaforo_verde_count"] or 0),
                    "sin_costo_count": int(consumo_dashboard["totals"]["sin_costo_count"] or 0),
                    "cobertura_pct": (
                        _to_float(consumo_dashboard["totals"]["cobertura_pct"])
                        if consumo_dashboard["totals"]["cobertura_pct"] is not None
                        else None
                    ),
                },
                "rows": [
                    {
                        "insumo_id": row["insumo_id"],
                        "insumo": row["insumo"],
                        "categoria": row["categoria"],
                        "unidad": row["unidad"],
                        "cantidad_plan": _to_float(row["cantidad_plan"]),
                        "cantidad_real": _to_float(row["cantidad_real"]),
                        "variacion_qty": _to_float(row["variacion_qty"]),
                        "costo_unitario": _to_float(row["costo_unitario"]),
                        "costo_plan": _to_float(row["costo_plan"]),
                        "costo_real": _to_float(row["costo_real"]),
                        "variacion_cost": _to_float(row["variacion_cost"]),
                        "consumo_pct": _to_float(row["consumo_pct"]) if row["consumo_pct"] is not None else None,
                        "estado": row["estado"],
                        "semaforo": row["semaforo"],
                        "sin_costo": bool(row["sin_costo"]),
                        "alerta": row["alerta"],
                    }
                    for row in consumo_dashboard["rows"]
                ],
            },
        }

        return Response(payload, status=status.HTTP_200_OK)



