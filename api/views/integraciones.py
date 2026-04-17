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



class IntegracionesDeactivateIdleClientsView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not can_view_audit(request.user):
            return Response(
                {"detail": "No tienes permisos para ejecutar operaciones de integración."},
                status=status.HTTP_403_FORBIDDEN,
            )

        serializer = IntegracionesDeactivateIdleClientsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        idle_days = serializer.validated_data["idle_days"]
        limit = serializer.validated_data["limit"]
        dry_run = bool(serializer.validated_data.get("dry_run"))
        summary = (
            _preview_deactivate_idle_api_clients(idle_days=idle_days, limit=limit)
            if dry_run
            else _deactivate_idle_api_clients(idle_days=idle_days, limit=limit)
        )
        if "dry_run" not in summary:
            summary["dry_run"] = dry_run
        log_event(
            request.user,
            "PREVIEW_DEACTIVATE_IDLE_API_CLIENTS" if dry_run else "DEACTIVATE_IDLE_API_CLIENTS",
            "integraciones.PublicApiClient",
            "",
            payload=summary,
        )
        return Response(
            {
                "action": "deactivate_idle_clients",
                "summary": summary,
            },
            status=status.HTTP_200_OK,
        )


class IntegracionesPurgeApiLogsView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not can_view_audit(request.user):
            return Response(
                {"detail": "No tienes permisos para ejecutar operaciones de integración."},
                status=status.HTTP_403_FORBIDDEN,
            )

        serializer = IntegracionesPurgeApiLogsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        retain_days = serializer.validated_data["retain_days"]
        max_delete = serializer.validated_data["max_delete"]
        dry_run = bool(serializer.validated_data.get("dry_run"))
        summary = (
            _preview_purge_api_logs(retain_days=retain_days, max_delete=max_delete)
            if dry_run
            else _purge_api_logs(retain_days=retain_days, max_delete=max_delete)
        )
        if "dry_run" not in summary:
            summary["dry_run"] = dry_run
        log_event(
            request.user,
            "PREVIEW_PURGE_API_LOGS" if dry_run else "PURGE_API_LOGS",
            "integraciones.PublicApiAccessLog",
            "",
            payload=summary,
        )
        return Response(
            {
                "action": "purge_api_logs",
                "summary": summary,
            },
            status=status.HTTP_200_OK,
        )


class IntegracionesMaintenanceRunView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not can_view_audit(request.user):
            return Response(
                {"detail": "No tienes permisos para ejecutar operaciones de integración."},
                status=status.HTTP_403_FORBIDDEN,
            )

        serializer = IntegracionesMaintenanceRunSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        dry_run = bool(data.get("dry_run"))

        deactivate_summary = (
            _preview_deactivate_idle_api_clients(idle_days=data["idle_days"], limit=data["idle_limit"])
            if dry_run
            else _deactivate_idle_api_clients(idle_days=data["idle_days"], limit=data["idle_limit"])
        )
        purge_summary = (
            _preview_purge_api_logs(retain_days=data["retain_days"], max_delete=data["max_delete"])
            if dry_run
            else _purge_api_logs(retain_days=data["retain_days"], max_delete=data["max_delete"])
        )
        payload = {
            "dry_run": dry_run,
            "deactivate_idle_clients": deactivate_summary,
            "purge_api_logs": purge_summary,
        }
        log_event(
            request.user,
            "PREVIEW_RUN_API_MAINTENANCE" if dry_run else "RUN_API_MAINTENANCE",
            "integraciones.Operaciones",
            "",
            payload=payload,
        )
        return Response(payload, status=status.HTTP_200_OK)


class IntegracionesOperationsHistoryView(APIView):
    permission_classes = [IsAuthenticated]

    ACTIONS = {
        "DEACTIVATE_IDLE_API_CLIENTS",
        "PREVIEW_DEACTIVATE_IDLE_API_CLIENTS",
        "PURGE_API_LOGS",
        "PREVIEW_PURGE_API_LOGS",
        "RUN_API_MAINTENANCE",
        "PREVIEW_RUN_API_MAINTENANCE",
    }

    @staticmethod
    def _export_csv(rows: list[AuditLog]) -> HttpResponse:
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="integraciones_operaciones_historial.csv"'
        writer = csv.writer(response)
        writer.writerow(["timestamp", "usuario", "action", "model", "object_id", "payload"])
        for row in rows:
            writer.writerow(
                [
                    row.timestamp.strftime("%Y-%m-%d %H:%M:%S") if row.timestamp else "",
                    row.user.username if row.user_id else "",
                    row.action,
                    row.model,
                    row.object_id,
                    json.dumps(row.payload or {}, ensure_ascii=False),
                ]
            )
        return response

    @staticmethod
    def _export_xlsx(rows: list[AuditLog]) -> HttpResponse:
        wb = Workbook()
        ws = wb.active
        ws.title = "historial"
        ws.append(["timestamp", "usuario", "action", "model", "object_id", "payload"])
        for row in rows:
            ws.append(
                [
                    row.timestamp.strftime("%Y-%m-%d %H:%M:%S") if row.timestamp else "",
                    row.user.username if row.user_id else "",
                    row.action,
                    row.model,
                    row.object_id,
                    json.dumps(row.payload or {}, ensure_ascii=False),
                ]
            )
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        response = HttpResponse(
            output.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = 'attachment; filename="integraciones_operaciones_historial.xlsx"'
        return response

    def get(self, request):
        if not can_view_audit(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar historial operativo de integraciones."},
                status=status.HTTP_403_FORBIDDEN,
            )

        serializer = IntegracionesOperationHistoryQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        action = (data.get("action") or "").strip().upper()
        user_filter = (data.get("user") or "").strip()
        model_filter = (data.get("model") or "").strip()
        q_filter = (data.get("q") or "").strip()
        if action and action not in self.ACTIONS:
            return Response(
                {"detail": "action inválida para historial de integraciones."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        sort_by = (data.get("sort_by") or "timestamp").strip().lower()
        sort_dir = (data.get("sort_dir") or "desc").strip().lower()
        allowed_sort = {
            "timestamp": "timestamp",
            "action": "action",
            "model": "model",
            "object_id": "object_id",
            "user": "user__username",
            "id": "id",
        }
        if sort_by not in allowed_sort:
            return Response(
                {"detail": "sort_by inválido. Usa timestamp, action, model, object_id, user o id."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if sort_dir not in {"asc", "desc"}:
            return Response(
                {"detail": "sort_dir inválido. Usa asc o desc."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        sort_field = allowed_sort[sort_by]
        primary_order = sort_field if sort_dir == "asc" else f"-{sort_field}"
        secondary_order = "id" if sort_dir == "asc" else "-id"

        qs = AuditLog.objects.filter(action__in=self.ACTIONS).select_related("user").order_by(
            primary_order,
            secondary_order,
        )
        if action:
            qs = qs.filter(action=action)
        if user_filter:
            qs = qs.filter(user__username__icontains=user_filter)
        if model_filter:
            qs = qs.filter(model__icontains=model_filter)
        if q_filter:
            qs = qs.filter(
                Q(action__icontains=q_filter)
                | Q(model__icontains=q_filter)
                | Q(object_id__icontains=q_filter)
                | Q(user__username__icontains=q_filter)
            )
        if data.get("date_from"):
            qs = qs.filter(timestamp__date__gte=data["date_from"])
        if data.get("date_to"):
            qs = qs.filter(timestamp__date__lte=data["date_to"])

        limit = int(data.get("limit") or 100)
        offset = int(data.get("offset") or 0)
        export = str(data.get("export") or "").strip().lower()
        rows_total = qs.count()
        rows = list(qs[offset : offset + limit])
        has_next = (offset + len(rows)) < rows_total
        has_prev = offset > 0
        next_offset = offset + limit if has_next else None
        prev_offset = max(offset - limit, 0) if has_prev else None
        by_action = {
            row["action"]: int(row["total"] or 0)
            for row in qs.values("action").annotate(total=Count("id")).order_by("-total", "action")
        }
        if export == "csv":
            return self._export_csv(rows)
        if export == "xlsx":
            return self._export_xlsx(rows)

        return Response(
            {
                "filters": {
                    "action": action or "",
                    "user": user_filter,
                    "model": model_filter,
                    "q": q_filter,
                    "date_from": data.get("date_from"),
                    "date_to": data.get("date_to"),
                    "limit": limit,
                    "offset": offset,
                    "sort_by": sort_by,
                    "sort_dir": sort_dir,
                    "export": export,
                },
                "totales": {
                    "rows_total": rows_total,
                    "rows_returned": len(rows),
                    "by_action": by_action,
                },
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "has_next": has_next,
                    "next_offset": next_offset,
                    "has_prev": has_prev,
                    "prev_offset": prev_offset,
                },
                "items": [
                    {
                        "id": row.id,
                        "timestamp": row.timestamp,
                        "user": row.user.username if row.user_id else "",
                        "action": row.action,
                        "model": row.model,
                        "object_id": row.object_id,
                        "payload": row.payload or {},
                    }
                    for row in rows
                ],
            },
            status=status.HTTP_200_OK,
        )


class IntegracionPointResumenView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_maestros(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar resumen de integración Point."},
                status=status.HTTP_403_FORBIDDEN,
            )

        insumos_activos_qs = Insumo.objects.filter(activo=True)
        insumos_activos = insumos_activos_qs.count()
        insumos_con_codigo = insumos_activos_qs.exclude(Q(codigo_point="") | Q(codigo_point__isnull=True)).count()
        insumos_sin_codigo = max(insumos_activos - insumos_con_codigo, 0)
        insumos_cobertura = round((insumos_con_codigo * 100.0 / insumos_activos), 2) if insumos_activos else 100.0

        recetas_total = Receta.objects.count()
        receta_ids_primary = set(
            Receta.objects.exclude(Q(codigo_point="") | Q(codigo_point__isnull=True)).values_list("id", flat=True)
        )
        receta_ids_alias = set(
            RecetaCodigoPointAlias.objects.filter(activo=True).values_list("receta_id", flat=True)
        )
        recetas_homologadas_ids = receta_ids_primary.union(receta_ids_alias)
        recetas_homologadas = len(recetas_homologadas_ids)
        recetas_sin_homologar = max(recetas_total - recetas_homologadas, 0)
        recetas_cobertura = round((recetas_homologadas * 100.0 / recetas_total), 2) if recetas_total else 100.0

        point_pending_by_tipo = {
            row["tipo"]: row["count"]
            for row in (
                PointPendingMatch.qs_operativos().values("tipo")
                .annotate(count=Count("id"))
                .order_by("tipo")
            )
        }
        point_pending_total = sum(point_pending_by_tipo.values())

        latest_run = AlmacenSyncRun.objects.only("id", "started_at", "pending_preview").order_by("-started_at").first()
        almacen_pending_count = len((latest_run.pending_preview or [])) if latest_run else 0

        recetas_pending_lines = (
            LineaReceta.objects.filter(insumo__isnull=True)
            .filter(match_status__in=[LineaReceta.STATUS_NEEDS_REVIEW, LineaReceta.STATUS_REJECTED])
            .count()
        )

        proveedores_activos = Proveedor.objects.filter(activo=True).count()
        proveedores_pending_point = int(point_pending_by_tipo.get(PointPendingMatch.TIPO_PROVEEDOR, 0))
        now_dt = timezone.now()
        since_24h = now_dt - timedelta(hours=24)
        since_48h = now_dt - timedelta(hours=48)
        current_window_qs = PublicApiAccessLog.objects.filter(created_at__gte=since_24h)
        previous_window_qs = PublicApiAccessLog.objects.filter(created_at__gte=since_48h, created_at__lt=since_24h)
        requests_24h = current_window_qs.count()
        errors_24h = current_window_qs.filter(status_code__gte=400).count()
        requests_prev_24h = previous_window_qs.count()
        errors_prev_24h = previous_window_qs.filter(status_code__gte=400).count()
        requests_delta_pct = _pct_change(requests_24h, requests_prev_24h)
        errors_delta_pct = _pct_change(errors_24h, errors_prev_24h)
        errors_by_endpoint_24h = list(
            PublicApiAccessLog.objects.filter(created_at__gte=since_24h, status_code__gte=400)
            .values("endpoint")
            .annotate(
                total=Count("id"),
                clientes=Count("client_id", distinct=True),
                last_at=Max("created_at"),
            )
            .order_by("-total", "endpoint")[:12]
        )
        errors_by_client_24h = list(
            PublicApiAccessLog.objects.filter(created_at__gte=since_24h, status_code__gte=400)
            .values("client__nombre")
            .annotate(
                total=Count("id"),
                last_at=Max("created_at"),
            )
            .order_by("-total", "client__nombre")[:12]
        )
        api_daily_trend = _build_public_api_daily_trend(days=7)
        api_7d_requests = sum(int(row.get("requests") or 0) for row in api_daily_trend)
        api_7d_errors = sum(int(row.get("errors") or 0) for row in api_daily_trend)
        api_7d_error_rate = round((api_7d_errors * 100.0 / api_7d_requests), 2) if api_7d_requests else 0.0
        since_30d = timezone.now() - timedelta(days=30)
        api_clients_total = PublicApiClient.objects.count()
        api_clients_active = PublicApiClient.objects.filter(activo=True).count()
        api_clients_inactive = max(api_clients_total - api_clients_active, 0)
        clients_with_activity_30d = set(
            PublicApiAccessLog.objects.filter(created_at__gte=since_30d)
            .values_list("client_id", flat=True)
            .distinct()
        )
        api_clients_unused_30d = PublicApiClient.objects.filter(activo=True).exclude(id__in=clients_with_activity_30d).count()
        api_clients_top_30d = list(
            PublicApiAccessLog.objects.filter(created_at__gte=since_30d)
            .values("client__nombre")
            .annotate(
                requests=Count("id"),
                errors=Count("id", filter=Q(status_code__gte=400)),
            )
            .order_by("-requests", "client__nombre")[:10]
        )

        stale_limit = timezone.now() - timedelta(hours=24)
        alertas_operativas = []
        if errors_24h:
            alertas_operativas.append(
                {
                    "nivel": "danger",
                    "titulo": "Errores API en últimas 24h",
                    "detalle": f"{errors_24h} requests con status >= 400.",
                }
            )
        if errors_24h >= 5 and errors_delta_pct >= 50:
            alertas_operativas.append(
                {
                    "nivel": "danger",
                    "titulo": "Spike de errores API (24h)",
                    "detalle": (
                        f"Errores 24h: {errors_24h} vs {errors_prev_24h} previos "
                        f"({errors_delta_pct:+.2f}%)."
                    ),
                }
            )
        if point_pending_total:
            alertas_operativas.append(
                {
                    "nivel": "warning",
                    "titulo": "Pendientes Point abiertos",
                    "detalle": (
                        f"Total {point_pending_total}. "
                        f"Insumos {point_pending_by_tipo.get(PointPendingMatch.TIPO_INSUMO, 0)}, "
                        f"productos {point_pending_by_tipo.get(PointPendingMatch.TIPO_PRODUCTO, 0)}, "
                        f"proveedores {point_pending_by_tipo.get(PointPendingMatch.TIPO_PROVEEDOR, 0)}."
                    ),
                }
            )
        if recetas_pending_lines:
            alertas_operativas.append(
                {
                    "nivel": "warning",
                    "titulo": "Líneas receta sin match",
                    "detalle": f"{recetas_pending_lines} líneas requieren homologación interna.",
                }
            )
        if not latest_run:
            alertas_operativas.append(
                {
                    "nivel": "warning",
                    "titulo": "Sync de almacén no ejecutado",
                    "detalle": "No hay corridas de sincronización registradas.",
                }
            )
        elif latest_run.started_at and latest_run.started_at < stale_limit:
            alertas_operativas.append(
                {
                    "nivel": "warning",
                    "titulo": "Sync de almacén desactualizado",
                    "detalle": f"Último sync: {latest_run.started_at:%Y-%m-%d %H:%M}.",
                }
            )
        if not alertas_operativas:
            alertas_operativas.append(
                {
                    "nivel": "ok",
                    "titulo": "Operación estable",
                    "detalle": "Sin alertas críticas en integración, match y sincronización.",
                }
            )

        operations_actions = {
            "DEACTIVATE_IDLE_API_CLIENTS",
            "PREVIEW_DEACTIVATE_IDLE_API_CLIENTS",
            "PURGE_API_LOGS",
            "PREVIEW_PURGE_API_LOGS",
            "RUN_API_MAINTENANCE",
            "PREVIEW_RUN_API_MAINTENANCE",
        }
        latest_operations = list(
            AuditLog.objects.filter(action__in=operations_actions)
            .select_related("user")
            .order_by("-timestamp")[:60]
        )

        def _latest_action(*action_names):
            action_set = set(action_names)
            for row in latest_operations:
                if row.action in action_set:
                    return {
                        "timestamp": row.timestamp,
                        "user": row.user.username if row.user_id else "",
                        "action": row.action,
                        "payload": row.payload or {},
                    }
            return None

        return Response(
            {
                "generated_at": timezone.now(),
                "api_24h": {
                    "requests": requests_24h,
                    "errors": errors_24h,
                    "errors_by_endpoint": errors_by_endpoint_24h,
                    "errors_by_client": errors_by_client_24h,
                },
                "api_24h_comparativo": {
                    "requests_prev_24h": requests_prev_24h,
                    "errors_prev_24h": errors_prev_24h,
                    "requests_delta_pct": requests_delta_pct,
                    "errors_delta_pct": errors_delta_pct,
                },
                "api_7d": {
                    "requests": api_7d_requests,
                    "errors": api_7d_errors,
                    "error_rate_pct": api_7d_error_rate,
                    "daily": api_daily_trend,
                },
                "api_clients": {
                    "total": api_clients_total,
                    "active": api_clients_active,
                    "inactive": api_clients_inactive,
                    "unused_30d": api_clients_unused_30d,
                    "top_30d": api_clients_top_30d,
                },
                "api_operations": {
                    "last_any": _latest_action(*operations_actions),
                    "last_maintenance": _latest_action("RUN_API_MAINTENANCE", "PREVIEW_RUN_API_MAINTENANCE"),
                    "last_purge": _latest_action("PURGE_API_LOGS", "PREVIEW_PURGE_API_LOGS"),
                    "last_deactivate": _latest_action(
                        "DEACTIVATE_IDLE_API_CLIENTS",
                        "PREVIEW_DEACTIVATE_IDLE_API_CLIENTS",
                    ),
                },
                "alertas_operativas": alertas_operativas,
                "insumos": {
                    "activos": insumos_activos,
                    "con_codigo_point": insumos_con_codigo,
                    "sin_codigo_point": insumos_sin_codigo,
                    "cobertura_pct": insumos_cobertura,
                },
                "recetas": {
                    "total": recetas_total,
                    "homologadas": recetas_homologadas,
                    "sin_homologar": recetas_sin_homologar,
                    "con_codigo_point_primario": len(receta_ids_primary),
                    "con_alias_codigo_point": len(receta_ids_alias),
                    "cobertura_pct": recetas_cobertura,
                },
                "proveedores": {
                    "activos": proveedores_activos,
                    "point_pending": proveedores_pending_point,
                },
                "point_pending": {
                    "total": point_pending_total,
                    "por_tipo": point_pending_by_tipo,
                },
                "inventario": {
                    "almacen_pending_preview": almacen_pending_count,
                    "almacen_latest_run_id": latest_run.id if latest_run else None,
                    "almacen_latest_run_at": latest_run.started_at if latest_run else None,
                    "recetas_pending_match": recetas_pending_lines,
                },
            },
            status=status.HTTP_200_OK,
        )



