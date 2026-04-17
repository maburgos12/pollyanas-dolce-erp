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



def _resolve_recipe_for_pos_row(*, row: dict[str, Any], receta_cache: dict[tuple[int, str, str], Receta | None]) -> Receta | None:
    receta_id = row.get("receta_id")
    receta_name = str(row.get("receta") or row.get("producto") or "").strip()
    codigo_point = str(row.get("codigo_point") or "").strip()
    return _resolve_receta_bulk_ref(
        receta_id=receta_id,
        receta_name=receta_name,
        codigo_point=codigo_point,
        cache=receta_cache,
    )


def _resolve_sucursal_for_pos_row(
    *,
    row: dict[str, Any],
    default_sucursal: Sucursal | None,
    sucursal_cache: dict[tuple[int, str, str, int], Sucursal | None],
) -> Sucursal | None:
    return _resolve_sucursal_bulk_ref(
        sucursal_id=row.get("sucursal_id"),
        sucursal_name=str(row.get("sucursal") or "").strip(),
        sucursal_codigo=str(row.get("sucursal_codigo") or "").strip(),
        default_sucursal=default_sucursal,
        cache=sucursal_cache,
    )


def _process_control_venta_pos_bulk(
    data: dict[str, Any],
    *,
    dry_run_override: bool | None = None,
) -> dict[str, Any]:
    rows = data["rows"]
    modo = data.get("modo") or "replace"
    fuente = (data.get("fuente") or "API_POS_VENTAS").strip()[:40] or "API_POS_VENTAS"
    dry_run = bool(data.get("dry_run", True) if dry_run_override is None else dry_run_override)
    stop_on_error = bool(data.get("stop_on_error", False))
    top = int(data.get("top") or 120)

    default_sucursal = None
    default_sucursal_id = data.get("sucursal_default_id")
    if default_sucursal_id is not None:
        default_sucursal = Sucursal.objects.filter(pk=default_sucursal_id, activa=True).first()
        if default_sucursal is None:
            raise ValueError("Sucursal default no encontrada o inactiva.")

    receta_cache: dict[tuple[int, str, str], Receta | None] = {}
    sucursal_cache: dict[tuple[int, str, str, int], Sucursal | None] = {}
    created = 0
    updated = 0
    skipped = 0
    terminated_early = False
    result_rows: list[dict[str, Any]] = []

    tx_cm = nullcontext() if dry_run else transaction.atomic()
    with tx_cm:
        for index, row in enumerate(rows, start=1):
            receta = _resolve_recipe_for_pos_row(row=row, receta_cache=receta_cache)
            producto_texto = str(row.get("producto") or row.get("receta") or "").strip()
            codigo_point = str(row.get("codigo_point") or "").strip()
            if receta is None and not (producto_texto or codigo_point):
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "producto_not_found",
                        "producto_input": producto_texto or codigo_point,
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            sucursal = _resolve_sucursal_for_pos_row(
                row=row,
                default_sucursal=default_sucursal,
                sucursal_cache=sucursal_cache,
            )
            has_sucursal_ref = bool(row.get("sucursal_id")) or bool(row.get("sucursal")) or bool(row.get("sucursal_codigo"))
            if has_sucursal_ref and sucursal is None:
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "sucursal_not_found",
                        "sucursal_input": row.get("sucursal_codigo") or row.get("sucursal") or row.get("sucursal_id"),
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            fecha = row["fecha"]
            cantidad = _to_decimal(row.get("cantidad"), default=Decimal("0"))
            tickets = int(row.get("tickets") or 0)
            monto_total_raw = row.get("monto_total")
            monto_total = _to_decimal(monto_total_raw) if monto_total_raw is not None else None

            existing_qs = VentaPOS.objects.filter(
                receta=receta,
                sucursal=sucursal,
                fecha=fecha,
                codigo_point=codigo_point,
                producto_texto=producto_texto,
            )
            existing = existing_qs.order_by("id").first()

            previous_qty = _to_decimal(existing.cantidad, default=Decimal("0")) if existing else Decimal("0")
            previous_tickets = int(existing.tickets or 0) if existing else 0
            if existing:
                if modo == "accumulate":
                    new_qty = previous_qty + cantidad
                    new_tickets = previous_tickets + tickets
                    new_monto = (_to_decimal(existing.monto_total, default=Decimal("0")) + _to_decimal(monto_total, default=Decimal("0")))
                else:
                    new_qty = cantidad
                    new_tickets = tickets
                    new_monto = monto_total
                action = "UPDATED"
                updated += 1
            else:
                new_qty = cantidad
                new_tickets = tickets
                new_monto = monto_total
                action = "CREATED"
                created += 1

            if not dry_run:
                if existing:
                    existing.cantidad = new_qty
                    existing.tickets = new_tickets
                    existing.monto_total = new_monto
                    existing.fuente = fuente
                    existing.save(update_fields=["cantidad", "tickets", "monto_total", "fuente", "actualizado_en"])
                else:
                    VentaPOS.objects.create(
                        receta=receta,
                        sucursal=sucursal,
                        fecha=fecha,
                        codigo_point=codigo_point,
                        producto_texto=producto_texto,
                        cantidad=new_qty,
                        tickets=new_tickets,
                        monto_total=new_monto,
                        fuente=fuente,
                    )

            result_rows.append(
                {
                    "row": index,
                    "status": action,
                    "receta_id": receta.id if receta else None,
                    "receta": receta.nombre if receta else "",
                    "producto": producto_texto,
                    "codigo_point": codigo_point,
                    "sucursal_id": sucursal.id if sucursal else None,
                    "sucursal": sucursal.nombre if sucursal else "",
                    "fecha": str(fecha),
                    "cantidad_prev": float(previous_qty),
                    "cantidad_nueva": float(new_qty),
                    "tickets_prev": previous_tickets,
                    "tickets_nuevos": new_tickets,
                }
            )

    error_count = sum(1 for row in result_rows if row.get("status") == "ERROR")
    return {
        "dry_run": dry_run,
        "mode": modo,
        "fuente": fuente,
        "terminated_early": terminated_early,
        "summary": {
            "total_rows": len(rows),
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "errors": error_count,
            "applied": 0 if dry_run else (created + updated),
        },
        "rows": result_rows[:top],
    }


def _process_control_merma_pos_bulk(
    data: dict[str, Any],
    *,
    dry_run_override: bool | None = None,
) -> dict[str, Any]:
    rows = data["rows"]
    modo = data.get("modo") or "replace"
    fuente = (data.get("fuente") or "API_POS_MERMAS").strip()[:40] or "API_POS_MERMAS"
    dry_run = bool(data.get("dry_run", True) if dry_run_override is None else dry_run_override)
    stop_on_error = bool(data.get("stop_on_error", False))
    top = int(data.get("top") or 120)

    default_sucursal = None
    default_sucursal_id = data.get("sucursal_default_id")
    if default_sucursal_id is not None:
        default_sucursal = Sucursal.objects.filter(pk=default_sucursal_id, activa=True).first()
        if default_sucursal is None:
            raise ValueError("Sucursal default no encontrada o inactiva.")

    receta_cache: dict[tuple[int, str, str], Receta | None] = {}
    sucursal_cache: dict[tuple[int, str, str, int], Sucursal | None] = {}
    created = 0
    updated = 0
    skipped = 0
    terminated_early = False
    result_rows: list[dict[str, Any]] = []

    tx_cm = nullcontext() if dry_run else transaction.atomic()
    with tx_cm:
        for index, row in enumerate(rows, start=1):
            receta = _resolve_recipe_for_pos_row(row=row, receta_cache=receta_cache)
            producto_texto = str(row.get("producto") or row.get("receta") or "").strip()
            codigo_point = str(row.get("codigo_point") or "").strip()
            if receta is None and not (producto_texto or codigo_point):
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "producto_not_found",
                        "producto_input": producto_texto or codigo_point,
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            sucursal = _resolve_sucursal_for_pos_row(
                row=row,
                default_sucursal=default_sucursal,
                sucursal_cache=sucursal_cache,
            )
            has_sucursal_ref = bool(row.get("sucursal_id")) or bool(row.get("sucursal")) or bool(row.get("sucursal_codigo"))
            if has_sucursal_ref and sucursal is None:
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "sucursal_not_found",
                        "sucursal_input": row.get("sucursal_codigo") or row.get("sucursal") or row.get("sucursal_id"),
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            fecha = row["fecha"]
            cantidad = _to_decimal(row.get("cantidad"), default=Decimal("0"))
            motivo = str(row.get("motivo") or "").strip()[:160]

            existing_qs = MermaPOS.objects.filter(
                receta=receta,
                sucursal=sucursal,
                fecha=fecha,
                codigo_point=codigo_point,
                producto_texto=producto_texto,
                motivo=motivo,
            )
            existing = existing_qs.order_by("id").first()

            previous_qty = _to_decimal(existing.cantidad, default=Decimal("0")) if existing else Decimal("0")
            if existing:
                new_qty = previous_qty + cantidad if modo == "accumulate" else cantidad
                action = "UPDATED"
                updated += 1
            else:
                new_qty = cantidad
                action = "CREATED"
                created += 1

            if not dry_run:
                if existing:
                    existing.cantidad = new_qty
                    existing.fuente = fuente
                    existing.save(update_fields=["cantidad", "fuente", "actualizado_en"])
                else:
                    MermaPOS.objects.create(
                        receta=receta,
                        sucursal=sucursal,
                        fecha=fecha,
                        codigo_point=codigo_point,
                        producto_texto=producto_texto,
                        cantidad=new_qty,
                        motivo=motivo,
                        fuente=fuente,
                    )

            result_rows.append(
                {
                    "row": index,
                    "status": action,
                    "receta_id": receta.id if receta else None,
                    "receta": receta.nombre if receta else "",
                    "producto": producto_texto,
                    "codigo_point": codigo_point,
                    "sucursal_id": sucursal.id if sucursal else None,
                    "sucursal": sucursal.nombre if sucursal else "",
                    "fecha": str(fecha),
                    "cantidad_prev": float(previous_qty),
                    "cantidad_nueva": float(new_qty),
                }
            )

    error_count = sum(1 for row in result_rows if row.get("status") == "ERROR")
    return {
        "dry_run": dry_run,
        "mode": modo,
        "fuente": fuente,
        "terminated_early": terminated_early,
        "summary": {
            "total_rows": len(rows),
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "errors": error_count,
            "applied": 0 if dry_run else (created + updated),
        },
        "rows": result_rows[:top],
    }


class ControlVentasPosImportPreviewView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not (can_manage_inventario(request.user) or can_manage_compras(request.user)):
            return Response(
                {"detail": "No tienes permisos para previsualizar importación POS."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ControlVentaPosBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_control_venta_pos_bulk(ser.validated_data, dry_run_override=True)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        payload["preview"] = True
        return Response(payload, status=status.HTTP_200_OK)


class ControlVentasPosImportConfirmView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not (can_manage_inventario(request.user) or can_manage_compras(request.user)):
            return Response(
                {"detail": "No tienes permisos para confirmar importación POS."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ControlVentaPosBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_control_venta_pos_bulk(ser.validated_data, dry_run_override=False)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        payload["preview"] = False
        return Response(payload, status=status.HTTP_200_OK)


class ControlVentasPosBulkUpsertView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not (can_manage_inventario(request.user) or can_manage_compras(request.user)):
            return Response(
                {"detail": "No tienes permisos para importar ventas POS."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ControlVentaPosBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_control_venta_pos_bulk(ser.validated_data)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        return Response(payload, status=status.HTTP_200_OK)


class ControlMermasPosImportPreviewView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not (can_manage_inventario(request.user) or can_manage_compras(request.user)):
            return Response(
                {"detail": "No tienes permisos para previsualizar importación de mermas POS."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ControlMermaPosBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_control_merma_pos_bulk(ser.validated_data, dry_run_override=True)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        payload["preview"] = True
        return Response(payload, status=status.HTTP_200_OK)


class ControlMermasPosImportConfirmView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not (can_manage_inventario(request.user) or can_manage_compras(request.user)):
            return Response(
                {"detail": "No tienes permisos para confirmar importación de mermas POS."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ControlMermaPosBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_control_merma_pos_bulk(ser.validated_data, dry_run_override=False)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        payload["preview"] = False
        return Response(payload, status=status.HTTP_200_OK)


class ControlMermasPosBulkUpsertView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not (can_manage_inventario(request.user) or can_manage_compras(request.user)):
            return Response(
                {"detail": "No tienes permisos para importar mermas POS."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ControlMermaPosBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_control_merma_pos_bulk(ser.validated_data)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        return Response(payload, status=status.HTTP_200_OK)


class ControlDiscrepanciasView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_reportes(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar discrepancias."},
                status=status.HTTP_403_FORBIDDEN,
            )

        date_from, date_to, period_resolved = resolve_period_range(
            period_raw=request.GET.get("periodo"),
            date_from_raw=request.GET.get("from"),
            date_to_raw=request.GET.get("to"),
        )
        sucursal_id = None
        sucursal_id_raw = (request.GET.get("sucursal_id") or "").strip()
        if sucursal_id_raw.isdigit():
            sucursal_id = int(sucursal_id_raw)
        threshold = _to_decimal(request.GET.get("threshold_pct"), default=Decimal("10"))
        top = _parse_bounded_int(request.GET.get("top"), default=300, min_value=1, max_value=1000)

        report = build_discrepancias_report(
            date_from=date_from,
            date_to=date_to,
            sucursal_id=sucursal_id,
            threshold_pct=threshold,
            top=top,
        )
        report["scope"] = {
            "periodo": period_resolved,
            "sucursal_id": sucursal_id,
            "top": top,
        }
        return Response(report, status=status.HTTP_200_OK)

