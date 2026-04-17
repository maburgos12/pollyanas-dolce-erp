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



class MRPExplodeView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        ser = MRPRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        receta_id = ser.validated_data["receta_id"]
        multiplicador: Decimal = ser.validated_data.get("multiplicador", Decimal("1"))

        receta = Receta.objects.get(pk=receta_id)
        agregados = {}

        for l in receta.lineas.select_related("insumo").all():
            canonical = canonical_insumo(l.insumo) if l.insumo_id else None
            display_insumo = canonical or l.insumo
            key = display_insumo.nombre if display_insumo else f"(NO MATCH) {l.insumo_texto}"
            if key not in agregados:
                agregados[key] = {
                    "insumo_id": display_insumo.id if display_insumo else None,
                    "nombre": key,
                    "cantidad": Decimal("0"),
                    "unidad": (
                        display_insumo.unidad_base.codigo
                        if display_insumo and display_insumo.unidad_base_id and display_insumo.unidad_base
                        else l.unidad_texto
                    ),
                    "costo": 0.0,
                    "insumo_canonical": bool(canonical and l.insumo_id and canonical.id != l.insumo_id),
                }
            qty = Decimal(str(l.cantidad or 0)) * multiplicador
            agregados[key]["cantidad"] += qty
            agregados[key]["costo"] += float(l.costo_total_estimado) * float(multiplicador)

        items = sorted(agregados.values(), key=lambda x: x["nombre"])
        costo_total = sum(i["costo"] for i in items)

        return Response({
            "receta_id": receta.id,
            "receta_nombre": receta.nombre,
            "multiplicador": str(multiplicador),
            "costo_total": costo_total,
            "items": [
                {
                    "insumo_id": i["insumo_id"],
                    "nombre": i["nombre"],
                    "cantidad": str(i["cantidad"]),
                    "unidad": i["unidad"],
                    "costo": i["costo"],
                    "insumo_canonical": bool(i.get("insumo_canonical")),
                } for i in items
            ],
        }, status=status.HTTP_200_OK)


class RecetaVersionesView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, receta_id: int):
        receta = get_object_or_404(Receta, pk=receta_id)
        data_unavailable = False
        warnings: list[str] = []
        try:
            asegurar_version_costeo(receta, fuente="API_VERSIONES")
        except (OperationalError, ProgrammingError):
            data_unavailable = True
            warnings.append("Versionado automático no disponible en este entorno.")
        limit = _parse_bounded_int(
            request.GET.get("limit", 25),
            default=25,
            min_value=1,
            max_value=200,
        )
        try:
            versiones = _load_versiones_costeo(receta, limit)
        except (OperationalError, ProgrammingError):
            data_unavailable = True
            versiones = []
            warnings.append("Histórico de versiones no disponible en este entorno.")
        payload = RecetaCostoVersionSerializer(versiones, many=True).data
        return Response(
            {
                "receta_id": receta.id,
                "receta_nombre": receta.nombre,
                "total": len(payload),
                "items": payload,
                "data_unavailable": data_unavailable,
                "warnings": warnings,
            },
            status=status.HTTP_200_OK,
        )


class RecetaCostoHistoricoView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, receta_id: int):
        receta = get_object_or_404(Receta, pk=receta_id)
        data_unavailable = False
        warnings: list[str] = []
        try:
            asegurar_version_costeo(receta, fuente="API_HISTORICO")
        except (OperationalError, ProgrammingError):
            data_unavailable = True
            warnings.append("Versionado automático no disponible en este entorno.")
        limit = _parse_bounded_int(
            request.GET.get("limit", 60),
            default=60,
            min_value=1,
            max_value=300,
        )
        try:
            versiones = _load_versiones_costeo(receta, limit)
        except (OperationalError, ProgrammingError):
            data_unavailable = True
            versiones = []
            warnings.append("Histórico de costos no disponible en este entorno.")
        payload = RecetaCostoVersionSerializer(versiones, many=True).data
        comparativo = comparativo_versiones(versiones)
        data = {
            "receta_id": receta.id,
            "receta_nombre": receta.nombre,
            "puntos": payload,
            "data_unavailable": data_unavailable,
            "warnings": warnings,
        }
        if comparativo:
            data["comparativo"] = {
                "version_actual": comparativo["version_actual"],
                "version_previa": comparativo["version_previa"],
                "delta_total": str(comparativo["delta_total"]),
                "delta_pct": str(comparativo["delta_pct"]),
            }

        base_raw = (request.GET.get("base") or "").strip()
        target_raw = (request.GET.get("target") or "").strip()
        if base_raw and target_raw:
            try:
                base_num = int(base_raw)
                target_num = int(target_raw)
            except ValueError:
                base_num = None
                target_num = None

            if base_num is not None and target_num is not None and base_num != target_num:
                by_version = {v.version_num: v for v in versiones}
                base_v = by_version.get(base_num)
                target_v = by_version.get(target_num)
                if base_v and target_v:
                    base_total = Decimal(str(base_v.costo_total or 0))
                    target_total = Decimal(str(target_v.costo_total or 0))
                    delta_total = target_total - base_total
                    delta_pct = None
                    if base_total > 0:
                        delta_pct = (delta_total * Decimal("100")) / base_total

                    data["comparativo_seleccionado"] = {
                        "base": base_v.version_num,
                        "target": target_v.version_num,
                        "delta_mp": str(Decimal(str(target_v.costo_mp or 0)) - Decimal(str(base_v.costo_mp or 0))),
                        "delta_mo": str(Decimal(str(target_v.costo_mo or 0)) - Decimal(str(base_v.costo_mo or 0))),
                        "delta_indirecto": str(Decimal(str(target_v.costo_indirecto or 0)) - Decimal(str(base_v.costo_indirecto or 0))),
                        "delta_total": str(delta_total),
                        "delta_pct_total": str(delta_pct) if delta_pct is not None else None,
                    }
        return Response(data, status=status.HTTP_200_OK)



