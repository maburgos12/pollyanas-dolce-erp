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



def _resolve_receta_bulk_ref(
    *,
    receta_id: int | None,
    receta_name: str,
    codigo_point: str,
    cache: dict[tuple[int, str, str], Receta | None],
) -> Receta | None:
    receta_id_int = int(receta_id) if receta_id else 0
    code_norm = (codigo_point or "").strip().lower()
    name_norm = (receta_name or "").strip().lower()
    key = (receta_id_int, code_norm, name_norm)
    if key in cache:
        return cache[key]

    receta = None
    if receta_id_int > 0:
        receta = Receta.objects.filter(pk=receta_id_int).first()
    if receta is None:
        receta = _resolve_receta_for_sales(receta_name, codigo_point)
    cache[key] = receta
    return receta


def _resolve_sucursal_bulk_ref(
    *,
    sucursal_id: int | None,
    sucursal_name: str,
    sucursal_codigo: str,
    default_sucursal: Sucursal | None,
    cache: dict[tuple[int, str, str, int], Sucursal | None],
) -> Sucursal | None:
    sucursal_id_int = int(sucursal_id) if sucursal_id else 0
    code_norm = (sucursal_codigo or "").strip().lower()
    name_norm = (sucursal_name or "").strip().lower()
    default_id = int(default_sucursal.id) if default_sucursal else 0
    key = (sucursal_id_int, code_norm, name_norm, default_id)
    if key in cache:
        return cache[key]

    sucursal = None
    if sucursal_id_int > 0:
        sucursal = Sucursal.objects.filter(pk=sucursal_id_int, activa=True).first()
    else:
        sucursal = _resolve_sucursal_for_sales(sucursal_name, sucursal_codigo, default_sucursal)
        if sucursal and not sucursal.activa:
            sucursal = None
    cache[key] = sucursal
    return sucursal


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _shift_month(start: date, months: int) -> date:
    month_index = (start.year * 12 + (start.month - 1)) + months
    year = month_index // 12
    month = (month_index % 12) + 1
    return date(year, month, 1)


def _forecast_backtest_windows(alcance: str, fecha_base: date, periods: int) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    if alcance == "mes":
        anchor = _month_start(fecha_base)
        for lag in range(periods, 0, -1):
            start = _shift_month(anchor, -lag)
            end = _shift_month(start, 1) - timedelta(days=1)
            windows.append((start, end))
        return windows

    if alcance == "fin_semana":
        wd = fecha_base.weekday()
        if wd == 5:
            anchor = fecha_base
        elif wd == 6:
            anchor = fecha_base - timedelta(days=1)
        else:
            anchor = fecha_base + timedelta(days=(5 - wd))
        for lag in range(periods, 0, -1):
            start = anchor - timedelta(days=7 * lag)
            windows.append((start, start + timedelta(days=1)))
        return windows

    # semana
    anchor = fecha_base - timedelta(days=fecha_base.weekday())
    for lag in range(periods, 0, -1):
        start = anchor - timedelta(days=7 * lag)
        windows.append((start, start + timedelta(days=6)))
    return windows



class MRPRequerimientosView(APIView):
    permission_classes = [IsAuthenticated]

    def _payload_from_plan(self, plan: PlanProduccion) -> list[tuple[Receta, Decimal]]:
        items_payload: list[tuple[Receta, Decimal]] = []
        for item in plan.items.select_related("receta").all():
            items_payload.append((item.receta, Decimal(str(item.cantidad or 0))))
        return items_payload

    def _payload_from_periodo(
        self,
        periodo: str,
        periodo_tipo: str,
    ) -> tuple[list[tuple[Receta, Decimal]], dict]:
        parsed = _parse_period(periodo)
        if not parsed:
            return [], {"periodo": periodo, "periodo_tipo": periodo_tipo, "planes_count": 0, "planes": []}

        year, month = parsed
        plans_qs = PlanProduccion.objects.filter(
            fecha_produccion__year=year,
            fecha_produccion__month=month,
        ).order_by("fecha_produccion", "id")
        if periodo_tipo == "q1":
            plans_qs = plans_qs.filter(fecha_produccion__day__lte=15)
        elif periodo_tipo == "q2":
            plans_qs = plans_qs.filter(fecha_produccion__day__gte=16)

        plans = list(plans_qs.only("id", "nombre", "fecha_produccion"))
        plan_ids = [p.id for p in plans]
        if not plan_ids:
            return [], {
                "periodo": f"{year:04d}-{month:02d}",
                "periodo_tipo": periodo_tipo,
                "planes_count": 0,
                "planes": [],
            }

        items_payload: list[tuple[Receta, Decimal]] = []
        items_qs = (
            PlanProduccionItem.objects.filter(plan_id__in=plan_ids)
            .select_related("receta")
            .only("receta_id", "cantidad", "plan_id", "receta__id", "receta__nombre")
        )
        for item in items_qs:
            items_payload.append((item.receta, Decimal(str(item.cantidad or 0))))

        return items_payload, {
            "periodo": f"{year:04d}-{month:02d}",
            "periodo_tipo": periodo_tipo,
            "planes_count": len(plans),
            "planes": [
                {
                    "id": p.id,
                    "nombre": p.nombre,
                    "fecha_produccion": str(p.fecha_produccion),
                }
                for p in plans[:50]
            ],
        }

    def _aggregate(self, items_payload: list[tuple[Receta, Decimal]]) -> dict:
        insumos = {}
        lineas_sin_match = 0
        lineas_sin_cantidad = 0
        lineas_sin_costo = 0

        for receta, factor in items_payload:
            if factor <= 0:
                continue
            for linea in receta.lineas.select_related("insumo", "insumo__unidad_base").all():
                if not linea.insumo_id:
                    lineas_sin_match += 1
                    continue

                qty = Decimal(str(linea.cantidad or 0))
                if qty <= 0:
                    lineas_sin_cantidad += 1
                    continue
                qty *= factor
                if qty <= 0:
                    continue

                unit_cost = Decimal(str(linea.costo_unitario_snapshot or 0))
                if unit_cost <= 0:
                    lineas_sin_costo += 1
                cost_total = qty * unit_cost if unit_cost > 0 else Decimal("0")

                row = insumos.setdefault(
                    linea.insumo_id,
                    {
                        "insumo_id": linea.insumo_id,
                        "insumo": linea.insumo.nombre,
                        "unidad": linea.unidad_texto or (linea.insumo.unidad_base.codigo if linea.insumo.unidad_base_id else ""),
                        "cantidad_requerida": Decimal("0"),
                        "costo_unitario": unit_cost,
                        "costo_total": Decimal("0"),
                    },
                )
                row["cantidad_requerida"] += qty
                row["costo_total"] += cost_total
                if row["costo_unitario"] <= 0 and unit_cost > 0:
                    row["costo_unitario"] = unit_cost

        existencias = {
            e.insumo_id: Decimal(str(e.stock_actual or 0))
            for e in ExistenciaInsumo.objects.filter(insumo_id__in=list(insumos.keys()))
        }

        alertas_capacidad = 0
        items = []
        for row in sorted(insumos.values(), key=lambda x: x["insumo"].lower()):
            stock = existencias.get(row["insumo_id"], Decimal("0"))
            faltante = row["cantidad_requerida"] - stock
            if faltante < 0:
                faltante = Decimal("0")
            alerta = faltante > 0
            if alerta:
                alertas_capacidad += 1
            items.append(
                {
                    "insumo_id": row["insumo_id"],
                    "insumo": row["insumo"],
                    "unidad": row["unidad"],
                    "cantidad_requerida": str(row["cantidad_requerida"]),
                    "stock_actual": str(stock),
                    "faltante": str(faltante),
                    "alerta_capacidad": alerta,
                    "costo_unitario": str(row["costo_unitario"]),
                    "costo_total": str(row["costo_total"]),
                }
            )

        return {
            "items": items,
            "totales": {
                "insumos": len(items),
                "costo_total": str(sum((Decimal(i["costo_total"]) for i in items), Decimal("0"))),
                "alertas_capacidad": alertas_capacidad,
                "lineas_sin_match": lineas_sin_match,
                "lineas_sin_cantidad": lineas_sin_cantidad,
                "lineas_sin_costo_unitario": lineas_sin_costo,
            },
        }

    def post(self, request):
        ser = MRPRequerimientosRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        plan_id = ser.validated_data.get("plan_id")
        periodo = (ser.validated_data.get("periodo") or "").strip()
        periodo_tipo = ser.validated_data.get("periodo_tipo") or "mes"
        raw_items = ser.validated_data.get("items") or []
        fecha_referencia = ser.validated_data.get("fecha_referencia")

        items_payload: list[tuple[Receta, Decimal]] = []
        source = "manual"
        plan = None
        periodo_scope = {
            "periodo": "",
            "periodo_tipo": periodo_tipo,
            "planes_count": 0,
            "planes": [],
        }

        if plan_id:
            plan = get_object_or_404(PlanProduccion, pk=plan_id)
            source = "plan"
            items_payload = self._payload_from_plan(plan)
            periodo_scope = {
                "periodo": plan.fecha_produccion.strftime("%Y-%m"),
                "periodo_tipo": "mes",
                "planes_count": 1,
                "planes": [
                    {
                        "id": plan.id,
                        "nombre": plan.nombre,
                        "fecha_produccion": str(plan.fecha_produccion),
                    }
                ],
            }
        elif periodo:
            source = "periodo"
            items_payload, periodo_scope = self._payload_from_periodo(periodo, periodo_tipo)
        else:
            for item in raw_items:
                receta = get_object_or_404(Receta, pk=item["receta_id"])
                items_payload.append((receta, Decimal(str(item["cantidad"]))))
            if fecha_referencia:
                periodo_scope["periodo"] = fecha_referencia.strftime("%Y-%m")

        data = self._aggregate(items_payload)
        response = {
            "source": source,
            "plan_id": plan.id if plan else None,
            "plan_nombre": plan.nombre if plan else "",
            "plan_fecha": str(plan.fecha_produccion) if plan else "",
            "periodo": periodo_scope["periodo"],
            "periodo_tipo": periodo_scope["periodo_tipo"],
            "planes_count": periodo_scope["planes_count"],
            "planes": periodo_scope["planes"],
            **data,
        }
        return Response(response, status=status.HTTP_200_OK)


class PlanProduccionListCreateView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _serialize_plan(plan: PlanProduccion, include_items: bool = False) -> dict:
        rows = list(plan.items.select_related("receta").all().order_by("id"))
        cantidad_total = sum((_to_decimal(r.cantidad) for r in rows), Decimal("0"))
        costo_total = sum((_to_decimal(r.costo_total_estimado) for r in rows), Decimal("0"))
        payload = {
            "id": plan.id,
            "nombre": plan.nombre,
            "fecha_produccion": str(plan.fecha_produccion),
            "notas": plan.notas or "",
            "items_count": len(rows),
            "cantidad_total": str(cantidad_total),
            "costo_total_estimado": str(costo_total.quantize(Decimal("0.001"))),
            "creado_por": plan.creado_por.username if plan.creado_por_id else "",
            "actualizado_en": plan.actualizado_en,
        }
        if include_items:
            payload["items"] = [
                {
                    "id": r.id,
                    "receta_id": r.receta_id,
                    "receta": r.receta.nombre,
                    "codigo_point": r.receta.codigo_point,
                    "cantidad": str(_to_decimal(r.cantidad)),
                    "costo_total_estimado": str(_to_decimal(r.costo_total_estimado).quantize(Decimal("0.001"))),
                    "notas": r.notas or "",
                }
                for r in rows
            ]
        return payload

    def get(self, request):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar planes de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )

        q = (request.GET.get("q") or "").strip()
        periodo = (request.GET.get("periodo") or request.GET.get("mes") or "").strip()
        fecha_desde_raw = (request.GET.get("fecha_desde") or "").strip()
        fecha_hasta_raw = (request.GET.get("fecha_hasta") or "").strip()
        include_items = _parse_bool(request.GET.get("include_items"), default=False)
        limit = _parse_bounded_int(request.GET.get("limit", 120), default=120, min_value=1, max_value=400)

        fecha_desde = _parse_iso_date(fecha_desde_raw)
        if fecha_desde_raw and fecha_desde is None:
            return Response(
                {"detail": "fecha_desde inválida. Usa formato YYYY-MM-DD."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        fecha_hasta = _parse_iso_date(fecha_hasta_raw)
        if fecha_hasta_raw and fecha_hasta is None:
            return Response(
                {"detail": "fecha_hasta inválida. Usa formato YYYY-MM-DD."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if fecha_desde and fecha_hasta and fecha_hasta < fecha_desde:
            return Response(
                {"detail": "fecha_hasta no puede ser menor que fecha_desde."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        qs = PlanProduccion.objects.prefetch_related("items__receta").order_by("-fecha_produccion", "-id")
        parsed_period = _parse_period(periodo)
        if periodo and not parsed_period:
            return Response(
                {"detail": "periodo inválido. Usa formato YYYY-MM."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if parsed_period:
            y, m = parsed_period
            qs = qs.filter(fecha_produccion__year=y, fecha_produccion__month=m)
            periodo = f"{y:04d}-{m:02d}"
        else:
            periodo = ""

        if fecha_desde:
            qs = qs.filter(fecha_produccion__gte=fecha_desde)
        if fecha_hasta:
            qs = qs.filter(fecha_produccion__lte=fecha_hasta)
        if q:
            qs = qs.filter(
                Q(nombre__icontains=q) | Q(notas__icontains=q) | Q(items__receta__nombre__icontains=q)
            ).distinct()

        plans = list(qs[:limit])
        items = []
        items_count_total = 0
        cantidad_total = Decimal("0")
        costo_total = Decimal("0")
        for p in plans:
            serialized = self._serialize_plan(p, include_items=include_items)
            items.append(serialized)
            items_count_total += int(serialized["items_count"])
            cantidad_total += _to_decimal(serialized["cantidad_total"])
            costo_total += _to_decimal(serialized["costo_total_estimado"])

        return Response(
            {
                "filters": {
                    "q": q,
                    "periodo": periodo,
                    "fecha_desde": str(fecha_desde) if fecha_desde else "",
                    "fecha_hasta": str(fecha_hasta) if fecha_hasta else "",
                    "include_items": include_items,
                    "limit": limit,
                },
                "totales": {
                    "planes": len(items),
                    "renglones": items_count_total,
                    "cantidad_total": str(cantidad_total),
                    "costo_total_estimado": str(costo_total.quantize(Decimal("0.001"))),
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not request.user.has_perm("recetas.add_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para crear planes de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = PlanProduccionCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        fecha_produccion = data.get("fecha_produccion") or timezone.localdate()
        nombre = (data.get("nombre") or "").strip()
        notas = (data.get("notas") or "").strip()
        rows = data["items"]

        receta_ids = sorted({int(row["receta_id"]) for row in rows})
        recetas = Receta.objects.filter(id__in=receta_ids).only("id", "nombre", "codigo_point")
        receta_map = {r.id: r for r in recetas}
        missing_ids = [rid for rid in receta_ids if rid not in receta_map]
        if missing_ids:
            return Response(
                {"detail": "Hay recetas inexistentes en items.", "missing_receta_ids": missing_ids},
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic():
            plan = PlanProduccion.objects.create(
                nombre=nombre or f"Plan {fecha_produccion} #{PlanProduccion.objects.count() + 1}",
                fecha_produccion=fecha_produccion,
                notas=notas,
                creado_por=request.user if request.user.is_authenticated else None,
            )
            for row in rows:
                receta = receta_map[int(row["receta_id"])]
                PlanProduccionItem.objects.create(
                    plan=plan,
                    receta=receta,
                    cantidad=_to_decimal(row.get("cantidad")),
                    notas=(row.get("notas") or "").strip()[:160],
                )

        payload = self._serialize_plan(plan, include_items=True)
        return Response(
            {"created": True, "plan": payload},
            status=status.HTTP_201_CREATED,
        )


class PlanProduccionDetailView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _serialize_item(row: PlanProduccionItem) -> dict:
        return {
            "id": row.id,
            "receta_id": row.receta_id,
            "receta": row.receta.nombre,
            "codigo_point": row.receta.codigo_point,
            "cantidad": str(_to_decimal(row.cantidad)),
            "costo_total_estimado": str(_to_decimal(row.costo_total_estimado).quantize(Decimal("0.001"))),
            "notas": row.notas or "",
        }

    @staticmethod
    def _plan_totals(plan: PlanProduccion) -> dict:
        rows = list(plan.items.select_related("receta").all())
        cantidad_total = sum((_to_decimal(r.cantidad) for r in rows), Decimal("0"))
        costo_total = sum((_to_decimal(r.costo_total_estimado) for r in rows), Decimal("0"))
        return {
            "items_count": len(rows),
            "cantidad_total": str(cantidad_total),
            "costo_total_estimado": str(costo_total.quantize(Decimal("0.001"))),
        }

    def get(self, request, plan_id: int):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar planes de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )
        plan = get_object_or_404(PlanProduccion.objects.prefetch_related("items__receta"), pk=plan_id)
        totals = self._plan_totals(plan)
        items = [self._serialize_item(r) for r in plan.items.select_related("receta").all().order_by("id")]
        return Response(
            {
                "id": plan.id,
                "nombre": plan.nombre,
                "fecha_produccion": str(plan.fecha_produccion),
                "notas": plan.notas or "",
                "creado_por": plan.creado_por.username if plan.creado_por_id else "",
                "actualizado_en": plan.actualizado_en,
                "totals": totals,
                "items": items,
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request, plan_id: int):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para editar planes de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )
        plan = get_object_or_404(PlanProduccion, pk=plan_id)
        ser = PlanProduccionUpdateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data
        update_fields = []
        if "nombre" in data:
            nombre = (data.get("nombre") or "").strip()
            if nombre:
                plan.nombre = nombre[:140]
                update_fields.append("nombre")
        if "fecha_produccion" in data:
            plan.fecha_produccion = data["fecha_produccion"]
            update_fields.append("fecha_produccion")
        if "notas" in data:
            plan.notas = (data.get("notas") or "").strip()
            update_fields.append("notas")
        if update_fields:
            plan.save(update_fields=update_fields + ["actualizado_en"])
        return Response(
            {
                "updated": bool(update_fields),
                "id": plan.id,
                "nombre": plan.nombre,
                "fecha_produccion": str(plan.fecha_produccion),
                "notas": plan.notas or "",
            },
            status=status.HTTP_200_OK,
        )

    def delete(self, request, plan_id: int):
        if not request.user.has_perm("recetas.delete_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para eliminar planes de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )
        plan = get_object_or_404(PlanProduccion, pk=plan_id)
        payload = {"id": plan.id, "nombre": plan.nombre}
        plan.delete()
        return Response(
            {"deleted": True, "plan": payload},
            status=status.HTTP_200_OK,
        )


class PlanProduccionItemCreateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, plan_id: int):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para editar renglones de plan de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )

        plan = get_object_or_404(PlanProduccion, pk=plan_id)
        ser = PlanProduccionItemCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data
        receta = Receta.objects.filter(pk=data["receta_id"]).only("id", "nombre", "codigo_point").first()
        if receta is None:
            return Response(
                {"detail": "receta_id no encontrada."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        item = PlanProduccionItem.objects.create(
            plan=plan,
            receta=receta,
            cantidad=_to_decimal(data["cantidad"]),
            notas=(data.get("notas") or "").strip()[:160],
        )
        return Response(
            {
                "created": True,
                "item": PlanProduccionDetailView._serialize_item(item),
                "plan_totals": PlanProduccionDetailView._plan_totals(plan),
            },
            status=status.HTTP_201_CREATED,
        )


class PlanProduccionItemDetailView(APIView):
    permission_classes = [IsAuthenticated]

    def patch(self, request, item_id: int):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para editar renglones de plan de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )

        item = get_object_or_404(PlanProduccionItem.objects.select_related("plan", "receta"), pk=item_id)
        ser = PlanProduccionItemUpdateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data
        update_fields = []
        if "receta_id" in data:
            receta = Receta.objects.filter(pk=data["receta_id"]).only("id", "nombre", "codigo_point").first()
            if receta is None:
                return Response(
                    {"detail": "receta_id no encontrada."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            item.receta = receta
            update_fields.append("receta")
        if "cantidad" in data:
            item.cantidad = _to_decimal(data["cantidad"])
            update_fields.append("cantidad")
        if "notas" in data:
            item.notas = (data.get("notas") or "").strip()[:160]
            update_fields.append("notas")
        if update_fields:
            item.save(update_fields=update_fields)
        item.refresh_from_db()
        return Response(
            {
                "updated": bool(update_fields),
                "item": PlanProduccionDetailView._serialize_item(item),
                "plan_totals": PlanProduccionDetailView._plan_totals(item.plan),
            },
            status=status.HTTP_200_OK,
        )

    def delete(self, request, item_id: int):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para eliminar renglones de plan de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )
        item = get_object_or_404(PlanProduccionItem.objects.select_related("plan"), pk=item_id)
        plan = item.plan
        payload = {"id": item.id, "plan_id": item.plan_id}
        item.delete()
        return Response(
            {
                "deleted": True,
                "item": payload,
                "plan_totals": PlanProduccionDetailView._plan_totals(plan),
            },
            status=status.HTTP_200_OK,
        )


class PlanDesdePronosticoCreateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.add_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para generar planes de producción."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = PlanDesdePronosticoRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        periodo = data["periodo"]
        incluir_preparaciones = bool(data.get("incluir_preparaciones"))
        nombre = (data.get("nombre") or "").strip() or f"Plan desde pronóstico {periodo}"
        fecha_produccion = data.get("fecha_produccion")
        if not fecha_produccion:
            fecha_produccion = date.fromisoformat(f"{periodo}-01")

        pronosticos_qs = (
            PronosticoVenta.objects.filter(periodo=periodo)
            .select_related("receta")
            .order_by("receta__nombre")
        )
        if not incluir_preparaciones:
            pronosticos_qs = pronosticos_qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)

        pronosticos = list(pronosticos_qs)
        if not pronosticos:
            return Response(
                {
                    "detail": "No hay pronósticos para generar plan en ese período con los filtros actuales.",
                    "periodo": periodo,
                    "incluir_preparaciones": incluir_preparaciones,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic():
            plan = PlanProduccion.objects.create(
                nombre=nombre[:140],
                fecha_produccion=fecha_produccion,
                notas=f"Generado desde pronóstico {periodo}",
                creado_por=request.user if request.user.is_authenticated else None,
            )
            created = 0
            skipped = 0
            for p in pronosticos:
                qty = _to_decimal(getattr(p, "cantidad", 0))
                if qty <= 0:
                    skipped += 1
                    continue
                PlanProduccionItem.objects.create(
                    plan=plan,
                    receta=p.receta,
                    cantidad=qty,
                    notas=f"Pronóstico {periodo}",
                )
                created += 1

            if created == 0:
                plan.delete()
                return Response(
                    {
                        "detail": "No se creó plan: todos los pronósticos tenían cantidad 0.",
                        "periodo": periodo,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

        return Response(
            {
                "plan_id": plan.id,
                "plan_nombre": plan.nombre,
                "plan_fecha": str(plan.fecha_produccion),
                "periodo": periodo,
                "incluir_preparaciones": incluir_preparaciones,
                "renglones_creados": created,
                "renglones_omitidos_cantidad_cero": skipped,
            },
            status=status.HTTP_201_CREATED,
        )


class ForecastBacktestView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar backtest de pronóstico."},
                status=status.HTTP_403_FORBIDDEN,
            )
        export_format = (request.GET.get("export") or "").strip().lower()
        if export_format and export_format not in {"csv", "xlsx"}:
            return Response(
                {"detail": "export inválido. Usa csv o xlsx."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        ser = ForecastBacktestRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        alcance = data.get("alcance") or "mes"
        fecha_base = data.get("fecha_base") or timezone.localdate()
        incluir_preparaciones = bool(data.get("incluir_preparaciones"))
        mix_adjustment_enabled = bool(data.get("mix_adjustment_enabled"))
        include_mix_compare = bool(data.get("include_mix_compare", True))
        safety_pct = _to_decimal(data.get("safety_pct"), default=Decimal("0"))
        min_confianza_pct = _to_decimal(data.get("min_confianza_pct"), default=Decimal("0"))
        escenario = str(data.get("escenario") or "base").lower()
        periods = int(data.get("periods") or 3)
        top = int(data.get("top") or 10)

        sucursal = None
        sucursal_id = data.get("sucursal_id")
        if sucursal_id is not None:
            sucursal = Sucursal.objects.filter(pk=sucursal_id, activa=True).first()
            if sucursal is None:
                return Response(
                    {"detail": "Sucursal no encontrada o inactiva."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        payload = _build_forecast_backtest_preview(
            alcance=alcance,
            fecha_base=fecha_base,
            periods=periods,
            sucursal=sucursal,
            incluir_preparaciones=incluir_preparaciones,
            safety_pct=safety_pct,
            min_confianza_pct=min_confianza_pct,
            escenario=escenario,
            top=top,
            mix_adjustment_enabled=mix_adjustment_enabled,
            include_mix_compare=include_mix_compare,
        )
        if payload is None:
            return Response(
                {"detail": "No hay historial suficiente para evaluar backtest en el alcance seleccionado."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if export_format:
            return _forecast_backtest_export_response(payload, export_format)
        return Response(payload, status=status.HTTP_200_OK)


class ForecastInsightsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar insights de pronóstico."},
                status=status.HTTP_403_FORBIDDEN,
            )

        months = _parse_bounded_int(request.GET.get("months", 12), default=12, min_value=1, max_value=36)
        top = _parse_bounded_int(request.GET.get("top", 20), default=20, min_value=1, max_value=100)
        offset_top = _parse_bounded_int(request.GET.get("offset_top", 0), default=0, min_value=0, max_value=5000)
        incluir_preparaciones = _parse_bool(request.GET.get("incluir_preparaciones"), default=False)
        export_format = (request.GET.get("export") or "").strip().lower()
        if export_format and export_format not in {"csv", "xlsx"}:
            return Response(
                {"detail": "Parámetro export inválido. Usa 'csv' o 'xlsx'."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        fecha_hasta_raw = (request.GET.get("fecha_hasta") or "").strip()
        fecha_hasta = _parse_iso_date(fecha_hasta_raw)
        if fecha_hasta_raw and fecha_hasta is None:
            return Response(
                {"detail": "fecha_hasta inválida. Usa formato YYYY-MM-DD."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if fecha_hasta is None:
            fecha_hasta = timezone.localdate()
        fecha_desde = fecha_hasta - timedelta(days=(months * 31) - 1)

        sucursal = None
        sucursal_id_raw = (request.GET.get("sucursal_id") or "").strip()
        if sucursal_id_raw:
            if not sucursal_id_raw.isdigit():
                return Response(
                    {"detail": "sucursal_id debe ser numérico."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            sucursal = Sucursal.objects.filter(pk=int(sucursal_id_raw), activa=True).first()
            if sucursal is None:
                return Response(
                    {"detail": "Sucursal no encontrada o inactiva."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        receta = None
        receta_id_raw = (request.GET.get("receta_id") or "").strip()
        if receta_id_raw:
            if not receta_id_raw.isdigit():
                return Response(
                    {"detail": "receta_id debe ser numérico."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            receta = Receta.objects.filter(pk=int(receta_id_raw)).first()
            if receta is None:
                return Response(
                    {"detail": "Receta no encontrada."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        qs = VentaHistorica.objects.filter(fecha__gte=fecha_desde, fecha__lte=fecha_hasta)
        if sucursal:
            qs = qs.filter(sucursal=sucursal)
        if receta:
            qs = qs.filter(receta=receta)
        if not incluir_preparaciones:
            qs = qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)

        rows = list(
            qs.values("fecha", "receta_id", "receta__nombre")
            .annotate(total=Sum("cantidad"))
            .order_by("fecha", "receta_id")
        )
        if not rows:
            payload = {
                "scope": {
                    "months": months,
                    "fecha_desde": str(fecha_desde),
                    "fecha_hasta": str(fecha_hasta),
                    "sucursal_id": sucursal.id if sucursal else None,
                    "sucursal": sucursal.nombre if sucursal else "Todas",
                    "receta_id": receta.id if receta else None,
                    "receta": receta.nombre if receta else "Todas",
                    "top": top,
                    "offset_top": offset_top,
                },
                "totales": {
                    "filas": 0,
                    "dias_con_venta": 0,
                    "recetas": 0,
                    "top_recetas_total": 0,
                    "top_recetas_returned": 0,
                    "cantidad_total": 0.0,
                    "promedio_diario": 0.0,
                },
                "seasonality": {"by_month": [], "by_weekday": []},
                "top_recetas": [],
            }
            if export_format:
                return _forecast_insights_export_response(payload, export_format)
            return Response(payload, status=status.HTTP_200_OK)

        date_totals: dict[date, Decimal] = defaultdict(lambda: Decimal("0"))
        recipe_totals: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))
        recipe_days: dict[int, set[date]] = defaultdict(set)
        recipe_names: dict[int, str] = {}

        for row in rows:
            d = row["fecha"]
            receta_id = int(row["receta_id"])
            qty = _to_decimal(row["total"])
            date_totals[d] += qty
            recipe_totals[receta_id] += qty
            recipe_days[receta_id].add(d)
            recipe_names[receta_id] = row["receta__nombre"]

        daily_values = list(date_totals.values())
        total_qty = sum(daily_values, Decimal("0"))
        global_avg = total_qty / Decimal(str(len(daily_values) or 1))

        month_map: dict[int, list[Decimal]] = defaultdict(list)
        weekday_map: dict[int, list[Decimal]] = defaultdict(list)
        for d, qty in date_totals.items():
            month_map[d.month].append(qty)
            weekday_map[d.weekday()].append(qty)

        month_labels = {
            1: "Ene",
            2: "Feb",
            3: "Mar",
            4: "Abr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Ago",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dic",
        }
        weekday_labels = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"]

        month_rows = []
        for month in sorted(month_map.keys()):
            samples = month_map[month]
            avg_qty = sum(samples, Decimal("0")) / Decimal(str(len(samples) or 1))
            index_pct = Decimal("100")
            if global_avg > 0:
                index_pct = (avg_qty / global_avg) * Decimal("100")
            month_rows.append(
                {
                    "month": month,
                    "label": month_labels.get(month, str(month)),
                    "samples": len(samples),
                    "avg_qty": _to_float(avg_qty),
                    "index_pct": _to_float(index_pct.quantize(Decimal("0.1"))),
                }
            )

        weekday_rows = []
        for wd in range(7):
            samples = weekday_map.get(wd, [])
            avg_qty = Decimal("0")
            index_pct = Decimal("0")
            if samples:
                avg_qty = sum(samples, Decimal("0")) / Decimal(str(len(samples)))
                index_pct = Decimal("100")
                if global_avg > 0:
                    index_pct = (avg_qty / global_avg) * Decimal("100")
            weekday_rows.append(
                {
                    "weekday": wd,
                    "label": weekday_labels[wd],
                    "samples": len(samples),
                    "avg_qty": _to_float(avg_qty),
                    "index_pct": _to_float(index_pct.quantize(Decimal("0.1"))) if samples else 0.0,
                }
            )

        top_recetas_all = []
        for receta_id, qty_total in sorted(recipe_totals.items(), key=lambda item: item[1], reverse=True):
            days_count = len(recipe_days.get(receta_id, set())) or 1
            avg_day = qty_total / Decimal(str(days_count))
            share = Decimal("0")
            if total_qty > 0:
                share = (qty_total / total_qty) * Decimal("100")
            top_recetas_all.append(
                {
                    "receta_id": receta_id,
                    "receta": recipe_names.get(receta_id) or f"Receta {receta_id}",
                    "cantidad_total": _to_float(qty_total),
                    "promedio_dia_activo": _to_float(avg_day.quantize(Decimal("0.001"))),
                    "dias_con_venta": days_count,
                    "participacion_pct": _to_float(share.quantize(Decimal("0.1"))),
                }
            )
        top_recetas = top_recetas_all[offset_top : offset_top + top]

        payload = {
            "scope": {
                "months": months,
                "fecha_desde": str(fecha_desde),
                "fecha_hasta": str(fecha_hasta),
                "sucursal_id": sucursal.id if sucursal else None,
                "sucursal": sucursal.nombre if sucursal else "Todas",
                "receta_id": receta.id if receta else None,
                "receta": receta.nombre if receta else "Todas",
                "top": top,
                "offset_top": offset_top,
            },
            "totales": {
                "filas": len(rows),
                "dias_con_venta": len(date_totals),
                "recetas": len(recipe_totals),
                "top_recetas_total": len(top_recetas_all),
                "top_recetas_returned": len(top_recetas),
                "cantidad_total": _to_float(total_qty),
                "promedio_diario": _to_float(global_avg.quantize(Decimal("0.001"))),
            },
            "seasonality": {
                "by_month": month_rows,
                "by_weekday": weekday_rows,
            },
            "top_recetas": top_recetas,
        }
        if export_format:
            return _forecast_insights_export_response(payload, export_format)
        return Response(payload, status=status.HTTP_200_OK)



