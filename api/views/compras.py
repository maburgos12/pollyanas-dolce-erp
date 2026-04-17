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



class ComprasSolicitudesImportPreviewView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not can_manage_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para importar solicitudes de compras."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ComprasSolicitudImportPreviewSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        periodo_tipo = (data.get("periodo_tipo") or "mes").strip() or "mes"
        periodo_mes = (data.get("periodo_mes") or "").strip()
        default_fecha = _default_fecha_requerida(periodo_tipo, periodo_mes)
        area_default = (data.get("area_default") or "Compras").strip() or "Compras"
        solicitante_default = (data.get("solicitante_default") or request.user.username).strip() or request.user.username
        estatus_default = (data.get("estatus_default") or SolicitudCompra.STATUS_BORRADOR).strip().upper()
        valid_status = {x[0] for x in SolicitudCompra.STATUS_CHOICES}
        if estatus_default not in valid_status:
            estatus_default = SolicitudCompra.STATUS_BORRADOR
        evitar_duplicados = bool(data.get("evitar_duplicados", True))
        min_score = int(data.get("score_min") or 90)
        top = int(data.get("top") or 120)

        provider_map = {
            normalizar_nombre(p.nombre): p
            for p in Proveedor.objects.filter(activo=True).only("id", "nombre")
        }
        match_cache: dict[str, tuple[Insumo | None, float, str]] = {}
        parsed_rows: list[dict] = []
        duplicate_keys_to_check: set[tuple[str, int, date]] = set()

        for idx, row in enumerate(data.get("rows") or [], start=1):
            insumo_raw = str(row.get("insumo") or "").strip()
            cantidad = _to_decimal(row.get("cantidad"), Decimal("0"))
            if insumo_raw:
                cache_key = normalizar_nombre(insumo_raw)
                insumo_match, score, method = match_cache.get(cache_key, (None, 0.0, "sin_match"))
                if cache_key not in match_cache:
                    insumo_match, score, method = match_insumo(insumo_raw)
                    insumo_match = canonical_insumo(insumo_match)
                    match_cache[cache_key] = (insumo_match, score, method)
            else:
                insumo_match, score, method = (None, 0.0, "sin_match")
            insumo_id = int(insumo_match.id) if (insumo_match and score >= min_score) else 0

            area = str(row.get("area") or area_default).strip() or area_default
            solicitante = str(row.get("solicitante") or solicitante_default).strip() or solicitante_default
            fecha_requerida = _parse_date_value(row.get("fecha_requerida"), default_fecha)
            estatus = str(row.get("estatus") or estatus_default).strip().upper()
            if estatus not in valid_status:
                estatus = estatus_default
            proveedor = _resolve_proveedor_name(str(row.get("proveedor") or ""), provider_map)
            if not proveedor and insumo_match:
                proveedor = insumo_match.proveedor_principal

            parsed_rows.append(
                {
                    "row_id": str(idx),
                    "source_row": idx,
                    "insumo_origen": insumo_raw,
                    "insumo_sugerencia": insumo_match.nombre if insumo_match else "",
                    "insumo_id": insumo_id,
                    "cantidad": cantidad,
                    "area": area,
                    "solicitante": solicitante,
                    "fecha_requerida": fecha_requerida,
                    "estatus": estatus,
                    "proveedor_id": int(proveedor.id) if proveedor else 0,
                    "score": float(score or 0),
                    "metodo": method,
                    "has_insumo_match": bool(insumo_match),
                }
            )
            if evitar_duplicados and insumo_id:
                duplicate_keys_to_check.add((area, insumo_id, fecha_requerida))

        duplicates_found: set[tuple[str, int, date]] = set()
        if duplicate_keys_to_check:
            areas = sorted({k[0] for k in duplicate_keys_to_check})
            insumo_ids = sorted({k[1] for k in duplicate_keys_to_check})
            fechas = sorted({k[2] for k in duplicate_keys_to_check})
            duplicates_found = {
                (area, int(insumo_id), fecha)
                for area, insumo_id, fecha in SolicitudCompra.objects.filter(
                    area__in=areas,
                    insumo_id__in=insumo_ids,
                    fecha_requerida__in=fechas,
                    estatus__in=_active_solicitud_statuses(),
                ).values_list("area", "insumo_id", "fecha_requerida")
            }

        preview_cost_by_insumo: dict[int, Decimal] = {}
        preview_insumo_ids = sorted({int(p["insumo_id"]) for p in parsed_rows if int(p["insumo_id"] or 0) > 0})
        if preview_insumo_ids:
            for c in CostoInsumo.objects.filter(insumo_id__in=preview_insumo_ids).order_by("insumo_id", "-fecha", "-id"):
                if c.insumo_id not in preview_cost_by_insumo:
                    preview_cost_by_insumo[c.insumo_id] = c.costo_unitario

        preview_rows: list[dict] = []
        ready_count = 0
        duplicates_count = 0
        without_match_count = 0
        invalid_qty_count = 0
        ready_qty_total = Decimal("0")
        ready_budget_total = Decimal("0")
        for parsed in parsed_rows:
            insumo_raw = str(parsed["insumo_origen"] or "").strip()
            cantidad = parsed["cantidad"]
            insumo_id = parsed["insumo_id"]
            costo_unitario = preview_cost_by_insumo.get(insumo_id, Decimal("0")) if insumo_id else Decimal("0")
            presupuesto_estimado = (cantidad * costo_unitario) if cantidad > 0 else Decimal("0")
            area = parsed["area"]
            fecha_requerida = parsed["fecha_requerida"]
            duplicate = bool(insumo_id and (area, insumo_id, fecha_requerida) in duplicates_found)

            notes: list[str] = []
            hard_error = False
            if not insumo_raw:
                notes.append("Insumo vacío en fila.")
                hard_error = True
            if not insumo_id:
                if parsed["has_insumo_match"]:
                    notes.append(f"Score de match insuficiente (<{min_score}).")
                else:
                    notes.append("Sin match de insumo.")
                hard_error = True
                without_match_count += 1
            if cantidad <= 0:
                notes.append("Cantidad inválida (debe ser > 0).")
                hard_error = True
                invalid_qty_count += 1
            if duplicate:
                notes.append("Posible duplicado con solicitud activa.")
                duplicates_count += 1

            include = not hard_error
            if include and cantidad > 0:
                ready_count += 1
                ready_qty_total += cantidad
                ready_budget_total += max(presupuesto_estimado, Decimal("0"))

            preview_rows.append(
                {
                    "row_id": parsed["row_id"],
                    "source_row": parsed["source_row"],
                    "insumo_origen": insumo_raw,
                    "insumo_sugerencia": parsed["insumo_sugerencia"],
                    "insumo_id": parsed["insumo_id"] or None,
                    "cantidad": str(cantidad),
                    "area": parsed["area"],
                    "solicitante": parsed["solicitante"],
                    "fecha_requerida": fecha_requerida.isoformat(),
                    "estatus": parsed["estatus"],
                    "proveedor_id": parsed["proveedor_id"] or None,
                    "score": round(float(parsed["score"] or 0), 1),
                    "metodo": parsed["metodo"],
                    "costo_unitario": str(costo_unitario),
                    "presupuesto_estimado": str(presupuesto_estimado),
                    "duplicate": duplicate,
                    "notes": " | ".join(notes),
                    "include": include,
                }
            )

        return Response(
            {
                "preview": {
                    "periodo_tipo": periodo_tipo,
                    "periodo_mes": periodo_mes,
                    "evitar_duplicados": evitar_duplicados,
                    "score_min": min_score,
                    "rows": preview_rows[:top],
                },
                "totales": {
                    "filas": len(preview_rows),
                    "ready_count": ready_count,
                    "excluded_count": max(0, len(preview_rows) - ready_count),
                    "duplicates_count": duplicates_count,
                    "without_match_count": without_match_count,
                    "invalid_qty_count": invalid_qty_count,
                    "ready_qty_total": str(ready_qty_total),
                    "ready_budget_total": str(ready_budget_total),
                },
            },
            status=status.HTTP_200_OK,
        )


class ComprasSolicitudesImportConfirmView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not can_manage_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para confirmar importación de solicitudes."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ComprasSolicitudImportConfirmSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        rows = data.get("rows") or []
        periodo_tipo = (data.get("periodo_tipo") or "mes").strip() or "mes"
        periodo_mes = (data.get("periodo_mes") or "").strip()
        evitar_duplicados = bool(data.get("evitar_duplicados", True))
        default_fecha = _default_fecha_requerida(periodo_tipo, periodo_mes)
        valid_status = {x[0] for x in SolicitudCompra.STATUS_CHOICES}

        normalized_rows: list[dict] = []
        for row in rows:
            row_copy = dict(row)
            canonical = canonical_insumo_by_id(row_copy.get("insumo_id"))
            if canonical:
                row_copy["insumo_id"] = canonical.id
            normalized_rows.append(row_copy)

        included_rows = [row for row in normalized_rows if bool(row.get("include", True))]
        insumo_ids = sorted({int(row.get("insumo_id") or 0) for row in included_rows if int(row.get("insumo_id") or 0) > 0})
        proveedor_ids = sorted(
            {
                int(row.get("proveedor_id") or 0)
                for row in included_rows
                if int(row.get("proveedor_id") or 0) > 0
            }
        )

        insumos_map = {}
        for insumo in Insumo.objects.filter(id__in=insumo_ids, activo=True).select_related("proveedor_principal"):
            canonical = canonical_insumo(insumo)
            if canonical:
                insumos_map[canonical.id] = canonical
        proveedores_map = Proveedor.objects.filter(activo=True, id__in=proveedor_ids).in_bulk()

        existing_duplicate_keys: set[tuple[str, int, date]] = set()
        if evitar_duplicados:
            batch_keys: set[tuple[str, int, date]] = set()
            for row in included_rows:
                area = (str(row.get("area") or "").strip() or "General")
                insumo_id = int(row.get("insumo_id") or 0)
                if insumo_id <= 0:
                    continue
                fecha_requerida = _parse_date_value(row.get("fecha_requerida"), default_fecha)
                batch_keys.add((area, insumo_id, fecha_requerida))
            if batch_keys:
                areas = sorted({k[0] for k in batch_keys})
                insumo_ids_batch = sorted({k[1] for k in batch_keys})
                fechas = sorted({k[2] for k in batch_keys})
                existing_duplicate_keys = {
                    (area, int(insumo_id), fecha)
                    for area, insumo_id, fecha in SolicitudCompra.objects.filter(
                        area__in=areas,
                        insumo_id__in=insumo_ids_batch,
                        fecha_requerida__in=fechas,
                        estatus__in=_active_solicitud_statuses(),
                    ).values_list("area", "insumo_id", "fecha_requerida")
                }

        created = 0
        skipped_invalid = 0
        skipped_duplicate = 0
        skipped_removed = max(0, len(rows) - len(included_rows))
        created_duplicate_keys: set[tuple[str, int, date]] = set()
        created_items: list[dict] = []

        with transaction.atomic():
            for row in normalized_rows:
                if not bool(row.get("include", True)):
                    continue

                area = (str(row.get("area") or "").strip() or "General")
                solicitante = (str(row.get("solicitante") or "").strip() or request.user.username)
                estatus = (str(row.get("estatus") or SolicitudCompra.STATUS_BORRADOR).strip().upper())
                if estatus not in valid_status:
                    estatus = SolicitudCompra.STATUS_BORRADOR

                insumo_id = int(row.get("insumo_id") or 0)
                insumo = insumos_map.get(insumo_id)
                if not insumo:
                    skipped_invalid += 1
                    continue

                cantidad = _to_decimal(row.get("cantidad"), Decimal("0"))
                if cantidad <= 0:
                    skipped_invalid += 1
                    continue

                fecha_requerida = _parse_date_value(row.get("fecha_requerida"), default_fecha)
                proveedor = proveedores_map.get(int(row.get("proveedor_id") or 0))
                if not proveedor:
                    proveedor = insumo.proveedor_principal

                duplicate_key = (area, int(insumo.id), fecha_requerida)
                if evitar_duplicados and ((duplicate_key in existing_duplicate_keys) or (duplicate_key in created_duplicate_keys)):
                    skipped_duplicate += 1
                    continue

                solicitud = SolicitudCompra.objects.create(
                    area=area,
                    solicitante=solicitante,
                    insumo=insumo,
                    proveedor_sugerido=proveedor,
                    cantidad=cantidad,
                    fecha_requerida=fecha_requerida,
                    estatus=estatus,
                )
                log_event(
                    request.user,
                    "CREATE",
                    "compras.SolicitudCompra",
                    solicitud.id,
                    {"folio": solicitud.folio, "source": "api_import_confirm"},
                )
                if evitar_duplicados:
                    created_duplicate_keys.add(duplicate_key)
                created += 1
                if len(created_items) < 80:
                    canonical_created = canonical_insumo_by_id(solicitud.insumo_id) or solicitud.insumo
                    created_items.append(
                        {
                            "id": solicitud.id,
                            "folio": solicitud.folio,
                            "insumo": canonical_created.nombre if canonical_created else solicitud.insumo.nombre,
                            "cantidad": str(solicitud.cantidad),
                            "fecha_requerida": solicitud.fecha_requerida.isoformat(),
                            "estatus": solicitud.estatus,
                        }
                    )

        return Response(
            {
                "periodo_tipo": periodo_tipo,
                "periodo_mes": periodo_mes,
                "totales": {
                    "rows": len(rows),
                    "created": created,
                    "skipped_removed": skipped_removed,
                    "skipped_duplicate": skipped_duplicate,
                    "skipped_invalid": skipped_invalid,
                },
                "items": created_items,
            },
            status=status.HTTP_200_OK,
        )


class ComprasSolicitudesListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar solicitudes de compra."},
                status=status.HTTP_403_FORBIDDEN,
            )

        estatus = (request.GET.get("estatus") or "").strip().upper()
        area = (request.GET.get("area") or "").strip()
        q = (request.GET.get("q") or "").strip()
        periodo = (request.GET.get("mes") or request.GET.get("periodo") or "").strip()
        limit = _parse_bounded_int(request.GET.get("limit", 120), default=120, min_value=1, max_value=500)
        offset = _parse_bounded_int(request.GET.get("offset", 0), default=0, min_value=0, max_value=200000)
        sort_by = (request.GET.get("sort_by") or "creado_en").strip().lower()
        sort_dir = (request.GET.get("sort_dir") or "desc").strip().lower()
        allowed_sort = {
            "creado_en": "creado_en",
            "folio": "folio",
            "fecha_requerida": "fecha_requerida",
            "cantidad": "cantidad",
            "estatus": "estatus",
            "area": "area",
            "solicitante": "solicitante",
            "insumo": "insumo__nombre",
            "proveedor": "proveedor_sugerido__nombre",
        }
        if sort_by not in allowed_sort:
            return Response(
                {
                    "detail": (
                        "sort_by inválido. Usa creado_en, folio, fecha_requerida, cantidad, estatus, "
                        "area, solicitante, insumo o proveedor."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        if sort_dir not in {"asc", "desc"}:
            return Response(
                {"detail": "sort_dir inválido. Usa asc o desc."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        qs = (
            SolicitudCompra.objects.select_related("insumo", "insumo__unidad_base", "proveedor_sugerido")
            .order_by("-creado_en")
        )
        valid_status = {choice[0] for choice in SolicitudCompra.STATUS_CHOICES}
        if estatus in valid_status:
            qs = qs.filter(estatus=estatus)
        else:
            estatus = ""
        if area:
            qs = qs.filter(area__icontains=area)
        parsed_period = _parse_period(periodo)
        if parsed_period:
            y, m = parsed_period
            qs = qs.filter(fecha_requerida__year=y, fecha_requerida__month=m)
            periodo = f"{y:04d}-{m:02d}"
        else:
            periodo = ""
        if q:
            qs = qs.filter(
                Q(folio__icontains=q)
                | Q(solicitante__icontains=q)
                | Q(area__icontains=q)
                | Q(insumo__nombre__icontains=q)
                | Q(proveedor_sugerido__nombre__icontains=q)
            )
        sort_field = allowed_sort[sort_by]
        order_expr = sort_field if sort_dir == "asc" else f"-{sort_field}"
        qs = qs.order_by(order_expr, "-id")

        rows_total = qs.count()
        by_status = {k: 0 for k in valid_status}
        for row in qs.order_by().values("estatus").annotate(rows=Count("id")):
            key = row.get("estatus")
            if key in by_status:
                by_status[key] = int(row.get("rows") or 0)

        rows = list(qs[offset : offset + limit])
        canonical_rows = []
        insumo_ids = set()
        total_qty_by_canonical: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))
        for r in rows:
            canonical = canonical_insumo(r.insumo)
            canonical_rows.append((r, canonical))
            if canonical:
                insumo_ids.add(canonical.id)
        for row in qs:
            canonical = canonical_insumo(row.insumo)
            if not canonical:
                continue
            insumo_ids.add(canonical.id)
            total_qty_by_canonical[canonical.id] += _to_decimal(row.cantidad)
        canonical_catalog_rows = {
            row["canonical"].id: row
            for row in canonicalized_active_insumos(limit=5000)
            if row["canonical"].id in insumo_ids
        }
        costo_ids = sorted(
            {
                member.id
                for row in canonical_catalog_rows.values()
                for member in row["items"]
            }
        )
        latest_cost_by_insumo: dict[int, Decimal] = {}
        if costo_ids:
            for c in CostoInsumo.objects.filter(insumo_id__in=costo_ids).order_by("insumo_id", "-fecha", "-id"):
                canonical = canonical_insumo_by_id(c.insumo_id)
                if canonical and canonical.id not in latest_cost_by_insumo:
                    latest_cost_by_insumo[canonical.id] = _to_decimal(c.costo_unitario)

        items = []
        presupuesto_total = Decimal("0")
        presupuesto_total_filtered = Decimal("0")
        for insumo_id, qty in total_qty_by_canonical.items():
            costo_unitario = _to_decimal(latest_cost_by_insumo.get(insumo_id, 0)).quantize(Decimal("0.01"))
            presupuesto_total_filtered += (qty * costo_unitario).quantize(Decimal("0.01"))
        for r, canonical in canonical_rows:
            display_insumo = canonical or r.insumo
            costo_unitario = _to_decimal(latest_cost_by_insumo.get(display_insumo.id, 0)).quantize(
                Decimal("0.01")
            )
            presupuesto = (_to_decimal(r.cantidad) * costo_unitario).quantize(Decimal("0.01"))
            presupuesto_total += presupuesto
            items.append(
                {
                    "id": r.id,
                    "folio": r.folio,
                    "estatus": r.estatus,
                    "area": r.area,
                    "solicitante": r.solicitante,
                    "insumo_id": display_insumo.id,
                    "insumo": display_insumo.nombre,
                    "unidad": (
                        display_insumo.unidad_base.codigo
                        if display_insumo.unidad_base_id and display_insumo.unidad_base
                        else ""
                    ),
                    "cantidad": str(r.cantidad),
                    "fecha_requerida": str(r.fecha_requerida),
                    "proveedor_sugerido": r.proveedor_sugerido.nombre if r.proveedor_sugerido_id else "",
                    "costo_unitario": str(costo_unitario),
                    "presupuesto_estimado": str(presupuesto),
                    "creado_en": r.creado_en,
                }
            )

        return Response(
            {
                "filters": {
                    "estatus": estatus,
                    "area": area,
                    "q": q,
                    "periodo": periodo,
                    "limit": limit,
                    "offset": offset,
                    "sort_by": sort_by,
                    "sort_dir": sort_dir,
                },
                "totales": {
                    "rows": len(items),
                    "rows_total": rows_total,
                    "rows_returned": len(items),
                    "presupuesto_estimado_total": str(presupuesto_total),
                    "presupuesto_estimado_total_filtered": str(presupuesto_total_filtered),
                    "by_status": by_status,
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )


class ComprasOrdenesListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar órdenes de compra."},
                status=status.HTTP_403_FORBIDDEN,
            )

        estatus = (request.GET.get("estatus") or "").strip().upper()
        q = (request.GET.get("q") or "").strip()
        periodo = (request.GET.get("mes") or request.GET.get("periodo") or "").strip()
        proveedor_id_raw = (request.GET.get("proveedor_id") or "").strip()
        limit = _parse_bounded_int(request.GET.get("limit", 120), default=120, min_value=1, max_value=500)
        offset = _parse_bounded_int(request.GET.get("offset", 0), default=0, min_value=0, max_value=200000)
        sort_by = (request.GET.get("sort_by") or "creado_en").strip().lower()
        sort_dir = (request.GET.get("sort_dir") or "desc").strip().lower()
        allowed_sort = {
            "creado_en": "creado_en",
            "folio": "folio",
            "fecha_emision": "fecha_emision",
            "fecha_entrega_estimada": "fecha_entrega_estimada",
            "monto_estimado": "monto_estimado",
            "estatus": "estatus",
            "proveedor": "proveedor__nombre",
            "referencia": "referencia",
        }
        if sort_by not in allowed_sort:
            return Response(
                {
                    "detail": (
                        "sort_by inválido. Usa creado_en, folio, fecha_emision, fecha_entrega_estimada, "
                        "monto_estimado, estatus, proveedor o referencia."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        if sort_dir not in {"asc", "desc"}:
            return Response(
                {"detail": "sort_dir inválido. Usa asc o desc."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        qs = OrdenCompra.objects.select_related("proveedor", "solicitud", "solicitud__insumo").order_by("-creado_en")
        valid_status = {choice[0] for choice in OrdenCompra.STATUS_CHOICES}
        if estatus in valid_status:
            qs = qs.filter(estatus=estatus)
        else:
            estatus = ""

        if proveedor_id_raw.isdigit():
            qs = qs.filter(proveedor_id=int(proveedor_id_raw))
        else:
            proveedor_id_raw = ""

        parsed_period = _parse_period(periodo)
        if parsed_period:
            y, m = parsed_period
            qs = qs.filter(fecha_emision__year=y, fecha_emision__month=m)
            periodo = f"{y:04d}-{m:02d}"
        else:
            periodo = ""

        if q:
            qs = qs.filter(
                Q(folio__icontains=q)
                | Q(referencia__icontains=q)
                | Q(proveedor__nombre__icontains=q)
                | Q(solicitud__folio__icontains=q)
            )
        sort_field = allowed_sort[sort_by]
        order_expr = sort_field if sort_dir == "asc" else f"-{sort_field}"
        qs = qs.order_by(order_expr, "-id")

        rows_total = qs.count()
        monto_total_filtered = _to_decimal(qs.aggregate(total=Sum("monto_estimado"))["total"])
        by_status = {k: 0 for k in valid_status}
        for row in qs.order_by().values("estatus").annotate(rows=Count("id")):
            key = row.get("estatus")
            if key in by_status:
                by_status[key] = int(row.get("rows") or 0)

        rows = list(qs[offset : offset + limit])
        items = []
        monto_total = Decimal("0")
        for r in rows:
            monto = _to_decimal(r.monto_estimado)
            monto_total += monto
            canonical_row_insumo = (
                canonical_insumo_by_id(r.solicitud.insumo_id)
                if r.solicitud_id and getattr(r.solicitud, "insumo_id", None) and r.solicitud
                else None
            )
            items.append(
                {
                    "id": r.id,
                    "folio": r.folio,
                    "estatus": r.estatus,
                    "proveedor_id": r.proveedor_id,
                    "proveedor": r.proveedor.nombre if r.proveedor_id else "",
                    "solicitud_id": r.solicitud_id,
                    "solicitud_folio": r.solicitud.folio if r.solicitud_id and r.solicitud else "",
                    "insumo": (
                        canonical_row_insumo.nombre
                        if canonical_row_insumo
                        else ""
                    ),
                    "referencia": r.referencia,
                    "fecha_emision": str(r.fecha_emision) if r.fecha_emision else "",
                    "fecha_entrega_estimada": str(r.fecha_entrega_estimada) if r.fecha_entrega_estimada else "",
                    "monto_estimado": str(monto),
                    "creado_en": r.creado_en,
                }
            )

        return Response(
            {
                "filters": {
                    "estatus": estatus,
                    "q": q,
                    "periodo": periodo,
                    "proveedor_id": proveedor_id_raw,
                    "limit": limit,
                    "offset": offset,
                    "sort_by": sort_by,
                    "sort_dir": sort_dir,
                },
                "totales": {
                    "rows": len(items),
                    "rows_total": rows_total,
                    "rows_returned": len(items),
                    "monto_estimado_total": str(monto_total),
                    "monto_estimado_total_filtered": str(monto_total_filtered),
                    "by_status": by_status,
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )


class ComprasRecepcionesListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar recepciones de compra."},
                status=status.HTTP_403_FORBIDDEN,
            )

        estatus = (request.GET.get("estatus") or "").strip().upper()
        q = (request.GET.get("q") or "").strip()
        periodo = (request.GET.get("mes") or request.GET.get("periodo") or "").strip()
        proveedor_id_raw = (request.GET.get("proveedor_id") or "").strip()
        limit = _parse_bounded_int(request.GET.get("limit", 120), default=120, min_value=1, max_value=500)
        offset = _parse_bounded_int(request.GET.get("offset", 0), default=0, min_value=0, max_value=200000)
        sort_by = (request.GET.get("sort_by") or "creado_en").strip().lower()
        sort_dir = (request.GET.get("sort_dir") or "desc").strip().lower()
        allowed_sort = {
            "creado_en": "creado_en",
            "folio": "folio",
            "fecha_recepcion": "fecha_recepcion",
            "conformidad_pct": "conformidad_pct",
            "estatus": "estatus",
            "proveedor": "orden__proveedor__nombre",
            "orden_folio": "orden__folio",
        }
        if sort_by not in allowed_sort:
            return Response(
                {
                    "detail": (
                        "sort_by inválido. Usa creado_en, folio, fecha_recepcion, conformidad_pct, "
                        "estatus, proveedor u orden_folio."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        if sort_dir not in {"asc", "desc"}:
            return Response(
                {"detail": "sort_dir inválido. Usa asc o desc."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        qs = RecepcionCompra.objects.select_related("orden", "orden__proveedor").order_by("-creado_en")
        valid_status = {choice[0] for choice in RecepcionCompra.STATUS_CHOICES}
        if estatus in valid_status:
            qs = qs.filter(estatus=estatus)
        else:
            estatus = ""

        if proveedor_id_raw.isdigit():
            qs = qs.filter(orden__proveedor_id=int(proveedor_id_raw))
        else:
            proveedor_id_raw = ""

        parsed_period = _parse_period(periodo)
        if parsed_period:
            y, m = parsed_period
            qs = qs.filter(fecha_recepcion__year=y, fecha_recepcion__month=m)
            periodo = f"{y:04d}-{m:02d}"
        else:
            periodo = ""

        if q:
            qs = qs.filter(
                Q(folio__icontains=q)
                | Q(orden__folio__icontains=q)
                | Q(orden__proveedor__nombre__icontains=q)
                | Q(observaciones__icontains=q)
            )
        sort_field = allowed_sort[sort_by]
        order_expr = sort_field if sort_dir == "asc" else f"-{sort_field}"
        qs = qs.order_by(order_expr, "-id")

        rows_total = qs.count()
        by_status = {k: 0 for k in valid_status}
        for row in qs.order_by().values("estatus").annotate(rows=Count("id")):
            key = row.get("estatus")
            if key in by_status:
                by_status[key] = int(row.get("rows") or 0)

        rows = list(qs[offset : offset + limit])
        items = []
        for r in rows:
            items.append(
                {
                    "id": r.id,
                    "folio": r.folio,
                    "estatus": r.estatus,
                    "orden_id": r.orden_id,
                    "orden_folio": r.orden.folio if r.orden_id and r.orden else "",
                    "orden_estatus": r.orden.estatus if r.orden_id and r.orden else "",
                    "proveedor_id": r.orden.proveedor_id if r.orden_id and r.orden else None,
                    "proveedor": (
                        r.orden.proveedor.nombre
                        if r.orden_id and r.orden and r.orden.proveedor_id and r.orden.proveedor
                        else ""
                    ),
                    "fecha_recepcion": str(r.fecha_recepcion) if r.fecha_recepcion else "",
                    "conformidad_pct": str(_to_decimal(r.conformidad_pct)),
                    "observaciones": r.observaciones,
                    "creado_en": r.creado_en,
                }
            )

        return Response(
            {
                "filters": {
                    "estatus": estatus,
                    "q": q,
                    "periodo": periodo,
                    "proveedor_id": proveedor_id_raw,
                    "limit": limit,
                    "offset": offset,
                    "sort_by": sort_by,
                    "sort_dir": sort_dir,
                },
                "totales": {
                    "rows": len(items),
                    "rows_total": rows_total,
                    "rows_returned": len(items),
                    "by_status": by_status,
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )


class ComprasSolicitudCreateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not can_manage_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para crear solicitudes de compra."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ComprasSolicitudCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        insumo = canonical_insumo_by_id(data["insumo_id"])
        if insumo is None:
            return Response(
                {"detail": "insumo_id no encontrado o inactivo."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        solicitante = (data.get("solicitante") or request.user.username or "").strip() or request.user.username
        area = (data["area"] or "").strip() or "General"
        auto_crear_orden = bool(data.get("auto_crear_orden"))
        orden_estatus = data.get("orden_estatus") or OrdenCompra.STATUS_BORRADOR
        if auto_crear_orden and not insumo.proveedor_principal_id:
            return Response(
                {
                    "detail": (
                        "No se pudo crear OC automática: el insumo no tiene proveedor "
                        "principal configurado."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic():
            solicitud = SolicitudCompra.objects.create(
                area=area,
                solicitante=solicitante[:120],
                insumo=insumo,
                proveedor_sugerido=insumo.proveedor_principal,
                cantidad=data["cantidad"],
                fecha_requerida=data.get("fecha_requerida") or timezone.localdate(),
                estatus=data.get("estatus") or SolicitudCompra.STATUS_BORRADOR,
            )

            orden = None
            if auto_crear_orden:
                proveedor = solicitud.proveedor_sugerido or insumo.proveedor_principal
                _, costo = _latest_cost_for_canonical(insumo.id, proveedor)
                costo_unitario = _to_decimal(costo.costo_unitario if costo else 0)
                monto_estimado = (data["cantidad"] * costo_unitario).quantize(Decimal("0.01"))
                orden = OrdenCompra.objects.create(
                    solicitud=solicitud,
                    proveedor=proveedor,
                    estatus=orden_estatus,
                    monto_estimado=monto_estimado,
                )

        payload = {
            "id": solicitud.id,
            "folio": solicitud.folio,
            "area": solicitud.area,
            "solicitante": solicitud.solicitante,
            "insumo_id": solicitud.insumo_id,
            "insumo": solicitud.insumo.nombre,
            "cantidad": str(solicitud.cantidad),
            "fecha_requerida": str(solicitud.fecha_requerida),
            "estatus": solicitud.estatus,
            "proveedor_sugerido_id": solicitud.proveedor_sugerido_id,
            "proveedor_sugerido": solicitud.proveedor_sugerido.nombre if solicitud.proveedor_sugerido_id else "",
            "auto_crear_orden": auto_crear_orden,
            "orden_id": orden.id if orden else None,
            "orden_folio": orden.folio if orden else "",
            "orden_estatus": orden.estatus if orden else "",
        }
        return Response(payload, status=status.HTTP_201_CREATED)


class ComprasSolicitudStatusUpdateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, solicitud_id: int):
        if not can_manage_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para aprobar/rechazar solicitudes de compra."},
                status=status.HTTP_403_FORBIDDEN,
            )

        solicitud = get_object_or_404(SolicitudCompra.objects.select_related("insumo"), pk=solicitud_id)
        ser = ComprasSolicitudStatusSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        estatus_new = ser.validated_data["estatus"]

        estatus_prev = solicitud.estatus
        if estatus_prev == estatus_new:
            return Response(
                {
                    "id": solicitud.id,
                    "folio": solicitud.folio,
                    "from": estatus_prev,
                    "to": estatus_new,
                    "updated": False,
                },
                status=status.HTTP_200_OK,
            )

        if not _can_transition_solicitud(estatus_prev, estatus_new):
            return Response(
                {
                    "detail": (
                        f"Transición inválida de solicitud: {estatus_prev} -> {estatus_new}."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        solicitud.estatus = estatus_new
        solicitud.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.SolicitudCompra",
            solicitud.id,
            {"from": estatus_prev, "to": estatus_new, "folio": solicitud.folio, "source": "api"},
        )

        return Response(
            {
                "id": solicitud.id,
                "folio": solicitud.folio,
                "from": estatus_prev,
                "to": estatus_new,
                "updated": True,
            },
            status=status.HTTP_200_OK,
        )


class ComprasSolicitudCrearOrdenView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, solicitud_id: int):
        if not can_manage_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para crear órdenes desde solicitud."},
                status=status.HTTP_403_FORBIDDEN,
            )

        solicitud = get_object_or_404(
            SolicitudCompra.objects.select_related("insumo", "proveedor_sugerido", "insumo__proveedor_principal"),
            pk=solicitud_id,
        )
        if solicitud.estatus != SolicitudCompra.STATUS_APROBADA:
            return Response(
                {"detail": f"La solicitud {solicitud.folio} no está aprobada."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        existing = (
            OrdenCompra.objects.filter(solicitud=solicitud)
            .exclude(estatus=OrdenCompra.STATUS_CERRADA)
            .select_related("proveedor")
            .first()
        )
        if existing:
            return Response(
                {
                    "created": False,
                    "id": existing.id,
                    "folio": existing.folio,
                    "estatus": existing.estatus,
                    "proveedor": existing.proveedor.nombre,
                    "solicitud_folio": solicitud.folio,
                },
                status=status.HTTP_200_OK,
            )

        ser = ComprasCrearOrdenSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data
        estatus = data.get("estatus") or OrdenCompra.STATUS_BORRADOR
        canonical_solicitud_insumo = canonical_insumo_by_id(solicitud.insumo_id) or solicitud.insumo
        if estatus not in {OrdenCompra.STATUS_BORRADOR, OrdenCompra.STATUS_ENVIADA}:
            return Response(
                {
                    "detail": (
                        "Estatus inicial de OC inválido. "
                        "Solo se permite BORRADOR o ENVIADA al crear desde solicitud."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        proveedor = None
        proveedor_id = data.get("proveedor_id")
        if proveedor_id:
            proveedor = get_object_or_404(Proveedor, pk=proveedor_id, activo=True)
        if proveedor is None:
            proveedor = solicitud.proveedor_sugerido or canonical_solicitud_insumo.proveedor_principal or solicitud.insumo.proveedor_principal
        if proveedor is None:
            return Response(
                {
                    "detail": (
                        f"La solicitud {solicitud.folio} no tiene proveedor sugerido "
                        "ni proveedor principal en el insumo."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        _, costo = _latest_cost_for_canonical(solicitud.insumo_id, proveedor)
        costo_unitario = _to_decimal(costo.costo_unitario if costo else 0)
        monto_estimado = ((solicitud.cantidad or Decimal("0")) * costo_unitario).quantize(Decimal("0.01"))

        orden = OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=proveedor,
            referencia=f"SOLICITUD:{solicitud.folio}",
            fecha_emision=data.get("fecha_emision") or timezone.localdate(),
            fecha_entrega_estimada=data.get("fecha_entrega_estimada") or solicitud.fecha_requerida,
            monto_estimado=monto_estimado,
            estatus=estatus,
        )
        log_event(
            request.user,
            "CREATE",
            "compras.OrdenCompra",
            orden.id,
            {"folio": orden.folio, "estatus": orden.estatus, "source": f"api:solicitud:{solicitud.folio}"},
        )

        return Response(
            {
                "created": True,
                "id": orden.id,
                "folio": orden.folio,
                "estatus": orden.estatus,
                "solicitud_id": solicitud.id,
                "solicitud_folio": solicitud.folio,
                "proveedor_id": proveedor.id,
                "proveedor": proveedor.nombre,
                "monto_estimado": str(orden.monto_estimado),
                "costo_unitario": str(costo_unitario),
            },
            status=status.HTTP_201_CREATED,
        )


class ComprasOrdenStatusUpdateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, orden_id: int):
        if not can_manage_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para operar órdenes de compra."},
                status=status.HTTP_403_FORBIDDEN,
            )

        orden = get_object_or_404(OrdenCompra, pk=orden_id)
        ser = ComprasOrdenStatusSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        estatus_new = ser.validated_data["estatus"]
        estatus_prev = orden.estatus

        if estatus_prev == estatus_new:
            return Response(
                {
                    "id": orden.id,
                    "folio": orden.folio,
                    "from": estatus_prev,
                    "to": estatus_new,
                    "updated": False,
                },
                status=status.HTTP_200_OK,
            )

        if estatus_new == OrdenCompra.STATUS_CERRADA:
            has_closed_recepcion = RecepcionCompra.objects.filter(
                orden=orden,
                estatus=RecepcionCompra.STATUS_CERRADA,
            ).exists()
            if not has_closed_recepcion:
                return Response(
                    {"detail": f"No puedes cerrar {orden.folio} sin al menos una recepción cerrada."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        if not _can_transition_orden(estatus_prev, estatus_new):
            return Response(
                {"detail": f"Transición inválida de orden: {estatus_prev} -> {estatus_new}."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        orden.estatus = estatus_new
        orden.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.OrdenCompra",
            orden.id,
            {"from": estatus_prev, "to": estatus_new, "folio": orden.folio, "source": "api"},
        )
        return Response(
            {
                "id": orden.id,
                "folio": orden.folio,
                "from": estatus_prev,
                "to": estatus_new,
                "updated": True,
            },
            status=status.HTTP_200_OK,
        )


class ComprasOrdenCreateRecepcionView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, orden_id: int):
        if not can_manage_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para registrar recepciones."},
                status=status.HTTP_403_FORBIDDEN,
            )

        orden = get_object_or_404(OrdenCompra.objects.select_related("proveedor"), pk=orden_id)
        if orden.estatus in {OrdenCompra.STATUS_BORRADOR, OrdenCompra.STATUS_CERRADA}:
            return Response(
                {"detail": f"La orden {orden.folio} no admite recepciones en estatus {orden.estatus}."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        ser = ComprasRecepcionCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        recepcion = RecepcionCompra.objects.create(
            orden=orden,
            fecha_recepcion=data.get("fecha_recepcion") or timezone.localdate(),
            conformidad_pct=data.get("conformidad_pct", Decimal("100")),
            estatus=data.get("estatus") or RecepcionCompra.STATUS_PENDIENTE,
            observaciones=(data.get("observaciones") or "").strip(),
        )
        log_event(
            request.user,
            "CREATE",
            "compras.RecepcionCompra",
            recepcion.id,
            {"folio": recepcion.folio, "estatus": recepcion.estatus, "source": "api"},
        )

        if recepcion.estatus == RecepcionCompra.STATUS_CERRADA:
            _apply_recepcion_to_inventario(recepcion, acted_by=request.user)
            if orden.estatus != OrdenCompra.STATUS_CERRADA:
                orden_prev = orden.estatus
                orden.estatus = OrdenCompra.STATUS_CERRADA
                orden.save(update_fields=["estatus"])
                log_event(
                    request.user,
                    "APPROVE",
                    "compras.OrdenCompra",
                    orden.id,
                    {"from": orden_prev, "to": OrdenCompra.STATUS_CERRADA, "folio": orden.folio, "source": recepcion.folio},
                )

        return Response(
            {
                "id": recepcion.id,
                "folio": recepcion.folio,
                "orden_id": orden.id,
                "orden_folio": orden.folio,
                "estatus": recepcion.estatus,
                "conformidad_pct": str(recepcion.conformidad_pct),
                "fecha_recepcion": str(recepcion.fecha_recepcion),
                "orden_estatus": orden.estatus,
            },
            status=status.HTTP_201_CREATED,
        )


class ComprasRecepcionStatusUpdateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, recepcion_id: int):
        if not can_manage_compras(request.user):
            return Response(
                {"detail": "No tienes permisos para cerrar recepciones."},
                status=status.HTTP_403_FORBIDDEN,
            )

        recepcion = get_object_or_404(RecepcionCompra.objects.select_related("orden"), pk=recepcion_id)
        ser = ComprasRecepcionStatusSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        estatus_new = ser.validated_data["estatus"]
        estatus_prev = recepcion.estatus

        if estatus_prev == estatus_new:
            return Response(
                {
                    "id": recepcion.id,
                    "folio": recepcion.folio,
                    "from": estatus_prev,
                    "to": estatus_new,
                    "updated": False,
                    "orden_estatus": recepcion.orden.estatus,
                },
                status=status.HTTP_200_OK,
            )

        if not _can_transition_recepcion(estatus_prev, estatus_new):
            return Response(
                {"detail": f"Transición inválida de recepción: {estatus_prev} -> {estatus_new}."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        recepcion.estatus = estatus_new
        recepcion.save(update_fields=["estatus"])
        log_event(
            request.user,
            "APPROVE",
            "compras.RecepcionCompra",
            recepcion.id,
            {"from": estatus_prev, "to": estatus_new, "folio": recepcion.folio, "source": "api"},
        )

        if estatus_new == RecepcionCompra.STATUS_CERRADA:
            _apply_recepcion_to_inventario(recepcion, acted_by=request.user)
            if recepcion.orden.estatus != OrdenCompra.STATUS_CERRADA:
                orden_prev = recepcion.orden.estatus
                recepcion.orden.estatus = OrdenCompra.STATUS_CERRADA
                recepcion.orden.save(update_fields=["estatus"])
                log_event(
                    request.user,
                    "APPROVE",
                    "compras.OrdenCompra",
                    recepcion.orden.id,
                    {"from": orden_prev, "to": OrdenCompra.STATUS_CERRADA, "folio": recepcion.orden.folio, "source": recepcion.folio},
                )

        return Response(
            {
                "id": recepcion.id,
                "folio": recepcion.folio,
                "from": estatus_prev,
                "to": estatus_new,
                "updated": True,
                "orden_id": recepcion.orden_id,
                "orden_folio": recepcion.orden.folio,
                "orden_estatus": recepcion.orden.estatus,
            },
            status=status.HTTP_200_OK,
        )



