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



class ActivosCalendarioMantenimientoView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar calendario de mantenimiento."},
                status=status.HTTP_403_FORBIDDEN,
            )

        today = timezone.localdate()
        date_from = _parse_iso_date(request.GET.get("from")) or today
        date_to = _parse_iso_date(request.GET.get("to")) or (date_from + timedelta(days=45))
        if date_to < date_from:
            date_to = date_from + timedelta(days=45)

        planes = list(
            PlanMantenimiento.objects.select_related("activo_ref")
            .filter(
                activo=True,
                estatus=PlanMantenimiento.ESTATUS_ACTIVO,
                proxima_ejecucion__isnull=False,
                proxima_ejecucion__gte=date_from,
                proxima_ejecucion__lte=date_to,
            )
            .order_by("proxima_ejecucion", "id")
        )
        ordenes = list(
            OrdenMantenimiento.objects.select_related("activo_ref", "plan_ref")
            .filter(
                fecha_programada__gte=date_from,
                fecha_programada__lte=date_to,
            )
            .order_by("fecha_programada", "id")
        )

        events = []
        for plan in planes:
            events.append(
                {
                    "fecha": str(plan.proxima_ejecucion),
                    "tipo": "PLAN",
                    "referencia": f"PLAN-{plan.id}",
                    "activo_id": plan.activo_ref_id,
                    "activo": plan.activo_ref.nombre,
                    "detalle": plan.nombre,
                    "estado": plan.estatus,
                    "responsable": plan.responsable or "",
                }
            )
        for orden in ordenes:
            events.append(
                {
                    "fecha": str(orden.fecha_programada),
                    "tipo": "ORDEN",
                    "referencia": orden.folio,
                    "activo_id": orden.activo_ref_id,
                    "activo": orden.activo_ref.nombre,
                    "detalle": orden.descripcion or orden.get_tipo_display(),
                    "estado": orden.estatus,
                    "responsable": orden.responsable or "",
                }
            )
        events.sort(key=lambda row: (row["fecha"], row["tipo"], row["referencia"]))

        return Response(
            {
                "range": {
                    "from": str(date_from),
                    "to": str(date_to),
                    "days": (date_to - date_from).days + 1,
                },
                "totales": {
                    "planes": len(planes),
                    "ordenes": len(ordenes),
                    "eventos": len(events),
                },
                "events": events,
            },
            status=status.HTTP_200_OK,
        )


class ActivosDisponibilidadView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar disponibilidad de activos."},
                status=status.HTTP_403_FORBIDDEN,
            )

        activos_qs = Activo.objects.filter(activo=True)
        total = activos_qs.count()
        operativos = activos_qs.filter(estado=Activo.ESTADO_OPERATIVO).count()
        en_mantenimiento = activos_qs.filter(estado=Activo.ESTADO_MANTENIMIENTO).count()
        fuera_servicio = activos_qs.filter(estado=Activo.ESTADO_FUERA_SERVICIO).count()
        disponibilidad_pct = round((operativos * 100.0 / total), 2) if total else 100.0

        criticidad_rows = (
            activos_qs.values("criticidad")
            .annotate(total=Count("id"))
            .order_by("criticidad")
        )
        criticidad = {row["criticidad"]: int(row["total"] or 0) for row in criticidad_rows}

        hoy = timezone.localdate()
        ordenes_abiertas = OrdenMantenimiento.objects.filter(
            estatus__in=[OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO]
        )
        planes_vencidos = PlanMantenimiento.objects.filter(
            activo=True,
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            proxima_ejecucion__isnull=False,
            proxima_ejecucion__lt=hoy,
        ).count()

        return Response(
            {
                "totales": {
                    "activos": total,
                    "operativos": operativos,
                    "en_mantenimiento": en_mantenimiento,
                    "fuera_servicio": fuera_servicio,
                    "disponibilidad_pct": disponibilidad_pct,
                    "ordenes_abiertas": ordenes_abiertas.count(),
                    "ordenes_en_proceso": ordenes_abiertas.filter(estatus=OrdenMantenimiento.ESTATUS_EN_PROCESO).count(),
                    "planes_vencidos": planes_vencidos,
                },
                "criticidad": {
                    "ALTA": criticidad.get(Activo.CRITICIDAD_ALTA, 0),
                    "MEDIA": criticidad.get(Activo.CRITICIDAD_MEDIA, 0),
                    "BAJA": criticidad.get(Activo.CRITICIDAD_BAJA, 0),
                },
            },
            status=status.HTTP_200_OK,
        )


class ActivosOrdenesView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not can_manage_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para crear órdenes de mantenimiento."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ActivosOrdenCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        activo = get_object_or_404(Activo, pk=data["activo_id"], activo=True)
        plan = None
        plan_id = data.get("plan_id")
        if plan_id:
            plan = get_object_or_404(PlanMantenimiento, pk=plan_id, activo_ref=activo)

        fecha_programada = data.get("fecha_programada") or timezone.localdate()
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo,
            plan_ref=plan,
            tipo=data.get("tipo") or OrdenMantenimiento.TIPO_PREVENTIVO,
            prioridad=data.get("prioridad") or OrdenMantenimiento.PRIORIDAD_MEDIA,
            fecha_programada=fecha_programada,
            responsable=(data.get("responsable") or "").strip(),
            descripcion=(data.get("descripcion") or "").strip(),
            creado_por=request.user,
        )
        BitacoraMantenimiento.objects.create(
            orden=orden,
            accion="CREADA",
            comentario="Orden creada desde API",
            usuario=request.user,
        )
        log_event(
            request.user,
            "CREATE",
            "activos.OrdenMantenimiento",
            orden.id,
            {
                "folio": orden.folio,
                "activo_id": orden.activo_ref_id,
                "plan_id": orden.plan_ref_id,
                "tipo": orden.tipo,
                "prioridad": orden.prioridad,
                "estatus": orden.estatus,
                "source": "api",
            },
        )

        return Response(
            {
                "id": orden.id,
                "folio": orden.folio,
                "activo_id": orden.activo_ref_id,
                "activo": orden.activo_ref.nombre,
                "plan_id": orden.plan_ref_id,
                "tipo": orden.tipo,
                "prioridad": orden.prioridad,
                "estatus": orden.estatus,
                "fecha_programada": str(orden.fecha_programada),
                "responsable": orden.responsable,
                "descripcion": orden.descripcion,
            },
            status=status.HTTP_201_CREATED,
        )


class ActivosOrdenStatusUpdateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, orden_id: int):
        if not can_manage_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para actualizar estatus de órdenes de mantenimiento."},
                status=status.HTTP_403_FORBIDDEN,
            )

        orden = get_object_or_404(OrdenMantenimiento.objects.select_related("plan_ref"), pk=orden_id)
        ser = ActivosOrdenStatusSerializer(data=request.data)
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

        allowed_transitions = {
            OrdenMantenimiento.ESTATUS_PENDIENTE: {
                OrdenMantenimiento.ESTATUS_EN_PROCESO,
                OrdenMantenimiento.ESTATUS_CERRADA,
                OrdenMantenimiento.ESTATUS_CANCELADA,
            },
            OrdenMantenimiento.ESTATUS_EN_PROCESO: {
                OrdenMantenimiento.ESTATUS_CERRADA,
                OrdenMantenimiento.ESTATUS_CANCELADA,
            },
            OrdenMantenimiento.ESTATUS_CERRADA: set(),
            OrdenMantenimiento.ESTATUS_CANCELADA: set(),
        }
        if estatus_new not in allowed_transitions.get(estatus_prev, set()):
            return Response(
                {"detail": f"Transición inválida: {estatus_prev} -> {estatus_new}."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        today = timezone.localdate()
        orden.estatus = estatus_new
        update_fields = ["estatus", "actualizado_en"]
        if estatus_new == OrdenMantenimiento.ESTATUS_EN_PROCESO and not orden.fecha_inicio:
            orden.fecha_inicio = today
            update_fields.append("fecha_inicio")
        if estatus_new == OrdenMantenimiento.ESTATUS_CERRADA:
            orden.fecha_cierre = today
            update_fields.append("fecha_cierre")
            if orden.plan_ref_id:
                plan = orden.plan_ref
                plan.ultima_ejecucion = today
                plan.recompute_next_date()
                plan.save(update_fields=["ultima_ejecucion", "proxima_ejecucion", "actualizado_en"])
        orden.save(update_fields=update_fields)

        BitacoraMantenimiento.objects.create(
            orden=orden,
            accion="ESTATUS",
            comentario=f"{estatus_prev} -> {estatus_new}",
            usuario=request.user,
        )
        log_event(
            request.user,
            "UPDATE",
            "activos.OrdenMantenimiento",
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
                "fecha_inicio": str(orden.fecha_inicio) if orden.fecha_inicio else None,
                "fecha_cierre": str(orden.fecha_cierre) if orden.fecha_cierre else None,
            },
            status=status.HTTP_200_OK,
        )



