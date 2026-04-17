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



class MasterDataNormalizeView(APIView):
    permission_classes = [IsAuthenticated]

    def _query_by_scope(self, scope: str, q: str):
        q_norm = normalizar_nombre(q) if q else ""
        if scope == "insumos":
            qs = Insumo.objects.all().order_by("id")
            if q:
                qs = qs.filter(
                    Q(nombre__icontains=q)
                    | Q(nombre_normalizado__icontains=q_norm)
                    | Q(codigo__icontains=q)
                    | Q(codigo_point__icontains=q)
                )
            return qs

        if scope == "recetas":
            qs = Receta.objects.all().order_by("id")
            if q:
                qs = qs.filter(
                    Q(nombre__icontains=q)
                    | Q(nombre_normalizado__icontains=q_norm)
                    | Q(codigo_point__icontains=q)
                )
            return qs

        if scope == "aliases_insumo":
            qs = InsumoAlias.objects.select_related("insumo").all().order_by("id")
            if q:
                qs = qs.filter(
                    Q(nombre__icontains=q)
                    | Q(nombre_normalizado__icontains=q_norm)
                    | Q(insumo__nombre__icontains=q)
                )
            return qs

        if scope == "receta_codigos_point":
            qs = RecetaCodigoPointAlias.objects.select_related("receta").all().order_by("id")
            if q:
                qs = qs.filter(
                    Q(codigo_point__icontains=q)
                    | Q(codigo_point_normalizado__icontains=normalizar_codigo_point(q))
                    | Q(nombre_point__icontains=q)
                    | Q(receta__nombre__icontains=q)
                )
            return qs
        return None

    def _serialize_row(self, scope: str, obj, current: str, suggested: str) -> dict[str, Any]:
        if scope == "insumos":
            label = obj.nombre
            extra = {"codigo": obj.codigo or "", "codigo_point": obj.codigo_point or ""}
            model_name = "maestros.Insumo"
        elif scope == "recetas":
            label = obj.nombre
            extra = {"codigo_point": obj.codigo_point or ""}
            model_name = "recetas.Receta"
        elif scope == "aliases_insumo":
            canonical = canonical_insumo(obj.insumo) if obj.insumo_id else None
            label = obj.nombre
            extra = {
                "insumo_id": canonical.id if canonical else obj.insumo_id,
                "insumo": canonical.nombre if canonical else (obj.insumo.nombre if obj.insumo_id else ""),
                "insumo_canonical": bool(canonical and canonical.id != obj.insumo_id),
            }
            model_name = "maestros.InsumoAlias"
        else:
            label = obj.codigo_point or ""
            extra = {"receta_id": obj.receta_id, "receta": obj.receta.nombre if obj.receta_id else ""}
            model_name = "recetas.RecetaCodigoPointAlias"

        return {
            "scope": scope,
            "model": model_name,
            "id": obj.id,
            "label": label,
            "actual": current,
            "sugerido": suggested,
            "changed": bool((current or "") != (suggested or "")),
            **extra,
        }

    def post(self, request):
        if not can_view_maestros(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar normalización de datos maestros."},
                status=status.HTTP_403_FORBIDDEN,
            )

        serializer = MasterNormalizeSerializer(data=request.data or {})
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data

        scope = payload["scope"]
        q = payload["q"]
        dry_run = bool(payload["dry_run"])
        limit = int(payload["limit"])
        offset = int(payload["offset"])
        selected_scopes = (
            ["insumos", "recetas", "aliases_insumo", "receta_codigos_point"]
            if scope == "all"
            else [scope]
        )

        if (not dry_run) and (not has_any_role(request.user, ROLE_ADMIN, ROLE_DG)):
            return Response(
                {"detail": "Solo ADMIN o DG pueden aplicar normalización persistente."},
                status=status.HTTP_403_FORBIDDEN,
            )

        summary_by_scope: dict[str, dict[str, int]] = {}
        rows: list[dict[str, Any]] = []
        total_candidates = 0
        total_changed = 0
        total_updated = 0

        for current_scope in selected_scopes:
            qs = self._query_by_scope(current_scope, q)
            if qs is None:
                continue
            scoped_total = qs.count()
            scoped_rows = qs[offset : offset + limit]
            scoped_changed = 0
            scoped_updated = 0

            for obj in scoped_rows:
                if current_scope in {"insumos", "recetas", "aliases_insumo"}:
                    current_val = str(getattr(obj, "nombre_normalizado", "") or "")
                    suggested = normalizar_nombre(getattr(obj, "nombre", "") or "")
                    field_name = "nombre_normalizado"
                else:
                    current_val = str(getattr(obj, "codigo_point_normalizado", "") or "")
                    suggested = normalizar_codigo_point(getattr(obj, "codigo_point", "") or "")
                    field_name = "codigo_point_normalizado"

                changed = current_val != suggested
                if changed:
                    scoped_changed += 1
                    if not dry_run:
                        setattr(obj, field_name, suggested)
                        obj.save(update_fields=[field_name])
                        scoped_updated += 1

                rows.append(self._serialize_row(current_scope, obj, current_val, suggested))

            total_candidates += scoped_total
            total_changed += scoped_changed
            total_updated += scoped_updated
            summary_by_scope[current_scope] = {
                "candidates": scoped_total,
                "returned": len(scoped_rows),
                "changed": scoped_changed,
                "updated": scoped_updated,
            }

        action = "PREVIEW_MASTER_NORMALIZE" if dry_run else "MASTER_NORMALIZE"
        log_event(
            request.user,
            action,
            "master.Normalization",
            "",
            payload={
                "scope": scope,
                "q": q,
                "dry_run": dry_run,
                "limit": limit,
                "offset": offset,
                "totals": {
                    "candidates": total_candidates,
                    "changed": total_changed,
                    "updated": total_updated,
                },
            },
        )

        return Response(
            {
                "dry_run": dry_run,
                "filters": {
                    "scope": scope,
                    "q": q,
                    "limit": limit,
                    "offset": offset,
                    "scopes_evaluated": selected_scopes,
                },
                "totales": {
                    "candidates": total_candidates,
                    "changed": total_changed,
                    "updated": total_updated,
                    "returned": len(rows),
                },
                "by_scope": summary_by_scope,
                "items": rows,
            },
            status=status.HTTP_200_OK,
        )


class MasterDataDuplicatesView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _build_group(
        group_type: str,
        key: str,
        members: list[dict[str, Any]],
        *,
        canonical_member_id: int | None = None,
    ) -> dict[str, Any]:
        members_sorted = sorted(
            members,
            key=lambda item: (
                str(item.get("nombre") or item.get("label") or "").lower(),
                int(item.get("id") or 0),
            ),
        )
        canonical_member = None
        if canonical_member_id:
            for member in members_sorted:
                if int(member.get("id") or 0) == int(canonical_member_id):
                    canonical_member = {
                        "id": int(member.get("id") or 0),
                        "nombre": member.get("nombre") or "",
                        "codigo_point": member.get("codigo_point") or "",
                        "model": member.get("model") or "",
                    }
                    break
        return {
            "group_type": group_type,
            "duplicate_key": key,
            "count": len(members_sorted),
            "canonical_member_id": canonical_member_id,
            "canonical_member": canonical_member,
            "members": members_sorted,
        }

    @staticmethod
    def _export_csv(groups: list[dict[str, Any]]) -> HttpResponse:
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="master_duplicates.csv"'
        writer = csv.writer(response)
        writer.writerow(["group_type", "duplicate_key", "count", "model", "id", "nombre", "activo", "codigo_point"])
        for group in groups:
            for member in group.get("members") or []:
                writer.writerow(
                    [
                        group.get("group_type") or "",
                        group.get("duplicate_key") or "",
                        group.get("count") or 0,
                        member.get("model") or "",
                        member.get("id") or "",
                        member.get("nombre") or "",
                        "1" if member.get("activo") else "0",
                        member.get("codigo_point") or "",
                    ]
                )
        return response

    @staticmethod
    def _export_xlsx(groups: list[dict[str, Any]]) -> HttpResponse:
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "duplicates"
        sheet.append(["group_type", "duplicate_key", "count", "model", "id", "nombre", "activo", "codigo_point"])
        for group in groups:
            for member in group.get("members") or []:
                sheet.append(
                    [
                        group.get("group_type") or "",
                        group.get("duplicate_key") or "",
                        group.get("count") or 0,
                        member.get("model") or "",
                        member.get("id") or "",
                        member.get("nombre") or "",
                        "1" if member.get("activo") else "0",
                        member.get("codigo_point") or "",
                    ]
                )

        output = BytesIO()
        workbook.save(output)
        output.seek(0)
        response = HttpResponse(
            output.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = 'attachment; filename="master_duplicates.xlsx"'
        return response

    def get(self, request):
        if not can_view_maestros(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar duplicados de datos maestros."},
                status=status.HTTP_403_FORBIDDEN,
            )

        serializer = MasterDuplicatesSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data

        scope = payload["scope"]
        q = payload["q"]
        include_inactive = bool(payload["include_inactive"])
        min_count = int(payload["min_count"])
        limit = int(payload["limit"])
        offset = int(payload["offset"])
        export_kind = payload.get("export", "")
        q_norm = normalizar_nombre(q) if q else ""

        selected_scopes = (
            ["insumos", "recetas", "proveedores", "codigos_point"]
            if scope == "all"
            else [scope]
        )
        groups: list[dict[str, Any]] = []
        by_scope_totals = {k: 0 for k in selected_scopes}

        if "insumos" in selected_scopes:
            qs = Insumo.objects.all().order_by("id")
            if not include_inactive:
                qs = qs.filter(activo=True)
            if q:
                qs = qs.filter(
                    Q(nombre__icontains=q)
                    | Q(nombre_normalizado__icontains=q_norm)
                    | Q(codigo__icontains=q)
                    | Q(codigo_point__icontains=q)
                )

            canonical_map = {
                row["normalized_name"]: row for row in canonicalized_active_insumos(limit=5000)
            }
            grouped = (
                qs.values("nombre_normalizado")
                .annotate(total=Count("id"))
                .filter(total__gte=min_count)
                .order_by("-total", "nombre_normalizado")
            )
            for row in grouped:
                dup_key = str(row.get("nombre_normalizado") or "")
                canonical_row = canonical_map.get(dup_key)
                canonical_member_id = int(canonical_row["canonical"].id) if canonical_row else None
                members = []
                for obj in qs.filter(nombre_normalizado=dup_key).order_by("id"):
                    members.append(
                        {
                            "model": "maestros.Insumo",
                            "id": int(obj.id),
                            "nombre": obj.nombre,
                            "activo": bool(obj.activo),
                            "codigo_point": obj.codigo_point or "",
                            "is_canonical": bool(canonical_member_id and int(obj.id) == canonical_member_id),
                        }
                    )
                groups.append(
                    self._build_group(
                        "insumos",
                        dup_key,
                        members,
                        canonical_member_id=canonical_member_id,
                    )
                )
            by_scope_totals["insumos"] = len([g for g in groups if g["group_type"] == "insumos"])

        if "recetas" in selected_scopes:
            qs = Receta.objects.all().order_by("id")
            if q:
                qs = qs.filter(
                    Q(nombre__icontains=q)
                    | Q(nombre_normalizado__icontains=q_norm)
                    | Q(codigo_point__icontains=q)
                )
            grouped = (
                qs.values("nombre_normalizado")
                .annotate(total=Count("id"))
                .filter(total__gte=min_count)
                .order_by("-total", "nombre_normalizado")
            )
            for row in grouped:
                dup_key = str(row.get("nombre_normalizado") or "")
                members = [
                    {
                        "model": "recetas.Receta",
                        "id": int(obj.id),
                        "nombre": obj.nombre,
                        "activo": True,
                        "codigo_point": obj.codigo_point or "",
                    }
                    for obj in qs.filter(nombre_normalizado=dup_key).order_by("id")
                ]
                groups.append(self._build_group("recetas", dup_key, members))
            by_scope_totals["recetas"] = len([g for g in groups if g["group_type"] == "recetas"])

        if "proveedores" in selected_scopes:
            qs = Proveedor.objects.all().order_by("id")
            if not include_inactive:
                qs = qs.filter(activo=True)
            if q:
                qs = qs.filter(nombre__icontains=q)

            index: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for provider in qs:
                key = normalizar_nombre(provider.nombre or "")
                if not key:
                    continue
                index[key].append(
                    {
                        "model": "maestros.Proveedor",
                        "id": int(provider.id),
                        "nombre": provider.nombre,
                        "activo": bool(provider.activo),
                        "codigo_point": "",
                    }
                )

            for key, members in sorted(index.items(), key=lambda item: (-len(item[1]), item[0])):
                if len(members) < min_count:
                    continue
                groups.append(self._build_group("proveedores", key, members))
            by_scope_totals["proveedores"] = len([g for g in groups if g["group_type"] == "proveedores"])

        if "codigos_point" in selected_scopes:
            code_map: dict[str, list[dict[str, Any]]] = defaultdict(list)

            insumos_qs = Insumo.objects.exclude(codigo_point="").order_by("id")
            recetas_qs = Receta.objects.exclude(codigo_point="").order_by("id")
            if not include_inactive:
                insumos_qs = insumos_qs.filter(activo=True)
            if q:
                insumos_qs = insumos_qs.filter(Q(codigo_point__icontains=q) | Q(nombre__icontains=q))
                recetas_qs = recetas_qs.filter(Q(codigo_point__icontains=q) | Q(nombre__icontains=q))

            for insumo in insumos_qs:
                key = normalizar_codigo_point(insumo.codigo_point or "")
                if not key:
                    continue
                code_map[key].append(
                    {
                        "model": "maestros.Insumo",
                        "id": int(insumo.id),
                        "nombre": insumo.nombre,
                        "activo": bool(insumo.activo),
                        "codigo_point": insumo.codigo_point or "",
                    }
                )

            for receta in recetas_qs:
                key = normalizar_codigo_point(receta.codigo_point or "")
                if not key:
                    continue
                code_map[key].append(
                    {
                        "model": "recetas.Receta",
                        "id": int(receta.id),
                        "nombre": receta.nombre,
                        "activo": True,
                        "codigo_point": receta.codigo_point or "",
                    }
                )

            for key, members in sorted(code_map.items(), key=lambda item: (-len(item[1]), item[0])):
                if len(members) < min_count:
                    continue
                groups.append(self._build_group("codigos_point", key, members))
            by_scope_totals["codigos_point"] = len([g for g in groups if g["group_type"] == "codigos_point"])

        groups.sort(key=lambda row: (-int(row.get("count") or 0), str(row.get("duplicate_key") or "").lower()))
        total_groups = len(groups)
        page_groups = groups[offset : offset + limit]

        if export_kind == "csv":
            return self._export_csv(page_groups)
        if export_kind == "xlsx":
            return self._export_xlsx(page_groups)

        return Response(
            {
                "filters": {
                    "scope": scope,
                    "q": q,
                    "include_inactive": include_inactive,
                    "min_count": min_count,
                    "limit": limit,
                    "offset": offset,
                },
                "totales": {
                    "groups_total": total_groups,
                    "groups_returned": len(page_groups),
                    "by_scope": by_scope_totals,
                },
                "items": page_groups,
            },
            status=status.HTTP_200_OK,
        )



