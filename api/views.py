from collections import defaultdict
from contextlib import nullcontext
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from django.db import transaction, OperationalError, ProgrammingError
from django.db.models import Count, Q, Sum
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.views import ObtainAuthToken

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
from inventario.models import AjusteInventario, AlmacenSyncRun, ExistenciaInsumo
from inventario.views import (
    _apply_ajuste,
    _apply_cross_filters,
    _build_cross_unified_rows,
    _build_pending_grouped,
    _resolve_cross_source_with_alias,
)
from maestros.models import CostoInsumo, Insumo, InsumoAlias, PointPendingMatch, Proveedor
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
    _build_forecast_from_history,
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
from .serializers import (
    ComprasSolicitudImportConfirmSerializer,
    ComprasSolicitudImportPreviewSerializer,
    ComprasCrearOrdenSerializer,
    ComprasOrdenStatusSerializer,
    ComprasRecepcionCreateSerializer,
    ComprasRecepcionStatusSerializer,
    ComprasSolicitudCreateSerializer,
    ComprasSolicitudStatusSerializer,
    ForecastBacktestRequestSerializer,
    ForecastEstadisticoGuardarSerializer,
    ForecastEstadisticoRequestSerializer,
    InventarioAjusteCreateSerializer,
    InventarioAjusteDecisionSerializer,
    InventarioAliasCreateSerializer,
    InventarioCrossPendientesResolveSerializer,
    InventarioAliasMassReassignSerializer,
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


class ApiTokenAuthView(ObtainAuthToken):
    permission_classes = [AllowAny]
    authentication_classes = []

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data["user"]
        token, _ = Token.objects.get_or_create(user=user)
        return Response(
            {
                "token": token.key,
                "user": {
                    "id": user.id,
                    "username": user.get_username(),
                    "is_superuser": bool(user.is_superuser),
                },
            },
            status=status.HTTP_200_OK,
        )


class ApiTokenRotateView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        Token.objects.filter(user=request.user).delete()
        token = Token.objects.create(user=request.user)
        return Response({"token": token.key}, status=status.HTTP_200_OK)


class ApiTokenRevokeView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        deleted, _ = Token.objects.filter(user=request.user).delete()
        return Response({"revoked": bool(deleted)}, status=status.HTTP_200_OK)


class ApiAuthMeView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = request.user
        group_names = list(user.groups.values_list("name", flat=True)) if user.is_authenticated else []
        return Response(
            {
                "id": user.id,
                "username": user.get_username(),
                "email": getattr(user, "email", "") or "",
                "is_superuser": bool(user.is_superuser),
                "is_staff": bool(user.is_staff),
                "groups": sorted(group_names),
                "permissions": {
                    "can_view_compras": can_view_compras(user),
                    "can_manage_compras": can_manage_compras(user),
                    "can_view_inventario": can_view_inventario(user),
                    "can_manage_inventario": can_manage_inventario(user),
                    "can_view_maestros": can_view_maestros(user),
                    "can_view_audit": can_view_audit(user),
                },
            },
            status=status.HTTP_200_OK,
        )


class AuditLogListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_audit(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar bitácora."},
                status=status.HTTP_403_FORBIDDEN,
            )

        q = (request.GET.get("q") or "").strip()
        action = (request.GET.get("action") or "").strip().upper()
        model_name = (request.GET.get("model") or "").strip()
        user_id_raw = (request.GET.get("user_id") or "").strip()
        limit = _parse_bounded_int(request.GET.get("limit", 200), default=200, min_value=1, max_value=1000)

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
        if user_id_raw:
            try:
                user_id = int(user_id_raw)
            except ValueError:
                return Response({"detail": "user_id inválido."}, status=status.HTTP_400_BAD_REQUEST)
            qs = qs.filter(user_id=user_id)

        total = qs.count()
        rows = []
        for log in qs[:limit]:
            rows.append(
                {
                    "id": log.id,
                    "timestamp": log.timestamp,
                    "action": log.action,
                    "model": log.model,
                    "object_id": log.object_id,
                    "user_id": log.user_id,
                    "user": log.user.username if log.user_id else "",
                    "payload": log.payload or {},
                }
            )
        return Response(
            {
                "filters": {
                    "q": q,
                    "action": action,
                    "model": model_name,
                    "user_id": user_id_raw,
                    "limit": limit,
                },
                "totales": {"rows": total, "returned": len(rows)},
                "items": rows,
            },
            status=status.HTTP_200_OK,
        )


def _can_approve_ajustes(user) -> bool:
    return has_any_role(user, ROLE_ADMIN, ROLE_DG)


def _serialize_ajuste_row(ajuste: AjusteInventario) -> dict:
    return {
        "id": ajuste.id,
        "folio": ajuste.folio,
        "insumo_id": ajuste.insumo_id,
        "insumo": ajuste.insumo.nombre if ajuste.insumo_id else "",
        "cantidad_sistema": str(ajuste.cantidad_sistema),
        "cantidad_fisica": str(ajuste.cantidad_fisica),
        "delta": str(_to_decimal(ajuste.cantidad_fisica) - _to_decimal(ajuste.cantidad_sistema)),
        "motivo": ajuste.motivo,
        "estatus": ajuste.estatus,
        "solicitado_por": ajuste.solicitado_por.username if ajuste.solicitado_por_id else "",
        "aprobado_por": ajuste.aprobado_por.username if ajuste.aprobado_por_id else "",
        "comentario_revision": ajuste.comentario_revision or "",
        "creado_en": ajuste.creado_en,
        "aprobado_en": ajuste.aprobado_en,
        "aplicado_en": ajuste.aplicado_en,
    }


def _serialize_forecast_compare(compare: dict | None, *, top: int = 120) -> dict:
    if not compare:
        return {
            "target_start": "",
            "target_end": "",
            "sucursal_id": None,
            "sucursal_nombre": "",
            "rows": [],
            "totals": {
                "forecast_total": 0.0,
                "solicitud_total": 0.0,
                "delta_total": 0.0,
                "ok_count": 0,
                "sobre_count": 0,
                "bajo_count": 0,
                "sin_base_count": 0,
                "en_rango_count": 0,
                "sobre_rango_count": 0,
                "bajo_rango_count": 0,
            },
        }

    rows = []
    for row in (compare.get("rows") or [])[:top]:
        variacion = row.get("variacion_pct")
        rows.append(
            {
                "receta_id": int(row.get("receta_id") or 0),
                "receta": row.get("receta") or "",
                "forecast_qty": _to_float(row.get("forecast_qty")),
                "forecast_low": _to_float(row.get("forecast_low")),
                "forecast_high": _to_float(row.get("forecast_high")),
                "solicitud_qty": _to_float(row.get("solicitud_qty")),
                "delta_qty": _to_float(row.get("delta_qty")),
                "variacion_pct": _to_float(variacion) if variacion is not None else None,
                "status": row.get("status") or "",
                "status_rango": row.get("status_rango") or "",
            }
        )
    totals = compare.get("totals") or {}
    return {
        "target_start": str(compare.get("target_start") or ""),
        "target_end": str(compare.get("target_end") or ""),
        "sucursal_id": compare.get("sucursal_id"),
        "sucursal_nombre": compare.get("sucursal_nombre") or "",
        "rows": rows,
        "totals": {
            "forecast_total": _to_float(totals.get("forecast_total")),
            "solicitud_total": _to_float(totals.get("solicitud_total")),
            "delta_total": _to_float(totals.get("delta_total")),
            "ok_count": int(totals.get("ok_count") or 0),
            "sobre_count": int(totals.get("sobre_count") or 0),
            "bajo_count": int(totals.get("bajo_count") or 0),
            "sin_base_count": int(totals.get("sin_base_count") or 0),
            "en_rango_count": int(totals.get("en_rango_count") or 0),
            "sobre_rango_count": int(totals.get("sobre_rango_count") or 0),
            "bajo_rango_count": int(totals.get("bajo_rango_count") or 0),
        },
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
            key = l.insumo.nombre if l.insumo else f"(NO MATCH) {l.insumo_texto}"
            if key not in agregados:
                agregados[key] = {"insumo_id": l.insumo.id if l.insumo else None, "nombre": key, "cantidad": Decimal("0"), "unidad": l.unidad_texto, "costo": 0.0}
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


class InventarioSugerenciasCompraView(APIView):
    permission_classes = [IsAuthenticated]

    def _resolve_scope(self, request):
        plan_id_raw = (request.GET.get("plan_id") or "").strip()
        periodo_raw = (request.GET.get("periodo") or "").strip()
        plan = None
        year = None
        month = None

        if plan_id_raw:
            try:
                plan_id = int(plan_id_raw)
            except ValueError:
                plan_id = None
            if plan_id:
                plan = get_object_or_404(PlanProduccion, pk=plan_id)
                year = plan.fecha_produccion.year
                month = plan.fecha_produccion.month

        if not plan:
            parsed = _parse_period(periodo_raw)
            if parsed:
                year, month = parsed
            else:
                today = timezone.localdate()
                year, month = today.year, today.month

        if plan:
            items_qs = PlanProduccionItem.objects.filter(plan_id=plan.id).select_related("receta", "plan")
        else:
            items_qs = PlanProduccionItem.objects.filter(
                plan__fecha_produccion__year=year,
                plan__fecha_produccion__month=month,
            ).select_related("receta", "plan")

        return items_qs, {
            "plan_id": plan.id if plan else None,
            "plan_nombre": plan.nombre if plan else "",
            "periodo": f"{year:04d}-{month:02d}",
        }

    def _requerimientos_plan(self, plan_items_qs) -> dict[int, Decimal]:
        requerimientos: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))
        receta_ids = set(plan_items_qs.values_list("receta_id", flat=True))
        if not receta_ids:
            return requerimientos

        lineas_by_receta: dict[int, list[LineaReceta]] = defaultdict(list)
        lineas_qs = (
            LineaReceta.objects.filter(receta_id__in=receta_ids)
            .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .only("receta_id", "insumo_id", "cantidad")
        )
        for linea in lineas_qs:
            if not linea.insumo_id:
                continue
            qty = _to_decimal(linea.cantidad)
            if qty <= 0:
                continue
            lineas_by_receta[linea.receta_id].append(linea)

        for item in plan_items_qs:
            factor = _to_decimal(item.cantidad)
            if factor <= 0:
                continue
            for linea in lineas_by_receta.get(item.receta_id, []):
                requerimientos[linea.insumo_id] += _to_decimal(linea.cantidad) * factor
        return requerimientos

    def _en_transito_por_insumo(self) -> dict[int, Decimal]:
        totals: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))
        transit_status = [
            OrdenCompra.STATUS_ENVIADA,
            OrdenCompra.STATUS_CONFIRMADA,
            OrdenCompra.STATUS_PARCIAL,
        ]
        ordenes = (
            OrdenCompra.objects.filter(estatus__in=transit_status, solicitud__isnull=False)
            .select_related("solicitud__insumo")
            .only("solicitud__insumo_id", "solicitud__cantidad")
        )
        for orden in ordenes:
            solicitud = orden.solicitud
            if not solicitud or not solicitud.insumo_id:
                continue
            qty = _to_decimal(solicitud.cantidad)
            if qty > 0:
                totals[solicitud.insumo_id] += qty
        return totals

    def get(self, request):
        include_all = _parse_bool(request.GET.get("include_all"), default=False)
        try:
            limit = int(request.GET.get("limit", 300))
        except ValueError:
            limit = 300
        limit = max(1, min(limit, 2000))

        plan_items_qs, scope = self._resolve_scope(request)
        requerimientos = self._requerimientos_plan(plan_items_qs)
        en_transito = self._en_transito_por_insumo()

        existencias_qs = ExistenciaInsumo.objects.select_related("insumo__unidad_base", "insumo__proveedor_principal").filter(
            insumo__activo=True
        )
        existencias_map = {e.insumo_id: e for e in existencias_qs}

        insumo_ids = set(existencias_map.keys()) | set(requerimientos.keys()) | set(en_transito.keys())
        if not insumo_ids:
            return Response(
                {
                    "scope": scope,
                    "formula": "compra_sugerida = (requerido + stock_seguridad) - (disponible + en_transito)",
                    "totales": {
                        "insumos": 0,
                        "criticos": 0,
                        "bajo_reorden": 0,
                        "compra_sugerida_total": "0",
                        "costo_estimado_total": "0",
                    },
                    "items": [],
                },
                status=status.HTTP_200_OK,
            )

        insumos = {
            i.id: i
            for i in Insumo.objects.filter(id__in=insumo_ids, activo=True).select_related("unidad_base", "proveedor_principal")
        }
        if not insumos:
            return Response(
                {
                    "scope": scope,
                    "formula": "compra_sugerida = (requerido + stock_seguridad) - (disponible + en_transito)",
                    "totales": {
                        "insumos": 0,
                        "criticos": 0,
                        "bajo_reorden": 0,
                        "compra_sugerida_total": "0",
                        "costo_estimado_total": "0",
                    },
                    "items": [],
                },
                status=status.HTTP_200_OK,
            )

        latest_cost: dict[int, Decimal] = {}
        for costo in (
            CostoInsumo.objects.filter(insumo_id__in=list(insumos.keys()))
            .only("insumo_id", "costo_unitario", "fecha", "id")
            .order_by("insumo_id", "-fecha", "-id")
        ):
            if costo.insumo_id not in latest_cost:
                latest_cost[costo.insumo_id] = _to_decimal(costo.costo_unitario)

        rows = []
        total_sugerido = Decimal("0")
        total_costo = Decimal("0")
        criticos = 0
        bajo_reorden = 0

        for insumo_id in sorted(insumos.keys(), key=lambda pk: insumos[pk].nombre.lower()):
            insumo = insumos[insumo_id]
            ex = existencias_map.get(insumo_id)

            stock_actual = _to_decimal(ex.stock_actual if ex else 0)
            stock_seguridad = _to_decimal(ex.stock_minimo if ex else 0)
            punto_reorden = _to_decimal(ex.punto_reorden if ex else 0)
            consumo_diario = _to_decimal(ex.consumo_diario_promedio if ex else 0)
            lead_time = int(ex.dias_llegada_pedido or 0) if ex else 0
            if lead_time <= 0 and insumo.proveedor_principal_id:
                lead_time = int(insumo.proveedor_principal.lead_time_dias or 0)
            lead_time = max(lead_time, 0)

            demanda_lead_time = consumo_diario * Decimal(str(lead_time))
            requerido_plan = _to_decimal(requerimientos.get(insumo_id, 0))
            requerido = requerido_plan if requerido_plan > demanda_lead_time else demanda_lead_time
            en_transito_qty = _to_decimal(en_transito.get(insumo_id, 0))

            sugerida = requerido + stock_seguridad - (stock_actual + en_transito_qty)
            if sugerida < 0:
                sugerida = Decimal("0")

            costo_unitario = _to_decimal(latest_cost.get(insumo_id, 0))
            costo_sugerido = sugerida * costo_unitario if sugerida > 0 and costo_unitario > 0 else Decimal("0")

            if stock_actual <= 0 and (requerido > 0 or punto_reorden > 0):
                estado = "CRITICO"
                criticos += 1
            elif punto_reorden > 0 and stock_actual < punto_reorden:
                estado = "BAJO_REORDEN"
                bajo_reorden += 1
            else:
                estado = "SUFICIENTE"

            if not include_all and sugerida <= 0:
                continue

            total_sugerido += sugerida
            total_costo += costo_sugerido

            rows.append(
                {
                    "insumo_id": insumo_id,
                    "insumo": insumo.nombre,
                    "unidad": insumo.unidad_base.codigo if insumo.unidad_base_id and insumo.unidad_base else "",
                    "proveedor_principal": insumo.proveedor_principal.nombre if insumo.proveedor_principal_id else "",
                    "stock_actual": str(stock_actual),
                    "stock_seguridad": str(stock_seguridad),
                    "punto_reorden": str(punto_reorden),
                    "en_transito": str(en_transito_qty),
                    "requerido_plan": str(requerido_plan),
                    "demanda_lead_time": str(demanda_lead_time),
                    "requerido_total": str(requerido),
                    "compra_sugerida": str(sugerida),
                    "lead_time_dias": lead_time,
                    "consumo_diario_promedio": str(consumo_diario),
                    "costo_unitario": str(costo_unitario),
                    "costo_compra_sugerida": str(costo_sugerido),
                    "estatus": estado,
                }
            )

        rows.sort(
            key=lambda x: (
                Decimal(x["compra_sugerida"]),
                Decimal(x["requerido_total"]),
                x["insumo"].lower(),
            ),
            reverse=True,
        )

        return Response(
            {
                "scope": {**scope, "include_all": include_all, "limit": limit},
                "formula": "compra_sugerida = (requerido + stock_seguridad) - (disponible + en_transito)",
                "totales": {
                    "insumos": len(rows[:limit]),
                    "criticos": criticos,
                    "bajo_reorden": bajo_reorden,
                    "compra_sugerida_total": str(total_sugerido),
                    "costo_estimado_total": str(total_costo),
                },
                "items": rows[:limit],
            },
            status=status.HTTP_200_OK,
        )


class InventarioAliasesListCreateView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _serialize_alias(alias: InsumoAlias) -> dict:
        return {
            "id": alias.id,
            "alias": alias.nombre,
            "normalizado": alias.nombre_normalizado,
            "insumo_id": alias.insumo_id,
            "insumo": alias.insumo.nombre if alias.insumo_id else "",
            "unidad": alias.insumo.unidad_base.codigo if alias.insumo_id and alias.insumo.unidad_base_id else "",
            "categoria": alias.insumo.categoria if alias.insumo_id else "",
        }

    def get(self, request):
        if not can_view_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar aliases de inventario."},
                status=status.HTTP_403_FORBIDDEN,
            )

        q = (request.GET.get("q") or "").strip()
        insumo_id_raw = (request.GET.get("insumo_id") or "").strip()
        limit = _parse_bounded_int(request.GET.get("limit", 250), default=250, min_value=1, max_value=1200)

        qs = InsumoAlias.objects.select_related("insumo__unidad_base").order_by("nombre")
        if q:
            q_norm = normalizar_nombre(q)
            qs = qs.filter(
                Q(nombre__icontains=q)
                | Q(nombre_normalizado__icontains=q_norm)
                | Q(insumo__nombre__icontains=q)
                | Q(insumo__codigo_point__icontains=q)
            )

        insumo_id = None
        if insumo_id_raw:
            try:
                insumo_id = int(insumo_id_raw)
            except ValueError:
                return Response(
                    {"detail": "insumo_id inválido."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            qs = qs.filter(insumo_id=insumo_id)

        total_rows = qs.count()
        rows = [self._serialize_alias(a) for a in qs[:limit]]
        return Response(
            {
                "filters": {"q": q, "insumo_id": insumo_id, "limit": limit},
                "totales": {"rows": total_rows, "returned": len(rows)},
                "items": rows,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not can_manage_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para crear/editar aliases de inventario."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = InventarioAliasCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        alias_name = (data.get("alias") or "").strip()
        alias_norm = normalizar_nombre(alias_name)
        if not alias_norm:
            return Response(
                {"detail": "Alias inválido: nombre vacío después de normalizar."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        insumo = Insumo.objects.filter(pk=data["insumo_id"], activo=True).select_related("unidad_base").first()
        if insumo is None:
            return Response(
                {"detail": "insumo_id no encontrado o inactivo."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        alias, created = InsumoAlias.objects.get_or_create(
            nombre_normalizado=alias_norm,
            defaults={"nombre": alias_name[:250], "insumo": insumo},
        )

        updated = False
        if not created:
            changed = []
            if alias.insumo_id != insumo.id:
                alias.insumo = insumo
                changed.append("insumo")
            if alias.nombre != alias_name[:250]:
                alias.nombre = alias_name[:250]
                changed.append("nombre")
            if changed:
                alias.save(update_fields=changed)
                updated = True

        point_resolved = 0
        recetas_resolved = 0
        if bool(data.get("resolver_cross_source", True)):
            point_resolved, recetas_resolved = _resolve_cross_source_with_alias(alias_name, insumo)

        alias.refresh_from_db()
        return Response(
            {
                "created": created,
                "updated": updated,
                "alias": self._serialize_alias(alias),
                "resolved": {
                    "point_pending": point_resolved,
                    "recetas_pending": recetas_resolved,
                },
            },
            status=status.HTTP_201_CREATED if created else status.HTTP_200_OK,
        )


class InventarioAliasesMassReassignView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not can_manage_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para reasignar aliases de inventario."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = InventarioAliasMassReassignSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        insumo = Insumo.objects.filter(pk=data["insumo_id"], activo=True).select_related("unidad_base").first()
        if insumo is None:
            return Response(
                {"detail": "insumo_id no encontrado o inactivo."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        alias_ids = list(dict.fromkeys(int(aid) for aid in data["alias_ids"]))
        aliases = list(InsumoAlias.objects.filter(id__in=alias_ids).select_related("insumo"))
        if not aliases:
            return Response(
                {"detail": "No se encontraron aliases con los IDs enviados."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        resolver_cross = bool(data.get("resolver_cross_source", True))
        updated = 0
        point_resolved = 0
        recetas_resolved = 0
        touched_ids: list[int] = []
        for alias in aliases:
            if alias.insumo_id == insumo.id:
                continue
            alias.insumo = insumo
            alias.save(update_fields=["insumo"])
            updated += 1
            touched_ids.append(alias.id)
            if resolver_cross:
                p_count, r_count = _resolve_cross_source_with_alias(alias.nombre, insumo)
                point_resolved += p_count
                recetas_resolved += r_count

        return Response(
            {
                "target_insumo": {"id": insumo.id, "nombre": insumo.nombre},
                "selected": len(alias_ids),
                "found": len(aliases),
                "updated": updated,
                "touched_alias_ids": touched_ids,
                "resolved": {
                    "point_pending": point_resolved,
                    "recetas_pending": recetas_resolved,
                },
            },
            status=status.HTTP_200_OK,
        )


class InventarioAliasesPendientesView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar pendientes de homologación."},
                status=status.HTTP_403_FORBIDDEN,
            )

        limit = _parse_bounded_int(request.GET.get("limit", 120), default=120, min_value=1, max_value=400)
        runs_to_scan = _parse_bounded_int(request.GET.get("runs", 5), default=5, min_value=1, max_value=30)
        point_tipo = (request.GET.get("point_tipo") or PointPendingMatch.TIPO_INSUMO).strip().upper()
        valid_point_tipos = {
            PointPendingMatch.TIPO_INSUMO,
            PointPendingMatch.TIPO_PROVEEDOR,
            PointPendingMatch.TIPO_PRODUCTO,
            "TODOS",
            "ALL",
        }
        if point_tipo not in valid_point_tipos:
            return Response(
                {"detail": "point_tipo inválido. Usa INSUMO, PROVEEDOR, PRODUCTO o TODOS."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        almacen_rows: list[dict] = []
        sync_runs = list(AlmacenSyncRun.objects.only("id", "started_at", "pending_preview").order_by("-started_at")[:runs_to_scan])
        for run in sync_runs:
            for row in run.pending_preview or []:
                nombre_origen = str((row or {}).get("nombre_origen") or "").strip()
                if not nombre_origen:
                    continue
                almacen_rows.append(
                    {
                        "run_id": run.id,
                        "run_started_at": run.started_at,
                        "nombre_origen": nombre_origen,
                        "nombre_normalizado": str((row or {}).get("nombre_normalizado") or normalizar_nombre(nombre_origen)),
                        "sugerencia": str((row or {}).get("suggestion") or ""),
                        "score": float((row or {}).get("score") or 0),
                        "metodo": str((row or {}).get("method") or ""),
                        "fuente": str((row or {}).get("fuente") or "ALMACEN"),
                    }
                )
                if len(almacen_rows) >= limit:
                    break
            if len(almacen_rows) >= limit:
                break

        point_qs = PointPendingMatch.objects.order_by("-fuzzy_score", "point_nombre")
        if point_tipo not in {"TODOS", "ALL"}:
            point_qs = point_qs.filter(tipo=point_tipo)
        point_total = point_qs.count()
        point_totals_by_tipo = {
            row["tipo"]: row["count"]
            for row in (
                point_qs.values("tipo")
                .annotate(count=Count("id"))
                .order_by("tipo")
            )
        }
        point_rows = [
            {
                "id": p.id,
                "tipo": p.tipo,
                "point_codigo": p.point_codigo,
                "point_nombre": p.point_nombre,
                "sugerencia": p.fuzzy_sugerencia or "",
                "score": float(p.fuzzy_score or 0),
                "metodo": p.method or "",
                "actualizado_en": p.actualizado_en,
            }
            for p in point_qs[:limit]
        ]

        recetas_qs = (
            LineaReceta.objects.filter(insumo__isnull=True)
            .filter(match_status__in=[LineaReceta.STATUS_NEEDS_REVIEW, LineaReceta.STATUS_REJECTED])
            .select_related("receta")
            .order_by("-match_score", "receta__nombre", "posicion")
        )
        recetas_total = recetas_qs.count()
        recetas_rows = [
            {
                "id": linea.id,
                "receta_id": linea.receta_id,
                "receta": linea.receta.nombre,
                "insumo_texto": linea.insumo_texto or "",
                "nombre_normalizado": normalizar_nombre(linea.insumo_texto or ""),
                "score": float(linea.match_score or 0),
                "metodo": linea.match_method or "",
                "estatus": linea.match_status,
            }
            for linea in recetas_qs[:limit]
        ]

        return Response(
            {
                "filters": {"limit": limit, "runs": runs_to_scan, "point_tipo": point_tipo},
                "totales": {
                    "almacen": len(almacen_rows),
                    "point": point_total,
                    "point_by_tipo": point_totals_by_tipo,
                    "recetas": recetas_total,
                },
                "items": {
                    "almacen": almacen_rows,
                    "point": point_rows,
                    "recetas": recetas_rows,
                },
            },
            status=status.HTTP_200_OK,
        )


class InventarioAliasesPendientesUnificadosView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar pendientes de homologación."},
                status=status.HTTP_403_FORBIDDEN,
            )

        limit = _parse_bounded_int(request.GET.get("limit", 120), default=120, min_value=1, max_value=600)
        runs_to_scan = _parse_bounded_int(request.GET.get("runs", 5), default=5, min_value=1, max_value=30)
        q = (request.GET.get("q") or "").strip()
        q_norm = normalizar_nombre(q)
        only_suggested = _parse_bool(request.GET.get("only_suggested"), default=False)
        min_sources = _parse_bounded_int(request.GET.get("min_sources", 1), default=1, min_value=1, max_value=3)
        score_min = float(_to_decimal(request.GET.get("score_min"), Decimal("0")))
        score_min = max(0.0, min(100.0, score_min))

        pending_rows: list[dict] = []
        sync_runs = list(AlmacenSyncRun.objects.only("id", "started_at", "pending_preview").order_by("-started_at")[:runs_to_scan])
        for run in sync_runs:
            for row in run.pending_preview or []:
                nombre_origen = str((row or {}).get("nombre_origen") or "").strip()
                if not nombre_origen:
                    continue
                pending_rows.append(
                    {
                        "nombre_origen": nombre_origen,
                        "nombre_normalizado": str((row or {}).get("nombre_normalizado") or normalizar_nombre(nombre_origen)),
                        "sugerencia": str((row or {}).get("suggestion") or ""),
                        "score": float((row or {}).get("score") or 0),
                        "source": str((row or {}).get("fuente") or "ALMACEN"),
                    }
                )

        pending_grouped = _build_pending_grouped(pending_rows)
        unified_rows, point_unmatched_count, receta_pending_lines = _build_cross_unified_rows(pending_grouped)
        filtered_rows = _apply_cross_filters(
            unified_rows,
            cross_q_norm=q_norm,
            cross_only_suggested=only_suggested,
            cross_min_sources=min_sources,
            cross_score_min=score_min,
        )

        overlap_2_plus = sum(1 for row in unified_rows if int(row.get("sources_active") or 0) >= 2)
        items = filtered_rows[:limit]

        return Response(
            {
                "filters": {
                    "limit": limit,
                    "runs": runs_to_scan,
                    "q": q,
                    "min_sources": min_sources,
                    "score_min": round(score_min, 2),
                    "only_suggested": only_suggested,
                },
                "totales": {
                    "runs_scanned": len(sync_runs),
                    "almacen_rows_raw": len(pending_rows),
                    "almacen_grouped": len(pending_grouped),
                    "unified_total": len(unified_rows),
                    "filtered_total": len(filtered_rows),
                    "returned": len(items),
                    "overlap_2_plus": overlap_2_plus,
                    "point_unmatched": point_unmatched_count,
                    "recetas_pending_lines": receta_pending_lines,
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )


class InventarioAliasesPendientesUnificadosResolveView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not has_any_role(request.user, ROLE_ADMIN, ROLE_DG, ROLE_COMPRAS):
            return Response(
                {"detail": "No tienes permisos para resolver pendientes unificados."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = InventarioCrossPendientesResolveSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        limit = int(data.get("limit") or 300)
        runs_to_scan = int(data.get("runs") or 5)
        q = str(data.get("q") or "").strip()
        q_norm = normalizar_nombre(q)
        min_sources = int(data.get("min_sources") or 2)
        score_min = float(data.get("score_min") or 0)
        score_min = max(0.0, min(100.0, score_min))
        only_suggested = bool(data.get("only_suggested", True))
        dry_run = bool(data.get("dry_run", True))
        nombres = [str(x or "").strip() for x in (data.get("nombres") or [])]
        selected_norms = {normalizar_nombre(x) for x in nombres if normalizar_nombre(x)}

        pending_rows: list[dict] = []
        sync_runs = list(AlmacenSyncRun.objects.only("id", "started_at", "pending_preview").order_by("-started_at")[:runs_to_scan])
        for run in sync_runs:
            for row in run.pending_preview or []:
                nombre_origen = str((row or {}).get("nombre_origen") or "").strip()
                if not nombre_origen:
                    continue
                pending_rows.append(
                    {
                        "nombre_origen": nombre_origen,
                        "nombre_normalizado": str((row or {}).get("nombre_normalizado") or normalizar_nombre(nombre_origen)),
                        "sugerencia": str((row or {}).get("suggestion") or ""),
                        "score": float((row or {}).get("score") or 0),
                        "source": str((row or {}).get("fuente") or "ALMACEN"),
                    }
                )

        pending_grouped = _build_pending_grouped(pending_rows)
        unified_rows, _, _ = _build_cross_unified_rows(pending_grouped)
        filtered_rows = _apply_cross_filters(
            unified_rows,
            cross_q_norm=q_norm,
            cross_only_suggested=only_suggested,
            cross_min_sources=min_sources,
            cross_score_min=score_min,
        )
        if selected_norms:
            filtered_rows = [row for row in filtered_rows if (row.get("nombre_normalizado") or "") in selected_norms]
        rows_to_process = filtered_rows[:limit]

        processed = 0
        resolved = 0
        created_aliases = 0
        updated_aliases = 0
        unchanged = 0
        skipped_no_suggestion = 0
        skipped_no_target = 0
        point_resolved_total = 0
        recetas_resolved_total = 0
        preview_actions: list[dict] = []

        write_context = nullcontext()
        if not dry_run:
            write_context = transaction.atomic()

        with write_context:
            for row in rows_to_process:
                processed += 1
                alias_name = str(row.get("nombre_muestra") or "").strip()
                alias_norm = normalizar_nombre(alias_name)
                suggestion_name = str(row.get("suggestion") or "").strip()
                suggestion_norm = normalizar_nombre(suggestion_name)
                if not suggestion_norm:
                    skipped_no_suggestion += 1
                    if len(preview_actions) < 120:
                        preview_actions.append(
                            {
                                "nombre_muestra": alias_name,
                                "sugerencia": suggestion_name,
                                "action": "skip_no_suggestion",
                            }
                        )
                    continue

                insumo_target = Insumo.objects.filter(activo=True, nombre_normalizado=suggestion_norm).first()
                if not insumo_target:
                    skipped_no_target += 1
                    if len(preview_actions) < 120:
                        preview_actions.append(
                            {
                                "nombre_muestra": alias_name,
                                "sugerencia": suggestion_name,
                                "action": "skip_no_target",
                            }
                        )
                    continue

                action_name = "noop"
                if not alias_norm or alias_norm == insumo_target.nombre_normalizado:
                    unchanged += 1
                    action_name = "noop_same_name"
                else:
                    alias_obj = InsumoAlias.objects.filter(nombre_normalizado=alias_norm).first()
                    if dry_run:
                        if alias_obj is None:
                            action_name = "create_alias"
                        elif alias_obj.insumo_id != insumo_target.id or alias_obj.nombre != alias_name[:250]:
                            action_name = "update_alias"
                        else:
                            action_name = "noop_alias_exists"
                            unchanged += 1
                    else:
                        if alias_obj is None:
                            InsumoAlias.objects.create(
                                nombre=alias_name[:250],
                                nombre_normalizado=alias_norm,
                                insumo=insumo_target,
                            )
                            created_aliases += 1
                            action_name = "create_alias"
                        else:
                            changes = []
                            if alias_obj.insumo_id != insumo_target.id:
                                alias_obj.insumo = insumo_target
                                changes.append("insumo")
                            if alias_obj.nombre != alias_name[:250]:
                                alias_obj.nombre = alias_name[:250]
                                changes.append("nombre")
                            if changes:
                                alias_obj.save(update_fields=changes)
                                updated_aliases += 1
                                action_name = "update_alias"
                            else:
                                unchanged += 1
                                action_name = "noop_alias_exists"

                if not dry_run:
                    p_count, r_count = _resolve_cross_source_with_alias(alias_name or suggestion_name, insumo_target)
                    point_resolved_total += p_count
                    recetas_resolved_total += r_count

                resolved += 1
                if len(preview_actions) < 120:
                    preview_actions.append(
                        {
                            "nombre_muestra": alias_name,
                            "sugerencia": suggestion_name,
                            "insumo_target": insumo_target.nombre,
                            "insumo_id": insumo_target.id,
                            "action": action_name,
                            "sources_active": int(row.get("sources_active") or 0),
                            "total_count": int(row.get("total_count") or 0),
                        }
                    )

        return Response(
            {
                "dry_run": dry_run,
                "filters": {
                    "q": q,
                    "runs": runs_to_scan,
                    "limit": limit,
                    "min_sources": min_sources,
                    "score_min": round(score_min, 2),
                    "only_suggested": only_suggested,
                    "selected_names_count": len(selected_norms),
                },
                "totales": {
                    "candidatos_filtrados": len(filtered_rows),
                    "procesados": processed,
                    "resueltos": resolved,
                    "aliases_creados": created_aliases,
                    "aliases_actualizados": updated_aliases,
                    "sin_cambio": unchanged,
                    "sin_sugerencia": skipped_no_suggestion,
                    "sin_insumo_objetivo": skipped_no_target,
                    "point_resueltos": point_resolved_total,
                    "recetas_resueltas": recetas_resolved_total,
                },
                "items": preview_actions,
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
                PointPendingMatch.objects.values("tipo")
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

        return Response(
            {
                "generated_at": timezone.now(),
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


class InventarioPointPendingResolveView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _resolve_pending_insumo_row(
        pending: PointPendingMatch,
        insumo_target: Insumo,
        create_aliases_enabled: bool,
    ) -> tuple[bool, bool, int]:
        point_code = (pending.point_codigo or "").strip()
        if point_code and insumo_target.codigo_point and insumo_target.codigo_point != point_code:
            return False, True, 0

        changed = []
        if point_code and insumo_target.codigo_point != point_code:
            insumo_target.codigo_point = point_code[:80]
            changed.append("codigo_point")
        if insumo_target.nombre_point != pending.point_nombre:
            insumo_target.nombre_point = (pending.point_nombre or "")[:250]
            changed.append("nombre_point")
        if changed:
            insumo_target.save(update_fields=changed)

        alias_created = 0
        if create_aliases_enabled:
            alias_norm = normalizar_nombre(pending.point_nombre or "")
            if alias_norm and alias_norm != insumo_target.nombre_normalizado:
                alias, was_created = InsumoAlias.objects.get_or_create(
                    nombre_normalizado=alias_norm,
                    defaults={"nombre": (pending.point_nombre or "")[:250], "insumo": insumo_target},
                )
                if not was_created and alias.insumo_id != insumo_target.id:
                    alias.insumo = insumo_target
                    alias.save(update_fields=["insumo"])
                if was_created:
                    alias_created = 1

        pending.delete()
        return True, False, alias_created

    def post(self, request):
        if not has_any_role(request.user, ROLE_ADMIN, ROLE_COMPRAS):
            return Response(
                {"detail": "No tienes permisos para resolver pendientes Point."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = InventarioPointPendingResolveSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        action = data["action"]
        tipo = data["tipo"]
        pending_ids = list(dict.fromkeys(data.get("pending_ids") or []))
        q = (data.get("q") or "").strip()
        score_min = float(data.get("score_min", 90.0) or 0)
        create_aliases = bool(data.get("create_aliases", True))

        selected_qs = PointPendingMatch.objects.filter(tipo=tipo)
        if pending_ids:
            selected_qs = selected_qs.filter(id__in=pending_ids)
        elif action == InventarioPointPendingResolveSerializer.ACTION_AUTO_RESOLVE_INSUMOS:
            if q:
                selected_qs = selected_qs.filter(
                    Q(point_nombre__icontains=q)
                    | Q(point_codigo__icontains=q)
                    | Q(fuzzy_sugerencia__icontains=q)
                )
        selected_qs = selected_qs.order_by("-fuzzy_score", "point_nombre")

        selected = list(selected_qs)
        if not selected:
            return Response(
                {"detail": "No se encontraron pendientes para procesar con los filtros enviados."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        resolved = 0
        conflicts = 0
        aliases_created = 0
        skipped_low_score = 0
        skipped_no_suggestion = 0
        skipped_no_target = 0
        proveedores_created = 0

        if action == InventarioPointPendingResolveSerializer.ACTION_RESOLVE_INSUMOS:
            insumo_target = Insumo.objects.filter(pk=data.get("insumo_id"), activo=True).first()
            if insumo_target is None:
                return Response(
                    {"detail": "insumo_id no encontrado o inactivo."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            for pending in selected:
                row_resolved, row_conflict, row_alias_created = self._resolve_pending_insumo_row(
                    pending=pending,
                    insumo_target=insumo_target,
                    create_aliases_enabled=create_aliases,
                )
                if row_conflict:
                    conflicts += 1
                    continue
                if row_resolved:
                    resolved += 1
                    aliases_created += row_alias_created

            return Response(
                {
                    "action": action,
                    "tipo": tipo,
                    "selected": len(selected),
                    "resolved": resolved,
                    "conflicts": conflicts,
                    "aliases_created": aliases_created,
                    "target": {"insumo_id": insumo_target.id, "insumo": insumo_target.nombre},
                },
                status=status.HTTP_200_OK,
            )

        if action == InventarioPointPendingResolveSerializer.ACTION_AUTO_RESOLVE_INSUMOS:
            if tipo != PointPendingMatch.TIPO_INSUMO:
                return Response(
                    {"detail": "La auto-resolución por sugerencia aplica solo para tipo=INSUMO."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            for pending in selected:
                if float(pending.fuzzy_score or 0.0) < score_min:
                    skipped_low_score += 1
                    continue

                sugerencia_norm = normalizar_nombre(pending.fuzzy_sugerencia or "")
                if not sugerencia_norm:
                    skipped_no_suggestion += 1
                    continue

                insumo_target = (
                    Insumo.objects.filter(activo=True, nombre_normalizado=sugerencia_norm)
                    .only("id", "codigo_point", "nombre_point", "nombre_normalizado")
                    .first()
                )
                if not insumo_target:
                    skipped_no_target += 1
                    continue

                row_resolved, row_conflict, row_alias_created = self._resolve_pending_insumo_row(
                    pending=pending,
                    insumo_target=insumo_target,
                    create_aliases_enabled=create_aliases,
                )
                if row_conflict:
                    conflicts += 1
                    continue
                if row_resolved:
                    resolved += 1
                    aliases_created += row_alias_created

            return Response(
                {
                    "action": action,
                    "tipo": tipo,
                    "selected": len(selected),
                    "resolved": resolved,
                    "conflicts": conflicts,
                    "aliases_created": aliases_created,
                    "skipped": {
                        "low_score": skipped_low_score,
                        "no_suggestion": skipped_no_suggestion,
                        "no_target": skipped_no_target,
                    },
                    "score_min": round(score_min, 2),
                },
                status=status.HTTP_200_OK,
            )

        if action == InventarioPointPendingResolveSerializer.ACTION_RESOLVE_PRODUCTOS:
            receta_target = Receta.objects.filter(pk=data.get("receta_id")).first()
            if receta_target is None:
                return Response(
                    {"detail": "receta_id no encontrada."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            for pending in selected:
                point_code = (pending.point_codigo or "").strip()
                if point_code:
                    point_norm = normalizar_codigo_point(point_code)
                    primary_norm = normalizar_codigo_point(receta_target.codigo_point)
                    if not receta_target.codigo_point:
                        receta_target.codigo_point = point_code[:80]
                        receta_target.save(update_fields=["codigo_point"])
                    elif primary_norm != point_norm:
                        if not point_norm:
                            conflicts += 1
                            continue
                        if not create_aliases:
                            conflicts += 1
                            continue

                        alias, was_created = RecetaCodigoPointAlias.objects.get_or_create(
                            codigo_point_normalizado=point_norm,
                            defaults={
                                "receta": receta_target,
                                "codigo_point": point_code[:80],
                                "nombre_point": (pending.point_nombre or "")[:250],
                                "activo": True,
                            },
                        )
                        if not was_created and alias.receta_id != receta_target.id:
                            conflicts += 1
                            continue

                        changed = []
                        if alias.codigo_point != point_code[:80]:
                            alias.codigo_point = point_code[:80]
                            changed.append("codigo_point")
                        if (pending.point_nombre or "").strip() and alias.nombre_point != (pending.point_nombre or "")[:250]:
                            alias.nombre_point = (pending.point_nombre or "")[:250]
                            changed.append("nombre_point")
                        if not alias.activo:
                            alias.activo = True
                            changed.append("activo")
                        if changed:
                            alias.save(update_fields=changed)
                        if was_created:
                            aliases_created += 1

                pending.delete()
                resolved += 1

            return Response(
                {
                    "action": action,
                    "tipo": tipo,
                    "selected": len(selected),
                    "resolved": resolved,
                    "conflicts": conflicts,
                    "aliases_created": aliases_created,
                    "target": {"receta_id": receta_target.id, "receta": receta_target.nombre},
                },
                status=status.HTTP_200_OK,
            )

        if action == InventarioPointPendingResolveSerializer.ACTION_RESOLVE_PROVEEDORES:
            proveedor_target = None
            proveedor_id = data.get("proveedor_id")
            if proveedor_id:
                proveedor_target = Proveedor.objects.filter(pk=proveedor_id).first()
                if proveedor_target is None:
                    return Response(
                        {"detail": "proveedor_id no encontrado."},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

            for pending in selected:
                if proveedor_target is None:
                    _, was_created = Proveedor.objects.get_or_create(
                        nombre=(pending.point_nombre or "")[:200],
                        defaults={"activo": True},
                    )
                    if was_created:
                        proveedores_created += 1
                pending.delete()
                resolved += 1

            return Response(
                {
                    "action": action,
                    "tipo": tipo,
                    "selected": len(selected),
                    "resolved": resolved,
                    "proveedores_created": proveedores_created,
                    "target": (
                        {"proveedor_id": proveedor_target.id, "proveedor": proveedor_target.nombre}
                        if proveedor_target
                        else None
                    ),
                },
                status=status.HTTP_200_OK,
            )

        if action == InventarioPointPendingResolveSerializer.ACTION_DISCARD:
            deleted, _ = PointPendingMatch.objects.filter(id__in=[p.id for p in selected], tipo=tipo).delete()
            return Response(
                {
                    "action": action,
                    "tipo": tipo,
                    "selected": len(selected),
                    "discarded": deleted,
                },
                status=status.HTTP_200_OK,
            )

        return Response({"detail": "Acción no soportada."}, status=status.HTTP_400_BAD_REQUEST)


class InventarioAjustesView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not can_view_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para consultar ajustes de inventario."},
                status=status.HTTP_403_FORBIDDEN,
            )

        estatus = (request.GET.get("estatus") or "").strip().upper()
        valid_status = {
            AjusteInventario.STATUS_PENDIENTE,
            AjusteInventario.STATUS_APLICADO,
            AjusteInventario.STATUS_RECHAZADO,
        }
        qs = AjusteInventario.objects.select_related("insumo", "solicitado_por", "aprobado_por").order_by("-creado_en")
        if estatus in valid_status:
            qs = qs.filter(estatus=estatus)

        limit = _parse_bounded_int(
            request.GET.get("limit", 120),
            default=120,
            min_value=1,
            max_value=500,
        )
        items = [_serialize_ajuste_row(a) for a in qs[:limit]]

        totals_qs = qs if estatus in valid_status else AjusteInventario.objects.all()
        return Response(
            {
                "filters": {"estatus": estatus if estatus in valid_status else "", "limit": limit},
                "totales": {
                    "rows": len(items),
                    "pendientes": totals_qs.filter(estatus=AjusteInventario.STATUS_PENDIENTE).count(),
                    "aplicados": totals_qs.filter(estatus=AjusteInventario.STATUS_APLICADO).count(),
                    "rechazados": totals_qs.filter(estatus=AjusteInventario.STATUS_RECHAZADO).count(),
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not can_manage_inventario(request.user):
            return Response(
                {"detail": "No tienes permisos para registrar ajustes de inventario."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = InventarioAjusteCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        insumo = get_object_or_404(Insumo, pk=data["insumo_id"])
        aplicar_inmediato = bool(data.get("aplicar_inmediato"))
        comentario = data.get("comentario_revision") or ""
        if aplicar_inmediato and not _can_approve_ajustes(request.user):
            return Response(
                {
                    "detail": (
                        "No tienes permisos para aplicar ajustes inmediatamente. "
                        "Registra el ajuste en pendiente y solicita aprobación."
                    )
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        with transaction.atomic():
            ajuste = AjusteInventario.objects.create(
                insumo=insumo,
                cantidad_sistema=data["cantidad_sistema"],
                cantidad_fisica=data["cantidad_fisica"],
                motivo=data["motivo"],
                estatus=AjusteInventario.STATUS_PENDIENTE,
                solicitado_por=request.user,
            )
            if aplicar_inmediato:
                _apply_ajuste(ajuste, request.user, comentario=comentario)

        payload = _serialize_ajuste_row(ajuste)
        payload["aplicado"] = ajuste.estatus == AjusteInventario.STATUS_APLICADO
        return Response(payload, status=status.HTTP_201_CREATED)


class InventarioAjusteDecisionView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, ajuste_id: int):
        if not _can_approve_ajustes(request.user):
            return Response(
                {"detail": "No tienes permisos para aprobar/rechazar ajustes de inventario."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ajuste = get_object_or_404(
            AjusteInventario.objects.select_related("insumo", "solicitado_por", "aprobado_por"),
            pk=ajuste_id,
        )
        ser = InventarioAjusteDecisionSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        action = ser.validated_data["action"]
        comentario = ser.validated_data.get("comentario_revision") or ""

        if action == "reject":
            if ajuste.estatus == AjusteInventario.STATUS_APLICADO:
                return Response(
                    {"detail": "No se puede rechazar un ajuste ya aplicado."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            ajuste.estatus = AjusteInventario.STATUS_RECHAZADO
            ajuste.aprobado_por = request.user
            ajuste.aprobado_en = timezone.now()
            ajuste.aplicado_en = None
            ajuste.comentario_revision = comentario
            ajuste.save(
                update_fields=[
                    "estatus",
                    "aprobado_por",
                    "aprobado_en",
                    "aplicado_en",
                    "comentario_revision",
                ]
            )
            log_event(
                request.user,
                "REJECT",
                "inventario.AjusteInventario",
                ajuste.id,
                {"folio": ajuste.folio, "estatus": ajuste.estatus, "comentario_revision": comentario},
            )
        else:
            if ajuste.estatus == AjusteInventario.STATUS_APLICADO:
                return Response(
                    {
                        "detail": "El ajuste ya estaba aplicado.",
                        "item": _serialize_ajuste_row(ajuste),
                    },
                    status=status.HTTP_200_OK,
                )
            if ajuste.estatus == AjusteInventario.STATUS_RECHAZADO:
                return Response(
                    {"detail": "El ajuste fue rechazado y no puede aplicarse."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            _apply_ajuste(ajuste, request.user, comentario=comentario)

        payload = _serialize_ajuste_row(ajuste)
        payload["action"] = action
        return Response(payload, status=status.HTTP_200_OK)


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

        included_rows = [row for row in rows if bool(row.get("include", True))]
        insumo_ids = sorted({int(row.get("insumo_id") or 0) for row in included_rows if int(row.get("insumo_id") or 0) > 0})
        proveedor_ids = sorted(
            {
                int(row.get("proveedor_id") or 0)
                for row in included_rows
                if int(row.get("proveedor_id") or 0) > 0
            }
        )

        insumos_map = Insumo.objects.select_related("proveedor_principal").in_bulk(insumo_ids)
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
            for row in rows:
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
                    created_items.append(
                        {
                            "id": solicitud.id,
                            "folio": solicitud.folio,
                            "insumo": solicitud.insumo.nombre,
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

        rows = list(qs[:limit])
        insumo_ids = [r.insumo_id for r in rows]
        latest_cost_by_insumo: dict[int, Decimal] = {}
        if insumo_ids:
            for c in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
                if c.insumo_id not in latest_cost_by_insumo:
                    latest_cost_by_insumo[c.insumo_id] = _to_decimal(c.costo_unitario)

        items = []
        presupuesto_total = Decimal("0")
        by_status = {k: 0 for k in valid_status}
        for r in rows:
            costo_unitario = _to_decimal(latest_cost_by_insumo.get(r.insumo_id, 0))
            presupuesto = (_to_decimal(r.cantidad) * costo_unitario).quantize(Decimal("0.01"))
            presupuesto_total += presupuesto
            by_status[r.estatus] = by_status.get(r.estatus, 0) + 1
            items.append(
                {
                    "id": r.id,
                    "folio": r.folio,
                    "estatus": r.estatus,
                    "area": r.area,
                    "solicitante": r.solicitante,
                    "insumo_id": r.insumo_id,
                    "insumo": r.insumo.nombre,
                    "unidad": r.insumo.unidad_base.codigo if r.insumo.unidad_base_id and r.insumo.unidad_base else "",
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
                },
                "totales": {
                    "rows": len(items),
                    "presupuesto_estimado_total": str(presupuesto_total),
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

        rows = list(qs[:limit])
        items = []
        monto_total = Decimal("0")
        by_status = {k: 0 for k in valid_status}
        for r in rows:
            monto = _to_decimal(r.monto_estimado)
            monto_total += monto
            by_status[r.estatus] = by_status.get(r.estatus, 0) + 1
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
                        r.solicitud.insumo.nombre
                        if r.solicitud_id and getattr(r.solicitud, "insumo_id", None) and r.solicitud.insumo
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
                },
                "totales": {
                    "rows": len(items),
                    "monto_estimado_total": str(monto_total),
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

        rows = list(qs[:limit])
        items = []
        by_status = {k: 0 for k in valid_status}
        for r in rows:
            by_status[r.estatus] = by_status.get(r.estatus, 0) + 1
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
                },
                "totales": {"rows": len(items), "by_status": by_status},
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

        insumo = get_object_or_404(Insumo, pk=data["insumo_id"])
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
                costo_qs = CostoInsumo.objects.filter(insumo=insumo).order_by("-fecha", "-id")
                costo = costo_qs.filter(proveedor=proveedor).first() or costo_qs.first()
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
            proveedor = solicitud.proveedor_sugerido or solicitud.insumo.proveedor_principal
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

        costo_qs = CostoInsumo.objects.filter(insumo=solicitud.insumo).order_by("-fecha", "-id")
        costo = costo_qs.filter(proveedor=proveedor).first() or costo_qs.first()
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

        ser = ForecastBacktestRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        alcance = data.get("alcance") or "mes"
        fecha_base = data.get("fecha_base") or timezone.localdate()
        incluir_preparaciones = bool(data.get("incluir_preparaciones"))
        safety_pct = _to_decimal(data.get("safety_pct"), default=Decimal("0"))
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

        windows = _forecast_backtest_windows(alcance, fecha_base, periods)
        if not windows:
            return Response(
                {"detail": "No se pudo construir ventanas de backtest."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        windows_payload: list[dict] = []
        sum_forecast_total = Decimal("0")
        sum_actual_total = Decimal("0")
        sum_abs_error = Decimal("0")
        ape_sum = Decimal("0")
        ape_count = 0

        for window_start, window_end in windows:
            periodo_window = f"{window_start.year:04d}-{window_start.month:02d}"
            forecast_result = _build_forecast_from_history(
                alcance=alcance,
                periodo=periodo_window,
                fecha_base=window_start,
                sucursal=sucursal,
                incluir_preparaciones=incluir_preparaciones,
                safety_pct=safety_pct,
            )
            forecast_map = {
                int(row["receta_id"]): _to_decimal(row["forecast_qty"])
                for row in (forecast_result.get("rows") or [])
            }

            actual_qs = VentaHistorica.objects.filter(fecha__gte=window_start, fecha__lte=window_end)
            if sucursal:
                actual_qs = actual_qs.filter(sucursal=sucursal)
            if not incluir_preparaciones:
                actual_qs = actual_qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
            actual_map = {
                int(row["receta_id"]): _to_decimal(row["total"])
                for row in actual_qs.values("receta_id").annotate(total=Sum("cantidad"))
            }

            union_ids = sorted(set(forecast_map.keys()) | set(actual_map.keys()))
            if not union_ids:
                continue

            receta_names = {
                r.id: r.nombre for r in Receta.objects.filter(id__in=union_ids).only("id", "nombre")
            }
            rows = []
            forecast_total = Decimal("0")
            actual_total = Decimal("0")
            abs_error_total = Decimal("0")
            local_ape_sum = Decimal("0")
            local_ape_count = 0
            for receta_id in union_ids:
                forecast_qty = forecast_map.get(receta_id, Decimal("0"))
                actual_qty = actual_map.get(receta_id, Decimal("0"))
                delta_qty = forecast_qty - actual_qty
                abs_error = abs(delta_qty)

                forecast_total += forecast_qty
                actual_total += actual_qty
                abs_error_total += abs_error

                variacion_pct = None
                status_tag = "SIN_BASE"
                if actual_qty > 0:
                    variacion_pct = ((delta_qty / actual_qty) * Decimal("100")).quantize(Decimal("0.1"))
                    local_ape_sum += abs(variacion_pct)
                    local_ape_count += 1
                    if variacion_pct > Decimal("10"):
                        status_tag = "SOBRE"
                    elif variacion_pct < Decimal("-10"):
                        status_tag = "BAJO"
                    else:
                        status_tag = "OK"

                rows.append(
                    {
                        "receta_id": receta_id,
                        "receta": receta_names.get(receta_id) or f"Receta {receta_id}",
                        "forecast_qty": float(forecast_qty),
                        "actual_qty": float(actual_qty),
                        "delta_qty": float(delta_qty),
                        "abs_error": float(abs_error),
                        "variacion_pct": float(variacion_pct) if variacion_pct is not None else None,
                        "status": status_tag,
                    }
                )

            rows.sort(key=lambda r: abs(r["abs_error"]), reverse=True)
            mae = (abs_error_total / Decimal(str(len(union_ids)))).quantize(Decimal("0.001"))
            mape = None
            if local_ape_count > 0:
                mape = (local_ape_sum / Decimal(str(local_ape_count))).quantize(Decimal("0.1"))
                ape_sum += local_ape_sum
                ape_count += local_ape_count

            sum_forecast_total += forecast_total
            sum_actual_total += actual_total
            sum_abs_error += abs_error_total

            windows_payload.append(
                {
                    "window_start": str(window_start),
                    "window_end": str(window_end),
                    "periodo": periodo_window,
                    "recetas_count": len(union_ids),
                    "forecast_total": float(forecast_total),
                    "actual_total": float(actual_total),
                    "bias_total": float((forecast_total - actual_total).quantize(Decimal("0.001"))),
                    "mae": float(mae),
                    "mape": float(mape) if mape is not None else None,
                    "top_errors": rows[:top],
                }
            )

        if not windows_payload:
            return Response(
                {"detail": "No hay historial suficiente para evaluar backtest en el alcance seleccionado."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        overall_mape = None
        if ape_count > 0:
            overall_mape = (ape_sum / Decimal(str(ape_count))).quantize(Decimal("0.1"))
        overall_mae = (sum_abs_error / Decimal(str(max(1, len(windows_payload))))).quantize(Decimal("0.001"))

        return Response(
            {
                "scope": {
                    "alcance": alcance,
                    "fecha_base": str(fecha_base),
                    "periods": periods,
                    "sucursal_id": sucursal.id if sucursal else None,
                    "sucursal_nombre": f"{sucursal.codigo} - {sucursal.nombre}" if sucursal else "Todas",
                },
                "totals": {
                    "windows_evaluated": len(windows_payload),
                    "forecast_total": float(sum_forecast_total),
                    "actual_total": float(sum_actual_total),
                    "bias_total": float((sum_forecast_total - sum_actual_total).quantize(Decimal("0.001"))),
                    "mae_promedio": float(overall_mae),
                    "mape_promedio": float(overall_mape) if overall_mape is not None else None,
                },
                "windows": windows_payload,
            },
            status=status.HTTP_200_OK,
        )


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
        incluir_preparaciones = _parse_bool(request.GET.get("incluir_preparaciones"), default=False)

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
            return Response(
                {
                    "scope": {
                        "months": months,
                        "fecha_desde": str(fecha_desde),
                        "fecha_hasta": str(fecha_hasta),
                        "sucursal_id": sucursal.id if sucursal else None,
                        "sucursal": sucursal.nombre if sucursal else "Todas",
                        "receta_id": receta.id if receta else None,
                        "receta": receta.nombre if receta else "Todas",
                    },
                    "totales": {
                        "filas": 0,
                        "dias_con_venta": 0,
                        "recetas": 0,
                        "cantidad_total": 0.0,
                        "promedio_diario": 0.0,
                    },
                    "seasonality": {"by_month": [], "by_weekday": []},
                    "top_recetas": [],
                },
                status=status.HTTP_200_OK,
            )

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

        top_recetas = []
        for receta_id, qty_total in sorted(recipe_totals.items(), key=lambda item: item[1], reverse=True)[:top]:
            days_count = len(recipe_days.get(receta_id, set())) or 1
            avg_day = qty_total / Decimal(str(days_count))
            share = Decimal("0")
            if total_qty > 0:
                share = (qty_total / total_qty) * Decimal("100")
            top_recetas.append(
                {
                    "receta_id": receta_id,
                    "receta": recipe_names.get(receta_id) or f"Receta {receta_id}",
                    "cantidad_total": _to_float(qty_total),
                    "promedio_dia_activo": _to_float(avg_day.quantize(Decimal("0.001"))),
                    "dias_con_venta": days_count,
                    "participacion_pct": _to_float(share.quantize(Decimal("0.1"))),
                }
            )

        return Response(
            {
                "scope": {
                    "months": months,
                    "fecha_desde": str(fecha_desde),
                    "fecha_hasta": str(fecha_hasta),
                    "sucursal_id": sucursal.id if sucursal else None,
                    "sucursal": sucursal.nombre if sucursal else "Todas",
                    "receta_id": receta.id if receta else None,
                    "receta": receta.nombre if receta else "Todas",
                },
                "totales": {
                    "filas": len(rows),
                    "dias_con_venta": len(date_totals),
                    "recetas": len(recipe_totals),
                    "cantidad_total": _to_float(total_qty),
                    "promedio_diario": _to_float(global_avg.quantize(Decimal("0.001"))),
                },
                "seasonality": {
                    "by_month": month_rows,
                    "by_weekday": weekday_rows,
                },
                "top_recetas": top_recetas,
            },
            status=status.HTTP_200_OK,
        )


class VentaHistoricaListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar historial de ventas."},
                status=status.HTTP_403_FORBIDDEN,
            )

        q = (request.GET.get("q") or "").strip()
        periodo = (request.GET.get("periodo") or request.GET.get("mes") or "").strip()
        sucursal_id_raw = (request.GET.get("sucursal_id") or "").strip()
        receta_id_raw = (request.GET.get("receta_id") or "").strip()
        fecha_desde_raw = (request.GET.get("fecha_desde") or "").strip()
        fecha_hasta_raw = (request.GET.get("fecha_hasta") or "").strip()
        limit = _parse_bounded_int(request.GET.get("limit", 150), default=150, min_value=1, max_value=1000)

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

        qs = VentaHistorica.objects.select_related("receta", "sucursal").order_by("-fecha", "-id")
        parsed_period = _parse_period(periodo)
        if periodo and not parsed_period:
            return Response(
                {"detail": "periodo inválido. Usa formato YYYY-MM."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if parsed_period:
            year, month = parsed_period
            qs = qs.filter(fecha__year=year, fecha__month=month)
            periodo = f"{year:04d}-{month:02d}"
        else:
            periodo = ""

        if fecha_desde:
            qs = qs.filter(fecha__gte=fecha_desde)
        if fecha_hasta:
            qs = qs.filter(fecha__lte=fecha_hasta)

        if sucursal_id_raw:
            if not sucursal_id_raw.isdigit():
                return Response(
                    {"detail": "sucursal_id debe ser numérico."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            qs = qs.filter(sucursal_id=int(sucursal_id_raw))

        if receta_id_raw:
            if not receta_id_raw.isdigit():
                return Response(
                    {"detail": "receta_id debe ser numérico."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            qs = qs.filter(receta_id=int(receta_id_raw))

        if q:
            qs = qs.filter(
                Q(receta__nombre__icontains=q)
                | Q(receta__codigo_point__icontains=q)
                | Q(sucursal__nombre__icontains=q)
                | Q(sucursal__codigo__icontains=q)
                | Q(fuente__icontains=q)
            )

        rows = list(qs[:limit])
        items = []
        cantidad_total = Decimal("0")
        tickets_total = 0
        monto_total = Decimal("0")
        by_sucursal: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

        for r in rows:
            cantidad = _to_decimal(r.cantidad)
            cantidad_total += cantidad
            tickets_total += int(r.tickets or 0)
            monto = _to_decimal(r.monto_total)
            monto_total += monto
            sucursal_key = r.sucursal.codigo if r.sucursal_id and r.sucursal else "GLOBAL"
            by_sucursal[sucursal_key] += cantidad
            items.append(
                {
                    "id": r.id,
                    "fecha": str(r.fecha),
                    "receta_id": r.receta_id,
                    "receta": r.receta.nombre,
                    "codigo_point": r.receta.codigo_point,
                    "sucursal_id": r.sucursal_id,
                    "sucursal": r.sucursal.nombre if r.sucursal_id and r.sucursal else "",
                    "sucursal_codigo": r.sucursal.codigo if r.sucursal_id and r.sucursal else "",
                    "cantidad": str(cantidad),
                    "tickets": int(r.tickets or 0),
                    "monto_total": str(monto),
                    "fuente": r.fuente,
                    "actualizado_en": r.actualizado_en,
                }
            )

        sucursales_payload = [
            {"sucursal": k, "cantidad_total": str(v)}
            for k, v in sorted(by_sucursal.items(), key=lambda entry: entry[1], reverse=True)
        ]

        return Response(
            {
                "filters": {
                    "q": q,
                    "periodo": periodo,
                    "sucursal_id": sucursal_id_raw,
                    "receta_id": receta_id_raw,
                    "fecha_desde": str(fecha_desde) if fecha_desde else "",
                    "fecha_hasta": str(fecha_hasta) if fecha_hasta else "",
                    "limit": limit,
                },
                "totales": {
                    "rows": len(items),
                    "cantidad_total": str(cantidad_total),
                    "tickets_total": tickets_total,
                    "monto_total": str(monto_total),
                    "by_sucursal": sucursales_payload,
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )


class PronosticoVentaListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar pronósticos de venta."},
                status=status.HTTP_403_FORBIDDEN,
            )

        q = (request.GET.get("q") or "").strip()
        periodo = (request.GET.get("periodo") or "").strip()
        periodo_desde = (request.GET.get("periodo_desde") or "").strip()
        periodo_hasta = (request.GET.get("periodo_hasta") or "").strip()
        receta_id_raw = (request.GET.get("receta_id") or "").strip()
        limit = _parse_bounded_int(request.GET.get("limit", 150), default=150, min_value=1, max_value=1000)

        if periodo:
            parsed_period = _parse_period(periodo)
            if not parsed_period:
                return Response(
                    {"detail": "periodo inválido. Usa formato YYYY-MM."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            year, month = parsed_period
            periodo = f"{year:04d}-{month:02d}"

        if periodo_desde:
            parsed_since = _parse_period(periodo_desde)
            if not parsed_since:
                return Response(
                    {"detail": "periodo_desde inválido. Usa formato YYYY-MM."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            periodo_desde = f"{parsed_since[0]:04d}-{parsed_since[1]:02d}"

        if periodo_hasta:
            parsed_until = _parse_period(periodo_hasta)
            if not parsed_until:
                return Response(
                    {"detail": "periodo_hasta inválido. Usa formato YYYY-MM."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            periodo_hasta = f"{parsed_until[0]:04d}-{parsed_until[1]:02d}"

        if periodo_desde and periodo_hasta and periodo_hasta < periodo_desde:
            return Response(
                {"detail": "periodo_hasta no puede ser menor que periodo_desde."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        qs = PronosticoVenta.objects.select_related("receta").order_by("-periodo", "receta__nombre")
        if periodo:
            qs = qs.filter(periodo=periodo)
        if periodo_desde:
            qs = qs.filter(periodo__gte=periodo_desde)
        if periodo_hasta:
            qs = qs.filter(periodo__lte=periodo_hasta)

        if receta_id_raw:
            if not receta_id_raw.isdigit():
                return Response(
                    {"detail": "receta_id debe ser numérico."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            qs = qs.filter(receta_id=int(receta_id_raw))

        if q:
            qs = qs.filter(
                Q(receta__nombre__icontains=q)
                | Q(receta__codigo_point__icontains=q)
                | Q(fuente__icontains=q)
            )

        rows = list(qs[:limit])
        items = []
        cantidad_total = Decimal("0")
        periodos = set()
        for r in rows:
            qty = _to_decimal(r.cantidad)
            cantidad_total += qty
            periodos.add(r.periodo)
            items.append(
                {
                    "id": r.id,
                    "receta_id": r.receta_id,
                    "receta": r.receta.nombre,
                    "codigo_point": r.receta.codigo_point,
                    "periodo": r.periodo,
                    "cantidad": str(qty),
                    "fuente": r.fuente,
                    "actualizado_en": r.actualizado_en,
                }
            )

        return Response(
            {
                "filters": {
                    "q": q,
                    "periodo": periodo,
                    "periodo_desde": periodo_desde,
                    "periodo_hasta": periodo_hasta,
                    "receta_id": receta_id_raw,
                    "limit": limit,
                },
                "totales": {
                    "rows": len(items),
                    "cantidad_total": str(cantidad_total),
                    "periodos_count": len(periodos),
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )


class VentasPipelineResumenView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar resumen del pipeline de ventas."},
                status=status.HTTP_403_FORBIDDEN,
            )

        periodo = (request.GET.get("periodo") or "").strip()
        top = _parse_bounded_int(request.GET.get("top", 120), default=120, min_value=1, max_value=500)
        if periodo:
            parsed_period = _parse_period(periodo)
            if not parsed_period:
                return Response(
                    {"detail": "periodo inválido. Usa formato YYYY-MM."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            periodo = f"{parsed_period[0]:04d}-{parsed_period[1]:02d}"
        else:
            today = timezone.localdate()
            periodo = f"{today.year:04d}-{today.month:02d}"
            parsed_period = (today.year, today.month)
        year, month = parsed_period

        incluir_preparaciones = _parse_bool(request.GET.get("incluir_preparaciones"), default=False)
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

        historial_qs = VentaHistorica.objects.filter(fecha__year=year, fecha__month=month)
        solicitudes_qs = SolicitudVenta.objects.filter(periodo=periodo)
        pronostico_qs = PronosticoVenta.objects.filter(periodo=periodo)

        if sucursal:
            historial_qs = historial_qs.filter(sucursal=sucursal)
            solicitudes_qs = solicitudes_qs.filter(sucursal=sucursal)

        if not incluir_preparaciones:
            historial_qs = historial_qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
            solicitudes_qs = solicitudes_qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)
            pronostico_qs = pronostico_qs.filter(receta__tipo=Receta.TIPO_PRODUCTO_FINAL)

        historial_total = _to_decimal(historial_qs.aggregate(total=Sum("cantidad"))["total"])
        solicitud_total = _to_decimal(solicitudes_qs.aggregate(total=Sum("cantidad"))["total"])
        pronostico_total = _to_decimal(pronostico_qs.aggregate(total=Sum("cantidad"))["total"])

        by_alcance = {k: Decimal("0") for k in [SolicitudVenta.ALCANCE_MES, SolicitudVenta.ALCANCE_SEMANA, SolicitudVenta.ALCANCE_FIN_SEMANA]}
        for row in solicitudes_qs.values("alcance").annotate(total=Sum("cantidad")):
            by_alcance[row["alcance"]] = _to_decimal(row["total"])

        cobertura_solicitud_pct = None
        if pronostico_total > 0:
            cobertura_solicitud_pct = ((solicitud_total / pronostico_total) * Decimal("100")).quantize(Decimal("0.1"))

        cumplimiento_historial_pct = None
        if solicitud_total > 0:
            cumplimiento_historial_pct = ((historial_total / solicitud_total) * Decimal("100")).quantize(Decimal("0.1"))

        latest_historial = historial_qs.order_by("-actualizado_en").values_list("actualizado_en", flat=True).first()
        latest_pronostico = pronostico_qs.order_by("-actualizado_en").values_list("actualizado_en", flat=True).first()
        latest_solicitud = solicitudes_qs.order_by("-actualizado_en").values_list("actualizado_en", flat=True).first()

        historial_map = {
            int(row["receta_id"]): _to_decimal(row["total"])
            for row in historial_qs.values("receta_id").annotate(total=Sum("cantidad"))
        }
        pronostico_map = {
            int(row["receta_id"]): _to_decimal(row["total"])
            for row in pronostico_qs.values("receta_id").annotate(total=Sum("cantidad"))
        }
        solicitud_map = {
            int(row["receta_id"]): _to_decimal(row["total"])
            for row in solicitudes_qs.values("receta_id").annotate(total=Sum("cantidad"))
        }
        receta_ids = sorted(set(historial_map.keys()) | set(pronostico_map.keys()) | set(solicitud_map.keys()))
        receta_names = {
            int(receta_id): nombre
            for receta_id, nombre in Receta.objects.filter(id__in=receta_ids).values_list("id", "nombre")
        }

        rows_tmp: list[dict[str, Any]] = []
        for receta_id in receta_ids:
            historial_qty = historial_map.get(receta_id, Decimal("0"))
            pronostico_qty = pronostico_map.get(receta_id, Decimal("0"))
            solicitud_qty = solicitud_map.get(receta_id, Decimal("0"))
            delta_solicitud_vs_pronostico = solicitud_qty - pronostico_qty
            delta_historial_vs_solicitud = historial_qty - solicitud_qty

            cobertura_pct = None
            if pronostico_qty > 0:
                cobertura_pct = ((solicitud_qty / pronostico_qty) * Decimal("100")).quantize(Decimal("0.1"))

            cumplimiento_pct = None
            if solicitud_qty > 0:
                cumplimiento_pct = ((historial_qty / solicitud_qty) * Decimal("100")).quantize(Decimal("0.1"))

            status_tag = "SIN_MOV"
            if solicitud_qty > 0:
                threshold = max(Decimal("1"), solicitud_qty * Decimal("0.10"))
                if delta_historial_vs_solicitud > threshold:
                    status_tag = "SOBRE"
                elif delta_historial_vs_solicitud < (threshold * Decimal("-1")):
                    status_tag = "BAJO"
                else:
                    status_tag = "OK"
            elif historial_qty > 0:
                status_tag = "SIN_SOLICITUD"
            elif pronostico_qty > 0:
                status_tag = "SIN_SOLICITUD"

            rows_tmp.append(
                {
                    "_sort": abs(delta_historial_vs_solicitud),
                    "receta_id": receta_id,
                    "receta": receta_names.get(receta_id) or f"Receta {receta_id}",
                    "historial_qty": _to_float(historial_qty),
                    "pronostico_qty": _to_float(pronostico_qty),
                    "solicitud_qty": _to_float(solicitud_qty),
                    "delta_solicitud_vs_pronostico": _to_float(delta_solicitud_vs_pronostico.quantize(Decimal("0.001"))),
                    "delta_historial_vs_solicitud": _to_float(delta_historial_vs_solicitud.quantize(Decimal("0.001"))),
                    "cobertura_pct": _to_float(cobertura_pct) if cobertura_pct is not None else None,
                    "cumplimiento_pct": _to_float(cumplimiento_pct) if cumplimiento_pct is not None else None,
                    "status": status_tag,
                }
            )
        rows_tmp.sort(key=lambda row: row["_sort"], reverse=True)
        rows = []
        for row in rows_tmp[:top]:
            row.pop("_sort", None)
            rows.append(row)

        return Response(
            {
                "scope": {
                    "periodo": periodo,
                    "sucursal_id": sucursal.id if sucursal else None,
                    "sucursal": sucursal.nombre if sucursal else "Todas",
                    "incluir_preparaciones": incluir_preparaciones,
                    "top": top,
                },
                "totales": {
                    "historial_qty": _to_float(historial_total),
                    "pronostico_qty": _to_float(pronostico_total),
                    "solicitud_qty": _to_float(solicitud_total),
                    "delta_solicitud_vs_pronostico": _to_float((solicitud_total - pronostico_total).quantize(Decimal("0.001"))),
                    "delta_historial_vs_solicitud": _to_float((historial_total - solicitud_total).quantize(Decimal("0.001"))),
                    "cobertura_solicitud_pct": _to_float(cobertura_solicitud_pct) if cobertura_solicitud_pct is not None else None,
                    "cumplimiento_historial_pct": _to_float(cumplimiento_historial_pct) if cumplimiento_historial_pct is not None else None,
                    "historial_recetas": historial_qs.values("receta_id").distinct().count(),
                    "pronostico_recetas": pronostico_qs.values("receta_id").distinct().count(),
                    "solicitud_recetas": solicitudes_qs.values("receta_id").distinct().count(),
                    "rows_count": len(receta_ids),
                },
                "solicitud_by_alcance": {
                    "MES": _to_float(by_alcance[SolicitudVenta.ALCANCE_MES]),
                    "SEMANA": _to_float(by_alcance[SolicitudVenta.ALCANCE_SEMANA]),
                    "FIN_SEMANA": _to_float(by_alcance[SolicitudVenta.ALCANCE_FIN_SEMANA]),
                },
                "latest_updates": {
                    "historial": latest_historial,
                    "pronostico": latest_pronostico,
                    "solicitud": latest_solicitud,
                },
                "rows": rows,
            },
            status=status.HTTP_200_OK,
        )


class SolicitudVentaListView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar solicitudes de venta."},
                status=status.HTTP_403_FORBIDDEN,
            )

        q = (request.GET.get("q") or "").strip()
        periodo = (request.GET.get("periodo") or "").strip()
        alcance = (request.GET.get("alcance") or "").strip().upper()
        sucursal_id_raw = (request.GET.get("sucursal_id") or "").strip()
        receta_id_raw = (request.GET.get("receta_id") or "").strip()
        fecha_desde_raw = (request.GET.get("fecha_desde") or "").strip()
        fecha_hasta_raw = (request.GET.get("fecha_hasta") or "").strip()
        limit = _parse_bounded_int(request.GET.get("limit", 150), default=150, min_value=1, max_value=1000)

        if periodo:
            parsed_period = _parse_period(periodo)
            if not parsed_period:
                return Response(
                    {"detail": "periodo inválido. Usa formato YYYY-MM."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            periodo = f"{parsed_period[0]:04d}-{parsed_period[1]:02d}"

        allowed_alcance = {
            SolicitudVenta.ALCANCE_MES,
            SolicitudVenta.ALCANCE_SEMANA,
            SolicitudVenta.ALCANCE_FIN_SEMANA,
        }
        if alcance and alcance not in allowed_alcance:
            return Response(
                {"detail": "alcance inválido. Usa MES, SEMANA o FIN_SEMANA."},
                status=status.HTTP_400_BAD_REQUEST,
            )

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

        qs = SolicitudVenta.objects.select_related("receta", "sucursal").order_by("-fecha_inicio", "receta__nombre")
        if periodo:
            qs = qs.filter(periodo=periodo)
        if alcance:
            qs = qs.filter(alcance=alcance)
        if fecha_desde:
            qs = qs.filter(fecha_inicio__gte=fecha_desde)
        if fecha_hasta:
            qs = qs.filter(fecha_fin__lte=fecha_hasta)

        if sucursal_id_raw:
            if not sucursal_id_raw.isdigit():
                return Response(
                    {"detail": "sucursal_id debe ser numérico."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            qs = qs.filter(sucursal_id=int(sucursal_id_raw))

        if receta_id_raw:
            if not receta_id_raw.isdigit():
                return Response(
                    {"detail": "receta_id debe ser numérico."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            qs = qs.filter(receta_id=int(receta_id_raw))

        if q:
            qs = qs.filter(
                Q(receta__nombre__icontains=q)
                | Q(receta__codigo_point__icontains=q)
                | Q(sucursal__nombre__icontains=q)
                | Q(sucursal__codigo__icontains=q)
                | Q(fuente__icontains=q)
            )

        rows = list(qs[:limit])
        items = []
        cantidad_total = Decimal("0")
        by_alcance = {k: 0 for k in allowed_alcance}
        for r in rows:
            qty = _to_decimal(r.cantidad)
            cantidad_total += qty
            by_alcance[r.alcance] = by_alcance.get(r.alcance, 0) + 1
            items.append(
                {
                    "id": r.id,
                    "receta_id": r.receta_id,
                    "receta": r.receta.nombre,
                    "codigo_point": r.receta.codigo_point,
                    "sucursal_id": r.sucursal_id,
                    "sucursal": r.sucursal.nombre if r.sucursal_id and r.sucursal else "",
                    "sucursal_codigo": r.sucursal.codigo if r.sucursal_id and r.sucursal else "",
                    "alcance": r.alcance,
                    "periodo": r.periodo,
                    "fecha_inicio": str(r.fecha_inicio),
                    "fecha_fin": str(r.fecha_fin),
                    "cantidad": str(qty),
                    "fuente": r.fuente,
                    "actualizado_en": r.actualizado_en,
                }
            )

        return Response(
            {
                "filters": {
                    "q": q,
                    "periodo": periodo,
                    "alcance": alcance,
                    "sucursal_id": sucursal_id_raw,
                    "receta_id": receta_id_raw,
                    "fecha_desde": str(fecha_desde) if fecha_desde else "",
                    "fecha_hasta": str(fecha_hasta) if fecha_hasta else "",
                    "limit": limit,
                },
                "totales": {
                    "rows": len(items),
                    "cantidad_total": str(cantidad_total),
                    "by_alcance": by_alcance,
                },
                "items": items,
            },
            status=status.HTTP_200_OK,
        )


def _process_pronostico_venta_bulk(
    data: dict[str, Any],
    *,
    dry_run_override: bool | None = None,
) -> dict[str, Any]:
    rows = data["rows"]
    modo = data.get("modo") or "replace"
    fuente = (data.get("fuente") or "API_PRON_BULK").strip()[:40] or "API_PRON_BULK"
    dry_run = bool(data.get("dry_run", True) if dry_run_override is None else dry_run_override)
    stop_on_error = bool(data.get("stop_on_error", False))
    top = int(data.get("top") or 120)

    receta_cache: dict[tuple[int, str, str], Receta | None] = {}
    created = 0
    updated = 0
    skipped = 0
    terminated_early = False
    result_rows: list[dict] = []

    tx_cm = nullcontext() if dry_run else transaction.atomic()
    with tx_cm:
        for index, row in enumerate(rows, start=1):
            receta_id = row.get("receta_id")
            receta_name = str(row.get("receta") or "").strip()
            codigo_point = str(row.get("codigo_point") or "").strip()
            receta = _resolve_receta_bulk_ref(
                receta_id=receta_id,
                receta_name=receta_name,
                codigo_point=codigo_point,
                cache=receta_cache,
            )
            if receta is None:
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "receta_not_found",
                        "receta_id": int(receta_id or 0) or None,
                        "receta_input": receta_name or codigo_point,
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            periodo = _normalize_periodo_mes(row.get("periodo"))
            cantidad = _to_decimal(row.get("cantidad"), default=Decimal("0"))
            existing = PronosticoVenta.objects.filter(receta=receta, periodo=periodo).first()

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
                    PronosticoVenta.objects.create(
                        receta=receta,
                        periodo=periodo,
                        cantidad=new_qty,
                        fuente=fuente,
                    )

            result_rows.append(
                {
                    "row": index,
                    "status": action,
                    "receta_id": receta.id,
                    "receta": receta.nombre,
                    "periodo": periodo,
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


class PronosticoVentaImportPreviewView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_pronosticoventa"):
            return Response(
                {"detail": "No tienes permisos para previsualizar pronósticos de venta."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = PronosticoVentaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        payload = _process_pronostico_venta_bulk(ser.validated_data, dry_run_override=True)
        payload["preview"] = True
        return Response(payload, status=status.HTTP_200_OK)


class PronosticoVentaImportConfirmView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_pronosticoventa"):
            return Response(
                {"detail": "No tienes permisos para confirmar importación de pronósticos de venta."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = PronosticoVentaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        payload = _process_pronostico_venta_bulk(ser.validated_data, dry_run_override=False)
        payload["preview"] = False
        return Response(payload, status=status.HTTP_200_OK)


class PronosticoVentaBulkUpsertView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_pronosticoventa"):
            return Response(
                {"detail": "No tienes permisos para importar pronósticos de venta."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = PronosticoVentaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        payload = _process_pronostico_venta_bulk(ser.validated_data)
        return Response(payload, status=status.HTTP_200_OK)


def _process_venta_historica_bulk(
    data: dict[str, Any],
    *,
    dry_run_override: bool | None = None,
) -> dict[str, Any]:
    rows = data["rows"]
    modo = data.get("modo") or "replace"
    fuente = (data.get("fuente") or "API_VENTAS_BULK").strip()[:40] or "API_VENTAS_BULK"
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
    result_rows: list[dict] = []

    tx_cm = nullcontext() if dry_run else transaction.atomic()
    with tx_cm:
        for index, row in enumerate(rows, start=1):
            receta_id = row.get("receta_id")
            receta_name = str(row.get("receta") or "").strip()
            codigo_point = str(row.get("codigo_point") or "").strip()
            receta = _resolve_receta_bulk_ref(
                receta_id=receta_id,
                receta_name=receta_name,
                codigo_point=codigo_point,
                cache=receta_cache,
            )
            if receta is None:
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "receta_not_found",
                        "receta_id": int(receta_id or 0) or None,
                        "receta_input": receta_name or codigo_point,
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            sucursal_id = row.get("sucursal_id")
            sucursal_name = str(row.get("sucursal") or "").strip()
            sucursal_codigo = str(row.get("sucursal_codigo") or "").strip()
            has_sucursal_ref = bool(sucursal_id) or bool(sucursal_name) or bool(sucursal_codigo)
            sucursal = _resolve_sucursal_bulk_ref(
                sucursal_id=sucursal_id,
                sucursal_name=sucursal_name,
                sucursal_codigo=sucursal_codigo,
                default_sucursal=default_sucursal,
                cache=sucursal_cache,
            )
            if has_sucursal_ref and sucursal is None:
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "sucursal_not_found",
                        "receta_id": receta.id,
                        "receta": receta.nombre,
                        "sucursal_input": sucursal_codigo or sucursal_name or str(sucursal_id),
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            fecha = row["fecha"]
            cantidad = _to_decimal(row.get("cantidad"), default=Decimal("0"))
            tickets = int(row.get("tickets") or 0)
            monto_total_raw = row.get("monto_total", None)
            monto_total = _to_decimal(monto_total_raw) if monto_total_raw is not None else None
            if monto_total is not None and monto_total <= 0:
                monto_total = None

            existing_qs = VentaHistorica.objects.filter(receta=receta, fecha=fecha)
            if sucursal:
                existing_qs = existing_qs.filter(sucursal=sucursal)
            else:
                existing_qs = existing_qs.filter(sucursal__isnull=True)
            existing = existing_qs.order_by("id").first()

            previous_qty = _to_decimal(existing.cantidad, default=Decimal("0")) if existing else Decimal("0")
            previous_tickets = int(existing.tickets or 0) if existing else 0
            if existing:
                if modo == "accumulate":
                    new_qty = previous_qty + cantidad
                    new_tickets = previous_tickets + tickets
                else:
                    new_qty = cantidad
                    new_tickets = tickets
                action = "UPDATED"
                updated += 1
            else:
                new_qty = cantidad
                new_tickets = tickets
                action = "CREATED"
                created += 1

            if not dry_run:
                if existing:
                    existing.cantidad = new_qty
                    existing.tickets = new_tickets
                    if modo == "accumulate":
                        if monto_total is not None:
                            existing.monto_total = _to_decimal(existing.monto_total, default=Decimal("0")) + monto_total
                    else:
                        existing.monto_total = monto_total
                    existing.fuente = fuente
                    existing.save(update_fields=["cantidad", "tickets", "monto_total", "fuente", "actualizado_en"])
                else:
                    VentaHistorica.objects.create(
                        receta=receta,
                        sucursal=sucursal,
                        fecha=fecha,
                        cantidad=new_qty,
                        tickets=new_tickets,
                        monto_total=monto_total,
                        fuente=fuente,
                    )

            result_rows.append(
                {
                    "row": index,
                    "status": action,
                    "receta_id": receta.id,
                    "receta": receta.nombre,
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


class VentaHistoricaImportPreviewView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para previsualizar historial de ventas."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = VentaHistoricaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_venta_historica_bulk(ser.validated_data, dry_run_override=True)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        payload["preview"] = True
        return Response(payload, status=status.HTTP_200_OK)


class VentaHistoricaImportConfirmView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para confirmar importación de historial de ventas."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = VentaHistoricaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_venta_historica_bulk(ser.validated_data, dry_run_override=False)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        payload["preview"] = False
        return Response(payload, status=status.HTTP_200_OK)


class VentaHistoricaBulkUpsertView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para importar historial de ventas."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = VentaHistoricaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_venta_historica_bulk(ser.validated_data)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(payload, status=status.HTTP_200_OK)


def _process_solicitud_venta_bulk(
    data: dict[str, Any],
    *,
    dry_run_override: bool | None = None,
) -> dict[str, Any]:
    rows = data["rows"]
    modo = data.get("modo") or "replace"
    fuente = (data.get("fuente") or "API_SOL_BULK").strip()[:40] or "API_SOL_BULK"
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
    result_rows: list[dict] = []

    tx_cm = nullcontext() if dry_run else transaction.atomic()
    with tx_cm:
        for index, row in enumerate(rows, start=1):
            receta_id = row.get("receta_id")
            receta_name = str(row.get("receta") or "").strip()
            codigo_point = str(row.get("codigo_point") or "").strip()
            receta = _resolve_receta_bulk_ref(
                receta_id=receta_id,
                receta_name=receta_name,
                codigo_point=codigo_point,
                cache=receta_cache,
            )
            if receta is None:
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "receta_not_found",
                        "receta_id": int(receta_id or 0) or None,
                        "receta_input": receta_name or codigo_point,
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            sucursal_id = row.get("sucursal_id")
            sucursal_name = str(row.get("sucursal") or "").strip()
            sucursal_codigo = str(row.get("sucursal_codigo") or "").strip()
            has_sucursal_ref = bool(sucursal_id) or bool(sucursal_name) or bool(sucursal_codigo)
            sucursal = _resolve_sucursal_bulk_ref(
                sucursal_id=sucursal_id,
                sucursal_name=sucursal_name,
                sucursal_codigo=sucursal_codigo,
                default_sucursal=default_sucursal,
                cache=sucursal_cache,
            )
            if has_sucursal_ref and sucursal is None:
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "sucursal_not_found",
                        "receta_id": receta.id,
                        "receta": receta.nombre,
                        "sucursal_input": sucursal_codigo or sucursal_name or str(sucursal_id),
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            alcance_ui = str(row.get("alcance") or "mes").strip().lower()
            alcance_model = _ui_to_model_alcance(alcance_ui)
            periodo_default = _normalize_periodo_mes(row.get("periodo"))
            fecha_base_default = row.get("fecha_base") or timezone.localdate()
            periodo, fecha_inicio, fecha_fin = _resolve_solicitud_window(
                alcance=alcance_model,
                periodo_raw=row.get("periodo"),
                fecha_base_raw=row.get("fecha_base"),
                fecha_inicio_raw=row.get("fecha_inicio"),
                fecha_fin_raw=row.get("fecha_fin"),
                periodo_default=periodo_default,
                fecha_base_default=fecha_base_default,
            )
            cantidad = _to_decimal(row.get("cantidad"), default=Decimal("0"))
            if cantidad <= 0:
                skipped += 1
                result_rows.append(
                    {
                        "row": index,
                        "status": "ERROR",
                        "reason": "invalid_qty",
                        "receta_id": receta.id,
                        "receta": receta.nombre,
                        "cantidad": float(cantidad),
                    }
                )
                if stop_on_error:
                    terminated_early = True
                    break
                continue

            existing = SolicitudVenta.objects.filter(
                receta=receta,
                sucursal=sucursal,
                alcance=alcance_model,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
            ).first()

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
                    existing.periodo = periodo
                    existing.cantidad = new_qty
                    existing.fuente = fuente
                    existing.save(update_fields=["periodo", "cantidad", "fuente", "actualizado_en"])
                else:
                    SolicitudVenta.objects.create(
                        receta=receta,
                        sucursal=sucursal,
                        alcance=alcance_model,
                        periodo=periodo,
                        fecha_inicio=fecha_inicio,
                        fecha_fin=fecha_fin,
                        cantidad=new_qty,
                        fuente=fuente,
                    )

            result_rows.append(
                {
                    "row": index,
                    "status": action,
                    "receta_id": receta.id,
                    "receta": receta.nombre,
                    "sucursal_id": sucursal.id if sucursal else None,
                    "sucursal": sucursal.nombre if sucursal else "",
                    "alcance": alcance_model,
                    "periodo": periodo,
                    "fecha_inicio": str(fecha_inicio),
                    "fecha_fin": str(fecha_fin),
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


class SolicitudVentaImportPreviewView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para previsualizar solicitudes de venta."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = SolicitudVentaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_solicitud_venta_bulk(ser.validated_data, dry_run_override=True)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        payload["preview"] = True
        return Response(payload, status=status.HTTP_200_OK)


class SolicitudVentaImportConfirmView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para confirmar importación de solicitudes de venta."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = SolicitudVentaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_solicitud_venta_bulk(ser.validated_data, dry_run_override=False)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        payload["preview"] = False
        return Response(payload, status=status.HTTP_200_OK)


class SolicitudVentaBulkUpsertView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para importar solicitudes de ventas."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = SolicitudVentaBulkSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            payload = _process_solicitud_venta_bulk(ser.validated_data)
        except ValueError as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(payload, status=status.HTTP_200_OK)


class ForecastEstadisticoView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.view_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para consultar pronóstico estadístico."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ForecastEstadisticoRequestSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        periodo = _normalize_periodo_mes(data.get("periodo"))
        fecha_base = data.get("fecha_base") or timezone.localdate()
        alcance = data.get("alcance") or "mes"
        incluir_preparaciones = bool(data.get("incluir_preparaciones"))
        safety_pct = _to_decimal(data.get("safety_pct"), default=Decimal("0"))
        top = int(data.get("top") or 120)

        sucursal = None
        sucursal_id = data.get("sucursal_id")
        if sucursal_id is not None:
            sucursal = Sucursal.objects.filter(pk=sucursal_id, activa=True).first()
            if sucursal is None:
                return Response(
                    {"detail": "Sucursal no encontrada o inactiva."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        result = _build_forecast_from_history(
            alcance=alcance,
            periodo=periodo,
            fecha_base=fecha_base,
            sucursal=sucursal,
            incluir_preparaciones=incluir_preparaciones,
            safety_pct=safety_pct,
        )
        payload = _forecast_session_payload(result, top_rows=top)

        compare_payload = None
        if bool(data.get("include_solicitud_compare", True)):
            full_payload = _forecast_session_payload(result, top_rows=max(len(result.get("rows") or []), 1))
            compare_payload = _serialize_forecast_compare(
                _forecast_vs_solicitud_preview(full_payload),
                top=top,
            )

        return Response(
            {
                "scope": {
                    "alcance": payload["alcance"],
                    "periodo": payload["periodo"],
                    "target_start": payload["target_start"],
                    "target_end": payload["target_end"],
                    "sucursal_nombre": payload["sucursal_nombre"],
                    "sucursal_id": payload.get("sucursal_id"),
                },
                "totals": payload["totals"],
                "rows": payload["rows"],
                "compare_solicitud": compare_payload,
            },
            status=status.HTTP_200_OK,
        )


class ForecastEstadisticoGuardarView(APIView):
    permission_classes = [IsAuthenticated]

    _ESCENARIO_TO_KEY = {
        "base": "forecast_qty",
        "bajo": "forecast_low",
        "alto": "forecast_high",
    }

    def post(self, request):
        if not request.user.has_perm("recetas.change_pronosticoventa"):
            return Response(
                {"detail": "No tienes permisos para guardar pronóstico estadístico."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = ForecastEstadisticoGuardarSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        periodo = _normalize_periodo_mes(data.get("periodo"))
        fecha_base = data.get("fecha_base") or timezone.localdate()
        alcance = data.get("alcance") or "mes"
        incluir_preparaciones = bool(data.get("incluir_preparaciones"))
        safety_pct = _to_decimal(data.get("safety_pct"), default=Decimal("0"))
        top = int(data.get("top") or 120)
        escenario = str(data.get("escenario") or "base").lower()
        qty_key = self._ESCENARIO_TO_KEY.get(escenario, "forecast_qty")
        replace_existing = bool(data.get("replace_existing", True))
        fuente = (data.get("fuente") or "API_FORECAST_STAT").strip()[:40] or "API_FORECAST_STAT"

        sucursal = None
        sucursal_id = data.get("sucursal_id")
        if sucursal_id is not None:
            sucursal = Sucursal.objects.filter(pk=sucursal_id, activa=True).first()
            if sucursal is None:
                return Response(
                    {"detail": "Sucursal no encontrada o inactiva."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        result = _build_forecast_from_history(
            alcance=alcance,
            periodo=periodo,
            fecha_base=fecha_base,
            sucursal=sucursal,
            incluir_preparaciones=incluir_preparaciones,
            safety_pct=safety_pct,
        )
        if not result.get("rows"):
            return Response(
                {"detail": "No hay historial suficiente para generar forecast en ese alcance/filtro."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = _forecast_session_payload(result, top_rows=top)
        recetas_map = {r.id: r for r in Receta.objects.filter(id__in=[int(x["receta_id"]) for x in result["rows"]]).only("id", "nombre")}

        created = 0
        updated = 0
        skipped_existing = 0
        skipped_invalid = 0
        applied_rows: list[dict[str, Any]] = []

        with transaction.atomic():
            for row in result["rows"]:
                receta_id = int(row["receta_id"])
                receta = recetas_map.get(receta_id)
                if receta is None:
                    skipped_invalid += 1
                    continue

                qty = _to_decimal(row.get(qty_key))
                if qty <= 0:
                    skipped_invalid += 1
                    continue
                qty = qty.quantize(Decimal("0.001"))

                existing = PronosticoVenta.objects.filter(receta=receta, periodo=result["periodo"]).first()
                old_qty = _to_decimal(existing.cantidad if existing else 0).quantize(Decimal("0.001"))
                if existing is None:
                    PronosticoVenta.objects.create(
                        receta=receta,
                        periodo=result["periodo"],
                        cantidad=qty,
                        fuente=fuente,
                    )
                    created += 1
                    action = "create"
                else:
                    if not replace_existing:
                        skipped_existing += 1
                        continue
                    existing.cantidad = qty
                    existing.fuente = fuente
                    existing.save(update_fields=["cantidad", "fuente", "actualizado_en"])
                    updated += 1
                    action = "update"

                if len(applied_rows) < top:
                    applied_rows.append(
                        {
                            "receta_id": receta.id,
                            "receta": receta.nombre,
                            "escenario": escenario,
                            "cantidad_anterior": _to_float(old_qty),
                            "cantidad_nueva": _to_float(qty),
                            "accion": action,
                        }
                    )

        return Response(
            {
                "scope": {
                    "alcance": payload["alcance"],
                    "periodo": payload["periodo"],
                    "target_start": payload["target_start"],
                    "target_end": payload["target_end"],
                    "sucursal_nombre": payload["sucursal_nombre"],
                    "sucursal_id": payload.get("sucursal_id"),
                    "escenario": escenario,
                    "qty_key": qty_key,
                },
                "totals": payload["totals"],
                "persisted": {
                    "created": created,
                    "updated": updated,
                    "skipped_existing": skipped_existing,
                    "skipped_invalid": skipped_invalid,
                    "applied": created + updated,
                },
                "rows": payload["rows"],
                "applied_rows": applied_rows,
            },
            status=status.HTTP_200_OK,
        )


class SolicitudVentaUpsertView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para capturar solicitudes de ventas."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = SolicitudVentaUpsertSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        receta = get_object_or_404(Receta, pk=data["receta_id"])
        sucursal = None
        if data.get("sucursal_id") is not None:
            sucursal = Sucursal.objects.filter(pk=data["sucursal_id"], activa=True).first()
            if sucursal is None:
                return Response(
                    {"detail": "Sucursal no encontrada o inactiva."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        alcance = _ui_to_model_alcance(data.get("alcance"))
        periodo_default = _normalize_periodo_mes(data.get("periodo"))
        fecha_base_default = data.get("fecha_base") or timezone.localdate()
        periodo, fecha_inicio, fecha_fin = _resolve_solicitud_window(
            alcance=alcance,
            periodo_raw=data.get("periodo"),
            fecha_base_raw=data.get("fecha_base"),
            fecha_inicio_raw=data.get("fecha_inicio"),
            fecha_fin_raw=data.get("fecha_fin"),
            periodo_default=periodo_default,
            fecha_base_default=fecha_base_default,
        )
        cantidad = _to_decimal(data.get("cantidad"))
        fuente = (data.get("fuente") or "API_SOL_VENTAS").strip()[:40] or "API_SOL_VENTAS"

        with transaction.atomic():
            record, created = SolicitudVenta.objects.get_or_create(
                receta=receta,
                sucursal=sucursal,
                alcance=alcance,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                defaults={
                    "periodo": periodo,
                    "cantidad": cantidad,
                    "fuente": fuente,
                },
            )
            if not created:
                record.periodo = periodo
                record.cantidad = cantidad
                record.fuente = fuente
                record.save(update_fields=["periodo", "cantidad", "fuente", "actualizado_en"])

        return Response(
            {
                "created": created,
                "id": record.id,
                "receta_id": record.receta_id,
                "receta": record.receta.nombre,
                "sucursal_id": record.sucursal_id,
                "sucursal": record.sucursal.nombre if record.sucursal_id else "",
                "alcance": record.alcance,
                "periodo": record.periodo,
                "fecha_inicio": str(record.fecha_inicio),
                "fecha_fin": str(record.fecha_fin),
                "cantidad": str(record.cantidad),
                "fuente": record.fuente,
            },
            status=status.HTTP_201_CREATED if created else status.HTTP_200_OK,
        )


class SolicitudVentaAplicarForecastView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        if not request.user.has_perm("recetas.change_planproduccion"):
            return Response(
                {"detail": "No tienes permisos para ajustar solicitudes de ventas."},
                status=status.HTTP_403_FORBIDDEN,
            )

        ser = SolicitudVentaAplicarForecastSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        sucursal = Sucursal.objects.filter(pk=data["sucursal_id"], activa=True).first()
        if sucursal is None:
            return Response(
                {"detail": "Sucursal no encontrada o inactiva."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        alcance = data.get("alcance") or "mes"
        periodo = _normalize_periodo_mes(data.get("periodo"))
        fecha_base = data.get("fecha_base") or timezone.localdate()
        incluir_preparaciones = bool(data.get("incluir_preparaciones"))
        safety_pct = _to_decimal(data.get("safety_pct"), default=Decimal("0"))
        dry_run = bool(data.get("dry_run", False))
        top = int(data.get("top") or 120)

        result = _build_forecast_from_history(
            alcance=alcance,
            periodo=periodo,
            fecha_base=fecha_base,
            sucursal=sucursal,
            incluir_preparaciones=incluir_preparaciones,
            safety_pct=safety_pct,
        )
        if not result.get("rows"):
            return Response(
                {"detail": "No hay historial suficiente para generar forecast en ese alcance/filtro."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        full_payload = _forecast_session_payload(result, top_rows=max(len(result.get("rows") or []), 1))
        compare_raw = _forecast_vs_solicitud_preview(full_payload)
        if not compare_raw or not compare_raw.get("rows"):
            return Response(
                {"detail": "No hay filas de comparación forecast vs solicitud para aplicar ajuste."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        modo = data.get("modo") or "desviadas"
        receta_id = data.get("receta_id")
        rows = list(compare_raw["rows"])
        if modo == "sobre":
            rows = [r for r in rows if r.get("status") == "SOBRE"]
        elif modo == "bajo":
            rows = [r for r in rows if r.get("status") == "BAJO"]
        elif modo == "todas":
            rows = [r for r in rows if r.get("status") in {"SOBRE", "BAJO", "SIN_BASE", "OK"}]
        elif modo == "receta":
            rows = [r for r in rows if int(r.get("receta_id") or 0) == int(receta_id)]
        else:
            rows = [r for r in rows if r.get("status") in {"SOBRE", "BAJO"}]

        if not rows:
            return Response(
                {"detail": "No hay filas objetivo para el modo seleccionado."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        model_alcance = _ui_to_model_alcance(alcance)
        target_start = result["target_start"]
        target_end = result["target_end"]
        result_periodo = result["periodo"]
        fuente = (data.get("fuente") or "API_FORECAST_ADJUST").strip()[:40] or "API_FORECAST_ADJUST"

        created = 0
        updated = 0
        skipped = 0
        applied = 0
        adjusted_rows = []
        tx_cm = nullcontext() if dry_run else transaction.atomic()
        with tx_cm:
            for row in rows:
                receta = Receta.objects.filter(pk=row["receta_id"]).first()
                if receta is None:
                    skipped += 1
                    continue
                nueva_cantidad = _to_decimal(row.get("forecast_qty"))
                if nueva_cantidad < 0:
                    skipped += 1
                    continue
                record = SolicitudVenta.objects.filter(
                    receta=receta,
                    sucursal=sucursal,
                    alcance=model_alcance,
                    fecha_inicio=target_start,
                    fecha_fin=target_end,
                ).first()
                was_created = record is None
                old_qty = _to_decimal(record.cantidad if record is not None else 0)
                if was_created:
                    if dry_run:
                        created += 1
                    else:
                        SolicitudVenta.objects.create(
                            receta=receta,
                            sucursal=sucursal,
                            alcance=model_alcance,
                            periodo=result_periodo,
                            fecha_inicio=target_start,
                            fecha_fin=target_end,
                            cantidad=nueva_cantidad,
                            fuente=fuente,
                        )
                        created += 1
                        applied += 1
                else:
                    if dry_run:
                        updated += 1
                    else:
                        record.periodo = result_periodo
                        record.cantidad = nueva_cantidad
                        record.fuente = fuente
                        record.save(update_fields=["periodo", "cantidad", "fuente", "actualizado_en"])
                        updated += 1
                        applied += 1

                adjusted_rows.append(
                    {
                        "receta_id": receta.id,
                        "receta": receta.nombre,
                        "anterior": _to_float(old_qty),
                        "nueva": _to_float(nueva_cantidad),
                        "accion": "create" if was_created else "update",
                        "status_before": row.get("status") or "",
                    }
                )

        compare_payload = _serialize_forecast_compare(compare_raw, top=top)
        return Response(
            {
                "scope": {
                    "alcance": alcance,
                    "periodo": result_periodo,
                    "target_start": str(target_start),
                    "target_end": str(target_end),
                    "sucursal_id": sucursal.id,
                    "sucursal_nombre": f"{sucursal.codigo} - {sucursal.nombre}",
                    "modo": modo,
                },
                "updated": {
                    "dry_run": dry_run,
                    "created": created,
                    "updated": updated,
                    "skipped": skipped,
                    "applied": applied,
                },
                "adjusted_rows": adjusted_rows[:top],
                "compare_solicitud": compare_payload,
            },
            status=status.HTTP_200_OK,
        )
