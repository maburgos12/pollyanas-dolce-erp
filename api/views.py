from collections import defaultdict
from contextlib import nullcontext
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from django.db import transaction, OperationalError, ProgrammingError
from django.db.models import Q, Sum
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated

from compras.models import OrdenCompra, RecepcionCompra, SolicitudCompra
from core.access import (
    ROLE_ADMIN,
    ROLE_DG,
    can_manage_compras,
    can_manage_inventario,
    can_view_compras,
    can_view_inventario,
    has_any_role,
)
from core.audit import log_event
from core.models import Sucursal
from compras.views import (
    _apply_recepcion_to_inventario,
    _build_budget_context,
    _build_budget_history,
    _build_category_dashboard,
    _can_transition_orden,
    _can_transition_recepcion,
    _can_transition_solicitud,
    _build_consumo_vs_plan_dashboard,
    _build_provider_dashboard,
    _filtered_solicitudes,
    _sanitize_consumo_ref_filter,
)
from inventario.models import AjusteInventario, ExistenciaInsumo
from inventario.views import _apply_ajuste
from maestros.models import CostoInsumo, Insumo, Proveedor
from recetas.models import (
    LineaReceta,
    PlanProduccion,
    PlanProduccionItem,
    PronosticoVenta,
    Receta,
    SolicitudVenta,
    VentaHistorica,
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
from recetas.utils.costeo_versionado import asegurar_version_costeo, comparativo_versiones
from .serializers import (
    ComprasCrearOrdenSerializer,
    ComprasOrdenStatusSerializer,
    ComprasRecepcionCreateSerializer,
    ComprasRecepcionStatusSerializer,
    ComprasSolicitudCreateSerializer,
    ComprasSolicitudStatusSerializer,
    ForecastBacktestRequestSerializer,
    ForecastEstadisticoRequestSerializer,
    InventarioAjusteCreateSerializer,
    InventarioAjusteDecisionSerializer,
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
                "solicitud_qty": _to_float(row.get("solicitud_qty")),
                "delta_qty": _to_float(row.get("delta_qty")),
                "variacion_pct": _to_float(variacion) if variacion is not None else None,
                "status": row.get("status") or "",
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
        data = ser.validated_data

        rows = data["rows"]
        modo = data.get("modo") or "replace"
        fuente = (data.get("fuente") or "API_PRON_BULK").strip()[:40] or "API_PRON_BULK"
        dry_run = bool(data.get("dry_run", True))
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
        return Response(
            {
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
            },
            status=status.HTTP_200_OK,
        )


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
        data = ser.validated_data

        rows = data["rows"]
        modo = data.get("modo") or "replace"
        fuente = (data.get("fuente") or "API_VENTAS_BULK").strip()[:40] or "API_VENTAS_BULK"
        dry_run = bool(data.get("dry_run", True))
        stop_on_error = bool(data.get("stop_on_error", False))
        top = int(data.get("top") or 120)

        default_sucursal = None
        default_sucursal_id = data.get("sucursal_default_id")
        if default_sucursal_id is not None:
            default_sucursal = Sucursal.objects.filter(pk=default_sucursal_id, activa=True).first()
            if default_sucursal is None:
                return Response(
                    {"detail": "Sucursal default no encontrada o inactiva."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

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
        return Response(
            {
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
            },
            status=status.HTTP_200_OK,
        )


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
        data = ser.validated_data

        rows = data["rows"]
        modo = data.get("modo") or "replace"
        fuente = (data.get("fuente") or "API_SOL_BULK").strip()[:40] or "API_SOL_BULK"
        dry_run = bool(data.get("dry_run", True))
        stop_on_error = bool(data.get("stop_on_error", False))
        top = int(data.get("top") or 120)

        default_sucursal = None
        default_sucursal_id = data.get("sucursal_default_id")
        if default_sucursal_id is not None:
            default_sucursal = Sucursal.objects.filter(pk=default_sucursal_id, activa=True).first()
            if default_sucursal is None:
                return Response(
                    {"detail": "Sucursal default no encontrada o inactiva."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

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
        return Response(
            {
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
            },
            status=status.HTTP_200_OK,
        )


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
        adjusted_rows = []
        with transaction.atomic():
            for row in rows:
                receta = Receta.objects.filter(pk=row["receta_id"]).first()
                if receta is None:
                    skipped += 1
                    continue
                nueva_cantidad = _to_decimal(row.get("forecast_qty"))
                if nueva_cantidad < 0:
                    skipped += 1
                    continue
                record, was_created = SolicitudVenta.objects.get_or_create(
                    receta=receta,
                    sucursal=sucursal,
                    alcance=model_alcance,
                    fecha_inicio=target_start,
                    fecha_fin=target_end,
                    defaults={
                        "periodo": result_periodo,
                        "cantidad": nueva_cantidad,
                        "fuente": fuente,
                    },
                )
                old_qty = _to_decimal(record.cantidad if not was_created else 0)
                if was_created:
                    created += 1
                else:
                    record.periodo = result_periodo
                    record.cantidad = nueva_cantidad
                    record.fuente = fuente
                    record.save(update_fields=["periodo", "cantidad", "fuente", "actualizado_en"])
                    updated += 1

                adjusted_rows.append(
                    {
                        "receta_id": receta.id,
                        "receta": receta.nombre,
                        "anterior": _to_float(old_qty),
                        "nueva": _to_float(nueva_cantidad),
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
                    "created": created,
                    "updated": updated,
                    "skipped": skipped,
                },
                "adjusted_rows": adjusted_rows[:top],
                "compare_solicitud": compare_payload,
            },
            status=status.HTTP_200_OK,
        )
