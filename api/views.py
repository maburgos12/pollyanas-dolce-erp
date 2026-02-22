from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from django.db import transaction, OperationalError, ProgrammingError
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated

from compras.models import SolicitudCompra
from compras.models import OrdenCompra
from core.access import can_manage_compras, can_view_compras
from compras.views import (
    _build_budget_context,
    _build_budget_history,
    _build_category_dashboard,
    _build_consumo_vs_plan_dashboard,
    _build_provider_dashboard,
    _filtered_solicitudes,
    _sanitize_consumo_ref_filter,
)
from inventario.models import ExistenciaInsumo
from maestros.models import CostoInsumo, Insumo
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta
from recetas.utils.costeo_versionado import asegurar_version_costeo, comparativo_versiones
from .serializers import (
    ComprasSolicitudCreateSerializer,
    MRPRequestSerializer,
    MRPRequerimientosRequestSerializer,
    RecetaCostoVersionSerializer,
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


class MRPRequerimientosView(APIView):
    permission_classes = [IsAuthenticated]

    def _aggregate(self, items_payload: list[tuple[Receta, Decimal]]) -> dict:
        insumos = {}
        lineas_sin_match = 0
        lineas_sin_cantidad = 0
        lineas_sin_costo = 0

        for receta, factor in items_payload:
            if factor <= 0:
                continue
            for linea in receta.lineas.select_related("insumo").all():
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
        raw_items = ser.validated_data.get("items") or []

        items_payload: list[tuple[Receta, Decimal]] = []
        source = "manual"
        plan = None

        if plan_id:
            plan = get_object_or_404(PlanProduccion, pk=plan_id)
            source = "plan"
            for item in plan.items.select_related("receta").all():
                items_payload.append((item.receta, Decimal(str(item.cantidad or 0))))
        else:
            for item in raw_items:
                receta = get_object_or_404(Receta, pk=item["receta_id"])
                items_payload.append((receta, Decimal(str(item["cantidad"]))))

        data = self._aggregate(items_payload)
        response = {
            "source": source,
            "plan_id": plan.id if plan else None,
            "plan_nombre": plan.nombre if plan else "",
            "plan_fecha": str(plan.fecha_produccion) if plan else "",
            **data,
        }
        return Response(response, status=status.HTTP_200_OK)
