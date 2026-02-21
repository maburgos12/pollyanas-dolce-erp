from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated

from compras.models import SolicitudCompra
from compras.models import OrdenCompra
from core.access import can_manage_compras
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
        asegurar_version_costeo(receta, fuente="API_VERSIONES")
        limit = int(request.GET.get("limit", 25))
        limit = max(1, min(limit, 200))
        versiones = list(receta.versiones_costo.order_by("-version_num")[:limit])
        payload = RecetaCostoVersionSerializer(versiones, many=True).data
        return Response(
            {
                "receta_id": receta.id,
                "receta_nombre": receta.nombre,
                "total": len(payload),
                "items": payload,
            },
            status=status.HTTP_200_OK,
        )


class RecetaCostoHistoricoView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, receta_id: int):
        receta = get_object_or_404(Receta, pk=receta_id)
        asegurar_version_costeo(receta, fuente="API_HISTORICO")
        limit = int(request.GET.get("limit", 60))
        limit = max(1, min(limit, 300))
        versiones = list(receta.versiones_costo.order_by("-version_num")[:limit])
        payload = RecetaCostoVersionSerializer(versiones, many=True).data
        comparativo = comparativo_versiones(versiones)
        data = {
            "receta_id": receta.id,
            "receta_nombre": receta.nombre,
            "puntos": payload,
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

        solicitud = SolicitudCompra.objects.create(
            area=area,
            solicitante=solicitante[:120],
            insumo=insumo,
            proveedor_sugerido=insumo.proveedor_principal,
            cantidad=data["cantidad"],
            fecha_requerida=data.get("fecha_requerida") or timezone.localdate(),
            estatus=data.get("estatus") or SolicitudCompra.STATUS_BORRADOR,
        )

        return Response(
            {
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
            },
            status=status.HTTP_201_CREATED,
        )


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
