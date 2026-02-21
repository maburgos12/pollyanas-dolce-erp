from decimal import Decimal
from django.shortcuts import get_object_or_404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated

from inventario.models import ExistenciaInsumo
from recetas.models import PlanProduccion, Receta
from recetas.utils.costeo_versionado import asegurar_version_costeo, comparativo_versiones
from .serializers import (
    MRPRequestSerializer,
    MRPRequerimientosRequestSerializer,
    RecetaCostoVersionSerializer,
)

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
        return Response(data, status=status.HTTP_200_OK)


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
