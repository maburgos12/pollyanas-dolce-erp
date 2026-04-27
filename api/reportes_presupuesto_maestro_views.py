from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation

from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import can_view_reportes
from reportes.models import AreaPresupuesto, LineaPresupuestoMensual, RubroPresupuesto
from reportes.services_presupuesto_maestro import (
    PresupuestoMaestroService,
    ensure_master_budget_areas,
    normalize_area_code,
    normalize_version,
)


def _money_payload(value):
    return None if value is None else str(value)


def _area_payload(area: AreaPresupuesto) -> dict[str, object]:
    return {
        "id": area.id,
        "codigo": area.codigo,
        "nombre": area.nombre,
        "orden": area.orden,
        "activa": area.activa,
    }


def _require_reportes(user):
    return can_view_reportes(user)


class PresupuestoAreasView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not _require_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar presupuesto."}, status=status.HTTP_403_FORBIDDEN)
        ensure_master_budget_areas()
        areas = AreaPresupuesto.objects.filter(activa=True).order_by("orden", "nombre")
        return Response({"areas": [_area_payload(area) for area in areas]})


class PresupuestoRubrosView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not _require_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar presupuesto."}, status=status.HTTP_403_FORBIDDEN)
        ensure_master_budget_areas()
        area_code = normalize_area_code(request.GET.get("area") or "")
        rubros = RubroPresupuesto.objects.select_related("area", "sucursal").filter(activo=True)
        if area_code:
            rubros = rubros.filter(area__codigo=area_code)
        payload = []
        for rubro in rubros.order_by("area__orden", "concepto", "sucursal__codigo"):
            payload.append(
                {
                    "id": rubro.id,
                    "area": _area_payload(rubro.area),
                    "concepto": rubro.concepto,
                    "codigo_cuenta": rubro.codigo_cuenta,
                    "tipo": rubro.tipo,
                    "sucursal": {
                        "id": rubro.sucursal_id,
                        "codigo": rubro.sucursal.codigo if rubro.sucursal_id else "",
                        "nombre": rubro.sucursal.nombre if rubro.sucursal_id else "",
                    },
                    "activo": rubro.activo,
                }
            )
        return Response({"rubros": payload})


class PresupuestoConsolidadoView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        if not _require_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar presupuesto."}, status=status.HTTP_403_FORBIDDEN)
        periodo = request.GET.get("periodo") or ""
        version = normalize_version(request.GET.get("version"))
        area = request.GET.get("area") or None
        try:
            data = PresupuestoMaestroService().build_consolidado(periodo=periodo, version=version, area=area)
        except Exception:
            return Response({"detail": "Periodo inválido. Usa formato YYYY-MM."}, status=status.HTTP_400_BAD_REQUEST)
        return Response(
            {
                "periodo": data["periodo"].isoformat(),
                "version": data["version"],
                "actual_source": data["actual_source"],
                "totales": {key: _money_payload(value) for key, value in data["totales"].items()},
                "areas": [
                    {
                        "id": area_payload["id"],
                        "codigo": area_payload["codigo"],
                        "nombre": area_payload["nombre"],
                        "total_presupuesto": _money_payload(area_payload["total_presupuesto"]),
                        "total_real": _money_payload(area_payload["total_real"]),
                        "total_varianza": _money_payload(area_payload["total_varianza"]),
                        "varianza_pct": _money_payload(area_payload["varianza_pct"]),
                        "rubros": [
                            {
                                **row,
                                "presupuesto": _money_payload(row["presupuesto"]),
                                "real": _money_payload(row["real"]),
                                "varianza": _money_payload(row["varianza"]),
                                "varianza_pct": _money_payload(row["varianza_pct"]),
                            }
                            for row in area_payload["rubros"]
                        ],
                    }
                    for area_payload in data["areas"]
                ],
            }
        )


class PresupuestoRubroLineasView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, rubro_id: int):
        if not _require_reportes(request.user):
            return Response({"detail": "No tienes permisos para editar presupuesto."}, status=status.HTTP_403_FORBIDDEN)
        rubro = get_object_or_404(RubroPresupuesto, pk=rubro_id)
        version = normalize_version(request.data.get("version"))
        year = int(request.data.get("year") or 2026)
        created = 0
        updated = 0
        for month in range(1, 13):
            raw_amount = request.data.get(f"{month:02d}") or request.data.get(str(month)) or 0
            try:
                amount = Decimal(str(raw_amount or "0"))
            except (InvalidOperation, TypeError, ValueError):
                amount = Decimal("0")
            _, was_created = LineaPresupuestoMensual.objects.update_or_create(
                rubro=rubro,
                periodo=date(year, month, 1),
                version=version,
                defaults={"monto_presupuesto": amount, "metadata": {"source": "api"}},
            )
            created += int(was_created)
            updated += int(not was_created)
        return Response({"rubro_id": rubro.id, "created": created, "updated": updated}, status=status.HTTP_200_OK)


class PresupuestoLineaView(APIView):
    permission_classes = [IsAuthenticated]

    def put(self, request, line_id: int):
        if not _require_reportes(request.user):
            return Response({"detail": "No tienes permisos para editar presupuesto."}, status=status.HTTP_403_FORBIDDEN)
        try:
            amount = Decimal(str(request.data.get("monto_presupuesto", "0")))
        except (InvalidOperation, TypeError, ValueError):
            return Response({"detail": "Monto inválido."}, status=status.HTTP_400_BAD_REQUEST)
        line = PresupuestoMaestroService().update_line_amount(line_id=line_id, amount=amount)
        return Response(
            {
                "id": line.id,
                "rubro_id": line.rubro_id,
                "periodo": line.periodo.isoformat(),
                "version": line.version,
                "monto_presupuesto": str(line.monto_presupuesto),
            }
        )
