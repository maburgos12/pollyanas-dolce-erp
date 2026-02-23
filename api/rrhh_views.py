from __future__ import annotations

from decimal import Decimal

from django.db import transaction
from django.db.models import Q, Sum
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.access import can_manage_rrhh, can_view_rrhh
from core.audit import log_event
from rrhh.models import Empleado, NominaLinea, NominaPeriodo

from .rrhh_serializers import (
    RRHHEmpleadoSerializer,
    RRHHNominaLineaSerializer,
    RRHHNominaLineaUpsertSerializer,
    RRHHNominaPeriodoSerializer,
)


class _RRHHBaseView(APIView):
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _bounded_int(value, *, default: int, min_value: int, max_value: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(min_value, min(parsed, max_value))


class RRHHEmpleadosView(_RRHHBaseView):
    def get(self, request):
        if not can_view_rrhh(request.user):
            return Response({"detail": "No tienes permisos para consultar RRHH."}, status=status.HTTP_403_FORBIDDEN)

        q = (request.query_params.get("q") or "").strip()
        activo_raw = (request.query_params.get("activo") or "").strip().lower()
        limit = self._bounded_int(request.query_params.get("limit"), default=50, min_value=1, max_value=500)
        offset = self._bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)

        qs = Empleado.objects.all()
        if q:
            qs = qs.filter(
                Q(nombre__icontains=q)
                | Q(codigo__icontains=q)
                | Q(area__icontains=q)
                | Q(puesto__icontains=q)
            )
        if activo_raw in {"1", "true", "yes"}:
            qs = qs.filter(activo=True)
        elif activo_raw in {"0", "false", "no"}:
            qs = qs.filter(activo=False)

        total = qs.count()
        rows = list(qs.order_by("nombre", "id")[offset : offset + limit])
        return Response(
            {
                "count": total,
                "limit": limit,
                "offset": offset,
                "results": RRHHEmpleadoSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not can_manage_rrhh(request.user):
            return Response({"detail": "No tienes permisos para crear empleados."}, status=status.HTTP_403_FORBIDDEN)

        serializer = RRHHEmpleadoSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        empleado = serializer.save()
        log_event(
            request.user,
            "CREATE",
            "rrhh.Empleado",
            str(empleado.id),
            {
                "codigo": empleado.codigo,
                "nombre": empleado.nombre,
            },
        )
        return Response(RRHHEmpleadoSerializer(empleado).data, status=status.HTTP_201_CREATED)


class RRHHNominasView(_RRHHBaseView):
    def get(self, request):
        if not can_view_rrhh(request.user):
            return Response({"detail": "No tienes permisos para consultar RRHH."}, status=status.HTTP_403_FORBIDDEN)

        estatus = (request.query_params.get("estatus") or "").strip().upper()
        tipo = (request.query_params.get("tipo_periodo") or "").strip().upper()
        limit = self._bounded_int(request.query_params.get("limit"), default=50, min_value=1, max_value=500)
        offset = self._bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)

        qs = NominaPeriodo.objects.all()
        if estatus:
            qs = qs.filter(estatus=estatus)
        if tipo:
            qs = qs.filter(tipo_periodo=tipo)

        total = qs.count()
        rows = list(qs.order_by("-fecha_fin", "-id")[offset : offset + limit])
        return Response(
            {
                "count": total,
                "limit": limit,
                "offset": offset,
                "results": RRHHNominaPeriodoSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not can_manage_rrhh(request.user):
            return Response({"detail": "No tienes permisos para crear nómina."}, status=status.HTTP_403_FORBIDDEN)

        serializer = RRHHNominaPeriodoSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        periodo = serializer.save(created_by=request.user)
        log_event(
            request.user,
            "CREATE",
            "rrhh.NominaPeriodo",
            str(periodo.id),
            {
                "folio": periodo.folio,
                "tipo_periodo": periodo.tipo_periodo,
                "fecha_inicio": str(periodo.fecha_inicio),
                "fecha_fin": str(periodo.fecha_fin),
            },
        )
        return Response(RRHHNominaPeriodoSerializer(periodo).data, status=status.HTTP_201_CREATED)


class RRHHNominaLineasView(_RRHHBaseView):
    def get(self, request, nomina_id: int):
        if not can_view_rrhh(request.user):
            return Response({"detail": "No tienes permisos para consultar RRHH."}, status=status.HTTP_403_FORBIDDEN)

        periodo = get_object_or_404(NominaPeriodo, pk=nomina_id)
        rows = list(periodo.lineas.select_related("empleado").order_by("empleado__nombre", "id"))
        return Response(
            {
                "periodo": RRHHNominaPeriodoSerializer(periodo).data,
                "lineas": RRHHNominaLineaSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request, nomina_id: int):
        if not can_manage_rrhh(request.user):
            return Response({"detail": "No tienes permisos para editar nómina."}, status=status.HTTP_403_FORBIDDEN)

        periodo = get_object_or_404(NominaPeriodo, pk=nomina_id)
        serializer = RRHHNominaLineaUpsertSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data

        empleado = get_object_or_404(Empleado, pk=payload["empleado_id"], activo=True)
        with transaction.atomic():
            linea, _ = NominaLinea.objects.get_or_create(periodo=periodo, empleado=empleado)
            linea.dias_trabajados = payload["dias_trabajados"]
            linea.salario_base = payload.get("salario_base") or 0
            linea.bonos = payload.get("bonos") or 0
            linea.descuentos = payload.get("descuentos") or 0
            linea.observaciones = payload.get("observaciones") or ""
            linea.save()
            periodo.recompute_totals()
            periodo.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])

        log_event(
            request.user,
            "UPDATE",
            "rrhh.NominaLinea",
            str(linea.id),
            {
                "folio": periodo.folio,
                "empleado": empleado.nombre,
                "neto": str(linea.neto_calculado),
            },
        )
        return Response(
            {
                "periodo": RRHHNominaPeriodoSerializer(periodo).data,
                "linea": RRHHNominaLineaSerializer(linea).data,
            },
            status=status.HTTP_200_OK,
        )


class RRHHDashboardView(_RRHHBaseView):
    def get(self, request):
        if not can_view_rrhh(request.user):
            return Response({"detail": "No tienes permisos para consultar RRHH."}, status=status.HTTP_403_FORBIDDEN)

        by_status = {
            key: NominaPeriodo.objects.filter(estatus=key).count()
            for key, _ in NominaPeriodo.ESTATUS_CHOICES
        }
        ultimo_periodo = NominaPeriodo.objects.order_by("-fecha_fin", "-id").first()
        return Response(
            {
                "empleados": {
                    "total": Empleado.objects.count(),
                    "activos": Empleado.objects.filter(activo=True).count(),
                },
                "nomina": {
                    "periodos_total": NominaPeriodo.objects.count(),
                    "by_status": by_status,
                    "ultimo_periodo": RRHHNominaPeriodoSerializer(ultimo_periodo).data if ultimo_periodo else None,
                    "monto_neto_total": str(
                        NominaPeriodo.objects.aggregate(total=Sum("total_neto")).get("total") or Decimal("0")
                    ),
                },
            },
            status=status.HTTP_200_OK,
        )
