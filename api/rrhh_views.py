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
from core.branch_catalog import resolver_sucursal_por_texto
from core.models import Sucursal
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
        area = (request.query_params.get("area") or "").strip()
        sucursal = (request.query_params.get("sucursal") or "").strip()
        sucursal_id = (request.query_params.get("sucursal_id") or "").strip()
        sin_sucursal = (request.query_params.get("sin_sucursal") or "").strip()
        limit = self._bounded_int(request.query_params.get("limit"), default=50, min_value=1, max_value=500)
        offset = self._bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)

        if activo_raw in {"0", "false", "no"}:
            qs = Empleado.objects.filter(activo=False)
        elif activo_raw in {"all", "todos"} and can_manage_rrhh(request.user):
            qs = Empleado.objects.all()  # rrhh-allow-inactive-history: endpoint administrativo explicito
        else:
            qs = Empleado.objects.filter(activo=True)
        if area:
            qs = qs.filter(area__iexact=area)
        # Filtrado canónico por FK (FASE 2). `sucursal_id` es la vía preferida; `sucursal`
        # (nombre) se conserva por compat resolviéndolo al id, sin igualdad exacta de texto.
        if sucursal_id.isdigit():
            qs = qs.filter(sucursal_ref_id=int(sucursal_id))
        elif sucursal:
            resuelta = resolver_sucursal_por_texto(sucursal)
            qs = qs.filter(sucursal_ref=resuelta) if resuelta else qs.filter(sucursal__iexact=sucursal)
        if sin_sucursal == "1":
            # Compat: lista general por texto (la usa el tab Producción). El "pendiente"
            # canónico de Ventas vive en el endpoint sin-asignar (por FK).
            qs = qs.filter(Q(sucursal="") | Q(sucursal__isnull=True))
        if q:
            qs = qs.filter(
                Q(nombre__icontains=q)
                | Q(codigo__icontains=q)
                | Q(rfc__icontains=q)
                | Q(curp__icontains=q)
                | Q(nss__icontains=q)
                | Q(area__icontains=q)
                | Q(puesto__icontains=q)
            )
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


class RRHHEmpleadosSinAsignarView(_RRHHBaseView):
    def get(self, request):
        if not can_view_rrhh(request.user):
            return Response({"detail": "No tienes permisos para consultar RRHH."}, status=status.HTTP_403_FORBIDDEN)

        area = (request.query_params.get("area") or "").strip()
        # Pendiente = sin vínculo canónico por id (incluye los que tienen texto pero
        # aún sin FK). Fuente única de "por asignar".
        qs = Empleado.objects.filter(activo=True, sucursal_ref__isnull=True)
        if area:
            qs = qs.filter(area__iexact=area)
        else:
            qs = qs.filter(area__in=["VENTAS", "PRODUCCION"])
        rows = list(qs.order_by("area", "nombre", "id"))
        return Response(
            {
                "count": len(rows),
                "results": RRHHEmpleadoSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )


class RRHHEmpleadoAsignarSucursalView(_RRHHBaseView):
    AREAS_PRODUCCION = {"PRODUCCION", "HORNOS", "EMBETUNADO", "ARMADO", "CRUCERO"}

    def patch(self, request, empleado_id: int):
        if not can_manage_rrhh(request.user):
            return Response({"detail": "No tienes permisos para editar RRHH."}, status=status.HTTP_403_FORBIDDEN)

        empleado = get_object_or_404(Empleado, pk=empleado_id, activo=True)
        sucursal_nombre = (request.data.get("sucursal") or "").strip()
        sucursal_id = request.data.get("sucursal_id")
        area_detalle = (request.data.get("area_detalle") or "").strip().upper()
        update_fields = ["updated_at"]

        # Este módulo es la FUENTE canónica: escribe el FK `sucursal_ref` (id estable)
        # y mantiene el texto `sucursal` sólo como display/legacy. Acepta sucursal_id
        # (preferido) o nombre (resuelto por el resolver canónico, sin match exacto).
        if "sucursal_id" in request.data or "sucursal" in request.data:
            if sucursal_id in (None, "", 0, "0") and not sucursal_nombre:
                sucursal = None
            elif sucursal_id not in (None, "", 0, "0"):
                sucursal = Sucursal.objects.filter(pk=sucursal_id, activa=True).first()
                if not sucursal:
                    return Response(
                        {"error": f"Sucursal id={sucursal_id} no encontrada o inactiva"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            else:
                sucursal = resolver_sucursal_por_texto(sucursal_nombre)
                if not sucursal:
                    return Response(
                        {"error": f"Sucursal '{sucursal_nombre}' no encontrada o inactiva"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            empleado.sucursal_ref = sucursal
            empleado.sucursal = sucursal.nombre if sucursal else ""
            update_fields.extend(["sucursal_ref", "sucursal"])

        if area_detalle:
            if area_detalle not in self.AREAS_PRODUCCION:
                return Response(
                    {"error": f"Área de producción '{area_detalle}' inválida"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            empleado.area = area_detalle
            update_fields.append("area")

        if update_fields == ["updated_at"]:
            return Response({"error": "Se requiere sucursal o area_detalle."}, status=status.HTTP_400_BAD_REQUEST)

        empleado.save(update_fields=update_fields)
        log_event(
            request.user,
            "UPDATE",
            "rrhh.Empleado",
            str(empleado.id),
            {"nombre": empleado.nombre, "sucursal": empleado.sucursal, "area": empleado.area},
        )
        return Response(RRHHEmpleadoSerializer(empleado).data, status=status.HTTP_200_OK)


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
            linea.horas_trabajadas = payload.get("horas_trabajadas") or 0
            linea.horas_dia = payload.get("horas_dia") or 0
            linea.horas_extra = payload.get("horas_extra") or 0
            linea.ausencias = payload.get("ausencias") or 0
            linea.incapacidades = payload.get("incapacidades") or 0
            linea.sdi = payload.get("sdi") or 0
            linea.sbc = payload.get("sbc") or 0
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
