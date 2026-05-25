from decimal import Decimal

from django.db import transaction
from django.shortcuts import get_object_or_404
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import BasePermission, IsAuthenticated
from rest_framework.response import Response

from core.models import Sucursal
from core.access import can_view_submodule
from rrhh.models import Empleado, NominaPeriodo
from rrhh.bonos_permisos import BasePermisosEquipoViewSet

from .empleados import empleados_elegibles_bonos_ventas
from .models import (
    BonoVentasEmpleado,
    ConfigBonoVentasPeriodo,
    RegistroDiarioVentas,
    VentaCategoriaSucursal,
)
from .serializers import (
    BonoVentasEmpleadoSerializer,
    BonoVentasResumenSerializer,
    ConfigBonoVentasPeriodoSerializer,
    RegistroDiarioVentasSerializer,
    VentaCategoriaSucursalSerializer,
)
from .services import sync_ventas_categorias


class CanAccessBonosVentas(BasePermission):
    def has_permission(self, request, view):
        user = request.user
        return bool(user and user.is_authenticated and can_view_submodule(user, "ventas", "bonos"))


def _recalcular_desde_registros(bono: BonoVentasEmpleado) -> None:
    registros = bono.registros.all()
    bono.dias_trabajados = registros.count()
    bono.dias_asistencia = registros.filter(tiene_asistencia=True).count()
    bono.dias_uniforme = registros.filter(tiene_uniforme=True).count()
    bono.dias_puntualidad = registros.filter(tiene_puntualidad=True).count()
    bono.recalcular()
    bono.save()


class ConfigBonoVentasPeriodoViewSet(viewsets.ModelViewSet):
    queryset = ConfigBonoVentasPeriodo.objects.all()
    serializer_class = ConfigBonoVentasPeriodoSerializer
    permission_classes = [IsAuthenticated, CanAccessBonosVentas]

    def get_queryset(self):
        qs = super().get_queryset()
        mes = self.request.query_params.get("mes")
        anio = self.request.query_params.get("anio")
        if mes:
            qs = qs.filter(mes=mes)
        if anio:
            qs = qs.filter(anio=anio)
        return qs

    def perform_create(self, serializer):
        serializer.save(creado_por=self.request.user if self.request.user.is_authenticated else None)

    @action(detail=True, methods=["post"], url_path="inicializar-bonos")
    def inicializar_bonos(self, request, pk=None):
        periodo = self.get_object()
        empleados = empleados_elegibles_bonos_ventas()
        creados = 0
        sin_sucursal = []
        for empleado in empleados:
            sucursal_nombre = (empleado.sucursal or "").strip()
            if not sucursal_nombre:
                sin_sucursal.append(empleado.nombre)
                continue
            try:
                sucursal_obj = Sucursal.objects.get(nombre__iexact=sucursal_nombre, activa=True)
            except Sucursal.DoesNotExist:
                sin_sucursal.append(f"{empleado.nombre} (sucursal desconocida: {sucursal_nombre!r})")
                continue
            _, created = BonoVentasEmpleado.objects.get_or_create(
                periodo=periodo,
                empleado=empleado,
                defaults={"sucursal": sucursal_obj},
            )
            if created:
                creados += 1
        return Response(
            {
                "creados": creados,
                "total_ventas": empleados.count(),
                "sin_sucursal": sin_sucursal,
            },
            status=status.HTTP_200_OK,
        )

    @action(detail=True, methods=["post"], url_path="aplicar-a-nomina")
    def aplicar_a_nomina(self, request, pk=None):
        periodo = self.get_object()
        nomina_id = request.data.get("nomina_periodo_id")
        if not nomina_id:
            return Response({"detail": "Se requiere nomina_periodo_id."}, status=status.HTTP_400_BAD_REQUEST)
        nomina = get_object_or_404(NominaPeriodo, pk=nomina_id)
        updated = periodo.aplicar_a_nomina(nomina)
        return Response({"actualizados": updated, "nomina_periodo_id": nomina.id}, status=status.HTTP_200_OK)


class VentaCategoriaSucursalViewSet(viewsets.ModelViewSet):
    queryset = VentaCategoriaSucursal.objects.select_related("periodo", "sucursal")
    serializer_class = VentaCategoriaSucursalSerializer
    permission_classes = [IsAuthenticated, CanAccessBonosVentas]

    def get_queryset(self):
        qs = super().get_queryset()
        periodo_mes = self.request.query_params.get("periodo_mes")
        periodo_anio = self.request.query_params.get("periodo_anio")
        sucursal = self.request.query_params.get("sucursal")
        if periodo_mes:
            qs = qs.filter(periodo__mes=periodo_mes)
        if periodo_anio:
            qs = qs.filter(periodo__anio=periodo_anio)
        if sucursal:
            qs = qs.filter(sucursal_id=sucursal)
        return qs

    @action(detail=False, methods=["post"], url_path="sync-pos-bridge")
    def sync_pos_bridge(self, request):
        periodo_id = request.data.get("periodo")
        if not periodo_id:
            return Response({"detail": "Se requiere periodo."}, status=status.HTTP_400_BAD_REQUEST)
        periodo = get_object_or_404(ConfigBonoVentasPeriodo, pk=periodo_id)
        sucursal_id = request.data.get("sucursal")
        updated = sync_ventas_categorias(periodo, sucursal_id=sucursal_id)
        return Response({"actualizados": updated}, status=status.HTTP_200_OK)


class BonoVentasEmpleadoViewSet(viewsets.ModelViewSet):
    queryset = BonoVentasEmpleado.objects.select_related("empleado", "periodo", "sucursal").prefetch_related("registros")
    serializer_class = BonoVentasEmpleadoSerializer
    permission_classes = [IsAuthenticated, CanAccessBonosVentas]

    def get_queryset(self):
        qs = super().get_queryset()
        mes = self.request.query_params.get("mes")
        anio = self.request.query_params.get("anio")
        sucursal = self.request.query_params.get("sucursal")
        if mes:
            qs = qs.filter(periodo__mes=mes)
        if anio:
            qs = qs.filter(periodo__anio=anio)
        if sucursal:
            qs = qs.filter(sucursal_id=sucursal)
        return qs

    def perform_create(self, serializer):
        bono = serializer.save()
        bono.recalcular()
        bono.save()

    def perform_update(self, serializer):
        bono = serializer.save()
        bono.recalcular()
        bono.save()

    @action(detail=False, methods=["get"], url_path="resumen")
    def resumen(self, request):
        mes = request.query_params.get("mes")
        anio = request.query_params.get("anio")
        if not mes or not anio:
            return Response({"detail": "Se requieren mes y anio."}, status=status.HTTP_400_BAD_REQUEST)
        bonos = list(
            BonoVentasEmpleado.objects.filter(periodo__mes=mes, periodo__anio=anio).select_related("empleado", "sucursal", "periodo")
        )
        with transaction.atomic():
            for bono in bonos:
                bono.recalcular()
                bono.save()
        data = BonoVentasResumenSerializer(bonos, many=True).data
        total = sum(Decimal(row["total_a_pagar"]) for row in data)
        return Response({"mes": int(mes), "anio": int(anio), "total_a_pagar": str(total), "bonos": data})

    @action(detail=True, methods=["post"], url_path="recalcular")
    def recalcular(self, request, pk=None):
        bono = self.get_object()
        _recalcular_desde_registros(bono)
        return Response(BonoVentasEmpleadoSerializer(bono).data, status=status.HTTP_200_OK)


class RegistroDiarioVentasViewSet(viewsets.ModelViewSet):
    queryset = RegistroDiarioVentas.objects.select_related("bono__empleado", "bono__periodo")
    serializer_class = RegistroDiarioVentasSerializer
    permission_classes = [IsAuthenticated, CanAccessBonosVentas]

    def get_queryset(self):
        qs = super().get_queryset()
        bono_id = self.request.query_params.get("bono")
        if bono_id:
            qs = qs.filter(bono_id=bono_id)
        return qs

    def perform_create(self, serializer):
        instance = serializer.save(capturado_por=self.request.user if self.request.user.is_authenticated else None)
        _recalcular_desde_registros(instance.bono)

    def perform_update(self, serializer):
        instance = serializer.save()
        _recalcular_desde_registros(instance.bono)


class PermisosVentasEquipoViewSet(BasePermisosEquipoViewSet):
    permission_classes = [IsAuthenticated, CanAccessBonosVentas]
    origen_solicitud = "bonos_ventas"

    def empleados_queryset(self):
        qs = empleados_elegibles_bonos_ventas()
        sucursal_id = self.request.query_params.get("sucursal")
        if sucursal_id:
            try:
                sucursal = Sucursal.objects.get(pk=sucursal_id)
                qs = qs.filter(sucursal__iexact=sucursal.nombre)
            except Sucursal.DoesNotExist:
                return Empleado.objects.none()
        return qs
