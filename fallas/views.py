from datetime import timedelta

from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Avg, Count, DurationField, ExpressionWrapper, F
from django.shortcuts import render
from django.utils import timezone
from rest_framework import generics, permissions
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication

from core.models import Sucursal, sucursales_operativas_q

from .models import BitacoraFalla, CategoriaFalla, ReporteFalla
from .serializers import (
    CambioEstatusSerializer,
    CategoriaFallaSerializer,
    ReporteFallaCreateSerializer,
    ReporteFallaDetailSerializer,
    ReporteFallaListSerializer,
    SucursalFallaSerializer,
)


class EsPersonalSucursal(permissions.BasePermission):
    """Cualquier usuario autenticado puede reportar fallas."""

    def has_permission(self, request, view):
        return bool(request.user and request.user.is_authenticated)


class EsComprasODG(permissions.BasePermission):
    """Solo compras_logistica o dg pueden gestionar fallas."""

    GRUPOS = {"compras_logistica", "dg"}

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        grupos = set(request.user.groups.values_list("name", flat=True))
        return bool(grupos & self.GRUPOS) or request.user.is_superuser


def _puede_gestionar_fallas(user) -> bool:
    if not user.is_authenticated:
        return False
    if user.is_staff or user.is_superuser:
        return True
    return user.groups.filter(name__in=["compras_logistica", "dg"]).exists()


class SucursalFallaListView(generics.ListAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    serializer_class = SucursalFallaSerializer
    permission_classes = [EsPersonalSucursal]

    def get_queryset(self):
        return Sucursal.objects.filter(sucursales_operativas_q()).order_by("codigo", "nombre")


class CategoriaFallaListView(generics.ListAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    queryset = CategoriaFalla.objects.filter(activo=True)
    serializer_class = CategoriaFallaSerializer
    permission_classes = [EsPersonalSucursal]


class ReporteFallaListCreateView(generics.ListCreateAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    permission_classes = [EsPersonalSucursal]

    def get_queryset(self):
        qs = ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por")
        estatus = self.request.query_params.get("estatus")
        sucursal = self.request.query_params.get("sucursal")
        prioridad = self.request.query_params.get("prioridad")
        if estatus:
            qs = qs.filter(estatus=estatus)
        if sucursal:
            qs = qs.filter(sucursal_id=sucursal)
        if prioridad:
            qs = qs.filter(prioridad=prioridad)

        user = self.request.user
        grupos = set(user.groups.values_list("name", flat=True))
        if not ({"compras_logistica", "dg", "supervisor_logistica"} & grupos) and not user.is_superuser:
            qs = qs.filter(reportado_por=user)
        return qs

    def get_serializer_class(self):
        if self.request.method == "POST":
            return ReporteFallaCreateSerializer
        return ReporteFallaListSerializer


class ReporteFallaDetailView(generics.RetrieveUpdateAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    queryset = ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por").prefetch_related(
        "bitacora__usuario"
    )
    serializer_class = ReporteFallaDetailSerializer
    permission_classes = [EsPersonalSucursal]

    def get_queryset(self):
        qs = super().get_queryset()
        user = self.request.user
        grupos = set(user.groups.values_list("name", flat=True))
        if not ({"compras_logistica", "dg", "supervisor_logistica"} & grupos) and not user.is_superuser:
            qs = qs.filter(reportado_por=user)
        return qs


@api_view(["POST"])
@authentication_classes([JWTAuthentication, TokenAuthentication, SessionAuthentication])
@permission_classes([EsComprasODG])
def cambiar_estatus(request, pk):
    """Transición de estatus con bitácora automática."""

    try:
        reporte = ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por").get(pk=pk)
    except ReporteFalla.DoesNotExist:
        return Response({"error": "Reporte no encontrado."}, status=404)

    serializer = CambioEstatusSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=400)

    data = serializer.validated_data
    estatus_anterior = reporte.estatus
    nuevo_estatus = data["estatus"]
    now = timezone.now()

    reporte.estatus = nuevo_estatus
    if nuevo_estatus == ReporteFalla.ESTATUS_REVISION and not reporte.fecha_asignacion:
        reporte.fecha_asignacion = now
    if data.get("asignado_a"):
        reporte.asignado_a = get_user_model().objects.get(pk=data["asignado_a"])
        if not reporte.fecha_asignacion:
            reporte.fecha_asignacion = now
    if nuevo_estatus == ReporteFalla.ESTATUS_RESUELTO and not reporte.fecha_resolucion:
        reporte.fecha_resolucion = now
    if nuevo_estatus in (ReporteFalla.ESTATUS_CERRADO, ReporteFalla.ESTATUS_CANCELADO):
        reporte.fecha_cierre = now
        reporte.cerrado_por = request.user
    if data.get("costo_estimado") is not None:
        reporte.costo_estimado = data["costo_estimado"]
    if data.get("costo_real") is not None:
        reporte.costo_real = data["costo_real"]
    if data.get("proveedor_servicio"):
        reporte.proveedor_servicio = data["proveedor_servicio"]
    reporte.save()

    BitacoraFalla.objects.create(
        reporte=reporte,
        usuario=request.user,
        estatus_anterior=estatus_anterior,
        estatus_nuevo=nuevo_estatus,
        comentario=data.get("comentario", ""),
    )

    try:
        from .tasks import notificar_cambio_estatus

        notificar_cambio_estatus.delay(reporte.pk, nuevo_estatus, request.user.pk)
    except Exception:
        pass

    return Response(ReporteFallaDetailSerializer(reporte, context={"request": request}).data)


@api_view(["GET"])
@authentication_classes([JWTAuthentication, TokenAuthentication, SessionAuthentication])
@permission_classes([EsComprasODG])
def dashboard_stats(request):
    """Estadísticas para el dashboard ejecutivo."""

    hoy = timezone.now()
    hace_30 = hoy - timedelta(days=30)
    activos = [ReporteFalla.ESTATUS_ABIERTO, ReporteFalla.ESTATUS_REVISION, ReporteFalla.ESTATUS_PROCESO]
    tiempos = (
        ReporteFalla.objects.filter(fecha_asignacion__isnull=False, fecha_reporte__gte=hace_30)
        .annotate(
            duracion=ExpressionWrapper(F("fecha_asignacion") - F("fecha_reporte"), output_field=DurationField())
        )
        .aggregate(promedio=Avg("duracion"))
    )

    promedio = tiempos["promedio"]
    return Response(
        {
            "total_abiertos": ReporteFalla.objects.filter(estatus__in=activos).count(),
            "criticos_activos": ReporteFalla.objects.filter(
                estatus__in=activos, prioridad=ReporteFalla.PRIORIDAD_CRITICA
            ).count(),
            "resueltos_mes": ReporteFalla.objects.filter(
                estatus=ReporteFalla.ESTATUS_RESUELTO, fecha_resolucion__gte=hace_30
            ).count(),
            "por_sucursal": list(
                ReporteFalla.objects.filter(estatus__in=activos)
                .values("sucursal__nombre")
                .annotate(total=Count("id"))
                .order_by("-total")
            ),
            "por_categoria": list(
                ReporteFalla.objects.filter(estatus__in=activos)
                .values("categoria__nombre")
                .annotate(total=Count("id"))
                .order_by("-total")
            ),
            "tiempo_respuesta_promedio_horas": round(promedio.total_seconds() / 3600, 1) if promedio else None,
        }
    )


@login_required
def dashboard_view(request):
    if not _puede_gestionar_fallas(request.user):
        raise PermissionDenied
    return render(request, "fallas/dashboard.html")


@login_required
def pwa_reporte(request):
    return render(request, "fallas/pwa_reporte.html")


@login_required
def pwa_mis_reportes(request):
    return render(request, "fallas/pwa_mis_reportes.html")
