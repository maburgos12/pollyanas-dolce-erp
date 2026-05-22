from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.shortcuts import render
from rest_framework import generics, permissions, status
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.tokens import RefreshToken

from core.access import can_view_module
from activos.models import Activo, OrdenMantenimiento
from logistica.models import ReparacionUnidad, ServicioRealizadoUnidad, TipoServicioUnidad, Unidad

from .serializers import (
    ActivoListSerializer,
    OrdenMantenimientoCreateSerializer,
    OrdenMantenimientoDetailSerializer,
    OrdenMantenimientoListSerializer,
    OrdenMantenimientoSeguimientoSerializer,
    ReparacionCreateSerializer,
    ReparacionListSerializer,
    ServicioCreateSerializer,
    ServicioListSerializer,
    TipoServicioSerializer,
    UnidadListSerializer,
)

AUTH = [JWTAuthentication, TokenAuthentication, SessionAuthentication]


class EsComprasODG(permissions.BasePermission):
    GRUPOS = {"compras_logistica", "dg", "DG", "mantenimiento", "MANTENIMIENTO"}

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        grupos = set(request.user.groups.values_list("name", flat=True))
        return bool(grupos & self.GRUPOS) or request.user.is_superuser or can_view_module(request.user, "activos")


class ActivoListView(generics.ListAPIView):
    authentication_classes = AUTH
    permission_classes = [EsComprasODG]
    serializer_class = ActivoListSerializer

    def get_queryset(self):
        qs = Activo.objects.select_related("sucursal").filter(activo=True)
        sucursal = self.request.query_params.get("sucursal")
        categoria = self.request.query_params.get("categoria")
        q = self.request.query_params.get("q")
        if sucursal:
            qs = qs.filter(sucursal_id=sucursal)
        if categoria:
            qs = qs.filter(categoria__icontains=categoria)
        if q:
            qs = qs.filter(
                Q(nombre__icontains=q)
                | Q(codigo__icontains=q)
                | Q(categoria__icontains=q)
                | Q(sucursal__nombre__icontains=q)
                | Q(ubicacion__icontains=q)
            )
        return qs.order_by("sucursal__nombre", "nombre", "codigo")


class UnidadListView(generics.ListAPIView):
    authentication_classes = AUTH
    permission_classes = [EsComprasODG]
    serializer_class = UnidadListSerializer
    queryset = Unidad.objects.filter(activa=True).select_related("sucursal").order_by("descripcion", "codigo")


class TipoServicioListView(generics.ListAPIView):
    authentication_classes = AUTH
    permission_classes = [EsComprasODG]
    serializer_class = TipoServicioSerializer
    queryset = TipoServicioUnidad.objects.filter(activo=True).order_by("nombre")


class OrdenMantenimientoListCreateView(generics.ListCreateAPIView):
    authentication_classes = AUTH
    permission_classes = [EsComprasODG]

    def get_serializer_class(self):
        if self.request.method == "POST":
            return OrdenMantenimientoCreateSerializer
        return OrdenMantenimientoListSerializer

    def get_queryset(self):
        qs = OrdenMantenimiento.objects.select_related(
            "activo_ref", "activo_ref__sucursal", "creado_por"
        ).prefetch_related("bitacora").order_by("-id")
        activo = self.request.query_params.get("activo")
        estatus = self.request.query_params.get("estatus")
        sucursal = self.request.query_params.get("sucursal")
        prioridad = self.request.query_params.get("prioridad")
        q = self.request.query_params.get("q")
        if activo:
            qs = qs.filter(activo_ref_id=activo)
        if estatus:
            qs = qs.filter(estatus=estatus)
        if sucursal:
            qs = qs.filter(activo_ref__sucursal_id=sucursal)
        if prioridad:
            qs = qs.filter(prioridad=prioridad)
        if q:
            qs = qs.filter(
                Q(folio__icontains=q)
                | Q(descripcion__icontains=q)
                | Q(responsable__icontains=q)
                | Q(activo_ref__nombre__icontains=q)
                | Q(activo_ref__codigo__icontains=q)
                | Q(activo_ref__sucursal__nombre__icontains=q)
            )
        return qs


class OrdenMantenimientoDetailView(generics.RetrieveAPIView):
    authentication_classes = AUTH
    permission_classes = [EsComprasODG]
    queryset = OrdenMantenimiento.objects.select_related("activo_ref", "activo_ref__sucursal").prefetch_related("bitacora")
    serializer_class = OrdenMantenimientoDetailSerializer

    def patch(self, request, *args, **kwargs):
        orden = self.get_object()
        serializer = OrdenMantenimientoSeguimientoSerializer(
            data=request.data,
            context={"orden": orden, "request": request},
        )
        serializer.is_valid(raise_exception=True)
        orden = serializer.save()
        return Response(OrdenMantenimientoDetailSerializer(orden).data, status=status.HTTP_200_OK)


class ReparacionListCreateView(generics.ListCreateAPIView):
    authentication_classes = AUTH
    permission_classes = [EsComprasODG]

    def get_serializer_class(self):
        if self.request.method == "POST":
            return ReparacionCreateSerializer
        return ReparacionListSerializer

    def get_queryset(self):
        qs = ReparacionUnidad.objects.select_related("unidad").order_by("-fecha_ingreso", "-id")
        unidad = self.request.query_params.get("unidad")
        if unidad:
            qs = qs.filter(unidad_id=unidad)
        return qs


class ServicioListCreateView(generics.ListCreateAPIView):
    authentication_classes = AUTH
    permission_classes = [EsComprasODG]

    def get_serializer_class(self):
        if self.request.method == "POST":
            return ServicioCreateSerializer
        return ServicioListSerializer

    def get_queryset(self):
        qs = ServicioRealizadoUnidad.objects.select_related("unidad", "tipo_servicio").order_by("-fecha_servicio", "-id")
        unidad = self.request.query_params.get("unidad")
        if unidad:
            qs = qs.filter(unidad_id=unidad)
        return qs


@api_view(["GET"])
@authentication_classes(AUTH)
@permission_classes([EsComprasODG])
def mi_perfil(request):
    user = request.user
    return Response(
        {
            "id": user.id,
            "nombre": user.get_full_name() or user.username,
            "username": user.username,
            "grupos": list(user.groups.values_list("name", flat=True)),
        }
    )


@api_view(["GET"])
@authentication_classes([SessionAuthentication])
@permission_classes([EsComprasODG])
def session_token(request):
    refresh = RefreshToken.for_user(request.user)
    return Response({"access": str(refresh.access_token), "refresh": str(refresh)})


@login_required
def pwa_mantenimiento(request):
    if not EsComprasODG().has_permission(request, None):
        raise PermissionDenied("No tienes permisos para usar Mantenimiento")
    return render(request, "mantenimiento/pwa.html")
