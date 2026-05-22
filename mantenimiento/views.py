from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from rest_framework import generics
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import BasePermission
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento
from core.access import can_manage_submodule, can_view_submodule
from fallas.models import BitacoraFalla, ReporteFalla
from logistica.models import ReparacionUnidad, ReporteUnidad, ServicioRealizadoUnidad, TipoServicioUnidad, Unidad

from .serializers import (
    ActivoListSerializer,
    OrdenMantenimientoCreateSerializer,
    OrdenMantenimientoListSerializer,
    ReparacionCreateSerializer,
    ReparacionListSerializer,
    ServicioCreateSerializer,
    ServicioListSerializer,
    TipoServicioSerializer,
    UnidadListSerializer,
)

AUTH = [JWTAuthentication, TokenAuthentication, SessionAuthentication]


class EsMantenimiento(BasePermission):
    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        return can_manage_submodule(request.user, "mantenimiento", "bandeja")


def _require_mantenimiento(user):
    if not can_view_submodule(user, "mantenimiento", "dashboard"):
        raise PermissionDenied("No tienes permisos para ver Mantenimiento.")


def _parse_decimal(raw):
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def _branch_statuses():
    return [ReporteFalla.ESTATUS_ABIERTO, ReporteFalla.ESTATUS_REVISION, ReporteFalla.ESTATUS_PROCESO]


def _order_open_statuses():
    return [OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO]


def _unit_open_statuses():
    return [ReporteUnidad.ESTATUS_ABIERTO, ReporteUnidad.ESTATUS_EN_PROCESO, ReporteUnidad.ESTATUS_PROGRAMADO]


def _branch_falla_item(reporte):
    return {
        "uid": f"falla:{reporte.id}",
        "tipo": "falla",
        "origen": "sucursales",
        "id": reporte.id,
        "titulo": reporte.titulo,
        "referencia": f"Falla #{reporte.id}",
        "ubicacion": reporte.sucursal.nombre if reporte.sucursal_id else "",
        "activo": str(reporte.activo_relacionado) if reporte.activo_relacionado_id else "",
        "categoria": reporte.categoria.nombre if reporte.categoria_id else "",
        "prioridad": reporte.get_prioridad_display(),
        "estatus": reporte.estatus,
        "estatus_display": reporte.get_estatus_display(),
        "descripcion": reporte.descripcion,
        "fecha": reporte.fecha_reporte,
        "proveedor": reporte.proveedor_servicio,
        "costo_estimado": reporte.costo_estimado,
        "costo_real": reporte.costo_real,
    }


def _branch_order_item(orden):
    return {
        "uid": f"orden:{orden.id}",
        "tipo": "orden",
        "origen": "sucursales",
        "id": orden.id,
        "titulo": orden.descripcion or orden.get_tipo_display(),
        "referencia": orden.folio,
        "ubicacion": orden.activo_ref.sucursal.nombre if orden.activo_ref_id and orden.activo_ref.sucursal_id else "",
        "activo": str(orden.activo_ref) if orden.activo_ref_id else "",
        "categoria": "Orden de mantenimiento",
        "prioridad": orden.get_prioridad_display(),
        "estatus": orden.estatus,
        "estatus_display": orden.get_estatus_display(),
        "descripcion": orden.descripcion,
        "fecha": orden.creado_en,
        "proveedor": orden.responsable,
        "costo_estimado": None,
        "costo_real": orden.costo_total,
    }


def _logistica_item(reporte):
    return {
        "uid": f"unidad:{reporte.id}",
        "tipo": "unidad",
        "origen": "logistica",
        "id": reporte.id,
        "titulo": reporte.get_tipo_display(),
        "referencia": f"Unidad #{reporte.id}",
        "ubicacion": reporte.unidad.codigo if reporte.unidad_id else "",
        "activo": str(reporte.unidad) if reporte.unidad_id else "",
        "categoria": "Unidad logística",
        "prioridad": reporte.get_severidad_display(),
        "estatus": reporte.estatus,
        "estatus_display": reporte.get_estatus_display(),
        "descripcion": reporte.descripcion,
        "fecha": reporte.fecha_reporte,
        "proveedor": reporte.proveedor_servicio,
        "costo_estimado": reporte.costo_servicio,
        "costo_real": reporte.costo_servicio,
    }


def _unified_items(origen=""):
    items = []
    if origen in ("", "sucursales"):
        fallas = (
            ReporteFalla.objects.filter(estatus__in=_branch_statuses())
            .select_related("sucursal", "categoria", "activo_relacionado")
            .order_by("-fecha_reporte")[:80]
        )
        ordenes = (
            OrdenMantenimiento.objects.filter(estatus__in=_order_open_statuses())
            .select_related("activo_ref", "activo_ref__sucursal")
            .order_by("-creado_en")[:80]
        )
        items.extend(_branch_falla_item(row) for row in fallas)
        items.extend(_branch_order_item(row) for row in ordenes)
    if origen in ("", "logistica"):
        reportes = (
            ReporteUnidad.objects.filter(estatus__in=_unit_open_statuses())
            .select_related("unidad", "repartidor__user")
            .order_by("-fecha_reporte")[:80]
        )
        items.extend(_logistica_item(row) for row in reportes)
    return sorted(items, key=lambda item: item["fecha"], reverse=True)


def _unified_counts():
    items = _unified_items("")
    return {
        "sucursales": sum(1 for item in items if item["origen"] == "sucursales"),
        "logistica": sum(1 for item in items if item["origen"] == "logistica"),
    }


def _update_response(request, data):
    if request.path.startswith("/api/"):
        return Response(data)
    return redirect("mantenimiento:dashboard")


class ActivoListView(generics.ListAPIView):
    authentication_classes = AUTH
    permission_classes = [EsMantenimiento]
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
    permission_classes = [EsMantenimiento]
    serializer_class = UnidadListSerializer
    queryset = Unidad.objects.filter(activa=True).select_related("sucursal").order_by("descripcion", "codigo")


class TipoServicioListView(generics.ListAPIView):
    authentication_classes = AUTH
    permission_classes = [EsMantenimiento]
    serializer_class = TipoServicioSerializer
    queryset = TipoServicioUnidad.objects.filter(activo=True).order_by("nombre")


class OrdenMantenimientoListCreateView(generics.ListCreateAPIView):
    authentication_classes = AUTH
    permission_classes = [EsMantenimiento]

    def get_serializer_class(self):
        if self.request.method == "POST":
            return OrdenMantenimientoCreateSerializer
        return OrdenMantenimientoListSerializer

    def get_queryset(self):
        qs = OrdenMantenimiento.objects.select_related(
            "activo_ref", "activo_ref__sucursal", "creado_por"
        ).order_by("-id")
        activo = self.request.query_params.get("activo")
        estatus = self.request.query_params.get("estatus")
        if activo:
            qs = qs.filter(activo_ref_id=activo)
        if estatus:
            qs = qs.filter(estatus=estatus)
        return qs


class ReparacionListCreateView(generics.ListCreateAPIView):
    authentication_classes = AUTH
    permission_classes = [EsMantenimiento]

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
    permission_classes = [EsMantenimiento]

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
@permission_classes([EsMantenimiento])
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
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def bandeja(request):
    origen = (request.query_params.get("origen") or "").strip().lower()
    if origen not in {"", "sucursales", "logistica"}:
        return Response({"error": "Origen no válido."}, status=400)
    items = _unified_items(origen)
    return Response(
        {
            "origen": origen or "todos",
            "counts": _unified_counts(),
            "items": items,
        }
    )


@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def actualizar_item(request, tipo, pk):
    tipo = (tipo or "").strip().lower()
    comentario = (request.data.get("comentario") or "").strip()
    proveedor = (request.data.get("proveedor_servicio") or "").strip()
    costo_estimado = _parse_decimal(request.data.get("costo_estimado"))
    costo_real = _parse_decimal(request.data.get("costo_real"))

    if tipo == "falla":
        reporte = get_object_or_404(ReporteFalla, pk=pk)
        estatus_anterior = reporte.estatus
        estatus = (request.data.get("estatus") or reporte.estatus).strip()
        if estatus not in {value for value, _label in ReporteFalla.ESTATUS}:
            return Response({"error": "Estatus no válido."}, status=400)
        now = timezone.now()
        reporte.estatus = estatus
        reporte.asignado_a = request.user
        if not reporte.fecha_asignacion:
            reporte.fecha_asignacion = now
        if estatus == ReporteFalla.ESTATUS_RESUELTO and not reporte.fecha_resolucion:
            reporte.fecha_resolucion = now
        if estatus in (ReporteFalla.ESTATUS_CERRADO, ReporteFalla.ESTATUS_CANCELADO):
            reporte.fecha_cierre = now
            reporte.cerrado_por = request.user
        if proveedor:
            reporte.proveedor_servicio = proveedor
        if costo_estimado is not None:
            reporte.costo_estimado = costo_estimado
        if costo_real is not None:
            reporte.costo_real = costo_real
        reporte.save()
        BitacoraFalla.objects.create(
            reporte=reporte,
            usuario=request.user,
            estatus_anterior=estatus_anterior if estatus != estatus_anterior else "",
            estatus_nuevo=estatus if estatus != estatus_anterior else "",
            comentario=comentario or "Seguimiento actualizado desde Mantenimiento.",
        )
        return _update_response(request, _branch_falla_item(reporte))

    if tipo == "unidad":
        reporte = get_object_or_404(ReporteUnidad, pk=pk)
        estatus = (request.data.get("estatus") or reporte.estatus).strip()
        if estatus not in {value for value, _label in ReporteUnidad.ESTATUS_CHOICES}:
            return Response({"error": "Estatus no válido."}, status=400)
        reporte.estatus = estatus
        reporte.asignado_a = request.user
        if proveedor:
            reporte.proveedor_servicio = proveedor
        if costo_estimado is not None:
            reporte.costo_servicio = costo_estimado
        if comentario:
            reporte.notas_compras = comentario
        reporte.save()
        return _update_response(request, _logistica_item(reporte))

    if tipo == "orden":
        orden = get_object_or_404(OrdenMantenimiento, pk=pk)
        estatus = (request.data.get("estatus") or orden.estatus).strip().upper()
        if estatus not in {value for value, _label in OrdenMantenimiento.ESTATUS_CHOICES}:
            return Response({"error": "Estatus no válido."}, status=400)
        orden.estatus = estatus
        if estatus == OrdenMantenimiento.ESTATUS_EN_PROCESO and not orden.fecha_inicio:
            orden.fecha_inicio = timezone.localdate()
        if estatus == OrdenMantenimiento.ESTATUS_CERRADA and not orden.fecha_cierre:
            orden.fecha_cierre = timezone.localdate()
        if proveedor:
            orden.responsable = proveedor
        if costo_real is not None:
            orden.costo_otros = costo_real
        orden.save()
        BitacoraMantenimiento.objects.create(
            orden=orden,
            usuario=request.user,
            accion="Seguimiento desde Mantenimiento",
            comentario=comentario or "Orden actualizada desde bandeja de mantenimiento.",
            costo_adicional=costo_real or Decimal("0"),
        )
        return _update_response(request, _branch_order_item(orden))

    return Response({"error": "Tipo no válido."}, status=400)


@login_required
def dashboard(request):
    _require_mantenimiento(request.user)
    origen = (request.GET.get("origen") or "").strip().lower()
    if origen not in {"", "sucursales", "logistica"}:
        return redirect("mantenimiento:dashboard")
    items = _unified_items(origen)
    return render(
        request,
        "mantenimiento/dashboard.html",
        {
            "items": items,
            "origen": origen or "todos",
            "counts": _unified_counts(),
            "estatus_fallas": ReporteFalla.ESTATUS,
            "estatus_unidad": ReporteUnidad.ESTATUS_CHOICES,
            "estatus_orden": OrdenMantenimiento.ESTATUS_CHOICES,
        },
    )


def pwa_mantenimiento(request):
    return render(request, "mantenimiento/pwa.html")
