from datetime import timedelta
from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from rest_framework import generics, status
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import BasePermission
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.tokens import RefreshToken

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento
from mantenimiento.models import SolicitudCancelacion
from core.access import can_manage_submodule, can_view_module, can_view_submodule, is_admin_or_dg
from core.models import sucursales_operativas
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla
from logistica.models import ReparacionUnidad, ReporteUnidad, ServicioRealizadoUnidad, TipoServicioUnidad, Unidad
from maestros.models import Proveedor

from .serializers import (
    ActivoListSerializer,
    ActivoQuickCreateSerializer,
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


class EsMantenimiento(BasePermission):
    GRUPOS = {"dg", "DG", "mantenimiento", "MANTENIMIENTO"}

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        grupos = set(request.user.groups.values_list("name", flat=True))
        return (
            request.user.is_superuser
            or bool(grupos & self.GRUPOS)
            or can_view_module(request.user, "activos")
            or can_manage_submodule(request.user, "mantenimiento", "bandeja")
            or can_view_submodule(request.user, "mantenimiento", "app")
            or can_view_submodule(request.user, "mantenimiento", "dashboard")
        )


def _require_mantenimiento(user):
    if is_admin_or_dg(user):
        return
    if not can_view_submodule(user, "mantenimiento", "dashboard"):
        raise PermissionDenied("No tienes permisos para ver Mantenimiento.")


def _puede_eliminar(user) -> bool:
    return user.is_authenticated and (user.is_superuser or user.is_staff or user.groups.filter(name__in=["dg", "DG"]).exists())


def _parse_decimal(raw):
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def _ensure_provider(nombre):
    nombre = (nombre or "").strip()
    if not nombre:
        return None
    proveedor, _created = Proveedor.objects.get_or_create(nombre=nombre, defaults={"activo": True})
    return proveedor


def _create_asset_from_followup(data, sucursal, proveedor_obj=None):
    nombre = (data.get("activo_nombre_nuevo") or "").strip()
    if not nombre:
        return None
    categoria = (data.get("activo_categoria_nueva") or "").strip() or "Mantenimiento"
    ubicacion = (data.get("activo_ubicacion_nueva") or "").strip()
    return Activo.objects.create(
        nombre=nombre,
        categoria=categoria,
        ubicacion=ubicacion,
        sucursal=sucursal,
        proveedor_mantenimiento=proveedor_obj,
        estado=Activo.ESTADO_OPERATIVO,
        criticidad=Activo.CRITICIDAD_MEDIA,
        activo=True,
    )


def _branch_statuses():
    return [ReporteFalla.ESTATUS_ABIERTO, ReporteFalla.ESTATUS_REVISION, ReporteFalla.ESTATUS_PROCESO]


def _order_open_statuses():
    return [OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO]


def _unit_open_statuses():
    return [ReporteUnidad.ESTATUS_ABIERTO, ReporteUnidad.ESTATUS_EN_PROCESO, ReporteUnidad.ESTATUS_PROGRAMADO]


def _dias_abierto(fecha):
    if not fecha:
        return 0
    ahora = timezone.now()
    delta = ahora - (fecha if fecha.tzinfo else timezone.make_aware(fecha))
    return max(0, delta.days)


def _semaforo(dias):
    if dias > 5:
        return "rojo"
    if dias > 2:
        return "amarillo"
    return "verde"


def _branch_falla_item(reporte):
    dias = _dias_abierto(reporte.fecha_reporte)
    return {
        "uid": f"falla:{reporte.id}",
        "tipo": "falla",
        "origen": "sucursales",
        "id": reporte.id,
        "titulo": reporte.titulo,
        "referencia": f"Falla #{reporte.id}",
        "ubicacion": reporte.sucursal.nombre if reporte.sucursal_id else "",
        "activo": str(reporte.activo_relacionado) if reporte.activo_relacionado_id else "",
        "activo_id": reporte.activo_relacionado_id or "",
        "area": reporte.get_area_display(),
        "categoria": reporte.categoria.nombre if reporte.categoria_id else "",
        "prioridad": reporte.get_prioridad_display(),
        "estatus": reporte.estatus,
        "estatus_display": reporte.get_estatus_display(),
        "descripcion": reporte.descripcion,
        "fecha": reporte.fecha_reporte,
        "proveedor": reporte.proveedor_servicio,
        "costo_estimado": reporte.costo_estimado,
        "costo_real": reporte.costo_real,
        "dias_abierto": dias,
        "semaforo": _semaforo(dias),
        "asignado": bool(getattr(reporte, "asignado_a_id", None)),
    }


def _branch_order_item(orden):
    dias = _dias_abierto(orden.creado_en)
    return {
        "uid": f"orden:{orden.id}",
        "tipo": "orden",
        "origen": "sucursales",
        "id": orden.id,
        "titulo": orden.descripcion or orden.get_tipo_display(),
        "referencia": orden.folio,
        "ubicacion": orden.activo_ref.sucursal.nombre if orden.activo_ref_id and orden.activo_ref.sucursal_id else "",
        "activo": str(orden.activo_ref) if orden.activo_ref_id else "",
        "activo_id": orden.activo_ref_id or "",
        "area": "Activo",
        "categoria": "Orden de mantenimiento",
        "prioridad": orden.get_prioridad_display(),
        "estatus": orden.estatus,
        "estatus_display": orden.get_estatus_display(),
        "descripcion": orden.descripcion,
        "fecha": orden.creado_en,
        "proveedor": orden.responsable,
        "costo_estimado": None,
        "costo_real": orden.costo_total,
        "dias_abierto": dias,
        "semaforo": _semaforo(dias),
        "asignado": bool(orden.responsable),
    }


def _logistica_item(reporte):
    dias = _dias_abierto(reporte.fecha_reporte)
    return {
        "uid": f"unidad:{reporte.id}",
        "tipo": "unidad",
        "origen": "logistica",
        "id": reporte.id,
        "titulo": reporte.get_tipo_display(),
        "referencia": f"Unidad #{reporte.id}",
        "ubicacion": reporte.unidad.codigo if reporte.unidad_id else "",
        "activo": str(reporte.unidad) if reporte.unidad_id else "",
        "activo_id": "",
        "area": "Logística",
        "categoria": "Unidad logística",
        "prioridad": reporte.get_severidad_display(),
        "estatus": reporte.estatus,
        "estatus_display": reporte.get_estatus_display(),
        "descripcion": reporte.descripcion,
        "fecha": reporte.fecha_reporte,
        "proveedor": reporte.proveedor_servicio,
        "costo_estimado": reporte.costo_servicio,
        "costo_real": reporte.costo_servicio,
        "dias_abierto": dias,
        "semaforo": _semaforo(dias),
        "asignado": bool(reporte.proveedor_servicio),
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


def _item_stage(item):
    estatus = str(item.get("estatus") or "").lower()
    proveedor = bool(item.get("proveedor"))
    costo_estimado = item.get("costo_estimado") is not None
    if estatus in {"cerrado", "cancelado", "resuelto", "cerrada", "cancelada"}:
        return "validacion"
    if estatus in {"abierto", "pendiente"}:
        return "nuevo"
    if estatus == "en_revision":
        return "diagnostico"
    if estatus == "programado":
        return "programado"
    if proveedor or costo_estimado:
        return "cotizacion"
    return "atencion"


def _top_counts(items, field, limit=4):
    counts = {}
    for item in items:
        key = item.get(field) or "Sin dato"
        counts[key] = counts.get(key, 0) + 1
    return sorted(counts.items(), key=lambda row: (-row[1], row[0]))[:limit]


def _kanban_columns(items):
    columns = [
        {"key": "nuevo", "label": "Nuevo", "hint": "Sin tomar", "items": []},
        {"key": "diagnostico", "label": "Diagnóstico", "hint": "Revisión inicial", "items": []},
        {"key": "cotizacion", "label": "Cotización", "hint": "Proveedor y monto", "items": []},
        {"key": "programado", "label": "Programado", "hint": "Fecha o visita", "items": []},
        {"key": "atencion", "label": "En atención", "hint": "Trabajo activo", "items": []},
        {"key": "validacion", "label": "Validación", "hint": "Cierre y factura", "items": []},
    ]
    by_key = {column["key"]: column for column in columns}
    for item in items:
        item["stage"] = _item_stage(item)
        by_key.get(item["stage"], by_key["atencion"])["items"].append(item)
    return columns


def _dashboard_summary(items):
    critical_labels = {"Crítica - Operación detenida", "Crítica", "Crítico"}
    costo_30d = sum(
        (item.get("costo_real") or item.get("costo_estimado") or Decimal("0")) for item in items
    )
    dias_list = [item["dias_abierto"] for item in items if item.get("dias_abierto", 0) > 0]
    tiempo_promedio = round(sum(dias_list) / len(dias_list), 1) if dias_list else 0
    sin_asignar = sum(1 for item in items if not item.get("asignado"))
    rojos = sum(1 for item in items if item.get("semaforo") == "rojo")
    return {
        "total": len(items),
        "fallas_activas": sum(1 for item in items if item["tipo"] == "falla"),
        "criticas": sum(1 for item in items if item.get("prioridad") in critical_labels),
        "ordenes_abiertas": sum(1 for item in items if item["tipo"] == "orden"),
        "reparaciones_flota": sum(1 for item in items if item["tipo"] == "unidad"),
        "costo_30d": costo_30d,
        "unidades_activas": Unidad.objects.filter(activa=True).count(),
        "activos_registrados": Activo.objects.filter(activo=True).count(),
        "tiempo_promedio": tiempo_promedio,
        "sin_asignar": sin_asignar,
        "rojos": rojos,
        "por_ubicacion": _top_counts(items, "ubicacion"),
        "por_area": _top_counts(items, "area"),
        "por_tipo": _top_counts(items, "categoria"),
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


class ActivoQuickCreateView(generics.CreateAPIView):
    authentication_classes = AUTH
    permission_classes = [EsMantenimiento]
    serializer_class = ActivoQuickCreateSerializer


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
    permission_classes = [EsMantenimiento]
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
def sucursales(request):
    data = [
        {"id": sucursal.id, "codigo": sucursal.codigo, "nombre": sucursal.nombre}
        for sucursal in sucursales_operativas().order_by("nombre", "codigo")
    ]
    return Response(data)


@api_view(["GET"])
@authentication_classes([SessionAuthentication])
@permission_classes([EsMantenimiento])
def session_token(request):
    refresh = RefreshToken.for_user(request.user)
    return Response({"access": str(refresh.access_token), "refresh": str(refresh)})


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
        proveedor_obj = _ensure_provider(proveedor) if proveedor else None
        if proveedor:
            reporte.proveedor_servicio = proveedor
        activo_id = request.data.get("activo_id")
        if activo_id:
            reporte.activo_relacionado = get_object_or_404(Activo, pk=activo_id, activo=True)
        else:
            nuevo_activo = _create_asset_from_followup(request.data, reporte.sucursal, proveedor_obj)
            if nuevo_activo:
                reporte.activo_relacionado = nuevo_activo
        if costo_estimado is not None:
            reporte.costo_estimado = costo_estimado
        if costo_real is not None:
            reporte.costo_real = costo_real
        reporte.save()
        # Si la falla se cierra, restaurar el activo vinculado a OPERATIVO
        if estatus in (ReporteFalla.ESTATUS_CERRADO, ReporteFalla.ESTATUS_RESUELTO):
            if reporte.activo_relacionado_id:
                activo_vinculado = reporte.activo_relacionado
                if activo_vinculado.estado == Activo.ESTADO_MANTENIMIENTO:
                    activo_vinculado.estado = Activo.ESTADO_OPERATIVO
                    activo_vinculado.save(update_fields=["estado", "actualizado_en"])
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
            _ensure_provider(proveedor)
            reporte.proveedor_servicio = proveedor
        costo_unidad = costo_real if costo_real is not None else costo_estimado
        if costo_unidad is not None:
            reporte.costo_servicio = costo_unidad
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
            _ensure_provider(proveedor)
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
def crear_falla(request):
    """Crea un ReporteFalla directamente desde el panel de mantenimiento (sin PWA)."""
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    sucursal_id = request.POST.get("sucursal") or ""
    categoria_id = request.POST.get("categoria") or ""
    titulo = (request.POST.get("titulo") or "").strip()
    descripcion = (request.POST.get("descripcion") or "").strip()
    area = (request.POST.get("area") or ReporteFalla.AREA_GENERAL).strip()
    prioridad = (request.POST.get("prioridad") or ReporteFalla.PRIORIDAD_MEDIA).strip()
    activo_id = (request.POST.get("activo_id") or "").strip()

    errores = []
    if not sucursal_id:
        errores.append("Selecciona una sucursal.")
    if not categoria_id:
        errores.append("Selecciona una categoría.")
    if not titulo:
        errores.append("El título es obligatorio.")
    if not descripcion:
        errores.append("La descripción es obligatoria.")

    from django.contrib import messages as msg
    if errores:
        for e in errores:
            msg.error(request, e)
        return redirect("mantenimiento:dashboard")

    from core.models import Sucursal
    sucursal = Sucursal.objects.filter(pk=sucursal_id).first()
    categoria = CategoriaFalla.objects.filter(pk=categoria_id, activo=True).first()
    if not sucursal or not categoria:
        msg.error(request, "Sucursal o categoría no válida.")
        return redirect("mantenimiento:dashboard")

    activo_obj = Activo.objects.filter(pk=activo_id, activo=True).first() if activo_id else None

    foto = request.FILES.get("foto_evidencia")
    reporte = ReporteFalla(
        sucursal=sucursal,
        categoria=categoria,
        area=area,
        titulo=titulo,
        descripcion=descripcion,
        prioridad=prioridad,
        activo_relacionado=activo_obj,
        reportado_por=request.user,
        estatus=ReporteFalla.ESTATUS_ABIERTO,
    )
    if foto:
        reporte.foto_evidencia = foto
    reporte.save()

    BitacoraFalla.objects.create(
        reporte=reporte,
        usuario=request.user,
        estatus_anterior="",
        estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
        comentario="Reporte creado desde panel de Mantenimiento.",
    )
    msg.success(request, f"Falla #{reporte.id} creada: {titulo}")
    return redirect("mantenimiento:dashboard")


@login_required
def dashboard(request):
    _require_mantenimiento(request.user)
    origen = (request.GET.get("origen") or "").strip().lower()
    if origen not in {"", "sucursales", "logistica"}:
        return redirect("mantenimiento:dashboard")
    items = _unified_items(origen)
    # Top-5 proveedores más usados en mantenimiento primero, luego el resto
    from django.db.models import Count as _Count
    top_ids = list(
        ReporteFalla.objects.exclude(proveedor_servicio="")
        .values("proveedor_servicio")
        .annotate(n=_Count("id"))
        .order_by("-n")
        .values_list("proveedor_servicio", flat=True)[:5]
    )
    top_proveedores = list(Proveedor.objects.filter(nombre__in=top_ids, activo=True))
    resto_proveedores = list(
        Proveedor.objects.filter(activo=True).exclude(nombre__in=top_ids).order_by("nombre")[:115]
    )
    provider_options = top_proveedores + resto_proveedores
    asset_options = Activo.objects.select_related("sucursal").filter(activo=True).order_by(
        "sucursal__nombre", "nombre", "codigo"
    )[:180]
    fleet_units = Unidad.objects.filter(activa=True).select_related("sucursal").order_by("codigo")[:6]
    sucursales_list = sucursales_operativas().order_by("nombre")
    categorias_list = CategoriaFalla.objects.filter(activo=True).order_by("orden", "nombre")

    today = timezone.localdate()
    # Todos los planes activos ordenados: vencidos primero, luego por fecha
    planes_proximos = list(
        PlanMantenimiento.objects.filter(
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            activo=True,
        )
        .select_related("activo_ref", "activo_ref__sucursal")
        .order_by("proxima_ejecucion")[:80]
    )
    for plan in planes_proximos:
        plan.dias_para_vencer = (plan.proxima_ejecucion - today).days if plan.proxima_ejecucion else None
        plan.vencido = plan.dias_para_vencer is not None and plan.dias_para_vencer < 0
        plan.urgente = plan.dias_para_vencer is not None and 0 <= plan.dias_para_vencer <= 7

    # Servicios de flota — todos con proxima_fecha pendiente, más reciente por unidad+tipo
    from django.db.models import Max
    ultimos_ids = (
        ServicioRealizadoUnidad.objects.filter(proxima_fecha__isnull=False)
        .values("unidad_id", "tipo_servicio_id")
        .annotate(ultimo_id=Max("id"))
        .values_list("ultimo_id", flat=True)
    )
    servicios_flota = list(
        ServicioRealizadoUnidad.objects.filter(id__in=ultimos_ids)
        .select_related("unidad", "unidad__sucursal", "tipo_servicio")
        .order_by("proxima_fecha")[:60]
    )
    for srv in servicios_flota:
        srv.dias_para_vencer = (srv.proxima_fecha - today).days if srv.proxima_fecha else None
        srv.vencido = srv.dias_para_vencer is not None and srv.dias_para_vencer < 0
        srv.urgente = srv.dias_para_vencer is not None and 0 <= srv.dias_para_vencer <= 7

    solicitudes_cancelacion = (
        SolicitudCancelacion.objects.filter(estatus=SolicitudCancelacion.ESTATUS_PENDIENTE)
        .select_related("solicitado_por")
        .order_by("-creado_en")[:20]
        if _puede_eliminar(request.user)
        else []
    )

    return render(
        request,
        "mantenimiento/dashboard.html",
        {
            "items": items,
            "kanban_columns": _kanban_columns(items),
            "summary": _dashboard_summary(items),
            "provider_options": provider_options,
            "asset_options": asset_options,
            "fleet_units": fleet_units,
            "sucursales_list": sucursales_list,
            "categorias_list": categorias_list,
            "origen": origen or "todos",
            "counts": _unified_counts(),
            "estatus_fallas": ReporteFalla.ESTATUS,
            "estatus_unidad": ReporteUnidad.ESTATUS_CHOICES,
            "estatus_orden": OrdenMantenimiento.ESTATUS_CHOICES,
            "areas_falla": ReporteFalla.AREAS,
            "prioridades_falla": ReporteFalla.PRIORIDAD,
            "planes_proximos": planes_proximos,
            "servicios_flota": servicios_flota,
            "solicitudes_cancelacion": solicitudes_cancelacion,
            "puede_eliminar": _puede_eliminar(request.user),
            "today": today,
            "plan_tipo_choices": PlanMantenimiento.TIPO_CHOICES,
            "plan_estatus_choices": PlanMantenimiento.ESTATUS_CHOICES,
            "activos_para_plan": list(Activo.objects.select_related("sucursal").filter(activo=True).order_by("sucursal__nombre", "nombre")[:400]),
            "unidades_para_servicio": list(Unidad.objects.filter(activa=True).select_related("sucursal").order_by("descripcion", "codigo")),
            "tipos_servicio": list(TipoServicioUnidad.objects.filter(activo=True).order_by("nombre")),
        },
    )


@login_required
def pwa_mantenimiento(request):
    if not (is_admin_or_dg(request.user) or EsMantenimiento().has_permission(request, None)):
        raise PermissionDenied("No tienes permisos para usar Mantenimiento")
    return render(request, "mantenimiento/pwa.html")


@login_required
def solicitar_cancelacion(request, tipo, pk):
    """Crea una SolicitudCancelacion y notifica al DG por email."""
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    from django.contrib import messages as msg
    from django.core.mail import send_mail
    from django.conf import settings as djsettings
    from django.contrib.auth import get_user_model

    tipo = (tipo or "").strip().lower()
    motivo = (request.POST.get("motivo") or "").strip()
    if not motivo:
        msg.error(request, "Debes indicar el motivo de cancelación.")
        return redirect("mantenimiento:dashboard")

    if tipo == "falla":
        obj = get_object_or_404(ReporteFalla, pk=pk)
        referencia = f"Falla #{obj.id} · {obj.titulo}"
    elif tipo == "unidad":
        obj = get_object_or_404(ReporteUnidad, pk=pk)
        referencia = f"Reporte unidad #{obj.id} · {obj.get_tipo_display()}"
    elif tipo == "orden":
        obj = get_object_or_404(OrdenMantenimiento, pk=pk)
        referencia = f"Orden {obj.folio}"
    else:
        msg.error(request, "Tipo no válido.")
        return redirect("mantenimiento:dashboard")

    if _puede_eliminar(request.user):
        # DG elimina directo sin pasar por solicitud
        obj.delete()
        msg.success(request, f"{referencia} eliminado.")
        return redirect("mantenimiento:dashboard")

    solicitud = SolicitudCancelacion.objects.create(
        tipo=tipo,
        objeto_id=pk,
        referencia=referencia,
        motivo=motivo,
        solicitado_por=request.user,
    )

    dg_emails = list(
        get_user_model()
        .objects.filter(groups__name__in=["dg", "DG"], email__isnull=False)
        .exclude(email="")
        .values_list("email", flat=True)
        .distinct()
    )
    if dg_emails:
        solicitante = request.user.get_full_name() or request.user.username
        asunto = f"[ERP Mantenimiento] Solicitud cancelación: {referencia}"
        cuerpo = (
            f"El usuario {solicitante} solicita cancelar el siguiente reporte:\n\n"
            f"Referencia: {referencia}\n"
            f"Motivo: {motivo}\n\n"
            f"Revisa y aprueba o rechaza en el ERP:\n"
            f"Mantenimiento > Solicitudes de cancelación\n\n"
            f"ID solicitud: #{solicitud.id}"
        )
        try:
            send_mail(asunto, cuerpo, djsettings.DEFAULT_FROM_EMAIL, dg_emails, fail_silently=True)
        except Exception:
            pass

    msg.success(request, f"Solicitud de cancelación enviada para '{referencia}'. El DG recibirá la notificación.")
    return redirect("mantenimiento:dashboard")


@login_required
def resolver_cancelacion(request, solicitud_id):
    """DG aprueba (elimina el objeto) o rechaza la solicitud."""
    if not _puede_eliminar(request.user):
        raise PermissionDenied("Solo el DG puede resolver solicitudes de cancelación.")
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    from django.contrib import messages as msg

    solicitud = get_object_or_404(SolicitudCancelacion, pk=solicitud_id, estatus=SolicitudCancelacion.ESTATUS_PENDIENTE)
    accion = (request.POST.get("accion") or "").strip().lower()
    notas = (request.POST.get("notas_resolucion") or "").strip()

    solicitud.resuelto_por = request.user
    solicitud.resuelto_en = timezone.now()
    solicitud.notas_resolucion = notas

    if accion == "aprobar":
        tipo = solicitud.tipo
        pk = solicitud.objeto_id
        eliminado = False
        if tipo == "falla":
            obj = ReporteFalla.objects.filter(pk=pk).first()
            if obj:
                obj.delete()
                eliminado = True
        elif tipo == "unidad":
            obj = ReporteUnidad.objects.filter(pk=pk).first()
            if obj:
                obj.delete()
                eliminado = True
        elif tipo == "orden":
            obj = OrdenMantenimiento.objects.filter(pk=pk).first()
            if obj:
                obj.delete()
                eliminado = True
        solicitud.estatus = SolicitudCancelacion.ESTATUS_APROBADA
        solicitud.save()
        if eliminado:
            msg.success(request, f"Solicitud #{solicitud.id} aprobada. '{solicitud.referencia}' eliminado.")
        else:
            msg.warning(request, f"Solicitud #{solicitud.id} aprobada, pero el objeto ya no existía.")
    else:
        solicitud.estatus = SolicitudCancelacion.ESTATUS_RECHAZADA
        solicitud.save()
        msg.info(request, f"Solicitud #{solicitud.id} rechazada.")

    return redirect("mantenimiento:dashboard")


@login_required
def gestionar_plan(request):
    """Crear o editar un PlanMantenimiento desde el módulo de mantenimiento."""
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    from django.contrib import messages as msg

    action = (request.POST.get("action") or "crear").strip().lower()

    if action == "editar":
        plan_id = _safe_int(request.POST.get("plan_id"))
        plan = get_object_or_404(PlanMantenimiento, pk=plan_id)
    else:
        activo_id = _safe_int(request.POST.get("activo_id"))
        if not activo_id:
            msg.error(request, "Selecciona un activo para el plan.")
            return redirect("mantenimiento:dashboard")
        activo_obj = get_object_or_404(Activo, pk=activo_id, activo=True)
        plan = PlanMantenimiento(activo_ref=activo_obj)

    nombre = (request.POST.get("nombre") or "").strip()
    if not nombre:
        msg.error(request, "El nombre del plan es obligatorio.")
        return redirect("mantenimiento:dashboard")

    tipo = (request.POST.get("tipo") or PlanMantenimiento.TIPO_PREVENTIVO).strip().upper()
    estatus = (request.POST.get("estatus") or PlanMantenimiento.ESTATUS_ACTIVO).strip().upper()
    plan.nombre = nombre
    plan.tipo = tipo if tipo in {x[0] for x in PlanMantenimiento.TIPO_CHOICES} else PlanMantenimiento.TIPO_PREVENTIVO
    plan.estatus = estatus if estatus in {x[0] for x in PlanMantenimiento.ESTATUS_CHOICES} else PlanMantenimiento.ESTATUS_ACTIVO
    plan.frecuencia_dias = max(1, _safe_int(request.POST.get("frecuencia_dias"), default=30))
    plan.tolerancia_dias = max(0, _safe_int(request.POST.get("tolerancia_dias"), default=0))
    plan.responsable = (request.POST.get("responsable") or "").strip()
    plan.instrucciones = (request.POST.get("instrucciones") or "").strip()
    from django.utils.dateparse import parse_date as _pd
    nueva_ultima = _pd(request.POST.get("ultima_ejecucion") or "")
    nueva_proxima = _pd(request.POST.get("proxima_ejecucion") or "")
    if nueva_ultima:
        plan.ultima_ejecucion = nueva_ultima
    if nueva_proxima:
        plan.proxima_ejecucion = nueva_proxima
    elif nueva_ultima and plan.frecuencia_dias:
        plan.recompute_next_date()
    plan.activo = True
    plan.save()

    verbo = "actualizado" if action == "editar" else "creado"
    msg.success(request, f"Plan '{plan.nombre}' {verbo}.")
    return redirect("mantenimiento:dashboard")


@login_required
def registrar_servicio_flota(request):
    """Registra un ServicioRealizadoUnidad desde el módulo de mantenimiento."""
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    from django.contrib import messages as msg
    from django.utils.dateparse import parse_date as _pd

    unidad_id = _safe_int(request.POST.get("unidad_id"))
    tipo_id = _safe_int(request.POST.get("tipo_servicio_id"))
    fecha_raw = request.POST.get("fecha_servicio") or ""
    fecha = _pd(fecha_raw) or timezone.localdate()

    if not unidad_id or not tipo_id:
        msg.error(request, "Selecciona unidad y tipo de servicio.")
        return redirect("mantenimiento:dashboard")

    unidad = get_object_or_404(Unidad, pk=unidad_id, activa=True)
    tipo_srv = get_object_or_404(TipoServicioUnidad, pk=tipo_id, activo=True)

    from django.utils.dateparse import parse_date as _pd
    km = _safe_int(request.POST.get("km_al_servicio")) or None
    proxima_manual = _pd(request.POST.get("proxima_fecha") or "")
    proximos_km_manual = _safe_int(request.POST.get("proximos_km")) or None

    srv = ServicioRealizadoUnidad(
        unidad=unidad,
        tipo_servicio=tipo_srv,
        fecha_servicio=fecha,
        km_al_servicio=km,
        proveedor=(request.POST.get("proveedor") or "").strip(),
        costo=_parse_decimal(request.POST.get("costo")),
        notas=(request.POST.get("notas") or "").strip(),
        registrado_por=request.user,
    )
    srv.save()  # save() auto-calcula proxima_fecha y proximos_km desde el tipo
    # Sobrescribir con valores manuales si el técnico los especificó
    if proxima_manual or proximos_km_manual:
        if proxima_manual:
            srv.proxima_fecha = proxima_manual
        if proximos_km_manual:
            srv.proximos_km = proximos_km_manual
        srv.save(update_fields=["proxima_fecha", "proximos_km"])
    msg.success(request, f"Servicio registrado: {tipo_srv.nombre} · {unidad.codigo}.")
    return redirect("mantenimiento:dashboard")


@login_required
def registrar_ejecucion_plan(request, pk):
    """Registra la ejecución de un plan de mantenimiento recurrente."""
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    plan = get_object_or_404(PlanMantenimiento, pk=pk, activo=True)
    from django.contrib import messages as msg

    fecha_raw = (request.POST.get("fecha_ejecucion") or "").strip()
    notas = (request.POST.get("notas") or "").strip()
    try:
        from django.utils.dateparse import parse_date
        fecha = parse_date(fecha_raw) or timezone.localdate()
    except Exception:
        fecha = timezone.localdate()

    plan.ultima_ejecucion = fecha
    plan.recompute_next_date()
    plan.save(update_fields=["ultima_ejecucion", "proxima_ejecucion", "actualizado_en"])

    # Crear orden de mantenimiento preventivo cerrada para trazabilidad
    orden = OrdenMantenimiento.objects.create(
        activo_ref=plan.activo_ref,
        plan_ref=plan,
        tipo=OrdenMantenimiento.TIPO_PREVENTIVO,
        prioridad=OrdenMantenimiento.PRIORIDAD_BAJA,
        estatus=OrdenMantenimiento.ESTATUS_CERRADA,
        fecha_programada=fecha,
        fecha_inicio=fecha,
        fecha_cierre=fecha,
        responsable=request.user.get_full_name() or request.user.username,
        descripcion=notas or f"Ejecución de plan: {plan.nombre}",
        origen=OrdenMantenimiento.ORIGEN_PLAN,
        creado_por=request.user,
    )
    BitacoraMantenimiento.objects.create(
        orden=orden,
        usuario=request.user,
        accion="Ejecución registrada",
        comentario=notas or f"Registrado desde bandeja de mantenimiento. Plan: {plan.nombre}",
    )
    msg.success(request, f"Plan '{plan.nombre}' ejecutado. Próxima: {plan.proxima_ejecucion}")
    return redirect("mantenimiento:dashboard")
