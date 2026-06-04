from datetime import timedelta

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator
from django.db.models import Avg, Count, DurationField, ExpressionWrapper, F, Q
from django.shortcuts import redirect, render
from django.utils import timezone
from rest_framework import generics, permissions
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication

from activos.models import Activo
from core.access import can_manage_submodule, can_view_module, can_view_submodule, is_admin_or_dg, is_repartidor_only
from core.models import Sucursal, sucursales_operativas_q

from .models import BitacoraFalla, CategoriaFalla, ReporteFalla
from .serializers import (
    ActivoFallaSerializer,
    CambioEstatusSerializer,
    CategoriaFallaSerializer,
    ReporteFallaCreateSerializer,
    ReporteFallaDetailSerializer,
    ReporteFallaListSerializer,
    ReporteFallaUpdateSerializer,
    SucursalFallaSerializer,
)


GRUPOS_AREA_FALLAS = {"ventas", "produccion"}
GRUPOS_REPORTE_FALLAS = {"personal_sucursal", "compras_logistica", "dg", *GRUPOS_AREA_FALLAS}
GRUPOS_GESTION_FALLAS = {"compras_logistica", "dg"}
GRUPOS_VER_TODO_FALLAS = {"compras_logistica", "dg", "supervisor_logistica"}


def _group_names(user) -> set[str]:
    if not user or not user.is_authenticated:
        return set()
    return set(user.groups.values_list("name", flat=True))


def _sucursal_usuario(user):
    profile = getattr(user, "userprofile", None)
    if profile and profile.sucursal_id:
        return profile.sucursal
    return None


def _puede_reportar_fallas(user) -> bool:
    if not user or not user.is_authenticated or is_repartidor_only(user):
        return False
    return can_view_submodule(user, "fallas", "reportar")


class EsPersonalSucursal(permissions.BasePermission):
    """Personal de sucursal o gestores autorizados pueden reportar fallas."""

    def has_permission(self, request, view):
        return is_admin_or_dg(request.user) or _puede_reportar_fallas(request.user)


class EsComprasODG(permissions.BasePermission):
    """Solo compras_logistica o dg pueden gestionar fallas."""

    GRUPOS = GRUPOS_GESTION_FALLAS

    def has_permission(self, request, view):
        return _puede_cambiar_estatus_fallas(request.user)


def _puede_gestionar_fallas(user) -> bool:
    if not user.is_authenticated:
        return False
    return can_view_module(user, "fallas")


def _puede_cambiar_estatus_fallas(user) -> bool:
    if not user.is_authenticated:
        return False
    return can_manage_submodule(user, "fallas", "gestion")


def _puede_modificar_reporte_propio(reporte, user) -> bool:
    return (
        user.is_authenticated
        and reporte.reportado_por_id == user.id
        and reporte.estatus == ReporteFalla.ESTATUS_ABIERTO
    )


def _filtrar_reportes_por_usuario(qs, user):
    grupos = _group_names(user)
    grupos_lower = {g.lower() for g in grupos}
    if user.is_superuser or grupos & GRUPOS_VER_TODO_FALLAS:
        return qs
    if "ventas" in grupos_lower:
        return qs.filter(Q(area=ReporteFalla.AREA_VENTAS) | Q(reportado_por=user))
    if "produccion" in grupos_lower:
        return qs.filter(Q(area=ReporteFalla.AREA_PRODUCCION) | Q(reportado_por=user))
    return qs.filter(reportado_por=user)


def _sucursales_disponibles_para_reporte(user):
    sucursales = Sucursal.objects.filter(sucursales_operativas_q()).order_by("nombre")
    if is_admin_or_dg(user) or _puede_cambiar_estatus_fallas(user):
        return sucursales
    grupos_lower = {g.lower() for g in _group_names(user)}
    if "produccion" in grupos_lower:
        return sucursales
    sucursal_usuario = _sucursal_usuario(user)
    return sucursales.filter(pk=sucursal_usuario.pk) if sucursal_usuario else sucursales.none()


class SucursalFallaListView(generics.ListAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    serializer_class = SucursalFallaSerializer
    permission_classes = [EsPersonalSucursal]

    def get_queryset(self):
        qs = Sucursal.objects.filter(sucursales_operativas_q()).order_by("nombre")
        if is_admin_or_dg(self.request.user) or _puede_cambiar_estatus_fallas(self.request.user):
            return qs
        grupos_lower = {g.lower() for g in _group_names(self.request.user)}
        if "produccion" in grupos_lower:
            return qs
        sucursal = _sucursal_usuario(self.request.user)
        if sucursal:
            return qs.filter(pk=sucursal.pk)
        return qs.none()


class CategoriaFallaListView(generics.ListAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    queryset = CategoriaFalla.objects.filter(activo=True)
    serializer_class = CategoriaFallaSerializer
    permission_classes = [EsPersonalSucursal]


class CategoriaFallaAdminView(generics.ListCreateAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    queryset = CategoriaFalla.objects.all().order_by("orden", "nombre")
    serializer_class = CategoriaFallaSerializer
    permission_classes = [EsComprasODG]


class CategoriaFallaUpdateView(generics.RetrieveUpdateAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    queryset = CategoriaFalla.objects.all()
    serializer_class = CategoriaFallaSerializer
    permission_classes = [EsComprasODG]


class ActivoFallaListView(generics.ListAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    serializer_class = ActivoFallaSerializer
    permission_classes = [EsPersonalSucursal]

    def get_queryset(self):
        qs = Activo.objects.filter(activo=True).order_by("nombre", "codigo")
        sucursal_id = self.request.query_params.get("sucursal")
        if not sucursal_id:
            return qs[:50]
        try:
            sucursal = Sucursal.objects.get(pk=sucursal_id)
        except (Sucursal.DoesNotExist, ValueError):
            return qs.none()
        return qs.filter(
            Q(sucursal_id=sucursal.pk) | Q(ubicacion__icontains=sucursal.nombre) | Q(ubicacion__icontains=sucursal.codigo)
        )[:50]


class ReporteFallaListCreateView(generics.ListCreateAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    permission_classes = [EsPersonalSucursal]

    def get_queryset(self):
        qs = ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por")
        if self.request.query_params.get("mine") in {"1", "true", "True"}:
            return qs.filter(reportado_por=self.request.user)
        estatus = self.request.query_params.get("estatus")
        sucursal = self.request.query_params.get("sucursal")
        prioridad = self.request.query_params.get("prioridad")
        area = self.request.query_params.get("area")
        if estatus:
            qs = qs.filter(estatus=estatus)
        if sucursal:
            qs = qs.filter(sucursal_id=sucursal)
        if prioridad:
            qs = qs.filter(prioridad=prioridad)
        if area:
            qs = qs.filter(area=area)

        return _filtrar_reportes_por_usuario(qs, self.request.user)

    def get_serializer_class(self):
        if self.request.method == "POST":
            return ReporteFallaCreateSerializer
        return ReporteFallaListSerializer


class ReporteFallaDetailView(generics.RetrieveUpdateAPIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    http_method_names = ["get", "patch", "put", "head", "options"]
    queryset = ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por").prefetch_related(
        "bitacora__usuario"
    )
    serializer_class = ReporteFallaDetailSerializer
    permission_classes = [EsPersonalSucursal]

    def get_queryset(self):
        qs = super().get_queryset()
        return _filtrar_reportes_por_usuario(qs, self.request.user)

    def get_serializer_class(self):
        if self.request.method in {"PUT", "PATCH"}:
            return ReporteFallaUpdateSerializer
        return ReporteFallaDetailSerializer

    def perform_update(self, serializer):
        reporte = self.get_object()
        if not _puede_modificar_reporte_propio(reporte, self.request.user):
            raise PermissionDenied
        sucursal = serializer.validated_data.get("sucursal")
        if sucursal and not _sucursales_disponibles_para_reporte(self.request.user).filter(pk=sucursal.pk).exists():
            raise PermissionDenied
        serializer.save()


@api_view(["POST"])
@authentication_classes([JWTAuthentication, TokenAuthentication, SessionAuthentication])
@permission_classes([EsComprasODG])
def cambiar_estatus(request, pk):
    """Transición de estatus y actualización de seguimiento con bitácora automática."""

    try:
        reporte = ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por").get(pk=pk)
    except ReporteFalla.DoesNotExist:
        return Response({"error": "Reporte no encontrado."}, status=404)

    serializer = CambioEstatusSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=400)

    data = serializer.validated_data
    estatus_anterior = reporte.estatus
    nuevo_estatus = data.get("estatus") or estatus_anterior
    estatus_cambio = nuevo_estatus != estatus_anterior
    now = timezone.now()

    if estatus_cambio:
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

    comentario = data.get("comentario", "")
    if estatus_cambio or comentario or data.get("asignado_a") or data.get("costo_estimado") is not None or data.get("costo_real") is not None or data.get("proveedor_servicio"):
        BitacoraFalla.objects.create(
            reporte=reporte,
            usuario=request.user,
            estatus_anterior=estatus_anterior if estatus_cambio else "",
            estatus_nuevo=nuevo_estatus if estatus_cambio else "",
            comentario=comentario or "Seguimiento actualizado.",
        )

    if estatus_cambio:
        try:
            from .tasks import notificar_cambio_estatus

            notificar_cambio_estatus.delay(reporte.pk, nuevo_estatus, request.user.pk)
        except Exception:
            pass

    return Response(ReporteFallaDetailSerializer(reporte, context={"request": request}).data)


@api_view(["GET"])
@authentication_classes([JWTAuthentication, TokenAuthentication, SessionAuthentication])
@permission_classes([EsPersonalSucursal])
def perfil_actual(request):
    """Perfil mínimo para la PWA de fallas."""

    user = request.user
    return Response(
        {
            "id": user.pk,
            "username": user.username,
            "nombre": user.get_full_name() or user.username,
            "groups": list(user.groups.values_list("name", flat=True)),
        }
    )


@api_view(["GET"])
@authentication_classes([JWTAuthentication, TokenAuthentication, SessionAuthentication])
@permission_classes([EsPersonalSucursal])
def dashboard_stats(request):
    """Estadísticas para el dashboard ejecutivo."""

    hoy = timezone.now()
    hace_30 = hoy - timedelta(days=30)
    activos = [ReporteFalla.ESTATUS_ABIERTO, ReporteFalla.ESTATUS_REVISION, ReporteFalla.ESTATUS_PROCESO]
    qs_base = _filtrar_reportes_por_usuario(ReporteFalla.objects.all(), request.user)
    area_param = request.query_params.get("area")
    if area_param:
        qs_base = qs_base.filter(area=area_param)
    tiempos = (
        qs_base.filter(fecha_asignacion__isnull=False, fecha_reporte__gte=hace_30)
        .annotate(
            duracion=ExpressionWrapper(F("fecha_asignacion") - F("fecha_reporte"), output_field=DurationField())
        )
        .aggregate(promedio=Avg("duracion"))
    )

    promedio = tiempos["promedio"]
    return Response(
        {
            "total_abiertos": qs_base.filter(estatus__in=activos).count(),
            "criticos_activos": qs_base.filter(estatus__in=activos, prioridad=ReporteFalla.PRIORIDAD_CRITICA).count(),
            "resueltos_mes": qs_base.filter(
                estatus=ReporteFalla.ESTATUS_RESUELTO, fecha_resolucion__gte=hace_30
            ).count(),
            "por_sucursal": list(
                qs_base.filter(estatus__in=activos)
                .values("sucursal__nombre")
                .annotate(total=Count("id"))
                .order_by("-total")
            ),
            "por_categoria": list(
                qs_base.filter(estatus__in=activos)
                .values("categoria__nombre")
                .annotate(total=Count("id"))
                .order_by("-total")
            ),
            "tiempo_respuesta_promedio_horas": round(promedio.total_seconds() / 3600, 1) if promedio else None,
        }
    )


@login_required
def dashboard_view(request):
    if not can_view_submodule(request.user, "fallas", "dashboard"):
        raise PermissionDenied
    grupos = _group_names(request.user)
    grupos_lower = {g.lower() for g in grupos}
    es_dg = request.user.is_superuser or bool(grupos & GRUPOS_GESTION_FALLAS)
    area_usuario = ""
    if "ventas" in grupos_lower and not (grupos & GRUPOS_VER_TODO_FALLAS) and not request.user.is_superuser:
        area_usuario = ReporteFalla.AREA_VENTAS
    elif "produccion" in grupos_lower and not (grupos & GRUPOS_VER_TODO_FALLAS) and not request.user.is_superuser:
        area_usuario = ReporteFalla.AREA_PRODUCCION
    return render(
        request,
        "fallas/dashboard.html",
        {
            "area_usuario": area_usuario,
            "es_dg": es_dg,
            "puede_gestionar": _puede_cambiar_estatus_fallas(request.user),
            "tab": request.GET.get("tab") or "reportes",
        },
    )


@login_required
def pwa_app(request):
    if not _puede_reportar_fallas(request.user):
        raise PermissionDenied
    return render(request, "fallas/pwa_reporte.html")


@login_required
def pwa_reporte(request):
    if not _puede_reportar_fallas(request.user):
        raise PermissionDenied

    sucursales = _sucursales_disponibles_para_reporte(request.user)
    categorias = CategoriaFalla.objects.filter(activo=True).order_by("orden", "nombre")
    activos = Activo.objects.filter(activo=True).order_by("nombre", "codigo")[:150]

    if request.method == "POST":
        sucursal_id = request.POST.get("sucursal")
        categoria_id = request.POST.get("categoria")
        foto = request.FILES.get("foto_evidencia")
        titulo = (request.POST.get("titulo") or "").strip()
        descripcion = (request.POST.get("descripcion") or "").strip()

        if not all([sucursal_id, categoria_id, titulo, descripcion, foto]):
            messages.error(request, "Completa sucursal, categoría, título, descripción y foto de evidencia.")
        elif not sucursales.filter(pk=sucursal_id).exists():
            messages.error(request, "No tienes permiso para reportar fallas en esa sucursal.")
        else:
            reporte = ReporteFalla.objects.create(
                sucursal_id=sucursal_id,
                categoria_id=categoria_id,
                activo_relacionado_id=request.POST.get("activo_relacionado") or None,
                area=request.POST.get("area") or ReporteFalla.AREA_GENERAL,
                titulo=titulo,
                descripcion=descripcion,
                prioridad=request.POST.get("prioridad") or ReporteFalla.PRIORIDAD_MEDIA,
                foto_evidencia=foto,
                latitud=request.POST.get("latitud") or None,
                longitud=request.POST.get("longitud") or None,
                reportado_por=request.user,
            )
            BitacoraFalla.objects.create(
                reporte=reporte,
                usuario=request.user,
                estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
                comentario="Reporte creado desde el ERP.",
            )
            messages.success(request, f"Reporte de falla #{reporte.id} registrado correctamente.")
            return redirect("fallas:pwa-mis-reportes")

    grupos_lower = {g.lower() for g in _group_names(request.user)}
    if "produccion" in grupos_lower:
        area_default = ReporteFalla.AREA_PRODUCCION
    elif "ventas" in grupos_lower:
        area_default = ReporteFalla.AREA_VENTAS
    else:
        area_default = ReporteFalla.AREA_GENERAL

    return render(
        request,
        "fallas/reporte_form.html",
        {
            "can_dashboard": can_view_submodule(request.user, "fallas", "dashboard"),
            "es_dg": request.user.is_superuser or request.user.groups.filter(name__in=["compras_logistica", "dg"]).exists(),
            "sucursales": sucursales,
            "categorias": categorias,
            "activos": activos,
            "areas": ReporteFalla.AREAS,
            "prioridades": ReporteFalla.PRIORIDAD,
            "area_default": area_default,
        },
    )


@login_required
def pwa_editar_reporte(request, pk):
    if not can_view_submodule(request.user, "fallas", "mis_reportes"):
        raise PermissionDenied

    reporte = (
        ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por")
        .filter(pk=pk)
        .first()
    )
    if not reporte or not _puede_modificar_reporte_propio(reporte, request.user):
        raise PermissionDenied

    sucursales = _sucursales_disponibles_para_reporte(request.user)
    categorias = CategoriaFalla.objects.filter(activo=True).order_by("orden", "nombre")
    activos = Activo.objects.filter(activo=True).order_by("nombre", "codigo")[:150]

    if request.method == "POST":
        sucursal_id = request.POST.get("sucursal")
        categoria_id = request.POST.get("categoria")
        titulo = (request.POST.get("titulo") or "").strip()
        descripcion = (request.POST.get("descripcion") or "").strip()

        if not all([sucursal_id, categoria_id, titulo, descripcion]):
            messages.error(request, "Completa sucursal, categoría, título y descripción.")
        elif not sucursales.filter(pk=sucursal_id).exists():
            messages.error(request, "No tienes permiso para usar esa sucursal.")
        else:
            reporte.sucursal_id = sucursal_id
            reporte.categoria_id = categoria_id
            reporte.activo_relacionado_id = request.POST.get("activo_relacionado") or None
            reporte.area = request.POST.get("area") or ReporteFalla.AREA_GENERAL
            reporte.titulo = titulo
            reporte.descripcion = descripcion
            reporte.prioridad = request.POST.get("prioridad") or ReporteFalla.PRIORIDAD_MEDIA
            reporte.latitud = request.POST.get("latitud") or None
            reporte.longitud = request.POST.get("longitud") or None
            foto = request.FILES.get("foto_evidencia")
            if foto:
                reporte.foto_evidencia = foto
            reporte.save()
            BitacoraFalla.objects.create(
                reporte=reporte,
                usuario=request.user,
                estatus_anterior=ReporteFalla.ESTATUS_ABIERTO,
                estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
                comentario="Reporte editado por el usuario que lo levantó.",
            )
            messages.success(request, f"Reporte de falla #{reporte.id} actualizado correctamente.")
            return redirect("fallas:pwa-mis-reportes")

    return render(
        request,
        "fallas/reporte_form.html",
        {
            "es_dg": request.user.is_superuser or request.user.groups.filter(name__in=["compras_logistica", "dg"]).exists(),
            "sucursales": sucursales,
            "categorias": categorias,
            "activos": activos,
            "areas": ReporteFalla.AREAS,
            "prioridades": ReporteFalla.PRIORIDAD,
            "reporte": reporte,
            "modo_edicion": True,
        },
    )


@login_required
def pwa_eliminar_reporte(request, pk):
    if request.method != "POST":
        return redirect("fallas:pwa-mis-reportes")
    if not can_view_submodule(request.user, "fallas", "mis_reportes"):
        raise PermissionDenied

    reporte = ReporteFalla.objects.filter(pk=pk).first()
    if not reporte or not _puede_modificar_reporte_propio(reporte, request.user):
        raise PermissionDenied

    reporte_id = reporte.id
    reporte.delete()
    messages.success(request, f"Reporte de falla #{reporte_id} eliminado correctamente.")
    return redirect("fallas:pwa-mis-reportes")


@login_required
def pwa_mis_reportes(request):
    if not can_view_submodule(request.user, "fallas", "mis_reportes"):
        raise PermissionDenied

    qs = ReporteFalla.objects.select_related("sucursal", "categoria", "reportado_por").order_by("-fecha_reporte")
    if request.user.is_superuser or _group_names(request.user) & GRUPOS_VER_TODO_FALLAS:
        qs = _filtrar_reportes_por_usuario(qs, request.user)
    else:
        qs = qs.filter(reportado_por=request.user)

    estatus = request.GET.get("estatus")
    prioridad = request.GET.get("prioridad")
    if estatus:
        qs = qs.filter(estatus=estatus)
    if prioridad:
        qs = qs.filter(prioridad=prioridad)

    reportes = Paginator(qs, 20).get_page(request.GET.get("page"))
    return render(
        request,
        "fallas/mis_reportes.html",
        {
            "can_dashboard": can_view_submodule(request.user, "fallas", "dashboard"),
            "es_dg": request.user.is_superuser or request.user.groups.filter(name__in=["compras_logistica", "dg"]).exists(),
            "reportes": reportes,
            "estatus_choices": ReporteFalla.ESTATUS,
            "prioridad_choices": ReporteFalla.PRIORIDAD,
            "estatus_actual": estatus or "",
            "prioridad_actual": prioridad or "",
        },
    )


@api_view(["GET"])
@authentication_classes([JWTAuthentication, TokenAuthentication, SessionAuthentication])
@permission_classes([EsComprasODG])
def usuarios_gestion(request):
    """Lista usuarios activos que pueden gestionar fallas."""

    User = get_user_model()
    qs = (
        User.objects.filter(groups__name__in=["compras_logistica", "dg"], is_active=True)
        .distinct()
        .order_by("first_name", "last_name", "username")
    )
    data = [
        {
            "id": user.pk,
            "username": user.username,
            "nombre": user.get_full_name() or user.username,
        }
        for user in qs
    ]
    return Response(data)
