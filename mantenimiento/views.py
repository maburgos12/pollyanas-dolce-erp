import re
from uuid import uuid4
from collections.abc import Mapping
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.conf import settings
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models.deletion import ProtectedError
from django.db.models import Count, Prefetch, Q
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.html import format_html
from django.utils import timezone
from django.views.decorators.cache import never_cache
from rest_framework import generics, status
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import BasePermission
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.tokens import RefreshToken

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento
from activos.services_ordenes import cambiar_estatus_orden, TransicionOrdenInvalida
from mantenimiento.models import SolicitudCancelacion, ProveedorServicio
from mantenimiento.evidence_validation import EvidenceValidationError, validate_evidence_files
from mantenimiento.services_access import (
    authorized_branch_ids,
    authorized_fallas,
    authorized_orders,
    authorized_repairs,
    authorized_unit_reports,
    authorized_unit_services,
    can_access_mantenimiento,
    can_view_costs,
    can_write_mantenimiento,
)
from mantenimiento.services_capturas_equipos import capturar_equipo, guardar_factura, CapturaEquipoError, validar_equipo
from mantenimiento.services_configuracion_planes import configurar_plan, planes_autorizados, activos_autorizados
from mantenimiento.services_history import continuidad_por_principal
from mantenimiento.services_vinculos import PROTECTED_MESSAGE
from core.access import can_manage_module, can_manage_submodule, can_view_module, can_view_submodule, is_admin_or_dg
from core.audit import log_event
from core.models import Sucursal, UserModuleAccess, sucursales_operativas
from fallas.models import BitacoraFalla, CategoriaFalla, EvidenciaSeguimientoFalla, ReporteFalla
from logistica.models import Repartidor, ReparacionUnidad, ReporteUnidad, ServicioRealizadoUnidad, TipoServicioUnidad, Unidad
from logistica.services_flota import validar_fecha_servicio_realizado
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

INSTALACION_CATEGORIAS = [
    "Instalaciones generales",
    "Plomería",
    "Pintura / obra civil",
    "Eléctrico",
    "Aire acondicionado",
    "Baños",
    "Impermeabilización",
]


class EsMantenimiento(BasePermission):
    GRUPOS = {"dg", "DG", "mantenimiento", "MANTENIMIENTO"}

    def has_permission(self, request, view):
        if request.method not in ("GET", "HEAD", "OPTIONS"):
            return _can_write_mantenimiento(request.user)
        return _can_access_mantenimiento(request.user)


def _can_access_mantenimiento(user) -> bool:
    return can_access_mantenimiento(user)


_can_write_mantenimiento = can_write_mantenimiento


def _require_mantenimiento(user):
    if is_admin_or_dg(user):
        return
    if not _can_access_mantenimiento(user):
        raise PermissionDenied("No tienes permisos para ver Mantenimiento.")


def _require_write_mantenimiento(user):
    if not _can_write_mantenimiento(user):
        raise PermissionDenied("No tienes permisos para modificar Mantenimiento.")


@never_cache
def pwa_sw(request):
    path = finders.find("mantenimiento/sw.js") or settings.BASE_DIR / "static" / "mantenimiento" / "sw.js"
    if not path or not Path(path).exists():
        raise Http404("Service worker de Mantenimiento no encontrado")
    with open(path, encoding="utf-8") as service_worker:
        return HttpResponse(service_worker.read(), content_type="application/javascript")


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


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _guardar_evidencias_falla(bitacora, archivos, user):
    creadas = []
    try:
        for archivo in archivos:
            if not archivo:
                continue
            evidencia = EvidenciaSeguimientoFalla(
                bitacora=bitacora, archivo=archivo,
                nombre=getattr(archivo, "name", "")[:255], subido_por=user,
            )
            creadas.append(evidencia)
            evidencia.save()
    except Exception:
        _eliminar_archivos_evidencias(creadas)
        raise
    return creadas


def _crear_reporte_falla_atomico(*, reporte_kwargs, usuario, comentario):
    if reporte_kwargs.get("activo_relacionado"):
        reporte_kwargs.setdefault("tipo_objetivo", ReporteFalla.OBJETIVO_EQUIPO)
        reporte_kwargs.setdefault("area_instalacion", "")
    else:
        reporte_kwargs.setdefault("tipo_objetivo", ReporteFalla.OBJETIVO_INSTALACION)
        reporte_kwargs.setdefault("area_instalacion", "Sucursal")
    reporte = ReporteFalla(**reporte_kwargs)
    try:
        with transaction.atomic():
            reporte.save()
            BitacoraFalla.objects.create(
                reporte=reporte, usuario=usuario, estatus_anterior="",
                estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO, comentario=comentario,
            )
    except Exception:
        if (reporte.foto_evidencia and reporte.foto_evidencia.name
                and getattr(reporte.foto_evidencia, "_committed", False)):
            reporte.foto_evidencia.delete(save=False)
        raise
    return reporte


def _eliminar_archivos_evidencias(evidencias):
    for evidencia in evidencias:
        if evidencia.archivo and getattr(evidencia.archivo, "_committed", False):
            evidencia.archivo.delete(save=False)


def _ensure_provider(nombre):
    nombre = (nombre or "").strip()
    if not nombre:
        return None
    ProveedorServicio.objects.get_or_create(nombre=nombre, defaults={"activo": True})
    proveedor, _created = Proveedor.objects.get_or_create(nombre=nombre, defaults={"activo": True})
    return proveedor


def _get_installation_asset(sucursal, categoria, proveedor_obj=None):
    categoria = categoria if categoria in INSTALACION_CATEGORIAS else INSTALACION_CATEGORIAS[0]
    nombre = f"{categoria} - {sucursal.nombre}"
    activo = Activo.objects.filter(nombre=nombre, sucursal=sucursal, categoria=categoria).first()
    if activo:
        updates = []
        if not activo.activo:
            activo.activo = True
            updates.append("activo")
        if proveedor_obj and activo.proveedor_mantenimiento_id != proveedor_obj.id:
            activo.proveedor_mantenimiento = proveedor_obj
            updates.append("proveedor_mantenimiento")
        if updates:
            activo.save(update_fields=updates)
        return activo
    return Activo.objects.create(
        nombre=nombre,
        categoria=categoria,
        ubicacion=sucursal.nombre,
        sucursal=sucursal,
        proveedor_mantenimiento=proveedor_obj,
        criticidad=Activo.CRITICIDAD_MEDIA,
        estado=Activo.ESTADO_OPERATIVO,
    )


def _asset_catalog_values(field):
    return list(
        Activo.objects.filter(activo=True)
        .exclude(**{f"{field}__exact": ""})
        .values_list(field, flat=True)
        .distinct()
        .order_by(field)
    )


def _create_asset_from_followup(data, sucursal, proveedor_obj=None):
    nombre = (data.get("activo_nombre_nuevo") or "").strip()
    if not nombre:
        return None
    categoria = (data.get("activo_categoria_nueva") or "").strip()
    ubicacion = (data.get("activo_ubicacion_nueva") or "").strip()
    categorias_validas = set(_asset_catalog_values("categoria"))
    ubicaciones_validas = set(_asset_catalog_values("ubicacion"))
    if not categoria or categoria not in categorias_validas:
        return None
    if ubicaciones_validas and ubicacion not in ubicaciones_validas:
        return None
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


def _get_proveedores_importables():
    from maestros.models import Insumo
    con_insumos = Insumo.objects.filter(
        proveedor_principal__isnull=False
    ).values_list("proveedor_principal_id", flat=True)
    ya_importados = ProveedorServicio.objects.values_list("nombre", flat=True)
    return list(
        Proveedor.objects.exclude(id__in=con_insumos)
        .filter(activo=True)
        .exclude(nombre__in=ya_importados)
        .order_by("nombre")
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
    ultima_bitacora = next(iter(getattr(reporte, "bitacora_reciente", [])), None)
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
        "foto_url": reporte.foto_evidencia.url if reporte.foto_evidencia else "",
        "reportado_por": reporte.reportado_por.get_full_name() or reporte.reportado_por.username,
        "ultimo_avance": ultima_bitacora.comentario if ultima_bitacora else "",
        "ultimo_avance_fecha": ultima_bitacora.timestamp if ultima_bitacora else None,
        "bitacora_total": getattr(reporte, "bitacora_total", 0),
        "fecha": reporte.fecha_reporte,
        "proveedor": reporte.proveedor_servicio,
        "costo_estimado": reporte.costo_estimado,
        "costo_real": reporte.costo_real,
        "dias_abierto": dias,
        "semaforo": _semaforo(dias),
        "asignado": bool(getattr(reporte, "asignado_a_id", None)),
        "duplicados_total": getattr(reporte, "duplicados_total", 0),
        "constataciones_total": getattr(reporte, "constataciones_total", 0),
        "primera_constatacion": getattr(reporte, "primera_constatacion", None),
        "ultima_constatacion": getattr(reporte, "ultima_constatacion", None),
    }


def _branch_order_item(orden):
    dias = _dias_abierto(orden.creado_en)
    proveedor = orden.proveedor_servicio.nombre if orden.proveedor_servicio_id else orden.responsable
    creado_por = ""
    if orden.creado_por_id:
        creado_por = orden.creado_por.get_full_name() or orden.creado_por.username
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
        "fecha_programada": orden.fecha_programada,
        "proveedor": proveedor,
        "costo_estimado": None,
        "costo_real": orden.costo_total,
        "dias_abierto": dias,
        "semaforo": _semaforo(dias),
        "asignado": bool(proveedor),
        "reportado_por": creado_por,
        "ultimo_avance": orden.nota_trabajo,
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
        "duplicados_total": getattr(reporte, "duplicados_total", 0),
        "reafirmaciones_total": getattr(reporte, "reafirmaciones_total", 0),
    }


def _branch_fallas(queryset, *, user, limit=80):
    queryset = (
        queryset.filter(duplicado_de__isnull=True)
        .annotate(
            duplicados_total=Count(
                "duplicados",
                filter=Q(
                    duplicados__pk__in=authorized_fallas(user).values("pk")
                ),
            )
        )
        .select_related("sucursal", "categoria", "activo_relacionado", "reportado_por")
        .prefetch_related(
            Prefetch(
                "bitacora",
                queryset=BitacoraFalla.objects.select_related("usuario").order_by("-timestamp"),
                to_attr="bitacora_reciente",
            )
        )
        .order_by("-fecha_reporte")
    )
    if limit is not None:
        queryset = queryset[:limit]
    fallas = list(queryset)
    continuidad = continuidad_por_principal(
        [falla.pk for falla in fallas], user=user
    )
    for falla in fallas:
        falla.bitacora_total = len(getattr(falla, "bitacora_reciente", []))
        metrics = continuidad.get(falla.pk, {})
        falla.constataciones_total = metrics.get("constataciones_total", 0)
        falla.primera_constatacion = metrics.get("primera_constatacion")
        falla.ultima_constatacion = metrics.get("ultima_constatacion")
    return fallas


def _unified_items(origen, user):
    items = []
    if origen in ("", "sucursales"):
        fallas_queryset = authorized_fallas(user)
        fallas = _branch_fallas(
            fallas_queryset.filter(estatus__in=_branch_statuses()), user=user
        )
        orders_queryset = authorized_orders(user)
        ordenes = (
            orders_queryset.filter(estatus__in=_order_open_statuses())
            .select_related("activo_ref", "activo_ref__sucursal", "creado_por", "proveedor_servicio")
            .order_by("-creado_en")[:80]
        )
        items.extend(_branch_falla_item(row) for row in fallas)
        items.extend(_branch_order_item(row) for row in ordenes)
    if origen in ("", "logistica"):
        reportes_queryset = authorized_unit_reports(user)
        reportes = (
            reportes_queryset.filter(estatus__in=_unit_open_statuses())
            .filter(duplicado_de__isnull=True)
            .annotate(
                duplicados_total=Count("duplicados", distinct=True),
                reafirmaciones_total=Count("reafirmaciones", distinct=True),
            )
            .select_related("unidad", "repartidor__user")
            .order_by("-fecha_reporte")[:80]
        )
        items.extend(_logistica_item(row) for row in reportes)
    return sorted(items, key=lambda item: item["fecha"], reverse=True)


def _unified_counts(user):
    items = _unified_items("", user)
    return {
        "sucursales": sum(1 for item in items if item["origen"] == "sucursales"),
        "logistica": sum(1 for item in items if item["origen"] == "logistica"),
    }


def _dashboard_origin_items(items):
    """Separate CEDIS for the desktop board without changing mobile origins."""
    falla_ids = [item["id"] for item in items if item["tipo"] == "falla"]
    order_ids = [item["id"] for item in items if item["tipo"] == "orden"]
    cedis_uids = {
        f"falla:{pk}" for pk in ReporteFalla.objects.filter(
            pk__in=falla_ids, sucursal__codigo="CEDIS"
        ).values_list("pk", flat=True)
    }
    cedis_uids.update(
        f"orden:{pk}" for pk in OrdenMantenimiento.objects.filter(
            pk__in=order_ids, activo_ref__sucursal__codigo="CEDIS"
        ).values_list("pk", flat=True)
    )
    return [dict(item, origen_tablero="cedis" if item["uid"] in cedis_uids else item["origen"])
            for item in items]


def _item_stage(item):
    estatus = str(item.get("estatus") or "").lower()
    proveedor = bool(item.get("proveedor"))
    costo_estimado = item.get("costo_estimado") is not None
    if item.get("tipo") == "orden" and estatus == "pendiente" and item.get("fecha_programada"):
        if item["fecha_programada"] > timezone.localdate():
            return "programado"
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


def _datos_captura_equipo(serializer, usuario, repetida):
    data = serializer.data
    if repetida and not can_view_costs(usuario):
        data = {key: value for key, value in data.items() if key not in {"costo_repuestos", "costo_mano_obra", "costo_otros", "costo_total"}}
    return data


class OrdenMantenimientoListCreateView(generics.ListCreateAPIView):
    authentication_classes = AUTH
    permission_classes = [EsMantenimiento]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(_datos_captura_equipo(serializer, request.user, serializer.captura_repetida), status=200 if serializer.captura_repetida else 201)

    def get_serializer_class(self):
        if self.request.method == "POST":
            return OrdenMantenimientoCreateSerializer
        return OrdenMantenimientoListSerializer

    def get_queryset(self):
        qs = OrdenMantenimiento.objects.select_related(
            "activo_ref", "activo_ref__sucursal", "creado_por", "responsable_usuario", "ejecutado_por"
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
                | Q(responsable_usuario__first_name__icontains=q)
                | Q(responsable_usuario__last_name__icontains=q)
                | Q(responsable_usuario__username__icontains=q)
                | Q(activo_ref__nombre__icontains=q)
                | Q(activo_ref__codigo__icontains=q)
                | Q(activo_ref__sucursal__nombre__icontains=q)
            )
        return qs


class OrdenMantenimientoDetailView(generics.RetrieveAPIView):
    authentication_classes = AUTH
    permission_classes = [EsMantenimiento]
    queryset = OrdenMantenimiento.objects.select_related(
        "activo_ref", "activo_ref__sucursal", "creado_por", "responsable_usuario", "ejecutado_por"
    ).prefetch_related("bitacora__usuario")
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
        qs = ServicioRealizadoUnidad.objects.vigentes().select_related("unidad", "tipo_servicio").order_by("-fecha_servicio", "-id")
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
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def catalogos_movil(request):
    responsable_ids = UserModuleAccess.objects.filter(
        module__startswith="mantenimiento",
        access="manage",
    ).values_list("user_id", flat=True)
    responsables = get_user_model().objects.filter(
        Q(id__in=responsable_ids) | Q(id=request.user.id),
        is_active=True,
    ).distinct().order_by("first_name", "last_name", "username")
    author_ids = {request.user.id}
    author_ids.update(
        authorized_fallas(request.user)
        .exclude(reportado_por_id__isnull=True)
        .values_list("reportado_por_id", flat=True)
    )
    author_ids.update(
        authorized_orders(request.user)
        .exclude(creado_por_id__isnull=True)
        .values_list("creado_por_id", flat=True)
    )
    author_ids.update(
        authorized_unit_reports(request.user)
        .exclude(repartidor__user_id__isnull=True)
        .values_list("repartidor__user_id", flat=True)
    )
    for queryset in (authorized_repairs(request.user), authorized_unit_services(request.user)):
        author_ids.update(
            queryset.exclude(registrado_por_id__isnull=True).values_list("registrado_por_id", flat=True)
        )
    autores = get_user_model().objects.filter(id__in=author_ids).order_by(
        "first_name",
        "last_name",
        "username",
    )
    return Response(
        {
            "responsables_mantenimiento": [
                {"id": user.id, "nombre": user.get_full_name() or user.username}
                for user in responsables
            ],
            "autores_mantenimiento": [
                {
                    "id": user.id,
                    "nombre": user.get_full_name() or user.username,
                    "activo": user.is_active,
                }
                for user in autores
            ],
            "categorias_falla": [
                {"id": categoria.id, "nombre": categoria.nombre}
                for categoria in CategoriaFalla.objects.filter(activo=True).order_by("orden", "nombre")
            ],
            "areas_falla": [{"value": value, "label": label} for value, label in ReporteFalla.AREAS],
            "prioridades_falla": [{"value": value, "label": label} for value, label in ReporteFalla.PRIORIDAD],
            "tipos_unidad": [{"value": value, "label": label} for value, label in ReporteUnidad.TIPO_CHOICES],
            "severidades_unidad": [{"value": value, "label": label} for value, label in ReporteUnidad.SEVERIDAD_CHOICES],
            "instalacion_categorias": INSTALACION_CATEGORIAS,
        }
    )


@api_view(["GET", "POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def proveedores_servicio(request):
    if request.method == "POST":
        nombre = (request.data.get("nombre") or "").strip()
        if not nombre:
            return Response({"nombre": "El nombre del proveedor es obligatorio."}, status=400)
        proveedor, _created = ProveedorServicio.objects.get_or_create(nombre=nombre, defaults={"activo": True})
        for field in ["contacto", "telefono", "especialidad", "notas"]:
            if field in request.data:
                setattr(proveedor, field, (request.data.get(field) or "").strip())
        proveedor.activo = request.data.get("activo", True) not in [False, "false", "0", 0]
        proveedor.save()
        return Response(_proveedor_payload(proveedor), status=201)
    data = [
        _proveedor_payload(proveedor)
        for proveedor in ProveedorServicio.objects.order_by("nombre")
    ]
    return Response(data)


@api_view(["PATCH", "DELETE"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def proveedor_servicio_detalle(request, pk):
    proveedor = get_object_or_404(ProveedorServicio, pk=pk)
    if request.method == "DELETE":
        proveedor.delete()
        return Response(status=204)
    nombre = (request.data.get("nombre") or proveedor.nombre).strip()
    if not nombre:
        return Response({"nombre": "El nombre del proveedor es obligatorio."}, status=400)
    proveedor.nombre = nombre
    for field in ["contacto", "telefono", "especialidad", "notas"]:
        if field in request.data:
            setattr(proveedor, field, (request.data.get(field) or "").strip())
    if "activo" in request.data:
        proveedor.activo = request.data.get("activo") not in [False, "false", "0", 0]
    proveedor.save()
    return Response(_proveedor_payload(proveedor))


@api_view(["GET"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def proveedores_importables_movil(request):
    return Response([{"id": proveedor.id, "nombre": proveedor.nombre} for proveedor in _get_proveedores_importables()])


@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def importar_proveedores_movil(request):
    ids = request.data.get("proveedor_ids") or []
    importados = []
    for prov in Proveedor.objects.filter(id__in=ids, activo=True):
        proveedor, created = ProveedorServicio.objects.get_or_create(nombre=prov.nombre, defaults={"activo": True})
        if created:
            importados.append(_proveedor_payload(proveedor))
    return Response({"importados": importados, "total": len(importados)}, status=201)


@api_view(["GET", "POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def planes_movil(request):
    actor = get_user_model().objects.get(pk=request.user.pk)  # rrhh-allow-inactive-history: fresh capability check below
    if not can_access_mantenimiento(actor):
        raise PermissionDenied('No tienes permisos para ver Mantenimiento.')
    if request.method == "POST":
        plan, replay = configurar_plan(usuario=actor, operacion='plan_create', data=request.data,
            guardar=_guardar_plan_desde_data)
        return Response(_plan_payload(plan), status=200 if replay else 201)
    try:
        page = max(1, int(request.query_params.get('page', 1)))
        size = min(120, max(1, int(request.query_params.get('page_size', 120))))
    except (ValueError, TypeError):
        return Response({'error': 'La página debe ser un número válido.'}, status=400)
    assets = request.query_params.get('catalogo') == 'activos'
    qs = activos_autorizados(actor).order_by('codigo', 'pk') if assets else planes_autorizados(actor).filter(activo=True).order_by('proxima_ejecucion', 'id')
    count = qs.count()
    rows = qs[(page - 1) * size:page * size]
    items = [{'id': obj.pk, 'codigo': obj.codigo, 'nombre': obj.nombre} for obj in rows] if assets else [_plan_payload(plan) for plan in rows]
    return Response({'items': items, 'pagination': {'count': count, 'page': page, 'page_size': size,
        'has_next': page * size < count}, 'choices': {
        'tipos': [{'value': value, 'label': label} for value, label in PlanMantenimiento.TIPO_CHOICES],
        'estatus': [{'value': value, 'label': label} for value, label in PlanMantenimiento.ESTATUS_CHOICES]}})


@api_view(["PATCH", "DELETE"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def plan_movil_detalle(request, pk):
    plan, replay = configurar_plan(usuario=request.user,
        operacion='plan_delete' if request.method == 'DELETE' else 'plan_update',
        data=request.data, guardar=_guardar_plan_desde_data, plan_id=pk)
    return Response(status=204) if request.method == 'DELETE' else Response(_plan_payload(plan))



@api_view(["GET"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def cancelaciones_movil(request):
    if not _puede_eliminar(request.user):
        return Response({"puede_resolver": False, "items": []})
    solicitudes = SolicitudCancelacion.objects.filter(
        estatus=SolicitudCancelacion.ESTATUS_PENDIENTE
    ).select_related("solicitado_por")
    return Response({"puede_resolver": True, "items": [_cancelacion_payload(s) for s in solicitudes]})


@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def resolver_cancelacion_movil(request, solicitud_id):
    if not _puede_eliminar(request.user):
        raise PermissionDenied("Solo DG puede resolver solicitudes de cancelación.")
    solicitud = get_object_or_404(
        SolicitudCancelacion,
        pk=solicitud_id,
        estatus=SolicitudCancelacion.ESTATUS_PENDIENTE,
    )
    accion = (request.data.get("accion") or "").strip().lower()
    if accion not in {"aprobar", "rechazar"}:
        return Response({"error": "Acción no válida."}, status=400)
    try:
        eliminado = _resolver_cancelacion_obj(
            solicitud, request.user, accion, (request.data.get("notas_resolucion") or "").strip(),
        )
    except ProtectedError:
        return Response({"error": PROTECTED_MESSAGE}, status=400)
    return Response({"ok": True, "estatus": solicitud.estatus, "eliminado": eliminado})


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
    items = _unified_items(origen, request.user)
    return Response(
        {
            "origen": origen or "todos",
            "counts": _unified_counts(request.user),
            "items": items,
        }
    )


def _fecha_plan_payload(fecha, today):
    if not fecha:
        return {"fecha": None, "dias": None, "estado": "sin_fecha"}
    dias = (fecha - today).days
    if dias < 0:
        estado = "vencido"
    elif dias <= 7:
        estado = "urgente"
    else:
        estado = "programado"
    return {"fecha": fecha.isoformat(), "dias": dias, "estado": estado}


def _proveedor_payload(proveedor):
    return {
        "id": proveedor.id,
        "nombre": proveedor.nombre,
        "contacto": proveedor.contacto,
        "telefono": proveedor.telefono,
        "especialidad": proveedor.especialidad,
        "notas": proveedor.notas,
        "activo": proveedor.activo,
    }


def _plan_payload(plan, today=None):
    today = today or timezone.localdate()
    return {
        "id": plan.id,
        "activo_id": plan.activo_ref_id,
        "activo": plan.activo_ref.nombre,
        "codigo": plan.activo_ref.codigo,
        "sucursal": plan.activo_ref.sucursal.nombre if plan.activo_ref.sucursal_id else "",
        "nombre": plan.nombre,
        "tipo": plan.tipo,
        "tipo_display": plan.get_tipo_display(),
        "estatus": plan.estatus,
        "estatus_display": plan.get_estatus_display(),
        "frecuencia_dias": plan.frecuencia_dias,
        "tolerancia_dias": plan.tolerancia_dias,
        "ultima_ejecucion": plan.ultima_ejecucion.isoformat() if plan.ultima_ejecucion else "",
        "proxima_ejecucion": plan.proxima_ejecucion.isoformat() if plan.proxima_ejecucion else "",
        "responsable": plan.responsable,
        "instrucciones": plan.instrucciones,
        "activo_plan": plan.activo,
        "revision_en": plan.actualizado_en.isoformat(),
        **_fecha_plan_payload(plan.proxima_ejecucion, today),
    }


def _cancelacion_payload(solicitud):
    solicitado = ""
    if solicitud.solicitado_por_id:
        solicitado = solicitud.solicitado_por.get_full_name() or solicitud.solicitado_por.username
    return {
        "id": solicitud.id,
        "tipo": solicitud.tipo,
        "tipo_display": solicitud.get_tipo_display(),
        "referencia": solicitud.referencia,
        "motivo": solicitud.motivo,
        "estatus": solicitud.estatus,
        "estatus_display": solicitud.get_estatus_display(),
        "solicitado_por": solicitado,
        "creado_en": timezone.localtime(solicitud.creado_en).strftime("%d/%m/%Y %H:%M"),
    }


def _guardar_plan_desde_data(plan, data):
    from django.utils.dateparse import parse_date as _pd

    nombre = (data.get("nombre") or "").strip()
    if not nombre:
        return "El nombre del plan es obligatorio."
    tipo = (data.get("tipo") or PlanMantenimiento.TIPO_PREVENTIVO).strip().upper()
    estatus = (data.get("estatus") or PlanMantenimiento.ESTATUS_ACTIVO).strip().upper()
    plan.nombre = nombre
    plan.tipo = tipo if tipo in {value for value, _label in PlanMantenimiento.TIPO_CHOICES} else PlanMantenimiento.TIPO_PREVENTIVO
    plan.estatus = estatus if estatus in {value for value, _label in PlanMantenimiento.ESTATUS_CHOICES} else PlanMantenimiento.ESTATUS_ACTIVO
    plan.frecuencia_dias = max(1, _safe_int(data.get("frecuencia_dias"), default=30))
    plan.tolerancia_dias = max(0, _safe_int(data.get("tolerancia_dias"), default=0))
    plan.responsable = (data.get("responsable") or "").strip()
    plan.instrucciones = (data.get("instrucciones") or "").strip()
    ultima = _pd((data.get("ultima_ejecucion") or "").strip())
    proxima = _pd((data.get("proxima_ejecucion") or "").strip())
    if ultima:
        plan.ultima_ejecucion = ultima
    if proxima:
        plan.proxima_ejecucion = proxima
    elif ultima and plan.frecuencia_dias:
        plan.recompute_next_date()
    plan.activo = True
    return ""


@transaction.atomic
def _resolver_cancelacion_obj(solicitud, user, accion, notas=""):
    locked = get_object_or_404(SolicitudCancelacion.objects.select_for_update(), pk=solicitud.pk,
                               estatus=SolicitudCancelacion.ESTATUS_PENDIENTE)
    eliminado = False
    if accion == "aprobar":
        model_map = {
            SolicitudCancelacion.TIPO_FALLA: ReporteFalla,
            SolicitudCancelacion.TIPO_UNIDAD: ReporteUnidad,
            SolicitudCancelacion.TIPO_ORDEN: OrdenMantenimiento,
        }
        model = model_map.get(locked.tipo)
        obj = model.objects.select_for_update().filter(pk=locked.objeto_id).first() if model else None
        if obj:
            obj.delete()
            eliminado = True
        locked.estatus = SolicitudCancelacion.ESTATUS_APROBADA
    else:
        locked.estatus = SolicitudCancelacion.ESTATUS_RECHAZADA
    locked.resuelto_por = user
    locked.resuelto_en = timezone.now()
    locked.notas_resolucion = notas
    locked.save()
    solicitud.estatus = locked.estatus
    return eliminado


@api_view(["GET"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def resumen_movil(request):
    plan_actor = get_user_model().objects.filter(pk=request.user.pk, is_active=True).first()
    today = timezone.localdate()
    items = _unified_items("", request.user)
    summary = _dashboard_summary(items)
    planes = []
    plan_qs = planes_autorizados(plan_actor).filter(estatus=PlanMantenimiento.ESTATUS_ACTIVO, activo=True)
    plan_count = plan_qs.count()
    for plan in plan_qs.order_by("proxima_ejecucion", "id")[:30]:
        planes.append(
            {
                "id": plan.id,
                "tipo": "plan",
                "titulo": plan.nombre,
                "activo": plan.activo_ref.nombre,
                "codigo": plan.activo_ref.codigo,
                "sucursal": plan.activo_ref.sucursal.nombre if plan.activo_ref.sucursal_id else "",
                "responsable": plan.responsable,
                "instrucciones": plan.instrucciones,
                **_fecha_plan_payload(plan.proxima_ejecucion, today),
            }
        )

    servicios = []
    vistos = set()
    servicios_qs = (
        ServicioRealizadoUnidad.objects.vigentes().filter(proxima_fecha__isnull=False)
        .select_related("unidad", "unidad__sucursal", "tipo_servicio")
        .order_by("proxima_fecha", "id")
    )
    for servicio in servicios_qs:
        key = (servicio.unidad_id, servicio.tipo_servicio_id)
        if key in vistos:
            continue
        vistos.add(key)
        servicios.append(
            {
                "id": servicio.id,
                "tipo": "servicio_flota",
                "titulo": servicio.tipo_servicio.nombre,
                "activo": servicio.unidad.descripcion,
                "codigo": servicio.unidad.codigo,
                "sucursal": servicio.unidad.sucursal.nombre if servicio.unidad.sucursal_id else "",
                "responsable": servicio.proveedor,
                "instrucciones": servicio.notas,
                "proximos_km": servicio.proximos_km,
                **_fecha_plan_payload(servicio.proxima_fecha, today),
            }
        )
        if len(servicios) >= 20:
            break

    agenda = sorted(
        planes + servicios,
        key=lambda row: (row["dias"] is None, row["dias"] if row["dias"] is not None else 99999, row["titulo"]),
    )
    return Response(
        {
            "fecha": today.isoformat(),
            "summary": {
                "total": summary["total"],
                "fallas_activas": summary["fallas_activas"],
                "ordenes_abiertas": summary["ordenes_abiertas"],
                "reparaciones_flota": summary["reparaciones_flota"],
                "criticas": summary["criticas"],
                "rojos": summary["rojos"],
                "sin_asignar": summary["sin_asignar"],
                "tiempo_promedio": summary["tiempo_promedio"],
                "activos_registrados": summary["activos_registrados"],
                "unidades_activas": summary["unidades_activas"],
                "costo_30d": str(summary["costo_30d"]),
            },
            "agenda": agenda[:40],
            "planes_coverage": {"total": plan_count, "mostrados": sum(row['tipo'] == 'plan' for row in agenda[:40]),
                "parcial": plan_count > sum(row['tipo'] == 'plan' for row in agenda[:40])},
            "agenda_counts": {
                "vencidos": sum(1 for row in agenda if row["estado"] == "vencido"),
                "urgentes": sum(1 for row in agenda if row["estado"] == "urgente"),
                "programados": sum(1 for row in agenda if row["estado"] == "programado"),
            },
        }
    )


def _datos_ejecucion_plan(data):
    from django.utils.dateparse import parse_date
    if not isinstance(data, Mapping):
        raise CapturaEquipoError("Envía los datos de la ejecución como objeto.", 400)
    raw = str(data.get("fecha_ejecucion") or "").strip()
    try:
        fecha = parse_date(raw) if raw else None
    except ValueError:
        fecha = None
    if raw and fecha is None:
        raise CapturaEquipoError("Captura una fecha de ejecución válida.", 400)
    return dict(clave=data.get("clave_captura"), fecha=fecha,
        notas=str(data.get("notas") or "").strip(),
        adicional=str(data.get("intervencion_adicional") or "").lower() in {"true", "1", "on", "yes"},
        motivo_adicional=str(data.get("motivo_adicional") or "").strip())


def _error_ejecucion_plan(exc):
    payload = {"ok": False, "error": str(exc.detail),
        "toast": {"type": "error", "message": str(exc.detail), "persistent": True}}
    if hasattr(exc, "ordenes"):
        payload["ordenes_abiertas"] = exc.ordenes
        payload["error_code"] = "ordenes_plan_abiertas"
    return payload


@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def ejecutar_plan_movil(request, pk):
    branch_ids = authorized_branch_ids(request.user)
    plans = PlanMantenimiento.objects.filter(activo=True)
    if branch_ids is not None:
        plans = plans.filter(activo_ref__sucursal_id__in=branch_ids)
    plan = get_object_or_404(plans, pk=pk)
    from mantenimiento.services_planes import registrar_ejecucion_plan as ejecutar
    try:
        datos = _datos_ejecucion_plan(request.data)
        orden, repetida = ejecutar(usuario=request.user, plan_id=plan.pk, movil=True, **datos)
    except CapturaEquipoError as exc:
        return Response(_error_ejecucion_plan(exc), status=exc.status_code)
    plan.refresh_from_db()
    payload = OrdenMantenimientoListSerializer(orden).data
    current_actor = get_user_model().objects.filter(pk=request.user.pk, is_active=True).first()
    if datos["clave"] is not None and (current_actor is None or not can_view_costs(current_actor)):
        payload = {k: v for k, v in payload.items() if k not in {"costo_repuestos", "costo_mano_obra", "costo_otros", "costo_total"}}
    return Response({"ok": True, "orden": payload,
        "proxima_ejecucion": plan.proxima_ejecucion.isoformat() if plan.proxima_ejecucion else None},
        status=200 if repetida or datos["clave"] is None else 201)


@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def crear_falla_movil(request):
    branch_ids = authorized_branch_ids(request.user)
    sucursales = Sucursal.objects.filter(activa=True)
    if branch_ids is not None:
        sucursales = sucursales.filter(pk__in=branch_ids)
    sucursal = get_object_or_404(sucursales, pk=_safe_int(request.data.get("sucursal")))
    categoria = get_object_or_404(CategoriaFalla, pk=_safe_int(request.data.get("categoria")), activo=True)
    titulo = (request.data.get("titulo") or "").strip()
    descripcion = (request.data.get("descripcion") or "").strip()
    if not titulo or not descripcion:
        return Response({"error": "Título y descripción son obligatorios."}, status=400)
    foto = request.FILES.get("foto_evidencia")
    try:
        fotos = validate_evidence_files([foto] if foto else [], images_only=True)
    except EvidenceValidationError as exc:
        return Response({"error": "La foto inicial no es válida.", "evidencias": exc.errors}, status=400)
    activo_obj = None
    activo_id = _safe_int(request.data.get("activo_id"))
    if activo_id:
        activo_obj = Activo.objects.filter(pk=activo_id, activo=True, sucursal=sucursal).first()
        if not activo_obj:
            return Response({"error": "El activo no corresponde a la sucursal seleccionada."}, status=400)
    reporte = _crear_reporte_falla_atomico(reporte_kwargs=dict(
        sucursal=sucursal,
        categoria=categoria,
        area=(request.data.get("area") or ReporteFalla.AREA_GENERAL).strip(),
        titulo=titulo,
        descripcion=descripcion,
        prioridad=(request.data.get("prioridad") or ReporteFalla.PRIORIDAD_MEDIA).strip(),
        activo_relacionado=activo_obj,
        reportado_por=request.user,
        estatus=ReporteFalla.ESTATUS_ABIERTO,
        foto_evidencia=fotos[0] if fotos else None,
    ), usuario=request.user, comentario="Reporte creado desde app móvil de mantenimiento.")
    return Response(_branch_falla_item(reporte), status=201)


def _validar_costo_equipo(data):
    from rest_framework import serializers
    raw = data.get("costo_total")
    if raw in (None, ""):
        return
    try:
        serializers.DecimalField(max_digits=18, decimal_places=2, min_value=Decimal("0")).run_validation(raw)
    except serializers.ValidationError:
        raise CapturaEquipoError("Captura un importe válido, no negativo y con máximo dos decimales.", 400)


def _contenido_servicio(data, factura=None):
    campos = ("modo_servicio", "alcance", "sucursal_id", "activo_id", "fecha_objetivo", "proveedor_servicio",
              "responsable", "descripcion", "costo_total", "nota_trabajo", "tipo", "prioridad", "origen", "cerrar_servicio")
    contenido = {campo: str(data.get(campo) or "").strip() for campo in campos}
    contenido["costo_total"] = _parse_decimal(data.get("costo_total")) or Decimal("0")
    if factura:
        contenido["factura"] = factura
    return contenido


@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def crear_servicio_movil(request):
    from django.utils.dateparse import parse_date

    if not isinstance(request.data, Mapping):
        return Response({"error": "Envía un objeto con los datos de la captura."}, status=400)

    for field in ("modo_servicio", "alcance", "descripcion", "fecha_objetivo", "proveedor_servicio", "responsable", "nota_trabajo", "tipo", "prioridad"):
        if request.data.get(field) is not None and not isinstance(request.data.get(field), str):
            return Response({"error": f"El campo {field} debe ser texto."}, status=400)
    modo = (request.data.get("modo_servicio") or "realizado").strip().lower()
    alcance = (request.data.get("alcance") or "activo").strip().lower()
    descripcion = (request.data.get("descripcion") or "").strip()
    if modo not in {"realizado", "pendiente"}:
        modo = "realizado"
    if alcance == "flota":
        alcance = "unidad"
    if alcance not in {"activo", "unidad", "instalacion"} or not descripcion:
        return Response({"error": "Alcance y descripción son obligatorios."}, status=400)

    if alcance == "activo":
        _validar_costo_equipo(request.data)
        for field, choices in (("tipo", OrdenMantenimiento.TIPO_CHOICES), ("prioridad", OrdenMantenimiento.PRIORIDAD_CHOICES)):
            if request.data.get(field) and request.data[field].strip().upper() not in {value for value, label in choices}:
                return Response({"error": f"El campo {field} no es válido."}, status=400)
    branch_ids = authorized_branch_ids(request.user)
    fecha_objetivo = parse_date((request.data.get("fecha_objetivo") or "").strip())
    if not fecha_objetivo:
        fecha_objetivo = timezone.localdate()
    proveedor_nombre = (request.data.get("proveedor_servicio") or "").strip()
    responsable = (request.data.get("responsable") or "").strip() or proveedor_nombre
    nota_trabajo = (request.data.get("nota_trabajo") or "").strip()
    costo_total = _parse_decimal(request.data.get("costo_total")) or Decimal("0")

    if alcance == "unidad":
        unidades = Unidad.objects.filter(activa=True)
        if branch_ids is not None:
            unidades = unidades.filter(sucursal_id__in=branch_ids)
        unidad = get_object_or_404(unidades, pk=_safe_int(request.data.get("unidad_id")))
        tipo_servicio, _created = TipoServicioUnidad.objects.get_or_create(
            nombre=descripcion[:100],
            defaults={"tipo_intervalo": TipoServicioUnidad.INTERVALO_TIEMPO, "activo": True},
        )
        servicio = ServicioRealizadoUnidad.objects.create(
            unidad=unidad,
            tipo_servicio=tipo_servicio,
            fecha_servicio=timezone.localdate() if modo == "pendiente" else fecha_objetivo,
            proveedor=proveedor_nombre or responsable,
            costo=None if modo == "pendiente" else costo_total,
            notas=nota_trabajo or descripcion,
            registrado_por=request.user,
            proxima_fecha=fecha_objetivo if modo == "pendiente" else None,
        )
        return Response(ServicioListSerializer(servicio).data, status=201)

    sucursales = Sucursal.objects.filter(activa=True)
    if branch_ids is not None:
        sucursales = sucursales.filter(pk__in=branch_ids)
    sucursal = get_object_or_404(sucursales, pk=_safe_int(request.data.get("sucursal_id")))
    proveedor_obj = _ensure_provider(proveedor_nombre) if alcance == "instalacion" else None
    if alcance == "instalacion":
        activo_obj = _get_installation_asset(
            sucursal,
            (request.data.get("instalacion_categoria") or INSTALACION_CATEGORIAS[0]).strip(),
            proveedor_obj,
        )
    else:
        activo_obj = get_object_or_404(Activo, pk=_safe_int(request.data.get("activo_id")), activo=True, sucursal=sucursal)
    proveedor_instalacion = proveedor_obj

    def crear(archivos_nuevos):
        proveedor_obj = _ensure_provider(proveedor_nombre) if alcance == "activo" else proveedor_instalacion
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo_obj,
            tipo=(request.data.get("tipo") or OrdenMantenimiento.TIPO_CORRECTIVO).strip().upper(),
            prioridad=(request.data.get("prioridad") or OrdenMantenimiento.PRIORIDAD_MEDIA).strip().upper(),
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE if modo == "pendiente" else OrdenMantenimiento.ESTATUS_EN_PROCESO,
            fecha_programada=fecha_objetivo,
            fecha_inicio=None if modo == "pendiente" else fecha_objetivo,
            responsable=responsable,
            descripcion=descripcion,
            costo_otros=Decimal("0") if modo == "pendiente" else costo_total,
            origen=OrdenMantenimiento.ORIGEN_INICIATIVA if modo == "pendiente" else OrdenMantenimiento.ORIGEN_EMERGENCIA,
            nota_trabajo=nota_trabajo,
            proveedor_servicio=proveedor_obj,
            creado_por=request.user,
        )
        BitacoraMantenimiento.objects.create(
            orden=orden,
            usuario=request.user,
            accion="SERVICIO_PROGRAMADO" if modo == "pendiente" else "SERVICIO_REGISTRADO",
            comentario=nota_trabajo or descripcion,
        )
        return orden

    if alcance == "activo":
        orden, repetida = capturar_equipo(
            usuario=request.user, activo=activo_obj, operacion="servicio_pwa",
            clave=request.data.get("clave_captura"),
            contenido=_contenido_servicio(request.data), crear=crear,
        )
    else:
        orden, repetida = crear([]), False
    return Response(_datos_captura_equipo(OrdenMantenimientoListSerializer(orden), request.user, repetida), status=200 if repetida else 201)



@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def crear_reporte_unidad_movil(request):
    branch_ids = authorized_branch_ids(request.user)
    unidades = Unidad.objects.filter(activa=True)
    if branch_ids is not None:
        unidades = unidades.filter(sucursal_id__in=branch_ids)
    unidad = get_object_or_404(unidades, pk=_safe_int(request.data.get("unidad")))
    tipo = (request.data.get("tipo") or "").strip()
    severidad = (request.data.get("severidad") or ReporteUnidad.SEVERIDAD_INFORMATIVO).strip()
    descripcion = (request.data.get("descripcion") or "").strip()
    if tipo not in {value for value, _label in ReporteUnidad.TIPO_CHOICES} or not descripcion:
        return Response({"error": "Tipo y descripción son obligatorios."}, status=400)
    reporte = ReporteUnidad.objects.create(
        unidad=unidad,
        tipo=tipo,
        severidad=severidad,
        descripcion=descripcion,
        kilometraje=_safe_int(request.data.get("kilometraje")) or None,
        ip_reporte=request.META.get("REMOTE_ADDR"),
        estatus=ReporteUnidad.ESTATUS_ABIERTO,
        asignado_a=request.user,
        notas_compras="Reporte levantado desde app móvil de mantenimiento.",
    )
    log_event(
        request.user,
        "CREATE",
        "logistica.ReporteUnidad",
        str(reporte.id),
        {"unidad": reporte.unidad.codigo, "tipo": reporte.tipo, "severidad": reporte.severidad, "origen": "mantenimiento_app"},
    )
    return Response(_logistica_item(reporte), status=201)


@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
@transaction.atomic
def actualizar_item(request, tipo, pk):
    tipo = (tipo or "").strip().lower()
    comentario = (request.data.get("comentario") or "").strip()
    proveedor = (request.data.get("proveedor_servicio") or "").strip()
    costo_estimado = _parse_decimal(request.data.get("costo_estimado"))
    costo_real = _parse_decimal(request.data.get("costo_real"))

    if tipo == "falla":
        try:
            evidencias = validate_evidence_files(request.FILES.getlist("evidencias_seguimiento"))
        except EvidenceValidationError as exc:
            return Response(
                {"error": "No se pudieron adjuntar las evidencias.", "evidencias": exc.errors},
                status=status.HTTP_400_BAD_REQUEST,
            )
        reporte = get_object_or_404(authorized_fallas(request.user), pk=pk)
        estatus_anterior = reporte.estatus
        estatus = (request.data.get("estatus") or reporte.estatus).strip()
        if estatus not in {value for value, _label in ReporteFalla.ESTATUS}:
            return Response({"error": "Estatus no válido."}, status=400)
        estatus_finales = {ReporteFalla.ESTATUS_RESUELTO, ReporteFalla.ESTATUS_CERRADO}
        finalizando = estatus_anterior not in estatus_finales and estatus in estatus_finales
        estimado_final = costo_estimado if costo_estimado is not None else reporte.costo_estimado
        confirmar_estimado = str(request.data.get("confirmar_costo_estimado") or "").strip().lower() in {
            "1",
            "true",
            "si",
            "sí",
            "yes",
            "on",
        }
        if finalizando and estimado_final is not None and costo_real is None and not confirmar_estimado:
            return Response(
                {"error": "Confirma el importe final o captura uno distinto antes de finalizar."},
                status=status.HTTP_400_BAD_REQUEST,
            )
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
            reporte.activo_relacionado = get_object_or_404(
                Activo, pk=activo_id, activo=True, sucursal_id=reporte.sucursal_id
            )
        else:
            nuevo_activo = _create_asset_from_followup(request.data, reporte.sucursal, proveedor_obj)
            if nuevo_activo:
                reporte.activo_relacionado = nuevo_activo
        if costo_estimado is not None:
            reporte.costo_estimado = costo_estimado
        if costo_real is not None:
            reporte.costo_real = costo_real
        elif finalizando and confirmar_estimado:
            reporte.costo_real = estimado_final
        reporte.save()
        # Si la falla se cierra, restaurar el activo vinculado a OPERATIVO
        if estatus in (ReporteFalla.ESTATUS_CERRADO, ReporteFalla.ESTATUS_RESUELTO):
            if reporte.activo_relacionado_id:
                activo_vinculado = reporte.activo_relacionado
                if activo_vinculado.estado == Activo.ESTADO_MANTENIMIENTO:
                    activo_vinculado.estado = Activo.ESTADO_OPERATIVO
                    activo_vinculado.save(update_fields=["estado", "actualizado_en"])
        bitacora = BitacoraFalla.objects.create(
            reporte=reporte,
            usuario=request.user,
            estatus_anterior=estatus_anterior if estatus != estatus_anterior else "",
            estatus_nuevo=estatus if estatus != estatus_anterior else "",
            comentario=comentario or "Seguimiento actualizado desde Mantenimiento.",
        )
        _guardar_evidencias_falla(bitacora, evidencias, request.user)
        return _update_response(request, _branch_falla_item(reporte))

    if tipo == "unidad":
        reporte = get_object_or_404(authorized_unit_reports(request.user), pk=pk)
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
        if estatus == ReporteUnidad.ESTATUS_CERRADO:
            # Los repetidos ligados a este no se trabajan por separado: si no los
            # arrastramos, se quedan abiertos para siempre.
            from logistica.services_reportes_repetidos import propagar_cierre_unidad

            propagar_cierre_unidad(reporte, estatus)
        return _update_response(request, _logistica_item(reporte))

    if tipo == "orden":
        orden = get_object_or_404(authorized_orders(request.user), pk=pk)
        estatus = (request.data.get("estatus") or "").strip().upper() or None
        if estatus and estatus not in {value for value, _label in OrdenMantenimiento.ESTATUS_CHOICES}:
            return Response({"error": "Estatus no válido."}, status=400)
        try:
            orden, _, _, bitacora = cambiar_estatus_orden(orden.id, estatus, request.user, source="mantenimiento")
        except TransicionOrdenInvalida as exc:
            return Response({"error": str(exc)}, status=400)
        if not proveedor and costo_real is None and not comentario:
            return _update_response(request, _branch_order_item(orden))
        if proveedor:
            _ensure_provider(proveedor)
            orden.responsable = proveedor
        if costo_real is not None:
            orden.costo_otros = costo_real
        orden.save(update_fields=["responsable", "costo_otros", "actualizado_en"])
        bitacora = bitacora or BitacoraMantenimiento(orden=orden, usuario=request.user)
        transicion = f"{bitacora.comentario} | " if bitacora.pk else ""
        bitacora.accion = "Seguimiento desde Mantenimiento"
        bitacora.comentario = transicion + (comentario or "Orden actualizada desde bandeja de mantenimiento.")
        bitacora.costo_adicional = costo_real or Decimal("0")
        bitacora.save()
        return _update_response(request, _branch_order_item(orden))

    return Response({"error": "Tipo no válido."}, status=400)


@login_required
def crear_falla(request):
    """Crea un ReporteFalla directamente desde el panel de mantenimiento (sin PWA)."""
    _require_write_mantenimiento(request.user)
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
    branch_ids = authorized_branch_ids(request.user)
    sucursales = Sucursal.objects.filter(activa=True)
    if branch_ids is not None:
        sucursales = sucursales.filter(pk__in=branch_ids)
    sucursal = sucursales.filter(pk=sucursal_id).first()
    categoria = CategoriaFalla.objects.filter(pk=categoria_id, activo=True).first()
    if not sucursal or not categoria:
        msg.error(request, "Sucursal o categoría no válida.")
        return redirect("mantenimiento:dashboard")

    activo_obj = None
    if activo_id:
        activo_obj = Activo.objects.filter(pk=activo_id, activo=True, sucursal=sucursal).first()
        if not activo_obj:
            msg.error(request, "El activo no pertenece a una sucursal autorizada.")
            return redirect("mantenimiento:dashboard")

    foto = request.FILES.get("foto_evidencia")
    try:
        validated_photos = validate_evidence_files([foto] if foto else [], images_only=True)
    except EvidenceValidationError as exc:
        for error in exc.errors:
            msg.error(request, error)
        return redirect("mantenimiento:dashboard")
    foto = validated_photos[0] if validated_photos else None
    reporte_kwargs = dict(
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
        reporte_kwargs["foto_evidencia"] = foto
    reporte = _crear_reporte_falla_atomico(
        reporte_kwargs=reporte_kwargs, usuario=request.user,
        comentario="Reporte creado desde panel de Mantenimiento.",
    )
    msg.success(request, f"Falla #{reporte.id} creada: {titulo}")
    return redirect("mantenimiento:dashboard")


def _respuesta_servicio(request, mensaje, *, orden=None, status_code=200):
    if (request.POST.get("alcance") or "activo").strip().lower() != "activo":
        from django.contrib import messages
        if orden:
            messages.success(request, mensaje)
        else:
            messages.error(request, mensaje)
        return redirect("mantenimiento:dashboard")
    if request.headers.get("x-requested-with") == "XMLHttpRequest" or "application/json" in request.headers.get("accept", ""):
        payload = {"ok": orden is not None, "orden_id": orden.pk if orden else None,
                   "toast": {"type": "success" if orden else "error", "message": mensaje, "persistent": orden is None}}
        if orden and (request.POST.get("alcance") or "activo") == "activo":
            payload.update(target="#ordenServicioResultado", html=str(format_html(
                '<div id="ordenServicioResultado" role="status" aria-live="polite"><p>{}</p>'
                '<button type="button" class="btn btn-secondary" onclick="nuevaCapturaEquipo()">Nueva captura</button></div>', mensaje,
            )))
        return JsonResponse(payload, status=status_code)
    if orden:
        from django.contrib import messages
        messages.success(request, mensaje)
        return redirect("mantenimiento:dashboard")
    request.captura_error = mensaje
    response = dashboard(request)
    response.status_code = status_code
    return response


@login_required
def crear_servicio_mantenimiento(request):
    """Registra un servicio realizado o programa una orden puntual sin reporte previo."""
    _require_write_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    from django.contrib import messages as msg
    from django.utils.dateparse import parse_date

    modo = (request.POST.get("modo_servicio") or "realizado").strip().lower()
    if modo not in {"realizado", "pendiente"}:
        modo = "realizado"

    alcance = (request.POST.get("alcance") or "activo").strip().lower()
    if alcance == "flota":
        alcance = "unidad"
    if alcance not in {"activo", "unidad", "instalacion"}:
        alcance = "activo"

    sucursal_id = _safe_int(request.POST.get("sucursal_id") or request.POST.get("sucursal"))
    activo_id = _safe_int(request.POST.get("activo_id"))
    unidad_id = _safe_int(request.POST.get("unidad_id"))
    instalacion_categoria = (request.POST.get("instalacion_categoria") or INSTALACION_CATEGORIAS[0]).strip()
    descripcion = (request.POST.get("descripcion") or "").strip()
    fecha_raw = (request.POST.get("fecha_objetivo") or "").strip()
    fecha_objetivo = parse_date(fecha_raw) if fecha_raw else None
    if not fecha_objetivo and modo == "realizado":
        fecha_objetivo = timezone.localdate()

    errores = []
    if alcance != "unidad" and not sucursal_id:
        errores.append("Selecciona una sucursal.")
    if alcance == "activo" and not activo_id:
        errores.append("Selecciona un activo o equipo.")
    if alcance == "unidad" and not unidad_id:
        errores.append("Selecciona una unidad logística.")
    if alcance == "instalacion" and instalacion_categoria not in INSTALACION_CATEGORIAS:
        errores.append("Selecciona un tipo de instalación válido.")
    if not descripcion:
        errores.append("Describe el servicio o pendiente.")
    if modo == "pendiente" and not fecha_objetivo:
        errores.append("Indica la fecha objetivo del servicio pendiente.")
    if errores:
        return _respuesta_servicio(request, " ".join(errores), status_code=400)

    if alcance == "activo":
        try:
            _validar_costo_equipo(request.POST)
        except CapturaEquipoError as exc:
            return _respuesta_servicio(request, str(exc.detail), status_code=exc.status_code)

    proveedor_nombre = (request.POST.get("proveedor_servicio") or "").strip()
    responsable = (request.POST.get("responsable") or "").strip() or proveedor_nombre
    nota_trabajo = (request.POST.get("nota_trabajo") or "").strip()
    costo_total = _parse_decimal(request.POST.get("costo_total")) or Decimal("0")
    cerrar_servicio = (request.POST.get("cerrar_servicio") or "").strip().lower() in {"1", "on", "true", "yes"}

    factura_archivo = request.FILES.get("factura_archivo")
    if factura_archivo and factura_archivo.size > 30 * 1024 * 1024:
        return _respuesta_servicio(request, "El archivo supera el límite de 30 MB.", status_code=400)

    if alcance == "unidad":
        unidades = Unidad.objects.filter(activa=True)
        branch_ids = authorized_branch_ids(request.user)
        if branch_ids is not None:
            unidades = unidades.filter(sucursal_id__in=branch_ids)
        unidad = get_object_or_404(unidades, pk=unidad_id)
        tipo_servicio, _created = TipoServicioUnidad.objects.get_or_create(
            nombre=descripcion[:100],
            defaults={
                "tipo_intervalo": TipoServicioUnidad.INTERVALO_TIEMPO,
                "activo": True,
                "notas": "Servicio puntual registrado desde mantenimiento.",
            },
        )
        servicio = ServicioRealizadoUnidad(
            unidad=unidad,
            tipo_servicio=tipo_servicio,
            fecha_servicio=fecha_objetivo if modo == "realizado" else timezone.localdate(),
            proveedor=proveedor_nombre or responsable,
            costo=costo_total if modo == "realizado" else None,
            archivo_factura=factura_archivo if modo == "realizado" else None,
            notas=nota_trabajo or descripcion,
            registrado_por=request.user,
        )
        servicio.save()
        if modo == "pendiente":
            servicio.proxima_fecha = fecha_objetivo
            servicio.save(update_fields=["proxima_fecha"])
            msg.success(request, f"Servicio de unidad programado: {unidad.codigo} · {fecha_objetivo:%d/%m/%Y}.")
        else:
            msg.success(request, f"Servicio de unidad registrado: {unidad.codigo}.")
        return redirect("mantenimiento:dashboard")

    branch_ids = authorized_branch_ids(request.user)
    sucursales = Sucursal.objects.filter(activa=True)
    if branch_ids is not None:
        sucursales = sucursales.filter(pk__in=branch_ids)
    sucursal = sucursales.filter(pk=sucursal_id).first()
    if not sucursal:
        return _respuesta_servicio(request, "Selecciona una sucursal válida.", status_code=400)

    proveedor_obj = _ensure_provider(proveedor_nombre) if alcance == "instalacion" else None
    if alcance == "instalacion":
        activo_obj = _get_installation_asset(sucursal, instalacion_categoria, proveedor_obj)
    else:
        activo_obj = get_object_or_404(Activo, pk=activo_id, activo=True)
        if activo_obj.sucursal_id != sucursal.id:
            return _respuesta_servicio(request, "El activo no pertenece a la sucursal seleccionada.", status_code=400)

    tipo_raw = (request.POST.get("tipo") or "").strip().upper()
    tipo_default = OrdenMantenimiento.TIPO_PREVENTIVO if modo == "pendiente" else OrdenMantenimiento.TIPO_CORRECTIVO
    tipo = tipo_raw if tipo_raw in {value for value, _label in OrdenMantenimiento.TIPO_CHOICES} else tipo_default
    prioridad_raw = (request.POST.get("prioridad") or "").strip().upper()
    prioridad = (
        prioridad_raw
        if prioridad_raw in {value for value, _label in OrdenMantenimiento.PRIORIDAD_CHOICES}
        else OrdenMantenimiento.PRIORIDAD_MEDIA
    )
    origen_raw = (request.POST.get("origen") or "").strip().upper()
    origen_default = OrdenMantenimiento.ORIGEN_INICIATIVA if modo == "pendiente" else OrdenMantenimiento.ORIGEN_EMERGENCIA
    origen = origen_raw if origen_raw in {value for value, _label in OrdenMantenimiento.ORIGEN_CHOICES} else origen_default

    if modo == "pendiente":
        estatus = OrdenMantenimiento.ESTATUS_PENDIENTE
        fecha_inicio = None
        fecha_cierre = None
        costo_total = Decimal("0")
    else:
        estatus = OrdenMantenimiento.ESTATUS_CERRADA if cerrar_servicio else OrdenMantenimiento.ESTATUS_EN_PROCESO
        fecha_inicio = fecha_objetivo
        fecha_cierre = fecha_objetivo if cerrar_servicio else None

    proveedor_instalacion = proveedor_obj

    def crear(archivos_nuevos):
        proveedor_obj = _ensure_provider(proveedor_nombre) if alcance == "activo" else proveedor_instalacion
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo_obj,
            tipo=tipo,
            prioridad=prioridad,
            estatus=estatus,
            fecha_programada=fecha_objetivo or timezone.localdate(),
            fecha_inicio=fecha_inicio,
            fecha_cierre=fecha_cierre,
            responsable=responsable,
            descripcion=descripcion,
            costo_otros=costo_total,
            origen=origen,
            nota_trabajo=nota_trabajo,
            proveedor_servicio=proveedor_obj,
            creado_por=request.user,
        )
        if factura_archivo:
            guardar_factura(orden, factura_archivo, archivos_nuevos)

        BitacoraMantenimiento.objects.create(
            orden=orden,
            usuario=request.user,
            accion="SERVICIO_PROGRAMADO" if modo == "pendiente" else "SERVICIO_REGISTRADO",
            comentario=nota_trabajo or descripcion,
        )

        return orden

    if alcance == "activo":
        try:
            orden, repetida = capturar_equipo(
                usuario=request.user, activo=activo_obj, operacion="servicio_web",
                clave=request.POST.get("clave_captura"),
                contenido=_contenido_servicio(request.POST, factura_archivo), crear=crear,
            )
        except CapturaEquipoError as exc:
            return _respuesta_servicio(request, str(exc.detail), status_code=exc.status_code)
    else:
        orden, repetida = crear([]), False
    mensaje = f"Servicio puntual programado: {orden.folio}." if modo == "pendiente" else f"Servicio registrado: {orden.folio}."
    return _respuesta_servicio(request, mensaje, orden=orden, status_code=200 if repetida else 201)

def crear_reporte_unidad(request):
    """Crea un ReporteUnidad desde Mantenimiento cuando el repartidor no lo capturó."""
    _require_mantenimiento(request.user)

    unidades = Unidad.objects.filter(activa=True).order_by("codigo")
    repartidores = Repartidor.objects.filter(user__is_active=True).select_related("user", "user__empleado_rrhh").order_by(
        "user__first_name", "user__username"
    )

    if request.method == "POST":
        unidad_id = (request.POST.get("unidad") or "").strip()
        tipo = (request.POST.get("tipo") or "").strip()
        severidad = (request.POST.get("severidad") or "").strip()
        descripcion = (request.POST.get("descripcion") or "").strip()
        kilometraje_raw = (request.POST.get("kilometraje") or "").strip()
        repartidor_id = (request.POST.get("repartidor") or "").strip()
        foto = request.FILES.get("foto")

        errors: dict[str, str] = {}
        if not unidad_id:
            errors["unidad"] = "Selecciona una unidad."
        if tipo not in {value for value, _label in ReporteUnidad.TIPO_CHOICES}:
            errors["tipo"] = "Tipo de reporte no válido."
        if severidad not in {value for value, _label in ReporteUnidad.SEVERIDAD_CHOICES}:
            errors["severidad"] = "Severidad no válida."
        if not descripcion:
            errors["descripcion"] = "La descripción es obligatoria."

        unidad = None
        if unidad_id and not errors.get("unidad"):
            unidad = Unidad.objects.filter(pk=unidad_id, activa=True).first()
            if not unidad:
                errors["unidad"] = "Unidad no encontrada."

        repartidor = None
        if repartidor_id:
            repartidor = Repartidor.objects.filter(pk=repartidor_id, user__is_active=True).first()
            if not repartidor:
                errors["repartidor"] = "Repartidor no encontrado."

        kilometraje = None
        if kilometraje_raw:
            try:
                kilometraje = int(kilometraje_raw)
            except ValueError:
                errors["kilometraje"] = "El kilometraje debe ser un número entero."
            else:
                if kilometraje < 0:
                    errors["kilometraje"] = "El kilometraje no puede ser negativo."

        if foto:
            allowed_content_types = {"image/jpeg", "image/png"}
            if foto.content_type not in allowed_content_types:
                errors["foto"] = "La evidencia debe ser una imagen JPG o PNG."
            elif foto.size > 10 * 1024 * 1024:
                errors["foto"] = "La evidencia no puede superar 10 MB."

        if not errors:
            from django.contrib import messages as msg

            reporte = ReporteUnidad.objects.create(
                unidad=unidad,
                repartidor=repartidor,
                tipo=tipo,
                severidad=severidad,
                descripcion=descripcion,
                kilometraje=kilometraje,
                foto=foto if foto else None,
                ip_reporte=request.META.get("REMOTE_ADDR"),
                estatus=ReporteUnidad.ESTATUS_ABIERTO,
                asignado_a=request.user,
                notas_compras="Reporte levantado desde Mantenimiento cuando no fue capturado en la app.",
            )
            log_event(
                request.user,
                "CREATE",
                "logistica.ReporteUnidad",
                str(reporte.id),
                {
                    "unidad": reporte.unidad.codigo,
                    "tipo": reporte.tipo,
                    "severidad": reporte.severidad,
                    "origen": "mantenimiento",
                },
            )
            msg.success(request, f"Reporte de unidad #{reporte.id} creado desde Mantenimiento.")
            return redirect("mantenimiento:dashboard")

        return render(
            request,
            "mantenimiento/reporte_unidad_form.html",
            {
                "unidades": unidades,
                "repartidores": repartidores,
                "tipo_choices": ReporteUnidad.TIPO_CHOICES,
                "severidad_choices": ReporteUnidad.SEVERIDAD_CHOICES,
                "errors": errors,
                "prev": {
                    "unidad": unidad_id,
                    "tipo": tipo,
                    "severidad": severidad,
                    "descripcion": descripcion,
                    "kilometraje": kilometraje_raw,
                    "repartidor": repartidor_id,
                },
            },
        )

    return render(
        request,
        "mantenimiento/reporte_unidad_form.html",
        {
            "unidades": unidades,
            "repartidores": repartidores,
            "tipo_choices": ReporteUnidad.TIPO_CHOICES,
            "severidad_choices": ReporteUnidad.SEVERIDAD_CHOICES,
            "errors": {},
            "prev": {},
        },
    )


@login_required
def dashboard(request):
    _require_mantenimiento(request.user)
    origen = (request.GET.get("origen") or "").strip().lower()
    if origen not in {"", "sucursales", "cedis", "logistica"}:
        return redirect("mantenimiento:dashboard")
    all_items = _dashboard_origin_items(_unified_items("", request.user))
    counts = {key: sum(item["origen_tablero"] == key for item in all_items)
              for key in ("sucursales", "cedis", "logistica")}
    counts["total"] = len(all_items)
    items = [item for item in all_items if not origen or item["origen_tablero"] == origen]
    requested_open = (request.GET.get("open") or "").strip()
    open_item_uid = requested_open if any(item["uid"] == requested_open for item in items) else ""
    display_items = list(items)
    if not open_item_uid:
        requested_match = re.fullmatch(r"falla:([1-9][0-9]*)", requested_open)
        if requested_match:
            requested = _branch_fallas(
                authorized_fallas(request.user).filter(pk=int(requested_match.group(1))),
                user=request.user,
                limit=1,
            )
            if requested:
                requested_item = _dashboard_origin_items([_branch_falla_item(requested[0])])[0]
                display_items.append(requested_item)
                open_item_uid = requested_item["uid"]
    provider_options = list(ProveedorServicio.objects.filter(activo=True).order_by("nombre")[:180])
    puede_crear_proveedor = _can_write_mantenimiento(request.user)
    proveedores_todos = list(ProveedorServicio.objects.order_by("nombre"))
    asset_options = Activo.objects.select_related("sucursal").filter(activo=True).order_by(
        "sucursal__nombre", "nombre", "codigo"
    )[:180]
    fleet_units = Unidad.objects.filter(activa=True).select_related("sucursal").order_by("descripcion", "codigo")
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
        plan.clave_captura = (request.POST.get("clave_captura") if getattr(request, "plan_captura_id", None) == plan.pk else str(uuid4()))
        plan.ordenes_abiertas = list(OrdenMantenimiento.objects.filter(plan_ref=plan, estatus__in=[OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO]))
        plan.dias_para_vencer = (plan.proxima_ejecucion - today).days if plan.proxima_ejecucion else None
        plan.vencido = plan.dias_para_vencer is not None and plan.dias_para_vencer < 0
        plan.urgente = plan.dias_para_vencer is not None and 0 <= plan.dias_para_vencer <= 7

    # Servicios de flota — todos con proxima_fecha pendiente, más reciente por unidad+tipo
    from django.db.models import Max
    ultimos_ids = (
        ServicioRealizadoUnidad.objects.vigentes().filter(proxima_fecha__isnull=False)
        .values("unidad_id", "tipo_servicio_id")
        .annotate(ultimo_id=Max("id"))
        .values_list("ultimo_id", flat=True)
    )
    servicios_flota = list(
        ServicioRealizadoUnidad.objects.vigentes().filter(id__in=ultimos_ids)
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
            "open_item_uid": open_item_uid,
            "kanban_columns": _kanban_columns(display_items),
            "summary": _dashboard_summary(items),
            "provider_options": provider_options,
            "puede_crear_proveedor": puede_crear_proveedor,
            "puede_escribir": puede_crear_proveedor,
            "asset_options": asset_options,
            "asset_categories": _asset_catalog_values("categoria"),
            "asset_locations": _asset_catalog_values("ubicacion"),
            "fleet_units": fleet_units,
            "sucursales_list": sucursales_list,
            "categorias_list": categorias_list,
            "origen": origen or "todos",
            "counts": counts,
            "estatus_fallas": ReporteFalla.ESTATUS,
            "estatus_unidad": ReporteUnidad.ESTATUS_CHOICES,
            "estatus_orden": OrdenMantenimiento.ESTATUS_CHOICES,
            "areas_falla": ReporteFalla.AREAS,
            "prioridades_falla": ReporteFalla.PRIORIDAD,
            "planes_proximos": planes_proximos,
            "plan_captura_error": getattr(request, "plan_captura_error", ""),
            "plan_captura_id": getattr(request, "plan_captura_id", None),
            "plan_captura_success": getattr(request, "plan_captura_success", None),
            "plan_captura_orden": getattr(request, "plan_captura_orden", None),
            "plan_ordenes_abiertas": getattr(request, "plan_ordenes_abiertas", []),
            "plan_captura_datos": dict(request.POST.items()) if getattr(request, "plan_captura_error", "") else {},
            "servicios_flota": servicios_flota,
            "solicitudes_cancelacion": solicitudes_cancelacion,
            "puede_eliminar": _puede_eliminar(request.user),
            "today": today,
            "clave_captura": request.POST.get("clave_captura") or str(uuid4()),
            "captura_error": getattr(request, "captura_error", ""),
            "captura_datos": dict(request.POST.items()) if getattr(request, "captura_error", "") else {},
            "plan_tipo_choices": PlanMantenimiento.TIPO_CHOICES,
            "plan_estatus_choices": PlanMantenimiento.ESTATUS_CHOICES,
            "orden_tipo_choices": OrdenMantenimiento.TIPO_CHOICES,
            "orden_prioridad_choices": OrdenMantenimiento.PRIORIDAD_CHOICES,
            "orden_origen_choices": OrdenMantenimiento.ORIGEN_CHOICES,
            "activos_para_plan": list(Activo.objects.select_related("sucursal").filter(activo=True).order_by("sucursal__nombre", "nombre")[:400]),
            "unidades_para_servicio": list(Unidad.objects.filter(activa=True).select_related("sucursal").order_by("descripcion", "codigo")),
            "instalacion_categorias": INSTALACION_CATEGORIAS,
            "proveedores_todos": proveedores_todos,
            "proveedores_sin_contacto": sum(
                1 for p in proveedores_todos if not p.telefono and not p.whatsapp
            ),
            "proveedores_importables": _get_proveedores_importables(),
        },
    )


@login_required
def eliminar_proveedor(request, pk):
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")
    from django.contrib import messages as msg
    prov = get_object_or_404(ProveedorServicio, pk=pk)
    nombre = prov.nombre
    prov.delete()
    msg.success(request, f"Proveedor '{nombre}' eliminado.")
    return redirect("mantenimiento:dashboard")


@api_view(["POST"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def alta_proveedor_seguimiento(request):
    """Alta de proveedor de servicio sin salir del seguimiento de fallas y reparaciones."""
    from fallas.serializers import AltaProveedorServicioSerializer

    from .services_proveedores import alta_proveedor_servicio

    serializer = AltaProveedorServicioSerializer(data=request.data)
    if not serializer.is_valid():
        return Response({"ok": False, "errors": serializer.errors, "toast": {
            "type": "error", "message": "Revisa los datos del proveedor.",
        }}, status=400)

    payload, codigo = alta_proveedor_servicio(serializer, request.user, "mantenimiento_seguimiento")
    return Response(payload, status=codigo)


@login_required
def gestionar_proveedor(request):
    """Crear o editar un ProveedorServicio desde el módulo de mantenimiento."""
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    from django.contrib import messages as msg

    action = (request.POST.get("action") or "crear").strip().lower()
    nombre = (request.POST.get("nombre") or "").strip()
    if not nombre:
        msg.error(request, "El nombre del proveedor es obligatorio.")
        return redirect("mantenimiento:dashboard")

    contacto = (request.POST.get("contacto") or "").strip()
    telefono = (request.POST.get("telefono") or "").strip()
    whatsapp = (request.POST.get("whatsapp") or "").strip()
    especialidad = (request.POST.get("especialidad") or "").strip()
    notas = (request.POST.get("notas") or "").strip()

    if action == "editar":
        prov_id = _safe_int(request.POST.get("proveedor_id"))
        prov = get_object_or_404(ProveedorServicio, pk=prov_id)
        activo = request.POST.get("activo", "1") != "0"
    else:
        if ProveedorServicio.objects.filter(nombre__iexact=nombre).exists():
            msg.warning(request, "Ya existe un proveedor de servicio con ese nombre.")
            return redirect("mantenimiento:dashboard")
        prov = ProveedorServicio()
        activo = True

    prov.nombre = nombre
    prov.contacto = contacto
    prov.telefono = telefono
    prov.whatsapp = whatsapp
    prov.especialidad = especialidad
    prov.notas = notas
    prov.activo = activo

    # Sin esto un dato más largo que la columna llega crudo a Postgres y revienta
    # con DataError (500), perdiendo todo lo que la usuaria ya había capturado.
    try:
        prov.full_clean()
    except ValidationError as exc:
        etiquetas = {f.name: f.verbose_name for f in ProveedorServicio._meta.get_fields()
                     if hasattr(f, "verbose_name")}
        detalle = "; ".join(f"{etiquetas.get(campo, campo).capitalize()}: {' '.join(errores)}"
                            for campo, errores in exc.message_dict.items())
        msg.error(request, f"No se guardó el proveedor. {detalle}")
        return redirect("mantenimiento:dashboard")

    prov.save()
    msg.success(request, f"Proveedor '{nombre}' {'actualizado' if action == 'editar' else 'creado'}.")
    return redirect("mantenimiento:dashboard")


@login_required
def pwa_mantenimiento(request):
    if not (is_admin_or_dg(request.user) or EsMantenimiento().has_permission(request, None)):
        raise PermissionDenied("No tienes permisos para usar Mantenimiento")
    return render(request, "mantenimiento/pwa.html")


@login_required
def marcar_duplicado(request, tipo, pk):
    """Liga un reporte repetido al que ya se está atendiendo (falla o unidad)."""
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")
    if not _can_write_mantenimiento(request.user):
        raise PermissionDenied("No tienes permisos para ligar reportes repetidos")

    from django.contrib import messages as msg

    from core.duplicados import DuplicadoInvalido

    tipo = (tipo or "").strip().lower()
    if tipo == "falla":
        from fallas.services_duplicados import marcar_duplicado as _vincular

        modelo, etiqueta = ReporteFalla, "Falla"
    elif tipo == "unidad":
        from logistica.services_reportes_repetidos import marcar_duplicado_unidad as _vincular

        modelo, etiqueta = ReporteUnidad, "Reporte de unidad"
    else:
        msg.error(request, "Tipo no válido.")
        return redirect("mantenimiento:dashboard")

    reporte = get_object_or_404(modelo, pk=pk)
    principal_id = (request.POST.get("principal_id") or "").strip()
    if not principal_id.isdigit():
        msg.error(request, "Selecciona el reporte que ya se está atendiendo.")
        return redirect("mantenimiento:dashboard")

    principal = modelo.objects.filter(pk=int(principal_id)).first()
    if principal is None:
        msg.error(request, "No se encontró el reporte principal seleccionado.")
        return redirect("mantenimiento:dashboard")

    try:
        destino = _vincular(reporte, principal, request.user)
    except DuplicadoInvalido as exc:
        msg.error(request, str(exc))
        return redirect("mantenimiento:dashboard")

    msg.success(
        request,
        f"{etiqueta} #{reporte.pk} ligado como repetido de #{destino.pk}. "
        "Sale de pendientes y se cerrará junto con él.",
    )
    return redirect("mantenimiento:dashboard")


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
        try:
            with transaction.atomic():
                obj = get_object_or_404(type(obj).objects.select_for_update(), pk=obj.pk)
                obj.delete()
        except ProtectedError:
            msg.error(request, PROTECTED_MESSAGE)
            return redirect("mantenimiento:dashboard")
        msg.success(request, f"{referencia} eliminado.")
        return redirect("mantenimiento:dashboard")

    # El motivo solo es obligatorio para la solicitud: el DG elimina directo y el
    # modal le oculta el campo, así que exigirlo antes dejaba un error sin campo visible.
    if not motivo:
        msg.error(request, "Debes indicar el motivo de cancelación.")
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

    if accion not in {"aprobar", "rechazar"}:
        msg.error(request, "Acción no válida.")
        return redirect("mantenimiento:dashboard")
    try:
        eliminado = _resolver_cancelacion_obj(solicitud, request.user, accion, notas)
    except ProtectedError:
        msg.error(request, PROTECTED_MESSAGE)
        return redirect("mantenimiento:dashboard")
    if accion == "aprobar":
        if eliminado:
            msg.success(request, f"Solicitud #{solicitud.id} aprobada. '{solicitud.referencia}' eliminado.")
        else:
            msg.warning(request, f"Solicitud #{solicitud.id} aprobada, pero el objeto ya no existía.")
    else:
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
def gestionar_tipo_servicio(request):
    """Crear o editar un TipoServicioUnidad desde mantenimiento."""
    _require_mantenimiento(request.user)
    if request.method != "POST":
        return redirect("mantenimiento:dashboard")

    from django.contrib import messages as msg

    action = (request.POST.get("action") or "crear").strip().lower()
    nombre = (request.POST.get("nombre") or "").strip()
    if not nombre:
        msg.error(request, "El nombre del tipo de servicio es obligatorio.")
        return redirect("mantenimiento:dashboard")

    tipo_intervalo = (request.POST.get("tipo_intervalo") or TipoServicioUnidad.INTERVALO_TIEMPO).strip().lower()
    if tipo_intervalo not in {TipoServicioUnidad.INTERVALO_KM, TipoServicioUnidad.INTERVALO_TIEMPO, TipoServicioUnidad.INTERVALO_AMBOS}:
        tipo_intervalo = TipoServicioUnidad.INTERVALO_TIEMPO

    intervalo_meses = _safe_int(request.POST.get("intervalo_meses")) or None
    intervalo_km = _safe_int(request.POST.get("intervalo_km")) or None
    notas = (request.POST.get("notas") or "").strip()

    if action == "editar":
        tipo_id = _safe_int(request.POST.get("tipo_id"))
        obj = get_object_or_404(TipoServicioUnidad, pk=tipo_id)
    else:
        obj = TipoServicioUnidad()

    obj.nombre = nombre
    obj.tipo_intervalo = tipo_intervalo
    obj.intervalo_meses = intervalo_meses
    obj.intervalo_km = intervalo_km
    obj.notas = notas
    obj.activo = True
    obj.save()

    verbo = "actualizado" if action == "editar" else "creado"
    msg.success(request, f"Tipo de servicio '{nombre}' {verbo}.")
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
    nombre_servicio = (request.POST.get("nombre_servicio") or "").strip()
    fecha_raw = request.POST.get("fecha_servicio") or ""
    fecha = _pd(fecha_raw) or timezone.localdate()

    if not unidad_id or not nombre_servicio:
        msg.error(request, "Selecciona una unidad e indica qué servicio se realizó.")
        return redirect("mantenimiento:dashboard")

    unidad = get_object_or_404(Unidad, pk=unidad_id, activa=True)
    tipo_srv, _ = TipoServicioUnidad.objects.get_or_create(
        nombre=nombre_servicio,
        defaults={"tipo_intervalo": TipoServicioUnidad.INTERVALO_TIEMPO, "activo": True},
    )

    modo = (request.POST.get("modo_servicio") or "realizado").strip().lower()
    proxima_manual = _pd(request.POST.get("proxima_fecha") or "")
    proximos_km_manual = _safe_int(request.POST.get("proximos_km")) or None

    if modo == "programado":
        # Sin historial previo: guardamos la fecha como "último conocido" hoy
        # y establecemos la próxima manualmente
        if not proxima_manual:
            msg.error(request, "En modo 'Programar próximo' debes indicar la próxima fecha.")
            return redirect("mantenimiento:dashboard")
        srv = ServicioRealizadoUnidad(
            unidad=unidad,
            tipo_servicio=tipo_srv,
            fecha_servicio=fecha,  # hoy como referencia de inicio
            notas=(request.POST.get("notas") or "Sin historial previo — fecha programada manualmente.").strip(),
            registrado_por=request.user,
            proxima_fecha=proxima_manual,
            proximos_km=proximos_km_manual,
        )
        srv.save()
    else:
        try:
            validar_fecha_servicio_realizado(fecha)
        except ValidationError as error:
            msg.error(request, error.messages[0])
            return redirect("mantenimiento:dashboard")
        km = _safe_int(request.POST.get("km_al_servicio")) or None
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
        srv.save()  # auto-calcula proxima_fecha desde el tipo
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
    if request.method != "POST":
        _require_mantenimiento(request.user)
        return redirect("mantenimiento:dashboard")
    from mantenimiento.services_planes import registrar_ejecucion_plan as ejecutar
    is_async = request.headers.get("x-requested-with") == "XMLHttpRequest" or "application/json" in request.headers.get("accept", "")
    try:
        _require_mantenimiento(request.user)
        plan = get_object_or_404(PlanMantenimiento, pk=pk, activo=True)
        datos = _datos_ejecucion_plan(request.POST)
        if not datos["clave"]:
            raise CapturaEquipoError("Falta la clave del intento. Recarga antes de iniciar una ejecución nueva.", 400)
        orden, repetida = ejecutar(usuario=request.user, plan_id=plan.pk, **datos)
    except (PermissionDenied, Http404) as exc:
        if is_async:
            code = 403 if isinstance(exc, PermissionDenied) else 404
            message = "El plan ya no está disponible o no tienes permiso para ejecutarlo."
            return JsonResponse({"ok": False, "error": message, "toast": {"type": "error", "message": message, "persistent": True}}, status=code)
        raise
    except CapturaEquipoError as exc:
        if is_async:
            return JsonResponse(_error_ejecucion_plan(exc), status=exc.status_code)
        request.plan_captura_error = str(exc.detail)
        request.plan_captura_id = plan.pk
        request.plan_ordenes_abiertas = getattr(exc, "ordenes", [])
        response = dashboard(request)
        response.status_code = exc.status_code
        return response
    plan.refresh_from_db()
    mensaje = f"Plan '{plan.nombre}' ejecutado. Próxima: {plan.proxima_ejecucion}"
    if is_async:
        days = (plan.proxima_ejecucion - timezone.localdate()).days if plan.proxima_ejecucion else None
        badge_label = "Sin fecha" if days is None else "Vencido" if days < 0 else "Hoy" if days == 0 else f"{days}d"
        badge_class = "is-danger" if days is not None and days < 0 else "is-gold" if days == 0 else "is-warning" if days is not None and days <= 7 else "is-success"
        return JsonResponse({"ok": True, "orden_id": orden.pk,
            "proxima_label": plan.proxima_ejecucion.strftime("%d/%m/%Y") if plan.proxima_ejecucion else "Sin fecha",
            "ultima_label": plan.ultima_ejecucion.strftime("%d/%m/%Y") if plan.ultima_ejecucion else "Sin fecha",
            "dias_para_vencer": days, "badge_label": badge_label, "badge_class": badge_class,
            "toast": {"type": "success", "message": mensaje},
            "target": f"#planResultado{plan.pk}", "html": str(format_html(
                '<div id="planResultado{}" role="status"><p>{}</p>'
                '<p><a href="{}?open=orden:{}#tab-seguimiento">Abrir orden {} y sus evidencias</a></p>'
                '<button type="button" class="btn btn-secondary" onclick="otraEjecucionPlan({})">Otra ejecución</button></div>',
                plan.pk, mensaje, reverse('mantenimiento:dashboard'), orden.pk, orden.folio, plan.pk))}, status=200 if repetida else 201)
    from django.contrib import messages
    messages.success(request, mensaje)
    request.plan_captura_success = plan.pk
    request.plan_captura_orden = orden
    return dashboard(request)
