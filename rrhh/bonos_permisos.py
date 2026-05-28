from __future__ import annotations

import unicodedata

from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.db.models import Q

from core.notificaciones import notificar_permiso_solicitado
from core.access import can_manage_rrhh, can_manage_submodule
from rrhh.models import Empleado, PermisoSalida
from rrhh.services_permisos import can_resolver_permiso_jefe, resolver_permiso_jefe


TIPO_LABELS = {
    PermisoSalida.TIPO_PERMISO_HORA: "Tiempo parcial",
    PermisoSalida.TIPO_PERMISO_DIA: "Dia completo",
    PermisoSalida.TIPO_SALIDA_PERSONAL: "Salida personal",
    PermisoSalida.TIPO_CITA_MEDICA: "Cita medica",
    PermisoSalida.TIPO_OTRO: "Otro",
}


def _parse_dt(value):
    if not value:
        return None
    dt = parse_datetime(str(value))
    if dt and timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _parse_bool(value, default=True):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "si", "sí", "yes", "y", "on"}


def _empleado_payload(empleado: Empleado) -> dict:
    return {
        "id": empleado.id,
        "codigo": empleado.codigo,
        "empleado_nombre": empleado.nombre,
        "area": empleado.area,
        "puesto": empleado.puesto,
        "sucursal_nombre": empleado.sucursal,
    }


def _permiso_payload(permiso: PermisoSalida, user=None) -> dict:
    if user is None:
        puede_editar = False
    else:
        puede_editar = _puede_editar_permiso(user, permiso)
    return {
        "id": permiso.id,
        "folio": permiso.folio,
        "empleado": permiso.empleado_id,
        "empleado_nombre": permiso.empleado.nombre,
        "area": permiso.empleado.area,
        "puesto": permiso.empleado.puesto,
        "sucursal_nombre": permiso.empleado.sucursal,
        "tipo": permiso.tipo,
        "tipo_label": TIPO_LABELS.get(permiso.tipo, permiso.tipo),
        "fecha_inicio": permiso.fecha_inicio.isoformat(),
        "fecha_fin": permiso.fecha_fin.isoformat() if permiso.fecha_fin else None,
        "motivo": permiso.motivo,
        "estado": permiso.estado,
        "estado_jefe": permiso.estado_jefe,
        "requiere_direccion": permiso.requiere_direccion,
        "estado_direccion": permiso.estado_direccion,
        "goce_sueldo": permiso.goce_sueldo,
        "goce_label": "Con goce" if permiso.goce_sueldo else "Sin goce",
        "origen_solicitud": permiso.origen_solicitud,
        "puede_preautorizar": can_resolver_permiso_jefe(user, permiso) if user is not None else False,
        "puede_editar": puede_editar,
        "creado_en": permiso.creado_en.isoformat(),
    }


def _puede_editar_permiso(user, permiso: PermisoSalida) -> bool:
    if permiso.estado != PermisoSalida.ESTADO_SOLICITADO or permiso.estado_jefe != PermisoSalida.ESTADO_JEFE_PENDIENTE:
        return False

    if can_manage_rrhh(user):
        return True

    if permiso.origen_solicitud == PermisoSalida.ORIGEN_BONOS_VENTAS:
        return can_manage_submodule(user, "ventas", "bonos")

    if permiso.origen_solicitud == PermisoSalida.ORIGEN_BONOS_PRODUCCION:
        return can_manage_submodule(user, "produccion", "bonos")

    return False


def _normalize_text(value: str | None) -> str:
    normalized = unicodedata.normalize("NFD", (value or "").strip().upper())
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalize_area_filter_value(raw: str | None) -> str:
    value = _normalize_text(raw)
    if not value:
        return ""
    if value in {"PRODUCCION", "PRODUC", "PROD", "PRODUCION"}:
        return "PRODUCCION"
    if value in {"HORNOS", "HORNO"}:
        return "HORNOS"
    if value in {"LOGISTICA", "LOGISTICO", "LOGÍSTICA"}:
        return "LOGISTICA"
    return value


def _permiso_area_filter_q(area_filter: str):
    if not area_filter:
        return Q()
    logistica_area_terms = ("LOGISTICA", "LOGISTICO")
    hornos_puestos = ("HORNOS", "HORNO")
    produccion_puestos = ("PRODUCCION", "EMBETUNADO")
    logistica_puestos = ("REPARTIDOR", "ENVIO_SUCURSAL")
    if area_filter == "PRODUCCION":
        return (
            Q(empleado__puesto_operativo__in=produccion_puestos)
            | Q(empleado__area__iexact="PRODUCCION")
        )
    if area_filter == "HORNOS":
        return (
            Q(empleado__area__iregex="HORNOS|HORNO")
            | Q(empleado__puesto_operativo__in=hornos_puestos)
        )
    if area_filter == "LOGISTICA":
        return (
            Q(empleado__departamento=Empleado.DEP_LOGISTICA)
            | Q(empleado__departamento_origen=Empleado.DEP_LOGISTICA)
            | Q(empleado__area__in=logistica_area_terms)
            | Q(empleado__area__iregex="LOGÍSTICA|LOGISTICA")
            | Q(empleado__puesto_operativo__in=logistica_puestos)
        )
    return Q(empleado__area__iexact=area_filter)


class BasePermisosEquipoViewSet(viewsets.ViewSet):
    permission_classes = [IsAuthenticated]
    origen_solicitud = PermisoSalida.ORIGEN_RRHH

    def empleados_queryset(self):
        return Empleado.objects.none()

    def filter_permisos(self, qs):
        area_filter = self.request.query_params.get("area")
        area_filter = _normalize_area_filter_value(area_filter)
        if area_filter:
            qs = qs.filter(_permiso_area_filter_q(area_filter))
        sucursal_filter = self.request.query_params.get("sucursal")
        if sucursal_filter:
            sucursal_filter = sucursal_filter.strip()
            from core.models import Sucursal

            sucursal_obj = None
            if sucursal_filter.isdigit():
                sucursal_obj = Sucursal.objects.filter(pk=sucursal_filter).values_list("nombre", flat=True).first()
            if sucursal_obj:
                qs = qs.filter(empleado__sucursal__iexact=sucursal_obj)
            else:
                qs = qs.filter(empleado__sucursal__iexact=sucursal_filter)
        return qs

    def _empleados(self):
        return self.empleados_queryset().filter(activo=True).order_by("nombre")

    def _permisos(self):
        empleado_ids = self._empleados().values_list("id", flat=True)
        qs = (
            PermisoSalida.objects.select_related("empleado", "autorizado_jefe_por", "autorizado_por")
            .filter(empleado_id__in=empleado_ids)
            .order_by("-fecha_inicio", "-id")
        )
        mes = self.request.query_params.get("mes")
        anio = self.request.query_params.get("anio")
        if mes:
            qs = qs.filter(fecha_inicio__month=mes)
        if anio:
            qs = qs.filter(fecha_inicio__year=anio)
        return self.filter_permisos(qs)

    def list(self, request):
        empleados = list(self._empleados())
        permisos = list(self._permisos())
        return Response(
            {
                "empleados": [_empleado_payload(emp) for emp in empleados],
                "permisos": [_permiso_payload(permiso, request.user) for permiso in permisos],
            }
        )

    def create(self, request):
        empleado_id = request.data.get("empleado")
        try:
            empleado = self._empleados().get(pk=empleado_id)
        except Empleado.DoesNotExist:
            return Response({"empleado": "Empleado fuera de tu equipo o inactivo."}, status=status.HTTP_400_BAD_REQUEST)

        fecha_inicio = _parse_dt(request.data.get("fecha_inicio"))
        if not fecha_inicio:
            return Response({"fecha_inicio": "Fecha/hora de inicio invalida."}, status=status.HTTP_400_BAD_REQUEST)

        fecha_fin = _parse_dt(request.data.get("fecha_fin"))
        tipo = request.data.get("tipo") or PermisoSalida.TIPO_PERMISO_HORA
        if tipo not in dict(PermisoSalida.TIPO_CHOICES):
            return Response({"tipo": "Tipo de permiso invalido."}, status=status.HTTP_400_BAD_REQUEST)

        motivo = (request.data.get("motivo") or "").strip()
        if not motivo:
            return Response({"motivo": "El motivo es obligatorio."}, status=status.HTTP_400_BAD_REQUEST)

        permiso = PermisoSalida.objects.create(
            empleado=empleado,
            tipo=tipo,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            motivo=motivo,
            goce_sueldo=_parse_bool(request.data.get("goce_sueldo"), default=True),
            origen_solicitud=self.origen_solicitud,
        )
        notificar_permiso_solicitado(permiso, actor=request.user)
        return Response(_permiso_payload(permiso, request.user), status=status.HTTP_201_CREATED)

    def _coerce_edicion_payload(self, permiso: PermisoSalida, request):
        data = request.data if request and hasattr(request, "data") else {}
        errors = []
        updates = {}

        if "tipo" in data:
            tipo = (data.get("tipo") or "").strip()
            if tipo not in dict(PermisoSalida.TIPO_CHOICES):
                errors.append("Tipo de permiso invalido.")
            else:
                updates["tipo"] = tipo

        if "fecha_inicio" in data:
            fecha_inicio = _parse_dt(data.get("fecha_inicio"))
            if fecha_inicio is None:
                errors.append("Fecha/hora de inicio invalida.")
            else:
                updates["fecha_inicio"] = fecha_inicio

        if "fecha_fin" in data:
            fecha_fin_raw = data.get("fecha_fin")
            if (fecha_fin_raw is None) or (str(fecha_fin_raw).strip() == ""):
                updates["fecha_fin"] = None
            else:
                fecha_fin = _parse_dt(fecha_fin_raw)
                if fecha_fin is None:
                    errors.append("Fecha/hora de fin invalida.")
                else:
                    updates["fecha_fin"] = fecha_fin

        if "motivo" in data:
            motivo = (data.get("motivo") or "").strip()
            if not motivo:
                errors.append("El motivo es obligatorio.")
            else:
                updates["motivo"] = motivo

        if "goce_sueldo" in data:
            updates["goce_sueldo"] = _parse_bool(data.get("goce_sueldo"), default=permiso.goce_sueldo)

        fecha_inicio = updates.get("fecha_inicio", permiso.fecha_inicio)
        fecha_fin = updates.get("fecha_fin", permiso.fecha_fin)
        if fecha_inicio and fecha_fin and fecha_fin < fecha_inicio:
            errors.append("La fecha final no puede ser anterior a la inicial.")
        return errors, updates

    def get_object(self):
        return get_object_or_404(self._permisos(), pk=self.kwargs["pk"])

    @action(detail=True, methods=["post"])
    def preautorizar(self, request, pk=None):
        permiso = self.get_object()
        resolver_permiso_jefe(permiso, request.user, aprobar=True)
        return Response(_permiso_payload(permiso, request.user))

    @action(detail=True, methods=["post"])
    def rechazar(self, request, pk=None):
        permiso = self.get_object()
        resolver_permiso_jefe(permiso, request.user, aprobar=False)
        return Response(_permiso_payload(permiso, request.user))

    @action(detail=True, methods=["post"], url_path="editar")
    def editar(self, request, pk=None):
        permiso = get_object_or_404(PermisoSalida.objects.select_related("empleado"), pk=self.kwargs["pk"])
        if not _puede_editar_permiso(request.user, permiso):
            raise PermissionDenied("No tienes permisos para editar este permiso.")
        if permiso.estado != PermisoSalida.ESTADO_SOLICITADO or permiso.estado_jefe != PermisoSalida.ESTADO_JEFE_PENDIENTE:
            return Response(
                {"detail": "Solo se pueden editar permisos en estado pendiente."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        errors, updates = self._coerce_edicion_payload(permiso, request)
        if errors:
            return Response({"detail": errors[0]}, status=status.HTTP_400_BAD_REQUEST)

        if not updates:
            return Response(_permiso_payload(permiso, request.user))

        for key, value in updates.items():
            setattr(permiso, key, value)
        permiso.save(update_fields=list(updates.keys()) + ["actualizado_en"])
        return Response(_permiso_payload(permiso, request.user))
