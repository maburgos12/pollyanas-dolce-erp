from __future__ import annotations

from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from core.notificaciones import notificar_permiso_solicitado
from rrhh.models import Empleado, PermisoSalida


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


def _permiso_payload(permiso: PermisoSalida) -> dict:
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
        "creado_en": permiso.creado_en.isoformat(),
    }


class BasePermisosEquipoViewSet(viewsets.ViewSet):
    permission_classes = [IsAuthenticated]
    origen_solicitud = PermisoSalida.ORIGEN_RRHH

    def empleados_queryset(self):
        return Empleado.objects.none()

    def filter_permisos(self, qs):
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
                "permisos": [_permiso_payload(permiso) for permiso in permisos],
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
        return Response(_permiso_payload(permiso), status=status.HTTP_201_CREATED)

    def get_object(self):
        return get_object_or_404(self._permisos(), pk=self.kwargs["pk"])

    @action(detail=True, methods=["post"])
    def preautorizar(self, request, pk=None):
        permiso = self.get_object()
        permiso.estado_jefe = PermisoSalida.ESTADO_JEFE_PREAUTORIZADO
        permiso.autorizado_jefe_por = request.user
        permiso.fecha_autorizacion_jefe = timezone.now()
        permiso.save(update_fields=["estado_jefe", "autorizado_jefe_por", "fecha_autorizacion_jefe", "actualizado_en"])
        return Response(_permiso_payload(permiso))

    @action(detail=True, methods=["post"])
    def rechazar(self, request, pk=None):
        permiso = self.get_object()
        permiso.estado_jefe = PermisoSalida.ESTADO_JEFE_RECHAZADO
        permiso.autorizado_jefe_por = request.user
        permiso.fecha_autorizacion_jefe = timezone.now()
        permiso.estado = PermisoSalida.ESTADO_RECHAZADO
        permiso.save(
            update_fields=[
                "estado_jefe",
                "autorizado_jefe_por",
                "fecha_autorizacion_jefe",
                "estado",
                "actualizado_en",
            ]
        )
        return Response(_permiso_payload(permiso))
