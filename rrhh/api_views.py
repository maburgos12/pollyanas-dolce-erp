from __future__ import annotations

from django.db.models import Q
from django.utils import timezone
from rest_framework import permissions, status, viewsets
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.decorators import action, api_view, authentication_classes, permission_classes
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTAuthentication

from core.access import can_manage_rrhh, can_view_rrhh
from core.notificaciones import notificar_hora_extra_solicitada
from recetas.utils.normalizacion import normalizar_nombre
from .models import AsistenciaEmpleado, Empleado, HoraExtra, PermisoSalida
from .serializers import AsistenciaSerializer, HoraExtraSerializer, PermisoSalidaSerializer
from .services import calcular_monto_hora_extra, usuario_jefe_directo_de_empleado
from .services_permisos import can_authorize_direccion


AUTH_CLASSES = [JWTAuthentication, TokenAuthentication, SessionAuthentication]


def empleado_de_usuario(user) -> Empleado | None:
    if not user or not user.is_authenticated:
        return None
    candidates = Empleado.objects.filter(activo=True)
    if user.email:
        empleado = candidates.filter(email__iexact=user.email).first()
        if empleado:
            return empleado
    empleado = candidates.filter(Q(codigo__iexact=user.username) | Q(nombre__iexact=user.get_full_name())).first()
    if empleado:
        return empleado

    user_tokens = set(normalizar_nombre(user.get_full_name() or "").split())
    if user_tokens:
        for empleado in candidates.only("id", "nombre", "nombre_normalizado", "activo"):
            if set((empleado.nombre_normalizado or normalizar_nombre(empleado.nombre)).split()) == user_tokens:
                return empleado
    return None


class _CapitalHumanoAccessMixin:
    authentication_classes = AUTH_CLASSES
    permission_classes = [permissions.IsAuthenticated]

    def _employee_scope(self):
        if can_view_rrhh(self.request.user):
            return None
        empleado = empleado_de_usuario(self.request.user)
        if not empleado:
            return "none"
        return empleado

    def _require_manage(self):
        if not can_manage_rrhh(self.request.user):
            raise PermissionDenied("No tienes permisos para autorizar Capital Humano.")

    def _apply_mis_and_limit(self, qs):
        if self.request.query_params.get("mis") == "true":
            empleado = empleado_de_usuario(self.request.user)
            qs = qs.filter(empleado=empleado) if empleado else qs.none()
        limit = self.request.query_params.get("limit")
        if limit:
            try:
                limit_value = max(int(limit), 0)
            except (TypeError, ValueError):
                raise ValidationError({"limit": "Debe ser un entero valido."})
            qs = qs[:limit_value]
        return qs


class AsistenciaViewSet(_CapitalHumanoAccessMixin, viewsets.ModelViewSet):
    serializer_class = AsistenciaSerializer

    def get_queryset(self):
        qs = AsistenciaEmpleado.objects.select_related("empleado", "turno", "sucursal")
        scope = self._employee_scope()
        if scope == "none":
            return qs.none()
        if isinstance(scope, Empleado):
            qs = qs.filter(empleado=scope)
        emp = self.request.query_params.get("empleado")
        mes = self.request.query_params.get("mes")
        if emp:
            qs = qs.filter(empleado_id=emp)
        if mes:
            qs = qs.filter(fecha__startswith=mes)
        return self._apply_mis_and_limit(qs)


class HoraExtraViewSet(_CapitalHumanoAccessMixin, viewsets.ModelViewSet):
    serializer_class = HoraExtraSerializer

    def get_queryset(self):
        qs = HoraExtra.objects.select_related("empleado", "jefe_directo", "autorizado_por")
        empleado = empleado_de_usuario(self.request.user)
        if can_view_rrhh(self.request.user):
            pass
        elif empleado:
            qs = qs.filter(Q(empleado=empleado) | Q(jefe_directo=self.request.user))
        else:
            qs = qs.filter(jefe_directo=self.request.user)
        estado = self.request.query_params.get("estado")
        if estado:
            qs = qs.filter(estado=estado)
        return self._apply_mis_and_limit(qs)

    def perform_create(self, serializer):
        empleado = serializer.validated_data.get("empleado") or empleado_de_usuario(self.request.user)
        if not empleado:
            raise ValidationError({"empleado": "No se pudo vincular tu usuario con un empleado activo."})
        if not can_view_rrhh(self.request.user) and empleado != empleado_de_usuario(self.request.user):
            raise PermissionDenied("No puedes registrar horas extra para otro empleado.")
        hora_extra = serializer.save(
            empleado=empleado,
            estado=HoraExtra.ESTADO_PENDIENTE,
            jefe_directo=usuario_jefe_directo_de_empleado(empleado),
        )
        notificar_hora_extra_solicitada(hora_extra, actor=self.request.user)

    @action(detail=True, methods=["post"])
    def autorizar(self, request, pk=None):
        he = self.get_object()
        if he.jefe_directo_id != request.user.id:
            raise PermissionDenied("Solo el jefe directo asignado puede autorizar esta hora extra.")
        he.estado = HoraExtra.ESTADO_AUTORIZADO
        he.autorizado_por = request.user
        he.fecha_autorizacion_jefe = timezone.now()
        calcular_monto_hora_extra(he)
        he.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
        return Response({"ok": True, "monto": str(he.monto_calculado)})

    @action(detail=True, methods=["post"])
    def rechazar(self, request, pk=None):
        he = self.get_object()
        if he.jefe_directo_id != request.user.id:
            raise PermissionDenied("Solo el jefe directo asignado puede rechazar esta hora extra.")
        he.estado = HoraExtra.ESTADO_RECHAZADO
        he.autorizado_por = request.user
        he.fecha_autorizacion_jefe = timezone.now()
        he.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
        return Response({"ok": True})


class PermisoSalidaViewSet(_CapitalHumanoAccessMixin, viewsets.ModelViewSet):
    serializer_class = PermisoSalidaSerializer

    def get_queryset(self):
        qs = PermisoSalida.objects.select_related("empleado", "autorizado_por")
        scope = self._employee_scope()
        if scope == "none":
            return qs.none()
        if isinstance(scope, Empleado):
            qs = qs.filter(empleado=scope)
        estado = self.request.query_params.get("estado")
        if estado:
            qs = qs.filter(estado=estado)
        return self._apply_mis_and_limit(qs)

    def perform_create(self, serializer):
        empleado = serializer.validated_data.get("empleado") or empleado_de_usuario(self.request.user)
        if not empleado:
            raise ValidationError({"empleado": "No se pudo vincular tu usuario con un empleado activo."})
        if not can_view_rrhh(self.request.user) and empleado != empleado_de_usuario(self.request.user):
            raise PermissionDenied("No puedes solicitar permisos para otro empleado.")
        serializer.save(empleado=empleado)

    @action(detail=True, methods=["post"])
    def aprobar(self, request, pk=None):
        self._require_manage()
        permiso = self.get_object()
        if (
            permiso.requiere_direccion
            and permiso.estado_direccion != PermisoSalida.ESTADO_DIRECCION_AUTORIZADO
        ):
            raise ValidationError({"direccion": "Este permiso requiere autorización de Dirección antes de RRHH."})
        permiso.estado = PermisoSalida.ESTADO_APROBADO
        permiso.autorizado_por = request.user
        permiso.save(update_fields=["estado", "autorizado_por", "actualizado_en"])
        return Response({"ok": True, "folio": permiso.folio})

    @action(detail=True, methods=["post"], url_path="autorizar-direccion")
    def autorizar_direccion(self, request, pk=None):
        if not can_authorize_direccion(request.user):
            raise PermissionDenied("Solo Dirección General puede autorizar este permiso.")
        permiso = self.get_object()
        if not permiso.requiere_direccion:
            raise ValidationError({"direccion": "Este permiso no requiere autorización de Dirección."})
        permiso.estado_direccion = PermisoSalida.ESTADO_DIRECCION_AUTORIZADO
        permiso.autorizado_direccion_por = request.user
        permiso.fecha_autorizacion_direccion = timezone.now()
        permiso.save(
            update_fields=[
                "estado_direccion",
                "autorizado_direccion_por",
                "fecha_autorizacion_direccion",
                "actualizado_en",
            ]
        )
        return Response({"ok": True, "folio": permiso.folio})

    @action(detail=True, methods=["post"], url_path="rechazar-direccion")
    def rechazar_direccion(self, request, pk=None):
        if not can_authorize_direccion(request.user):
            raise PermissionDenied("Solo Dirección General puede rechazar este permiso.")
        permiso = self.get_object()
        if not permiso.requiere_direccion:
            raise ValidationError({"direccion": "Este permiso no requiere autorización de Dirección."})
        permiso.estado_direccion = PermisoSalida.ESTADO_DIRECCION_RECHAZADO
        permiso.autorizado_direccion_por = request.user
        permiso.fecha_autorizacion_direccion = timezone.now()
        permiso.estado = PermisoSalida.ESTADO_RECHAZADO
        permiso.save(
            update_fields=[
                "estado_direccion",
                "autorizado_direccion_por",
                "fecha_autorizacion_direccion",
                "estado",
                "actualizado_en",
            ]
        )
        return Response({"ok": True})

    @action(detail=True, methods=["post"])
    def rechazar(self, request, pk=None):
        self._require_manage()
        permiso = self.get_object()
        permiso.estado = PermisoSalida.ESTADO_RECHAZADO
        permiso.autorizado_por = request.user
        permiso.save(update_fields=["estado", "autorizado_por", "actualizado_en"])
        return Response({"ok": True})


@api_view(["GET"])
@authentication_classes(AUTH_CLASSES)
@permission_classes([permissions.IsAuthenticated])
def capital_humano_me(request):
    empleado = empleado_de_usuario(request.user)
    if not empleado:
        return Response({"empleado": None}, status=status.HTTP_200_OK)
    return Response(
        {
            "empleado": empleado.id,
            "nombre": empleado.nombre,
            "codigo": empleado.codigo,
            "sucursal": empleado.sucursal,
            "puede_gestionar": can_manage_rrhh(request.user),
        }
    )


@api_view(["GET"])
@authentication_classes(AUTH_CLASSES)
@permission_classes([permissions.IsAuthenticated])
def mi_perfil(request):
    empleado = empleado_de_usuario(request.user)
    if not empleado:
        return Response(
            {
                "nombre": request.user.get_full_name() or request.user.username,
                "puesto": "",
                "area": "",
                "codigo": "",
                "sucursal": "",
            }
        )
    return Response(
        {
            "nombre": empleado.nombre,
            "puesto": empleado.puesto or "",
            "area": empleado.area or "",
            "codigo": empleado.codigo or "",
            "sucursal": str(empleado.sucursal) if empleado.sucursal else "",
        }
    )
