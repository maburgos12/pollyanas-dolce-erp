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
from .models import AsistenciaEmpleado, Empleado, HoraExtra, PermisoSalida, SolicitudVacaciones
from .serializers import AsistenciaSerializer, HoraExtraSerializer, PermisoSalidaSerializer, SolicitudVacacionesSerializer
from .services import calcular_monto_hora_extra, usuario_jefe_directo_de_empleado
from .services_permisos import resolver_permiso_direccion
from .services_vacaciones import (
    aprobar_solicitud_vacaciones_rrhh,
    can_gestionar_vacaciones_jefe,
    crear_solicitud_vacaciones,
    preautorizar_solicitud_vacaciones_jefe,
    rechazar_solicitud_vacaciones,
    saldo_vacaciones_empleado,
)


AUTH_CLASSES = [JWTAuthentication, TokenAuthentication, SessionAuthentication]


def empleado_de_usuario(user) -> Empleado | None:
    if not user or not user.is_authenticated:
        return None
    candidates = Empleado.objects.filter(activo=True)
    empleado = getattr(user, "empleado_rrhh", None)
    if empleado and empleado.activo:
        return empleado
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

    def update(self, request, *args, **kwargs):
        raise PermissionDenied("Las correcciones de permisos deben registrarse desde el flujo auditado de permisos de equipo.")

    def partial_update(self, request, *args, **kwargs):
        raise PermissionDenied("Las correcciones de permisos deben registrarse desde el flujo auditado de permisos de equipo.")

    def destroy(self, request, *args, **kwargs):
        raise PermissionDenied("Los permisos se eliminan con motivo desde el flujo auditado de permisos de equipo.")

    @action(detail=True, methods=["post"])
    def aprobar(self, request, pk=None):
        raise PermissionDenied("Capital Humano captura, consulta y archiva permisos; no los autoriza.")

    @action(detail=True, methods=["post"], url_path="autorizar-direccion")
    def autorizar_direccion(self, request, pk=None):
        permiso = self.get_object()
        resolver_permiso_direccion(permiso, request.user, aprobar=True)
        return Response({"ok": True, "folio": permiso.folio})

    @action(detail=True, methods=["post"], url_path="rechazar-direccion")
    def rechazar_direccion(self, request, pk=None):
        permiso = self.get_object()
        resolver_permiso_direccion(permiso, request.user, aprobar=False)
        return Response({"ok": True})

    @action(detail=True, methods=["post"])
    def rechazar(self, request, pk=None):
        raise PermissionDenied("Capital Humano captura, consulta y archiva permisos; no los rechaza.")


class SolicitudVacacionesViewSet(_CapitalHumanoAccessMixin, viewsets.ModelViewSet):
    serializer_class = SolicitudVacacionesSerializer

    def get_queryset(self):
        qs = SolicitudVacaciones.objects.select_related(
            "empleado",
            "jefe_directo",
            "preautorizado_por",
            "aprobado_rrhh_por",
        )
        empleado = empleado_de_usuario(self.request.user)
        if can_view_rrhh(self.request.user):
            pass
        elif empleado:
            qs = qs.filter(Q(empleado=empleado) | Q(jefe_directo=self.request.user))
        else:
            qs = qs.filter(jefe_directo=self.request.user)
        if self.request.query_params.get("equipo") == "true":
            qs = qs.filter(jefe_directo=self.request.user)
        estado = self.request.query_params.get("estado")
        if estado:
            qs = qs.filter(estado=estado)
        return self._apply_mis_and_limit(qs.order_by("-creado_en"))

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        empleado = serializer.validated_data.get("empleado") or empleado_de_usuario(request.user)
        if not empleado:
            raise ValidationError({"empleado": "No se pudo vincular el usuario con un empleado activo."})
        empleado_actual = empleado_de_usuario(request.user)
        puede_crear = (
            can_view_rrhh(request.user)
            or empleado == empleado_actual
            or can_gestionar_vacaciones_jefe(request.user, empleado)
        )
        if not puede_crear:
            raise PermissionDenied("Solo puedes crear vacaciones propias o de tu equipo directo.")
        solicitud = crear_solicitud_vacaciones(
            empleado=empleado,
            fecha_inicio=serializer.validated_data["fecha_inicio"],
            fecha_fin=serializer.validated_data["fecha_fin"],
            motivo=serializer.validated_data.get("motivo", ""),
            actor=request.user,
        )
        return Response(self.get_serializer(solicitud).data, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=["post"])
    def preautorizar(self, request, pk=None):
        solicitud = self.get_object()
        preautorizar_solicitud_vacaciones_jefe(solicitud, request.user, aprobar=True)
        return Response(self.get_serializer(solicitud).data)

    @action(detail=True, methods=["post"], url_path="rechazar-jefe")
    def rechazar_jefe(self, request, pk=None):
        solicitud = self.get_object()
        preautorizar_solicitud_vacaciones_jefe(solicitud, request.user, aprobar=False)
        return Response(self.get_serializer(solicitud).data)

    @action(detail=True, methods=["post"], url_path="aprobar-rrhh")
    def aprobar_rrhh(self, request, pk=None):
        solicitud = self.get_object()
        aprobar_solicitud_vacaciones_rrhh(solicitud, request.user)
        return Response(self.get_serializer(solicitud).data)

    @action(detail=True, methods=["post"], url_path="rechazar-rrhh")
    def rechazar_rrhh(self, request, pk=None):
        solicitud = self.get_object()
        rechazar_solicitud_vacaciones(solicitud, request.user)
        return Response(self.get_serializer(solicitud).data)

    @action(detail=False, methods=["get"])
    def saldo(self, request):
        empleado = empleado_de_usuario(request.user)
        if not empleado:
            return Response({"empleado": None, "saldo": None})
        return Response({"empleado": empleado.id, "saldo": saldo_vacaciones_empleado(empleado)})


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
