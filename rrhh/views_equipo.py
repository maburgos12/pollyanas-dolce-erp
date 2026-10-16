"""Captura de jefaturas sobre las mismas fuentes que usan Bonos y RRHH."""
from decimal import Decimal

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.shortcuts import render
from rest_framework import serializers

from core.audit import log_event
from core.notificaciones import notificar_hora_extra_solicitada
from .bonos_horas_extra import BaseHorasExtraEquipoViewSet
from .bonos_permisos import BasePermisosEquipoViewSet
from .models import Empleado, PermisoSalida
from .serializers import PermisoSalidaSerializer
from .services_extra_bloqueos import bloquear_hora_extra


def empleados_equipo(user):
    return Empleado.objects.filter(activo=True, jefe_directo__usuario_erp=user).exclude(usuario_erp=user)


@login_required
def mi_equipo(request):
    if not empleados_equipo(request.user).exists():
        raise PermissionDenied("No tienes colaboradores directos activos.")
    return render(request, "rrhh/mi_equipo.html", {"tipos_permiso": PermisoSalida.TIPO_CHOICES})


class PermisosMiEquipoViewSet(BasePermisosEquipoViewSet):
    def empleados_queryset(self):
        return empleados_equipo(self.request.user)

    def _validar(self, request):
        datos = PermisoSalidaSerializer(data=request.data, context={"request": request})
        datos.is_valid(raise_exception=True)
        serializers.CharField(allow_blank=False).run_validation(request.data.get("motivo"))

    @transaction.atomic
    def create(self, request):
        serializers.IntegerField(min_value=1).run_validation(request.data.get("empleado"))
        self._validar(request)
        return super().create(request)

    @transaction.atomic
    def editar(self, request, pk=None):
        self.get_object()
        self._validar(request)
        return super().editar(request, pk=pk)


class HorasExtraMiEquipoViewSet(BaseHorasExtraEquipoViewSet):
    def empleados_queryset(self):
        return empleados_equipo(self.request.user)

    def _validar(self, request):
        serializers.DateField().run_validation(request.data.get("fecha"))
        serializers.DecimalField(max_digits=4, decimal_places=2, min_value=Decimal("0.01")).run_validation(request.data.get("horas"))
        serializers.CharField(allow_blank=False).run_validation(request.data.get("notas"))

    def create(self, request):
        serializers.IntegerField(min_value=1).run_validation(request.data.get("empleado"))
        self._validar(request)
        return super().create(request)

    def after_create(self, hora_extra):
        notificar_hora_extra_solicitada(hora_extra, actor=self.request.user)

    @transaction.atomic
    def editar(self, request, pk=None):
        original = self.get_object()
        self._validar(request)
        destino = serializers.DateField().run_validation(request.data.get("fecha"))
        original, _ = bloquear_hora_extra(original.pk, jornadas_adicionales=[(original.empleado_id, destino)])
        antes = {"fecha": original.fecha.isoformat(), "horas": str(original.horas), "notas": original.notas}
        response = super().editar(request, pk=pk)
        if response.status_code == 200:
            log_event(request.user, "corregir", "rrhh.HoraExtra", original.pk, {
                "motivo": request.data.get("motivo_cambio"), "antes": antes,
                "despues": {campo: response.data[campo] for campo in antes},
            })
        return response
