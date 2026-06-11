from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.shortcuts import get_object_or_404
from django.utils.dateparse import parse_date
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from rrhh.models import Empleado, HoraExtra
from rrhh.services import calcular_monto_hora_extra, usuario_jefe_directo_de_empleado


ESTADOS_HORA_EXTRA_ACTIVOS = {
    HoraExtra.ESTADO_PENDIENTE,
    HoraExtra.ESTADO_AUTORIZADO,
    HoraExtra.ESTADO_PAGADO,
}


def _parse_decimal(value) -> Decimal | None:
    try:
        parsed = Decimal(str(value or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _hora_extra_payload(hora_extra: HoraExtra, user=None) -> dict:
    jefe_nombre = ""
    if hora_extra.jefe_directo_id:
        jefe_nombre = hora_extra.jefe_directo.get_full_name() or hora_extra.jefe_directo.username
    puede_autorizar = bool(
        user
        and getattr(user, "is_authenticated", False)
        and hora_extra.estado == HoraExtra.ESTADO_PENDIENTE
        and hora_extra.jefe_directo_id == user.id
    )
    return {
        "id": hora_extra.id,
        "empleado": hora_extra.empleado_id,
        "empleado_nombre": hora_extra.empleado.nombre,
        "fecha": hora_extra.fecha.isoformat(),
        "horas": str(hora_extra.horas),
        "monto_calculado": str(hora_extra.monto_calculado or "0"),
        "estado": hora_extra.estado,
        "jefe_directo": hora_extra.jefe_directo_id,
        "jefe_directo_nombre": jefe_nombre,
        "notas": hora_extra.notas,
        "creado_en": hora_extra.creado_en.isoformat(),
        "puede_autorizar": puede_autorizar,
    }


class BaseHorasExtraEquipoViewSet(viewsets.ViewSet):
    permission_classes = [IsAuthenticated]

    def empleados_queryset(self):
        return Empleado.objects.none()

    def filter_horas_extra(self, qs):
        return qs

    def can_gestionar_empleado(self, empleado: Empleado) -> bool:
        jefe_usuario_id = getattr(getattr(empleado, "jefe_directo", None), "usuario_erp_id", None)
        return bool(jefe_usuario_id and jefe_usuario_id == self.request.user.id)

    def _empleados(self):
        return self.empleados_queryset().filter(activo=True).order_by("nombre")

    def _horas_extra(self):
        empleado_ids = self._empleados().values_list("id", flat=True)
        qs = (
            HoraExtra.objects.select_related("empleado", "jefe_directo", "autorizado_por")
            .filter(empleado_id__in=empleado_ids)
            .order_by("-fecha", "-id")
        )
        mes = self.request.query_params.get("mes")
        anio = self.request.query_params.get("anio")
        estado = self.request.query_params.get("estado")
        if mes:
            qs = qs.filter(fecha__month=mes)
        if anio:
            qs = qs.filter(fecha__year=anio)
        if estado:
            qs = qs.filter(estado=estado)
        return self.filter_horas_extra(qs)

    def list(self, request):
        empleados = list(self._empleados())
        horas_extra = list(self._horas_extra())
        return Response(
            {
                "empleados": [
                    {
                        "id": empleado.id,
                        "codigo": empleado.codigo,
                        "empleado_nombre": empleado.nombre,
                        "area": empleado.area,
                        "puesto": empleado.puesto,
                        "sucursal_nombre": empleado.sucursal,
                    }
                    for empleado in empleados
                ],
                "horas_extra": [_hora_extra_payload(hora_extra, request.user) for hora_extra in horas_extra],
            }
        )

    def create(self, request):
        empleado_id = request.data.get("empleado")
        try:
            empleado = self._empleados().get(pk=empleado_id)
        except Empleado.DoesNotExist:
            return Response({"empleado": "Empleado fuera de tu equipo o inactivo."}, status=status.HTTP_400_BAD_REQUEST)
        if not self.can_gestionar_empleado(empleado):
            return Response({"empleado": "Solo puedes registrar horas extra para tu equipo directo."}, status=status.HTTP_403_FORBIDDEN)

        fecha = parse_date(str(request.data.get("fecha") or ""))
        horas = _parse_decimal(request.data.get("horas"))
        notas = (request.data.get("notas") or "").strip()
        if not fecha:
            return Response({"fecha": "La fecha es obligatoria."}, status=status.HTTP_400_BAD_REQUEST)
        if horas is None:
            return Response({"horas": "Captura horas extra mayores a cero."}, status=status.HTTP_400_BAD_REQUEST)
        if not notas:
            return Response({"notas": "El motivo es obligatorio."}, status=status.HTTP_400_BAD_REQUEST)
        if HoraExtra.objects.filter(empleado=empleado, fecha=fecha, estado__in=ESTADOS_HORA_EXTRA_ACTIVOS).exists():
            return Response(
                {"detail": "Ya existe una hora extra activa para este empleado y fecha."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        hora_extra = HoraExtra.objects.create(
            empleado=empleado,
            fecha=fecha,
            horas=horas,
            estado=HoraExtra.ESTADO_PENDIENTE,
            jefe_directo=usuario_jefe_directo_de_empleado(empleado),
            notas=notas,
        )
        return Response(_hora_extra_payload(hora_extra, request.user), status=status.HTTP_201_CREATED)

    def get_object(self):
        return get_object_or_404(self._horas_extra(), pk=self.kwargs["pk"])

    @action(detail=True, methods=["post"])
    def autorizar(self, request, pk=None):
        hora_extra = self.get_object()
        if hora_extra.jefe_directo_id != request.user.id:
            return Response({"detail": "Solo el jefe directo asignado puede autorizar esta hora extra."}, status=status.HTTP_403_FORBIDDEN)
        if hora_extra.estado != HoraExtra.ESTADO_PENDIENTE:
            return Response({"detail": "Solo se pueden autorizar horas extra pendientes."}, status=status.HTTP_400_BAD_REQUEST)
        hora_extra.estado = HoraExtra.ESTADO_AUTORIZADO
        hora_extra.autorizado_por = request.user
        from django.utils import timezone

        hora_extra.fecha_autorizacion_jefe = timezone.now()
        calcular_monto_hora_extra(hora_extra)
        hora_extra.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
        return Response(_hora_extra_payload(hora_extra, request.user), status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"])
    def rechazar(self, request, pk=None):
        hora_extra = self.get_object()
        if hora_extra.jefe_directo_id != request.user.id:
            return Response({"detail": "Solo el jefe directo asignado puede rechazar esta hora extra."}, status=status.HTTP_403_FORBIDDEN)
        if hora_extra.estado != HoraExtra.ESTADO_PENDIENTE:
            return Response({"detail": "Solo se pueden rechazar horas extra pendientes."}, status=status.HTTP_400_BAD_REQUEST)
        hora_extra.estado = HoraExtra.ESTADO_RECHAZADO
        hora_extra.autorizado_por = request.user
        from django.utils import timezone

        hora_extra.fecha_autorizacion_jefe = timezone.now()
        hora_extra.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
        return Response(_hora_extra_payload(hora_extra, request.user), status=status.HTTP_200_OK)
