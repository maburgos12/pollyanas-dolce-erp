from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.shortcuts import get_object_or_404
from django.utils.dateparse import parse_date
from django.utils import timezone
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

ESTADOS_HORA_EXTRA_EDITABLES = {HoraExtra.ESTADO_PENDIENTE}
ESTADOS_HORA_EXTRA_ELIMINABLES = {HoraExtra.ESTADO_PENDIENTE}


def _parse_decimal(value) -> Decimal | None:
    try:
        parsed = Decimal(str(value or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _motivo_cambio(request) -> str:
    motivo = (request.data.get("motivo_cambio") or "").strip()
    if not motivo:
        motivo = (request.data.get("motivo_eliminacion") or "").strip()
    return motivo


def _folio_hora_extra(hora_extra: HoraExtra) -> str:
    if not hora_extra.pk:
        return "HE-SIN-FOLIO"
    return f"HE-{hora_extra.fecha:%Y%m%d}-{hora_extra.pk:05d}"


def _estado_label(hora_extra: HoraExtra) -> str:
    return dict(HoraExtra.ESTADO_CHOICES).get(hora_extra.estado, hora_extra.estado)


def _append_nota_operativa(hora_extra: HoraExtra, texto: str) -> None:
    nota_actual = (hora_extra.notas or "").strip()
    hora_extra.notas = f"{nota_actual}\n\n{texto}".strip()


def _hora_extra_payload(hora_extra: HoraExtra, user=None, puede_gestionar: bool = False) -> dict:
    jefe_nombre = ""
    if hora_extra.jefe_directo_id:
        jefe_nombre = hora_extra.jefe_directo.get_full_name() or hora_extra.jefe_directo.username
    puede_autorizar = bool(
        user
        and getattr(user, "is_authenticated", False)
        and hora_extra.estado == HoraExtra.ESTADO_PENDIENTE
        and hora_extra.jefe_directo_id == user.id
    )
    puede_editar = bool(puede_gestionar and hora_extra.estado in ESTADOS_HORA_EXTRA_EDITABLES)
    puede_eliminar = bool(puede_gestionar and hora_extra.estado in ESTADOS_HORA_EXTRA_ELIMINABLES)
    return {
        "id": hora_extra.id,
        "folio": _folio_hora_extra(hora_extra),
        "empleado": hora_extra.empleado_id,
        "empleado_nombre": hora_extra.empleado.nombre,
        "area": hora_extra.empleado.area,
        "puesto": hora_extra.empleado.puesto,
        "sucursal_nombre": hora_extra.empleado.sucursal,
        "fecha": hora_extra.fecha.isoformat(),
        "horas": str(hora_extra.horas),
        "monto_calculado": str(hora_extra.monto_calculado or "0"),
        "estado": hora_extra.estado,
        "estado_label": _estado_label(hora_extra),
        "jefe_directo": hora_extra.jefe_directo_id,
        "jefe_directo_nombre": jefe_nombre,
        "notas": hora_extra.notas,
        "creado_en": hora_extra.creado_en.isoformat(),
        "puede_autorizar": puede_autorizar,
        "puede_editar": puede_editar,
        "puede_eliminar": puede_eliminar,
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

    def empleado_payload(self, empleado: Empleado) -> dict:
        return {
            "id": empleado.id,
            "codigo": empleado.codigo,
            "empleado_nombre": empleado.nombre,
            "area": empleado.area,
            "puesto": empleado.puesto,
            "sucursal_nombre": empleado.sucursal,
        }

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
                "empleados": [self.empleado_payload(empleado) for empleado in empleados],
                "horas_extra": [
                    _hora_extra_payload(
                        hora_extra,
                        request.user,
                        puede_gestionar=self.can_gestionar_empleado(hora_extra.empleado),
                    )
                    for hora_extra in horas_extra
                ],
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
        return Response(
            _hora_extra_payload(
                hora_extra,
                request.user,
                puede_gestionar=self.can_gestionar_empleado(hora_extra.empleado),
            ),
            status=status.HTTP_201_CREATED,
        )

    def get_object(self):
        return get_object_or_404(self._horas_extra(), pk=self.kwargs["pk"])

    @action(detail=True, methods=["post"])
    def editar(self, request, pk=None):
        hora_extra = self.get_object()
        if not self.can_gestionar_empleado(hora_extra.empleado):
            return Response({"detail": "No tienes permiso para editar esta hora extra."}, status=status.HTTP_403_FORBIDDEN)
        if hora_extra.estado not in ESTADOS_HORA_EXTRA_EDITABLES:
            return Response({"detail": "Solo se pueden editar horas extra pendientes."}, status=status.HTTP_400_BAD_REQUEST)

        motivo_cambio = _motivo_cambio(request)
        if not motivo_cambio:
            return Response({"motivo_cambio": "Explica por que se corrige esta hora extra."}, status=status.HTTP_400_BAD_REQUEST)

        fecha = parse_date(str(request.data.get("fecha") or ""))
        horas = _parse_decimal(request.data.get("horas"))
        notas = (request.data.get("notas") or "").strip()
        if not fecha:
            return Response({"fecha": "La fecha es obligatoria."}, status=status.HTTP_400_BAD_REQUEST)
        if horas is None:
            return Response({"horas": "Captura horas extra mayores a cero."}, status=status.HTTP_400_BAD_REQUEST)
        if not notas:
            return Response({"notas": "El motivo es obligatorio."}, status=status.HTTP_400_BAD_REQUEST)
        if (
            HoraExtra.objects.filter(empleado=hora_extra.empleado, fecha=fecha, estado__in=ESTADOS_HORA_EXTRA_ACTIVOS)
            .exclude(pk=hora_extra.pk)
            .exists()
        ):
            return Response(
                {"detail": "Ya existe una hora extra activa para este empleado y fecha."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        cambios = {}
        if hora_extra.fecha != fecha:
            cambios["fecha"] = {"antes": hora_extra.fecha.isoformat(), "despues": fecha.isoformat()}
        if hora_extra.horas != horas:
            cambios["horas"] = {"antes": str(hora_extra.horas), "despues": str(horas)}
        if (hora_extra.notas or "") != notas:
            cambios["notas"] = {"antes": hora_extra.notas or "", "despues": notas}
        if not cambios:
            return Response({"detail": "No hay cambios para guardar."}, status=status.HTTP_400_BAD_REQUEST)

        hora_extra.fecha = fecha
        hora_extra.horas = horas
        hora_extra.notas = notas
        _append_nota_operativa(
            hora_extra,
            f"Correccion registrada por {request.user.get_username()} el {timezone.localtime():%Y-%m-%d %H:%M}: {motivo_cambio}",
        )
        hora_extra.save(update_fields=["fecha", "horas", "notas"])
        return Response(
            _hora_extra_payload(
                hora_extra,
                request.user,
                puede_gestionar=self.can_gestionar_empleado(hora_extra.empleado),
            )
        )

    @action(detail=True, methods=["post"])
    def eliminar(self, request, pk=None):
        hora_extra = self.get_object()
        if not self.can_gestionar_empleado(hora_extra.empleado):
            return Response({"detail": "No tienes permiso para eliminar esta hora extra."}, status=status.HTTP_403_FORBIDDEN)
        if hora_extra.estado not in ESTADOS_HORA_EXTRA_ELIMINABLES:
            return Response({"detail": "Solo se pueden eliminar horas extra pendientes."}, status=status.HTTP_400_BAD_REQUEST)
        motivo_cambio = _motivo_cambio(request)
        if not motivo_cambio:
            return Response({"motivo_cambio": "Explica por que se elimina esta hora extra."}, status=status.HTTP_400_BAD_REQUEST)

        hora_extra.estado = HoraExtra.ESTADO_CANCELADO
        hora_extra.autorizado_por = request.user
        hora_extra.fecha_autorizacion_jefe = timezone.now()
        _append_nota_operativa(
            hora_extra,
            f"Cancelada por {request.user.get_username()} el {timezone.localtime():%Y-%m-%d %H:%M}: {motivo_cambio}",
        )
        hora_extra.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe", "notas"])
        return Response(
            _hora_extra_payload(
                hora_extra,
                request.user,
                puede_gestionar=self.can_gestionar_empleado(hora_extra.empleado),
            )
        )

    @action(detail=True, methods=["post"])
    def autorizar(self, request, pk=None):
        hora_extra = self.get_object()
        if hora_extra.jefe_directo_id != request.user.id:
            return Response({"detail": "Solo el jefe directo asignado puede autorizar esta hora extra."}, status=status.HTTP_403_FORBIDDEN)
        if hora_extra.estado != HoraExtra.ESTADO_PENDIENTE:
            return Response({"detail": "Solo se pueden autorizar horas extra pendientes."}, status=status.HTTP_400_BAD_REQUEST)
        hora_extra.estado = HoraExtra.ESTADO_AUTORIZADO
        hora_extra.autorizado_por = request.user

        hora_extra.fecha_autorizacion_jefe = timezone.now()
        calcular_monto_hora_extra(hora_extra)
        hora_extra.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
        return Response(
            _hora_extra_payload(
                hora_extra,
                request.user,
                puede_gestionar=self.can_gestionar_empleado(hora_extra.empleado),
            ),
            status=status.HTTP_200_OK,
        )

    @action(detail=True, methods=["post"])
    def rechazar(self, request, pk=None):
        hora_extra = self.get_object()
        if hora_extra.jefe_directo_id != request.user.id:
            return Response({"detail": "Solo el jefe directo asignado puede rechazar esta hora extra."}, status=status.HTTP_403_FORBIDDEN)
        if hora_extra.estado != HoraExtra.ESTADO_PENDIENTE:
            return Response({"detail": "Solo se pueden rechazar horas extra pendientes."}, status=status.HTTP_400_BAD_REQUEST)
        hora_extra.estado = HoraExtra.ESTADO_RECHAZADO
        hora_extra.autorizado_por = request.user

        hora_extra.fecha_autorizacion_jefe = timezone.now()
        hora_extra.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
        return Response(
            _hora_extra_payload(
                hora_extra,
                request.user,
                puede_gestionar=self.can_gestionar_empleado(hora_extra.empleado),
            ),
            status=status.HTTP_200_OK,
        )
