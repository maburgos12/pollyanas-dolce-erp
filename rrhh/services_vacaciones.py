from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.conf import settings
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Q, Sum
from django.utils import timezone

from core.access import can_manage_rrhh
from core.models import AuditLog

from .models import (
    AplicacionGoceVacaciones,
    Empleado,
    IncapacidadEmpleado,
    MovimientoVacaciones,
    PoliticaVacaciones,
    SolicitudVacaciones,
)
from .services_permisos import permiso_requiere_autorizacion_direccion, usuario_direccion_general_para_autorizacion
from .services_vacaciones_saldos import (
    consumir_reservas_goce,
    liberar_reservas_goce,
    reservar_goce_fifo,
    validar_reservas_goce,
)


DESCANSOS_OFICIALES_FIJOS = {
    (1, 1),
    (5, 1),
    (9, 16),
    (12, 25),
}


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + (n - 1) * 7)


def es_descanso_oficial(fecha: date) -> bool:
    if (fecha.month, fecha.day) in DESCANSOS_OFICIALES_FIJOS:
        return True
    if fecha == _nth_weekday(fecha.year, 2, 0, 1):
        return True
    if fecha == _nth_weekday(fecha.year, 3, 0, 3):
        return True
    if fecha == _nth_weekday(fecha.year, 11, 0, 3):
        return True
    return fecha.year >= 2024 and fecha.month == 10 and fecha.day == 1 and (fecha.year - 2024) % 6 == 0


def es_dia_laborable(fecha: date) -> bool:
    if fecha.weekday() == 6:
        return False
    return not es_descanso_oficial(fecha)


def contar_dias_laborables(fecha_inicio: date, fecha_fin: date) -> Decimal:
    if fecha_fin < fecha_inicio:
        raise ValidationError("La fecha final no puede ser anterior a la fecha inicial.")
    cursor = fecha_inicio
    dias = 0
    while cursor <= fecha_fin:
        if es_dia_laborable(cursor):
            dias += 1
        cursor += timedelta(days=1)
    return Decimal(dias)


def antiguedad_anios(empleado: Empleado, *, al: date | None = None) -> int:
    al = al or timezone.localdate()
    if not empleado.fecha_ingreso:
        return 0
    anios = al.year - empleado.fecha_ingreso.year
    if (al.month, al.day) < (empleado.fecha_ingreso.month, empleado.fecha_ingreso.day):
        anios -= 1
    return max(anios, 0)


def politica_para_empleado(empleado: Empleado, *, al: date | None = None) -> PoliticaVacaciones | None:
    anios = antiguedad_anios(empleado, al=al)
    return (
        PoliticaVacaciones.objects.filter(
            activo=True,
            antiguedad_desde__lte=anios,
        )
        .filter(antiguedad_hasta__isnull=True)
        .order_by("antiguedad_desde")
        .last()
        or PoliticaVacaciones.objects.filter(
            activo=True,
            antiguedad_desde__lte=anios,
            antiguedad_hasta__gte=anios,
        )
        .order_by("antiguedad_desde")
        .last()
    )


def dias_generados_para_empleado(empleado: Empleado, *, al: date | None = None) -> Decimal:
    politica = politica_para_empleado(empleado, al=al)
    return politica.dias_laborables if politica else Decimal("0")


def saldo_vacaciones_empleado(empleado: Empleado, *, periodo_anio: int | None = None, al: date | None = None) -> dict:
    al = al or timezone.localdate()
    periodo_anio = periodo_anio or al.year
    generado = dias_generados_para_empleado(empleado, al=al)
    movimientos = MovimientoVacaciones.objects.filter(empleado=empleado, periodo_anio=periodo_anio)
    consumido = movimientos.filter(tipo=MovimientoVacaciones.TIPO_CONSUMIDO).aggregate(total=Sum("dias"))["total"] or Decimal("0")
    reservado = movimientos.filter(tipo=MovimientoVacaciones.TIPO_RESERVADO).aggregate(total=Sum("dias"))["total"] or Decimal("0")
    liberado = movimientos.filter(tipo=MovimientoVacaciones.TIPO_LIBERADO).aggregate(total=Sum("dias"))["total"] or Decimal("0")
    ajuste = movimientos.filter(tipo=MovimientoVacaciones.TIPO_AJUSTE).aggregate(total=Sum("dias"))["total"] or Decimal("0")
    reservado_neto = max(reservado - liberado, Decimal("0"))
    disponible = generado + ajuste - consumido - reservado_neto
    return {
        "periodo_anio": periodo_anio,
        "generado": generado,
        "consumido": consumido,
        "reservado": reservado_neto,
        "disponible": disponible,
    }


def usuario_jefe_directo_vacaciones(empleado: Empleado):
    if permiso_requiere_autorizacion_direccion(empleado):
        direccion = usuario_direccion_general_para_autorizacion()
        if direccion:
            return direccion
    jefe = getattr(empleado, "jefe_directo", None)
    return getattr(jefe, "usuario_erp", None)


def can_gestionar_vacaciones_jefe(user, empleado: Empleado | None) -> bool:
    if not user or not getattr(user, "is_authenticated", False) or not empleado:
        return False
    if getattr(user, "is_superuser", False):
        return True
    return getattr(usuario_jefe_directo_vacaciones(empleado), "id", None) == user.id


def vacaciones_jefe_q(user) -> Q:
    if not user or not getattr(user, "is_authenticated", False):
        return Q(pk__in=[])
    if getattr(user, "is_superuser", False):
        return Q(pk__isnull=False)
    return Q(jefe_directo=user)


def can_resolver_vacaciones_jefe(user, solicitud: SolicitudVacaciones) -> bool:
    if not solicitud or solicitud.estado != SolicitudVacaciones.ESTADO_SOLICITADA:
        return False
    return can_gestionar_vacaciones_jefe(user, solicitud.empleado)


def goce_vacacional_fifo_activo() -> bool:
    return bool(getattr(settings, "VACACIONES_GOCE_FIFO_ACTIVO", False))


def _solicitud_usa_goce_fifo(solicitud: SolicitudVacaciones) -> bool:
    return solicitud.aplicaciones_goce.exists()


def _solicitud_tiene_reserva_legacy(solicitud: SolicitudVacaciones) -> bool:
    return solicitud.movimientos.filter(
        tipo=MovimientoVacaciones.TIPO_RESERVADO
    ).exists()


def _registrar_movimiento_legacy(
    solicitud: SolicitudVacaciones,
    *,
    tipo: str,
    descripcion: str,
    actor=None,
) -> MovimientoVacaciones:
    return MovimientoVacaciones.objects.create(
        empleado=solicitud.empleado,
        solicitud=solicitud,
        tipo=tipo,
        dias=solicitud.dias_laborables,
        periodo_anio=solicitud.fecha_inicio.year,
        descripcion=descripcion,
        actor=actor if getattr(actor, "is_authenticated", False) else None,
    )


def crear_solicitud_vacaciones(*, empleado: Empleado, fecha_inicio: date, fecha_fin: date, motivo: str, actor=None) -> SolicitudVacaciones:
    if not empleado or not empleado.activo:
        raise ValidationError("Selecciona un empleado activo.")
    dias = contar_dias_laborables(fecha_inicio, fecha_fin)
    if dias <= 0:
        raise ValidationError("El periodo no contiene días laborables.")
    with transaction.atomic():
        traslape = SolicitudVacaciones.objects.filter(
            empleado=empleado,
            estado__in=[
                SolicitudVacaciones.ESTADO_SOLICITADA,
                SolicitudVacaciones.ESTADO_PREAUTORIZADA,
                SolicitudVacaciones.ESTADO_APROBADA,
            ],
            fecha_inicio__lte=fecha_fin,
            fecha_fin__gte=fecha_inicio,
        ).exists()
        if traslape:
            raise ValidationError("Ya existe una solicitud de vacaciones en ese periodo.")
        incapacidad = (
            IncapacidadEmpleado.objects.filter(
                empleado=empleado,
                estado__in=[IncapacidadEmpleado.ESTADO_ACTIVA, IncapacidadEmpleado.ESTADO_CERRADA],
                fecha_inicio__lte=fecha_fin,
                fecha_fin__gte=fecha_inicio,
            )
            .order_by("fecha_inicio", "id")
            .first()
        )
        if incapacidad:
            folio = f" {incapacidad.folio}" if incapacidad.folio else ""
            raise ValidationError(
                f"El periodo cruza incapacidad{folio} del {incapacidad.fecha_inicio:%Y-%m-%d} al {incapacidad.fecha_fin:%Y-%m-%d}."
            )
        if not goce_vacacional_fifo_activo():
            saldo = saldo_vacaciones_empleado(
                empleado,
                periodo_anio=fecha_inicio.year,
                al=fecha_inicio,
            )
            if dias > saldo["disponible"]:
                raise ValidationError(
                    f"Saldo insuficiente. Disponible: {saldo['disponible']} días."
                )
        actor_autenticado = actor if getattr(actor, "is_authenticated", False) else None
        solicitud = SolicitudVacaciones.objects.create(
            empleado=empleado,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            dias_laborables=dias,
            motivo=motivo,
            jefe_directo=usuario_jefe_directo_vacaciones(empleado),
            creado_por=actor_autenticado,
        )
        if goce_vacacional_fifo_activo():
            aplicaciones = reservar_goce_fifo(solicitud, dias, actor=actor_autenticado)
            for aplicacion in aplicaciones:
                MovimientoVacaciones.objects.create(
                    empleado=empleado,
                    solicitud=solicitud,
                    tipo=MovimientoVacaciones.TIPO_RESERVADO,
                    dias=aplicacion.dias,
                    periodo_anio=aplicacion.periodo.aniversario.year,
                    descripcion=f"Reserva por solicitud {solicitud.folio}",
                    actor=actor_autenticado,
                )
        else:
            _registrar_movimiento_legacy(
                solicitud,
                tipo=MovimientoVacaciones.TIPO_RESERVADO,
                descripcion=f"Reserva por solicitud {solicitud.folio}",
                actor=actor_autenticado,
            )
    return solicitud


def preautorizar_solicitud_vacaciones_jefe(
    solicitud: SolicitudVacaciones,
    user,
    *,
    aprobar: bool,
) -> SolicitudVacaciones:
    with transaction.atomic():
        solicitud = SolicitudVacaciones.objects.select_for_update().get(pk=solicitud.pk)
        if not can_resolver_vacaciones_jefe(user, solicitud):
            raise PermissionDenied("Solo el jefe directo asignado puede resolver esta solicitud de vacaciones.")
        solicitud.preautorizado_por = user
        solicitud.fecha_preautorizacion = timezone.now()
        if aprobar:
            solicitud.estado = SolicitudVacaciones.ESTADO_PREAUTORIZADA
        else:
            descripcion = f"Liberación por rechazo de jefe {solicitud.folio}"
            if _solicitud_usa_goce_fifo(solicitud):
                liberar_reservas_goce(solicitud, actor=user, descripcion=descripcion)
            elif _solicitud_tiene_reserva_legacy(solicitud) or not goce_vacacional_fifo_activo():
                _registrar_movimiento_legacy(
                    solicitud,
                    tipo=MovimientoVacaciones.TIPO_LIBERADO,
                    descripcion=descripcion,
                    actor=user,
                )
            else:
                liberar_reservas_goce(solicitud, actor=user, descripcion=descripcion)
            solicitud.estado = SolicitudVacaciones.ESTADO_RECHAZADA
        solicitud.save(update_fields=["estado", "preautorizado_por", "fecha_preautorizacion", "actualizado_en"])
    return solicitud


def aprobar_solicitud_vacaciones_rrhh(solicitud: SolicitudVacaciones, user) -> SolicitudVacaciones:
    with transaction.atomic():
        solicitud = SolicitudVacaciones.objects.select_for_update().get(pk=solicitud.pk)
        if not can_manage_rrhh(user):
            raise PermissionDenied("Solo Capital Humano puede aprobar vacaciones.")
        if solicitud.estado not in {SolicitudVacaciones.ESTADO_SOLICITADA, SolicitudVacaciones.ESTADO_PREAUTORIZADA}:
            raise ValidationError("La solicitud ya fue resuelta.")
        if _solicitud_usa_goce_fifo(solicitud):
            if solicitud.fecha_fin < timezone.localdate():
                consumir_reservas_goce(solicitud, actor=user)
            else:
                validar_reservas_goce(solicitud)
        elif _solicitud_tiene_reserva_legacy(solicitud) or not goce_vacacional_fifo_activo():
            _registrar_movimiento_legacy(
                solicitud,
                tipo=MovimientoVacaciones.TIPO_LIBERADO,
                descripcion=f"Cierre de reserva por aprobación {solicitud.folio}",
                actor=user,
            )
            _registrar_movimiento_legacy(
                solicitud,
                tipo=MovimientoVacaciones.TIPO_CONSUMIDO,
                descripcion=f"Consumo por aprobación {solicitud.folio}",
                actor=user,
            )
        else:
            consumir_reservas_goce(solicitud, actor=user)
        solicitud.estado = SolicitudVacaciones.ESTADO_APROBADA
        solicitud.aprobado_rrhh_por = user
        solicitud.fecha_aprobacion_rrhh = timezone.now()
        solicitud.save(update_fields=["estado", "aprobado_rrhh_por", "fecha_aprobacion_rrhh", "actualizado_en"])
    return solicitud


def consumir_solicitudes_vacaciones_completadas(*, fecha_corte: date | None = None) -> int:
    """Consume reservas aprobadas únicamente después de terminar el goce programado."""
    fecha_corte = fecha_corte or timezone.localdate()
    solicitud_ids = list(
        SolicitudVacaciones.objects.filter(
            estado=SolicitudVacaciones.ESTADO_APROBADA,
            fecha_fin__lt=fecha_corte,
            aplicaciones_goce__estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
        )
        .values_list("id", flat=True)
        .distinct()
    )
    consumidas = 0
    for solicitud_id in solicitud_ids:
        with transaction.atomic():
            solicitud = SolicitudVacaciones.objects.select_for_update().get(pk=solicitud_id)
            if (
                solicitud.estado != SolicitudVacaciones.ESTADO_APROBADA
                or solicitud.fecha_fin >= fecha_corte
                or not solicitud.aplicaciones_goce.filter(
                    estado=AplicacionGoceVacaciones.ESTADO_RESERVADA
                ).exists()
            ):
                continue
            consumir_reservas_goce(solicitud, actor=None)
            consumidas += 1
    return consumidas


def reclasificar_solicitudes_futuras_consumidas(
    *,
    fecha_corte: date | None = None,
    aplicar: bool = False,
) -> dict[str, int]:
    """Devuelve a reserva el goce futuro consumido por el comportamiento anterior."""
    fecha_corte = fecha_corte or timezone.localdate()
    solicitud_ids = list(
        SolicitudVacaciones.objects.filter(
            estado=SolicitudVacaciones.ESTADO_APROBADA,
            fecha_fin__gte=fecha_corte,
            aplicaciones_goce__estado=AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
        )
        .values_list("id", flat=True)
        .distinct()
    )
    if not aplicar:
        aplicaciones = AplicacionGoceVacaciones.objects.filter(
            solicitud_id__in=solicitud_ids,
            estado=AplicacionGoceVacaciones.ESTADO_CONSUMIDA,
        ).count()
        return {"solicitudes": len(solicitud_ids), "aplicaciones": aplicaciones}

    solicitudes_actualizadas = 0
    aplicaciones_actualizadas = 0
    for solicitud_id in solicitud_ids:
        with transaction.atomic():
            solicitud = SolicitudVacaciones.objects.select_for_update().get(pk=solicitud_id)
            if (
                solicitud.estado != SolicitudVacaciones.ESTADO_APROBADA
                or solicitud.fecha_fin < fecha_corte
            ):
                continue
            aplicaciones = list(
                solicitud.aplicaciones_goce.select_for_update()
                .select_related("periodo")
                .filter(estado=AplicacionGoceVacaciones.ESTADO_CONSUMIDA)
            )
            if not aplicaciones:
                continue
            for aplicacion in aplicaciones:
                aplicacion.estado = AplicacionGoceVacaciones.ESTADO_RESERVADA
                aplicacion.save(update_fields=["estado"])
                MovimientoVacaciones.objects.create(
                    empleado_id=solicitud.empleado_id,
                    solicitud=solicitud,
                    tipo=MovimientoVacaciones.TIPO_AJUSTE,
                    dias=Decimal("0"),
                    periodo_anio=aplicacion.periodo.aniversario.year,
                    descripcion=(
                        "[regularización-goce-futuro] "
                        f"{aplicacion.dias} días pasan de consumidos a reservados "
                        f"hasta {solicitud.fecha_fin.isoformat()}."
                    ),
                )
            AuditLog.objects.create(
                action="REGULARIZE",
                model="rrhh.SolicitudVacaciones",
                object_id=str(solicitud.pk),
                payload={
                    "folio": solicitud.folio,
                    "motivo": "goce_futuro_consumido_a_reservado",
                    "fecha_corte": fecha_corte.isoformat(),
                    "aplicaciones": [
                        {
                            "periodo": aplicacion.periodo.aniversario.year,
                            "dias": str(aplicacion.dias),
                        }
                        for aplicacion in aplicaciones
                    ],
                },
            )
            solicitudes_actualizadas += 1
            aplicaciones_actualizadas += len(aplicaciones)
    return {
        "solicitudes": solicitudes_actualizadas,
        "aplicaciones": aplicaciones_actualizadas,
    }


def rechazar_solicitud_vacaciones(solicitud: SolicitudVacaciones, user) -> SolicitudVacaciones:
    with transaction.atomic():
        solicitud = SolicitudVacaciones.objects.select_for_update().get(pk=solicitud.pk)
        if not can_manage_rrhh(user):
            raise PermissionDenied("Solo Capital Humano puede rechazar vacaciones.")
        if solicitud.estado in {SolicitudVacaciones.ESTADO_APROBADA, SolicitudVacaciones.ESTADO_RECHAZADA, SolicitudVacaciones.ESTADO_CANCELADA}:
            raise ValidationError("La solicitud ya fue resuelta.")
        descripcion = f"Liberación por rechazo {solicitud.folio}"
        if _solicitud_usa_goce_fifo(solicitud):
            liberar_reservas_goce(solicitud, actor=user, descripcion=descripcion)
        elif _solicitud_tiene_reserva_legacy(solicitud) or not goce_vacacional_fifo_activo():
            _registrar_movimiento_legacy(
                solicitud,
                tipo=MovimientoVacaciones.TIPO_LIBERADO,
                descripcion=descripcion,
                actor=user,
            )
        else:
            liberar_reservas_goce(solicitud, actor=user, descripcion=descripcion)
        solicitud.estado = SolicitudVacaciones.ESTADO_RECHAZADA
        solicitud.aprobado_rrhh_por = user
        solicitud.fecha_aprobacion_rrhh = timezone.now()
        solicitud.save(update_fields=["estado", "aprobado_rrhh_por", "fecha_aprobacion_rrhh", "actualizado_en"])
    return solicitud
