from __future__ import annotations

import calendar
from datetime import date
from decimal import Decimal

from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.utils import timezone

from core.access import ROLE_ADMIN, ROLE_DG, has_any_role

from .models import Prestamo, PrestamoCuota


def _siguiente_quincena(f: date) -> date:
    if f.day <= 15:
        ultimo = calendar.monthrange(f.year, f.month)[1]
        return date(f.year, f.month, ultimo)
    mes = f.month + 1 if f.month < 12 else 1
    anio = f.year if f.month < 12 else f.year + 1
    return date(anio, mes, 15)


def generar_cuotas(prestamo: Prestamo) -> list[PrestamoCuota]:
    """
    Genera las cuotas proyectadas a partir de fecha_deposito o fecha_solicitud.
    La primera cuota cae en la siguiente quincena de la fecha base.
    """
    cuotas = []
    fecha = _siguiente_quincena(prestamo.fecha_deposito or prestamo.fecha_solicitud)

    for i in range(1, prestamo.num_quincenas + 1):
        cuotas.append(
            PrestamoCuota(
                prestamo=prestamo,
                numero_quincena=i,
                fecha_quincena=fecha,
                monto_esperado=prestamo.descuento_quincenal,
                estado=PrestamoCuota.ESTADO_PENDIENTE,
            )
        )
        fecha = _siguiente_quincena(fecha)

    PrestamoCuota.objects.bulk_create(cuotas, ignore_conflicts=True)
    return cuotas


def can_autorizar_prestamo_jefe(user, prestamo: Prestamo) -> bool:
    if not user or not user.is_authenticated or not prestamo:
        return False
    if prestamo.estado != Prestamo.ESTADO_SOLICITADO:
        return False
    if not usuario_equivale_jefe_prestamo(user, prestamo):
        return False
    return not _same_user_or_email(getattr(prestamo.empleado, "usuario_erp", None), user)


def prestamos_jefe_q(user) -> Q:
    if not user or not user.is_authenticated:
        return Q(pk__in=[])
    filtro = Q(jefe_directo=user)
    email = _email(user)
    if email:
        filtro |= Q(jefe_directo__email__iexact=email)
    return filtro


def usuario_equivale_jefe_prestamo(user, prestamo: Prestamo) -> bool:
    if not user or not user.is_authenticated or not getattr(prestamo, "jefe_directo_id", None):
        return False
    if prestamo.jefe_directo_id == user.id:
        return True
    return _email(user) and _email(prestamo.jefe_directo) == _email(user)


def _same_user_or_email(left, right) -> bool:
    if not left or not right:
        return False
    if getattr(left, "id", None) == getattr(right, "id", None):
        return True
    return bool(_email(left) and _email(left) == _email(right))


def _email(user) -> str:
    return (getattr(user, "email", "") or "").strip().lower()


def autorizar_prestamo_jefe(prestamo: Prestamo, user) -> Prestamo:
    if not can_autorizar_prestamo_jefe(user, prestamo):
        raise PermissionDenied("Solo el jefe directo asignado puede autorizar este préstamo.")
    prestamo.firma_jefe = True
    prestamo.autorizado_jefe = user
    prestamo.fecha_auth_jefe = timezone.now()
    prestamo.estado = Prestamo.ESTADO_AUTORIZADO
    prestamo.save(update_fields=["firma_jefe", "autorizado_jefe", "fecha_auth_jefe", "estado", "actualizado_en"])
    return prestamo


def can_autorizar_prestamo_direccion(user, prestamo: Prestamo | None = None) -> bool:
    if not user or not user.is_authenticated:
        return False
    if not (user.is_superuser or has_any_role(user, ROLE_DG, ROLE_ADMIN)):
        return False
    if prestamo and getattr(prestamo.empleado, "usuario_erp_id", None) == user.id:
        return False
    return True


def aprobar_prestamo_direccion(prestamo: Prestamo, user) -> Prestamo:
    if not can_autorizar_prestamo_direccion(user, prestamo):
        raise PermissionDenied("Solo Dirección puede aprobar préstamos y generar cuotas.")
    if prestamo.estado != Prestamo.ESTADO_AUTORIZADO:
        raise PermissionDenied("El préstamo requiere autorización previa del jefe directo.")
    prestamo.firma_direccion = True
    prestamo.autorizado_dg = user
    prestamo.fecha_auth_dg = timezone.now()
    prestamo.estado = Prestamo.ESTADO_ACTIVO
    prestamo.save(update_fields=["firma_direccion", "autorizado_dg", "fecha_auth_dg", "estado", "actualizado_en"])
    generar_cuotas(prestamo)
    return prestamo


def aplicar_cobro_manual(
    cuota: PrestamoCuota,
    monto: Decimal,
    user,
    nota: str = "",
    *,
    fuente: str = PrestamoCuota.FUENTE_MANUAL,
) -> PrestamoCuota:
    """
    Registra un cobro sobre una cuota y recalcula el saldo del préstamo.
    """
    monto = Decimal(str(monto or "0")).quantize(Decimal("0.01"))
    cuota.monto_cobrado = monto
    cuota.fecha_cobro = date.today()
    cuota.registrado_por = user
    cuota.fuente = fuente
    cuota.nota = nota

    if monto >= cuota.monto_esperado:
        cuota.estado = PrestamoCuota.ESTADO_COBRADO
    elif monto > 0:
        cuota.estado = PrestamoCuota.ESTADO_PARCIAL
    else:
        cuota.estado = PrestamoCuota.ESTADO_OMITIDO

    cuota.save()
    cuota.prestamo.recalcular_saldo()
    return cuota
