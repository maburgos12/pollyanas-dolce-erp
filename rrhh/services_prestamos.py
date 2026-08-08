from __future__ import annotations

import calendar
from datetime import date
from decimal import Decimal

from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from rest_framework.exceptions import ValidationError

from core.access import ROLE_ADMIN, ROLE_DG, has_any_role
from core.notificaciones import notificar_prestamo_solicitado

from .models import Empleado, Prestamo, PrestamoCuota
from .services import usuario_jefe_directo_de_empleado


@transaction.atomic
def crear_solicitud_prestamo(
    *,
    empleado: Empleado,
    actor,
    concepto: str,
    metodo_pago: str,
    importe,
    num_quincenas,
    fecha_deposito: date | None = None,
    require_active_manager: bool = True,
) -> Prestamo:
    """Crea una solicitud sin permitir que el cliente controle autorizaciones."""
    empleado = (
        Empleado.objects.select_for_update(of=("self",))
        .select_related("jefe_directo__usuario_erp")
        .get(pk=empleado.pk)
    )
    concepto = (concepto or "").strip()
    if not concepto:
        raise ValidationError({"concepto": "El concepto es obligatorio."})
    if metodo_pago not in dict(Prestamo.METODO_CHOICES):
        raise ValidationError({"metodo_pago": "Selecciona un metodo de pago valido."})

    try:
        importe = Decimal(str(importe))
    except (TypeError, ValueError, ArithmeticError):
        raise ValidationError({"importe": "El importe debe ser numerico."}) from None
    if importe <= 0:
        raise ValidationError({"importe": "El importe debe ser mayor a cero."})

    try:
        num_quincenas = int(num_quincenas)
    except (TypeError, ValueError):
        raise ValidationError({"num_quincenas": "Indica un numero de quincenas valido."}) from None
    if num_quincenas <= 0:
        raise ValidationError({"num_quincenas": "Indica al menos una quincena."})

    jefe_directo = usuario_jefe_directo_de_empleado(empleado)
    if require_active_manager and (not jefe_directo or not jefe_directo.is_active):
        raise ValidationError(
            {"empleado": "El empleado no tiene una jefa directa activa con usuario ERP."}
        )

    deuda = Prestamo.objects.filter(
        empleado=empleado,
        estado__in=[
            Prestamo.ESTADO_SOLICITADO,
            Prestamo.ESTADO_AUTORIZADO,
            Prestamo.ESTADO_APROBADO,
            Prestamo.ESTADO_ACTIVO,
        ],
        saldo_actual__gt=0,
    ).first()
    if deuda:
        raise ValidationError(
            {"empleado": f"Aun tiene un prestamo vigente: {deuda.folio} · saldo ${deuda.saldo_actual}."}
        )

    descuento = (importe / Decimal(str(num_quincenas))).quantize(Decimal("0.01"))
    prestamo = Prestamo.objects.create(
        empleado=empleado,
        concepto=concepto,
        metodo_pago=metodo_pago,
        fecha_solicitud=timezone.localdate(),
        fecha_deposito=fecha_deposito,
        importe=importe,
        num_quincenas=num_quincenas,
        descuento_quincenal=descuento,
        saldo_actual=importe,
        estado=Prestamo.ESTADO_SOLICITADO,
        jefe_directo=jefe_directo,
        creado_por=actor,
    )
    notificar_prestamo_solicitado(prestamo, actor=actor)
    return prestamo


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
    if _same_user_or_email(getattr(prestamo.empleado, "usuario_erp", None), user):
        return False
    return getattr(user, "is_superuser", False) or usuario_equivale_jefe_prestamo(user, prestamo)


def prestamos_jefe_q(user) -> Q:
    if not user or not user.is_authenticated:
        return Q(pk__in=[])
    if getattr(user, "is_superuser", False):
        return Q(pk__isnull=False)
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


def prestamos_por_autorizar_q(user) -> Q:
    if not user or not user.is_authenticated:
        return Q(pk__in=[])

    pendientes = Q(estado=Prestamo.ESTADO_SOLICITADO) & prestamos_jefe_q(user)
    if can_autorizar_prestamo_direccion(user):
        pendientes |= Q(estado=Prestamo.ESTADO_AUTORIZADO) & ~Q(empleado__usuario_erp=user)
    return pendientes


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
