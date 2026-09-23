"""Resolución de horas extra compartida por web y APIs, con bloqueo por jornada."""
from decimal import Decimal

from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.utils import timezone

from .models import HoraExtra
from .services_extra_bloqueos import bloquear_hora_extra
from .services_extra_conciliacion import (
    contexto_hora_extra,
    diagnosticar_horas_extra,
    es_bloque_extra_autorizable,
    evidencia_ajuste_extra,
    saldo_automatico_esperado,
)
from .services_horas_extra_jefatura import jefatura_hora_extra_actualizada


def _usuario_puede_resolver(he, usuario, permitir_superusuario):
    return he.jefe_directo_id == usuario.id or (permitir_superusuario and usuario.is_superuser)


def _append_nota_operativa(hora_extra, nota):
    hora_extra.notas = '\n'.join(filter(None, [(hora_extra.notas or '').strip(), nota.strip()]))


@transaction.atomic
def ajustar_hora_extra_pendiente(
    hora_extra_id,
    usuario,
    *,
    horas,
    motivo,
    permitir_superusuario=True,
):
    """Ajusta una propuesta automática pendiente al bloque acordado por su autorizador."""
    he, registros_dia = bloquear_hora_extra(hora_extra_id)
    if not jefatura_hora_extra_actualizada(he):
        return he, '', 'La jefatura de esta detección cambió. Sincroniza el jefe directo antes de editarla.'
    if not _usuario_puede_resolver(he, usuario, permitir_superusuario):
        raise PermissionDenied('Solo el jefe directo asignado puede editar esta hora extra.')
    if he.estado != HoraExtra.ESTADO_PENDIENTE:
        return he, '', 'La hora extra ya no está pendiente. Recarga la lista para revisar su estado actual.'
    if not he.asistencia_id:
        return he, '', 'Esta edición aplica únicamente a propuestas automáticas.'
    if (he.asistencia.empleado_id, he.asistencia.fecha) != (he.empleado_id, he.fecha):
        return he, '', 'La asistencia vinculada no corresponde al empleado y fecha. Corrige y reevalúa antes de editar.'

    motivo = (motivo or '').strip()
    if not motivo:
        return he, '', 'El motivo del ajuste es obligatorio.'
    try:
        horas = Decimal(horas).quantize(Decimal('0.01'))
    except (TypeError, ValueError, ArithmeticError):
        return he, '', 'Captura un tiempo válido en bloques de 30 minutos.'
    if not es_bloque_extra_autorizable(horas):
        return he, '', 'El tiempo a autorizar debe usar bloques de 30 minutos.'

    diagnostico = diagnosticar_horas_extra(he.asistencia)
    saldo = saldo_automatico_esperado(diagnostico, registros_dia, he)
    if saldo is None or saldo <= 0:
        return he, '', 'Ya no existe un saldo automático vigente para ajustar.'
    if he.horas == horas:
        return he, '', 'El tiempo a autorizar no cambió.'

    he.horas = horas
    he.ajuste_autorizacion = evidencia_ajuste_extra(
        he, saldo, motivo, usuario.get_username(),
    )
    _append_nota_operativa(
        he,
        f'Correccion registrada por {usuario.get_username()} el '
        f'{timezone.localtime():%Y-%m-%d %H:%M}: {motivo}',
    )
    he.save(update_fields=['horas', 'ajuste_autorizacion', 'notas'])
    return he, f'Tiempo extra ajustado para {he.empleado.nombre}.', ''


@transaction.atomic
def resolver_hora_extra(hora_extra_id, action, usuario, *, permitir_superusuario=True):
    """Bloquea asistencia antes de extras, también para capturas manuales.

    Las señales de HoraExtra pueden invocar el generador; comparten este orden.
    Los adaptadores conservan su alcance de consulta y formato de respuesta.
    """
    he, registros_dia = bloquear_hora_extra(hora_extra_id)
    if not jefatura_hora_extra_actualizada(he):
        return he, "", "La jefatura de esta detección cambió. Sincroniza el jefe directo antes de resolverla."
    if not _usuario_puede_resolver(he, usuario, permitir_superusuario):
        verbo = "rechazar" if action == "rechazar" else "autorizar"
        raise PermissionDenied(f"Solo el jefe directo asignado puede {verbo} esta hora extra.")
    if action not in {"autorizar", "rechazar"}:
        return he, "", "La acción solicitada no es válida."
    if he.estado != HoraExtra.ESTADO_PENDIENTE:
        return he, "", "La hora extra ya no está pendiente. Recarga la lista para revisar su estado actual."
    if action == "autorizar":
        if he.asistencia_id and (he.asistencia.empleado_id, he.asistencia.fecha) != (he.empleado_id, he.fecha):
            return he, "", "La asistencia vinculada no corresponde al empleado y fecha. Corrige y reevalúa antes de autorizar."
        if he.asistencia_id and not es_bloque_extra_autorizable(he.horas):
            return he, "", "Ajusta el tiempo a bloques de 30 minutos antes de autorizar."
        contexto = contexto_hora_extra(he, registros_dia)
        if not contexto["puede_autorizar"]:
            return he, "", contexto["motivo_bloqueo"]
        from .services import calcular_monto_hora_extra

        calcular_monto_hora_extra(he)
        he.estado = HoraExtra.ESTADO_AUTORIZADO
        message = f"Hora extra autorizada para {he.empleado.nombre}."
    else:
        he.estado = HoraExtra.ESTADO_RECHAZADO
        message = f"Hora extra rechazada para {he.empleado.nombre}."
    he.autorizado_por = usuario
    he.fecha_autorizacion_jefe = timezone.now()
    he.save(update_fields=["estado", "autorizado_por", "fecha_autorizacion_jefe"])
    return he, message, ""
