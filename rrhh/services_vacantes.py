from __future__ import annotations

from datetime import date

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from core.access import ROLE_ADMIN, ROLE_DG, can_manage_submodule, can_view_submodule, has_any_role
from core.models import Notificacion
from core.notificaciones import crear_notificaciones, usuarios_direccion_general, usuarios_por_grupo

from .models import Empleado, VacanteCobertura, VacanteMovimiento, VacanteRRHH


def _normalizar_texto(value: str | None) -> str:
    return (value or "").strip().upper()


def _as_int(value, default: int = 1) -> int:
    try:
        return max(int(value or default), 1)
    except (TypeError, ValueError):
        return default


def can_gestionar_vacantes(user) -> bool:
    return bool(can_manage_submodule(user, "rrhh", "vacantes"))


def can_ver_vacante(user, vacante: VacanteRRHH | None = None) -> bool:
    if not user or not user.is_authenticated:
        return False
    if can_view_submodule(user, "rrhh", "vacantes") or _es_direccion(user):
        return True
    if vacante and vacante.solicitado_por_id == user.id:
        return True
    return False


def can_aprobar_vacante_direccion(user, vacante: VacanteRRHH | None = None) -> bool:
    if not user or not user.is_authenticated:
        return False
    if not _es_direccion(user):
        return False
    if vacante and vacante.solicitado_por_id == user.id:
        return False
    return True


def crear_solicitud_vacante(
    *,
    area: str,
    puesto: str,
    fecha_solicitada: date | None = None,
    solicitado_por=None,
    creado_por=None,
    sucursal=None,
    departamento: str = "",
    cantidad_solicitada: int = 1,
    tipo_solicitud: str = VacanteRRHH.TIPO_REEMPLAZO,
    prioridad: str = VacanteRRHH.PRIORIDAD_NORMAL,
    fecha_necesaria: date | None = None,
    motivo_solicitud: str = "",
    sugerencias: str = "",
) -> VacanteRRHH:
    with transaction.atomic():
        vacante = VacanteRRHH.objects.create(
            area=_normalizar_texto(area),
            puesto=_normalizar_texto(puesto),
            sucursal=sucursal,
            departamento=_normalizar_texto(departamento),
            cantidad_solicitada=_as_int(cantidad_solicitada),
            tipo_solicitud=tipo_solicitud or VacanteRRHH.TIPO_REEMPLAZO,
            prioridad=prioridad or VacanteRRHH.PRIORIDAD_NORMAL,
            fecha_solicitada=fecha_solicitada or timezone.localdate(),
            fecha_necesaria=fecha_necesaria,
            estado=VacanteRRHH.ESTADO_SOLICITADA,
            motivo_solicitud=(motivo_solicitud or "").strip(),
            sugerencias=(sugerencias or "").strip(),
            solicitado_por=solicitado_por,
            creado_por=creado_por or solicitado_por,
        )
        _registrar_movimiento(vacante, "", VacanteRRHH.ESTADO_SOLICITADA, creado_por or solicitado_por, "Solicitud creada")
    notificar_vacante_solicitada(vacante, actor=creado_por or solicitado_por)
    return vacante


def enviar_vacante_direccion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    _require_rrhh(user)
    if vacante.estado not in {
        VacanteRRHH.ESTADO_SOLICITADA,
        VacanteRRHH.ESTADO_REVISION_RRHH,
        VacanteRRHH.ESTADO_AUTORIZADA,
    }:
        raise ValidationError("La vacante no está en un estado válido para enviarse a Dirección.")
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_PENDIENTE_DIRECCION,
        user,
        comentario or "Validada por Capital Humano y enviada a Dirección.",
        extra_updates={
            "validado_rrhh_por": user,
            "fecha_validacion_rrhh": timezone.now(),
        },
        notify=lambda updated: notificar_vacante_para_direccion(updated, actor=user),
    )


def aprobar_vacante_direccion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    if not can_aprobar_vacante_direccion(user, vacante):
        raise PermissionDenied("Solo Dirección puede aprobar vacantes y no puede aprobar solicitudes propias.")
    if vacante.estado != VacanteRRHH.ESTADO_PENDIENTE_DIRECCION:
        raise ValidationError("La vacante debe estar pendiente de Dirección para aprobarse.")
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_AUTORIZADA,
        user,
        comentario or "Vacante aprobada por Dirección.",
        extra_updates={
            "autorizado_por": user,
            "fecha_autorizacion": timezone.now(),
        },
        notify=lambda updated: notificar_vacante_aprobada(updated, actor=user),
    )


def rechazar_vacante_direccion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    if not can_aprobar_vacante_direccion(user, vacante):
        raise PermissionDenied("Solo Dirección puede rechazar vacantes y no puede rechazar solicitudes propias.")
    if vacante.estado != VacanteRRHH.ESTADO_PENDIENTE_DIRECCION:
        raise ValidationError("La vacante debe estar pendiente de Dirección para rechazarse.")
    now = timezone.now()
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_RECHAZADA,
        user,
        comentario or "Vacante rechazada por Dirección.",
        extra_updates={
            "rechazado_por": user,
            "fecha_rechazo": now,
            "motivo_rechazo": (comentario or "").strip(),
        },
        notify=lambda updated: notificar_vacante_rechazada(updated, actor=user),
    )


def iniciar_reclutamiento_vacante(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    _require_rrhh(user)
    if vacante.estado not in {
        VacanteRRHH.ESTADO_SOLICITADA,
        VacanteRRHH.ESTADO_AUTORIZADA,
        VacanteRRHH.ESTADO_PAUSADA,
    }:
        raise ValidationError("La vacante no puede pasar a reclutamiento desde su estado actual.")
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_RECLUTAMIENTO,
        user,
        comentario or "Capital Humano inicia reclutamiento.",
    )


def pausar_vacante(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    _require_rrhh(user)
    if vacante.estado in {VacanteRRHH.ESTADO_CUBIERTA, VacanteRRHH.ESTADO_CANCELADA, VacanteRRHH.ESTADO_RECHAZADA}:
        raise ValidationError("La vacante no puede pausarse desde su estado actual.")
    return _transition(vacante, VacanteRRHH.ESTADO_PAUSADA, user, comentario or "Vacante pausada.")


def cancelar_vacante(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    _require_rrhh(user)
    if vacante.estado == VacanteRRHH.ESTADO_CUBIERTA:
        raise ValidationError("Una vacante cubierta no puede cancelarse.")
    return _transition(vacante, VacanteRRHH.ESTADO_CANCELADA, user, comentario or "Vacante cancelada.")


def cubrir_vacante(
    vacante: VacanteRRHH,
    empleado: Empleado,
    user,
    *,
    fecha_cobertura: date | None = None,
    nota: str = "",
) -> VacanteCobertura:
    _require_rrhh(user)
    if vacante.estado not in {
        VacanteRRHH.ESTADO_RECLUTAMIENTO,
        VacanteRRHH.ESTADO_AUTORIZADA,
        VacanteRRHH.ESTADO_SOLICITADA,
    }:
        raise ValidationError("La vacante no acepta cobertura desde su estado actual.")
    with transaction.atomic():
        vacante = VacanteRRHH.objects.select_for_update().get(pk=vacante.pk)
        cobertura = VacanteCobertura.objects.create(
            vacante=vacante,
            empleado=empleado,
            fecha_cobertura=fecha_cobertura or timezone.localdate(),
            nota=(nota or "").strip(),
            creado_por=user,
        )
        estado_anterior = vacante.estado
        vacante.empleado_cubrio = empleado
        if vacante.coberturas.count() >= vacante.cantidad_solicitada:
            vacante.estado = VacanteRRHH.ESTADO_CUBIERTA
            vacante.fecha_cubierta = cobertura.fecha_cobertura
        else:
            vacante.estado = VacanteRRHH.ESTADO_RECLUTAMIENTO
        vacante.save(update_fields=["empleado_cubrio", "estado", "fecha_cubierta", "actualizado_en"])
        _registrar_movimiento(
            vacante,
            estado_anterior,
            vacante.estado,
            user,
            nota or f"Cobertura registrada con {empleado.nombre}.",
        )
    notificar_vacante_cubierta(vacante, actor=user)
    return cobertura


def notificar_vacante_solicitada(vacante: VacanteRRHH, *, actor=None) -> int:
    return crear_notificaciones(
        usuarios_por_grupo("RRHH"),
        titulo=f"Vacante solicitada: {vacante.area} / {vacante.puesto}",
        mensaje=f"{vacante.folio} · {vacante.cantidad_solicitada} plaza(s) · {vacante.get_prioridad_display()}",
        url=f"/rrhh/vacantes/{vacante.id}/",
        tipo=Notificacion.TIPO_SISTEMA,
        prioridad=Notificacion.PRIORIDAD_ALTA if vacante.prioridad != VacanteRRHH.PRIORIDAD_NORMAL else Notificacion.PRIORIDAD_NORMAL,
        actor=actor,
        objeto_tipo="rrhh.VacanteRRHH",
        objeto_id=vacante.id,
        excluir=actor,
    )


def notificar_vacante_para_direccion(vacante: VacanteRRHH, *, actor=None) -> int:
    return crear_notificaciones(
        usuarios_direccion_general(),
        titulo=f"Vacante para aprobar: {vacante.area} / {vacante.puesto}",
        mensaje=f"{vacante.folio} validada por Capital Humano. Falta Dirección.",
        url=f"/rrhh/vacantes/{vacante.id}/",
        tipo=Notificacion.TIPO_SISTEMA,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="rrhh.VacanteRRHH",
        objeto_id=vacante.id,
        excluir=actor,
    )


def notificar_vacante_aprobada(vacante: VacanteRRHH, *, actor=None) -> int:
    return crear_notificaciones(
        _usuarios_interesados(vacante),
        titulo=f"Vacante aprobada: {vacante.area} / {vacante.puesto}",
        mensaje=f"{vacante.folio} aprobada por Dirección.",
        url=f"/rrhh/vacantes/{vacante.id}/",
        tipo=Notificacion.TIPO_SISTEMA,
        actor=actor,
        objeto_tipo="rrhh.VacanteRRHH",
        objeto_id=vacante.id,
        excluir=actor,
    )


def notificar_vacante_rechazada(vacante: VacanteRRHH, *, actor=None) -> int:
    return crear_notificaciones(
        _usuarios_interesados(vacante),
        titulo=f"Vacante rechazada: {vacante.area} / {vacante.puesto}",
        mensaje=f"{vacante.folio} rechazada por Dirección.",
        url=f"/rrhh/vacantes/{vacante.id}/",
        tipo=Notificacion.TIPO_SISTEMA,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="rrhh.VacanteRRHH",
        objeto_id=vacante.id,
        excluir=actor,
    )


def notificar_vacante_cubierta(vacante: VacanteRRHH, *, actor=None) -> int:
    if vacante.estado != VacanteRRHH.ESTADO_CUBIERTA:
        return 0
    return crear_notificaciones(
        _usuarios_interesados(vacante),
        titulo=f"Vacante cubierta: {vacante.area} / {vacante.puesto}",
        mensaje=f"{vacante.folio} quedó cubierta.",
        url=f"/rrhh/vacantes/{vacante.id}/",
        tipo=Notificacion.TIPO_SISTEMA,
        actor=actor,
        objeto_tipo="rrhh.VacanteRRHH",
        objeto_id=vacante.id,
        excluir=actor,
    )


def _transition(
    vacante: VacanteRRHH,
    nuevo_estado: str,
    user,
    comentario: str,
    *,
    extra_updates: dict | None = None,
    notify=None,
) -> VacanteRRHH:
    with transaction.atomic():
        vacante = VacanteRRHH.objects.select_for_update().get(pk=vacante.pk)
        estado_anterior = vacante.estado
        vacante.estado = nuevo_estado
        update_fields = ["estado", "actualizado_en"]
        for field, value in (extra_updates or {}).items():
            setattr(vacante, field, value)
            update_fields.append(field)
        vacante.save(update_fields=update_fields)
        _registrar_movimiento(vacante, estado_anterior, nuevo_estado, user, comentario)
    if notify:
        notify(vacante)
    return vacante


def _registrar_movimiento(vacante: VacanteRRHH, estado_anterior: str, estado_nuevo: str, actor, comentario: str):
    return VacanteMovimiento.objects.create(
        vacante=vacante,
        estado_anterior=estado_anterior or "",
        estado_nuevo=estado_nuevo,
        actor=actor if getattr(actor, "is_authenticated", False) else None,
        comentario=(comentario or "").strip(),
    )


def _require_rrhh(user) -> None:
    if not can_gestionar_vacantes(user):
        raise PermissionDenied("Solo Capital Humano puede gestionar solicitudes de vacantes.")


def _es_direccion(user) -> bool:
    return bool(user and user.is_authenticated and (user.is_superuser or has_any_role(user, ROLE_DG, ROLE_ADMIN)))


def _usuarios_interesados(vacante: VacanteRRHH) -> list:
    User = get_user_model()
    ids = {vacante.solicitado_por_id, vacante.creado_por_id}
    users = list(User.objects.filter(id__in=[user_id for user_id in ids if user_id]))
    users.extend(usuarios_por_grupo("RRHH"))
    return users
