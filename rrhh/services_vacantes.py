from __future__ import annotations

from datetime import date

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from core.access import (
    ROLE_ADMIN,
    ROLE_DG,
    ROLE_RRHH,
    can_manage_submodule,
    has_any_role,
)
from core.models import Notificacion
from core.notificaciones import crear_notificaciones, usuarios_direccion_general, usuarios_por_grupo

from .models import (
    AltaPendienteEmpleado,
    CandidatoVacante,
    Empleado,
    VacanteCobertura,
    VacanteMovimiento,
    VacanteRRHH,
    VacanteSeguimiento,
)
from .services_niveles import empleado_es_liderazgo, jefatura_q, liderazgo_q


def _normalizar_texto(value: str | None) -> str:
    return (value or "").strip().upper()


def _as_int(value, default: int = 1) -> int:
    try:
        return max(int(value or default), 1)
    except (TypeError, ValueError):
        return default


DIRECCION_DEPARTAMENTOS = {
    Empleado.DEP_ADMINISTRACION,
    Empleado.DEP_VENTAS,
    Empleado.DEP_PRODUCCION,
    Empleado.DEP_RRHH,
    Empleado.DEP_COMPRAS,
    Empleado.DEP_LOGISTICA,
}

DIRECCION_PUESTO_KEYWORDS = (
    "JEFE",
    "JEFATURA",
    "ENCARGAD",
    "RESPONSABLE",
    "COORDINADOR",
    "LIDER",
    "SUPERVISOR",
    "GERENTE",
    "DIRECTOR",
    "DIRECCION",
    "DIRECCIÓN",
)

def can_gestionar_vacantes(user) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return bool(has_any_role(user, ROLE_RRHH) and can_manage_submodule(user, "rrhh", "vacantes"))


def can_solicitar_vacantes(user) -> bool:
    if not user or not user.is_authenticated:
        return False
    if can_gestionar_vacantes(user) or _es_direccion(user):
        return True
    return _usuario_tiene_jefatura(user)


def can_ver_vacante(user, vacante: VacanteRRHH | None = None) -> bool:
    if not user or not user.is_authenticated:
        return False
    if can_gestionar_vacantes(user):
        return True
    if _es_direccion(user):
        return True
    if vacante and vacante.solicitado_por_id == user.id:
        return True
    if vacante and vacante.creado_por_id == user.id:
        return True
    if vacante and (vacante.autorizador_asignado_id == user.id or vacante.autorizado_por_id == user.id):
        return True
    return False


def can_autorizar_vacante(user, vacante: VacanteRRHH | None = None) -> bool:
    if not user or not user.is_authenticated:
        return False
    if vacante is None:
        return vacantes_por_autorizar_count(user) > 0
    if vacante.estado != VacanteRRHH.ESTADO_PENDIENTE_DIRECCION:
        return False
    if vacante.solicitado_por_id == user.id or vacante.creado_por_id == user.id:
        return False
    if vacante.requiere_direccion:
        return _es_direccion(user)
    if vacante.autorizador_asignado_id:
        return vacante.autorizador_asignado_id == user.id
    return False


def can_aprobar_vacante_direccion(user, vacante: VacanteRRHH | None = None) -> bool:
    if vacante and not vacante.requiere_direccion:
        return False
    return can_autorizar_vacante(user, vacante)


def filtrar_vacantes_para_usuario(user, qs=None):
    qs = qs if qs is not None else VacanteRRHH.objects.all()
    if can_gestionar_vacantes(user) or _es_direccion(user):
        return qs
    if not user or not user.is_authenticated:
        return qs.none()

    filtros = (
        Q(solicitado_por=user)
        | Q(creado_por=user)
        | Q(autorizador_asignado=user)
        | Q(autorizado_por=user)
    )
    return qs.filter(filtros).distinct()


def vacantes_por_autorizar_count(user) -> int:
    if not user or not user.is_authenticated:
        return 0
    qs = VacanteRRHH.objects.filter(estado=VacanteRRHH.ESTADO_PENDIENTE_DIRECCION)
    filtros = Q(autorizador_asignado=user)
    if _es_direccion(user):
        filtros |= Q(requiere_direccion=True)
    return qs.filter(filtros).exclude(Q(solicitado_por=user) | Q(creado_por=user)).distinct().count()


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
    estado_inicial: str = VacanteRRHH.ESTADO_SOLICITADA,
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
            estado=estado_inicial or VacanteRRHH.ESTADO_SOLICITADA,
            motivo_solicitud=(motivo_solicitud or "").strip(),
            sugerencias=(sugerencias or "").strip(),
            solicitado_por=solicitado_por,
            creado_por=creado_por or solicitado_por,
        )
        _registrar_movimiento(vacante, "", vacante.estado, creado_por or solicitado_por, "Solicitud creada")
    notificar_vacante_solicitada(vacante, actor=creado_por or solicitado_por)
    return vacante


def enviar_vacante_autorizacion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    _require_rrhh(user)
    if vacante.estado not in {
        VacanteRRHH.ESTADO_SOLICITADA,
        VacanteRRHH.ESTADO_REVISION_RRHH,
    }:
        raise ValidationError("La vacante no está en un estado válido para enviarse a autorización.")
    requiere_direccion = vacante_requiere_autorizacion_direccion(vacante)
    autorizador = None if requiere_direccion else resolver_autorizador_vacante(vacante)
    if not requiere_direccion and not autorizador:
        raise ValidationError("No hay jefe directo asignado para autorizar esta vacante. Revisa Organización de Capital Humano.")
    extra_updates = {
        "validado_rrhh_por": user,
        "fecha_validacion_rrhh": timezone.now(),
        "requiere_direccion": requiere_direccion,
        "tipo_autorizacion": (
            VacanteRRHH.AUTORIZACION_DIRECCION
            if requiere_direccion
            else VacanteRRHH.AUTORIZACION_JEFE_DIRECTO
        ),
        "autorizador_asignado": autorizador,
    }
    if autorizador and autorizador.id == vacante.solicitado_por_id:
        extra_updates.update(
            {
                "autorizado_por": autorizador,
                "fecha_autorizacion": timezone.now(),
            }
        )
        return _transition(
            vacante,
            VacanteRRHH.ESTADO_AUTORIZADA,
            user,
            comentario or "Validada por Capital Humano; solicitud autorizada por la jefatura solicitante.",
            extra_updates=extra_updates,
            notify=lambda updated: notificar_vacante_aprobada(updated, actor=user),
        )
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_PENDIENTE_DIRECCION,
        user,
        comentario or "Validada por Capital Humano y enviada a autorización.",
        extra_updates=extra_updates,
        notify=lambda updated: notificar_vacante_para_autorizacion(updated, actor=user),
    )


def aprobar_vacante_autorizacion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    if not can_autorizar_vacante(user, vacante):
        raise PermissionDenied("Solo el autorizador asignado puede aprobar esta vacante.")
    if vacante.estado != VacanteRRHH.ESTADO_PENDIENTE_DIRECCION:
        raise ValidationError("La vacante debe estar pendiente de autorización para aprobarse.")
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_AUTORIZADA,
        user,
        comentario or "Vacante aprobada.",
        extra_updates={
            "autorizado_por": user,
            "fecha_autorizacion": timezone.now(),
        },
        notify=lambda updated: notificar_vacante_aprobada(updated, actor=user),
    )


def rechazar_vacante_autorizacion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    if not can_autorizar_vacante(user, vacante):
        raise PermissionDenied("Solo el autorizador asignado puede rechazar esta vacante.")
    if vacante.estado != VacanteRRHH.ESTADO_PENDIENTE_DIRECCION:
        raise ValidationError("La vacante debe estar pendiente de autorización para rechazarse.")
    now = timezone.now()
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_RECHAZADA,
        user,
        comentario or "Vacante rechazada.",
        extra_updates={
            "rechazado_por": user,
            "fecha_rechazo": now,
            "motivo_rechazo": (comentario or "").strip(),
        },
        notify=lambda updated: notificar_vacante_rechazada(updated, actor=user),
    )


def enviar_vacante_direccion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    return enviar_vacante_autorizacion(vacante, user, comentario)


def aprobar_vacante_direccion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    return aprobar_vacante_autorizacion(vacante, user, comentario)


def rechazar_vacante_direccion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    return rechazar_vacante_autorizacion(vacante, user, comentario)


def iniciar_reclutamiento_vacante(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    _require_rrhh(user)
    if vacante.estado not in {
        VacanteRRHH.ESTADO_AUTORIZADA,
        VacanteRRHH.ESTADO_PAUSADA,
    }:
        raise ValidationError("La vacante debe estar autorizada antes de iniciar reclutamiento.")
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
    }:
        raise ValidationError("La vacante debe estar autorizada antes de registrar cobertura.")
    with transaction.atomic():
        vacante = VacanteRRHH.objects.select_for_update().get(pk=vacante.pk)
        if vacante.estado not in {
            VacanteRRHH.ESTADO_RECLUTAMIENTO,
            VacanteRRHH.ESTADO_AUTORIZADA,
        }:
            raise ValidationError("La vacante debe estar autorizada antes de registrar cobertura.")
        if vacante.coberturas.count() >= vacante.cantidad_solicitada:
            raise ValidationError("La vacante ya tiene cubierta la cantidad solicitada.")
        if VacanteCobertura.objects.filter(vacante=vacante, empleado=empleado).exists():
            raise ValidationError("Este empleado ya cubre esta vacante.")
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


def crear_alta_pendiente_desde_candidato(
    candidato: CandidatoVacante,
    user,
    comentario: str = "",
) -> AltaPendienteEmpleado:
    _require_rrhh(user)
    if candidato.empleado_id:
        raise ValidationError("Este candidato ya está ligado a un empleado.")
    if candidato.etapa_actual not in {
        CandidatoVacante.ETAPA_SELECCIONADO,
        CandidatoVacante.ETAPA_DOCUMENTOS,
        CandidatoVacante.ETAPA_PRUEBA,
    }:
        raise ValidationError("Solo candidatos seleccionados, en documentos o prueba pueden enviarse a alta.")

    with transaction.atomic():
        candidato = CandidatoVacante.objects.select_for_update().get(pk=candidato.pk)
        if candidato.empleado_id:
            raise ValidationError("Este candidato ya está ligado a un empleado.")
        if candidato.etapa_actual not in {
            CandidatoVacante.ETAPA_SELECCIONADO,
            CandidatoVacante.ETAPA_DOCUMENTOS,
            CandidatoVacante.ETAPA_PRUEBA,
        }:
            raise ValidationError("Solo candidatos seleccionados, en documentos o prueba pueden enviarse a alta.")
        vacante = VacanteRRHH.objects.select_for_update().get(pk=candidato.vacante_id)
        if vacante.estado != VacanteRRHH.ESTADO_RECLUTAMIENTO:
            raise ValidationError("Solo se pueden preparar altas pendientes de vacantes en reclutamiento.")
        pendiente = AltaPendienteEmpleado.objects.filter(
            candidato=candidato,
            estado=AltaPendienteEmpleado.ESTADO_PENDIENTE,
        ).first()
        if pendiente:
            return pendiente

        pendiente = AltaPendienteEmpleado.objects.create(
            vacante=vacante,
            candidato=candidato,
            nombre=candidato.nombre,
            telefono=candidato.telefono,
            email=candidato.email,
            sucursal=vacante.sucursal.nombre if vacante.sucursal_id else "",
            departamento=vacante.departamento,
            area=vacante.area,
            puesto=vacante.puesto,
            fecha_ingreso_sugerida=vacante.fecha_necesaria,
            nota=(comentario or "").strip(),
            creado_por=user,
        )
        VacanteSeguimiento.objects.create(
            vacante=vacante,
            candidato_obj=candidato,
            etapa=VacanteSeguimiento.ETAPA_DOCUMENTOS,
            candidato=candidato.nombre,
            comentario=(comentario or "Candidato enviado a alta pendiente de empleado.").strip(),
            creado_por=user,
        )
    return pendiente


def consumir_alta_pendiente(
    alta: AltaPendienteEmpleado,
    empleado: Empleado,
    user,
    *,
    fecha_cobertura: date | None = None,
    nota: str = "",
) -> VacanteCobertura:
    _require_rrhh(user)
    with transaction.atomic():
        alta = (
            AltaPendienteEmpleado.objects.select_for_update()
            .select_related("candidato", "vacante")
            .get(pk=alta.pk)
        )
        if alta.estado != AltaPendienteEmpleado.ESTADO_PENDIENTE:
            raise ValidationError("Esta alta pendiente ya fue procesada.")
        candidato = CandidatoVacante.objects.select_for_update().get(pk=alta.candidato_id)
        if candidato.empleado_id and candidato.empleado_id != empleado.id:
            raise ValidationError("Este candidato ya está ligado a otro empleado.")
        if candidato.etapa_actual not in {
            CandidatoVacante.ETAPA_SELECCIONADO,
            CandidatoVacante.ETAPA_DOCUMENTOS,
            CandidatoVacante.ETAPA_PRUEBA,
        }:
            raise ValidationError("El candidato ya no está en una etapa válida para alta.")

        candidato.empleado = empleado
        candidato.etapa_actual = CandidatoVacante.ETAPA_CONTRATADO
        candidato.save(update_fields=["empleado", "etapa_actual", "actualizado_en"])

        cobertura = cubrir_vacante(
            alta.vacante,
            empleado,
            user,
            fecha_cobertura=fecha_cobertura or alta.fecha_ingreso_sugerida,
            nota=nota or f"Alta creada desde candidato {candidato.nombre}.",
        )
        alta.empleado = empleado
        alta.estado = AltaPendienteEmpleado.ESTADO_CONVERTIDA
        alta.convertida_en = timezone.now()
        alta.save(update_fields=["empleado", "estado", "convertida_en", "actualizado_en"])
    return cobertura


def devolver_vacante_correccion(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    _require_rrhh(user)
    if vacante.estado in {VacanteRRHH.ESTADO_CUBIERTA, VacanteRRHH.ESTADO_CANCELADA}:
        raise ValidationError("Una vacante cerrada no puede devolverse a corrección.")
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_DEVUELTA_CORRECCION,
        user,
        comentario or "Solicitud devuelta a corrección.",
    )


def reenviar_vacante_revision(vacante: VacanteRRHH, user, comentario: str = "") -> VacanteRRHH:
    if vacante.estado != VacanteRRHH.ESTADO_DEVUELTA_CORRECCION:
        raise ValidationError("La vacante no está devuelta a corrección.")
    if not (can_gestionar_vacantes(user) or vacante.solicitado_por_id == user.id or vacante.creado_por_id == user.id):
        raise PermissionDenied("Solo el solicitante, capturista o Capital Humano puede reenviar esta vacante.")
    return _transition(
        vacante,
        VacanteRRHH.ESTADO_REVISION_RRHH,
        user,
        comentario or "Solicitud corregida y reenviada a revisión de Capital Humano.",
    )


def agregar_seguimiento_vacante(
    vacante: VacanteRRHH,
    user,
    *,
    etapa: str = VacanteSeguimiento.ETAPA_COMENTARIO,
    candidato: str = "",
    comentario: str = "",
    fecha: date | None = None,
) -> VacanteSeguimiento:
    _require_rrhh(user)
    if vacante.estado in {VacanteRRHH.ESTADO_CANCELADA, VacanteRRHH.ESTADO_RECHAZADA, VacanteRRHH.ESTADO_CUBIERTA}:
        raise ValidationError("No se puede agregar seguimiento a una vacante cerrada.")
    if not (comentario or "").strip():
        raise ValidationError("El seguimiento requiere comentario.")
    return VacanteSeguimiento.objects.create(
        vacante=vacante,
        etapa=etapa or VacanteSeguimiento.ETAPA_COMENTARIO,
        candidato=(candidato or "").strip(),
        comentario=(comentario or "").strip(),
        fecha=fecha or timezone.localdate(),
        creado_por=user,
    )


def vacante_requiere_autorizacion_direccion(vacante: VacanteRRHH) -> bool:
    """
    Requiere DG solo si el puesto solicitado es de primer nivel en el organigrama
    (jefatura sin jefe_directo asignado = reporta directo a Dirección General).
    Cualquier puesto operativo o jefatura intermedia NO requiere DG.
    """
    departamento = _inferir_departamento_vacante(vacante)
    if not departamento:
        return False
    # Buscar jefatura del departamento que no tenga jefe_directo (primer nivel)
    jefatura_primer_nivel = (
        Empleado.objects.filter(
            activo=True,
            departamento=departamento,
            jefe_directo__isnull=True,
        )
        .filter(jefatura_q())
        .exists()
    )
    if not jefatura_primer_nivel:
        return False
    # Solo aplica si el puesto solicitado ES esa jefatura de primer nivel
    texto_puesto = _normalizar_texto(vacante.puesto)
    return any(keyword in texto_puesto for keyword in DIRECCION_PUESTO_KEYWORDS)


def resolver_autorizador_vacante(vacante: VacanteRRHH, *, exclude_user=None):
    if vacante_requiere_autorizacion_direccion(vacante):
        return None

    exclude_ids = {getattr(exclude_user, "id", None)}
    exclude_ids = {user_id for user_id in exclude_ids if user_id}
    departamento = _inferir_departamento_vacante(vacante)

    # Si el solicitante ya es jefatura de ese departamento, se auto-autoriza
    if _usuario_es_jefatura_departamento(vacante.solicitado_por, departamento):
        return vacante.solicitado_por

    # Buscar jefatura del departamento con usuario ERP activo
    jefe = (
        Empleado.objects.select_related("usuario_erp")
        .filter(activo=True, usuario_erp__is_active=True, departamento=departamento)
        .filter(liderazgo_q())
        .exclude(usuario_erp_id__in=exclude_ids)
        .order_by("id")
        .first()
    )
    if jefe and jefe.usuario_erp_id:
        return jefe.usuario_erp

    # Sin jefatura configurada no se debe autoautorizar: Capital Humano debe
    # corregir Organización antes de enviar la vacante.
    return None


def _usuario_es_jefatura_departamento(user, departamento: str) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    departamento = _normalizar_texto(departamento)
    empleado = getattr(user, "empleado_rrhh", None)
    return empleado_es_liderazgo(empleado, departamento=departamento)


def _usuario_tiene_jefatura(user) -> bool:
    empleado = getattr(user, "empleado_rrhh", None)
    return empleado_es_liderazgo(empleado) or bool(empleado and empleado.colaboradores_directos.exists())


def usuarios_autorizadores_vacante(vacante: VacanteRRHH) -> list:
    if vacante.requiere_direccion:
        return usuarios_direccion_general()
    if vacante.autorizador_asignado_id:
        return [vacante.autorizador_asignado]
    autorizador = resolver_autorizador_vacante(vacante)
    return [autorizador] if autorizador else []


def _inferir_departamento_vacante(vacante: VacanteRRHH) -> str:
    departamento = _normalizar_texto(vacante.departamento)
    if departamento:
        return departamento
    texto = f"{vacante.area or ''} {vacante.puesto or ''}".upper()
    if "VENTA" in texto or "CAJ" in texto or "CALL" in texto or "REPART" in texto:
        return Empleado.DEP_VENTAS
    if "PRODU" in texto or "HORNO" in texto or "ARMADO" in texto or "EMBET" in texto:
        return Empleado.DEP_PRODUCCION
    if "COMPR" in texto:
        return Empleado.DEP_COMPRAS
    if "LOGIST" in texto or "LOGÍST" in texto or "ENVIO" in texto or "ENVÍO" in texto:
        return Empleado.DEP_LOGISTICA
    if "RRHH" in texto or "RECURSOS HUMANOS" in texto or "CAPITAL HUMANO" in texto:
        return Empleado.DEP_RRHH
    if "ADMIN" in texto or "ALMAC" in texto or "AFAN" in texto or "LIMPIEZA" in texto:
        return Empleado.DEP_ADMINISTRACION
    return ""


def agregar_candidato(
    vacante: VacanteRRHH,
    user,
    *,
    nombre: str,
    telefono: str = "",
    email: str = "",
    fuente: str = CandidatoVacante.FUENTE_REFERENCIA,
    notas: str = "",
) -> CandidatoVacante:
    _require_rrhh(user)
    if vacante.estado not in {VacanteRRHH.ESTADO_RECLUTAMIENTO, VacanteRRHH.ESTADO_AUTORIZADA}:
        raise ValidationError("Solo se pueden agregar candidatos cuando la vacante está en reclutamiento.")
    nombre = (nombre or "").strip()
    if not nombre:
        raise ValidationError("El nombre del candidato es obligatorio.")
    return CandidatoVacante.objects.create(
        vacante=vacante,
        nombre=nombre,
        telefono=(telefono or "").strip(),
        email=(email or "").strip(),
        fuente=fuente or CandidatoVacante.FUENTE_REFERENCIA,
        notas=(notas or "").strip(),
        etapa_actual=CandidatoVacante.ETAPA_ENTREVISTA,
        creado_por=user,
    )


def avanzar_etapa_candidato(
    candidato: CandidatoVacante,
    user,
    nueva_etapa: str,
    comentario: str = "",
) -> CandidatoVacante:
    _require_rrhh(user)
    etapas_validas = [e for e, _ in CandidatoVacante.ETAPA_CHOICES]
    if nueva_etapa not in etapas_validas:
        raise ValidationError("Etapa inválida.")
    candidato.etapa_actual = nueva_etapa
    candidato.save(update_fields=["etapa_actual", "actualizado_en"])
    if comentario.strip():
        VacanteSeguimiento.objects.create(
            vacante=candidato.vacante,
            candidato_obj=candidato,
            etapa=nueva_etapa,
            candidato=candidato.nombre,
            comentario=comentario.strip(),
            creado_por=user,
        )
    return candidato


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


def notificar_vacante_para_autorizacion(vacante: VacanteRRHH, *, actor=None) -> int:
    usuarios = usuarios_autorizadores_vacante(vacante)
    if not usuarios:
        return 0
    responsable = "Dirección General" if vacante.requiere_direccion else "jefe directo"
    return crear_notificaciones(
        usuarios,
        titulo=f"Vacante para aprobar: {vacante.area} / {vacante.puesto}",
        mensaje=f"{vacante.folio} validada por Capital Humano. Falta autorización de {responsable}.",
        url=f"/rrhh/vacantes/{vacante.id}/",
        tipo=Notificacion.TIPO_SISTEMA,
        prioridad=Notificacion.PRIORIDAD_ALTA,
        actor=actor,
        objeto_tipo="rrhh.VacanteRRHH",
        objeto_id=vacante.id,
        excluir=actor,
    )


def notificar_vacante_para_direccion(vacante: VacanteRRHH, *, actor=None) -> int:
    return notificar_vacante_para_autorizacion(vacante, actor=actor)


def notificar_vacante_aprobada(vacante: VacanteRRHH, *, actor=None) -> int:
    return crear_notificaciones(
        _usuarios_interesados(vacante),
        titulo=f"Vacante aprobada: {vacante.area} / {vacante.puesto}",
        mensaje=f"{vacante.folio} autorizada para reclutamiento.",
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
        mensaje=f"{vacante.folio} rechazada por el autorizador.",
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
    ids = {vacante.solicitado_por_id, vacante.creado_por_id, vacante.autorizador_asignado_id, vacante.autorizado_por_id}
    users = list(User.objects.filter(id__in=[user_id for user_id in ids if user_id]))
    users.extend(usuarios_por_grupo("RRHH"))
    return users
