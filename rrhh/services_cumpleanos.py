"""Calendario y despacho de cumpleaños, usando exclusivamente personas RRHH."""
from __future__ import annotations

import hashlib
import logging
from datetime import date, timedelta

from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone

from core.access import ROLE_DG, ROLE_RRHH, can_manage_submodule, has_any_role
from core.contactos import resolver_correo
from core.notificaciones import PUBLIC_BASE_URL, crear_notificacion
from .models import AvisoCumpleanos, Empleado

logger = logging.getLogger(__name__)
NIVELES_JEFATURA = (Empleado.NIVEL_JEFATURA, Empleado.NIVEL_DIRECCION)


def _activo(user):
    if not user or not getattr(user, 'is_authenticated', False) or not getattr(user, 'is_active', False):
        return False
    empleado = getattr(user, 'empleado_rrhh', None)
    if empleado and not empleado.activo:
        return False
    perfil = getattr(user, 'userprofile', None)
    return user.is_superuser or not (perfil and perfil.lock_rrhh)


def vista_global_cumpleanos(user):
    if not _activo(user):
        return False
    if user.is_superuser:
        return True
    if has_any_role(user, ROLE_RRHH, ROLE_DG):
        return True
    empleado = getattr(user, 'empleado_rrhh', None)
    return bool(empleado and (empleado.nivel_organizacional == Empleado.NIVEL_DIRECCION
                or (empleado.departamento == Empleado.DEP_RRHH
                    and empleado.nivel_organizacional in NIVELES_JEFATURA)))


def puede_ver_cumpleanos(user):
    if not _activo(user):
        return False
    if vista_global_cumpleanos(user):
        return True
    empleado = getattr(user, 'empleado_rrhh', None)
    return bool(empleado and empleado.nivel_organizacional in NIVELES_JEFATURA
                and empleado.departamento)


def puede_gestionar_cumpleanos(user):
    if not _activo(user):
        return False
    if user.is_superuser:
        return True
    empleado = getattr(user, 'empleado_rrhh', None)
    es_ch = has_any_role(user, ROLE_RRHH) or bool(
        empleado and empleado.departamento == Empleado.DEP_RRHH
        and empleado.nivel_organizacional in NIVELES_JEFATURA)
    return es_ch and can_manage_submodule(user, 'rrhh', 'empleados')


def empleados_visibles(user):
    qs = Empleado.objects.filter(activo=True).select_related('sucursal_ref')
    if vista_global_cumpleanos(user):
        return qs
    if not puede_ver_cumpleanos(user):
        return qs.none()
    jefe = user.empleado_rrhh
    return qs.filter(Q(departamento=jefe.departamento) | Q(jefe_directo=jefe))


def fecha_cumpleanos(nacimiento: date, anio: int) -> date:
    try:
        return nacimiento.replace(year=anio)
    except ValueError:
        # Único caso de una fecha válida que no existe en otro año: 29/02.
        return date(anio, 2, 28)


def eventos_entre(queryset, desde: date, hasta: date) -> list[dict]:
    eventos = []
    for empleado in queryset.filter(fecha_nacimiento__isnull=False):
        for anio in range(desde.year, hasta.year + 1):
            fecha = fecha_cumpleanos(empleado.fecha_nacimiento, anio)
            if desde <= fecha <= hasta:
                eventos.append({'empleado': empleado, 'fecha': fecha})
    return sorted(eventos, key=lambda e: (e['fecha'], e['empleado'].nombre, e['empleado'].pk))


def _eventos_aviso(aviso, hoy):
    # Los reintentos no recuperan cumpleaños de ayer ni resúmenes de otra semana.
    if not puede_ver_cumpleanos(aviso.usuario):
        return []
    if aviso.tipo == 'hoy':
        desde = hasta = aviso.fecha_referencia
        envio = desde
    elif aviso.tipo == 'anticipado':
        desde = hasta = aviso.fecha_referencia
        envio = desde - timedelta(days=3)
    else:
        desde = aviso.fecha_referencia
        hasta = desde + timedelta(days=6)
        envio = desde
    if envio != hoy:
        return []
    return eventos_entre(empleados_visibles(aviso.usuario), desde, hasta)


def _mensaje(aviso, eventos):
    titulo = {
        'hoy': 'Cumpleaños de hoy', 'anticipado': 'Cumpleaños dentro de tres días',
        'semanal': 'Cumpleaños de esta semana',
    }[aviso.tipo]
    lineas = []
    for evento in eventos:
        empleado = evento['empleado']
        linea = f"{evento['fecha']:%d/%m} · {empleado.nombre}"
        if empleado.departamento:
            linea += f' · {empleado.get_departamento_display()}'
        if empleado.sucursal_display:
            linea += f' · {empleado.sucursal_display}'
        lineas.append(linea)
    return titulo, '\n'.join(lineas)


def _usuario_fresco(user_id):
    return get_user_model().objects.select_related('empleado_rrhh', 'userprofile').get(pk=user_id)


def _enviar_aviso(aviso_id, hoy):
    with transaction.atomic():
        aviso = AvisoCumpleanos.objects.select_for_update().get(pk=aviso_id)
        aviso.usuario = _usuario_fresco(aviso.usuario_id)
        eventos = _eventos_aviso(aviso, hoy)
        if not eventos:
            if aviso.estado_correo in {'pendiente', 'fallido', 'sin_contacto'}:
                aviso.estado_correo = 'omitido'
                aviso.error_correo = 'Sin cumpleaños activos dentro del alcance actual.'
                aviso.save(update_fields=['estado_correo', 'error_correo', 'actualizado_en'])
            return
        if not aviso.notificacion_id:
            titulo, mensaje = _mensaje(aviso, eventos)
            aviso.notificacion = crear_notificacion(
                usuario=aviso.usuario, titulo=titulo, mensaje=mensaje,
                url=reverse('rrhh:rrhh_cumpleanos'),
                objeto_tipo='rrhh.AvisoCumpleanos', objeto_id=aviso.pk,
            )
            aviso.save(update_fields=['notificacion', 'actualizado_en'])
        if aviso.estado_correo in {'enviado', 'en_proceso', 'incierto', 'omitido'}:
            return
        correo, motivo = resolver_correo(aviso.usuario)
        if not correo:
            aviso.estado_correo = 'sin_contacto'
            aviso.error_correo = motivo
            aviso.save(update_fields=['estado_correo', 'error_correo', 'actualizado_en'])
            return
        # Volver a consultar activos y alcance antes de reservar el segundo canal.
        aviso.usuario = _usuario_fresco(aviso.usuario_id)
        eventos = _eventos_aviso(aviso, hoy)
        if not eventos:
            aviso.estado_correo = 'omitido'
            aviso.save(update_fields=['estado_correo', 'actualizado_en'])
            return
        titulo, texto = _mensaje(aviso, eventos)
        cuerpo = texto + '\n\nAbrir calendario: ' + PUBLIC_BASE_URL + reverse('rrhh:rrhh_cumpleanos')
        clave = hashlib.sha256(
            f'{aviso.tipo}|{aviso.fecha_referencia}|{correo.lower()}|{cuerpo}'.encode()
        ).hexdigest()
        aviso.correo_destino = correo
        aviso.estado_correo = 'en_proceso'
        aviso.clave_correo = clave
        aviso.intentos += 1
        aviso.error_correo = ''
        try:
            with transaction.atomic():
                aviso.save(update_fields=['correo_destino', 'estado_correo', 'clave_correo',
                                          'intentos', 'error_correo', 'actualizado_en'])
        except IntegrityError:
            if not AvisoCumpleanos.objects.exclude(pk=aviso.pk).filter(clave_correo=clave).exists():
                raise
            aviso.clave_correo = None
            aviso.estado_correo = 'omitido'
            aviso.error_correo = 'Mensaje idéntico ya reservado para una cuenta con el mismo correo.'
            aviso.save(update_fields=['estado_correo', 'error_correo', 'correo_destino', 'actualizado_en'])
            return
    # Reserva confirmada ANTES de cualquier efecto externo. Si el proceso muere
    # después de la aceptación, en_proceso conserva la incertidumbre sin duplicar.
    mensaje = EmailMultiAlternatives(subject=f'[ERP] {titulo}', body=cuerpo, to=[correo])
    try:
        enviados = mensaje.send(fail_silently=False)
    except Exception as exc:
        detalle = str(exc)[:400]
        # El backend existente distingue rechazo HTTP y transporte incierto.
        estado = 'fallido' if ('Resend API error' in detalle or detalle == 'RESEND_API_KEY no configurado.') else 'incierto'
        logger.warning('Cumpleaños: correo de aviso %s con estado %s', aviso_id, estado)
        referencia = ''
    else:
        estado = 'enviado' if enviados else 'fallido'
        detalle = '' if enviados else 'El backend de correo no aceptó el mensaje.'
        referencia = getattr(mensaje, 'resend_email_id', '') or ''
    with transaction.atomic():
        aviso = AvisoCumpleanos.objects.select_for_update().get(pk=aviso_id)
        aviso.estado_correo = estado
        aviso.error_correo = detalle
        aviso.referencia_correo = referencia
        if estado == 'fallido':
            aviso.clave_correo = None
        aviso.save(update_fields=['estado_correo', 'error_correo', 'referencia_correo',
                                  'clave_correo', 'actualizado_en'])


def generar_avisos_cumpleanos(hoy=None):
    hoy = hoy or timezone.localdate()
    tipos = [('hoy', hoy), ('anticipado', hoy + timedelta(days=3))]
    if hoy.weekday() == 0:
        tipos.append(('semanal', hoy))
    usuarios = list(get_user_model().objects.filter(is_active=True).select_related('empleado_rrhh', 'userprofile'))
    usuarios = [u for u in usuarios if puede_ver_cumpleanos(u)]
    # Cuando Dirección tiene dos cuentas con el mismo buzón, el mensaje global
    # se reserva primero y se deduplica por correo + contenido, no por nombre.
    usuarios.sort(key=lambda u: (not vista_global_cumpleanos(u), u.pk))
    ids = set()
    for usuario in usuarios:
        for tipo, referencia in tipos:
            candidato = AvisoCumpleanos(usuario=usuario, tipo=tipo, fecha_referencia=referencia)
            if not _eventos_aviso(candidato, hoy):
                continue
            aviso, _ = AvisoCumpleanos.objects.get_or_create(
                usuario=usuario, tipo=tipo, fecha_referencia=referencia,
            )
            ids.add(aviso.pk)
    # Revalidar también avisos que quedaron pendientes antes de una baja/cambio.
    pendientes = AvisoCumpleanos.objects.filter(
        Q(tipo='hoy', fecha_referencia=hoy)
        | Q(tipo='anticipado', fecha_referencia=hoy + timedelta(days=3))
        | Q(tipo='semanal', fecha_referencia=hoy),
    ).filter(estado_correo__in=['pendiente', 'fallido', 'sin_contacto'])
    ids.update(pendientes.values_list('pk', flat=True))
    for aviso_id in sorted(ids):
        _enviar_aviso(aviso_id, hoy)
    avisos = AvisoCumpleanos.objects.filter(pk__in=ids)
    return {
        'ok': not avisos.filter(estado_correo__in=['fallido', 'incierto', 'en_proceso']).exists(),
        'avisos': len(ids), 'fallidos': avisos.filter(estado_correo='fallido').count(),
        'aceptados_correo': avisos.filter(estado_correo='enviado').count(),
        'inciertos': avisos.filter(estado_correo__in=['incierto', 'en_proceso']).count(),
    }
