"""Creación atómica y reintentos de las tres capturas directas de equipos."""
import hashlib
import json
import logging
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Model
from rest_framework.exceptions import APIException

from core.audit import log_event
from mantenimiento.models import ComprobanteCapturaEquipo
from mantenimiento.services_access import authorized_branch_ids, authorized_orders, can_write_mantenimiento


logger = logging.getLogger(__name__)


class CapturaEquipoError(APIException):
    status_code = 409

    def __init__(self, mensaje, status_code=409):
        self.status_code = status_code
        super().__init__(mensaje)


def validar_equipo(usuario, activo):
    if not usuario.is_active or not can_write_mantenimiento(usuario):
        raise PermissionDenied('No tienes permisos para registrar mantenimiento.')
    branches = authorized_branch_ids(usuario)
    if not activo.activo or (branches is not None and activo.sucursal_id not in branches):
        raise PermissionDenied('El equipo no está disponible en tu ámbito autorizado.')


def _normalizar(value):
    if isinstance(value, Model):
        return value.pk
    if isinstance(value, Decimal):
        return format(value.normalize(), 'f')
    if isinstance(value, (date, datetime, UUID)):
        return str(value)
    if hasattr(value, 'chunks'):
        position = value.tell()
        try:
            value.seek(0)
            digest = hashlib.sha256()
            for chunk in value.chunks():
                digest.update(chunk)
            return {'nombre': value.name, 'sha256': digest.hexdigest()}
        finally:
            value.seek(position)
    if isinstance(value, dict):
        return {str(k): _normalizar(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalizar(v) for v in value]
    return value


def huella_captura(contenido):
    return hashlib.sha256(json.dumps(_normalizar(contenido), sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def guardar_factura(orden, archivo, archivos_nuevos):
    """Registrar el nombre antes del INSERT/UPDATE para limpiar incluso si falla SQL."""
    field = orden.factura_archivo
    field.save(archivo.name, archivo, save=False)
    archivos_nuevos.append((field.storage, field.name))
    orden.save(update_fields=['factura_archivo'])


def capturar_equipo(*, usuario, activo, operacion, clave, contenido, crear):
    return capturar_equipo_autorizado(usuario=usuario, activo=activo, operacion=operacion,
        clave=clave, contenido=contenido, crear=crear, validar=validar_equipo,
        ordenes=authorized_orders)


def capturar_equipo_autorizado(*, usuario, activo, operacion, clave, contenido, crear, validar, ordenes, audit_metadata=None):
    """Motor común; cada consumidor mantiene explícitamente su permiso y ámbito."""
    validar(usuario, activo)
    if clave is not None:
        try:
            clave = UUID(str(clave))
        except (ValueError, TypeError, AttributeError):
            raise CapturaEquipoError('La clave de captura debe ser un UUID válido.', 400)
    huella = huella_captura(contenido)
    archivos_nuevos = []
    try:
        with transaction.atomic():
            recibo = None
            if clave is not None:
                # UNIQUE espera al otro intento concurrente; get_or_create usa savepoint.
                recibo, nuevo = ComprobanteCapturaEquipo.objects.get_or_create(
                    usuario=usuario, operacion=operacion, clave=clave, defaults={'huella': huella},
                )
                if not nuevo:
                    if recibo.huella != huella:
                        raise CapturaEquipoError('Esta captura ya se envió con otros datos. Conserva el intento original y revisa el conflicto.')
                    if recibo.orden_id is None:
                        raise CapturaEquipoError('La orden de esta captura fue eliminada; este intento no puede recrearla.', 410)
                    orden = ordenes(usuario).filter(pk=recibo.orden_id, activo_ref=activo).first()
                    if orden is None:
                        raise PermissionDenied('La orden ya no está en tu ámbito autorizado.')
                    return orden, True
            orden = crear(archivos_nuevos)
            log_event(usuario, 'CREATE', 'activos.OrdenMantenimiento', str(orden.pk), audit_metadata(orden) if audit_metadata else {'origen': operacion})
            if recibo:
                recibo.orden = orden
                recibo.save(update_fields=['orden'])
            return orden, False
    except Exception:
        for storage, name in archivos_nuevos:
            try:
                storage.delete(name)
            except Exception:
                # Intentar todos los archivos sin ocultar la causa del rollback.
                logger.exception('No se pudo limpiar archivo de captura revertida: %s', name)
        raise
