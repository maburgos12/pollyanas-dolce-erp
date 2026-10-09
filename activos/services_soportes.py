"""Mutaciones documentales atómicas; storage no participa del rollback SQL."""
import logging
from pathlib import PurePosixPath
from uuid import uuid4

from django.db import transaction

from core.audit import log_event
from .models import BitacoraMantenimiento, EvidenciaOrden, OrdenMantenimiento

logger = logging.getLogger(__name__)


def _referenciado(nombre):
    return (OrdenMantenimiento.objects.filter(factura_archivo=nombre).exists()
            or EvidenciaOrden.objects.filter(archivo=nombre).exists())


def _retirar(storage, nombre, orden_id, usuario, avisos):
    if not nombre:
        return
    try:
        # Coordina con todas las mutaciones de soportes de esta orden.
        with transaction.atomic():
            OrdenMantenimiento.objects.select_for_update().get(pk=orden_id)
            if not _referenciado(nombre):
                storage.delete(nombre)
    except Exception:
        logger.exception("Soporte pendiente de limpieza: orden=%s archivo=%s", orden_id, nombre)
        avisos.append("El cambio quedó guardado; un archivo pendiente de limpieza requiere revisión.")
        try:
            log_event(usuario, "UPDATE", "activos.OrdenMantenimiento", orden_id,
                      {"soporte_limpieza_pendiente": nombre})
        except Exception:
            logger.exception("No se pudo registrar la limpieza pendiente en auditoría")


def mutar_soporte(*, orden_id, usuario, accion, datos, archivo=None, evidencia_id=None):
    """Retorna advertencias sólo después de confirmar el commit durable."""
    avisos = []
    nuevo = None
    propio = False
    storage = None
    candidato = None
    guardado = None
    try:
        with transaction.atomic(durable=True):
            orden = OrdenMantenimiento.objects.select_for_update().get(pk=orden_id)
            anterior = None
            if accion == "eliminar":
                evidencia = EvidenciaOrden.objects.get(pk=evidencia_id, orden=orden)
                storage = evidencia.archivo.storage
                anterior = evidencia.archivo.name
                objeto_id = evidencia.pk
                evidencia.delete()
                comentario = "Evidencia eliminada: " + anterior
                modelo, evento = "activos.EvidenciaOrden", "DELETE"
            else:
                objeto = orden if accion == "update_factura" else EvidenciaOrden(orden=orden, subido_por=usuario)
                campo = objeto._meta.get_field("factura_archivo" if accion == "update_factura" else "archivo")
                storage = campo.storage
                if archivo:
                    # No se reutilizan nombres aportados por clientes ni rutas históricas.
                    extension = PurePosixPath(archivo.name).suffix[:16]
                    candidato = campo.generate_filename(objeto, uuid4().hex + extension)
                    if storage.exists(candidato):
                        raise OSError("El nombre reservado para el soporte ya existe")
                    guardado = storage.save(candidato, archivo, max_length=campo.max_length)
                    nuevo = guardado
                    # Un backend que devuelve otra ruta no acredita propiedad de ésta.
                    propio = guardado == candidato
                    if not propio:
                        raise OSError("Storage devolvió una ruta cuya propiedad no pudo verificarse")
                if accion == "update_factura":
                    anterior = orden.factura_archivo.name
                    orden.numero_factura = datos.get("numero_factura", "").strip()
                    orden.nota_trabajo = datos.get("nota_trabajo", "").strip()
                    campos = ["numero_factura", "nota_trabajo", "actualizado_en"]
                    if nuevo:
                        orden.factura_archivo = nuevo
                        campos.append("factura_archivo")
                    else:
                        anterior = None
                    orden.save(update_fields=campos)
                    comentario = f"Factura/nota actualizada: {orden.numero_factura or '—'}"
                    objeto_id, modelo, evento = orden.pk, "activos.OrdenMantenimiento", "UPDATE"
                else:
                    objeto.archivo = nuevo
                    tipo = datos.get("tipo", "FOTO").strip().upper()
                    objeto.tipo = tipo if tipo in dict(EvidenciaOrden.TIPO_CHOICES) else EvidenciaOrden.TIPO_FOTO
                    objeto.descripcion = datos.get("descripcion", "").strip()
                    objeto.save()
                    comentario = f"Evidencia subida: {nuevo} ({objeto.tipo})"
                    objeto_id, modelo, evento = objeto.pk, "activos.EvidenciaOrden", "CREATE"
            BitacoraMantenimiento.objects.create(orden=orden, accion="FACTURA" if accion == "update_factura" else "EVIDENCIA",
                                                comentario=comentario, usuario=usuario)
            payload = {"orden": orden.folio, "folio": orden.folio, "archivo": nuevo or anterior}
            if accion == "update_factura":
                payload["numero_factura"] = orden.numero_factura
            log_event(usuario, evento, modelo, objeto_id, payload)
            if anterior:
                transaction.on_commit(lambda: _retirar(storage, anterior, orden_id, usuario, avisos))
    except Exception:
        # Sólo la ruta acreditada de este intento; nunca el soporte anterior.
        if nuevo and propio:
            _retirar(storage, nuevo, orden_id, usuario, avisos)
        elif candidato:
            # Sin retorno exacto no hay prueba de qué ruta creó el backend.
            # Conservar y registrar; no convertir el error original en un borrado.
            logger.exception("Storage sin propiedad acreditada: orden=%s candidato=%s retorno=%s",
                             orden_id, candidato, guardado)
            try:
                log_event(usuario, "UPDATE", "activos.OrdenMantenimiento", orden_id,
                          {"soporte_storage_revision": {"candidato": candidato, "retorno": guardado}})
            except Exception:
                logger.exception("No se pudo auditar el soporte de propiedad incierta")
        raise
    return avisos
