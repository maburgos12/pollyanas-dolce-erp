"""Revisión de lectura y aplicación global de decisiones explícitas de bitácora."""
from decimal import Decimal

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction

from core.access import can_manage_inventario
from core.audit import log_event
from mantenimiento.services_access import authorized_orders
from mantenimiento.services_capturas_equipos import (
    CapturaEquipoError, capturar_equipo_autorizado, huella_captura, validar_equipo,
)
from mantenimiento.services_vinculos_proveedores import actor_actual, _limite_explicito_mantenimiento
from .models import Activo, BitacoraMantenimiento, OrdenMantenimiento, OrigenImportacionBitacora
from .utils.bitacora_import import MAX_SOURCE_ROWS, _as_date, preview_bitacora


def actor_importacion(usuario):
    actor = actor_actual(usuario)
    if not can_manage_inventario(actor):
        raise PermissionDenied('Necesitas gestionar Activos para revisar una importación.')
    return actor


def _id(value, nombre):
    text = str(value or '')
    if not text.isascii() or not text.isdigit() or len(text) > 19 or not 0 < int(text) <= 9223372036854775807:
        raise CapturaEquipoError(f'Selecciona {nombre} válido.', 400)
    return int(text)


def _validar(actor, activo):
    if _limite_explicito_mantenimiento(actor) in {'none', 'view'}:
        raise PermissionDenied('Tu permiso vigente no permite registrar mantenimiento.')
    validar_equipo(actor, activo)


def _decision(raw):
    if not isinstance(raw, dict):
        raise CapturaEquipoError('Revisa las decisiones del archivo.', 400)
    accion = str(raw.get('accion') or '').strip()
    if accion not in {'crear', 'vincular', 'descartar', ''}:
        raise CapturaEquipoError('Elige crear, vincular o dejar pendiente.', 400)
    fila, slot = _id(raw.get('fila'), 'una fila'), _id(raw.get('slot'), 'un servicio')
    if fila > MAX_SOURCE_ROWS or slot not in (1,2):
        raise CapturaEquipoError('La fila o el servicio no pertenece al archivo.', 400)
    if accion in {'', 'descartar'}:
        return dict(fila=fila, slot=slot, accion=accion)
    motivo, evidencia = str(raw.get('motivo') or '').strip(), str(raw.get('evidencia') or '').strip()
    if not motivo or not evidencia or len(motivo) > 2000 or len(evidencia) > 2000 or '\x00' in motivo or '\x00' in evidencia:
        raise CapturaEquipoError('Indica motivo y evidencia documental (máximo 2000 caracteres).', 400)
    return dict(fila=fila, slot=slot, accion=accion, activo_id=_id(raw.get('activo_id'), 'un equipo'),
                orden_id=_id(raw.get('orden_id'), 'una orden') if accion == 'vincular' else None,
                motivo=motivo, evidencia=evidencia)


def _validar_origen(actor, origen):
    activo = Activo.objects.filter(pk=origen.activo_original_id).first()
    if activo is None:
        raise PermissionDenied('El equipo original ya no está disponible en tu ámbito.')
    _validar(actor, activo)
    if origen.orden_id is not None and not authorized_orders(actor).filter(pk=origen.orden_id, activo_ref=activo).exists():
        raise PermissionDenied('La orden original ya no está en tu ámbito autorizado.')


def _comprobar_reintento(actor, origen, huella):
    # Primero permisos actuales, incluso en conflictos y resultados eliminados.
    _validar_origen(actor, origen)
    if origen.huella_revision != huella:
        raise CapturaEquipoError('Este servicio ya tiene otra decisión, motivo o evidencia. Se conserva la revisión original.', 409)
    if origen.orden_id is None:
        raise CapturaEquipoError('El trabajo confirmado fue eliminado. Este origen no puede recrearlo.', 410)


def revisar_importacion(*, usuario, archivo, decisiones, sheet_name=''):
    actor = actor_importacion(usuario)
    preview = preview_bitacora(archivo, sheet_name=sheet_name)
    if not isinstance(decisiones, list) or len(decisiones) > 2 * MAX_SOURCE_ROWS:
        raise CapturaEquipoError('Revisa la lista de decisiones.', 400)
    sources = {(row['fila'],row['slot']): row for row in preview['servicios']}
    revisadas, seen = [], set()
    for raw in decisiones:
        decision = _decision(raw)
        key = (decision['fila'],decision['slot'])
        if key not in sources or key in seen:
            raise CapturaEquipoError('Una selección no existe en el archivo o está repetida.', 400)
        seen.add(key)
        if decision['accion'] in {'', 'descartar'}:
            continue
        servicio = sources[key]
        activo = Activo.objects.filter(pk=decision['activo_id']).first()
        if activo is None:
            raise PermissionDenied('El equipo no está disponible en tu ámbito autorizado.')
        crear = decision['accion'] == 'crear'
        _validar(actor, activo)
        identidad = dict(archivo_sha256=preview['sha256'], hoja=preview['sheet_name'], fila=key[0], slot=key[1])
        huella = huella_captura({'fuente':servicio,'decision':decision})
        origen = OrigenImportacionBitacora.objects.filter(**identidad).first()
        if origen:
            _comprobar_reintento(actor, origen, huella)
        orden = None
        if not crear:
            orden = authorized_orders(actor).filter(pk=decision['orden_id'], activo_ref=activo).first()
            if orden is None:
                raise PermissionDenied('La orden debe pertenecer al equipo seleccionado y a tu ámbito.')
        if crear:
            if servicio['fecha'] is None or servicio['costo'] is None:
                raise CapturaEquipoError('No se puede crear: la fecha o el costo de la fuente es desconocido. Deja pendiente o vincula un trabajo existente.', 400)
            try:
                OrdenMantenimiento._meta.get_field('costo_otros').clean(Decimal(servicio['costo']), None)
            except ValidationError:
                raise CapturaEquipoError('El costo de la fuente excede el formato nativo del trabajo.', 400)
        revisadas.append(dict(servicio=servicio, decision=decision, activo=activo, orden=orden,
                              identidad=identidad, huella=huella, origen=origen))
    return actor, preview, revisadas


@transaction.atomic
def aplicar_importacion(*, usuario, archivo, decisiones, confirmado, sheet_name=''):
    actor, preview, revisadas = revisar_importacion(usuario=usuario, archivo=archivo,
                                                  decisiones=decisiones, sheet_name=sheet_name)
    if confirmado is not True:
        raise CapturaEquipoError('Revisa y confirma expresamente las decisiones antes de aplicar.', 400)
    # Bloquea los destinos actuales en orden estable, antes de cualquier escritura.
    asset_ids = {row['activo'].pk for row in revisadas}
    asset_ids.update(row['origen'].activo_original_id for row in revisadas if row['origen'])
    activos = {a.pk:a for a in Activo.objects.select_for_update(no_key=True).filter(pk__in=asset_ids).order_by('pk')}
    order_ids = {row['orden'].pk for row in revisadas if row['orden']}
    order_ids.update(row['origen'].orden_id for row in revisadas if row['origen'] and row['origen'].orden_id)
    ordenes = {o.pk:o for o in OrdenMantenimiento.objects.select_for_update(no_key=True).filter(pk__in=order_ids).order_by('pk')}
    results = []
    # Orden estable de índices UNIQUE para evitar deadlocks entre archivos iguales.
    for row in sorted(revisadas, key=lambda row: (row['identidad']['fila'],row['identidad']['slot'])):
        actor = actor_importacion(usuario)
        activo = activos.get(row['activo'].pk)
        if activo is None:
            raise PermissionDenied('El equipo seleccionado ya no está disponible.')
        _validar(actor, activo)
        servicio, decision = row['servicio'], row['decision']
        origen, nuevo = OrigenImportacionBitacora.objects.get_or_create(**row['identidad'], defaults=dict(
            huella_revision=row['huella'], fuente=servicio, decision=decision,
            autor=actor, autor_original_id=actor.pk, activo_original_id=activo.pk))
        actor = actor_importacion(usuario)
        _validar(actor, activo)
        if not nuevo:
            _comprobar_reintento(actor, origen, row['huella'])
            results.append(dict(origen_id=origen.pk, orden_id=origen.orden_id, reintento=True))
            continue
        if decision['accion'] == 'crear':
            def crear(archivos_nuevos):
                fecha = _as_date(servicio['fecha'])
                orden = OrdenMantenimiento.objects.create(activo_ref=activo, creado_por=actor,
                    tipo='PREVENTIVO', prioridad='MEDIA', estatus='CERRADA',
                    fecha_programada=fecha, fecha_inicio=fecha, fecha_cierre=fecha,
                    responsable='Servicio externo', descripcion='Servicio importado desde bitácora histórica',
                    costo_otros=Decimal(servicio['costo']))
                BitacoraMantenimiento.objects.create(orden=orden, accion='IMPORT_SERVICIO',
                    comentario=decision['motivo'], usuario=actor)
                return orden
            orden, _ = capturar_equipo_autorizado(usuario=actor, activo=activo, operacion='bitacora_importacion',
                clave=None, contenido={'fuente':servicio,'decision':decision}, crear=crear,
                validar=_validar, ordenes=authorized_orders)
        else:
            orden = ordenes.get(decision['orden_id'])
            if orden is None or orden.activo_ref_id != activo.pk or not authorized_orders(actor).filter(pk=orden.pk, activo_ref=activo).exists():
                raise PermissionDenied('La orden ya no pertenece al equipo o a tu ámbito autorizado.')
        origen.orden, origen.orden_original_id = orden, orden.pk
        origen.save(update_fields=['orden','orden_original_id'])
        log_event(actor, 'IMPORT', 'activos.BitacoraImport', str(origen.pk), dict(
            filename=preview['filename'], sheet_name=preview['sheet_name'], source_format=preview['source_format'],
            origen_id=origen.pk, activo_id=activo.pk, orden_id=orden.pk, fila=servicio['fila'], slot=servicio['slot'],
            decision=decision, dry_run=False, filas_validas=1, activos_creados=0, activos_actualizados=0,
            servicios_creados=int(decision['accion']=='crear'), servicios_omitidos=int(decision['accion']=='vincular')))
        results.append(dict(origen_id=origen.pk, orden_id=orden.pk, reintento=False))
    return results
