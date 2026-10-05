"""Flujo progresivo sin almacenar el archivo ni la revisión en la vista previa."""
import base64
import binascii
from zipfile import BadZipFile

from django.core import signing
from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.core.files.uploadedfile import SimpleUploadedFile
from django.http import JsonResponse
from django.template.loader import render_to_string

from mantenimiento.services_access import authorized_orders, can_view_costs
from mantenimiento.services_capturas_equipos import CapturaEquipoError
from .services_importacion import actor_importacion, aplicar_importacion, revisar_importacion
from .services_pasaporte import activos_autorizados
from .utils.bitacora_import import MAX_FILE_BYTES, leer_archivo, preview_bitacora

SALT = 'activos.bitacora.revision.v1'


def _token(datos):
    value = signing.dumps(datos, salt=SALT, compress=True)
    limite = min(900 * 1024, (settings.DATA_UPLOAD_MAX_MEMORY_SIZE or 2500000) // 2)
    if len(value.encode()) > limite:
        raise ValueError('La revisión supera 1 MB. Divide el archivo por hoja o en archivos menores; conserva las decisiones para volver a revisar.')
    return value


def _leer_token(value):
    if not value or len(value) > 4 * MAX_FILE_BYTES:
        raise ValueError('La revisión no está disponible. Conserva el archivo y vuelve a obtener la vista previa.')
    try:
        datos = signing.loads(value, salt=SALT, max_age=24 * 60 * 60)
    except signing.SignatureExpired:
        datos = signing.loads(value, salt=SALT)
        datos['_expirada'] = True
    raw = base64.b64decode(datos['bytes'], validate=True)
    if len(raw) > MAX_FILE_BYTES:
        raise ValueError('El archivo supera 2 MB.')
    return datos, SimpleUploadedFile(datos['filename'], raw)


def _desde_post(post, preview, anteriores=()):
    anteriores = {(d['fila'],d['slot']):d for d in anteriores}
    return [dict(fila=row['fila'], slot=row['slot'], **{
        key: post.get(f"{key}_{row['fila']}_{row['slot']}", '')
        for key in ('accion','activo_id','orden_id','motivo','evidencia')})
        if f"accion_{row['fila']}_{row['slot']}" in post else anteriores.get((row['fila'],row['slot']),dict(fila=row['fila'],slot=row['slot'],accion=''))
        for row in preview['servicios']]


def procesar_importacion(request):
    actor = None
    contexto = {'importacion_abierta': True, 'importacion_post': request.POST}
    status, error = 200, ''
    try:
        actor = actor_importacion(request.user)
        contexto['importacion_costos'] = can_view_costs(actor)
        fase = 'page' if 'mover_pagina' in request.POST else request.POST.get('fase', 'preview')
        if fase == 'preview' and request.FILES.get('archivo_bitacora'):
            archivo = request.FILES['archivo_bitacora']
            nombre, raw = leer_archivo(archivo)
            datos = dict(filename=nombre, bytes=base64.b64encode(raw).decode(),
                         hoja=request.POST.get('sheet_name', '').strip())
        else:
            token = request.POST.get('revision_token') if fase == 'confirm' else request.POST.get('archivo_token')
            datos, archivo = _leer_token(token)
        preview = preview_bitacora(archivo, sheet_name=datos['hoja'])
        contexto['importacion_preview'] = preview
        expirada = datos.pop('_expirada', False)
        contexto['archivo_token'] = request.POST.get('archivo_token') or request.POST.get('revision_token')
        decisiones = _desde_post(request.POST, preview, datos.get('decisiones', []))
        contexto['archivo_token'] = _token({**{k:v for k,v in datos.items() if k != 'revisado'}, 'decisiones':decisiones})
        if fase == 'confirm' and not expirada:
            contexto['revision_token'] = request.POST.get('revision_token')
        if fase == 'confirm' and expirada:
            raise CapturaEquipoError('La revisión venció. Se conserva el archivo; revisa de nuevo las decisiones antes de confirmar.', 409)
        if fase in {'preview','edit','page'}:
            pass
        elif fase == 'review':
            _, _, revisadas = revisar_importacion(usuario=actor, archivo=archivo,
                sheet_name=datos['hoja'], decisiones=decisiones)
            if not revisadas:
                raise ValueError('Selecciona al menos un servicio para crear o vincular; los pendientes no se aplican.')
            contexto.update(importacion_revisadas=revisadas,
                            revision_token=_token({**datos, 'decisiones':decisiones, 'revisado':True}))
        elif fase == 'confirm':
            if datos.get('revisado') is not True:
                raise ValueError('Obtén primero la revisión de las decisiones.')
            resultados = aplicar_importacion(usuario=actor, archivo=archivo, sheet_name=datos['hoja'],
                decisiones=datos['decisiones'], confirmado=request.POST.get('confirmado') == '1')
            contexto['importacion_resultados'] = resultados
            decisiones = datos['decisiones']
        else:
            raise ValueError('La etapa de revisión no es válida.')
        rows = []
        seleccion = {(d['fila'],d['slot']):d for d in decisiones}
        for servicio in preview['servicios']:
            rows.append({**servicio, 'seleccion':seleccion.get((servicio['fila'],servicio['slot']),{})})
        pagina = str(request.POST.get('mover_pagina', request.POST.get('pagina', '0')))
        if not pagina.isascii() or not pagina.isdigit() or len(pagina) > 4:
            raise ValueError('La página de revisión no es válida.')
        pagina = min(int(pagina), max(0,(len(rows)-1)//100))
        contexto.update(importacion_filas=rows[pagina*100:(pagina+1)*100], pagina_importacion=pagina,
            pagina_anterior=pagina-1 if pagina else None,
            pagina_siguiente=pagina+1 if (pagina+1)*100 < len(rows) else None,
            archivo_token=_token({**{k:v for k,v in datos.items() if k != 'revisado'},'decisiones':decisiones}))
    except PermissionDenied as exc:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({'ok': False, 'toast': {'type': 'error', 'message':str(exc), 'persistent':True}}, status=403)
        raise
    except CapturaEquipoError as exc:
        status, error = exc.status_code, str(exc.detail)
    except signing.BadSignature:
        status, error = 400, 'La revisión no es válida. Conserva el archivo y solicita otra vista previa.'
    except (ValueError, KeyError, TypeError, binascii.Error, BadZipFile) as exc:
        status, error = 400, str(exc)
    contexto.update(importacion_error=error,
                    importacion_equipos=activos_autorizados(actor).filter(activo=True).order_by('nombre'),
                    importacion_ordenes=authorized_orders(actor).select_related('activo_ref').order_by('-fecha_programada'))
    if error and contexto.get('importacion_preview'):
        filas = [{**row, 'seleccion':decision} for row, decision in zip(
            contexto['importacion_preview']['servicios'], _desde_post(request.POST, contexto['importacion_preview'], datos.get('decisiones', [])))]
        pagina = str(request.POST.get('pagina', '0'))
        pagina = min(int(pagina), max(0,(len(filas)-1)//100)) if pagina.isascii() and pagina.isdigit() and len(pagina) <= 4 else 0
        contexto.update(importacion_filas=filas[pagina*100:(pagina+1)*100], pagina_importacion=pagina,
            pagina_anterior=pagina-1 if pagina else None, pagina_siguiente=pagina+1 if (pagina+1)*100 < len(filas) else None)
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return JsonResponse(dict(ok=not error, target='#importacion-bitacora',
            html=render_to_string('activos/_importacion_bitacora.html', contexto, request=request),
            archivo_token=contexto.get('archivo_token'), revision_token=contexto.get('revision_token'),
            toast={'type':'error' if error else 'success', 'message': error or ('Importación confirmada.' if contexto.get('importacion_resultados') else 'Revisa los servicios del archivo.')}), status=status)
    return contexto, status
