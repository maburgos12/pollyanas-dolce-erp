"""Archivos privados de conteos, sin URL pública bajo MEDIA_URL."""
import hashlib
from pathlib import Path
from uuid import uuid4

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.files.storage import FileSystemStorage
from django.db import transaction
from django.http import FileResponse, Http404
from django.shortcuts import get_object_or_404

from mantenimiento.evidence_validation import EvidenceValidationError, validate_evidence_files
from .conteos_access import puede_capturar, puede_revisar
from .models_conteos import ConteoSucursal, EventoConteoSucursal, OperacionConteoSucursal
from .services_conteos import ConteoConflict, _hash, _uuid


def _storage():
    return FileSystemStorage(location=getattr(settings,'CONTEOS_PRIVATE_ROOT',Path(settings.BASE_DIR)/'storage'/'conteos_evidencias'))


def adjuntar_evidencia(*,conteo_id,actor,version,request_id,uploaded):
    if uploaded is None: raise ValidationError('Selecciona un archivo.')
    if uploaded.size > 10*1024*1024: raise ValidationError('La evidencia excede 10 MB.')
    try: validate_evidence_files([uploaded])
    except EvidenceValidationError as exc: raise ValidationError(str(exc)) from exc
    digest=hashlib.sha256()
    for chunk in uploaded.chunks(): digest.update(chunk)
    uploaded.seek(0)
    token=_uuid(request_id)
    fingerprint=_hash({'action':'evidencia','actor':actor.pk,'version':version,'nombre':uploaded.name,'sha256':digest.hexdigest()})
    storage=_storage()
    stored=None
    try:
        with transaction.atomic():
            count=ConteoSucursal.objects.select_for_update().select_related('sucursal').get(pk=conteo_id)
            reviewer=puede_revisar(actor,count)
            if not reviewer and not puede_capturar(actor,count): raise ValidationError('Sin acceso al conteo.')
            previous=count.operaciones.filter(request_id=token).first()
            if previous:
                if previous.fingerprint!=fingerprint: raise ConteoConflict('La solicitud ya se usó con otro archivo o contenido.')
                return previous.result
            if count.version != version: raise ConteoConflict('El conteo cambió. Actualiza antes de adjuntar.')
            if count.estado=='CANCELADO' or (not reviewer and count.estado not in ('CAPTURA','RECONTEO')):
                raise ValidationError('La captura ya fue enviada; solicita revisión para agregar evidencia.')
            stored=storage.save(str(uuid4())+Path(uploaded.name).suffix.lower(),uploaded)
            event=EventoConteoSucursal.objects.create(conteo=count,actor=actor,action='evidencia',payload={
                'archivo':stored,'nombre':uploaded.name,'sha256':digest.hexdigest(),'tamano':uploaded.size,'ronda':count.ronda})
            count.version+=1
            count.save(update_fields=['version'])
            result={'id':count.pk,'version':count.version,'ronda':count.ronda,'estado':count.estado,'evidencia_id':event.pk}
            OperacionConteoSucursal.objects.create(conteo=count,actor=actor,request_id=token,fingerprint=fingerprint,result=result)
            return result
    except Exception:
        if stored: storage.delete(stored)
        raise


def descargar_evidencia(count,event_id,user):
    event=get_object_or_404(count.eventos,pk=event_id,action='evidencia')
    if not puede_revisar(user,count) and count.estado in ('CAPTURA','RECONTEO') and event.payload.get('ronda')!=count.ronda:
        raise Http404
    name=event.payload.get('archivo','')
    if not name or Path(name).name!=name: raise Http404
    storage=_storage()
    if not storage.exists(name): raise Http404('Archivo no disponible.')
    response=FileResponse(storage.open(name,'rb'),as_attachment=True,filename=event.payload.get('nombre','evidencia'))
    response['X-Content-Type-Options']='nosniff'
    response['Cache-Control']='private, no-store'
    return response
