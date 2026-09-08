"""Publicación verificada de puntos de minuta por la API oficial del origen."""
from django.utils import timezone

from .agente_dg_client import AgenteDGError, get_minute_agreement, patch_minute_agreement
from .models import SeguimientoChecklistItem


def sincronizar_checklist_minuta(item, *, check=None, completado=None):
    source_id=(item.metadata or {}).get('source_id')
    source=get_minute_agreement(source_id)
    if source.get('archived_at') or source.get('status') in {'COMPLETED','CANCELLED'}:
        raise AgenteDGError('La minuta está cerrada en origen; actualiza el acuerdo.')
    remote=[dict(point) for point in source.get('checklist_items', [])]
    max_length=SeguimientoChecklistItem._meta.get_field('titulo').max_length
    def key(text):
        return str(text).strip()[:max_length].casefold()
    local=list(item.checklist.all())
    targets=[check] if check is not None else local
    if check is None and {key(p.get('text','')) for p in remote} != {key(p.titulo) for p in local}:
        raise AgenteDGError('Los puntos cambiaron en origen; actualiza el acuerdo antes de entregar.')
    desired={}
    changed=False
    for point in targets:
        matches=[entry for entry in remote if key(entry.get('text',''))==key(point.titulo)]
        if len(matches)!=1:
            raise AgenteDGError('No se puede identificar el punto en origen; actualiza el acuerdo.')
        entry=matches[0]
        value=bool(completado) if check is not None else point.completado
        desired[key(point.titulo)]=value
        if bool(entry.get('completed')) != value:
            entry['completed']=value
            entry['completed_at']=(point.completado_at or timezone.now()).isoformat() if value else None
            changed=True
    confirmed=patch_minute_agreement(source_id,checklist_items=remote) if changed else source
    actual=confirmed.get('checklist_items',[])
    for title,value in desired.items():
        matches=[entry for entry in actual if key(entry.get('text',''))==title]
        if len(matches)!=1 or bool(matches[0].get('completed')) != value:
            raise AgenteDGError('El origen no confirmó el punto; vuelve a consultar antes de reintentar.')
    if confirmed.get('id') != source_id or not confirmed.get('updated_at'):
        raise AgenteDGError('Respuesta del origen sin identidad o fecha de actualización.')
    # La API puede haber llamado nuestro webhook. Leer su metadata antes de guardar
    # el punto de control; no sostener un bloqueo de fila durante la llamada HTTP.
    item.refresh_from_db()
    item.metadata={**(item.metadata or {}),'checklist_writeback':{
        'updated_at':confirmed['updated_at'],'items':actual,
    }}
    item.save(update_fields=['metadata','updated_at'])
