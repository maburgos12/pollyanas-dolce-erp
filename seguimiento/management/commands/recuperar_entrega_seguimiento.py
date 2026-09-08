"""Recuperación puntual de una entrega borrada por sincronización, con auditoría."""
import json

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from core.access import can_review_seguimiento_global
from core.audit import log_event
from core.models import AuditLog
from seguimiento.models import SeguimientoItem


class Command(BaseCommand):
    help = "Inspecciona una entrega auditada; --apply restaura solo su estado de revisión. No envía avisos."

    def add_arguments(self, parser):
        parser.add_argument('--item', type=int, required=True)
        parser.add_argument('--actor', required=True)
        parser.add_argument('--apply', action='store_true')

    @transaction.atomic
    def handle(self, *args, **options):
        actor = get_user_model().objects.filter(username=options['actor'], is_active=True).first()
        if not actor or not can_review_seguimiento_global(actor):
            raise CommandError('El actor debe ser un usuario activo de Dirección General.')
        item = SeguimientoItem.objects.select_for_update().get(pk=options['item'])
        latest = AuditLog.objects.filter(model='SeguimientoItem', object_id=str(item.pk), action__in=[
            'seguimiento.entrega', 'seguimiento.aprobar', 'seguimiento.devolver',
            'seguimiento.retractar', 'seguimiento.completar',
        ]).order_by('-timestamp', '-pk').first()
        if not latest or latest.action != 'seguimiento.entrega':
            raise CommandError('No hay una entrega sin resolver que pueda recuperarse.')
        metadata = item.metadata or {}
        if item.esta_cerrado or metadata.get('source_archived_at') or str(metadata.get('source_status','')).upper() in {
            'CANCELLED','CANCELED','COMPLETED','CLOSED','APPROVED','REVIEWED',
        }:
            raise CommandError('El acuerdo está cerrado o cancelado en ERP/origen; requiere revisión individual.')
        if not item.requiere_aprobacion:
            raise CommandError('El acuerdo no requiere aprobación.')
        if item.comentarios.filter(tipo='REVISION_DG', created_at__gt=latest.timestamp).exists():
            raise CommandError('Hay una intervención DG posterior; no se recupera automáticamente.')
        result = {'item':item.pk, 'titulo':item.titulo, 'entrega_audit_id':latest.pk,
                  'entrega_at':latest.timestamp.isoformat(), 'estado_previo':item.estatus,
                  'puntos':item.checklist.count(), 'completos':item.checklist.filter(completado=True).count(),
                  'aplicado':False}
        if options['apply'] and not metadata.get('revision_erp'):
            item.estatus = SeguimientoItem.ESTATUS_EN_REVISION
            item.metadata = {**metadata, 'revision_erp': {'estatus': item.estatus,
                'actor_id':latest.user_id, 'at':latest.timestamp.isoformat(), 'recovered_from_audit_id':latest.pk}}
            item.save(update_fields=['estatus','metadata','updated_at'])
            log_event(actor,'seguimiento.recuperar_entrega','SeguimientoItem',item.pk,
                      {**result,'autorizacion':'Corrección de entrega solicitada por DG','recuperado_at':timezone.now().isoformat()})
            result['aplicado']=True
        self.stdout.write(json.dumps(result, ensure_ascii=False))
