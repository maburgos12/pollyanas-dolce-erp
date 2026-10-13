"""Consulta Point fuera del bloqueo de captura y publica solo información auditada."""
import hashlib
import inspect
import json
import logging
from contextlib import ExitStack
from uuid import uuid4

from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointInventorySnapshot, PointSyncJob
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService, AuditStockHistoryError, SOURCE_NAME, HISTORY_LIMIT
from pos_bridge.services.historical_inventory_capture import HistoricalInventoryCaptureError
from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.services.point_http_client import PointHttpSessionClient

from .conteos_conciliacion import MZ, _pendiente, calcular_cierre_linea, cierre_dia
from .conteos_point import sucursal_point_conteo
from .models_conteos import ConteoSucursal, EventoConteoSucursal
from .services_conteos import lecturas_efectivas

logger = logging.getLogger(__name__)


def huella_lecturas(readings):
    return hashlib.sha256(json.dumps([
        [r.pk,r.linea_id,r.ronda,str(r.cantidad),r.contado_en.isoformat() if r.contado_en else None]
        for r in readings], sort_keys=True).encode()).hexdigest()


def _adquirir_historial(client, settings, branch, product, fecha):
    rows = client.get_stock_history(product.external_id,branch.external_id,movements=HISTORY_LIMIT)
    receipt = timezone.now().isoformat()
    raw = json.dumps(rows,sort_keys=True,default=str).encode()
    digest = hashlib.sha256(raw).hexdigest()
    settings.raw_exports_dir.mkdir(parents=True,exist_ok=True)
    source_file = settings.raw_exports_dir / f'conteo-{branch.pk}-{product.pk}-{uuid4()}.json'
    with source_file.open('xb') as file:
        file.write(raw)
    code = inspect.getsource(_adquirir_historial)
    evidence = {
        'source':SOURCE_NAME, 'domain':'PRODUCT', 'response_complete':True,
        'branch_id':branch.pk, 'product_id':product.pk,
        'request':{'path':'/Stock/GetHistorial','params':{
            'tipo':'false','almacen':str(branch.external_id),'pkproducto':str(product.external_id),
            'movimientos':str(HISTORY_LIMIT),'tipoMovimiento':''}},
        'retrieved_at':receipt, 'history_limit':HISTORY_LIMIT, 'fetched_rows':len(rows),
        'raw_sha256':digest,'original_locator':{'source_file':str(source_file),'source_line':1},
        'request_provenance':{'kind':'DERIVED_FROM_ACQUISITION_SCRIPT',
            'source_file':__file__,'source_code':code,'source_sha256':hashlib.sha256(code.encode()).hexdigest(),
            'client_contract':'PointHttpSessionClient.get_stock_history'},
    }
    service = AuditStockHistoryService()
    service.ingest_original_response(branch,product,fecha.replace(day=1),rows,evidence=evidence)
    return _historial(service,branch,product)


def _historial(service,branch,product):
    record = service._existing_import(branch,product)
    if not record or record.point_branch_id != branch.pk or record.point_product_id != product.pk:
        return None
    batch = service._original_batch(record)
    if batch:
        batch.update(branch_id=branch.pk,product_id=product.pk)
    return batch


def _publicar(count, readings, result):
    with transaction.atomic():
        current = ConteoSucursal.objects.select_for_update().get(pk=count.pk)
        if (current.estado not in ('ENVIADO','ACEPTADO') or current.version != count.version or current.conciliacion != count.conciliacion
                or huella_lecturas(lecturas_efectivas(current)) != huella_lecturas(readings)):
            return False
        previous = current.conciliacion
        comparable = lambda value: {k:v for k,v in value.items() if k not in ('procesado_en','ejecucion')}
        if comparable(previous) == comparable(result):
            return True
        current.conciliacion = result
        current.save(update_fields=['conciliacion'])
        EventoConteoSucursal.objects.create(conteo=current,actor=None,action='conciliacion_nocturna',payload=result)
        return True


def conciliar_conteos_nocturnos():
    now = timezone.now()
    counts = ConteoSucursal.objects.filter(
        Q(conciliacion={}) | Q(conciliacion__reintentar=True),
        estado__in=('ENVIADO','ACEPTADO'),fecha__lt=now.astimezone(MZ).date(),
    ).order_by('pk')
    service, batches = AuditStockHistoryService(), {}
    summary = {'procesados':0,'calculados':0,'pendientes':0,'reintentar':False}
    with ExitStack() as stack:
        client, settings, attempted = None, None, False
        for count in counts:
            readings = lecturas_efectivas(count)
            branch = sucursal_point_conteo(count)
            close = cierre_dia(count.fecha)
            lines = {}
            for reading in readings:
                snapshot, batch = None, None
                if not branch:
                    result = _pendiente('Sucursal Point sin correspondencia única.')
                else:
                    result = calcular_cierre_linea(reading,snapshot=None,batch=None,cierre=close,branch_id=branch.pk)
                    if result['reintentar']:
                        snapshot = PointInventorySnapshot.objects.filter(
                            branch=branch,product_id=reading.linea.producto_id,
                            sync_job__status=PointSyncJob.STATUS_SUCCESS,
                            sync_job__job_type=PointSyncJob.JOB_TYPE_INVENTORY,
                            sync_job__finished_at__lte=now,
                        ).select_related('sync_job').order_by('-captured_at','-pk').first()
                        key = (branch.pk,reading.linea.producto_id)
                        try:
                            if key not in batches:
                                try:
                                    batches[key] = _historial(service,branch,reading.linea.producto)
                                except (AuditStockHistoryError,HistoricalInventoryCaptureError,ValueError,TypeError):
                                    batches[key] = None
                            batch = batches[key]
                            result = calcular_cierre_linea(reading,snapshot=snapshot,batch=batch,cierre=close,branch_id=branch.pk)
                            if snapshot and result['reintentar']:
                                if not attempted:
                                    attempted = True
                                    if stack.enter_context(point_account_session_lock(wait=False)):
                                        settings = load_point_bridge_settings()
                                        client = stack.enter_context(PointHttpSessionClient(settings))
                                        client.login()
                                if client:
                                    batches[key] = _adquirir_historial(client,settings,branch,reading.linea.producto,count.fecha)
                                    result = calcular_cierre_linea(reading,snapshot=snapshot,batch=batches[key],cierre=close,branch_id=branch.pk)
                                else:
                                    result = _pendiente('Point ocupado por otra sincronización; consulta pendiente.',reintentar=True)
                        except Exception:
                            logger.exception('Consulta nocturna pendiente: conteo=%s linea=%s',count.pk,reading.linea_id)
                            # A failed session is never reused to publish quantities.
                            client = None
                            result = _pendiente('No se pudo acreditar la consulta Point; se reintentará.',reintentar=True)
                lines[str(reading.linea_id)] = result
            calculated = sum(item['estado']=='CALCULADA' for item in lines.values())
            retry = any(item['reintentar'] for item in lines.values())
            result = {'estado':'CALCULADA' if calculated and calculated==len(lines) else 'PARCIAL' if calculated else 'PENDIENTE',
                'lineas':lines,'cierre_en':close.isoformat(),'procesado_en':timezone.now().isoformat(),
                'huella_lecturas':huella_lecturas(readings),'ronda':count.ronda,
                'reintentar':retry,'ejecucion':str(uuid4())}
            if _publicar(count,readings,result):
                summary['procesados'] += 1
                summary['calculados'] += calculated
                summary['pendientes'] += len(lines)-calculated
                summary['reintentar'] |= retry
            else:
                summary['reintentar'] = True
    return summary
