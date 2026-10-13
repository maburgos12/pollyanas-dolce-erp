"""Conciliación informativa al cierre; jamás cambia la observación física o saldos."""
from datetime import datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.utils import timezone
from django.utils.dateparse import parse_datetime
from pos_bridge.models import PointSyncJob
from pos_bridge.services.audit_stock_history_service import AuditStockHistoryService, _normalized, _boolean
from pos_bridge.services.historical_inventory_capture import (
    historical_waste_effect, point_stock_history_instant,
)
from .conteos_point import leer_fila_point, misma_unidad

MZ = ZoneInfo('America/Mazatlan')


def cierre_dia(fecha):
    return datetime.combine(fecha + timedelta(days=1), time.min, MZ)


def _pendiente(motivo, *, reintentar=False, consultar_historial=False):
    return {
        'estado':'PENDIENTE', 'motivo':motivo, 'reintentar':reintentar,
        'consultar_historial':consultar_historial,
        'fisico_al_cierre':None, 'point_al_cierre':None, 'diferencia':None,
        'movimientos_fisico':[], 'movimientos_point':[], 'posteriores_cierre':[],
    }


def _efecto(raw, *, fisico):
    previous = Decimal(str(raw['Existencia_anterior']))
    new = Decimal(str(raw['Existencia_nueva']))
    quantity = Decimal(str(raw['Cantidad']))
    delta = new - previous
    waste = historical_waste_effect(raw)
    if waste.status == 'INVALID':
        raise ValueError('Cancelación de merma sin efecto acreditado.')
    if waste.status == 'VALID':
        return delta
    cancelled = raw['Cancelado'] is True or str(raw['Cancelado']).lower() == 'true'
    if cancelled:
        if delta:
            raise ValueError('Movimiento cancelado con efecto no acreditado.')
        return Decimal('0')
    category = AuditStockHistoryService._category(raw.get('Movimiento',''), delta)
    if category is None:
        raise ValueError('Tipo de movimiento sin correspondencia comprobada.')
    if abs(delta) != abs(quantity):
        raise ValueError('Cantidad del movimiento no corresponde al cambio de saldo.')
    if category == 'identified_adjustment':
        if fisico:
            raise ValueError('Un ajuste Point no acredita un movimiento físico.')
        words = set(_normalized(raw.get('Movimiento','')).split())
        if ('SALIDA' in words and delta > 0) or ('ENTRADA' in words and delta < 0):
            raise ValueError('Dirección del ajuste Point sin evidencia consistente.')
        return delta
    positive = category in {'production','transfer_in','conversion_in'}
    if _normalized(raw.get('Movimiento','')) == 'CANCELACION VENTA':
        positive = True
    if delta and (delta > 0) != positive:
        raise ValueError('Dirección del movimiento sin evidencia consistente.')
    return delta


def _intervalo(rows, start, end, *, fisico):
    lower, upper = min(start,end), max(start,end)
    selected = [raw for raw in rows if lower <= point_stock_history_instant(raw) < upper]
    amount = sum((_efecto(raw,fisico=fisico) for raw in selected), Decimal('0'))
    return (amount if start <= end else -amount), [raw['FK_Movimiento'] for raw in selected]


def calcular_cierre_linea(reading, *, snapshot, batch, cierre, branch_id, now=None):
    now = now or timezone.now()
    line = reading.linea
    if reading.cantidad is None:
        return _pendiente('Sin cantidad física; se conserva la incidencia.')
    if reading.contado_en is None:
        return _pendiente('Falta hora declarada del conteo; la última edición no la sustituye.')
    if not line.producto_id:
        return _pendiente('No hay historial de movimientos de insumo acreditado para este cierre.')
    if timezone.is_naive(reading.contado_en) or reading.contado_en > now or cierre > now:
        return _pendiente('La hora física o el cierre aún no son comprobables.')
    if snapshot is None:
        return _pendiente('Falta snapshot o historial original completo de Point.',reintentar=True)
    cycle = snapshot.sync_job
    if (snapshot.branch_id != branch_id
            or snapshot.product_id != line.producto_id
            or cycle.status != PointSyncJob.STATUS_SUCCESS
            or cycle.job_type != PointSyncJob.JOB_TYPE_INVENTORY
            or not cycle.finished_at
            or not cycle.started_at <= snapshot.captured_at <= cycle.finished_at <= now):
        return _pendiente('El snapshot no pertenece a un ciclo Point completo y válido.',reintentar=True)
    raw_stock = leer_fila_point(snapshot.raw_payload,codigo=line.codigo)
    if raw_stock is None or raw_stock[0] != snapshot.stock or not misma_unidad(raw_stock[1],line.unidad):
        return _pendiente('Cantidad, código o unidad Point sin evidencia consistente.',reintentar=True)
    if batch is None:
        return _pendiente('Falta historial original completo de Point.',reintentar=True,consultar_historial=True)
    if batch.get('branch_id') != branch_id or batch.get('product_id') != line.producto_id:
        return _pendiente('El historial no corresponde a esta sucursal y producto.',reintentar=True,consultar_historial=True)
    evidence = batch['evidence']
    rows = batch['unique_rows']
    lower = min(reading.contado_en,cierre,cycle.started_at)
    upper = max(reading.contado_en,cierre,cycle.finished_at)
    receipt = parse_datetime(evidence['retrieved_at'])
    if not receipt or timezone.is_naive(receipt) or not upper <= receipt <= now:
        return _pendiente('Historial recibido antes de completar la ventana requerida.',reintentar=True,consultar_historial=True)
    if evidence['original_count'] >= evidence['history_limit'] and (
            not rows or min(point_stock_history_instant(raw) for raw in rows) >= lower):
        return _pendiente('El límite del historial no cubre el inicio de la ventana.',reintentar=True)
    if any(cycle.started_at <= point_stock_history_instant(raw) <= cycle.finished_at for raw in rows):
        return _pendiente('Hubo movimientos durante la extracción del snapshot; su corte es ambiguo.',reintentar=True)
    if any(abs((point_stock_history_instant(raw)-reading.contado_en).total_seconds()) < 1 for raw in rows):
        return _pendiente('No se puede ordenar el conteo y un movimiento en el mismo segundo.')
    if any(lower <= parse_datetime(gap['movement_at']) <= upper for gap in batch['stock_chain_gaps']):
        return _pendiente('La cadena de saldos Point tiene un tramo sin acreditar.',reintentar=True,consultar_historial=True)
    ordered = sorted(rows,key=lambda raw:(point_stock_history_instant(raw),raw['FK_Movimiento']))
    effective = [raw for raw in ordered if not _boolean(raw.get('Cancelado'))
                 or historical_waste_effect(raw).status == 'VALID']
    before = [raw for raw in effective if point_stock_history_instant(raw) < cycle.started_at]
    after = [raw for raw in effective if point_stock_history_instant(raw) > cycle.finished_at]
    # A late/cached response must not lend an inconsistent balance to the snapshot.
    if (before and Decimal(str(before[-1]['Existencia_nueva'])) != snapshot.stock
            or after and Decimal(str(after[0]['Existencia_anterior'])) != snapshot.stock):
        return _pendiente('El historial no confirma el saldo de la ventana del snapshot.',reintentar=True,consultar_historial=True)
    try:
        physical_delta, physical_ids = _intervalo(rows,reading.contado_en,cierre,fisico=True)
        point_delta, point_ids = _intervalo(rows,snapshot.captured_at,cierre,fisico=False)
    except ValueError as exc:
        return _pendiente(str(exc))
    physical = reading.cantidad + physical_delta
    if physical < 0:
        return _pendiente('La proyección física resulta negativa; revisar conteo y movimientos.')
    point = snapshot.stock + point_delta
    return {
        'estado':'CALCULADA', 'motivo':'', 'reintentar':False,
        'fisico_al_cierre':str(physical), 'point_al_cierre':str(point),
        'diferencia':str(physical-point),
        'movimientos_fisico':physical_ids, 'movimientos_point':point_ids,
        'posteriores_cierre':[raw['FK_Movimiento'] for raw in ordered
            if cierre <= point_stock_history_instant(raw) < upper],
        'contado_en':reading.contado_en.isoformat(), 'cierre_en':cierre.isoformat(),
        'snapshot_id':snapshot.pk, 'sync_job_id':cycle.pk,
        'capturado_point_en':snapshot.captured_at.isoformat(),
        'historial_id':evidence['import_id'], 'historial_sha256':evidence['raw_sha256'],
        'historial_recibido_en':evidence['retrieved_at'],
    }
