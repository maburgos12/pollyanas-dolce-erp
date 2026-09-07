"""Referencias Point pasivas: jamás sincroniza, crea maestros o ajusta saldos."""
from decimal import Decimal, InvalidOperation
import re

from django.utils import timezone
from unidecode import unidecode
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointInsumoInventorySnapshot, PointSyncJob


def _normal(value):
    return unidecode(str(value or '')).strip().lower()


def misma_unidad(first, second):
    aliases = {'pieza': 'pza', 'piezas': 'pza', 'pz': 'pza', 'pzas': 'pza',
               'kilogramo': 'kg', 'kilogramos': 'kg', 'kilo': 'kg',
               'gramo': 'g', 'gramos': 'g', 'litro': 'l', 'litros': 'l', 'lt': 'l'}
    a, b = _normal(first), _normal(second)
    return bool(a and b and aliases.get(a, a) == aliases.get(b, b))


def leer_fila_point(raw, *, codigo):
    """Admite columnas visibles y el formato Point con PK/categoría ocultas.

    Se exige evidencia cruda, identidad y unidad. No usa el parser que convierte
    datos ausentes en cero. Los negativos Point son referencias válidas, no físicos.
    """
    if not isinstance(raw, dict):
        return None
    headers, values = raw.get('headers'), raw.get('row')
    if not isinstance(headers, list) or not isinstance(values, list):
        return None
    headings = [_normal(h) for h in headers]
    if headings[:3] == ['codigo', 'producto', 'cantidad'] and len(values) == len(headers) + 2 and len(values) >= 6:
        code, amount, unit = values[1], values[4], values[5]
    elif len(values) == len(headers) and all(key in headings for key in ['codigo', 'cantidad', 'unidad']):
        code, amount, unit = (values[headings.index(key)] for key in ['codigo', 'cantidad', 'unidad'])
    else:
        return None
    if str(code).strip() != str(codigo).strip() or not str(unit or '').strip():
        return None
    number = str(amount if amount is not None else '').strip()
    if not re.fullmatch(r'-?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?', number):
        return None
    try:
        quantity = Decimal(number.replace(',', ''))
    except InvalidOperation:
        return None
    if not quantity.is_finite():
        return None
    return quantity, str(unit).strip()


def _cycle(branch, cutoff, *, insumos=False):
    model = PointInsumoInventorySnapshot if insumos else PointInventorySnapshot
    snapshot = model.objects.filter(
        branch=branch, captured_at__lte=cutoff,
        sync_job__status=PointSyncJob.STATUS_SUCCESS,
        sync_job__job_type=PointSyncJob.JOB_TYPE_INVENTORY,
        sync_job__finished_at__lte=cutoff,
    ).select_related('sync_job').order_by('-captured_at', '-id').first()
    return snapshot.sync_job if snapshot else None


def referencia_conteo(conteo, *, now=None):
    now = now or timezone.now()
    cutoff = conteo.iniciado_en or conteo.creado_en
    result = {'estado': 'SIN_REFERENCIA', 'corte_verificado': False,
              'consultado_en': now.isoformat(), 'corte_conteo': cutoff.isoformat(),
              'advertencias': ['La hora de extracción no certifica un corte físico.'], 'lineas': {}}
    branches = list(PointBranch.objects.filter(erp_branch_id=conteo.sucursal_id, status=PointBranch.STATUS_ACTIVE)[:2])
    if len(branches) != 1:
        result['advertencias'].append('Sucursal Point sin correspondencia única.')
        return result
    branch = branches[0]
    result['sucursal_point_id'] = branch.pk
    lines = list(conteo.lineas.all())
    # A count uses one cycle only. Missing source rows stay partial.
    job = _cycle(branch, cutoff, insumos=not any(line.producto_id for line in lines))
    for insumos in (False, True):
        subset = [line for line in lines if bool(line.insumo_id) == insumos]
        if not subset:
            continue
        if not job:
            for line in subset:
                result['lineas'][str(line.pk)] = {'cantidad': None, 'unidad': '', 'error': 'Sin ciclo Point exitoso anterior al inicio.'}
            continue
        model = PointInsumoInventorySnapshot if insumos else PointInventorySnapshot
        field = 'insumo_id' if insumos else 'product_id'
        ids = [line.insumo_id if insumos else line.producto_id for line in subset]
        rows = list(model.objects.filter(branch=branch, sync_job=job, **{field + '__in': ids}).order_by('id'))
        by_item = {}
        duplicates = set()
        for row in rows:
            item_id = getattr(row, field)
            if item_id in by_item:
                duplicates.add(item_id)
            by_item[item_id] = row
        for line in subset:
            identity = line.insumo_id if insumos else line.producto_id
            row = by_item.get(identity)
            reading = {'cantidad': None, 'unidad': '', 'sync_job_id': job.pk,
                       'inicio_extraccion': job.started_at.isoformat(),
                       'fin_extraccion': job.finished_at.isoformat(), 'error': ''}
            if not row or identity in duplicates or row.captured_at > cutoff:
                reading['error'] = 'Artículo ausente, duplicado o posterior al inicio en el ciclo.'
            else:
                raw = leer_fila_point(row.raw_payload, codigo=line.codigo)
                amount_db = row.point_quantity if insumos else row.stock
                if raw is None or raw[0] != amount_db:
                    reading['error'] = 'Cantidad o unidad sin evidencia Point consistente.'
                elif not misma_unidad(raw[1], line.unidad):
                    reading['error'] = 'La unidad Point no corresponde a la unidad contada.'
                else:
                    reading.update(cantidad=str(raw[0]), unidad=raw[1], snapshot_id=row.pk,
                                   capturado_en=row.captured_at.isoformat())
            result['lineas'][str(line.pk)] = reading
    available = sum(row.get('cantidad') is not None for row in result['lineas'].values())
    result['estado'] = 'REFERENCIA' if available == len(lines) and available else ('PARCIAL' if available else 'SIN_REFERENCIA')
    result['advertencias'].append('Referencia histórica informativa; no modifica saldos ni acredita movimientos durante el conteo.')
    return result
