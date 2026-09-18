"""Official Point counting units, independent of recipe yield and stock."""
from django.utils import timezone

COUNT_UNIT_KEY = 'point_count_unit'
CATALOG_ENDPOINT = '/Catalogos/get_productos'


def catalog_count_unit(row, units=None):
    code = str(row.get('Codigo') or '').strip()
    unit_id = row.get('FK_Unidad')
    label = str(row.get('Unidad') or '').strip()
    if not label and units is not None:
        label = str(units.get(str(unit_id), {}).get('Abreviacion') or '').strip()
    if not code or not label or len(label) > 60:
        return None
    return {'codigo': code, 'unidad': label, 'point_unit_id': unit_id,
            'endpoint': CATALOG_ENDPOINT, 'consultado_en': timezone.now().isoformat()}


def product_count_unit(product):
    evidence = (product.metadata or {}).get(COUNT_UNIT_KEY)
    if not isinstance(evidence, dict) or evidence.get('endpoint') != CATALOG_ENDPOINT:
        return '', ''
    unit = evidence.get('unidad')
    if evidence.get('codigo') != product.sku or not isinstance(unit, str) or not unit.strip() or len(unit) > 60:
        return '', ''
    return unit.strip(), 'Unidad del catálogo oficial Point'
