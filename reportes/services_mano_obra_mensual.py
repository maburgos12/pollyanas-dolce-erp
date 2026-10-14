"""Reparto mensual sobre producción; no escribe nómina, inventario ni resultados."""
import calendar
import hashlib
import json
import unicodedata
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.core.exceptions import ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, transaction
from django.db.models import F
from django.utils import timezone

from core.access import ROLE_ADMIN, ROLE_DG, can_view_reportes, has_any_role
from core.models import AuditLog
from pos_bridge.models import PointProductionLine, PointDailySale
from reportes.models import CierreManoObraMensual, RecetaCostoHistoricoMensual
from rrhh.models import NominaLinea, NominaPeriodo

COMPONENTES = {
    'elaboracion': 'Elaboración y apoyo',
    'supervision': 'Supervisión y control',
    'despacho': 'Coordinación de despacho',
}
CENTAVO = Decimal('0.01')


def puede_revisar(user):
    return can_view_reportes(user) and has_any_role(user, ROLE_DG, ROLE_ADMIN)


def limites(periodo):
    try:
        inicio = date.fromisoformat(f'{periodo}-01')
        if inicio.strftime('%Y-%m') != periodo or inicio.year < 2000:
            raise ValueError
    except (TypeError, ValueError):
        raise ValidationError('Selecciona un mes válido (AAAA-MM).')
    return inicio, inicio.replace(day=calendar.monthrange(inicio.year, inicio.month)[1])


def _texto(value):
    return ''.join(c for c in unicodedata.normalize('NFKD', value or '') if not unicodedata.combining(c)).strip().upper()


def componente(persona):
    puesto = _texto(persona['empleado__puesto_operativo'])
    cargo = _texto(persona['empleado__puesto'])
    if puesto == 'ENVIO_SUCURSAL':
        return 'despacho'
    if puesto in {'HORNOS', 'ARMADO', 'EMBETUNADO', 'PREPARACION', 'CUARTOS_FRIOS'}:
        return 'elaboracion'
    if cargo in {'JEFE DE PRODUCCION', 'ENCARGADA DE PRODUCCION', 'ENCARGADO DE PRODUCCION', 'ENCARGADO DE CONTROL DE PRODUCCION', 'ENCARGADA DE CONTROL DE PRODUCCION'}:
        return 'supervision'
    return None


def leer_fuentes(periodo):
    """Un SELECT para que nómina, cantidades y costos compartan snapshot MVCC."""
    inicio, fin = limites(periodo)
    periodos = NominaPeriodo.objects.filter(fecha_fin__range=(inicio, fin))
    queries = {
        'periodos': periodos.values('id', 'fecha_inicio', 'fecha_fin', 'estatus'),
        'nomina': NominaLinea.objects.filter(periodo_id__in=periodos.values('id'), departamento_snapshot='PRODUCCION').values(
            'id', 'periodo_id', 'empleado_id', 'total_percepciones', 'departamento_snapshot',
            'empleado__nombre', 'empleado__puesto', 'empleado__puesto_operativo'),
        'sin_departamento': NominaLinea.objects.filter(periodo_id__in=periodos.values('id'), departamento_snapshot='').values('id'),
        'produccion': PointProductionLine.objects.filter(production_date__range=(inicio, fin)).values(
            'id', 'branch_id', 'production_external_id', 'detail_external_id', 'production_date',
            'receta_id', 'insumo_id', 'item_code', 'item_name', 'unit', 'produced_quantity', 'is_insumo',
            'receta__nombre', 'receta__tipo', 'receta__codigo_point', 'receta__rendimiento_cantidad',
            'receta__rendimiento_unidad__codigo'),
        'costos': RecetaCostoHistoricoMensual.objects.filter(periodo=inicio).values(
            'id', 'receta_id', 'costo_total', 'costo_por_unidad_rendimiento', 'coverage_pct', 'metadata'),
    }
    parts, params, aliases = [], [], {}
    for name, qs in queries.items():
        aliases[name] = {f'f{i}': field for i, field in enumerate(qs.query.values_select)}
        sql, values = qs.order_by('id').values(**{alias: F(field) for alias, field in aliases[name].items()}).query.sql_with_params()
        parts.append("%s, (SELECT COALESCE(jsonb_agg(to_jsonb(r)), '[]'::jsonb) FROM (" + sql + ') r)')
        params.extend([name, *values])
    with connection.cursor() as cursor:
        cursor.execute('SELECT jsonb_build_object(' + ','.join(parts) + ')::text', params)
        result = json.loads(cursor.fetchone()[0], parse_float=Decimal)
        return {name: [{aliases[name][key]: value for key, value in row.items()} for row in rows] for name, rows in result.items()}


def calcular(periodo, fuentes=None):
    inicio, fin = limites(periodo)
    fuentes = leer_fuentes(periodo) if fuentes is None else fuentes
    pendientes, notas = [], []
    fondos = {key: Decimal('0') for key in COMPONENTES}
    personas = {}
    validos = {p['id'] for p in fuentes['periodos'] if p['estatus'] in {'PAGADA', 'CERRADA'}}
    if not validos:
        pendientes.append('No hay nómina cerrada o pagada en el mes.')
    if any(p['id'] not in validos for p in fuentes['periodos']):
        pendientes.append('Hay periodos de nómina sin cerrar.')
    dias = set()
    for p in fuentes['periodos']:
        if p['id'] not in validos:
            continue
        a, b = date.fromisoformat(str(p['fecha_inicio'])), date.fromisoformat(str(p['fecha_fin']))
        if a < inicio or b > fin:
            pendientes.append('Una nómina cruza el límite del mes; requiere conciliación.')
        dias.update(a + timedelta(days=i) for i in range((b - a).days + 1))
    if any(inicio + timedelta(days=i) not in dias for i in range(fin.day)):
        pendientes.append('La nómina no cubre todos los días del mes.')
    if fin >= timezone.localdate():
        pendientes.append('El mes todavía no ha terminado.')
    if fuentes['sin_departamento']:
        pendientes.append('Existen líneas de nómina sin departamento histórico.')
    fondo, pendiente_nomina = Decimal('0'), Decimal('0')
    for n in fuentes['nomina']:
        if n['periodo_id'] not in validos:
            continue
        monto = Decimal(str(n['total_percepciones']))
        key = componente(n)
        fondo += monto
        if monto < 0:
            pendientes.append('Hay percepciones negativas; revisar nómina.')
        if key:
            fondos[key] += monto
        else:
            pendiente_nomina += monto
            pendientes.append(f"Clasificar el puesto de {n['empleado__nombre']}.")
        p = personas.setdefault(n['empleado_id'], {'id': n['empleado_id'], 'nombre': n['empleado__nombre'],
            'puesto': n['empleado__puesto'] or n['empleado__puesto_operativo'],
            'componente': COMPONENTES.get(key, 'Pendiente'), 'monto': Decimal('0')})
        p['monto'] += monto
    if not personas:
        pendientes.append('No hay personal de Producción en la nómina del mes.')
    costos = {c['receta_id']: c for c in fuentes['costos']}
    productos, claves = {}, set()
    internas, sin_vinculo = 0, 0
    for p in fuentes['produccion']:
        clave = (p['branch_id'], p['production_external_id'], p['detail_external_id'])
        if clave in claves:
            pendientes.append('Hay claves de producción duplicadas.')
            continue
        claves.add(clave)
        if not p['receta_id']:
            sin_vinculo += 1
            continue
        if p['receta__tipo'] != 'PRODUCTO_FINAL':
            internas += 1
            continue
        r = productos.setdefault(p['receta_id'], {'id': p['receta_id'], 'nombre': p['receta__nombre'],
            'codigo': p['receta__codigo_point'], 'cantidad': Decimal('0'), 'errores': [],
            'rendimiento': p['receta__rendimiento_cantidad']})
        cantidad = Decimal(str(p['produced_quantity']))
        r['cantidad'] += cantidad
        if cantidad < 0 or p['is_insumo'] or p['unit'].upper() not in {'PZA', 'U'} or p['item_code'] != r['codigo']:
            r['errores'].append('Revisar identidad, unidad o cantidad en Point.')
        if not p['receta__rendimiento_cantidad'] or Decimal(str(p['receta__rendimiento_cantidad'])) <= 0 or not p['receta__rendimiento_unidad__codigo']:
            r['errores'].append('Confirmar rendimiento de receta.')
        elif p['receta__rendimiento_unidad__codigo'].upper() not in {'PZA', 'U'}:
            r['errores'].append('Conciliar unidad de rendimiento con Point.')
    if sin_vinculo:
        pendientes.append(f'{sin_vinculo} líneas de producción sin receta vinculada.')
    rows = []
    for r in sorted(productos.values(), key=lambda x: x['nombre']):
        c = costos.get(r['id'])
        r['mp'] = None
        if c and Decimal(str(c['coverage_pct'])) == 100:
            value = c['costo_por_unidad_rendimiento']
            if not value:
                rendimiento = Decimal(str(r['rendimiento'] or 0))
                value = Decimal(str(c['costo_total'])) / rendimiento if rendimiento > 0 else c['costo_total']
            r['mp'] = Decimal(str(value))
            if r['mp'] <= 0:
                r['errores'].append('Costo mensual no positivo.')
        else:
            r['errores'].append('Costo mensual ausente o incompleto.')
        r['errores'] = sorted(set(r['errores']))
        r['base'] = r['cantidad'] * r['mp'] if r['mp'] is not None else None
        if r['errores']:
            pendientes.append(f"{r['nombre']}: {' '.join(r['errores'])}")
        rows.append(r)
    base = sum((r['base'] or Decimal('0') for r in rows), Decimal('0'))
    # Un costo ausente no debe cargar su cuota a los productos que sí tienen costo.
    repartible = bool(rows) and base > 0 and all(r['mp'] is not None and r['mp'] > 0 and r['cantidad'] >= 0 for r in rows)
    if not repartible:
        pendientes.append('No se puede repartir hasta completar la base de todos los productos.')
    asignado = Decimal('0')
    for r in rows:
        r.update(mo=None, fabricacion=None, con_despacho=None, asignado=None, componentes=[])
        if repartible and r['cantidad'] > 0:
            importes = {k: (fondos[k] * r['base'] / base).quantize(CENTAVO, rounding=ROUND_HALF_UP) for k in fondos}
            r['asignado'] = sum(importes.values(), Decimal('0'))
            r['mo'] = r['asignado'] / r['cantidad']
            r['fabricacion'] = r['mp'] + (importes['elaboracion'] + importes['supervision']) / r['cantidad']
            r['con_despacho'] = r['mp'] + r['mo']
            r['componentes'] = [{'nombre': COMPONENTES[k], 'unitario': v / r['cantidad'], 'monto': v} for k, v in importes.items()]
            asignado += r['asignado']
    notas.append('Puestos actuales contrastados con nómina histórica; confirmar cobertura y clasificación al aprobar.')
    notas.append('Reparto por materia prima. No mide tiempos, cargas patronales completas, inventario intermedio ni costo de ventas.')
    notas.append('El costo mensual puede usar estructura actual de receta; verificar su vigencia en el mes.')
    fingerprint = hashlib.sha256(json.dumps({'version': 1, 'periodo': periodo, 'fuentes': fuentes}, cls=DjangoJSONEncoder, sort_keys=True).encode()).hexdigest()
    return {'periodo': periodo, 'fingerprint': fingerprint, 'fondo': fondo, 'fondos': [{'nombre': COMPONENTES[k], 'monto': v} for k, v in fondos.items()],
        'personas': list(personas.values()), 'productos': rows, 'unidades': sum((r['cantidad'] for r in rows), Decimal('0')),
        'base': base, 'asignado': asignado, 'pendiente_nomina': pendiente_nomina,
        'redondeo': sum(fondos.values()) - asignado if repartible else None,
        'pendiente_reparto': Decimal('0') if repartible else sum(fondos.values()),
        'pendientes': sorted(set(pendientes)), 'notas': notas, 'lineas_produccion': len(fuentes['produccion']),
        'preparaciones': internas, 'sin_vinculo': sin_vinculo, 'fuentes': fuentes}


@transaction.atomic
def aprobar(periodo, fingerprint, user):
    if not puede_revisar(user):
        from django.core.exceptions import PermissionDenied
        raise PermissionDenied
    inicio, _ = limites(periodo)
    with connection.cursor() as cursor:
        cursor.execute('SELECT pg_advisory_xact_lock(%s, %s)', [7414, inicio.year * 100 + inicio.month])
    actual = calcular(periodo)
    if actual['fingerprint'] != fingerprint:
        raise ValidationError('Las fuentes cambiaron. Revisa la simulación actualizada antes de aprobar.')
    if actual['pendientes']:
        raise ValidationError('Resuelve los pendientes antes de aprobar el cierre.')
    anterior = CierreManoObraMensual.objects.filter(periodo=inicio).order_by('-revision').first()
    if anterior and anterior.fingerprint == fingerprint:
        return anterior
    cierre = CierreManoObraMensual.objects.create(periodo=inicio, revision=anterior.revision + 1 if anterior else 1,
        fingerprint=fingerprint, resultado=json.loads(json.dumps(actual, cls=DjangoJSONEncoder)), aprobado_por=user)
    AuditLog.objects.create(user=user, action='APPROVE', model='reportes.CierreManoObraMensual', object_id=str(cierre.pk),
        payload={'periodo': periodo, 'revision': cierre.revision, 'fingerprint': fingerprint})
    return cierre


def analisis_aprobado(periodo):
    inicio, fin = limites(periodo)
    cierre = CierreManoObraMensual.objects.filter(periodo=inicio).order_by('-revision').first()
    if not cierre:
        return {'cierre': None, 'filas': [], 'periodo': periodo}
    from django.db.models import Sum
    ventas = {r['receta_id']: r for r in PointDailySale.objects.filter(sale_date__range=(inicio, fin), receta_id__isnull=False)
        .values('receta_id').annotate(cantidad=Sum('quantity'), importe=Sum('total_amount'))}
    filas = []
    for p in cierre.resultado['productos']:
        venta = ventas.get(p['id'])
        precio = venta['importe'] / venta['cantidad'] if venta and venta['cantidad'] > 0 else None
        costo = Decimal(p['con_despacho']) if p['con_despacho'] is not None else None
        filas.append({**p, 'precio': precio, 'margen': precio - costo if precio is not None and costo is not None else None})
    return {'cierre': cierre, 'filas': filas, 'periodo': periodo}
