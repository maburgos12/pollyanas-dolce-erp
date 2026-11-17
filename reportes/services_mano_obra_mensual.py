"""Reparto mensual sobre producción; no escribe nómina, inventario ni resultados."""
import calendar
import hashlib
import json
import unicodedata
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_DOWN

from django.core.exceptions import ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, transaction
from django.db.models import F, OuterRef, Q, Subquery
from django.db.models.functions import JSONObject, TruncDate
from django.utils import timezone

from core.access import ROLE_ADMIN, ROLE_DG, can_view_reportes, has_any_role
from core.models import AuditLog
from pos_bridge.models import PointProductionLine
from recetas.models import RecetaCodigoPointAlias, RecetaCostoSemanal, RecetaCostoVersion
from reportes.models import CierreManoObraMensual, RecetaCostoHistoricoMensual, LineaPresupuestoMensual
from reportes.services_costeo_logistica import repartir_centavos
from rrhh.models import NominaLinea, NominaPeriodo
from ventas.services.sales_read_service import get_point_sales_recipe_totals

COMPONENTES = {
    'elaboracion': 'Elaboración y apoyo',
    'supervision': 'Supervisión y control',
    'despacho': 'Coordinación de despacho',
}
CENTAVO = Decimal('0.01')
SERVICIOS = ('Agua potable', 'Energía eléctrica', 'Gas')


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
    historica = persona.get('snapshot_origen') == 'HISTORICA'
    puesto = _texto(persona.get('puesto_operativo_snapshot') if historica else persona['empleado__puesto_operativo'])
    cargo = '' if historica else _texto(persona['empleado__puesto'])
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
    produccion = PointProductionLine.objects.filter(production_date__range=(inicio, fin))
    aliases = RecetaCodigoPointAlias.objects.filter(
        activo=True, codigo_point__in=produccion.filter(receta__isnull=True).values('item_code'),
    )
    costos = RecetaCostoHistoricoMensual.objects.filter(periodo=inicio)
    version_coincidente = RecetaCostoVersion.objects.filter(
        receta_id=OuterRef('receta_id'), version_num=OuterRef('version_receta'),
        costo_mp=OuterRef('costo_mp'), costo_total=OuterRef('costo_total'),
        costo_mo=0, costo_indirecto=0,
    ).order_by().annotate(evidencia=JSONObject(
        id='id', fecha=TruncDate('creado_en', tzinfo=timezone.get_default_timezone()),
        cantidad='rendimiento_cantidad',
        unidad='rendimiento_unidad', lote='lote_referencia', hash='hash_snapshot',
    )).values('evidencia')
    semanales = RecetaCostoSemanal.objects.filter(
        Q(receta_id__in=produccion.values('receta_id')) | Q(receta_id__in=aliases.values('receta_id')),
        scope_type=RecetaCostoSemanal.SCOPE_RECIPE, week_start__lte=fin, week_end__gte=inicio,
    ).exclude(receta_id__in=costos.values('receta_id')).annotate(version=Subquery(version_coincidente))
    queries = {
        # El real consolidado ya contiene el reparto entre Matriz y Producción.
        'servicios': LineaPresupuestoMensual.objects.filter(
            periodo=inicio, version='ORIGINAL', rubro__activo=True,
            rubro__area__codigo='produccion', rubro__sucursal__isnull=True,
            rubro__concepto__in=SERVICIOS).values(
                'id', 'rubro_id', 'rubro__concepto', 'monto_real', 'fuente_real', 'metadata'),
        'periodos': periodos.values('id', 'fecha_inicio', 'fecha_fin', 'estatus'),
        'nomina': NominaLinea.objects.filter(periodo_id__in=periodos.values('id'), departamento_snapshot='PRODUCCION').values(
            'id', 'periodo_id', 'empleado_id', 'total_percepciones', 'departamento_snapshot',
            'snapshot_origen', 'puesto_operativo_snapshot',
            'empleado__nombre', 'empleado__puesto', 'empleado__puesto_operativo'),
        'sin_departamento': NominaLinea.objects.filter(periodo_id__in=periodos.values('id'), departamento_snapshot='').values('id'),
        'produccion': produccion.values(
            'id', 'branch_id', 'production_external_id', 'detail_external_id', 'production_date',
            'receta_id', 'insumo_id', 'item_code', 'item_name', 'unit', 'produced_quantity', 'is_insumo',
            'receta__nombre', 'receta__tipo', 'receta__codigo_point', 'receta__rendimiento_cantidad',
            'receta__rendimiento_unidad__codigo', 'requested_quantity',
            'raw_payload__detail__IsInsumo', 'raw_payload__detail__Cantidad_producida'),
        'aliases_receta': aliases.values(
            'id', 'codigo_point', 'receta_id', 'receta__nombre', 'receta__tipo', 'receta__codigo_point',
            'receta__rendimiento_cantidad', 'receta__rendimiento_unidad__codigo'),
        'costos': costos.values(
            'id', 'receta_id', 'costo_total', 'costo_por_unidad_rendimiento', 'coverage_pct', 'metadata'),
        'costos_semanales': semanales.values(
            'id', 'receta_id', 'identity_key', 'week_start', 'week_end', 'version_receta',
            'costo_mp', 'costo_mo', 'costo_indirecto', 'costo_total', 'metadata', 'version'),
    }
    parts, params, aliases = [], [], {}
    for name, qs in queries.items():
        aliases[name] = {f'f{i}': field for i, field in enumerate((*qs.query.values_select, *qs.query.annotation_select))}
        sql, values = qs.order_by('id').values(**{alias: F(field) for alias, field in aliases[name].items()}).query.sql_with_params()
        parts.append("%s, (SELECT COALESCE(jsonb_agg(to_jsonb(r)), '[]'::jsonb) FROM (" + sql + ') r)')
        params.extend([name, *values])
    with connection.cursor() as cursor:
        cursor.execute('SELECT jsonb_build_object(' + ','.join(parts) + ')::text', params)
        result = json.loads(cursor.fetchone()[0], parse_float=Decimal)
        return {name: [{aliases[name][key]: value for key, value in row.items()} for row in rows] for name, rows in result.items()}


def repartir_servicios(rows, fuentes, pendientes):
    servicios = []
    for nombre in SERVICIOS:
        candidatos = [s for s in fuentes.get('servicios', []) if s['rubro__concepto'] == nombre]
        s = candidatos[0] if len(candidatos) == 1 else None
        monto = Decimal(str(s['monto_real'])) if s and s['monto_real'] is not None else None
        if monto is None or monto < 0 or s['fuente_real'] != 'AUTO:GASTO_OPERATIVO':
            pendientes.append(f'{nombre}: falta un real único consolidado desde gastos de Producción.')
            monto = None
        servicios.append({'nombre': nombre, 'monto': monto, 'linea_id': s['id'] if s else None})
    completo = all(s['monto'] is not None for s in servicios)
    fondo = sum((s['monto'] for s in servicios), Decimal('0')) if completo else None
    base = sum((r['base'] or Decimal('0') for r in rows), Decimal('0'))
    repartible = completo and bool(rows) and base > 0 and all(
        not r['errores'] and r['mp'] is not None and r['mp'] > 0 and r['cantidad'] >= 0 for r in rows)
    for r in rows:
        r.update(servicios=None, servicios_asignados=None, servicios_detalle=[],
                 fabricacion_servicios=None, con_despacho_servicios=None)
    if repartible:
        positivos = [r for r in rows if r['cantidad'] > 0]
        for s in servicios:
            cuotas = {r['id']: s['monto'] * r['base'] / base for r in positivos}
            importes = {key: cuota.quantize(CENTAVO, rounding=ROUND_DOWN) for key, cuota in cuotas.items()}
            # Mayores restos: conservar cada centavo sin cargar el ajuste a un solo producto.
            centavos = int((s['monto'] - sum(importes.values())) / CENTAVO)
            orden = sorted(cuotas, key=lambda key: (-(cuotas[key] - importes[key]), key))
            for key in orden[:centavos]:
                importes[key] += CENTAVO
            for r in positivos:
                monto = importes[r['id']]
                r['servicios_detalle'].append({'nombre': s['nombre'], 'monto': monto, 'unitario': monto / r['cantidad']})
        for r in positivos:
            r['servicios_asignados'] = sum((s['monto'] for s in r['servicios_detalle']), Decimal('0'))
            r['servicios'] = r['servicios_asignados'] / r['cantidad']
            if r['fabricacion'] is not None:
                r['fabricacion_servicios'] = r['fabricacion'] + r['servicios']
                r['con_despacho_servicios'] = r['con_despacho'] + r['servicios']
    asignado = sum((r['servicios_asignados'] or Decimal('0') for r in rows), Decimal('0'))
    return {'fondos': servicios, 'fondo': fondo, 'asignado': asignado,
            'pendiente': fondo - asignado if fondo is not None else None, 'completo': completo}


def costo_semanal_por_produccion(receta_id, cantidades, snapshots):
    """Valida una única semana por fecha y devuelve su base de MP trazable."""
    evidencia = []
    for fecha, cantidad in sorted(cantidades.items()):
        candidatos = [s for s in snapshots if str(s['week_start']) <= fecha <= str(s['week_end'])]
        if len(candidatos) != 1:
            return []
        snapshot = candidatos[0]
        version = snapshot['version']
        costo = Decimal(str(snapshot['costo_mp']))
        metadata = snapshot['metadata']
        if (
            snapshot['identity_key'] != f'RECIPE:{receta_id}'
            or not isinstance(metadata, dict) or metadata.get('cost_source') != 'VERSION_COSTEO'
            or not version or str(version['fecha']) > fecha
            or costo <= 0 or costo != Decimal(str(snapshot['costo_total']))
            or snapshot['costo_mo'] != 0 or snapshot['costo_indirecto'] != 0
        ):
            return []
        rendimiento = version['cantidad']
        unidad = (version['unidad'] or '').upper()
        if rendimiento is not None:
            rendimiento = Decimal(str(rendimiento))
            if rendimiento <= 0 or (unidad not in {'PZA', 'U'} and (unidad or rendimiento != 1)):
                return []
            costo /= rendimiento
        elif Decimal(str(version['lote'])) != 1 or (unidad and unidad not in {'PZA', 'U'}):
            return []
        evidencia.append({
            'fecha': fecha, 'cantidad': cantidad, 'costo_mp_unitario': costo,
            'semana_id': snapshot['id'], 'version_id': version['id'],
        })
    return evidencia


def insumo_sin_produccion(linea):
    cero_point = linea.get('raw_payload__detail__Cantidad_producida')
    return (
        linea['receta_id'] is None and linea['insumo_id'] is not None
        and linea['is_insumo'] and linea.get('raw_payload__detail__IsInsumo') is True
        and linea['produced_quantity'] == 0
        and type(cero_point) in (int, Decimal) and cero_point == 0
    )


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
            'puesto': (n.get('puesto_operativo_snapshot') or 'Sin puesto histórico') if n.get('snapshot_origen') == 'HISTORICA'
                else n['empleado__puesto'] or n['empleado__puesto_operativo'],
            'componente': COMPONENTES.get(key, 'Pendiente'), 'monto': Decimal('0')})
        p['monto'] += monto
    if not personas:
        pendientes.append('No hay personal de Producción en la nómina del mes.')
    costos = {c['receta_id']: c for c in fuentes['costos']}
    semanales = defaultdict(list)
    for snapshot in fuentes.get('costos_semanales', []):
        semanales[snapshot['receta_id']].append(snapshot)
    cantidades_por_fecha = defaultdict(lambda: defaultdict(Decimal))
    insumos_cero = []
    productos, claves = {}, set()
    internas, sin_vinculo = 0, 0
    aliases = {a['codigo_point']: a for a in fuentes['aliases_receta']}
    resueltas_alias = 0
    for p in fuentes['produccion']:
        clave = (p['branch_id'], p['production_external_id'], p['detail_external_id'])
        if clave in claves:
            pendientes.append('Hay claves de producción duplicadas.')
            continue
        claves.add(clave)
        if insumo_sin_produccion(p):
            insumos_cero.append(p)
            continue
        if not p['receta_id'] and p['item_code'] in aliases:
            alias = aliases[p['item_code']]
            p = {**p, **{k: v for k, v in alias.items() if k == 'receta_id' or k.startswith('receta__')}}
            resueltas_alias += 1
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
        if cantidad > 0:
            cantidades_por_fecha[r['id']][str(p['production_date'])] += cantidad
        if cantidad < 0 or p['is_insumo'] or p['unit'].upper() not in {'PZA', 'U'} or p['item_code'] != r['codigo']:
            r['errores'].append('Revisar identidad, unidad o cantidad en Point.')
        rendimiento = p['receta__rendimiento_cantidad']
        unidad = p['receta__rendimiento_unidad__codigo']
        if rendimiento is not None and Decimal(str(rendimiento)) <= 0:
            r['errores'].append('El rendimiento explícito debe ser positivo.')
        elif (unidad and unidad.upper() not in {'PZA', 'U'}) or (rendimiento is not None and Decimal(str(rendimiento)) != 1 and not unidad):
            r['errores'].append('Conciliar unidad de rendimiento con Point.')
    if sin_vinculo:
        pendientes.append(f'{sin_vinculo} líneas de producción sin receta vinculada.')
    rows = []
    for r in sorted(productos.values(), key=lambda x: x['nombre']):
        c = costos.get(r['id'])
        r.update(mp=None, base=None, semanas_mp=[])
        if c and Decimal(str(c['coverage_pct'])) == 100:
            value = c['costo_por_unidad_rendimiento']
            if not value:
                rendimiento = Decimal(str(r['rendimiento'] or 0))
                value = Decimal(str(c['costo_total'])) / rendimiento if rendimiento > 0 else c['costo_total']
            r['mp'] = Decimal(str(value))
            if r['mp'] <= 0:
                r['errores'].append('Costo mensual no positivo.')
        elif c is None and not r['errores'] and r['cantidad'] > 0:
            r['semanas_mp'] = costo_semanal_por_produccion(r['id'], cantidades_por_fecha[r['id']], semanales[r['id']])
            if r['semanas_mp']:
                r['base'] = sum(s['cantidad'] * s['costo_mp_unitario'] for s in r['semanas_mp'])
                r['mp'] = r['base'] / r['cantidad']
            else:
                r['errores'].append('Costo mensual ausente y costo semanal sin respaldo para toda la producción.')
        else:
            r['errores'].append('Costo mensual ausente o incompleto.')
        r['errores'] = sorted(set(r['errores']))
        if r['base'] is None and r['mp'] is not None:
            r['base'] = r['cantidad'] * r['mp']
        if r['errores']:
            pendientes.append(f"{r['nombre']}: {' '.join(r['errores'])}")
        rows.append(r)
    base = sum((r['base'] or Decimal('0') for r in rows), Decimal('0'))
    # Un costo ausente no debe cargar su cuota a los productos que sí tienen costo.
    repartible = bool(personas) and fondo > 0 and pendiente_nomina == 0 and bool(rows) and base > 0 and all(r['mp'] is not None and r['mp'] > 0 and r['cantidad'] >= 0 for r in rows)
    if any(not v.is_finite() or v < 0 or v != v.quantize(CENTAVO) for v in fondos.values()):
        pendientes.append('Los fondos por componente deben ser no negativos y expresarse en centavos.')
        repartible = False
    if not repartible:
        pendientes.append('No se puede repartir hasta completar la base de todos los productos.')
    bases = {r['id']: r['base'] for r in rows if r['cantidad'] > 0}
    cuotas = {k: repartir_centavos(monto, bases) for k, monto in fondos.items()} if repartible else {}
    asignado = Decimal('0')
    for r in rows:
        r.update(mo=None, fabricacion=None, con_despacho=None, asignado=None, componentes=[])
        if repartible and r['cantidad'] > 0:
            importes = {k: por_producto[r['id']] for k, por_producto in cuotas.items()}
            r['asignado'] = sum(importes.values(), Decimal('0'))
            r['mo'] = r['asignado'] / r['cantidad']
            r['fabricacion'] = r['mp'] + (importes['elaboracion'] + importes['supervision']) / r['cantidad']
            r['con_despacho'] = r['mp'] + r['mo']
            r['componentes'] = [{'nombre': COMPONENTES[k], 'unitario': v / r['cantidad'], 'monto': v} for k, v in importes.items()]
            asignado += r['asignado']
    notas.append('Las recetas de producto terminado sin rendimiento explícito corresponden a una pieza, según confirmación de Dirección del 14/10/2026; no representan rendimiento por batida ni productividad diaria.')
    if resueltas_alias:
        notas.append(f'{resueltas_alias} líneas históricas reconocidas mediante alias de receta ya registrados en el ERP; sin modificar la producción original.')
    notas.append('Puestos actuales contrastados con nómina histórica; confirmar cobertura y clasificación al aprobar.')
    notas.append('Reparto por materia prima. No mide tiempos, cargas patronales completas, inventario intermedio ni costo de ventas.')
    notas.append('El costo mensual puede usar estructura actual de receta; verificar su vigencia en el mes.')
    notas.append('La mano de obra se reparte en centavos por componente, por mayores restos.')
    servicios = repartir_servicios(rows, fuentes, pendientes)
    notas.append('Servicios: agua potable, energía eléctrica y gas del real consolidado de Producción (versión Original). Ya incluyen el reparto entre centros; no se vuelve a aplicar 65%. No se suma agua purificada ni se duplican facturas, pagos o gastos.')
    notas.append('Servicios por participación en materia prima, con centavos distribuidos por mayores restos. Se conservan los importes registrados, incluidos impuestos cuando formen parte del gasto; no es costo neto fiscal ni medición de consumo por producto. No incluye renta ni otros indirectos.')
    fingerprint = hashlib.sha256(json.dumps({'version': 4, 'periodo': periodo, 'fuentes': fuentes}, cls=DjangoJSONEncoder, sort_keys=True).encode()).hexdigest()
    return {'periodo': periodo, 'fingerprint': fingerprint, 'fondo': fondo, 'fondos': [{'nombre': COMPONENTES[k], 'monto': v} for k, v in fondos.items()],
        'personas': list(personas.values()), 'productos': rows, 'unidades': sum((r['cantidad'] for r in rows), Decimal('0')),
        'base': base, 'asignado': asignado, 'pendiente_nomina': pendiente_nomina,
        'redondeo': sum(fondos.values()) - asignado if repartible else None,
        'pendiente_reparto': Decimal('0') if repartible else sum(fondos.values()),
        'pendientes': sorted(set(pendientes)), 'notas': notas, 'lineas_produccion': len(fuentes['produccion']),
        'preparaciones': internas, 'sin_vinculo': sin_vinculo, 'insumos_cero': insumos_cero, 'fuentes': fuentes, 'servicios': servicios}


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
    ventas = {r['receta_id']: r for r in get_point_sales_recipe_totals(start_date=inicio, end_date=fin)}
    filas = []
    for p in cierre.resultado['productos']:
        venta = ventas.get(p['id'])
        precio = venta['importe'] / venta['cantidad'] if venta and venta['cantidad'] > 0 else None
        # Los cierres anteriores conservan su alcance y sus importes originales.
        valor = p.get('con_despacho_servicios', p['con_despacho'])
        costo = Decimal(valor) if valor is not None else None
        filas.append({**p, 'fabricacion': p.get('fabricacion_servicios', p['fabricacion']),
            'costo_comparado': costo, 'precio': precio, 'margen': precio - costo if precio is not None and costo is not None else None})
    return {'cierre': cierre, 'filas': filas, 'periodo': periodo}
