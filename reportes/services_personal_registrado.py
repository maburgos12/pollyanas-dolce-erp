"""Lectura de listas de raya; conserva importes y adscripción, sin recalcular nóminas."""
from calendar import monthrange
from datetime import timedelta
from decimal import Decimal

from rrhh.models import NominaLinea
from .clasificacion_nomina import destino_de

COLUMNAS_CARGAS = frozenset(('AU', 'AV', 'AW', 'AX', 'AY', 'AZ', 'BB', 'BC', 'BD'))
COLUMNAS_RCV = frozenset(('AV', 'AX', 'BB'))


def leer_personal_registrado(start, end):
    rows = {}
    lines = (NominaLinea.objects.filter(
        periodo__fecha_fin__gte=start, periodo__fecha_fin__lt=end,
        periodo__estatus__in=('CERRADA', 'PAGADA'))
        .select_related('periodo', 'empleado', 'sucursal_snapshot')
        .prefetch_related('conceptos').order_by('periodo_id', 'id'))
    for line in lines:
        period, emp = line.periodo, line.empleado
        month = period.fecha_fin.replace(day=1)
        row = rows.setdefault(month, dict(percepciones=Decimal(0), despensa=Decimal(0),
            total=Decimal(0), periodos={}, grupos={}, pendientes=[], dias=set(),
            componentes_cargas={}, destinos_cargas={}, errores_cargas=[], prestaciones_pagadas=Decimal(0),
            personas_rcv=set(), rcv_nomina=Decimal(0)))
        if period.pk not in row['periodos']:
            row['periodos'][period.pk] = dict(id=period.pk, inicio=period.fecha_inicio,
                fin=period.fecha_fin, estatus=period.estatus)
            if period.fecha_inicio < month:
                row['pendientes'].append(f'Periodo {period.pk} cruza meses; importe por fecha final, sin prorrateo.')
            day = max(month, period.fecha_inicio)
            while day <= period.fecha_fin:
                row['dias'].add(day.day)
                day += timedelta(days=1)
        concepts = [c for c in line.conceptos.all() if c.tipo == 'PERCEPCION']
        row['prestaciones_pagadas'] += sum((c.importe for c in concepts
            if c.codigo_concepto in ('19', '20', '21', '22', '24')), Decimal(0))
        concept_total = sum((c.importe for c in concepts), Decimal(0))
        vouchers = sum((c.importe for c in concepts if c.codigo_concepto == '32'), Decimal(0))
        extra = Decimal(0)
        if concepts and concept_total == line.total_percepciones:
            pass  # Despensa incluida: no se suma otra vez.
        elif concepts and concept_total - vouchers == line.total_percepciones:
            extra = vouchers
        else:
            row['pendientes'].append(f'Línea {line.pk}: percepciones sin conciliación de conceptos.')
        department = line.departamento_snapshot
        historica = line.snapshot_origen == NominaLinea.SNAPSHOT_HISTORICA
        # Las filas legadas sin snapshot conservan su lectura; el histórico nunca toma el puesto actual.
        function = emp._clave_funcion(line.puesto_operativo_snapshot if
            line.puesto_operativo_snapshot is not None or historica else emp.puesto_operativo)
        level = line.nivel_organizacional_snapshot if (
            line.nivel_organizacional_snapshot is not None or historica) else emp.nivel_organizacional
        default = {'VENTAS': {'CAJAS': 'tienda', 'AUXILIAR_CAJAS': 'tienda',
            'CALL_CENTER': 'call_center', 'REPARTIDOR': 'logistica'}.get(function, 'ventas_pendiente'),
            'PRODUCCION': 'produccion', 'ADMINISTRACION': 'administracion',
            'RRHH': 'administracion', 'LOGISTICA': 'logistica',
            'MANTENIMIENTO': 'mantenimiento'}.get(department, 'sin_clasificar')
        destination = destino_de(level, function, predeterminado=default).lower()
        if emp._clave_funcion(level) not in ('JEFATURA', 'DIRECCION'):
            destination = {'LIMPIEZA': 'limpieza', 'ALMACEN': 'almacen'}.get(function, destination)
        if destination in ('sin_clasificar', 'ventas_pendiente'):
            row['pendientes'].append(f'Línea {line.pk}: función pendiente de clasificar en RRHH.')
        key = (line.sucursal_snapshot_id, destination)
        group = row['grupos'].setdefault(key, dict(sucursal_id=line.sucursal_snapshot_id,
            sucursal=line.sucursal_snapshot.nombre if line.sucursal_snapshot else 'Sin sucursal histórica',
            destino=destination, percepciones=Decimal(0), despensa=Decimal(0), total=Decimal(0), personas=set(),
            cargas_sin_isn=Decimal(0), personas_cargas=[]))
        prefijo = f'M{month:%Y%m}:'
        cargas = [c for c in line.conceptos.all() if c.tipo == 'OBLIGACION'
                  and c.codigo_concepto.startswith(prefijo)]
        columnas = [c.codigo_concepto[len(prefijo):] for c in cargas]
        if cargas:
            completas = len(columnas) == len(COLUMNAS_CARGAS) and set(columnas) == COLUMNAS_CARGAS
            solo_rcv = len(columnas) == len(COLUMNAS_RCV) and set(columnas) == COLUMNAS_RCV
            if not (completas or solo_rcv) or emp.pk in row['personas_rcv']:
                row['errores_cargas'].append(f'Línea {line.pk}: cargas mensuales incompletas o duplicadas.')
            else:
                row['personas_rcv'].add(emp.pk)
                row['rcv_nomina'] += sum((c.importe for col, c in zip(columnas, cargas)
                                         if col in COLUMNAS_RCV), Decimal(0))
                if completas:
                    row['destinos_cargas'][emp.pk] = destination
                    group['personas_cargas'].append(emp.pk)
                    for col, cargo in zip(columnas, cargas):
                        row['componentes_cargas'][col] = row['componentes_cargas'].get(col, Decimal(0)) + cargo.importe
                        if col != 'AY':
                            group['cargas_sin_isn'] += cargo.importe
        group['personas'].add(emp.pk)
        for target in (row, group):
            target['percepciones'] += line.total_percepciones
            target['despensa'] += extra
            target['total'] += line.total_percepciones + extra
    for month, row in rows.items():
        row['mes_cubierto'] = len(row.pop('dias')) == monthrange(month.year, month.month)[1]
        if not row['mes_cubierto']:
            row['pendientes'].append('Los periodos registrados no cubren todos los días del mes.')
        row['periodos'] = list(row['periodos'].values())
        row['grupos'] = list(row['grupos'].values())
        sucursales = {}
        departamentos = {}
        personas_mes = set()
        row['otros_costos'] = []
        for group in row['grupos']:
            departamento = departamentos.setdefault(group['destino'], dict(
                destino=group['destino'], personas=set(), percepciones=Decimal(0),
                despensa=Decimal(0), total=Decimal(0), cargas_sin_isn=Decimal(0)))
            departamento['cargas_sin_isn'] += group['cargas_sin_isn']
            departamento['personas'].update(group['personas'])
            personas_mes.update(group['personas'])
            for field in ('percepciones', 'despensa', 'total'):
                departamento[field] += group[field]
            if group['destino'] in ('tienda', 'ventas_pendiente'):
                branch = sucursales.setdefault(group['sucursal_id'], dict(sucursal_id=group['sucursal_id'],
                    sucursal=group['sucursal'], personas=set(), percepciones=Decimal(0),
                    despensa=Decimal(0), total=Decimal(0), puestos_pendientes=0))
                branch['personas'].update(group['personas'])
                if group['destino'] == 'ventas_pendiente':
                    branch['puestos_pendientes'] += len(group['personas'])
                for field in ('percepciones', 'despensa', 'total'):
                    branch[field] += group[field]
            else:
                row['otros_costos'].append(group)
            group['personas'] = len(group['personas'])
            group['destino_label'] = {'tienda': 'Personal de tienda', 'logistica': 'Logística',
                'administracion': 'Administración', 'produccion': 'Producción', 'cedis': 'Cuartos fríos',
                'mantenimiento': 'Mantenimiento',
                'call_center': 'Call center', 'limpieza': 'Limpieza', 'almacen': 'Almacén',
                'ventas_pendiente': 'Ventas · puesto específico pendiente',
                'sin_clasificar': 'Departamento o función pendiente'}[group['destino']]
            departamento['destino_label'] = group['destino_label']
        row['departamentos'] = sorted(departamentos.values(), key=lambda g: g['destino_label'])
        for departamento in row['departamentos']:
            departamento['personas'] = len(departamento['personas'])
        row['personas'] = len(personas_mes)
        # Una familia completa acredita su importe, sin cerrar las demás cargas ni su reparto.
        row['rcv_cubierto'] = (row.pop('personas_rcv') == personas_mes
                              and row['mes_cubierto'] and not row['errores_cargas'])
        row['cargas_cubiertas'] = (set(row['destinos_cargas']) == personas_mes
                                   and row['mes_cubierto'] and not row['pendientes']
                                   and not row['errores_cargas'])
        row['sucursales_ventas'] = sorted(sucursales.values(), key=lambda b: b['sucursal'])
        for branch in row['sucursales_ventas']:
            branch['personas'] = len(branch['personas'])
        row['total_ventas'] = sum((b['total'] for b in row['sucursales_ventas']), Decimal(0))
        row['total_otros'] = sum((g['total'] for g in row['otros_costos']), Decimal(0))
    return rows
