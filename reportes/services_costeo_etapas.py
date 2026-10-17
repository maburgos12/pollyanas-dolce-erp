"""Análisis mensual de tres etapas; sólo lectura, sin alterar cierres ni nómina."""
from collections import defaultdict
from decimal import Decimal, ROUND_DOWN

from django.db.models import Q, Sum

from core.models import Sucursal
from logistica.models import (BitacoraSalidaLlegada, CargaCombustibleUnidad,
    Repartidor, RutaEntrega, RutaCargaChecklistLinea, SolicitudDomicilio)
from pos_bridge.models import PointSalesDailyProductFact
from reportes.models import LineaPresupuestoMensual, ReglaAsignacionGasto, RecetaCostoHistoricoMensual, ProductoReventaCostoHistoricoMensual
from reportes.services_costeo_cargas import leer_cargas_pagadas
from reportes.services_costeo_logistica import leer_reparto_logistico
from reportes.services_rentabilidad_gastos import leer_gastos_mensuales
from reportes.services_rentabilidad_personal import leer_personal_mensual
from reportes.services_mano_obra_mensual import limites
from reportes.services_rentabilidad_mensual import costos_de_sucursal
from rrhh.models import NominaLinea
from ventas.services.sales_read_service import get_sales_range_grouped

ZERO = Decimal('0')
CENT = Decimal('.01')


def asignar_parcial(fondo, bases, denominador):
    """El denominador conserva actividad no atribuida; nunca reparte el remanente."""
    if fondo < 0 or denominador <= 0 or any(v < 0 for v in bases.values()) or sum(bases.values(), ZERO) > denominador:
        return {}, fondo
    importes = {k: (fondo * v / denominador).quantize(CENT, rounding=ROUND_DOWN) for k, v in bases.items()}
    return importes, fondo - sum(importes.values(), ZERO)


def leer_logistica(inicio, fin, mo):
    rutas = list(RutaEntrega.objects.filter(fecha_ruta__range=(inicio, fin)).exclude(estatus='CANCELADA').values(
        'id', 'folio', 'fecha_ruta', 'estatus', 'unidad_operativa_id', 'repartidor_id', 'acompanante_id',
        'bitacora_salida_id', 'hora_inicio_real', 'hora_cierre_real', 'ruta_programada_distancia_metros'))
    turnos = list(BitacoraSalidaLlegada.objects.filter(fecha__range=(inicio, fin)).values(
        'id', 'folio', 'unidad_id', 'unidad__codigo', 'repartidor_id', 'cerrada', 'km_salida', 'km_llegada'))
    combustible = list(CargaCombustibleUnidad.objects.filter(bitacora__fecha__range=(inicio, fin))
        .values('unidad_id').annotate(monto=Sum('importe_total')))
    ids = {r['repartidor_id'] for r in rutas} | {r['acompanante_id'] for r in rutas} | {t['repartidor_id'] for t in turnos}
    usuarios = dict(Repartidor.objects.filter(id__in=ids).values_list('id', 'user_id'))
    produccion_ids = {p['id'] for p in mo['personas']}
    nominas = list(NominaLinea.objects.filter(
        Q(empleado__usuario_erp_id__in=usuarios.values()) | Q(empleado__puesto_operativo='REPARTIDOR') | Q(empleado_id__in=produccion_ids),
        periodo__fecha_fin__range=(inicio, fin), periodo__estatus__in=['CERRADA', 'PAGADA'])
        .values('id', 'empleado_id', 'empleado__nombre', 'empleado__puesto_operativo',
            'empleado__usuario_erp_id', 'empleado__departamento', 'empleado__sucursal_ref_id',
            'departamento_snapshot', 'total_percepciones', 'horas_trabajadas'))
    personal, exclusiones, candidatos = {}, defaultdict(lambda: ZERO), []
    for n in nominas:
        if n['empleado_id'] in produccion_ids:
            if n['empleado__departamento'] == 'VENTAS' and n['empleado__sucursal_ref_id']:
                exclusiones[n['empleado__sucursal_ref_id']] += n['total_percepciones']
            continue
        if n['empleado__puesto_operativo'] != 'REPARTIDOR':
            if not n['empleado__departamento']:
                candidatos.append(n)
            continue
        p = personal.setdefault(n['empleado_id'], {'id': n['empleado_id'], 'nombre': n['empleado__nombre'],
            'usuario_id': n['empleado__usuario_erp_id'], 'monto': ZERO, 'horas': ZERO})
        p['monto'] += n['total_percepciones']
        p['horas'] += n['horas_trabajadas']
        # Mismo filtro que el lector de nómina de sucursal: descontar sólo lo incluido allí.
        if n['empleado__departamento'] == 'VENTAS' and n['empleado__sucursal_ref_id']:
            exclusiones[n['empleado__sucursal_ref_id']] += n['total_percepciones']
    gastos = list(LineaPresupuestoMensual.objects.filter(periodo=inicio, version='ORIGINAL',
        rubro__activo=True, rubro__area__codigo='logistica').values(
            'id', 'rubro__concepto', 'monto_real', 'fuente_real', 'metadata'))
    permitidas = {'AUTO:COMBUSTIBLE_UNIDAD', 'AUTO:MANTENIMIENTO_UNIDAD', 'AUTO:OBLIGACION_GASTO'}
    conocidos = [g for g in gastos if g['monto_real'] is not None and g['monto_real'] >= 0
                 and g['fuente_real'] in permitidas and not (g['metadata'] or {}).get('sin_datos_fuente')]
    combustible_real = sum((g['monto_real'] for g in conocidos if g['fuente_real'] == 'AUTO:COMBUSTIBLE_UNIDAD'), ZERO)
    combustible_cargas = sum((c['monto'] for c in combustible), ZERO)
    combustible_conciliado = bool(combustible) and combustible_real == combustible_cargas
    unidades = {}
    for t in turnos:
        u = unidades.setdefault(t['unidad_id'], {'id': t['unidad_id'], 'codigo': t['unidad__codigo'],
            'km': ZERO, 'pendientes': [], 'combustible': None, 'asignado': ZERO})
        km = t['km_llegada'] - t['km_salida'] if t['cerrada'] and t['km_llegada'] is not None else None
        t['km'] = Decimal(km) if km is not None and km >= 0 else None
        if t['km'] is None:
            u['pendientes'].append(t['folio'])
        else:
            u['km'] += t['km']
    for c in combustible:
        if c['unidad_id'] in unidades:
            unidades[c['unidad_id']]['combustible'] = c['monto']
    por_turno = defaultdict(list)
    for r in rutas:
        por_turno[r['bitacora_salida_id']].append(r)
        a, b = r['hora_inicio_real'], r['hora_cierre_real']
        duracion = Decimal(str((b - a).total_seconds())) / 3600 if a and b else None
        r.update(horas=duracion, nomina=None, combustible=None, pendiente='',
                 km_programados=Decimal(r.get('ruta_programada_distancia_metros') or 0) / 1000)
        # Apertura de ruta no prueba jornada: sólo referencia operativa hasta 24 h.
        if duracion is None or duracion <= 0 or duracion > 24 or r['estatus'] != 'COMPLETADA':
            r['pendiente'] = 'Duración pendiente de validar'
    for p in personal.values():
        propias = [r for r in rutas if p['usuario_id'] is not None and p['usuario_id'] in
                   {usuarios.get(r['repartidor_id']), usuarios.get(r['acompanante_id'])}]
        validas = [r for r in propias if not r['pendiente']]
        # Intervalos superpuestos no se cobran dos veces al mismo empleado.
        orden = sorted(validas, key=lambda r: r['hora_inicio_real'])
        solapadas = {r['id'] for i, r in enumerate(orden) for s in orden[:i]
                    if r['hora_inicio_real'] < s['hora_cierre_real']}
        solapadas |= {s['id'] for i, r in enumerate(orden) for s in orden[:i]
                     if r['hora_inicio_real'] < s['hora_cierre_real']}
        bases = {r['id']: r['horas'] for r in validas if r['id'] not in solapadas}
        importes, resto = asignar_parcial(p['monto'], bases, p['horas'])
        p.update(asignado=sum(importes.values(), ZERO), pendiente=resto)
        for r in propias:
            if r['id'] in importes:
                r['nomina'] = (r['nomina'] or ZERO) + importes[r['id']]
    for u in unidades.values():
        bases = {}
        if combustible_conciliado and u['combustible'] is not None and not u['pendientes']:
            for t in turnos:
                vinculadas = por_turno[t['id']]
                if t['unidad_id'] == u['id'] and len(vinculadas) == 1:
                    r = vinculadas[0]
                    if r['unidad_operativa_id'] == u['id'] and not r['pendiente'] and t['km'] is not None:
                        bases[r['id']] = t['km']
            importes, _ = asignar_parcial(u['combustible'], bases, u['km'])
            for r in rutas:
                if r['id'] in importes:
                    r['combustible'] = importes[r['id']]
                    u['asignado'] += importes[r['id']]
        u['pendiente'] = u['combustible'] - u['asignado'] if u['combustible'] is not None else None
    despacho = sum((p['monto'] for p in mo['personas'] if p['componente'] == 'Coordinación de despacho'), ZERO)
    fondo = despacho + sum((p['monto'] for p in personal.values()), ZERO) + sum((g['monto_real'] for g in conocidos), ZERO)
    asignado = sum(((r['nomina'] or ZERO) + (r['combustible'] or ZERO) for r in rutas), ZERO)
    carga = list(RutaCargaChecklistLinea.objects.filter(checklist__ruta_id__in=[r['id'] for r in rutas],
        superada_por__isnull=True).values('erp_destination_branch__nombre', 'estatus', 'unit').annotate(cantidad=Sum('cantidad_cargada')))
    return {'rutas': rutas, 'unidades': list(unidades.values()), 'personal': list(personal.values()),
        'candidatos': candidatos, 'exclusiones': dict(exclusiones), 'gastos': gastos, 'carga': carga,
        'despacho': despacho, 'fondo': fondo, 'asignado': asignado, 'pendiente': fondo - asignado,
        'combustible_conciliado': combustible_conciliado, 'turnos': len(turnos),
        'sin_ruta': sum(1 for t in turnos if not por_turno[t['id']])}


def calcular_etapas(mo):
    inicio, fin = limites(mo['periodo'])
    logistica = leer_logistica(inicio, fin, mo)
    gastos = leer_gastos_mensuales(inicio, usar_reparto_consolidado=True)
    personal = leer_personal_mensual(inicio)
    cargas = leer_cargas_pagadas(inicio, mo['personas'])
    if cargas['confirmado']:
        # Sustituir la cobertura fiscal por el pago confirmado; jamás sumar ambos.
        personal = {key: [f for f in personal[key] if f['familia'] != 'cargas_patronales']
                    for key in ('filas', 'pendientes')}
    costos = {key: gastos[key] + personal[key] for key in ('filas', 'pendientes')}
    costos['filas'] += [f for f in cargas['filas'] if f['area'] == 'gastos-venta']
    cargas_logistica = sum((f['monto_mensual'] for f in cargas['filas'] if f['area'] == 'logistica'), ZERO)
    logistica['cargas_patronales'] = cargas_logistica
    logistica['fondo'] += cargas_logistica
    logistica['pendiente'] += cargas_logistica
    logistica['reparto'] = leer_reparto_logistico(inicio, logistica)
    por_destino = {d['id']: d['monto'] for d in logistica['reparto']['destinos']}
    adicionales = [f for f in gastos['filas'] if f['area'] == 'produccion'
                   and f['familia'] in {'renta', 'mantenimiento'}]
    adicionales += [f for f in cargas['filas'] if f['area'] == 'produccion']
    fondo_adicional = sum((f['monto_mensual'] for f in adicionales), ZERO)
    tasa_adicional = fondo_adicional / mo['base'] if mo['base'] > 0 else None
    ventas = get_sales_range_grouped(start_date=inicio, end_date=fin, dimension='branch')
    productos = {p['id']: p for p in mo['productos']}
    historicos = {c.receta_id: c for c in RecetaCostoHistoricoMensual.objects.filter(
        periodo=inicio, coverage_pct=100).select_related('receta__rendimiento_unidad')}
    # Misma tasa mensual por MP; las recetas vendidas no necesitan producción final ese mes.
    fondos_fabrica = sum((f['monto'] for f in mo['fondos'] if f['nombre'] != 'Coordinación de despacho'), ZERO)
    servicios = mo['servicios']['fondo']
    tasa_fabrica = ((fondos_fabrica + servicios + fondo_adicional) / mo['base']
                    if servicios is not None and mo['base'] > 0 and not mo['pendiente_reparto'] else None)
    reventa = defaultdict(list)
    reventa_por_id = {}
    for c in ProductoReventaCostoHistoricoMensual.objects.filter(periodo=inicio, costo_promedio__gt=0).select_related('producto_point'):
        reventa[c.producto_point.sku].append(c)
        reventa_por_id[c.producto_point_id] = c
    reglas_venta = list(ReglaAsignacionGasto.objects.filter(activo=True, categoria_gasto__codigo='OPEX_TOTAL_SUC').values('id', 'base_reparto'))
    regla_venta = reglas_venta[0] if len(reglas_venta) == 1 and reglas_venta[0]['base_reparto'] == 'VENTAS' else None
    sucursales, sin_ventas = [], []
    con_ventas = {v['branch_id'] for v in ventas['rows']}
    for s in Sucursal.objects.all().order_by('nombre'):
        gasto = costos_de_sucursal(costos, s.id)
        retiro = logistica['exclusiones'].get(s.id, ZERO)
        gasto_venta = gasto['total'] - retiro
        if s.id not in con_ventas:
            if gasto['total']:
                sin_ventas.append({'nombre': s.nombre, 'monto': gasto_venta})
            continue
        v = next(v for v in ventas['rows'] if v['branch_id'] == s.id)
        detalle = get_sales_range_grouped(start_date=inicio, end_date=fin, dimension='product', sucursales=[s.id])
        conciliado = (ventas['coverage_accepted'] and detalle['coverage_accepted']
            and ventas['source'] == detalle['source']
            and sum((p['total_sales'] for p in detalle['rows']), ZERO) == v['total_sales'])
        filas, costo_fabricacion = [], ZERO
        for p in detalle['rows']:
            fabricado = productos.get(p['recipe_id'])
            unitario = fabricado['fabricacion_servicios'] if fabricado else None
            fuente_costo = 'Producción mensual' if unitario is not None else ''
            if unitario is not None and tasa_adicional is not None:
                unitario += fabricado['mp'] * tasa_adicional
            elif not fabricado:
                c = historicos.get(p['recipe_id'])
                if c and tasa_fabrica is not None:
                    receta = c.receta
                    unidad = receta.rendimiento_unidad.codigo if receta.rendimiento_unidad else ''
                    rendimiento = receta.rendimiento_cantidad
                    if (not unidad or unidad.upper() in {'PZA', 'U'}) and (rendimiento is None or rendimiento == 1):
                        mp = c.costo_por_unidad_rendimiento or c.costo_total
                        if mp > 0:
                            unitario = mp * (1 + tasa_fabrica)
                            fuente_costo = 'Receta mensual · tasa de fabricación por MP'

            reventa_exacta = reventa_por_id.get(p.get('point_product_id'))
            candidatos_reventa = [reventa_exacta] if reventa_exacta else reventa.get(p.get('product_code'), [])
            if unitario is None and not p['recipe_id'] and len(candidatos_reventa) == 1:
                unitario = candidatos_reventa[0].costo_promedio
                fuente_costo = 'Costo mensual de reventa Point'
            costo = unitario * p['total_quantity'] if unitario is not None and p['total_quantity'] >= 0 and conciliado else None
            if costo is not None:
                costo_fabricacion += costo
            es_servicio = p.get('product_code') in {'0303', '0304'}
            filas.append({**p, 'es_servicio': es_servicio, 'fuente_costo': fuente_costo, 'fabricacion_unitaria': unitario, 'fabricacion': costo})
        cuotas, resto_venta = ({}, gasto_venta)
        if conciliado and regla_venta and gasto_venta >= 0:
            cuotas, resto_venta = asignar_parcial(gasto_venta,
                {i: p['total_sales'] for i, p in enumerate(filas)}, v['total_sales'])
        for i, p in enumerate(filas):
            p['gasto_venta'] = cuotas.get(i)
            p['gasto_venta_unitario'] = cuotas[i] / p['total_quantity'] if i in cuotas and p['total_quantity'] > 0 else None
            p['contribucion_parcial'] = p['total_sales'] - p['fabricacion'] - cuotas[i] if p['fabricacion'] is not None and i in cuotas else None
        sucursales.append({'id': s.id, 'nombre': s.nombre, 'ventas': v['total_sales'],
            'logistica_recibida': por_destino.get(s.id),
            'gasto_venta': gasto_venta if gasto_venta >= 0 else None, 'nomina_retirada': retiro,
            'fabricacion': costo_fabricacion, 'productos': filas, 'venta_sin_asignar': resto_venta, 'costos': gasto,
            'pendientes_producto': sum(p['fabricacion'] is None and not p['es_servicio'] for p in filas), 'ventas_conciliadas': conciliado})
    # Universo comercial de servicios, separado de notas/solicitudes operativas.
    servicios = list(PointSalesDailyProductFact.objects.filter(sale_date__range=(inicio, fin),
        point_product__sku__in=['0303', '0304']).values('point_product__sku', 'producto_nombre_historico')
        .annotate(cantidad=Sum('total_cantidad'), importe=Sum('total_venta')))
    domicilios = list(SolicitudDomicilio.objects.filter(pedido_cliente__point_note_sold_at__date__range=(inicio, fin),
        duplicado_de__isnull=True).exclude(estatus='CANCELADO').values(
            'id', 'pedido_cliente__point_note_id', 'repartidor_id', 'unidad_id', 'parada_ruta_id', 'entregado_en'))
    return {'logistica': logistica, 'sucursales': sucursales, 'sin_ventas': sin_ventas,
        'cargas': cargas, 'adicionales_fabricacion': adicionales, 'fondo_adicional_fabricacion': fondo_adicional,
        'venta_total': sum((s['ventas'] for s in sucursales), ZERO),
        'gasto_venta_total': sum((s['gasto_venta'] or ZERO for s in sucursales), ZERO),
        'servicios_domicilio': servicios, 'domicilios': domicilios,
        'domicilios_sin_vinculo': sum(not (d['repartidor_id'] and d['unidad_id'] and d['parada_ruta_id'] and d['entregado_en']) for d in domicilios),
        'fuente_ventas': ventas['source_detail'], 'regla_venta': regla_venta}
