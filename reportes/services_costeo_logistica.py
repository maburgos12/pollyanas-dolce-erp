"""Reparto gerencial del fondo mensual; no escribe movimientos ni mide consumo."""
from collections import defaultdict
from decimal import Decimal, ROUND_DOWN

from inventario.conteos_point import misma_unidad
from logistica.models import RutaCargaChecklistLinea
from reportes.models import RecetaCostoHistoricoMensual, ProductoReventaCostoHistoricoMensual

ZERO = Decimal('0')
CENT = Decimal('.01')


def repartir_centavos(fondo, bases):
    """Mayor residuo, desempate estable; conserva cada centavo del fondo."""
    if not fondo.is_finite() or fondo < 0 or fondo != fondo.quantize(CENT):
        raise ValueError('Fondo inválido')
    if any(not b.is_finite() or b < 0 for b in bases.values()):
        raise ValueError('Base inválida')
    positivas = {k: b for k, b in bases.items() if b > 0}
    total = sum(positivas.values(), ZERO)
    if not total:
        return {}
    exactos = {k: fondo * b / total for k, b in positivas.items()}
    cuotas = {k: x.quantize(CENT, rounding=ROUND_DOWN) for k, x in exactos.items()}
    orden = sorted(cuotas, key=lambda k: (-(exactos[k] - cuotas[k]), str(k)))
    for k in orden[:int((fondo - sum(cuotas.values(), ZERO)) / CENT)]:
        cuotas[k] += CENT
    return cuotas


def valorar_carga(linea, recetas, reventa):
    t = linea.point_transfer_line
    if not t or t.is_cancelled or not misma_unidad(linea.unit, t.unit):
        return None, 'Transferencia ausente, cancelada o unidad distinta'
    if t.unit_cost > 0:
        return t.unit_cost, f'Transferencia Point #{t.pk}'
    c = recetas.get(t.receta_id)
    if c:
        unidad = c.receta.rendimiento_unidad.codigo if c.receta.rendimiento_unidad else 'PZA'
        if misma_unidad(linea.unit, unidad):
            costo = c.costo_por_unidad_rendimiento
            if not costo and c.receta.rendimiento_cantidad in (None, Decimal(1)):
                costo = c.costo_total
            if costo and costo > 0:
                return costo, f'Receta mensual #{c.pk}'
    # SKU no es único: FK_articulo identifica la variante exacta de Point.
    detalle = (t.raw_payload or {}).get('detail') or {}
    external_id = str(detalle.get('FK_articulo', ''))
    c = reventa.get(external_id)
    if c and not t.is_insumo and misma_unidad(linea.unit, 'PZA'):
        return c.costo_promedio, f'Reventa mensual #{c.pk}'
    return None, 'Sin costo positivo comprobable en el periodo'


def distribuir_fondo(fondo, rutas, lineas):
    por_ruta = defaultdict(list)
    for l in lineas:
        por_ruta[l['ruta_id']].append(l)
    cuotas = (repartir_centavos(fondo, {r['id']: r['km_reparto'] for r in rutas})
              if rutas and all(r['km_reparto'] > 0 for r in rutas) else {})
    resultado = {'fondo': fondo, 'rutas': [], 'productos': [], 'destinos': [],
                 'sin_valor': [], 'asignado_rutas': sum(cuotas.values(), ZERO)}
    productos, destinos = {}, {}
    for r in rutas:
        carga = por_ruta[r['id']]
        importe = cuotas.get(r['id'])
        incompletas = [l for l in carga if l['valor'] is None or l['sucursal_id'] is None]
        resultado['sin_valor'].extend(incompletas)
        reparto = {}
        if importe is not None and carga and not incompletas:
            reparto = repartir_centavos(importe, {l['id']: l['valor'] for l in carga})
        destino_unico = {l['sucursal_id'] for l in carga}
        # Una ruta con destino único sí identifica su sucursal aunque falte valorar accesorios.
        por_destino = defaultdict(lambda: ZERO)
        if importe is not None and len(destino_unico) == 1 and None not in destino_unico:
            por_destino[next(iter(destino_unico))] = importe
        else:
            for l in carga:
                if l['id'] in reparto:
                    por_destino[l['sucursal_id']] += reparto[l['id']]
        for sid, monto in por_destino.items():
            nombre = next(l['sucursal'] for l in carga if l['sucursal_id'] == sid)
            d = destinos.setdefault(sid, {'id': sid, 'nombre': nombre, 'monto': ZERO})
            d['monto'] += monto
        for l in carga:
            if l['id'] not in reparto:
                continue
            key = (l['sucursal_id'], l['identidad'], l['unidad'])
            p = productos.setdefault(key, {**l, 'cantidad': ZERO, 'monto': ZERO, 'valor': ZERO, 'fuentes': set()})
            p['cantidad'] += l['cantidad']
            p['monto'] += reparto[l['id']]
            p['valor'] += l['valor']
            p['fuentes'].add(l['fuente'])
        resultado['rutas'].append({**r, 'monto': importe,
            'valor': sum((l['valor'] for l in carga if l['valor'] is not None), ZERO),
            'lineas': len(carga), 'sin_valor': len(incompletas),
            'asignado_productos': sum(reparto.values(), ZERO),
            'pendiente': (importe or ZERO) - sum(reparto.values(), ZERO)})
    for p in productos.values():
        p['unitario'] = p['monto'] / p['cantidad']
        p['fuentes'] = sorted(p['fuentes'])
    resultado['productos'] = sorted(productos.values(), key=lambda p: (p['sucursal'], p['nombre'], p['unidad'], p['identidad']))
    resultado['destinos'] = sorted(destinos.values(), key=lambda d: d['nombre'])
    resultado['asignado_destinos'] = sum((d['monto'] for d in destinos.values()), ZERO)
    resultado['asignado_productos'] = sum((p['monto'] for p in productos.values()), ZERO)
    resultado['pendiente_destinos'] = fondo - resultado['asignado_destinos']
    resultado['pendiente_productos'] = fondo - resultado['asignado_productos']
    return resultado


def leer_reparto_logistico(periodo, logistica):
    rutas = logistica.get('rutas', [])
    lineas = list(RutaCargaChecklistLinea.objects.filter(
        checklist__ruta_id__in=[r['id'] for r in rutas], superada_por__isnull=True,
        cantidad_cargada__gt=0).exclude(estatus__in=['SUPERADA', 'NO_APLICA']).select_related(
            'checklist__ruta__bitacora_salida', 'point_transfer_line', 'erp_destination_branch'))
    recetas = {c.receta_id: c for c in RecetaCostoHistoricoMensual.objects.filter(
        periodo=periodo, coverage_pct=100).select_related('receta__rendimiento_unidad')}
    reventa = {c.producto_point.external_id: c for c in ProductoReventaCostoHistoricoMensual.objects.filter(
        periodo=periodo, costo_promedio__gt=0).select_related('producto_point')}
    rutas_carga = {l.checklist.ruta_id: l.checklist.ruta for l in lineas}
    usos_bitacora = defaultdict(int)
    for r in rutas:
        if r['bitacora_salida_id']:
            usos_bitacora[r['bitacora_salida_id']] += 1
    preparadas = []
    for r in rutas:
        km, fuente = r['km_programados'], 'Distancia programada de la ruta'
        if km <= 0 and r['id'] in rutas_carga:
            b = rutas_carga[r['id']].bitacora_salida
            if (b and b.cerrada and b.km_llegada is not None and usos_bitacora[b.pk] == 1
                    and b.unidad_id == r['unidad_operativa_id']):
                km = Decimal(b.km_llegada - b.km_salida)
                fuente = f'Odómetros de bitácora {b.folio}'
        preparadas.append({**r, 'km_reparto': max(km, ZERO), 'fuente_km': fuente})
    carga = []
    for l in lineas:
        costo, fuente = valorar_carga(l, recetas, reventa)
        t = l.point_transfer_line
        identidad = f'linea:{l.pk}'
        if t:
            external_id = ((t.raw_payload or {}).get('detail') or {}).get('FK_articulo', '')
            if t.receta_id:
                identidad = f'receta:{t.receta_id}'
            elif t.is_insumo:
                identidad = f'insumo:{t.insumo_id or external_id or t.pk}'
            else:
                identidad = f'articulo:{external_id}'
        if identidad == 'articulo:':
            identidad = f'transferencia:{t.pk}'
        carga.append({'id': l.pk, 'ruta_id': l.checklist.ruta_id, 'ruta': l.checklist.ruta.folio, 'sucursal_id': l.erp_destination_branch_id,
            'sucursal': l.erp_destination_branch.nombre if l.erp_destination_branch_id else 'Sin destino',
            'codigo': l.item_code, 'nombre': l.item_name, 'identidad': identidad, 'unidad': l.unit,
            'cantidad': l.cantidad_cargada, 'valor': costo * l.cantidad_cargada if costo is not None else None,
            'fuente': fuente, 'transferencia_id': l.point_transfer_line_id})
    return distribuir_fondo(logistica['fondo'], preparadas, carga)
