"""Conciliación documental de combustible, sin alterar facturas ni cargas.

Anticipos, consumo físico y pagos son hechos diferentes. Los centavos no
identifican al consumidor y una resta mensual no demuestra un faltante.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from xml.etree import ElementTree as ET

from django.conf import settings
from django.utils import timezone

CENT = Decimal('0.01')
ZERO = Decimal('0')
# Claves comprobadas en los CFDI de las fuentes existentes. Otras requieren revisión.
CLAVES_COMBUSTIBLE = {'15101505': 'DIESEL', '15101514': 'GASOLINA', '15101515': 'GASOLINA'}
# Alias comprobado por RFC en fotos y XML; no deducir identidad por parecido.
RFC_ESTACIONES = {'SERVICIO CARRANZA': 'FAOR391222TTA'}
RFC_PROVEEDORES_VERIFICADOS = frozenset(RFC_ESTACIONES.values())
PALABRAS_COMBUSTIBLE = ('DIESEL', 'DIÉSEL', 'MAGNA', 'PREMIUM', 'GASOLINA')


def _es_monto_redondo(total: Decimal) -> bool:
    """Compatibilidad del helper histórico; no usarlo para atribuir consumo."""
    return total == total.to_integral_value() and int(total) % 100 == 0


def _mes_siguiente(d: date) -> date:
    return (d.replace(day=28) + timedelta(days=4)).replace(day=1)


def _decimal(value):
    try:
        numero = Decimal(str(value))
        return numero if numero.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def _nombre(elemento):
    return elemento.tag.rsplit('}', 1)[-1]


def _leer_documento(cfdi):
    """Obtener importes brutos por concepto, sin clasificar desde texto global."""
    try:
        if len((cfdi.xml_raw or '').encode()) > 2 * 1024 * 1024:
            raise ValueError('XML excede límite de lectura')
        raiz = ET.fromstring(cfdi.xml_raw or '')
        conceptos = []
        for e in raiz.iter():
            if _nombre(e) != 'Concepto':
                continue
            bruto = Decimal(e.attrib['Importe']) - Decimal(e.get('Descuento', '0'))
            for t in e.iter():
                if _nombre(t) in ('Traslado', 'Retencion') and t.get('Importe') is not None:
                    bruto += Decimal(t.get('Importe')) * (-1 if _nombre(t) == 'Retencion' else 1)
            if not bruto.is_finite() or bruto < ZERO:
                raise ValueError('Importe de concepto inválido')
            conceptos.append({
                'tipo': CLAVES_COMBUSTIBLE.get(e.get('ClaveProdServ')),
                'descripcion': e.get('Descripcion', ''),
                'importe': bruto, 'identificacion': e.get('NoIdentificacion', ''),
                'litros': _decimal(e.get('Cantidad')) if e.get('ClaveUnidad') == 'LTR' else None,
            })
        total_xml = _decimal(raiz.get('Total'))
        consistente = (bool(conceptos) and total_xml is not None
                       and sum((c['importe'] for c in conceptos), ZERO).quantize(CENT, rounding=ROUND_HALF_UP) == total_xml
                       and total_xml == cfdi.total)
        return {'conceptos': conceptos, 'consistente': consistente,
                'serie': raiz.get('Serie', ''), 'folio': raiz.get('Folio', ''),
                'aplica_anticipos': [x.get('UUID', '').upper() for g in raiz
                                      if _nombre(g) == 'CfdiRelacionados' and g.get('TipoRelacion') == '07'
                                      for x in g if _nombre(x) == 'CfdiRelacionado'],
                'relaciones': [dict(e.attrib) for e in raiz.iter()
                               if _nombre(e) in ('CfdiRelacionados', 'CfdiRelacionado')]}
    except (ET.ParseError, InvalidOperation, ValueError, KeyError):
        return None


def _circuito_anticipos(leidos, inicio, fin, proveedores):
    """Reconstruir relaciones explícitas; nunca repartir una aplicación con FIFO."""
    documentos = {}
    for c, d in leidos:
        if d and c.moneda == 'MXN' and c.rfc_emisor in proveedores:
            documentos[c.uuid.upper()] = {**d, 'uuid': c.uuid.upper(), 'total': c.total,
                'fecha': timezone.localtime(c.fecha_emision).date(), 'rfc': c.rfc_emisor,
                'tipo': c.tipo_comprobante, 'factura': d['serie'] + d['folio']}
    anticipos = {u: d for u, d in documentos.items() if d['tipo'] == 'I'
                 and d['consistente'] and d['conceptos']
                 and all('ANTICIPO' in x['descripcion'].upper() for x in d['conceptos'])}
    notas = defaultdict(list)
    for d in documentos.values():
        if (d['tipo'] == 'E' and d['consistente'] and len(d['aplica_anticipos']) == 1
                and d['conceptos'] and all('ANTICIPO' in x['descripcion'].upper() for x in d['conceptos'])):
            notas[d['aplica_anticipos'][0]].append(d)
    aplicaciones = []
    for u, d in documentos.items():
        if d['tipo'] != 'I' or not d['aplica_anticipos'] or not any(x['tipo'] for x in d['conceptos']):
            continue
        refs = set(d['aplica_anticipos'])
        nc = notas[u]
        valida = (d['consistente'] and len(nc) == 1 and nc[0]['total'] == d['total']
                  and nc[0]['rfc'] == d['rfc'] and nc[0]['fecha'] < fin
                  and all(a in anticipos and anticipos[a]['rfc'] == d['rfc']
                          and anticipos[a]['fecha'] < fin for a in refs))
        aplicaciones.append({'uuid': u, 'factura': d['factura'], 'fecha': d['fecha'],
            'total': d['total'], 'anticipos': sorted(refs), 'rfc': d['rfc'],
            'nota': nc[0]['factura'] if len(nc) == 1 else '',
            'nota_uuid': nc[0]['uuid'] if len(nc) == 1 else '', 'confirmada': valida})
    circuitos = []
    for rfc in sorted(proveedores):
        mes = [a for a in aplicaciones if a['rfc'] == rfc and inicio <= a['fecha'] < fin]
        seleccion = {u for u, d in anticipos.items() if d['rfc'] == rfc and inicio <= d['fecha'] < fin}
        seleccion.update(u for a in mes for u in a['anticipos'])
        if not seleccion:
            continue
        # Incluir aplicaciones previas de los mismos anticipos para recuperar arrastre.
        relacionadas = []
        while True:
            relacionadas = [a for a in aplicaciones if a['rfc'] == rfc and a['fecha'] < fin
                            and seleccion.intersection(a['anticipos'])]
            ampliada = seleccion | {u for a in relacionadas for u in a['anticipos']}
            if ampliada == seleccion:
                break
            seleccion = ampliada
        completo = all(u in anticipos for u in seleccion) and all(a['confirmada'] for a in relacionadas)
        comprado = sum((anticipos[u]['total'] for u in seleccion if u in anticipos
                        and inicio <= anticipos[u]['fecha'] < fin), ZERO)
        aplicado = sum((a['total'] for a in mes if a['confirmada']), ZERO)
        arrastre = None
        residual = None
        if completo:
            arrastre = (sum((anticipos[u]['total'] for u in seleccion if anticipos[u]['fecha'] < inicio), ZERO)
                        - sum((a['total'] for a in relacionadas if a['fecha'] < inicio), ZERO))
            residual = arrastre + comprado - aplicado
            if arrastre < ZERO or residual < ZERO:
                arrastre = residual = None
        circuitos.append({'rfc': rfc, 'arrastre_documental': arrastre, 'anticipos_mes': comprado,
                          'aplicaciones_mes': aplicado, 'residual_documental': residual})
    return aplicaciones, circuitos


def conciliar_combustible(periodo: date) -> dict:
    """Cruzar folios con evidencia disponible; conservar pendientes sin escribir."""
    from logistica.models import CargaCombustibleUnidad
    from sat_client.models import CfdiDescargado
    from syncfy_client.models import MovimientoBancario
    from reportes.services_isn import RFC_EMPRESA

    inicio = periodo.replace(day=1)
    fin = _mes_siguiente(inicio)
    desde = (inicio - timedelta(days=1)).replace(day=1)
    hasta = _mes_siguiente(fin)
    rfc = (getattr(settings, 'SAT_RFC', '') or RFC_EMPRESA).strip().upper()
    docs = list(CfdiDescargado.objects.filter(
        fecha_emision__date__gte=desde, fecha_emision__date__lt=hasta,
        tipo_cfdi='recibido', tipo_comprobante__in=('I', 'E'), rfc_receptor=rfc,
        estatus__iexact='vigente').order_by('fecha_emision', 'uuid'))
    leidos = [(c, _leer_documento(c)) for c in docs]
    proveedores = set(RFC_PROVEEDORES_VERIFICADOS)
    for c, d in leidos:
        if d and d['consistente'] and c.tipo_comprobante == 'I' and c.moneda == 'MXN' and any(x['tipo'] for x in d['conceptos']):
            proveedores.add(c.rfc_emisor)

    aplicaciones, circuitos = _circuito_anticipos(leidos, inicio, fin, proveedores)
    uuids_aplicacion = {a['uuid'] for a in aplicaciones}
    facturas, anticipos, egresos, pendientes, exclusiones = [], [], [], [], []
    total_facturado = {'DIESEL': ZERO, 'GASOLINA': ZERO}
    por_ticket = defaultdict(list)
    for c, d in leidos:
        fecha = timezone.localtime(c.fecha_emision).date()
        del_mes = inicio <= fecha < fin
        if not d:
            if del_mes:
                pendientes.append(f'CFDI {c.uuid}: XML ausente o inválido; clasificación pendiente.')
            continue
        fuel = [x for x in d['conceptos'] if x['tipo']]
        es_anticipo = (bool(d['conceptos']) and c.rfc_emisor in proveedores
                       and all('ANTICIPO' in x['descripcion'].upper() for x in d['conceptos']))
        relevante = bool(fuel) or es_anticipo or c.rfc_emisor in proveedores
        base = {'uuid': c.uuid, 'fecha': fecha.isoformat(), 'emisor': c.nombre_emisor or c.rfc_emisor,
                'rfc_emisor': c.rfc_emisor, 'factura': d['serie'] + d['folio'],
                'total': c.total, 'relaciones': d['relaciones']}
        if c.moneda != 'MXN':
            if relevante and del_mes:
                pendientes.append(f'CFDI {c.uuid}: moneda {c.moneda}, pendiente de conversión; no sumado en MXN.')
            continue
        if c.tipo_comprobante == 'E':
            if relevante and del_mes:
                egresos.append(base)
            continue
        if es_anticipo:
            if del_mes:
                if d['consistente']:
                    anticipos.append(base)
                else:
                    pendientes.append(f'Anticipo {c.uuid}: XML y total no cuadran; no sumado.')
            continue
        if not fuel:
            if del_mes and any(any(k in x['descripcion'].upper() for k in PALABRAS_COMBUSTIBLE) for x in d['conceptos']):
                exclusiones.append({**base, 'motivo': 'Sin clave de combustible verificada; no sumar por palabras como Premium.'})
            continue
        if not d['consistente']:
            if del_mes:
                pendientes.append(f'CFDI {c.uuid}: conceptos, impuestos o total no cuadran; no distribuir automáticamente.')
            continue
        for x in fuel:
            bruto = x['importe'].quantize(CENT, rounding=ROUND_HALF_UP)
            # Folio específico del formato de permiso observado; nunca extraer un número arbitrario.
            match = re.fullmatch(r'PL/\d+/EXP/ES/\d{4}-(\d+)', x['identificacion'])
            item = {**base, 'tipo': x['tipo'], 'total': bruto, 'ticket': match.group(1) if match else '',
                    'litros': x['litros'], 'carga_id': None, 'unidad': None,
                    'estado': 'SIN CARGA IDENTIFICADA', 'del_mes': del_mes,
                    'es_aplicacion': c.uuid.upper() in uuids_aplicacion}
            if del_mes:
                total_facturado[x['tipo']] += bruto
                facturas.append(item)
            if item['ticket']:
                por_ticket[(c.rfc_emisor, item['ticket'])].append(item)

    registros = list(CargaCombustibleUnidad.objects.filter(
        fecha_registro__date__gte=inicio, fecha_registro__date__lt=fin
    ).select_related('unidad').order_by('fecha_registro', 'id'))
    lecturas = [(c, c.auditoria_detalle.get('ticket_leido', {})) for c in registros]
    claves = Counter((RFC_ESTACIONES.get(str(t.get('estacion', '')).strip().upper()), str(t.get('folio', '')).strip())
                     for c, t in lecturas if t.get('folio'))
    hashes = Counter(c.ticket_sha256 for c in registros if c.ticket_sha256)
    cargas, bitacora = [], {}
    total_cruzado = ZERO
    for c, t in lecturas:
        codigo = c.unidad.codigo
        grupo = bitacora.setdefault(codigo, {'cargas': 0, 'total': ZERO})
        grupo['cargas'] += 1
        grupo['total'] += c.importe_total
        folio = str(t.get('folio', '')).strip()
        clave = (RFC_ESTACIONES.get(str(t.get('estacion', '')).strip().upper()), folio)
        item = {'id': c.id, 'unidad': codigo, 'fecha': timezone.localtime(c.fecha_registro).date().isoformat(),
                'fecha_ticket': t.get('fecha'), 'ticket': folio, 'importe': c.importe_total,
                'estado': 'TICKET_SIN_CFDI_LOCALIZADO', 'uuid_cfdi': None, 'factura': None}
        if not (c.foto_ticket and c.auditoria_estado == 'ok' and t.get('es_ticket') is True
                and t.get('legible') is True and folio):
            item['estado'] = 'RESPALDO_PENDIENTE'
        elif claves[clave] > 1 or (c.ticket_sha256 and hashes[c.ticket_sha256] > 1):
            item['estado'] = 'DUPLICADO_REVISAR'
        elif not clave[0]:
            item['estado'] = 'ESTACION_PENDIENTE'
        elif not t.get('fecha') or not inicio.isoformat() <= str(t['fecha']) < fin.isoformat():
            item['estado'] = 'FECHA_TICKET_REVISAR'
        else:
            candidatos = por_ticket.get(clave, [])
            monto, litros = _decimal(t.get('importe_total')), _decimal(t.get('litros'))
            if len(candidatos) > 1:
                item['estado'] = 'CFDI_MULTIPLE_REVISAR'
            elif len(candidatos) == 1:
                f = candidatos[0]
                if (monto is not None and monto == c.importe_total == f['total']
                        and litros is not None and f['litros'] is not None
                        and abs(litros - f['litros']) <= Decimal('0.02')):
                    item.update(estado='CRUCE_DOCUMENTAL', uuid_cfdi=f['uuid'], factura=f['factura'])
                    f.update(carga_id=c.id, unidad=codigo, estado='CRUCE_DOCUMENTAL')
                    total_cruzado += c.importe_total
                else:
                    item['estado'] = 'IMPORTE_O_LITROS_REVISAR'
        cargas.append(item)

    banco = MovimientoBancario.objects.filter(fecha_transaccion__date__gte=inicio, fecha_transaccion__date__lt=fin)
    n_banco = banco.count()
    pagos = list(MovimientoBancario.objects.filter(
        cfdi_relacionado__uuid__in=[a['uuid'] for a in anticipos], conciliado=True,
        tipo='cargo', moneda='MXN', movimiento_relacionado__isnull=True).select_related('cuenta', 'cfdi_relacionado').order_by('fecha_transaccion'))
    pagos_anticipos = [{'id': p.pk, 'fecha': timezone.localtime(p.fecha_transaccion).date().isoformat(),
        'banco': p.cuenta.nombre_display, 'cuenta': (p.cuenta.numero_cuenta or '')[-4:],
        'importe': p.monto, 'uuid': p.cfdi_relacionado.uuid,
        'referencia': p.extra_raw.get('referencia', ''), 'nota': p.nota_conciliacion} for p in pagos]
    devoluciones = []
    for p in banco.filter(tipo='abono', conciliado=True, moneda='MXN',
            movimiento_relacionado__isnull=False).select_related('movimiento_relacionado', 'cuenta'):
        cargo = p.movimiento_relacionado
        raw = cargo.extra_raw.get('raw', {})
        if (isinstance(raw, dict) and raw.get('rfc_proveedor') in proveedores
                and cargo.cuenta_id == p.cuenta_id and cargo.tipo == 'cargo' and cargo.monto == p.monto
                and cargo.movimiento_relacionado_id == p.pk and cargo.conciliado):
            devoluciones.append({'fecha': timezone.localtime(p.fecha_transaccion).date().isoformat(),
                'importe': p.monto, 'referencia': p.extra_raw.get('referencia', ''), 'nota': p.nota_conciliacion})
    total_bitacora = sum((c.importe_total for c in registros), ZERO)
    return {'periodo': inicio.strftime('%Y-%m'), 'rfc_receptor': rfc, 'fecha_corte': timezone.localdate().isoformat(),
            'aplicaciones': [a for a in aplicaciones if inicio <= a['fecha'] < fin],
            'circuitos': circuitos, 'pagos_anticipos': pagos_anticipos, 'devoluciones': devoluciones,
            'total_pagos_anticipos': sum((p['importe'] for p in pagos_anticipos), ZERO),
            'estado': 'PARCIAL', 'facturas': facturas, 'anticipos': anticipos, 'egresos': egresos,
            'exclusiones': exclusiones, 'pendientes': pendientes, 'cargas': cargas,
            'total_facturado': total_facturado, 'total_anticipos': sum((a['total'] for a in anticipos), ZERO),
            'bitacora': bitacora, 'total_bitacora': total_bitacora, 'total_cruzado': total_cruzado,
            'total_cargas_pendientes': total_bitacora - total_cruzado,
            'saldo_vales': None, 'movimientos_bancarios_mes': n_banco,
            # Compatibilidad: no convertir una diferencia documental en faltante monetario.
            'diferencia': None}


def render_conciliacion_texto(datos: dict) -> str:
    lineas = [f"Conciliación documental combustible {datos['periodo']} — PARCIAL",
              f"RFC receptor: {datos['rfc_receptor']} · corte: {datos['fecha_corte']}", '',
              'LECTURA DEL MES:',
              'Las transferencias compran saldo anticipado. Los canjes consumen ese saldo.',
              'Factura de consumo y nota de aplicación respaldan un mismo uso del anticipo.',
              f"Anticipos facturados: ${datos['total_anticipos']:,.2f}. Pagos vinculados en ERP: ${datos.get('total_pagos_anticipos', ZERO):,.2f}."]
    for c in datos.get('circuitos', []):
        saldo = c['residual_documental']
        arrastre = c['arrastre_documental']
        lineas.append(f"  {c['rfc']}: arrastre documental {('$' + format(arrastre, ',.2f')) if arrastre is not None else 'pendiente'}; aplicaciones ${c['aplicaciones_mes']:,.2f}; residual del circuito {('$' + format(saldo, ',.2f')) if saldo is not None else 'pendiente'}.")
    lineas += ['El residual documental no certifica el saldo físico de vales ni otros saldos históricos.', '',
               'DETALLE FISCAL DEL COMBUSTIBLE (no sumar otra vez a los anticipos):']
    for f in datos['facturas']:
        unidad = f['unidad'] or 'sin unidad/persona identificada'
        lineas.append(f"  {f['fecha']} | {f['emisor']} | {f['factura']} | {f['tipo']} | ${f['total']:,.2f} | ticket {f['ticket'] or 'no identificado'} | {unidad} | {f['estado']} | {'aplicación de anticipo' if f.get('es_aplicacion') else 'fuera del circuito de anticipos; pago separado'} | UUID {f['uuid']}")
    if not datos['facturas']:
        lineas.append('  Sin consumo facturado identificado; revisar cobertura SAT y conceptos.')
    lineas.append(f"  Diésel ${datos['total_facturado']['DIESEL']:,.2f} · Gasolina ${datos['total_facturado']['GASOLINA']:,.2f}")
    lineas += ['', f"ANTICIPOS FACTURADOS: ${datos['total_anticipos']:,.2f} (no sumar como consumo)"]
    for a in datos['anticipos']:
        lineas.append(f"  {a['fecha']} | {a['emisor']} | {a['factura']} | ${a['total']:,.2f} | UUID {a['uuid']}")
    lineas += ['', 'PAGOS DE ANTICIPOS COMPROBADOS Y VINCULADOS:']
    for p in datos.get('pagos_anticipos', []):
        lineas.append(f"  {p['fecha']} | {p['banco']} *{p['cuenta']} | ${p['importe']:,.2f} | rastreo {p['referencia']} | CFDI {p['uuid']}")
    for d in datos.get('devoluciones', []):
        lineas.append(f"  {d['fecha']} | envío devuelto ${d['importe']:,.2f} | efecto neto $0.00 | rastreo {d['referencia']} | {d['nota']}")
    lineas += ['', 'APLICACIONES FISCALES (cada pareja se cuenta una vez):']
    for a in datos.get('aplicaciones', []):
        lineas.append(f"  {a['fecha']} | {a['factura']} / {a['nota'] or 'nota pendiente'} | ${a['total']:,.2f} | {'relaciones verificadas' if a['confirmada'] else 'revisar relaciones/importes'} | anticipos: {', '.join(a['anticipos'])}")
    lineas += ['', 'BITÁCORA POR UNIDAD (fecha de captura):']
    for unidad, b in sorted(datos['bitacora'].items()):
        lineas.append(f"  {unidad} | {b['cargas']} cargas | ${b['total']:,.2f}")
    lineas += [f"Total registrado: ${datos['total_bitacora']:,.2f}",
               f"Cruces documentales por folio, estación, importe y litros: ${datos['total_cruzado']:,.2f}",
               f"Cargas pendientes de conciliación documental: ${datos['total_cargas_pendientes']:,.2f}", '', 'DETALLE DE CARGAS:']
    for c in datos['cargas']:
        lineas.append(f"  #{c['id']} | {c['fecha']} | {c['unidad']} | ${c['importe']:,.2f} | ticket {c['ticket'] or 'sin ticket'} | {c['estado']} | CFDI {c['uuid_cfdi'] or 'no cruzado'}")
    if datos['egresos']:
        lineas += ['', 'EGRESOS: revisar aplicación con UUID relacionados; no equivalen a devolución bancaria.']
        for e in datos['egresos']:
            lineas.append(f"  {e['fecha']} | {e['factura']} | ${e['total']:,.2f} | UUID {e['uuid']} | relaciones {e['relaciones']}")
    if datos['exclusiones']:
        lineas += ['', 'DOCUMENTOS NO SUMADOS POR CLASIFICACIÓN:']
        for e in datos['exclusiones']:
            lineas.append(f"  {e['emisor']} | ${e['total']:,.2f} | UUID {e['uuid']} | {e['motivo']}")
    lineas += ['', 'PENDIENTES PARA CIERRE:',
               '  Canjes físicos: comprobar vales/tickets y bitácora. Las aplicaciones fiscales no prueban por sí solas la entrega ni la unidad.',
               '  Combustible fuera del circuito de anticipos: conciliar su pago por separado; no atribuirlo a una unidad sin evidencia.',
               '  Los anticipos, su aplicación fiscal y los tickets no son gastos independientes.',
               '  Un cruce documental no confirma pago ni canje; OCR debe cotejarse con la foto ante inconsistencias.',
               f"  Movimientos bancarios disponibles del mes: {datos['movimientos_bancarios_mes']}; la selección importada no garantiza cobertura completa de la cuenta."]
    if not datos['movimientos_bancarios_mes']:
        lineas.append('  Sin cobertura bancaria del mes en ERP; pago pendiente de comprobación.')
    lineas.extend('  ' + p for p in datos['pendientes'])
    lineas.append('  La búsqueda incluye CFDI del mes anterior y siguiente disponibles; ausencia local no prueba ausencia en SAT.')
    return '\n'.join(lineas)
