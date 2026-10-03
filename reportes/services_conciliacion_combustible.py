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
                'relaciones': [dict(e.attrib) for e in raiz.iter()
                               if _nombre(e) in ('CfdiRelacionados', 'CfdiRelacionado')]}
    except (ET.ParseError, InvalidOperation, ValueError, KeyError):
        return None


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
                    'estado': 'SIN CARGA IDENTIFICADA', 'del_mes': del_mes}
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
    total_bitacora = sum((c.importe_total for c in registros), ZERO)
    return {'periodo': inicio.strftime('%Y-%m'), 'rfc_receptor': rfc, 'fecha_corte': timezone.localdate().isoformat(),
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
              'CONSUMO FACTURADO (anticipos separados):']
    for f in datos['facturas']:
        unidad = f['unidad'] or 'sin unidad/persona identificada'
        lineas.append(f"  {f['fecha']} | {f['emisor']} | {f['factura']} | {f['tipo']} | ${f['total']:,.2f} | ticket {f['ticket'] or 'no identificado'} | {unidad} | {f['estado']} | UUID {f['uuid']}")
    if not datos['facturas']:
        lineas.append('  Sin consumo facturado identificado; revisar cobertura SAT y conceptos.')
    lineas.append(f"  Diésel ${datos['total_facturado']['DIESEL']:,.2f} · Gasolina ${datos['total_facturado']['GASOLINA']:,.2f}")
    lineas += ['', f"ANTICIPOS FACTURADOS: ${datos['total_anticipos']:,.2f} (no sumar como consumo)"]
    for a in datos['anticipos']:
        lineas.append(f"  {a['fecha']} | {a['emisor']} | {a['factura']} | ${a['total']:,.2f} | UUID {a['uuid']}")
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
               '  Saldo inicial/final de vales y canjes: no comprobados. Solicitar estado de cuenta de gasolinera.',
               '  Confirmar persona/unidad de gasolina sin carga identificada; centavos no prueban consumo personal.',
               '  Los anticipos, su aplicación fiscal y los tickets no son gastos independientes.',
               '  Un cruce documental no confirma pago ni canje; OCR debe cotejarse con la foto ante inconsistencias.',
               f"  Movimientos bancarios disponibles del mes: {datos['movimientos_bancarios_mes']}; disponibilidad no implica pago conciliado."]
    if not datos['movimientos_bancarios_mes']:
        lineas.append('  Sin cobertura bancaria del mes en ERP; pago pendiente de comprobación.')
    lineas.extend('  ' + p for p in datos['pendientes'])
    lineas.append('  La búsqueda incluye CFDI del mes anterior y siguiente disponibles; ausencia local no prueba ausencia en SAT.')
    return '\n'.join(lineas)
