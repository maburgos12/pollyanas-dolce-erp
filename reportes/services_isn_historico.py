"""Conciliación corporativa de 2025; no distribuye impuestos ni modifica nómina."""
from calendar import monthrange
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
import re
from xml.etree import ElementTree as ET

from django.db import transaction
from django.utils import timezone
from unidecode import unidecode

from rrhh.models import NominaConceptoLinea, NominaImportacion, NominaLinea, NominaPeriodo
from sat_client.models import CfdiDescargado
from .models import ExpedienteISN
from .services_isn import (
    MAX_CFDI_XML_BYTES, RFC_EMPRESA, TOLERANCIA_ISN_CALCULADO, ZERO,
    _bloquear_periodo_isn, _resolver_cfdi_isn, calcular_isn_sinaloa, extraer_isn_cfdi, money,
)

NS = '{http://www.sat.gob.mx/nomina12}'
METODO = 'EMPRESA_HISTORICA_2025_V1'
# Art.22 Ley de Hacienda de Sinaloa; códigos CONTPAQi cotejados con el CFDI.
EXENTOS = {'20', '22', '25', '26', '29'}
CODIGOS = {'1', '4', '10', '11', '14', '15', '17', '19', '21', '24', '31', '32'} | EXENTOS
TIPOS_EXENTOS = {'20': '021', '22': '021', '25': '003', '26': '025', '29': '022',
                 '24': '002', '31': '005', '32': '029'}
AJUSTES_CONCILIADOS = {'CARGO_GLOBAL_Y_ASIGNACION_AUTORIZADA_CONCILIADOS',
    'PAGO_CONCILIADO_CON_LISTA', 'ASIGNACION_EDENRED_Y_SALIDA_BANCARIA_CONCILIADAS'}


def _huella(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _nombre(value):
    return ' '.join(unidecode(value).upper().split())


def preparar_isn_empresa_2025(periodo, *, uuid=None):
    if not isinstance(periodo, date) or periodo.year != 2025 or periodo.day != 1:
        raise ValueError('La conciliación de empresa solo admite meses de 2025.')
    cfdi = _resolver_cfdi_isn(periodo, uuid)
    _, importe = extraer_isn_cfdi(cfdi)
    fin = date(2025, periodo.month, monthrange(2025, periodo.month)[1])
    periodos = list(NominaPeriodo.objects.filter(fecha_fin__year=2025, fecha_fin__lte=fin,
        estatus__in=('CERRADA', 'PAGADA')).order_by('fecha_fin', 'id'))
    ids = [p.pk for p in periodos]
    importaciones = list(NominaImportacion.objects.filter(periodo_id__in=ids,
        estatus='IMPORTADA').order_by('id'))
    imports = defaultdict(list)
    for imp in importaciones:
        imports[imp.periodo_id].append(imp)
    lineas = list(NominaLinea.objects.filter(periodo_id__in=ids).select_related('periodo')
        .prefetch_related('conceptos').order_by('periodo__fecha_fin', 'id'))
    if not lineas:
        raise ValueError('No existe nómina histórica cerrada.')
    uuids = [u.upper() for imp in importaciones for u in (imp.resumen or {}).get('uuids', [])]
    if len(uuids) != len(set(uuids)):
        raise ValueError('Un CFDI de nómina figura en varias importaciones.')
    fiscales = {c.uuid.upper(): c for c in CfdiDescargado.objects.filter(uuid__in=uuids)}
    aguinaldo = defaultdict(lambda: ZERO)
    dias = defaultdict(set)
    vistos, personas, fuentes, meses = set(), set(), [], defaultdict(lambda: ZERO)
    conceptos_mes = defaultdict(lambda: ZERO)
    dias_persona = set()
    for linea in lineas:
        p = linea.periodo
        if (linea.snapshot_origen != NominaLinea.SNAPSHOT_HISTORICA
                or p.tipo_periodo not in ('QUINCENAL', 'EXTRAORDINARIA')
                or p.fecha_inicio.year != 2025 or p.fecha_inicio > p.fecha_fin
                or p.fecha_inicio.month != p.fecha_fin.month):
            raise ValueError(f'Línea {linea.pk}: origen o calendario histórico no soportado.')
        candidatos = imports[p.pk]
        if len(candidatos) != 1 or not re.fullmatch('[0-9a-f]{64}', candidatos[0].archivo_hash):
            raise ValueError(f'Periodo {p.pk}: falta importación histórica única con hash.')
        imp = candidatos[0]
        match = re.search(r'CFDI ([A-Fa-f0-9-]{36})', linea.observaciones)
        clave = match[1].upper() if match else ''
        if clave not in (imp.resumen or {}).get('uuids', []) or clave in vistos:
            raise ValueError(f'Línea {linea.pk}: UUID ausente, ajeno o repetido.')
        vistos.add(clave)
        fiscal = fiscales.get(clave)
        if (fiscal is None or fiscal.estatus.upper().strip() != 'VIGENTE'
                or fiscal.rfc_emisor != RFC_EMPRESA or fiscal.tipo_comprobante != 'N'
                or fiscal.tipo_cfdi != 'emitido'):
            raise ValueError(f'Línea {linea.pk}: falta CFDI de nómina vigente del empleador.')
        xml = fiscal.xml_raw or ''
        if (len(xml.encode()) > MAX_CFDI_XML_BYTES or '<!DOCTYPE' in xml.upper()
                or '<!ENTITY' in xml.upper()):
            raise ValueError('XML de nómina fuera del contrato permitido.')
        try:
            root = ET.fromstring(xml.lstrip('\ufeff'))
            nomina = root.find('.//' + NS + 'Nomina')
            if nomina is None or nomina.get('FechaFinalPago') != p.fecha_fin.isoformat():
                raise ValueError('Periodo fiscal distinto de la nómina histórica.')
            originales, exentos, nombres = defaultdict(lambda: ZERO), defaultdict(lambda: ZERO), {}
            for c in nomina.findall('.//' + NS + 'Percepcion'):
                codigo = str(int(c.attrib['Clave']))
                if codigo in TIPOS_EXENTOS and c.get('TipoPercepcion') != TIPOS_EXENTOS[codigo]:
                    raise ValueError('El concepto exento no coincide con su tipo fiscal.')
                gravado, exento = Decimal(c.attrib['ImporteGravado']), Decimal(c.attrib['ImporteExento'])
                if not gravado.is_finite() or not exento.is_finite() or min(gravado, exento) < ZERO:
                    raise ValueError('Importe fiscal inválido.')
                originales[codigo] += gravado + exento
                exentos[codigo] += exento
                nombres[_nombre(c.attrib['Concepto'])] = codigo
        except (ET.ParseError, KeyError, ArithmeticError) as exc:
            raise ValueError(f'Línea {linea.pk}: XML fiscal inválido.') from exc
        ajustes = (imp.resumen or {}).get('ajustes_por_uuid', {}).get(clave, [])
        esperados = dict(originales)
        for ajuste in ajustes:
            nombre = _nombre(ajuste.get('concepto', ''))
            codigo = nombres.get(nombre) or {'AJUSTE EN SUELDOS': '17'}.get(nombre)
            monto = Decimal(ajuste['importe'])
            if (codigo is None or ajuste.get('estado') not in AJUSTES_CONCILIADOS
                    or not ajuste.get('soporte') or not monto.is_finite()):
                raise ValueError(f'Línea {linea.pk}: ajuste sin conciliación documental.')
            esperados[codigo] = esperados.get(codigo, ZERO) + monto
        reales = defaultdict(lambda: ZERO)
        for c in linea.conceptos.all():
            if c.tipo == NominaConceptoLinea.TIPO_PERCEPCION:
                reales[c.codigo_concepto.strip()] += c.importe
        if ({k: v for k, v in reales.items() if v} != {k: v for k, v in esperados.items() if v}
                or set(reales) - CODIGOS or any(v < ZERO for v in reales.values())):
            raise ValueError(f'Línea {linea.pk}: conceptos reales no concilian con CFDI y ajustes.')
        total = sum(reales.values(), ZERO)
        if linea.total_percepciones not in (total, total - reales['32']):
            raise ValueError(f'Línea {linea.pk}: total de percepciones inconsistente.')
        # Los límites de previsión social vienen del comprobante, no de una exención total supuesta.
        base = total - sum((reales[c] for c in EXENTOS), ZERO)
        base -= sum((min(reales[c], exentos[c]) for c in ('31', '32')), ZERO)
        uma = Decimal('108.57') if p.fecha_fin.month == 1 else Decimal('113.14')
        previo = aguinaldo[linea.empleado_id]
        acumulado = previo + reales['24']
        gravado = max(ZERO, acumulado - 30 * uma) - max(ZERO, previo - 30 * uma)
        base -= reales['24'] - gravado
        aguinaldo[linea.empleado_id] = acumulado
        meses[p.fecha_fin.month] += base
        if p.tipo_periodo == 'QUINCENAL':
            dia = p.fecha_inicio
            while dia <= p.fecha_fin:
                key = (linea.empleado_id, dia)
                if key in dias_persona:
                    raise ValueError(f'Línea {linea.pk}: nómina ordinaria solapada para la misma persona.')
                dias_persona.add(key)
                dias[dia.month].add(dia.day)
                dia += timedelta(days=1)
        if p.fecha_fin.month == periodo.month:
            personas.add(linea.empleado_id)
            for c, monto in reales.items():
                conceptos_mes[c] += monto
        fuentes.append((linea.pk, p.pk, linea.empleado_id, p.fecha_inicio, p.fecha_fin,
            p.tipo_periodo, linea.total_percepciones, clave, _huella(xml), imp.archivo_hash,
            ajustes, dict(reales)))
    if vistos != set(uuids):
        raise ValueError('Las líneas no cubren todos los CFDI de las importaciones históricas.')
    for mes in range(1, periodo.month + 1):
        if len(dias[mes]) != monthrange(2025, mes)[1]:
            raise ValueError(f'Falta cobertura ordinaria del mes {mes}; no se presume base anual completa.')
    base = money(meses[periodo.month])
    calculado = calcular_isn_sinaloa(base)
    diferencia = money(importe - calculado)
    return dict(metodo=METODO, periodo=periodo.isoformat(), cfdi_id=cfdi.pk, uuid=cfdi.uuid,
        importe_cfdi=str(importe), base_calculada=str(base), isn_calculado=str(calculado),
        diferencia_isn=str(diferencia), estado=(ExpedienteISN.ESTADO_APLICADO
            if abs(diferencia) <= TOLERANCIA_ISN_CALCULADO else ExpedienteISN.ESTADO_DISCREPANCIA),
        huella_fuentes=_huella((fuentes, cfdi.xml_raw)), personas=len(personas),
        periodos=[p.pk for p in periodos if p.fecha_fin.month == periodo.month],
        conceptos={k: str(v) for k, v in sorted(conceptos_mes.items()) if v},
        distribucion='SIN_DISTRIBUCION', pago_bancario='NO_CERTIFICADO',
        criterio='Contraste CFDI estatal con nómina real y ajustes documentados; no declara pago bancario.')


@transaction.atomic
def aplicar_isn_empresa_2025(preview, *, aplicado_por=None):
    periodo = date.fromisoformat(preview['periodo'])
    if periodo.year != 2025 or periodo.day != 1 or preview.get('metodo') != METODO:
        raise ValueError('El expediente histórico solo admite meses de 2025.')
    _bloquear_periodo_isn(periodo)
    # Bloquear padres impide editar/insertar detalles durante la revalidación.
    list(NominaPeriodo.objects.order_by('pk').select_for_update(of=('self',)).filter(fecha_fin__year=2025,
        fecha_fin__lte=date(2025, periodo.month, monthrange(2025, periodo.month)[1])))
    list(NominaLinea.objects.order_by('pk').select_for_update(of=('self',)).filter(periodo__fecha_fin__year=2025,
        periodo__fecha_fin__month__lte=periodo.month))
    list(NominaConceptoLinea.objects.order_by('pk').select_for_update(of=('self',)).filter(linea__periodo__fecha_fin__year=2025,
        linea__periodo__fecha_fin__month__lte=periodo.month))
    imports = list(NominaImportacion.objects.order_by('pk').select_for_update(of=('self',)).filter(periodo__fecha_fin__year=2025,
        periodo__fecha_fin__month__lte=periodo.month))
    fiscales = [u for imp in imports for u in (imp.resumen or {}).get('uuids', [])]
    list(CfdiDescargado.objects.order_by('pk').select_for_update(of=('self',)).filter(uuid__in=[preview['uuid'], *fiscales]))
    actual = preparar_isn_empresa_2025(periodo, uuid=preview['uuid'])
    if actual != preview:
        raise ValueError('Las fuentes o el preview cambiaron; vuelva a conciliar.')
    existentes = list(ExpedienteISN.objects.order_by('pk').select_for_update(of=('self',)).filter(periodo=periodo))
    if existentes:
        if (len(existentes) == 1 and existentes[0].metadata == preview
                and existentes[0].uuid == preview['uuid']
                and existentes[0].estado == preview['estado']
                and existentes[0].importe_pagado == Decimal(preview['importe_cfdi'])
                and existentes[0].base_gravada_calculada == Decimal(preview['base_calculada'])
                and existentes[0].base_declarada is None
                and not existentes[0].distribuciones.exists()):
            return existentes[0]
        raise ValueError('Ya existe un expediente distinto; no se sobrescribe ni acepta una diferencia.')
    aplicado = preview['estado'] == ExpedienteISN.ESTADO_APLICADO
    return ExpedienteISN.objects.create(periodo=periodo, revision=1, cfdi_id=preview['cfdi_id'],
        uuid=preview['uuid'], importe_pagado=preview['importe_cfdi'],
        base_gravada_calculada=preview['base_calculada'], estado=preview['estado'],
        aplicado_por=aplicado_por if aplicado else None,
        aplicado_en=timezone.now() if aplicado else None, metadata=preview)
