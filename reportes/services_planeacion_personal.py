"""Planeación reproducible: ventas observadas y costo documental, sin estimar faltantes.

El presupuesto no es una autorización. No modifica RRHH ni anualiza meses parciales.
"""
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import re
from xml.etree import ElementTree as ET

from django.db.models import Count, Max, Min, Q, Sum
from django.db.models.functions import TruncMonth
from django.utils import timezone

from core.models import Sucursal
from rrhh.models import Empleado
from sat_client.models import CfdiDescargado
from ventas.services.sales_canonical_source import official_point_sales_rows_for_range
from .models import ExpedienteCedulaIMSS, ExpedienteISN
from .services_isn import extraer_isn_cfdi

ZERO = Decimal('0')
CENT = Decimal('0.01')
# Identidades del empleador y del registro presentes en los comprobantes fuente.
RFC = 'GEF211230KR2'
REGISTRO = 'E5240157100'
NOMINA_NS = '{http://www.sat.gob.mx/nomina12}'
CFDI_NS = '{http://www.sat.gob.mx/cfd/4}'


def _normalizar_registro_patronal(valor):
    return re.sub(r'[^A-Z0-9]', '', str(valor or '').upper())


def money(value):
    return Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def month_shift(value, delta):
    year, month = divmod(value.year * 12 + value.month - 1 + delta, 12)
    return date(year, month + 1, 1)


def budget_policy(sales):
    """Tres meses consecutivos; objetivo limitado además por el peor mes observado."""
    if len(sales) != 3 or any(value is None or value <= 0 for value in sales):
        return None
    average = sum(sales, ZERO) / 3
    low = min(sales)
    target = min(average * Decimal('.25'), low * Decimal('.27'))
    return dict(average=money(average), low=money(low), target=money(target),
                ceiling=money(average * Decimal('.27')),
                target_low_pct=money(target / low * 100))


def parse_payroll(xml):
    root = ET.fromstring(xml.lstrip('\ufeff'))
    payroll = root.find('.//' + NOMINA_NS + 'Nomina')
    if payroll is None:
        raise ValueError('Sin complemento de nómina')
    end = date.fromisoformat(payroll.attrib['FechaFinalPago'])
    amount = Decimal(payroll.attrib.get('TotalPercepciones', '0'))
    return end.replace(day=1), amount, payroll.attrib.get('TipoNomina', ''), root


def build_personnel_plan(cutoff=None):
    today = timezone.localdate()
    last_month = today.replace(day=1) - timedelta(days=1)
    cutoff = min(cutoff or last_month, last_month)
    end = month_shift(cutoff.replace(day=1), 1)
    start = min(date(cutoff.year, 1, 1), month_shift(end, -3))
    period_count = (end.year - start.year) * 12 + end.month - start.month
    periods = [month_shift(start, n) for n in range(period_count)]
    rows = {m: dict(month=m, sales=None, ordinary=ZERO, extraordinary=ZERO,
                    imss=None, rcv=None, isn=None, fees=None, cfdis=0,
                    sources=[], errors=[]) for m in periods}
    sales = official_point_sales_rows_for_range(
        start_date=start, end_date=end - timedelta(days=1))
    for item in sales.annotate(month=TruncMonth('sale_date')).values('month').annotate(
            amount=Sum('total_amount'), branches=Count('branch_id', distinct=True)):
        month = item['month']
        if hasattr(month, 'date'):
            month = month.date()
        rows[month].update(sales=item['amount'], branches=item['branches'])

    # El expediente aplicado es el control fiscal canónico del ISN. Se carga
    # antes que los CFDI para que el comprobante fuente no se sume otra vez.
    expedientes_isn = ExpedienteISN.objects.filter(
        periodo__gte=start,
        periodo__lt=end,
        estado=ExpedienteISN.ESTADO_APLICADO,
    ).only('id', 'periodo', 'uuid', 'importe_pagado')
    periodos_isn_aplicados = set()
    for expediente in expedientes_isn:
        row = rows.get(expediente.periodo)
        if row is None:
            continue
        periodos_isn_aplicados.add(expediente.periodo)
        row['isn'] = expediente.importe_pagado
        row['sources'].append(dict(
            kind='ISN · expediente aplicado',
            id=expediente.pk,
            reference=expediente.uuid,
            amount=expediente.importe_pagado,
            reconciled=True,
        ))

    # Una sola entidad; el receptor individual nunca se exporta.
    invoices = CfdiDescargado.objects.filter(
        Q(rfc_emisor=RFC, tipo_comprobante='N', tipo_cfdi='emitido') |
        Q(rfc_receptor=RFC, tipo_cfdi='recibido', tipo_comprobante='I',
          nombre_emisor__icontains='EDENRED') |
        Q(rfc_receptor=RFC, tipo_cfdi='recibido', tipo_comprobante='I',
          rfc_emisor='GES8101015I7'),
        fecha_emision__date__gte=start, fecha_emision__date__lte=today,
        estatus__iexact='vigente', moneda='MXN',
    ).only('id', 'uuid', 'xml_raw', 'tipo_comprobante', 'tipo_cfdi',
           'nombre_emisor', 'fecha_emision', 'total', 'rfc_emisor',
           'rfc_receptor', 'estatus').order_by('id')
    unparsed = []
    for invoice in invoices.iterator():
        try:
            source_reconciled = None
            if invoice.tipo_comprobante == 'N':
                month, amount, kind, root = parse_payroll(invoice.xml_raw or '')
                if month not in rows:
                    continue
                if kind not in ('O', 'E'):
                    raise ValueError('Tipo de nómina no reconocido')
                key = 'ordinary' if kind == 'O' else 'extraordinary'
                rows[month][key] += amount
                rows[month]['cfdis'] += 1
                detail = 'Nómina ordinaria' if kind == 'O' else 'Nómina extraordinaria'
            else:
                root = ET.fromstring((invoice.xml_raw or '').lstrip('\ufeff'))
                concepts = root.findall('.//' + CFDI_NS + 'Concepto')
                if invoice.rfc_emisor == 'GES8101015I7':
                    month, amount = extraer_isn_cfdi(invoice)
                    if month not in rows or month in periodos_isn_aplicados:
                        continue
                    key, detail = 'isn', 'ISN · CFDI estatal sin expediente'
                    source_reconciled = False
                else:
                    # No sumar cargas de saldo: los vales ya están en percepciones.
                    issued = root.attrib.get('Fecha', '')[:10]
                    month = (date.fromisoformat(issued) if issued else
                             timezone.localtime(invoice.fecha_emision).date()).replace(day=1)
                    if month not in rows:
                        continue
                    if invoice.total == ZERO and root.find(
                        './/{http://www.sat.gob.mx/valesdedespensa}ValesDeDespensa'
                    ) is not None:
                        # CFDI informativo de dispersiones, con total cero; no es comisión.
                        continue
                    descriptions = [c.attrib.get('Descripcion', '').upper() for c in concepts]
                    if not descriptions or not all(
                        any(service in d for service in (
                            'COMISION', 'COMISIÓN', 'MANEJO DE CUENTA',
                            'TARJETAS REPOSICION', 'TARJETAS REPOSICIÓN',
                            'CARGO POR ENVIO', 'CARGO POR ENVÍO'))
                        for d in descriptions
                    ):
                        if descriptions and all(any(load in d for load in (
                            'VALES', 'DISPERSION', 'DISPERSIÓN', 'SALDO', 'DOTACION', 'DOTACIÓN'))
                            for d in descriptions):
                            continue
                        raise ValueError('Conceptos de Edenred sin clasificación inequívoca')
                    amount = invoice.total  # IVA incluido, sin netear créditos fiscales.
                    key, detail = 'fees', 'Servicio de vales · total con IVA'
                if month not in rows:
                    continue
                rows[month][key] = (rows[month][key] or ZERO) + amount
            source = dict(kind=detail, id=invoice.pk,
                          reference=invoice.uuid, amount=amount)
            if source_reconciled is not None:
                source['reconciled'] = source_reconciled
            rows[month]['sources'].append(source)
        except (ET.ParseError, ValueError, KeyError, TypeError, ArithmeticError):
            unparsed.append(invoice.pk)

    # Sólo control corporativo canónico: cada expediente aplicado se cuenta una vez.
    expedientes = ExpedienteCedulaIMSS.objects.filter(
        Q(
            tipo=ExpedienteCedulaIMSS.TIPO_MENSUAL,
            periodo__gte=start,
            periodo__lt=end,
        ) | Q(
            tipo=ExpedienteCedulaIMSS.TIPO_BIMESTRAL,
            periodo__gte=start,
            periodo__lte=end,
        ),
        estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
    ).prefetch_related('documentos')
    for expediente in expedientes:
        if _normalizar_registro_patronal(expediente.registro_patronal) != REGISTRO:
            continue
        if expediente.tipo == ExpedienteCedulaIMSS.TIPO_MENSUAL:
            key = 'imss'
            distribucion = [(expediente.periodo, expediente.total_patronal)]
        elif expediente.tipo == ExpedienteCedulaIMSS.TIPO_BIMESTRAL:
            key = 'rcv'
            primer_mes = month_shift(expediente.periodo, -1)
            primera_mitad = (expediente.total_patronal / 2).quantize(CENT)
            distribucion = [
                (primer_mes, primera_mitad),
                (expediente.periodo, expediente.total_patronal - primera_mitad),
            ]
        else:
            continue
        documento = next(
            (item for item in expediente.documentos.all() if item.clase == 'SUA_XLS'),
            None,
        )
        reference = documento.nombre_original if documento else f'Expediente IMSS #{expediente.pk}'
        for periodo, monto in distribucion:
            if periodo not in rows:
                continue
            row = rows[periodo]
            if row[key] is not None:
                row['errors'].append('Control patronal duplicado: ' + key)
                continue
            row[key] = monto
            row['sources'].append(dict(
                kind=key + ' · control patronal', id=expediente.pk,
                reference=reference, amount=monto,
            ))

    for row in rows.values():
        if not row['cfdis']:
            row['ordinary'] = row['extraordinary'] = None
        components = [row[k] for k in ('ordinary', 'extraordinary', 'imss', 'rcv', 'isn', 'fees')]
        row['documented'] = (sum((v for v in components if v is not None), ZERO)
                             if row['sources'] else None)
        row['coverage'] = [label for key, label in (
            ('imss', 'IMSS'), ('rcv', 'RCV/Infonavit'), ('isn', 'ISN'), ('fees', 'servicio de vales')
        ) if row[key] is not None]
        row['reconciled_components'] = (all(v is not None for v in components)
                                         and row['cfdis'] > 0 and not row['errors'] and not unparsed)
        row['ratio'] = (money(row['documented'] / row['sales'] * 100)
                        if row['sales'] and row['documented'] is not None else None)
        row['target'] = money(row['sales'] * Decimal('.25')) if row['sales'] else None
        row['ceiling'] = money(row['sales'] * Decimal('.27')) if row['sales'] else None

    recent = [month_shift(end, n) for n in (-3, -2, -1)]
    policy = budget_policy([rows.get(m, {}).get('sales') for m in recent])
    branch_rows = list(sales.values('branch_id', 'branch__name', 'branch__erp_branch_id').annotate(
        first=Min('sale_date'), last=Max('sale_date'), amount=Sum('total_amount'))
        .order_by('branch__name'))
    branch_months = list(sales.annotate(month=TruncMonth('sale_date')).values(
        'branch_id', 'month').annotate(amount=Sum('total_amount')))
    for branch in branch_rows:
        recent_sales = []
        for month in recent:
            values = [r['amount'] for r in branch_months
                      if r['branch_id'] == branch['branch_id'] and r['month'] == month]
            recent_sales.append(values[0] if len(values) == 1 else None)
        branch['recent_average'] = money(sum(recent_sales, ZERO) / 3) if all(
            value is not None for value in recent_sales) else None
    staff = list(Empleado.objects.filter(activo=True, tipo_personal=Empleado.TIPO_POLLYANA)
                 .values('departamento', 'tipo_contrato').annotate(
                     people=Count('id'), daily_salary=Sum('salario_diario')).order_by('departamento'))
    for item in staff:
        item['department_label'] = dict(Empleado.DEP_CHOICES).get(item['departamento'], item['departamento'])
        item['contract_label'] = dict(Empleado.CONTRATO_CHOICES).get(item['tipo_contrato'], item['tipo_contrato'])
    sources = [dict(month=r['month'], **s) for r in rows.values() for s in r['sources']]
    return dict(cutoff=end - timedelta(days=1), start=start, generated_at=timezone.now(),
                months=list(rows.values()), policy=policy, recent=recent, branches=branch_rows,
                staff=staff, people=sum(s['people'] for s in staff), sources=sources,
                unparsed=unparsed, rfc=RFC,
                sales_total=sum((r['sales'] or ZERO for r in rows.values()), ZERO),
                documented_total=sum((r['documented'] or ZERO for r in rows.values()), ZERO),
                branch_catalog=list(Sucursal.objects.values('codigo', 'nombre', 'activa', 'fecha_apertura')))
