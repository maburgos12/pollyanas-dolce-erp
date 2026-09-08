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
from pos_bridge.models import PointDailySale
from rrhh.models import Empleado
from sat_client.models import CfdiDescargado
from .models import LineaPresupuestoMensual

ZERO = Decimal('0')
CENT = Decimal('0.01')
# Identidades del empleador y del registro presentes en los comprobantes fuente.
RFC = 'GEF211230KR2'
REGISTRO = 'E5240157100'
NOMINA_NS = '{http://www.sat.gob.mx/nomina12}'
CFDI_NS = '{http://www.sat.gob.mx/cfd/4}'


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
    sales = PointDailySale.objects.filter(sale_date__gte=start, sale_date__lt=end)
    for item in sales.annotate(month=TruncMonth('sale_date')).values('month').annotate(
            amount=Sum('total_amount'), branches=Count('branch_id', distinct=True)):
        month = item['month']
        if hasattr(month, 'date'):
            month = month.date()
        rows[month].update(sales=item['amount'], branches=item['branches'])

    # Una sola entidad; el receptor individual nunca se exporta.
    invoices = CfdiDescargado.objects.filter(
        Q(rfc_emisor=RFC, tipo_comprobante='N', tipo_cfdi='emitido') |
        Q(rfc_receptor=RFC, tipo_cfdi='recibido', tipo_comprobante='I',
          nombre_emisor__icontains='EDENRED') |
        Q(rfc_receptor=RFC, tipo_cfdi='recibido', tipo_comprobante='I',
          rfc_emisor='GES8101015I7'),
        fecha_emision__date__gte=start, fecha_emision__date__lte=today,
        estatus__iexact='vigente', moneda='MXN',
    ).only('id', 'uuid', 'xml_raw', 'tipo_comprobante', 'nombre_emisor',
           'fecha_emision', 'total', 'rfc_emisor').order_by('id')
    unparsed = []
    for invoice in invoices.iterator():
        try:
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
                    matching = [c for c in concepts if 'nomina' in
                                c.attrib.get('Descripcion', '').lower()]
                    if not matching:
                        continue
                    matches = {re.match(r'(\d{4})(\d{2})\b', c.attrib.get('NoIdentificacion', ''))
                               for c in matching}
                    months = {date(int(m[1]), int(m[2]), 1) for m in matches if m}
                    if len(months) != 1:
                        raise ValueError('Periodo fiscal no inequívoco')
                    month = months.pop()
                    amount = sum((Decimal(c.attrib['Importe']) for c in matching), ZERO)
                    key, detail = 'isn', 'Impuesto sobre nómina · periodo del concepto'
                else:
                    # No sumar cargas de saldo: los vales ya están en percepciones.
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
                    issued = root.attrib.get('Fecha', '')[:10]
                    month = (date.fromisoformat(issued) if issued else
                             timezone.localtime(invoice.fecha_emision).date()).replace(day=1)
                    amount = invoice.total  # IVA incluido, sin netear créditos fiscales.
                    key, detail = 'fees', 'Servicio de vales · total con IVA'
                if month not in rows:
                    continue
                rows[month][key] = (rows[month][key] or ZERO) + amount
            rows[month]['sources'].append(dict(kind=detail, id=invoice.pk,
                                               reference=invoice.uuid, amount=amount))
        except (ET.ParseError, ValueError, KeyError, TypeError, ArithmeticError):
            unparsed.append(invoice.pk)

    # Sólo control corporativo: las distribuciones departamentales NO se vuelven a sumar.
    contributions = LineaPresupuestoMensual.objects.filter(
        periodo__gte=start, periodo__lt=end, version='ORIGINAL',
        fuente_real='AUTO:SIPARE', rubro__activo=True, rubro__area__codigo='nomina',
    ).select_related('rubro')
    for line in contributions:
        meta = line.metadata.get('cedula_imss', {})
        document = line.metadata.get('cedula_imss_documento', {})
        registration = meta.get('registro_patronal') or document.get('registro_patronal', '')
        if re.sub(r'\W', '', registration) != REGISTRO:
            continue
        concept = line.rubro.concepto.strip().casefold()
        key = 'imss' if concept == 'imss' else 'rcv' if concept in ('infonavit', 'infonavit rcv') else None
        if not key or line.monto_real is None:
            continue
        row = rows[line.periodo]
        if row[key] is not None:
            row['errors'].append('Control patronal duplicado: ' + concept)
        else:
            row[key] = line.monto_real
        reference = document.get('archivo') or ('AUTO:SIPARE / ' + REGISTRO)
        row['sources'].append(dict(kind=concept + ' · control patronal', id=line.pk,
                                  reference=reference, amount=line.monto_real))

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
