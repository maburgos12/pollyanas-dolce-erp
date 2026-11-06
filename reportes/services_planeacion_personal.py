"""Planeación reproducible: ventas observadas y costo documental, sin estimar faltantes.

El presupuesto no es una autorización. No modifica RRHH ni anualiza meses parciales.
"""
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import re
from xml.etree import ElementTree as ET

from django.db.models import Count, Exists, Max, Min, OuterRef, Q, Sum
from django.db.models.functions import TruncMonth
from django.utils import timezone

from core.models import Sucursal
from rrhh.models import Empleado, NominaImportacion
from sat_client.models import CfdiDescargado
from ventas.services.sales_canonical_source import official_point_sales_rows_for_range
from .models import ExpedienteCedulaIMSS, ExpedienteISN, FactVentaDiaria
from .sales_confidence import selected_sales_facts
from .services_isn import extraer_isn_cfdi
from .services_personal_registrado import leer_personal_registrado
from .services_personal_global import cargas_mensuales_personal, resumen_personal_global
from .services_cedula_pago import leer_pago_conciliado

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


def _ajustes_conciliados(registered, fiscal_uuids):
    controls = {month: dict(amount=ZERO, count=0, errors=[]) for month in registered}
    period_ids = {p['id'] for row in registered.values() for p in row['periodos']}
    accepted = {'CARGO_GLOBAL_Y_ASIGNACION_AUTORIZADA_CONCILIADOS',
                'PAGO_CONCILIADO_CON_LISTA', 'ASIGNACION_EDENRED_Y_SALIDA_BANCARIA_CONCILIADAS'}
    seen = set()
    for source in NominaImportacion.objects.filter(
            periodo_id__in=period_ids, estatus='IMPORTADA').select_related('periodo'):
        control = controls[source.periodo.fecha_fin.replace(day=1)]
        try:
            adjustments = (source.resumen or {}).get('ajustes_por_uuid', {})
            if not adjustments:
                continue
            if not re.fullmatch('[0-9a-f]{64}', source.archivo_hash):
                raise ValueError
            amount, count, uuids = ZERO, 0, set()
            for uuid, items in adjustments.items():
                uuid = uuid.upper()
                if uuid in seen or uuid in uuids or uuid not in fiscal_uuids.get(source.periodo.fecha_fin.replace(day=1), set()):
                    raise ValueError
                if not items or uuid not in {u.upper() for u in source.resumen.get('uuids', [])}:
                    raise ValueError
                for item in items:
                    value = Decimal(item['importe'])
                    if (item['estado'] not in accepted or not item.get('soporte') or not value.is_finite()
                            or value != value.quantize(CENT)):
                        raise ValueError
                    amount += value
                    count += 1
                uuids.add(uuid)
            control['amount'] += amount
            control['count'] += count
            seen.update(uuids)
        except (ValueError, TypeError, KeyError, ArithmeticError, AttributeError):
            control['errors'].append(f'Importación de nómina #{source.pk}: ajustes sin conciliación comprobable.')
    return controls


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

    # El export histórico cubre días sin captura oficial; nunca se suman ambas fuentes del mismo local/día.
    # shortcut: histórico auditado de 2025; ampliar tras verificar identidades y linaje de otros ejercicios.
    historical_candidates = selected_sales_facts(start_date=start, end_date=end - timedelta(days=1)).filter(
        source_kind=FactVentaDiaria.SOURCE_AUTHORITATIVE, fecha__year=2025)
    for item in historical_candidates.filter(sucursal_id__isnull=True).annotate(
            month=TruncMonth('fecha')).values('month').annotate(count=Count('id')):
        rows[item['month']]['errors'].append(
            f"{item['count']} ventas históricas sin sucursal: no se incorporan sin resolver su identidad.")
    historical = historical_candidates.filter(sucursal_id__isnull=False).annotate(point_day=Exists(sales.filter(
        sale_date=OuterRef('fecha'), branch__erp_branch_id=OuterRef('sucursal_id'),
    ))).filter(point_day=False)
    historical_months = list(historical.annotate(month=TruncMonth('fecha')).values(
        'month', 'sucursal_id', 'sucursal__nombre').annotate(
            amount=Sum('venta_total'), first=Min('fecha'), last=Max('fecha')))
    for item in historical_months:
        row = rows[item['month']]
        row['sales'] = (row['sales'] or ZERO) + item['amount']

    # El expediente aplicado es el control fiscal canónico del ISN. Se carga
    # antes que los CFDI para que el comprobante fuente no se sume otra vez.
    expedientes_isn = ExpedienteISN.objects.filter(
        periodo__gte=start,
        periodo__lt=end,
        estado=ExpedienteISN.ESTADO_APLICADO,
    ).select_related('cfdi').only(
        'id', 'periodo', 'uuid', 'importe_pagado', 'metadata', 'cfdi__estatus'
    )
    periodos_isn_aplicados = set()
    for expediente in expedientes_isn:
        row = rows.get(expediente.periodo)
        if row is None:
            continue
        if (expediente.cfdi.estatus or '').strip().upper() != 'VIGENTE':
            row['errors'].append(
                f'El CFDI de ISN ya no esta vigente: {expediente.uuid}.'
            )
            continue
        periodos_isn_aplicados.add(expediente.periodo)
        row['isn'] = expediente.importe_pagado
        diferencia_aceptada = bool(
            (expediente.metadata or {}).get('diferencia_isn_aceptada')
        )
        if diferencia_aceptada:
            row['errors'].append(
                'ISN con diferencia aceptada contra la base reconstruida: '
                f"{(expediente.metadata or {}).get('diferencia_isn', 'N/D')}."
            )
        row['sources'].append(dict(
            kind=(
                'ISN · expediente aplicado con diferencia'
                if diferencia_aceptada
                else 'ISN · expediente aplicado'
            ),
            id=expediente.pk,
            reference=expediente.uuid,
            amount=expediente.importe_pagado,
            reconciled=not diferencia_aceptada,
        ))

    # La discrepancia histórica nunca convierte el comprobante en costo conciliado.
    for expediente in ExpedienteISN.objects.filter(periodo__gte=start, periodo__lt=end,
            periodo__year=2025, estado=ExpedienteISN.ESTADO_DISCREPANCIA,
            metadata__metodo='EMPRESA_HISTORICA_2025_V1'):
        rows[expediente.periodo]['errors'].append(
            f"ISN empresa: expediente #{expediente.pk} con diferencia de "
            f"{expediente.metadata.get('diferencia_isn', 'N/D')} contra nómina histórica; pendiente.")

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
    unparsed, excluded_fiscal = [], []
    fiscal_uuids = {m: set() for m in periods}
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
                fiscal_uuids[month].add(invoice.uuid.upper())
                detail = 'Nómina ordinaria' if kind == 'O' else 'Nómina extraordinaria'
            else:
                root = ET.fromstring((invoice.xml_raw or '').lstrip('\ufeff'))
                concepts = root.findall('.//' + CFDI_NS + 'Concepto')
                descriptions = [re.sub('[^A-Z]', '', c.attrib.get('Descripcion', '').upper())
                                for c in concepts]
                if invoice.total == ZERO and descriptions and all(d == 'SINMOVIMIENTOS' for d in descriptions):
                    issued = timezone.localtime(invoice.fecha_emision).date()
                    if start <= issued < end:
                        excluded_fiscal.append(dict(id=invoice.pk,
                            reason='Documento sin movimientos e importe cero; no genera costo de personal.'))
                    continue
                if invoice.rfc_emisor == 'GES8101015I7':
                    try:
                        month, amount = extraer_isn_cfdi(invoice)
                    except ValueError:
                        identifica_nomina = any('NOMINA' in (
                            c.attrib.get('NoIdentificacion', '') + ' ' + c.attrib.get('Descripcion', '')
                            ).upper().replace('Ó', 'O') for c in concepts)
                        ajeno = not identifica_nomina and descriptions and all(any(word in d for word in (
                            'PEAJE', 'PLACA', 'CALCOMANIA', 'PROEDUC', 'CRUZROJA', 'CUOTADIF'))
                            for d in descriptions)
                        if not ajeno:
                            raise
                        issued = timezone.localtime(invoice.fecha_emision).date()
                        if start <= issued < end:
                            excluded_fiscal.append(dict(id=invoice.pk,
                                reason='Peaje, trámite vehicular o documento sin movimientos; no es ISN.'))
                        continue
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
                            'TARJETAS TITULAR',
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
        try:
            pago = leer_pago_conciliado(expediente)
        except ValueError as exc:
            for row in rows.values():
                row['errors'].append(str(exc))
            continue
        costo = pago['gasto_empresa'] if pago else expediente.total_patronal
        if expediente.tipo == ExpedienteCedulaIMSS.TIPO_MENSUAL:
            key = 'imss'
            distribucion = [(expediente.periodo, costo)]
        elif expediente.tipo == ExpedienteCedulaIMSS.TIPO_BIMESTRAL:
            key = 'rcv'
            primer_mes = month_shift(expediente.periodo, -1)
            primera_mitad = (costo / 2).quantize(CENT)
            distribucion = [
                (primer_mes, primera_mitad),
                (expediente.periodo, costo - primera_mitad),
            ]
        else:
            continue
        documento = next(
            (item for item in expediente.documentos.all() if item.clase in ('SUA_XLS', 'SUA_PDF')),
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
            if pago:
                row[key + '_label'] = ('IMSS' if key == 'imss' else 'RCV / Infonavit') + ' · gasto de empresa conciliado'
            row['sources'].append(dict(
                kind=key + (' · gasto de empresa conciliado con pago' if pago else ' · control patronal'), id=expediente.pk,
                reference=reference, amount=monto,
            ))

    registered = leer_personal_registrado(start, end)
    adjustments = _ajustes_conciliados(registered, fiscal_uuids)
    for row in rows.values():
        row['registered'] = registered.get(row['month'])
        row['cargas_nomina'] = cargas_mensuales_personal(row)
        control = row['cargas_nomina']
        if control['error']:
            row['errors'].append(control['error'])
        if row['registered']:
            cargas_cubiertas = row['registered']['cargas_cubiertas'] and not control['error']
            for group in row['registered']['grupos']:
                group['cargas'] = (group['cargas_sin_isn'] + sum(
                    (control['reparto'][p] for p in group['personas_cargas']), ZERO)
                    if cargas_cubiertas else None)
                group['costo_personal'] = (group['total'] + group['cargas'] + group['ajuste_pago_real']
                                           if cargas_cubiertas else None)
            for branch in row['registered']['sucursales_ventas']:
                branch['cargas'] = sum((g['cargas'] for g in row['registered']['grupos']
                    if g['sucursal_id'] == branch['sucursal_id'] and g['destino'] == 'tienda'), ZERO) if cargas_cubiertas else None
                branch['costo_personal'] = (branch['total'] + branch['cargas'] + branch['ajuste_pago_real']
                                            if cargas_cubiertas else None)
        for key in control['usados']:
            row[key] = control['valores'][key]
            row['sources'].append(dict(
                kind=('ISN · devengado calculado en ERP' if key == 'isn'
                      else key.upper() + ' · cargas mensuales de nómina'),
                id=max(p['id'] for p in row['registered']['periodos']),
                reference=f'Conceptos mensuales M{row["month"]:%Y%m}', amount=row[key],
            ))
        if not row['cfdis']:
            row['ordinary'] = row['extraordinary'] = None
        components = [row[k] for k in ('ordinary', 'extraordinary', 'imss', 'rcv', 'isn', 'fees')]
        row['documented'] = (sum((v for v in components if v is not None), ZERO)
                             if row['sources'] else None)
        row['coverage'] = [label for key, label in (
            ('imss', 'IMSS'), ('rcv', 'RCV/Infonavit'), ('isn', 'ISN'), ('fees', 'servicio de vales')
        ) if row[key] is not None]
        row['reconciled_components'] = (all(v is not None for v in components)
                                         and row['cfdis'] > 0 and not row['errors'] and not unparsed
                                         and not any(
                                             source.get('reconciled') is False
                                             for source in row['sources']
                                         ))
        row['ratio'] = (money(row['documented'] / row['sales'] * 100)
                        if row['sales'] and row['documented'] is not None else None)
        row['target'] = money(row['sales'] * Decimal('.25')) if row['sales'] else None
        row['ceiling'] = money(row['sales'] * Decimal('.27')) if row['sales'] else None
        row['payroll_difference'] = (row['ordinary'] + row['extraordinary'] - row['registered']['total']
            if row['cfdis'] and row['registered'] else None)
        control = adjustments.get(row['month'], dict(amount=ZERO, count=0, errors=[]))
        row['payroll_adjustments'] = control['amount']
        row['payroll_adjustment_count'] = control['count']
        row['payroll_unexplained'] = (row['payroll_difference'] + control['amount']
                                     if row['payroll_difference'] is not None else None)
        row['payroll_reconciled'] = row['payroll_unexplained'] == ZERO and not control['errors']
        row['errors'].extend(control['errors'])
        if control['errors']:
            row['reconciled_components'] = False

    recent = [month_shift(end, n) for n in (-3, -2, -1)]
    policy = budget_policy([rows.get(m, {}).get('sales') for m in recent])
    branch_rows = list(sales.values('branch_id', 'branch__name', 'branch__erp_branch_id').annotate(
        first=Min('sale_date'), last=Max('sale_date'), amount=Sum('total_amount'))
        .order_by('branch__name'))
    branch_months = list(sales.annotate(month=TruncMonth('sale_date')).values(
        'branch_id', 'month').annotate(amount=Sum('total_amount')))
    for item in branch_rows + branch_months:
        item['branch_key'] = str(item['branch_id'])
    historical_branches = {}
    for item in historical_months:
        key = f"ERP:{item['sucursal_id']}"
        branch_months.append(dict(branch_key=key, month=item['month'], amount=item['amount']))
        branch = historical_branches.setdefault(key, dict(branch_key=key, branch_id=None,
            branch__erp_branch_id=item['sucursal_id'], branch__name=item['sucursal__nombre'],
            first=item['first'], last=item['last'], amount=ZERO))
        branch['first'], branch['last'] = min(branch['first'], item['first']), max(branch['last'], item['last'])
        branch['amount'] += item['amount']
    branch_rows.extend(historical_branches.values())
    branch_ids = {b['branch_key']: b['branch__erp_branch_id'] for b in branch_rows}
    branch_sales = {}
    month_branches = {m: set() for m in periods}
    for item in branch_months:
        sid = branch_ids[item['branch_key']]
        month_branches[item['month']].add(('ERP', sid) if sid is not None else ('POINT', item['branch_key']))
        key = (item['month'], sid)
        branch_sales[key] = branch_sales.get(key, ZERO) + item['amount']
    for month, branches in month_branches.items():
        rows[month]['branches'] = len(branches) if branches else None
    for month, registered_row in registered.items():
        unclassified = {g['sucursal_id'] for g in registered_row['grupos']
                        if g['destino'] in ('sin_clasificar', 'ventas_pendiente')}
        for group in registered_row['grupos']:
            group['ventas'] = (branch_sales.get((month, group['sucursal_id']))
                               if group['sucursal_id'] is not None else None)
            group['porcentaje'] = (money(group['total'] / group['ventas'] * 100)
                if group['ventas'] and group['destino'] == 'tienda'
                and group['sucursal_id'] not in unclassified and registered_row['mes_cubierto'] else None)
    for month, registered_row in registered.items():
        for branch in registered_row['sucursales_ventas']:
            branch['ventas'] = branch_sales.get((month, branch['sucursal_id'])) if branch['sucursal_id'] else None
            branch['porcentaje'] = (money(branch['total'] / branch['ventas'] * 100)
                if branch['ventas'] and registered_row['mes_cubierto'] else None)
            branch['costo_porcentaje'] = (money(branch['costo_personal'] / branch['ventas'] * 100)
                if branch['ventas'] and branch['costo_personal'] is not None else None)
    for branch in branch_rows:
        recent_sales = []
        for month in recent:
            values = [r['amount'] for r in branch_months
                      if r['branch_key'] == branch['branch_key'] and r['month'] == month]
            recent_sales.append(values[0] if len(values) == 1 else None)
        branch['recent_average'] = money(sum(recent_sales, ZERO) / 3) if all(
            value is not None for value in recent_sales) else None
    staff = list(Empleado.objects.filter(activo=True, tipo_personal=Empleado.TIPO_POLLYANA)
                 .values('departamento', 'tipo_contrato').annotate(
                     people=Count('id'), daily_salary=Sum('salario_diario')).order_by('departamento'))
    for item in staff:
        item['department_label'] = dict(Empleado.DEP_CHOICES).get(item['departamento'], item['departamento'])
        item['contract_label'] = dict(Empleado.CONTRATO_CHOICES).get(item['tipo_contrato'], item['tipo_contrato'])
    selected = rows[month_shift(end, -1)]
    selected['global'] = resumen_personal_global(selected)
    sources = [dict(month=r['month'], **s) for r in rows.values() for s in r['sources']]
    return dict(cutoff=end - timedelta(days=1), start=start, generated_at=timezone.now(),
                months=list(rows.values()), selected=rows[month_shift(end, -1)],
                policy=policy, recent=recent, branches=branch_rows,
                staff=staff, people=sum(s['people'] for s in staff), sources=sources,
                unparsed=unparsed, excluded_fiscal=excluded_fiscal, rfc=RFC,
                sales_total=sum((r['sales'] or ZERO for r in rows.values()), ZERO),
                documented_total=sum((r['documented'] or ZERO for r in rows.values()), ZERO),
                branch_catalog=list(Sucursal.objects.values('codigo', 'nombre', 'activa', 'fecha_apertura')))
