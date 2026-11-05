"""Consolidado mensual de fuentes existentes, sin recalcular ni prorratear gastos."""
from decimal import Decimal, ROUND_HALF_UP

from .models import AreaPresupuesto
from .services_rentabilidad_gastos import leer_gastos_mensuales
from .services_isn import bases_gravadas_empleados, calcular_isn_sinaloa, prorratear_isn

ZERO = Decimal('0')


def cargas_mensuales_personal(selected):
    registered = selected['registered']
    cargas = (registered or {}).get('componentes_cargas', {})
    cargas_cubiertas = (registered or {}).get('cargas_cubiertas', False)
    patronal = {'imss': None, 'rcv': None, 'isn': None}
    cargas_error = ''
    isn_calculado = False
    if cargas_cubiertas:
        patronal['imss'] = sum((cargas[c] for c in ('AU', 'AW', 'AZ', 'BC')), ZERO)
        patronal['rcv'] = sum((cargas[c] for c in ('AV', 'AX', 'BB')), ZERO)
        try:
            bases = bases_gravadas_empleados(selected['month'])
            if set(bases) != set(registered['destinos_cargas']):
                raise ValueError('La base de ISN no cubre las mismas personas que las cargas de nómina.')
            patronal['isn'] = selected['isn']
            if patronal['isn'] is None:
                patronal['isn'] = calcular_isn_sinaloa(sum(bases.values(), ZERO))
                isn_calculado = True
            reparto = ({persona: ZERO for persona in bases}
                       if not sum(bases.values(), ZERO) and not patronal['isn']
                       else prorratear_isn(bases, patronal['isn']))
            for key in ('imss', 'rcv'):
                if selected[key] is not None and selected[key] != patronal[key]:
                    raise ValueError('Las cargas de nómina no concilian con el control patronal aplicado.')
        except ValueError as exc:
            cargas_error = str(exc)
    return dict(valores=patronal, error=cargas_error, isn_calculado=isn_calculado,
                reparto=reparto if not cargas_error and cargas_cubiertas else {},
                usados=[k for k, v in patronal.items() if selected[k] is None and v is not None])


def resumen_personal_global(selected):
    registered = selected['registered']
    sales = selected['sales']

    def porcentaje(amount):
        return ((amount / sales * 100).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
                if amount is not None and sales is not None and sales > 0 else None)

    departments = [dict(g, porcentaje=porcentaje(g['total']))
                   for g in (registered or {}).get('departamentos', [])]
    cargas = (registered or {}).get('componentes_cargas', {})
    cargas_cubiertas = (registered or {}).get('cargas_cubiertas', False)
    control_cargas = selected.get('cargas_nomina') or cargas_mensuales_personal(selected)
    patronal = control_cargas['valores']
    cargas_error = control_cargas['error']
    isn_calculado = control_cargas['isn_calculado']
    if cargas_cubiertas and not cargas_error:
        reparto = control_cargas['reparto']
        for department in departments:
            department['isn'] = sum((monto for persona, monto in reparto.items()
                if registered['destinos_cargas'][persona] == department['destino']), ZERO)
            department['cargas'] = department['cargas_sin_isn'] + department['isn']
            department['costo_personal'] = department['total'] + department['cargas']
            department['costo_porcentaje'] = porcentaje(department['costo_personal'])
    for department in departments:
        for key in ('cargas', 'costo_personal', 'costo_porcentaje'):
            department.setdefault(key, None)
    expenses = leer_gastos_mensuales(selected['month'], usar_reparto_consolidado=True)
    # Un importe con evidencia incompleta no forma parte del total respaldado.
    incompletos = [dict(f, monto_mensual=None) for f in expenses['filas'] if f['estado'] != 'COMPLETO']
    expenses['pendientes'].extend(incompletos)
    expenses['filas'] = [f for f in expenses['filas'] if f['estado'] == 'COMPLETO']
    labels = dict(AreaPresupuesto.objects.values_list('codigo', 'nombre'))
    areas = {}
    for expense in expenses['filas']:
        area = areas.setdefault(expense['area'], dict(
            codigo=expense['area'], nombre=labels.get(expense['area'], expense['area']) or 'Área pendiente',
            total=ZERO, registros=0, parciales=0))
        area['total'] += expense['monto_mensual']
        area['registros'] += 1
        area['parciales'] += expense['estado'] != 'COMPLETO'
    for area in areas.values():
        area['porcentaje'] = porcentaje(area['total'])
    expense_total = sum((g['total'] for g in areas.values()), ZERO) if areas else None
    components = [dict(clave=key, nombre=label, monto=(registered or {}).get(key))
                  for key, label in (('percepciones', 'Percepciones de las listas de raya'),
                                     ('despensa', 'Vales adicionales, no incluidos en percepciones'))]
    for key, label in (('imss', 'IMSS patronal'), ('rcv', 'RCV / Infonavit patronal'),
                       ('isn', 'Impuesto sobre nómina'), ('fees', 'Servicio de vales, con IVA')):
        amount = selected[key]
        if amount is None and patronal.get(key) is not None:
            amount = patronal[key]
        if key in control_cargas['usados']:
            label += ' · devengado calculado en ERP' if key == 'isn' else ' · registrado en nómina'
        components.append(dict(clave=key, nombre=label, monto=amount))
    if cargas_cubiertas and cargas.get('BD'):
        components.append(dict(clave='otras_cargas', nombre='Otras obligaciones registradas en nómina', monto=cargas['BD']))
    components += [dict(clave='provisiones', nombre='Prestaciones devengadas no pagadas · por conciliar', monto=None)]
    labor_total = (sum((c['monto'] for c in components if c['monto'] is not None), ZERO)
                   if registered else None)
    components.append(dict(clave='gastos', nombre='Gastos operativos identificados', monto=expense_total))
    for component in components:
        component['porcentaje'] = porcentaje(component['monto'])
    # Los CFDI y la diferencia fiscal son contraste, nunca otra percepción sumable.
    total = labor_total + (expense_total or ZERO) if labor_total is not None else None
    # Sólo se exporta la trazabilidad necesaria; no pagos, cuentas ni archivos privados.
    fields = ('origen', 'registro_id', 'clave', 'area', 'concepto', 'monto_mensual', 'estado', 'detalle', 'referencia_soporte')
    costo_departamentos = (sum((g['costo_personal'] for g in departments), ZERO)
                           if departments and all(g['costo_personal'] is not None for g in departments) else None)
    return dict(
        ventas=sales, departamentos=departments, personas=(registered or {}).get('personas'),
        cargas_error=cargas_error, isn_calculado=isn_calculado,
        costo_departamentos=costo_departamentos, costo_departamentos_porcentaje=porcentaje(costo_departamentos),
        cargas_total=costo_departamentos - registered['total'] if costo_departamentos is not None else None,
        nomina=(registered or {}).get('total'), nomina_porcentaje=porcentaje((registered or {}).get('total')),
        gastos_areas=sorted(areas.values(), key=lambda g: g['nombre']), gastos_total=expense_total,
        componentes=components, personal_subtotal=labor_total, personal_porcentaje=porcentaje(labor_total),
        subtotal=total, porcentaje=porcentaje(total), costo_integral=None,
        fuentes_gastos=[{k: f.get(k, '') for k in fields} for f in expenses['filas']],
        pendientes_gastos=[{k: f.get(k, '') for k in fields} for f in expenses['pendientes']],
        gastos_parciales=len(incompletos),
    )
