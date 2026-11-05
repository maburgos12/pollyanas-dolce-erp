"""Consolidado mensual de fuentes existentes, sin recalcular ni prorratear gastos."""
from decimal import Decimal, ROUND_HALF_UP

from .models import AreaPresupuesto
from .services_rentabilidad_gastos import leer_gastos_mensuales

ZERO = Decimal('0')


def resumen_personal_global(selected):
    registered = selected['registered']
    sales = selected['sales']

    def porcentaje(amount):
        return ((amount / sales * 100).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
                if amount is not None and sales is not None and sales > 0 else None)

    departments = [dict(g, porcentaje=porcentaje(g['total']))
                   for g in (registered or {}).get('departamentos', [])]
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
    components += [dict(clave=key, nombre=selected.get(key + '_label', label), monto=selected[key]) for key, label in (
        ('imss', 'IMSS patronal'), ('rcv', 'RCV / Infonavit patronal'),
        ('isn', 'Impuesto sobre nómina'), ('fees', 'Servicio de vales, con IVA'))]
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
    return dict(
        ventas=sales, departamentos=departments, personas=(registered or {}).get('personas'),
        nomina=(registered or {}).get('total'), nomina_porcentaje=porcentaje((registered or {}).get('total')),
        gastos_areas=sorted(areas.values(), key=lambda g: g['nombre']), gastos_total=expense_total,
        componentes=components, personal_subtotal=labor_total, personal_porcentaje=porcentaje(labor_total),
        subtotal=total, porcentaje=porcentaje(total), costo_integral=None,
        fuentes_gastos=[{k: f.get(k, '') for k in fields} for f in expenses['filas']],
        pendientes_gastos=[{k: f.get(k, '') for k in fields} for f in expenses['pendientes']],
        gastos_parciales=len(incompletos),
    )
