import csv
from datetime import date
from io import StringIO

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from core.access import can_view_reportes
from .services_planeacion_personal import build_personnel_plan


@login_required
@require_GET
def planeacion_personal(request):
    if not can_view_reportes(request.user):
        raise PermissionDenied
    cutoff = None
    if request.GET.get('mes'):
        try:
            cutoff = date.fromisoformat(request.GET['mes'] + '-01')
            if cutoff.year < 2000:
                raise ValueError
        except ValueError:
            return HttpResponseBadRequest('El mes debe tener formato AAAA-MM.')
    data = build_personnel_plan(cutoff)
    if request.GET.get('formato') == 'json':
        return JsonResponse(data)
    if request.GET.get('formato') == 'csv':
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Periodo', 'Ventas Point con impuestos', 'Objetivo 25%', 'Limite 27%',
                         'Percepciones ordinarias', 'Percepciones extraordinarias',
                         'IMSS patronal', 'RCV Infonavit patronal', 'ISN',
                         'Servicio vales con IVA', 'Suma documentada', 'Familias documentadas',
                         'Lista de raya con despensa', 'Diferencia CFDI menos lista',
                         'Periodos cubren mes', 'Pendientes lista de raya'])
        for row in data['months']:
            writer.writerow([row['month']] + [row[k] for k in (
                'sales', 'target', 'ceiling', 'ordinary', 'extraordinary', 'imss',
                'rcv', 'isn', 'fees', 'documented')] + [', '.join(row['coverage']),
                (row['registered'] or {}).get('total'), row['payroll_difference'],
                (row['registered'] or {}).get('mes_cubierto'),
                '; '.join((row['registered'] or {}).get('pendientes', []))])
        writer.writerow([])
        writer.writerow(['Periodo', 'Sucursal histórica', 'Destino según función actual',
                         'Personas', 'Percepciones', 'Despensa adicional', 'Subtotal sin cargas',
                         'Ventas con impuestos', '% tienda sin cargas'])
        for row in data['months']:
            for group in (row['registered'] or {}).get('grupos', []):
                writer.writerow([row['month']] + [group[k] for k in ('sucursal', 'destino_label',
                    'personas', 'percepciones', 'despensa', 'total', 'ventas', 'porcentaje')])
        writer.writerow([])
        writer.writerow(['Periodo', 'Ventas por sucursal', 'Personas en nómina', 'Venta con impuestos',
                         'Percepciones', 'Despensa adicional', 'Subtotal registrado', '% subtotal sobre venta',
                         'Puestos pendientes'])
        for row in data['months']:
            for branch in (row['registered'] or {}).get('sucursales_ventas', []):
                writer.writerow([row['month']] + [branch[k] for k in ('sucursal', 'personas', 'ventas',
                    'percepciones', 'despensa', 'total', 'porcentaje', 'puestos_pendientes')])
        writer.writerow([])
        writer.writerow(['Periodo', 'Fuente', 'ID ERP', 'Referencia', 'Importe'])
        for source in data['sources']:
            writer.writerow([source[k] for k in ('month', 'kind', 'id', 'reference', 'amount')])
        global_row = data['selected']['global']
        writer.writerow([])
        writer.writerow(['Global por departamento', 'Personas', 'Percepciones', 'Vales adicionales',
                         'Nomina registrada', 'Cargas patronales e ISN', 'Personal con cargas sin provisiones', '% ventas totales'])
        for group in global_row['departamentos']:
            writer.writerow([group[k] for k in ('destino_label', 'personas', 'percepciones',
                                               'despensa', 'total', 'cargas', 'costo_personal', 'costo_porcentaje')])
        writer.writerow(['Total nomina y vales', global_row['personas'], '', '',
                         global_row['nomina'], global_row['cargas_total'],
                         global_row['costo_departamentos'], global_row['costo_departamentos_porcentaje']])
        writer.writerow([])
        writer.writerow(['Gastos operativos por area', 'Importe mensual', '% ventas totales', 'Registros parciales'])
        for area in global_row['gastos_areas']:
            writer.writerow([area[k] for k in ('nombre', 'total', 'porcentaje', 'parciales')])
        writer.writerow([])
        writer.writerow(['Resumen final', 'Importe del mes', '% ventas totales'])
        writer.writerow(['Ventas totales con impuestos', global_row['ventas']])
        for component in global_row['componentes']:
            writer.writerow([component[k] for k in ('nombre', 'monto', 'porcentaje')])
        writer.writerow(['Subtotal personal con cargas identificadas', global_row['personal_subtotal'],
                         global_row['personal_porcentaje']])
        writer.writerow(['Subtotal personal y gastos identificados', global_row['subtotal'], global_row['porcentaje']])
        writer.writerow(['Costo integral cerrado', 'Pendiente de conciliacion'])
        writer.writerow(['Alcance', 'No incluye materia prima, empaque de fabricacion ni inversiones. '
                         'No sumar subtotales entre si ni CFDI a listas. Vacio significa pendiente, no cero.'])
        writer.writerow(['Diferencia CFDI menos lista, no sumable', data['selected']['payroll_difference']])
        writer.writerow(['Periodos cubren mes', (data['selected']['registered'] or {}).get('mes_cubierto')])
        writer.writerow(['Pendientes lista', '; '.join((data['selected']['registered'] or {}).get('pendientes', []))])
        writer.writerow([])
        writer.writerow(['Fuentes de gastos / pendientes', 'ID', 'Clave de asignacion', 'Area',
                         'Concepto', 'Importe mensual', 'Estado', 'Detalle', 'Comprobante vinculado'])
        for expense in global_row['fuentes_gastos'] + global_row['pendientes_gastos']:
            writer.writerow([expense[k] for k in ('origen', 'registro_id', 'clave', 'area', 'concepto',
                                                  'monto_mensual', 'estado', 'detalle', 'referencia_soporte')])
        response = HttpResponse('\ufeff' + output.getvalue(), content_type='text/csv; charset=utf-8')
        response['Content-Disposition'] = 'attachment; filename="planeacion_personal.csv"'
        return response
    from .views import _reportes_module_tabs
    data['module_tabs'] = _reportes_module_tabs('planeacion_personal')
    return render(request, 'reportes/planeacion_personal.html', data)
