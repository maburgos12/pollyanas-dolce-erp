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
                         'Servicio vales con IVA', 'Suma documentada', 'Familias documentadas'])
        for row in data['months']:
            writer.writerow([row['month']] + [row[k] for k in (
                'sales', 'target', 'ceiling', 'ordinary', 'extraordinary', 'imss',
                'rcv', 'isn', 'fees', 'documented')] + [', '.join(row['coverage'])])
        writer.writerow([])
        writer.writerow(['Periodo', 'Fuente', 'ID ERP', 'Referencia', 'Importe'])
        for source in data['sources']:
            writer.writerow([source[k] for k in ('month', 'kind', 'id', 'reference', 'amount')])
        response = HttpResponse('\ufeff' + output.getvalue(), content_type='text/csv; charset=utf-8')
        response['Content-Disposition'] = 'attachment; filename="planeacion_personal.csv"'
        return response
    from .views import _reportes_module_tabs
    data['module_tabs'] = _reportes_module_tabs('planeacion_personal')
    return render(request, 'reportes/planeacion_personal.html', data)
