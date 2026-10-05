import csv
from urllib.parse import urlencode

from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.core.paginator import Paginator
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET

from .services_conciliacion_documental import ESTADOS, leer_conciliacion_documental


def _consulta(request):
    return leer_conciliacion_documental(request.user, **{k: request.GET.get(k, '') for k in ('anio', 'mes', 'sucursal', 'estado')})


@login_required
@require_GET
@never_cache
def conciliacion_documental(request):
    try:
        datos = _consulta(request)
    except ValidationError as exc:
        return render(request, 'mantenimiento/conciliacion_documental.html', {
            'error': '; '.join(exc.messages), 'anio': request.GET.get('anio', timezone.localdate().year),
            'mes': request.GET.get('mes', ''), 'estado': request.GET.get('estado', ''),
            'estados': ESTADOS, 'meses': list(range(1, 13)),
        }, status=400)
    datos['filtros_query'] = urlencode({k: datos[k] for k in ('anio', 'mes', 'sucursal', 'estado')})
    datos['filas_total'] = len(datos['filas'])
    datos['pagina'] = Paginator(datos['filas'], 50).get_page(request.GET.get('page'))
    datos['filas'] = datos['pagina'].object_list
    return render(request, 'mantenimiento/conciliacion_documental.html', datos)


@login_required
@require_GET
@never_cache
def conciliacion_documental_csv(request):
    try:
        datos = _consulta(request)
    except ValidationError as exc:
        return JsonResponse({'ok': False, 'message': '; '.join(exc.messages)}, status=400)
    response = HttpResponse(content_type='text/csv; charset=utf-8')
    response['Content-Disposition'] = f'attachment; filename="mantenimiento-documental-{datos["anio"]}.csv"'
    response['Cache-Control'] = 'private, no-store'
    response.write('\ufeff')
    writer = csv.writer(response)
    columnas = [('tipo', 'Origen'), ('id', 'ID original'), ('titulo', 'Trabajo'), ('fecha', 'Fecha fuente'), ('origen_fecha', 'Origen de fecha'),
        ('sucursal', 'Sucursal actual'), ('ubicacion', 'Ubicación actual'), ('estado', 'Estado'),
        ('soporte', 'Soporte'), ('documentos', 'Documentos consultables'), ('duplicado_operativo', 'Duplicidad operativa')]
    if datos['puede_ver_costos']:
        columnas += [('clasificacion', 'Naturaleza del costo'), ('importe_fuente', 'Importe fuente'), ('incluido_vigente', 'Incluido en índice vigente'), ('motivo_indice', 'Criterio del índice')]
    writer.writerow([titulo for _, titulo in columnas])
    for fila in datos['filas']:
        valores = []
        for key, _ in columnas:
            valor = fila.get(key)
            texto = '' if valor is None else str(valor)
            # Títulos y referencias son texto externo, nunca fórmulas de Excel.
            externo = key in ('titulo', 'sucursal', 'ubicacion', 'soporte', 'estado')
            valores.append("'" + texto if externo and texto.lstrip().startswith(('=', '+', '-', '@')) else texto)
        writer.writerow(valores)
    return response
