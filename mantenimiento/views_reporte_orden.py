from uuid import uuid4
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse
from django.shortcuts import render
from django.template.loader import render_to_string
from django.views.decorators.http import require_http_methods
from rest_framework.exceptions import APIException
from mantenimiento.services_access import can_access_mantenimiento
from mantenimiento.services_reporte_orden import contexto_orden_desde_reporte, crear_orden_desde_reporte


@login_required
@require_http_methods(['GET', 'POST'])
def orden_desde_reporte(request, pk):
    if not can_access_mantenimiento(request.user):
        raise PermissionDenied
    context = contexto_orden_desde_reporte(request.user, pk)
    context['clave_captura'] = request.POST.get('clave_captura') or str(uuid4())
    context['datos'] = request.POST if request.method == 'POST' else context
    code = 200
    if request.method == 'POST':
        try:
            orden, replay = crear_orden_desde_reporte(usuario=request.user, reporte_id=pk, datos=request.POST)
            context = contexto_orden_desde_reporte(request.user, pk)
            context['clave_captura'] = str(uuid4())
            context['datos'] = context.copy()
            context['resultado'] = f'Orden {orden.folio} vinculada. El reporte conserva sus estados, importes y evidencias.'
            code = 200 if replay else 201
        except APIException as error:
            context['error'] = str(error.detail)
            code = error.status_code
    async_request = 'application/json' in request.headers.get('Accept', '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    if async_request:
        html = render_to_string('mantenimiento/reporte_orden_form.html', context, request=request)
        if request.method == 'GET':
            return JsonResponse({'html':html})
        return JsonResponse({'ok':not context.get('error'), 'target':f'#reporteOrdenResultado-{pk}', 'html':html,
            'toast':{'type':'error' if context.get('error') else 'success', 'message':context.get('error') or context.get('resultado'), 'persistent':bool(context.get('error'))}}, status=code)
    return render(request, 'mantenimiento/reporte_orden.html', context, status=code)
