import csv
import json
import re
from datetime import timedelta
from uuid import UUID, uuid4

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator
from django.db import connection, transaction
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from core.access import can_manage_module, can_view_reportes
from inventario.models import InsumoAuditReview, InsumoAuditSnapshot
from inventario.services_documentary_audit import month


def _access(user):
    if not can_view_reportes(user):
        raise PermissionDenied('No tienes acceso a Reportes.')


def _csv_cell(value):
    text = '' if value is None else str(value)
    if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        return text
    return "'" + text if text.lstrip().startswith(('=', '+', '-', '@')) else text


@login_required
@require_GET
def dashboard(request):
    _access(request.user)
    try:
        period = month(request.GET.get('mes') or timezone.localdate().strftime('%Y-%m'))
    except ValueError as exc:
        return HttpResponse(str(exc), status=400)
    latest = InsumoAuditSnapshot.objects.filter(periodo=period).first()
    snapshot = latest
    version = request.GET.get('revision', '')
    if version:
        if not version.isdigit():
            return HttpResponse('Revisión no válida.', status=400)
        snapshot = get_object_or_404(InsumoAuditSnapshot, pk=version, periodo=period)
    previous = InsumoAuditSnapshot.objects.filter(periodo=period, pk__lt=snapshot.pk).first() if snapshot else None
    location = request.GET.get('ubicacion', '')
    if location not in ('', 'ALMACEN', 'CEDIS'):
        return HttpResponse('Ubicación no válida.', status=400)
    search = request.GET.get('q', '').strip()[:100]
    rows = snapshot.payload['rows'] if snapshot else []
    rows = [r for r in rows if (not location or r['location'] == location) and (not search or search.casefold() in (r['name']+' '+r['point_code']).casefold())]
    case = next((r for r in rows if r['key'] == request.GET.get('caso')), None)
    if request.GET.get('caso') and case is None:
        raise Http404('El caso no pertenece a esta revisión o filtro.')
    if request.GET.get('export') == 'csv':
        response = HttpResponse(content_type='text/csv; charset=utf-8')
        response['Content-Disposition'] = f'attachment; filename="auditoria_documental_{period:%Y%m}.csv"'
        response.write('\ufeff')
        writer = csv.writer(response)
        writer.writerow(['revision','periodo','insumo_id','codigo_point','insumo','ubicacion','unidad','compras_kardex','transferencias_entrada','transferencias_salida','consumo_calculado_BOM','consumo_calculado_venta','consumo_no_medido','ajustes','otras_entradas','otras_salidas','neto_libro','consumo_fisico','diferencia_fisica','estado','excepciones'])
        for row in rows:
            t = row['totals']
            writer.writerow([_csv_cell(v) for v in [snapshot.pk, period, row['insumo_id'], row['point_code'], row['name'], row['location'], row['unit'], *[t[k] for k in ('purchase','transfer_in','transfer_out','calculated','calculated_sale','unverified','adjustment','other_in','other_out')], row['ledger_net'], None, None, row['status'], '; '.join(i['code'] for i in row['issues'])]])
        response['Cache-Control'] = 'no-store'
        return response
    if request.GET.get('export') == 'json':
        payload = {'snapshot_id': snapshot.pk if snapshot else None, 'fingerprint': snapshot.fingerprint if snapshot else None, 'period': str(period), 'coverage': snapshot.payload['coverage'] if snapshot else {}, 'issues': snapshot.payload['issues'] if snapshot else [], 'rows': [case] if case else rows}
        response = JsonResponse(payload, json_dumps_params={'ensure_ascii': False})
        response['Content-Disposition'] = f'attachment; filename="evidencia_documental_{period:%Y%m}.json"'
        response['Cache-Control'] = 'no-store'
        return response
    reviews = list(snapshot.revisiones.filter(caso=case['key']).select_related('actor').order_by('creado_en', 'pk')) if case else []
    latest_explanation = next((r for r in reversed(reviews) if r.accion == 'EXPLICAR'), None)
    can_approve = bool(latest_explanation and latest_explanation.actor_id != request.user.pk and not any(r.explicacion_id == latest_explanation.pk for r in reviews))
    if case:
        case = dict(case, evidence_json=json.dumps(case['evidence'], ensure_ascii=False, indent=2))
    context = {
        'snapshot': snapshot, 'selected_period': period.strftime('%Y-%m'), 'location': location,
        'search': search, 'page_obj': Paginator(rows, 50).get_page(request.GET.get('page')),
        'row_count': len(rows), 'case': case, 'reviews': reviews,
        'request_id': str(uuid4()), 'can_review': snapshot == latest and can_manage_module(request.user, 'reportes'),
        'is_current': snapshot == latest, 'previous': previous,
        'can_approve': can_approve, 'explanation': latest_explanation,
        'stale': snapshot and timezone.now()-snapshot.creado_en > timedelta(hours=26),
        'global_issues': snapshot.payload['issues'][:50] if snapshot else [],
        'global_issue_count': len(snapshot.payload['issues']) if snapshot else 0,
    }
    response = render(request, 'reportes/auditoria_documental.html', context)
    response['Cache-Control'] = 'no-store'
    return response


def _response(request, message, url, *, status=200):
    ok = status == 200
    if 'application/json' in request.headers.get('Accept', '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        data = {'ok': ok, 'message': message, 'toast': {'message': message, 'level': 'success' if ok else 'error', 'persistent': not ok}}
        if ok:
            data.update(redirect=url, reload=True)
        return JsonResponse(data, status=status)
    if ok:
        messages.success(request, message)
        return redirect(url)
    # HTML conserva el formulario y su texto; el envío progresivo muestra el toast.
    return render(request, 'reportes/auditoria_documental_error.html', {'error': message, 'nota': request.POST.get('nota', ''), 'back_url': url}, status=status)


@login_required
@require_POST
def review(request, snapshot_id):
    _access(request.user)
    if not can_manage_module(request.user, 'reportes'):
        raise PermissionDenied('Se requiere gestión de Reportes para revisar evidencia.')
    snapshot = get_object_or_404(InsumoAuditSnapshot, pk=snapshot_id)
    case_key = request.POST.get('caso', '')
    if not any(r['key'] == case_key for r in snapshot.payload['rows']):
        raise Http404('Caso inexistente.')
    url = reverse('reportes:auditoria_documental') + f'?mes={snapshot.periodo:%Y-%m}&caso={case_key}#revision'
    action = request.POST.get('accion')
    note = request.POST.get('nota', '').strip()
    try:
        request_id = UUID(request.POST.get('request_id', ''))
    except ValueError:
        return _response(request, 'Intento no válido. Recarga el caso conservando tu nota.', url, status=400)
    if action not in ('EXPLICAR', 'APROBAR') or not 10 <= len(note) <= 4000:
        return _response(request, 'Incluye una nota de 10 a 4,000 caracteres y una acción válida.', url, status=400)
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_advisory_xact_lock(%s, %s)', [731107, request_id.int % 2147483647])
            cursor.execute('SELECT pg_advisory_xact_lock(%s, %s)', [731106, snapshot.periodo.year*100+snapshot.periodo.month])
        existing = InsumoAuditReview.objects.filter(request_id=request_id).first()
        if existing:
            if (existing.actor_id, existing.snapshot_id, existing.caso, existing.accion, existing.nota) == (request.user.pk, snapshot.pk, case_key, action, note):
                return _response(request, 'Revisión ya recibida.', url)
            return _response(request, 'Este intento ya corresponde a otra revisión.', url, status=409)
        if InsumoAuditSnapshot.objects.filter(periodo=snapshot.periodo).first().pk != snapshot.pk:
            return _response(request, 'La evidencia cambió. Consulta la nueva revisión antes de enviar tu explicación.', url, status=409)
        explanation = None
        if action == 'APROBAR':
            explanation = snapshot.revisiones.filter(caso=case_key, accion='EXPLICAR').order_by('-pk').first()
            if explanation is None or str(explanation.pk) != request.POST.get('explicacion') or explanation.actor_id == request.user.pk:
                return _response(request, 'Debe aprobar otra persona y sobre la última explicación.', url, status=409)
            if InsumoAuditReview.objects.filter(explicacion=explanation).exists():
                return _response(request, 'La explicación ya fue aprobada.', url, status=409)
        InsumoAuditReview.objects.create(snapshot=snapshot, caso=case_key, accion=action, nota=note, actor=request.user, request_id=request_id, explicacion=explanation)
    return _response(request, 'Explicación registrada.' if action == 'EXPLICAR' else 'Explicación aprobada. Esto no ajusta inventario ni acredita consumo físico.', url)
