import json
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Count, Q
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from django.views.decorators.http import require_GET
from core.access import can_view_reportes, can_review_seguimiento_global
from inventario.models import InsumoReconciliationCase
from inventario.services_documentary_audit import month
from seguimiento.services import empleado_de_usuario


@login_required
@require_GET
def dashboard(request):
    cases = InsumoReconciliationCase.objects.select_related('solicitud__responsable_user','insumo')
    if not can_view_reportes(request.user):
        scope = Q(solicitud__responsable_user=request.user) | Q(solicitud__participantes_user=request.user)
        employee = empleado_de_usuario(request.user)
        if employee:
            scope |= Q(solicitud__responsable_empleado=employee) | Q(solicitud__participantes_empleado=employee)
        cases = cases.filter(scope).distinct()
    selected = None
    if request.GET.get('caso'):
        if not request.GET['caso'].isdigit():
            return HttpResponse('Expediente no válido.',status=400)
        selected = get_object_or_404(cases,pk=request.GET['caso'])
    try:
        period = month(request.GET.get('mes') or (selected.periodo.strftime('%Y-%m') if selected else timezone.localdate().strftime('%Y-%m')))
    except ValueError as exc:
        return HttpResponse(str(exc),status=400)
    if selected and selected.periodo != period:
        return HttpResponse('El expediente pertenece a otro periodo.',status=400)
    period_cases = cases.filter(periodo=period)
    states = list(period_cases.values('estado').annotate(total=Count('pk')).order_by('estado'))
    labels=dict(InsumoReconciliationCase._meta.get_field('estado').choices)
    for entry in states:
        entry['label']=labels[entry['estado']]
    state = request.GET.get('estado','')
    if state and state not in {'CONCILIADO','EXPLICADO','ACLARACION','RESPUESTA'}:
        return HttpResponse('Estado no válido.',status=400)
    search = request.GET.get('q','').strip()[:100]
    if state:
        period_cases=period_cases.filter(estado=state)
    if search:
        period_cases=period_cases.filter(Q(titulo__icontains=search)|Q(conclusion__icontains=search))
    if request.GET.get('export') == 'json':
        data=[{'id':c.pk,'periodo':str(c.periodo),'estado':c.estado,'titulo':c.titulo,'conclusion':c.conclusion,
            'solicitud_id':c.solicitud_id,'evidencia':c.evidencia} for c in ([selected] if selected else period_cases)]
        response=HttpResponse(json.dumps(data,ensure_ascii=False,default=str),content_type='application/json')
        response['Content-Disposition']='attachment; filename="conciliacion_insumos.json"'
    else:
        events=list(selected.eventos.order_by('-pk')[:20]) if selected else []
        response=render(request,'reportes/conciliacion_insumos.html',{'selected':selected,'period':period.strftime('%Y-%m'),
            'states':states,'state':state,'search':search,'page_obj':Paginator(period_cases,40).get_page(request.GET.get('page')),
            'events':events,'evidence_json':json.dumps(selected.evidencia,ensure_ascii=False,indent=2) if selected else '',
            'dg_access':can_review_seguimiento_global(request.user)})
    response['Cache-Control']='no-store'
    return response
