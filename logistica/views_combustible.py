"""Correcciones de combustible: captura, historial y aviso en una transacción."""
from decimal import Decimal

from django import forms
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core import signing
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import Http404, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.template.loader import render_to_string
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_http_methods

from core.access import ROLE_ADMIN, ROLE_DG, can_manage_submodule, has_any_role
from core.audit import log_event
from core.models import AuditLog
from core.notificaciones import crear_notificacion
from .models import CargaCombustibleUnidad
from .tasks import auditar_ticket_combustible

AVISO_CORRECCION = 'logistica.correccion_combustible'
MODELO_CARGA = 'logistica.CargaCombustibleUnidad'


def puede_corregir_combustible(user):
    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and can_manage_submodule(user, 'logistica', 'bitacoras')


class CorreccionCombustibleForm(forms.Form):
    importe_total = forms.DecimalField(label='Importe total (MXN)', min_value=Decimal('0.01'), max_digits=10, decimal_places=2)
    litros = forms.DecimalField(label='Litros', min_value=Decimal('0.001'), max_digits=8, decimal_places=3)
    motivo = forms.CharField(label='Motivo de la corrección', max_length=500, widget=forms.Textarea(attrs={'rows': 3}))
    revision = forms.CharField(widget=forms.HiddenInput)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs['class'] = 'input-field'


def _valores(carga):
    return {'importe_total': str(carga.importe_total), 'litros': str(carga.litros)}


def _revision(carga):
    ultimo = AuditLog.objects.filter(model=MODELO_CARGA, object_id=str(carga.pk), action='UPDATE').order_by('-pk').values_list('pk', flat=True).first()
    return {'id': carga.pk, **_valores(carga), 'ultimo_cambio': ultimo}


@login_required
@never_cache
@require_http_methods(['GET', 'POST'])
def combustible_editar(request, pk):
    if getattr(request, 'preview_actor', None) or not puede_corregir_combustible(request.user):
        raise PermissionDenied('Solo DG/Administración con gestión de Bitácoras puede corregir combustible.')
    response_status = 200
    with transaction.atomic():
        carga = get_object_or_404(
            CargaCombustibleUnidad.objects.select_for_update(of=('self',)).select_related('bitacora', 'unidad', 'repartidor__user'), pk=pk,
        )
        revision = _revision(carga)
        form = CorreccionCombustibleForm(request.POST if request.method == 'POST' else None, initial={
            **_valores(carga), 'revision': signing.dumps(revision, salt=AVISO_CORRECCION),
        })
        if request.method == 'POST' and form.is_valid():
            try:
                vigente = signing.loads(form.cleaned_data['revision'], salt=AVISO_CORRECCION, max_age=86400) == revision
            except signing.BadSignature:
                vigente = False
            if not vigente:
                form.add_error(None, 'El registro cambió o el formulario venció. Recarga la página y revisa los valores antes de guardar.')
                response_status = 409
            elif not carga.repartidor.user.is_active:
                form.add_error(None, 'El capturista está inactivo. No se puede entregar el aviso; no se guardó la corrección.')
            else:
                antes = _valores(carga)
                despues = {campo: str(form.cleaned_data[campo]) for campo in antes}
                cambios = [campo for campo in antes if Decimal(antes[campo]) != Decimal(despues[campo])]
                if not cambios:
                    messages.info(request, 'No hubo cambios en el importe ni en los litros.')
                    return redirect('logistica:combustible_editar', pk=pk)
                auditoria_anterior = {'estado': carga.auditoria_estado, 'detalle': carga.auditoria_detalle, 'motivos': carga.auditoria_motivos}
                carga.importe_total = form.cleaned_data['importe_total']
                carga.litros = form.cleaned_data['litros']
                carga.auditoria_estado = CargaCombustibleUnidad.AUDITORIA_PENDIENTE
                carga.auditoria_motivos = []
                carga.auditoria_detalle = {}
                carga.auditoria_score = 0
                # La marca también impide que una lectura iniciada antes sobrescriba este cambio.
                carga.auditoria_analizada_en = timezone.now()
                carga.save(update_fields=['importe_total', 'litros', 'auditoria_estado', 'auditoria_motivos', 'auditoria_detalle', 'auditoria_score', 'auditoria_analizada_en'])
                log_event(request.user, 'UPDATE', MODELO_CARGA, str(pk), {
                    'antes': antes, 'despues': despues, 'motivo': form.cleaned_data['motivo'],
                    'auditoria_anterior': auditoria_anterior,
                })
                fecha = timezone.localtime(carga.fecha_consumo).strftime('%d/%m/%Y')
                origen = f'la bitácora {carga.bitacora.folio}' if carga.bitacora_id else f'la carga sin turno #{carga.pk}'
                lineas = [f'Administración revisó el ticket de {origen}, del {fecha}, y corrigió estos datos:']
                if 'importe_total' in cambios:
                    lineas.append(f"Importe: de ${Decimal(antes['importe_total']):,.2f} a ${carga.importe_total:,.2f} MXN.")
                if 'litros' in cambios:
                    lineas.append(f"Litros: de {antes['litros']} a {carga.litros:.3f} L.")
                lineas.extend([
                    f"Motivo: {form.cleaned_data['motivo']}",
                    'Antes de guardar tus próximas cargas, verifica que el importe y los litros coincidan con la foto del ticket.',
                ])
                crear_notificacion(
                    usuario=carga.repartidor.user, actor=request.user,
                    titulo='Se corrigió tu registro de combustible', mensaje='\n\n'.join(lineas),
                    objeto_tipo=AVISO_CORRECCION, objeto_id=pk, url=reverse('logistica:pwa_app'),
                )
                transaction.on_commit(lambda: auditar_ticket_combustible.delay(pk), robust=True)
                messages.success(request, 'Corrección guardada. El capturista recibirá el aviso al entrar; la auditoría del ticket quedó pendiente de actualizar.')
                return redirect('logistica:combustible_editar', pk=pk)
    historial = AuditLog.objects.filter(model=MODELO_CARGA, object_id=str(pk), action='UPDATE').select_related('user').order_by('-pk')[:20]
    return render(request, 'logistica/combustible_editar.html', {'carga': carga, 'form': form, 'historial': historial}, status=response_status)


@login_required
@never_cache
@require_http_methods(['GET', 'POST'])
def combustible_nuevo(request):
    from .combustible_captura import CapturaCombustibleForm, CargaDuplicada, registrar_carga_sin_turno
    if getattr(request, 'preview_actor', None) or not puede_corregir_combustible(request.user):
        raise PermissionDenied('Solo DG/Administración con gestión de Bitácoras puede registrar cargas sin turno.')
    form = CapturaCombustibleForm(request.POST or None, request.FILES or None)
    carga = None
    status = 200
    if request.method == 'POST':
        if form.is_valid():
            try:
                carga = registrar_carga_sin_turno(form=form, actor=request.user, ip=request.META.get('REMOTE_ADDR'))
            except CargaDuplicada as exc:
                form.add_error(None, str(exc))
                status = 409
        else:
            status = 409 if any(e.code == 'duplicate_ticket' for errors in form.errors.as_data().values() for e in errors) else 400
        if carga:
            mensaje = f'Carga #{carga.pk} registrada en {carga.unidad.codigo} por ${carga.importe_total:,.2f} MXN.'
            if 'application/json' in request.headers.get('Accept', ''):
                return JsonResponse({'ok': True, 'carga_id': carga.pk, 'target': '#combustible-captura',
                    'html': render_to_string('logistica/_combustible_captura.html', {'carga': carga}, request=request),
                    'toast': {'type': 'success', 'message': mensaje}})
            messages.success(request, mensaje)
            return redirect(f"{reverse('logistica:combustible_nuevo')}?carga={carga.pk}#combustible-captura")
        if 'application/json' in request.headers.get('Accept', ''):
            errores = ' '.join(f"{form.fields[campo].label if campo in form.fields else 'Ticket'}: {' '.join(textos)}" for campo, textos in form.errors.items())
            return JsonResponse({'ok': False, 'toast': {'type': 'error', 'message': errores}}, status=status or 400)
    elif request.GET.get('carga'):
        if not request.GET['carga'].isdigit():
            raise Http404
        carga = get_object_or_404(CargaCombustibleUnidad.objects.select_related('unidad', 'repartidor__user', 'registrado_por'), pk=request.GET['carga'], bitacora__isnull=True)
    return render(request, 'logistica/combustible_nuevo.html', {'form': form, 'carga': carga}, status=status)
