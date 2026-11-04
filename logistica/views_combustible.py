"""Correcciones de combustible: captura, historial y aviso en una transacción."""
from decimal import Decimal

from django import forms
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core import signing
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
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
    litros = forms.DecimalField(label='Litros', min_value=Decimal('0.01'), max_digits=7, decimal_places=2)
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
                fecha = timezone.localtime(carga.fecha_registro).strftime('%d/%m/%Y')
                lineas = [f'Administración revisó el ticket de la bitácora {carga.bitacora.folio}, del {fecha}, y corrigió estos datos:']
                if 'importe_total' in cambios:
                    lineas.append(f"Importe: de ${Decimal(antes['importe_total']):,.2f} a ${carga.importe_total:,.2f} MXN.")
                if 'litros' in cambios:
                    lineas.append(f"Litros: de {antes['litros']} a {carga.litros:.2f} L.")
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
