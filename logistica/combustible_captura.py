"""Captura administrativa de tickets sin fabricar turnos o recorridos."""
import hashlib
from decimal import Decimal

from django import forms
from django.core.exceptions import PermissionDenied
from django.db import connection, transaction
from django.db.models import Q
from django.utils import timezone

from core.audit import log_event
from .models import CargaCombustibleUnidad, Repartidor, Unidad


class CargaDuplicada(Exception):
    def __init__(self, carga):
        self.carga = carga
        super().__init__(f'El ticket ya está registrado en la carga #{carga.pk} de {carga.unidad.codigo}.')


class CapturaCombustibleForm(forms.ModelForm):
    fecha_carga = forms.DateTimeField(label='Fecha y hora del ticket', widget=forms.DateTimeInput(format='%Y-%m-%dT%H:%M', attrs={'type': 'datetime-local'}))
    litros = forms.DecimalField(label='Litros', max_digits=8, decimal_places=3, min_value=Decimal('0.001'))
    importe_total = forms.DecimalField(label='Importe total (MXN)', max_digits=10, decimal_places=2, min_value=Decimal('0.01'))
    estacion = forms.CharField(label='Estación', max_length=160)
    folio_ticket = forms.CharField(label='Folio del ticket', max_length=80)
    foto_ticket = forms.ImageField(label='Foto del ticket')

    class Meta:
        model = CargaCombustibleUnidad
        fields = ['unidad', 'repartidor', 'fecha_carga', 'estacion', 'folio_ticket', 'litros', 'importe_total', 'foto_ticket', 'observaciones']
        labels = {'repartidor': 'Responsable de la carga', 'observaciones': 'Combustible y observaciones'}
        widgets = {'observaciones': forms.Textarea(attrs={'rows': 3})}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['unidad'].queryset = Unidad.objects.filter(activa=True)
        self.fields['repartidor'].queryset = Repartidor.objects.filter(user__is_active=True).select_related('user')
        for field in self.fields.values():
            field.widget.attrs['class'] = 'input-field'
        self.fields['litros'].widget.attrs.update(step='0.001', inputmode='decimal')
        self.fields['importe_total'].widget.attrs.update(step='0.01', inputmode='decimal')

    def clean_estacion(self):
        return ' '.join(self.cleaned_data['estacion'].upper().split())

    def clean_folio_ticket(self):
        return self.cleaned_data['folio_ticket'].strip().upper()

    def clean_fecha_carga(self):
        value = self.cleaned_data['fecha_carga']
        if value > timezone.now():
            raise forms.ValidationError('La fecha del ticket no puede estar en el futuro.')
        return value


def registrar_carga_sin_turno(*, form, actor, ip=None):
    from .views_combustible import puede_corregir_combustible
    if not puede_corregir_combustible(actor):
        raise PermissionDenied
    if not form.is_valid():
        raise ValueError('La captura debe ser válida antes de guardarse.')
    carga = form.save(commit=False)
    foto = form.cleaned_data['foto_ticket']
    digest = hashlib.sha256()
    for chunk in foto.chunks():
        digest.update(chunk)
    foto.seek(0)
    carga.ticket_sha256 = digest.hexdigest()
    carga.registrado_por = actor
    carga.ip_registro = ip
    archivo_guardado = None
    try:
        with transaction.atomic():
            # Serializa reintentos administrativos, incluso en distintas unidades.
            with connection.cursor() as cursor:
                cursor.execute('SELECT pg_advisory_xact_lock(%s, %s)', [20261127, 1])
            duplicada = CargaCombustibleUnidad.objects.select_related('unidad').filter(
                Q(ticket_sha256=carga.ticket_sha256)
                | Q(estacion=carga.estacion, folio_ticket=carga.folio_ticket)
                | Q(auditoria_detalle__ticket_leido__estacion__iexact=carga.estacion,
                    auditoria_detalle__ticket_leido__folio=carga.folio_ticket)
            ).first()
            if duplicada:
                raise CargaDuplicada(duplicada)
            carga.foto_ticket.save(foto.name, foto, save=False)
            archivo_guardado = carga.foto_ticket.name
            carga.save()
            log_event(actor, 'CREATE', 'logistica.CargaCombustibleUnidad', str(carga.pk), {
                'origen': 'captura_sin_turno', 'unidad': carga.unidad.codigo,
                'repartidor_id': carga.repartidor_id, 'fecha_carga': carga.fecha_carga.isoformat(),
                'estacion': carga.estacion, 'folio_ticket': carga.folio_ticket,
                'litros': str(carga.litros), 'importe_total': str(carga.importe_total),
                'ticket_sha256': carga.ticket_sha256,
            })
    except Exception:
        if archivo_guardado:
            carga.foto_ticket.storage.delete(archivo_guardado)
        raise
    return carga
