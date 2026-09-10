"""Formularios de corrección y evidencia de compra departamental."""
from pathlib import Path
from decimal import Decimal

from django import forms
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import UploadedFile
from django.utils import timezone
from django.db.models import Q

from .forms_cotizaciones import CotizacionDepartamentalForm
from .models import CompraRealizadaDepartamental


class EditarCotizacionDepartamentalForm(CotizacionDepartamentalForm):
    version = forms.IntegerField(widget=forms.HiddenInput, min_value=1)
    motivo = forms.CharField(label="Motivo del cambio", widget=forms.Textarea(attrs={"rows": 2}), max_length=2000)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['proveedor'].widget.attrs.pop('form', None)
        self.initial['version'] = self.instance.version
        # Mostrar proveedores históricos sin permitir cambiar a otro proveedor inactivo.
        self.fields['proveedor'].queryset = self.fields['proveedor'].queryset.model.objects.filter(
            Q(activo=True) | Q(pk=self.instance.proveedor_id)
        ).order_by('nombre')
        if hasattr(self.item, 'linea_orden'):
            for name in ('proveedor', 'cantidad_ofertada'):
                self.fields[name].help_text = 'La orden ya existe: este dato debe conservarse.'


class RegistrarCompraDepartamentalForm(forms.ModelForm):
    cotizacion_id = forms.IntegerField(widget=forms.HiddenInput, min_value=1)
    version = forms.IntegerField(widget=forms.HiddenInput, min_value=1)

    class Meta:
        model = CompraRealizadaDepartamental
        fields = ['fecha_compra', 'importe_final', 'numero_pedido', 'comprobante']
        labels = {'fecha_compra': 'Fecha de compra', 'importe_final': 'Importe final de la compra (MXN)',
                  'numero_pedido': 'Número de pedido o referencia', 'comprobante': 'Comprobante de compra'}
        widgets = {'fecha_compra': forms.DateInput(attrs={'type': 'date'}, format='%Y-%m-%d'),
                   'comprobante': forms.FileInput(attrs={'accept': '.pdf,.jpg,.jpeg,.png,.webp'}),
                   'importe_final': forms.NumberInput(attrs={'min': '0.01', 'step': '0.01'})}

    def clean_fecha_compra(self):
        value = self.cleaned_data['fecha_compra']
        if value > timezone.localdate():
            raise ValidationError('La fecha de compra no puede ser futura.')
        return value

    def clean_importe_final(self):
        value = self.cleaned_data['importe_final']
        if value <= Decimal('0'):
            raise ValidationError('El importe final debe ser mayor que cero.')
        return value

    def clean_comprobante(self):
        return validar_comprobante(self.cleaned_data['comprobante'])


def validar_comprobante(value):
    # Un FieldFile sin reemplazo ya se validó al subirse; solo se revisa lo nuevo.
    if isinstance(value, UploadedFile):
        if value.size > 10 * 1024 * 1024:
            raise ValidationError('El comprobante no puede superar 10 MB.')
        suffix = Path(value.name).suffix.lower()
        header = value.read(16)
        value.seek(0)
        signatures = {'.pdf': header.startswith(b'%PDF-'), '.jpg': header.startswith(b'\xff\xd8\xff'),
                      '.jpeg': header.startswith(b'\xff\xd8\xff'), '.png': header.startswith(b'\x89PNG\r\n\x1a\n'),
                      '.webp': header.startswith(b'RIFF') and header[8:12] == b'WEBP'}
        if not signatures.get(suffix, False):
            raise ValidationError('Adjunta un comprobante PDF, JPG, PNG o WebP válido.')
    return value


class CorregirCompraDepartamentalForm(forms.ModelForm):
    """Corrige una compra ya pagada. El comprobante nuevo es opcional."""

    version = forms.IntegerField(widget=forms.HiddenInput, min_value=1)
    motivo = forms.CharField(label='Motivo de la corrección', widget=forms.Textarea(attrs={'rows': 2}), max_length=2000)

    class Meta:
        model = CompraRealizadaDepartamental
        fields = ['fecha_compra', 'importe_final', 'numero_pedido', 'comprobante']
        labels = {'fecha_compra': 'Fecha de compra', 'importe_final': 'Importe realmente pagado (MXN)',
                  'numero_pedido': 'Número de pedido o referencia',
                  'comprobante': 'Reemplazar comprobante (opcional)'}
        help_texts = {'importe_final': 'Cópialo del comprobante, no de la cotización.'}
        widgets = {'fecha_compra': forms.DateInput(attrs={'type': 'date'}, format='%Y-%m-%d'),
                   'comprobante': forms.FileInput(attrs={'accept': '.pdf,.jpg,.jpeg,.png,.webp'}),
                   'importe_final': forms.NumberInput(attrs={'min': '0.01', 'step': '0.01'})}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['comprobante'].required = False
        self.initial['version'] = self.instance.version

    def clean_fecha_compra(self):
        value = self.cleaned_data['fecha_compra']
        if value > timezone.localdate():
            raise ValidationError('La fecha de compra no puede ser futura.')
        return value

    def clean_importe_final(self):
        value = self.cleaned_data['importe_final']
        if value <= Decimal('0'):
            raise ValidationError('El importe debe ser mayor que cero.')
        return value

    def clean_comprobante(self):
        return validar_comprobante(self.cleaned_data.get('comprobante'))
