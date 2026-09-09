"""Validación de cotizaciones y altas explícitas desde compras departamentales."""
import unicodedata
from decimal import Decimal
from urllib.parse import urlsplit

from django import forms
from django.core.exceptions import ValidationError
from django.db import connection, transaction

from core.models import AuditLog
from maestros.models import Proveedor
from .models import CotizacionCompraDepartamental


class ProveedorCotizacionForm(forms.Form):
    nombre = forms.CharField(label='Nombre del proveedor o vendedor', max_length=200)
    lead_time_dias = forms.IntegerField(label='Plazo de entrega (días)', min_value=0, initial=0)

    def clean_nombre(self):
        return ' '.join(self.cleaned_data['nombre'].split())


class CotizacionDepartamentalForm(forms.ModelForm):
    class Meta:
        model = CotizacionCompraDepartamental
        fields = ['proveedor', 'plataforma', 'enlace_producto', 'cantidad_ofertada', 'costo_unitario',
                  'descuento', 'impuestos', 'envio', 'instalacion', 'otros_cargos', 'documento',
                  'garantia_observaciones']
        labels = {'proveedor':'Proveedor o vendedor', 'plataforma':'Dónde cotizas', 'enlace_producto':'Enlace al producto',
                  'cantidad_ofertada':'Cantidad ofertada', 'costo_unitario':'Costo unitario cotizado',
                  'envio':'Envío', 'instalacion':'Instalación', 'garantia_observaciones':'Garantía u observaciones'}
        widgets = {'documento':forms.FileInput(attrs={'accept':'application/pdf,image/*'}),
                   'garantia_observaciones':forms.Textarea(attrs={'rows':3}),
                   'enlace_producto':forms.URLInput(attrs={'placeholder':'https://…'})}

    def __init__(self, *args, item, **kwargs):
        self.item = item
        if args and args[0] is not None:
            data = args[0].copy()
            for key in ['descuento','impuestos','envio','instalacion','otros_cargos']:
                if not data.get(key):
                    data[key] = '0'
            if not data.get('cantidad_ofertada'):
                data['cantidad_ofertada'] = str(item.cantidad)
            args = (data, *args[1:])
        kwargs.setdefault('auto_id', f'cotizacion-{item.pk}-%s')
        super().__init__(*args, **kwargs)
        self.fields['proveedor'].queryset = Proveedor.objects.filter(activo=True).order_by('nombre')
        self.fields['proveedor'].empty_label = 'Selecciona un proveedor o vendedor'
        self.fields['proveedor'].widget.attrs.update({'form':f'cotizacion-form-{item.pk}'})
        self.fields['plataforma'].widget.attrs['data-native-select'] = 'true'
        self.fields['cantidad_ofertada'].min_value = Decimal('0.001')
        self.fields['cantidad_ofertada'].widget.attrs.update({'min':'0.001','step':'0.001'})
        self.initial.setdefault('cantidad_ofertada', item.cantidad)
        for key in ['costo_unitario','descuento','impuestos','envio','instalacion','otros_cargos']:
            self.fields[key].widget.attrs.update({'min':'0','step':'0.01'})

    def clean_enlace_producto(self):
        url = self.cleaned_data['enlace_producto']
        if url:
            parsed = urlsplit(url)
            if parsed.scheme not in ('http','https') or parsed.username or parsed.password:
                raise ValidationError('Usa un enlace HTTP o HTTPS sin credenciales.')
        return url

    def clean(self):
        cleaned = super().clean()
        quantity = cleaned.get('cantidad_ofertada')
        if quantity is not None and quantity <= 0:
            self.add_error('cantidad_ofertada','La cantidad debe ser mayor que cero.')
        for key in ['costo_unitario','descuento','impuestos','envio','instalacion','otros_cargos']:
            value = cleaned.get(key)
            if value is not None and value < 0:
                self.add_error(key,'El importe no puede ser negativo.')
        if cleaned.get('plataforma') and not cleaned.get('enlace_producto'):
            self.add_error('enlace_producto','Agrega el enlace al producto para esta compra en línea.')
        values = [cleaned.get(k) for k in ['cantidad_ofertada','costo_unitario','descuento']]
        if all(v is not None for v in values) and values[2] > values[0] * values[1]:
            self.add_error('descuento','El descuento no puede superar el subtotal.')
        return cleaned


def _clave_nombre(nombre):
    return ' '.join(''.join(c for c in unicodedata.normalize('NFKD',nombre)
                           if not unicodedata.combining(c)).casefold().split())


@transaction.atomic
def crear_proveedor_cotizacion(*, nombre, lead_time_dias, actor, item):
    # Mismo patrón que el alta explícita en Fallas: incluye catálogo vacío y altas simultáneas.
    with connection.cursor() as cursor:
        cursor.execute('LOCK TABLE maestros_proveedor IN SHARE ROW EXCLUSIVE MODE')
    existente = next((p for p in Proveedor.objects.order_by('pk') if _clave_nombre(p.nombre) == _clave_nombre(nombre)),None)
    if existente:
        if not existente.activo:
            raise ValidationError(f'«{existente.nombre}» ya está registrado como inactivo. Revisa su ficha en Proveedores.')
        raise ValidationError(f'Ya existe «{existente.nombre}». Selecciónalo en la lista de proveedores.')
    proveedor = Proveedor.objects.create(nombre=nombre,lead_time_dias=lead_time_dias,activo=True)
    AuditLog.objects.create(user=actor,action='CREATE',model='maestros.Proveedor',object_id=str(proveedor.pk),
                            payload={'origen':'compras_departamentales','item_id':item.pk,'nombre':nombre})
    return proveedor
