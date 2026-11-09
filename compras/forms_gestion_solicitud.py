from django import forms

from .models import ItemCompraDepartamental, SolicitudCompraDepartamental


class EditarSolicitudForm(forms.ModelForm):
    version = forms.CharField(widget=forms.HiddenInput)
    periodo = forms.DateField(
        label="Mes de planeación", input_formats=["%Y-%m"],
        widget=forms.DateInput(format="%Y-%m", attrs={"type": "month"}),
    )

    class Meta:
        model = SolicitudCompraDepartamental
        fields = ("area", "tipo", "periodo", "motivo", "justificacion_extraordinaria")
        labels = {"motivo": "Motivo general", "justificacion_extraordinaria": "Justificación extraordinaria o de emergencia"}
        widgets = {
            "motivo": forms.Textarea(attrs={"rows": 2}),
            "justificacion_extraordinaria": forms.Textarea(attrs={"rows": 2}),
        }


class EditarArticuloForm(forms.ModelForm):
    class Meta:
        model = ItemCompraDepartamental
        fields = (
            "descripcion", "categoria", "cantidad", "unidad", "costo_unitario_estimado",
            "fecha_requerida", "prioridad", "imagen",
        )
        labels = {
            "descripcion": "Descripción", "categoria": "Categoría",
            "costo_unitario_estimado": "Costo unitario estimado",
            "fecha_requerida": "Fecha requerida", "imagen": "Imagen de referencia",
        }
        widgets = {
            "fecha_requerida": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
            "cantidad": forms.NumberInput(attrs={"min": "0.001"}),
            "costo_unitario_estimado": forms.NumberInput(attrs={"min": "0"}),
        }

    def clean_cantidad(self):
        cantidad = self.cleaned_data["cantidad"]
        if cantidad <= 0:
            raise forms.ValidationError("La cantidad debe ser mayor que cero.")
        return cantidad

    def clean_costo_unitario_estimado(self):
        costo = self.cleaned_data["costo_unitario_estimado"]
        if costo is not None and costo < 0:
            raise forms.ValidationError("El costo estimado debe ser mayor o igual a cero.")
        return costo


class ArticulosSolicitudFormSet(forms.BaseInlineFormSet):
    def clean(self):
        super().clean()
        if any(self.errors):
            return
        ids = [form.cleaned_data["id"].pk for form in self.forms if form.cleaned_data.get("id")]
        esperados = {item.pk for item in self.get_queryset()}
        if len(ids) != len(esperados) or set(ids) != esperados:
            raise forms.ValidationError("Los artículos cambiaron. Vuelve a abrir la solicitud.")


EditarArticulosFormSet = forms.inlineformset_factory(
    SolicitudCompraDepartamental, ItemCompraDepartamental,
    form=EditarArticuloForm, formset=ArticulosSolicitudFormSet,
    extra=0, min_num=1, validate_min=True,
)


class EliminarSolicitudForm(forms.Form):
    version = forms.CharField(widget=forms.HiddenInput)
    motivo = forms.CharField(
        label="Motivo", max_length=2000, widget=forms.Textarea(attrs={"rows": 3}),
    )
