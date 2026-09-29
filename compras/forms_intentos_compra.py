"""Captura validada para cancelar intentos y registrar reembolsos."""

from decimal import Decimal

from django import forms
from django.core.exceptions import ValidationError
from django.utils import timezone

from .forms_edicion_compra import validar_comprobante
from .models import CompraRealizadaDepartamental, IntentoCompraDepartamental


class CancelarIntentoCompraForm(forms.Form):
    version = forms.IntegerField(min_value=1, widget=forms.HiddenInput)
    motivo = forms.ChoiceField(choices=IntentoCompraDepartamental.MOTIVO_CHOICES)
    detalle = forms.CharField(max_length=2000, widget=forms.Textarea(attrs={"rows": 3}))

    def __init__(self, *args, intento, **kwargs):
        super().__init__(*args, **kwargs)
        self.intento = intento
        self.initial.setdefault("version", intento.version)
        self.compra = CompraRealizadaDepartamental.objects.filter(intento=intento).first()
        if self.compra:
            self.fields["reembolso_solicitado_en"] = forms.DateField(
                label="Fecha de solicitud de reembolso",
                widget=forms.DateInput(attrs={"type": "date"}, format="%Y-%m-%d"),
            )
            self.fields["reembolso_solicitado"] = forms.DecimalField(
                label="Importe solicitado", min_value=Decimal("0.01"),
                max_value=self.compra.importe_final,
                max_digits=14, decimal_places=2,
                help_text=f"Máximo reembolsable: ${self.compra.importe_final:.2f}",
                error_messages={
                    "max_value": "El reembolso no puede superar la compra pagada.",
                },
            )
            self.fields["evidencia_solicitud_reembolso"] = forms.FileField(
                label="Evidencia de solicitud", required=False,
                widget=forms.FileInput(attrs={"accept": ".pdf,.jpg,.jpeg,.png,.webp"}),
            )

    def clean_detalle(self):
        detalle = self.cleaned_data["detalle"].strip()
        if not detalle:
            raise ValidationError("Describe por qué se cancela el intento.")
        return detalle

    def clean_reembolso_solicitado_en(self):
        fecha = self.cleaned_data["reembolso_solicitado_en"]
        if fecha > timezone.localdate():
            raise ValidationError("La fecha no puede ser futura.")
        return fecha

    def clean_reembolso_solicitado(self):
        importe = self.cleaned_data["reembolso_solicitado"]
        if importe > self.compra.importe_final:
            raise ValidationError("El reembolso no puede superar la compra pagada.")
        return importe

    def clean_evidencia_solicitud_reembolso(self):
        return validar_comprobante(self.cleaned_data.get("evidencia_solicitud_reembolso"))


class RegistrarReembolsoCompraForm(forms.Form):
    version = forms.IntegerField(min_value=1, widget=forms.HiddenInput)
    fecha = forms.DateField(widget=forms.DateInput(attrs={"type": "date"}, format="%Y-%m-%d"))
    importe = forms.DecimalField(min_value=Decimal("0.01"), max_digits=14, decimal_places=2)
    referencia = forms.CharField(max_length=160, required=False)
    comprobante = forms.FileField(
        required=False, widget=forms.FileInput(attrs={"accept": ".pdf,.jpg,.jpeg,.png,.webp"}),
    )

    def __init__(self, *args, intento, **kwargs):
        super().__init__(*args, **kwargs)
        self.intento = intento
        self.initial.setdefault("version", intento.version)

    def clean_fecha(self):
        fecha = self.cleaned_data["fecha"]
        if fecha > timezone.localdate():
            raise ValidationError("La fecha no puede ser futura.")
        return fecha

    def clean_importe(self):
        importe = self.cleaned_data["importe"]
        if importe > self.intento.saldo_reembolso:
            raise ValidationError("El reembolso supera el saldo solicitado.")
        return importe

    def clean_comprobante(self):
        return validar_comprobante(self.cleaned_data.get("comprobante"))
