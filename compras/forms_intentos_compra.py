"""Captura validada para cancelar intentos y registrar reembolsos."""

from decimal import Decimal

from django import forms
from django.core.exceptions import ValidationError
from django.utils import timezone

from .models import CompraRealizadaDepartamental, IntentoCompraDepartamental
from .validaciones_archivos import validar_comprobante


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
                max_digits=14, decimal_places=2,
                help_text=(
                    f"Producto pagado: ${self.compra.importe_final:.2f}. "
                    "Cualquier exceso necesita cargos adicionales documentados."
                ),
            )
            self.fields["reembolso_cargos_adicionales"] = forms.DecimalField(
                label="Cargos adicionales documentados",
                min_value=Decimal("0.00"), initial=Decimal("0.00"), required=False,
                max_digits=14, decimal_places=2,
                help_text=(
                    "Envío, ajuste u otro cargo incluido por el proveedor en esta devolución."
                ),
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

    def clean_reembolso_cargos_adicionales(self):
        return self.cleaned_data.get("reembolso_cargos_adicionales") or Decimal("0.00")

    def clean_evidencia_solicitud_reembolso(self):
        return validar_comprobante(self.cleaned_data.get("evidencia_solicitud_reembolso"))

    def clean(self):
        cleaned = super().clean()
        if not self.compra:
            return cleaned
        total = cleaned.get("reembolso_solicitado")
        cargos = cleaned.get("reembolso_cargos_adicionales")
        evidencia = cleaned.get("evidencia_solicitud_reembolso")
        if total is not None and cargos is not None:
            if cargos > total:
                self.add_error(
                    "reembolso_cargos_adicionales",
                    "Los cargos adicionales no pueden superar el total solicitado.",
                )
            elif total - cargos > self.compra.importe_final:
                self.add_error(
                    "reembolso_solicitado",
                    "La parte del producto no puede superar la compra pagada.",
                )
            if cargos > 0 and not evidencia:
                self.add_error(
                    "evidencia_solicitud_reembolso",
                    "Adjunta evidencia cuando el reembolso incluya cargos adicionales.",
                )
        return cleaned


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
