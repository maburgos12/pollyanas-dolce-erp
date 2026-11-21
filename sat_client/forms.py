from django import forms
from django.core.exceptions import ValidationError
from django.core.files.uploadhandler import MemoryFileUploadHandler, StopUpload
from django.views.decorators.debug import sensitive_variables

from sat_client.services.efirma import MAX_FILE_BYTES, validar_efirma


class EfirmaUploadHandler(MemoryFileUploadHandler):
    """Keep key uploads in bounded memory, including when the usual disk threshold is lower."""

    def handle_raw_input(self, *args, **kwargs):
        self.activated = True
        self.received = 0

    def receive_data_chunk(self, raw_data, start):
        self.received += len(raw_data)
        if start + len(raw_data) > MAX_FILE_BYTES or self.received > 2 * MAX_FILE_BYTES:
            self.file.close()
            raise StopUpload(connection_reset=True)
        return super().receive_data_chunk(raw_data, start)


class EfirmaForm(forms.Form):
    certificado = forms.FileField(label="Certificado .cer", widget=forms.ClearableFileInput(attrs={"accept": ".cer", "class": "form-control"}))
    llave = forms.FileField(label="Llave privada .key", widget=forms.ClearableFileInput(attrs={"accept": ".key", "class": "form-control"}))
    password = forms.CharField(label="Contraseña de la llave privada", max_length=256, strip=False,
                               widget=forms.PasswordInput(attrs={"autocomplete": "off", "class": "form-control"}))

    @sensitive_variables()
    def clean(self):
        data = super().clean()
        if not all(name in data for name in ("certificado", "llave", "password")):
            return data
        for name in ("certificado", "llave"):
            if data[name].size > MAX_FILE_BYTES:
                self.add_error(name, "El archivo no debe superar 16 KB.")
        if self.errors:
            return data
        try:
            self.credentials, self.certificate = validar_efirma(
                data["certificado"].read(MAX_FILE_BYTES + 1), data["llave"].read(MAX_FILE_BYTES + 1), data["password"])
        except ValidationError as exc:
            self.add_error(None, exc)
        return data
