"""Validaciones neutrales para archivos de compras."""

from pathlib import Path

from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import UploadedFile


def validar_comprobante(value, *, exigir_archivo_nuevo=False):
    """Valida tamaño, extensión y firma de un comprobante recién subido."""
    if exigir_archivo_nuevo and not isinstance(value, UploadedFile):
        raise ValidationError("Adjunta un comprobante nuevo desde tu dispositivo.")
    # Un FieldFile sin reemplazo ya se validó al subirse; solo se revisa lo nuevo.
    if isinstance(value, UploadedFile):
        if value.size > 10 * 1024 * 1024:
            raise ValidationError("El comprobante no puede superar 10 MB.")
        suffix = Path(value.name).suffix.lower()
        header = value.read(16)
        value.seek(0)
        signatures = {
            ".pdf": header.startswith(b"%PDF-"),
            ".jpg": header.startswith(b"\xff\xd8\xff"),
            ".jpeg": header.startswith(b"\xff\xd8\xff"),
            ".png": header.startswith(b"\x89PNG\r\n\x1a\n"),
            ".webp": header.startswith(b"RIFF") and header[8:12] == b"WEBP",
        }
        if not signatures.get(suffix, False):
            raise ValidationError("Adjunta un comprobante PDF, JPG, PNG o WebP válido.")
    return value
