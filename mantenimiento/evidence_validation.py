from pathlib import Path

from django.utils.text import get_valid_filename


MAX_FILES = 5
IMAGE_MAX_SIZE = 10 * 1024 * 1024
PDF_MAX_SIZE = 30 * 1024 * 1024

ALLOWED = {
    ".jpg": ("image/jpeg", IMAGE_MAX_SIZE, lambda data: data.startswith(b"\xff\xd8\xff")),
    ".jpeg": ("image/jpeg", IMAGE_MAX_SIZE, lambda data: data.startswith(b"\xff\xd8\xff")),
    ".png": ("image/png", IMAGE_MAX_SIZE, lambda data: data.startswith(b"\x89PNG\r\n\x1a\n")),
    ".webp": ("image/webp", IMAGE_MAX_SIZE, lambda data: len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP"),
    ".pdf": ("application/pdf", PDF_MAX_SIZE, lambda data: data.startswith(b"%PDF-")),
}


class EvidenceValidationError(ValueError):
    def __init__(self, errors):
        self.errors = errors
        super().__init__(" ".join(errors))


def validate_evidence_files(files, *, images_only=False):
    files = [file for file in files if file]
    errors = []
    if len(files) > MAX_FILES:
        errors.append("Puedes adjuntar un máximo de 5 evidencias por avance.")

    for uploaded in files:
        original_name = Path(uploaded.name or "").name
        safe_name = get_valid_filename(original_name)
        extension = Path(safe_name).suffix.lower()
        rule = ALLOWED.get(extension)
        if images_only and extension == ".pdf":
            rule = None
        if not safe_name or not rule:
            errors.append(f"{original_name or 'Archivo'}: tipo de archivo no permitido.")
            continue
        expected_mime, max_size, signature_matches = rule
        if uploaded.content_type != expected_mime:
            errors.append(f"{original_name}: el tipo declarado no coincide con la extensión.")
            continue
        if uploaded.size > max_size:
            limit = 30 if extension == ".pdf" else 10
            errors.append(f"{original_name}: excede el límite de {limit} MB.")
            continue
        try:
            uploaded.seek(0)
            header = uploaded.read(16)
        finally:
            uploaded.seek(0)
        if not signature_matches(header):
            errors.append(f"{original_name}: el contenido no coincide con el tipo de archivo.")
            continue
        stem = Path(safe_name).stem
        suffix = Path(safe_name).suffix
        uploaded.name = f"{stem[:255 - len(suffix)]}{suffix}"

    if errors:
        for uploaded in files:
            uploaded.seek(0)
        raise EvidenceValidationError(errors)
    return files
