"""Identidad de una captura: un reintento devuelve la misma merma."""
import json
from hashlib import sha256
from uuid import UUID

from django.core.exceptions import ValidationError
from django.db import connection


class CapturaConflict(ValidationError):
    pass


def identificar_captura(model, *, request_id, actor_id, sucursal_id, payload, files):
    # Los formularios anteriores al despliegue siguen funcionando sin clave.
    if not request_id:
        return None, "", None
    try:
        request_id = UUID(str(request_id))
    except (ValueError, TypeError, AttributeError):
        raise ValidationError("Identificador de captura inválido. Recarga el formulario conservando tu borrador.")
    evidence = {}
    for field, uploads in files.items():
        evidence[field] = []
        for upload in uploads:
            digest = sha256()
            position = upload.tell()
            try:
                for chunk in upload.chunks():
                    digest.update(chunk)
            finally:
                upload.seek(position)
            evidence[field].append(digest.hexdigest())
    fingerprint = sha256(json.dumps(
        {"actor": actor_id, "sucursal": sucursal_id, "datos": payload, "fotos": evidence},
        sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()
    lock = int.from_bytes(sha256(f"merma:{model._meta.label_lower}:{request_id}".encode()).digest()[:8], "big", signed=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", [lock])
        if not cursor.fetchone()[0]:
            raise CapturaConflict("Esta captura se está guardando. Se conserva tu borrador; espera y vuelve a intentar.")
    previous = model.objects.filter(request_id=request_id).first()
    if previous and previous.payload_hash != fingerprint:
        raise CapturaConflict("Esta captura ya fue registrada con otros datos. Revisa el historial antes de iniciar una nueva merma.")
    return request_id, fingerprint, previous
