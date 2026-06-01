from __future__ import annotations

import hmac
import json
import logging

from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .services_hikvision import procesar_eventos_hik

log = logging.getLogger("rrhh.receptor_hik")


def _auth_ok(request) -> bool:
    key = (request.headers.get("X-API-Key") or "").strip()
    if not key:
        return False

    expected = (getattr(settings, "ERP_PUBLIC_API_KEY", "") or "").strip()
    if expected and hmac.compare_digest(key, expected):
        return True

    try:
        from integraciones.models import PublicApiClient

        client = PublicApiClient.objects.filter(clave_prefijo=key[:12], activo=True).first()
        if client and client.validate(key):
            client.mark_used()
            return True
    except Exception as exc:
        log.warning("No se pudo validar PublicApiClient para receptor Hik: %s", exc)
    return False


@csrf_exempt
@require_POST
def receptor_asistencia_hik(request):
    if not _auth_ok(request):
        return JsonResponse({"error": "No autorizado"}, status=401)

    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "JSON invalido"}, status=400)

    eventos = body.get("eventos", [])
    if not isinstance(eventos, list):
        return JsonResponse({"error": "eventos debe ser una lista"}, status=400)

    return JsonResponse(procesar_eventos_hik(eventos))
