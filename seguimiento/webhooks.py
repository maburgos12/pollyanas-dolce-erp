from __future__ import annotations

import hashlib
import hmac
import json

from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from seguimiento.services import upsert_agente_dg_payload


def _expected_signature(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _is_valid_signature(body: bytes, signature: str, secret: str) -> bool:
    if not signature:
        return False
    return hmac.compare_digest(signature, _expected_signature(body, secret))


@csrf_exempt
@require_POST
def agente_dg_webhook(request):
    secret = (getattr(settings, "AGENTE_DG_WEBHOOK_SECRET", "") or "").strip()
    if not secret:
        return JsonResponse({"ok": False, "error": "webhook_no_configurado"}, status=503)

    if not _is_valid_signature(request.body, request.headers.get("X-Agente-DG-Signature", ""), secret):
        return JsonResponse({"ok": False, "error": "firma_invalida"}, status=403)

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except ValueError:
        return JsonResponse({"ok": False, "error": "json_invalido"}, status=400)

    try:
        counters = upsert_agente_dg_payload(payload)
    except ValueError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=400)

    return JsonResponse({"ok": True, **counters})
