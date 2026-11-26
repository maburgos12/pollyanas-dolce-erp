"""Callback público de estados WhatsApp; exige verificación y firma de Meta."""

import hashlib
import hmac
import json
import os

from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .services_clima_meta import registrar_estado

MAX_WEBHOOK_BYTES = 2 * 1024 * 1024


@csrf_exempt
@require_http_methods(["GET", "POST"])
def estados_meta_clima(request):
    if request.method == "GET":
        token = os.getenv("CLIMA_META_WEBHOOK_VERIFY_TOKEN", "")
        recibido = request.GET.get("hub.verify_token", "")
        if not token:
            return HttpResponse(status=503)
        if request.GET.get("hub.mode") != "subscribe" or not hmac.compare_digest(token, recibido):
            return HttpResponse(status=403)
        challenge = request.GET.get("hub.challenge", "")
        if not challenge or len(challenge) > 256:
            return HttpResponse(status=400)
        return HttpResponse(challenge, content_type="text/plain")

    secreto = os.getenv("META_APP_SECRET", "")
    if not secreto:
        return HttpResponse(status=503)
    try:
        content_length = int(request.META.get("CONTENT_LENGTH") or 0)
    except ValueError:
        return HttpResponse(status=400)
    if content_length > MAX_WEBHOOK_BYTES:
        return HttpResponse(status=413)
    cuerpo = request.body
    if len(cuerpo) > MAX_WEBHOOK_BYTES:
        return HttpResponse(status=413)
    firma = request.headers.get("X-Hub-Signature-256", "")
    esperada = "sha256=" + hmac.new(secreto.encode(), cuerpo, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(firma, esperada):
        return HttpResponse(status=403)
    try:
        evento = json.loads(cuerpo)
    except (ValueError, UnicodeDecodeError):
        return HttpResponse(status=400)
    if not isinstance(evento, dict) or evento.get("object") != "whatsapp_business_account":
        return HttpResponse(status=200)
    phone_id = os.getenv("META_WHATSAPP_PHONE_NUMBER_ID", "")
    for entrada in evento.get("entry", []):
        if not isinstance(entrada, dict):
            continue
        for cambio in entrada.get("changes", []):
            if not isinstance(cambio, dict) or cambio.get("field") != "messages":
                continue
            valor = cambio.get("value") or {}
            metadata = valor.get("metadata") if isinstance(valor, dict) else None
            if not isinstance(metadata, dict) or not phone_id or str(metadata.get("phone_number_id")) != phone_id:
                continue
            for estado in valor.get("statuses", []):
                if isinstance(estado, dict):
                    registrar_estado(estado.get("id"), estado.get("status"), estado.get("timestamp"))
    return HttpResponse(status=200)
