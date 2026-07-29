from __future__ import annotations

import hmac
import json
import logging

from django.conf import settings
from django.http import JsonResponse
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .services_hikvision import procesar_eventos_hik
from .services_hik_ingesta import CONTRACT_VERSION, MAX_BATCH_SIZE, ingest_batch
from .models import EstadoIntegracionHik

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


@csrf_exempt
@require_POST
def receptor_asistencia_hik_v2(request):
    if not _auth_ok(request):
        return JsonResponse({"error": "No autorizado"}, status=401)

    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "JSON invalido", "error_code": "invalid_json"}, status=400)

    if body.get("contract_version") != CONTRACT_VERSION:
        return JsonResponse(
            {"error": "contract_version debe ser 2", "error_code": "invalid_contract_version"},
            status=400,
        )

    events = body.get("events")
    if not isinstance(events, list):
        return JsonResponse(
            {"error": "events debe ser una lista", "error_code": "invalid_events"},
            status=400,
        )
    if len(events) > MAX_BATCH_SIZE:
        return JsonResponse(
            {"error": "El lote excede el maximo permitido", "error_code": "batch_too_large"},
            status=400,
        )

    batch_id = str(body.get("batch_id") or "").strip()
    if not batch_id:
        return JsonResponse(
            {"error": "batch_id es obligatorio", "error_code": "missing_batch_id"},
            status=400,
        )

    return JsonResponse(
        {
            "contract_version": CONTRACT_VERSION,
            "batch_id": batch_id,
            "results": ingest_batch(events),
        }
    )


def _aware_datetime(value):
    parsed = parse_datetime(str(value or "").strip())
    if parsed is None or timezone.is_naive(parsed):
        return None
    return parsed


def _nonnegative_int(value):
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return None


@csrf_exempt
@require_POST
def receptor_salud_hik_v2(request):
    if not _auth_ok(request):
        return JsonResponse({"error": "No autorizado"}, status=401)

    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "JSON invalido", "error_code": "invalid_json"}, status=400)

    status = str(body.get("status") or "").strip()
    allowed = {
        EstadoIntegracionHik.ESTADO_HEALTHY,
        EstadoIntegracionHik.ESTADO_RECOVERING,
        EstadoIntegracionHik.ESTADO_ACTION_REQUIRED,
    }
    if status not in allowed:
        return JsonResponse({"error": "status invalido", "error_code": "invalid_status"}, status=400)

    numeric_fields = {
        "outbox_pending": _nonnegative_int(body.get("outbox_pending")),
        "identity_deferred": _nonnegative_int(body.get("identity_deferred")),
        "failure_count": _nonnegative_int(body.get("failure_count")),
    }
    if any(value is None for value in numeric_fields.values()):
        return JsonResponse(
            {"error": "Contadores invalidos", "error_code": "invalid_counters"},
            status=400,
        )

    state, _ = EstadoIntegracionHik.objects.update_or_create(
        nombre="hikconnect_cloud",
        defaults={
            "estado": status,
            "ultimo_ciclo_en": _aware_datetime(body.get("last_cycle_at")),
            "ultimo_exito_en": _aware_datetime(body.get("last_success_at")),
            "ultima_marca_cloud_en": _aware_datetime(body.get("last_cloud_record_at")),
            **numeric_fields,
            "incident_key": str(body.get("incident_key") or "").strip()[:128],
            "ultimo_error": str(body.get("last_error") or "")[:4000],
        },
    )
    return JsonResponse({"ok": True, "status": state.estado, "reported_at": state.reportado_en.isoformat()})
