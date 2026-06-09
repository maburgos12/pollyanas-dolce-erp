from __future__ import annotations

import json
from typing import Any

from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_POST

from core.access import is_admin_or_dg
from core.audit import log_event
from syncfy_client.models import CuentaBancaria
from syncfy_client.services.auth import obtener_token
from syncfy_client.services.base import SyncfyConfigurationError, SyncfyServiceError
from syncfy_client.services.credenciales import listar_credenciales


def _assert_syncfy_access(request: HttpRequest) -> None:
    if not request.user.is_authenticated:
        raise PermissionDenied("Debes iniciar sesion.")
    if not is_admin_or_dg(request.user):
        raise PermissionDenied("No tienes permisos para administrar conexiones bancarias.")


def bancos_view(request: HttpRequest) -> HttpResponse:
    if not request.user.is_authenticated:
        return redirect("/login/?next=/syncfy/bancos/")
    _assert_syncfy_access(request)

    widget_token = ""
    syncfy_error = ""
    try:
        widget_token = obtener_token()
    except (SyncfyConfigurationError, SyncfyServiceError, ValueError) as exc:
        syncfy_error = str(exc)

    cuentas = list(CuentaBancaria.objects.order_by("banco"))
    return render(
        request,
        "syncfy_client/bancos.html",
        {
            "cuentas": cuentas,
            "widget_token": widget_token,
            "syncfy_error": syncfy_error,
            "widget_version": "1.6.1",
        },
    )


@require_POST
def guardar_credential_view(request: HttpRequest, banco: str) -> JsonResponse:
    _assert_syncfy_access(request)
    cuenta = get_object_or_404(CuentaBancaria, banco=banco)
    payload = _json_payload(request)
    id_credential = str(payload.get("id_credential") or payload.get("idCredential") or "").strip()
    id_site = str(payload.get("id_site") or payload.get("idSite") or "").strip()

    if not id_credential:
        return JsonResponse({"ok": False, "error": "Syncfy no envio id_credential."}, status=400)
    if id_site and id_site != cuenta.id_site_syncfy:
        return JsonResponse({"ok": False, "error": "La credencial no corresponde al banco seleccionado."}, status=400)

    anterior = cuenta.id_credential or ""
    cuenta.id_credential = id_credential
    cuenta.save(update_fields=["id_credential", "actualizado_en"])
    _log_credential_update(request, cuenta, anterior=anterior, source="widget_event")
    return JsonResponse({"ok": True, "cuenta": _cuenta_payload(cuenta)})


@require_POST
def sincronizar_credenciales_view(request: HttpRequest) -> JsonResponse:
    _assert_syncfy_access(request)
    try:
        token = obtener_token()
        credenciales = listar_credenciales(token=token)
    except (SyncfyConfigurationError, SyncfyServiceError, ValueError) as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=502)

    actualizadas = 0
    cuentas = list(CuentaBancaria.objects.order_by("banco"))
    credenciales_por_banco: dict[str, dict[str, Any]] = {}
    for cuenta in cuentas:
        credencial = _elegir_credencial(cuenta.id_site_syncfy, credenciales)
        if not credencial:
            continue
        credenciales_por_banco[cuenta.banco] = credencial
        id_credential = str(credencial.get("id_credential") or "").strip()
        if not id_credential:
            continue
        anterior = cuenta.id_credential or ""
        if anterior != id_credential:
            cuenta.id_credential = id_credential
            cuenta.save(update_fields=["id_credential", "actualizado_en"])
            _log_credential_update(request, cuenta, anterior=anterior, source="credentials_refresh")
            actualizadas += 1

    cuentas_payload = [_cuenta_payload(cuenta, credenciales_por_banco.get(cuenta.banco)) for cuenta in cuentas]
    autorizadas = sum(1 for cuenta in cuentas_payload if cuenta["syncfy_authorized"])
    no_autorizadas = sum(1 for cuenta in cuentas_payload if cuenta["syncfy_code"] and not cuenta["syncfy_authorized"])
    return JsonResponse(
        {
            "ok": True,
            "credenciales": len(credenciales),
            "actualizadas": actualizadas,
            "autorizadas": autorizadas,
            "no_autorizadas": no_autorizadas,
            "cuentas": cuentas_payload,
        }
    )


def _json_payload(request: HttpRequest) -> dict[str, Any]:
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _elegir_credencial(id_site: str, credenciales: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches = [cred for cred in credenciales if str(cred.get("id_site") or "") == id_site]
    if not matches:
        return None
    matches.sort(
        key=lambda cred: (
            _safe_int(cred.get("is_authorized")),
            _safe_int(cred.get("dt_authorized")),
            _safe_int(cred.get("dt_refresh")),
            _safe_int(cred.get("dt_execute")),
        ),
        reverse=True,
    )
    return matches[0]


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _cuenta_payload(cuenta: CuentaBancaria, credencial: dict[str, Any] | None = None) -> dict[str, Any]:
    id_credential = cuenta.id_credential or ""
    syncfy_authorized = bool(credencial and _safe_int(credencial.get("is_authorized")) == 1)
    syncfy_code = _safe_int(credencial.get("code")) if credencial else 0
    if syncfy_authorized:
        estado_syncfy = "Autorizado"
    elif syncfy_code:
        estado_syncfy = f"No autorizado ({syncfy_code})"
    elif id_credential:
        estado_syncfy = "Credencial guardada"
    else:
        estado_syncfy = "Pendiente"
    return {
        "banco": cuenta.banco,
        "nombre_display": cuenta.nombre_display,
        "id_site_syncfy": cuenta.id_site_syncfy,
        "activa": cuenta.activa,
        "tiene_id_credential": bool(id_credential),
        "id_credential_mask": _mask(id_credential),
        "syncfy_authorized": syncfy_authorized,
        "syncfy_code": syncfy_code,
        "estado_syncfy": estado_syncfy,
        "tiene_id_account": bool(cuenta.id_account),
        "numero_cuenta_mask": _mask(cuenta.numero_cuenta or "", suffix=4),
    }


def _mask(value: str, *, suffix: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= suffix:
        return "*" * len(value)
    return f"...{value[-suffix:]}"


def _log_credential_update(request: HttpRequest, cuenta: CuentaBancaria, *, anterior: str, source: str) -> None:
    log_event(
        request.user,
        "UPDATE",
        "syncfy_client.CuentaBancaria",
        str(cuenta.pk),
        {
            "banco": cuenta.banco,
            "source": source,
            "had_previous_credential": bool(anterior),
            "has_credential": bool(cuenta.id_credential),
        },
    )
