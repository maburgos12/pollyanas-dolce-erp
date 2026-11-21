from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.template.loader import render_to_string
from django.urls import reverse
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.views.decorators.debug import sensitive_post_parameters

from core.access import ROLE_DG, can_view_submodule, has_any_role
from sat_client.forms import EfirmaForm, EfirmaUploadHandler
from sat_client.models import SolicitudDocumentoSat
from sat_client.services.base import SatConfigurationError
from sat_client.services.documentos import solicitar_documento_sat
from sat_client.services.efirma import activar_efirma, estado_efirma, probar_efirma
from sat_client.services.fiscal_tools import documentos_sat_estado, estado_sat


def _assert_sat_access(request: HttpRequest) -> None:
    if not can_view_submodule(request.user, "conciliacion", "fiscal"):
        raise PermissionDenied("No tienes acceso al panel SAT fiscal.")


@login_required
@never_cache
@csrf_exempt
def panel_fiscal(request: HttpRequest) -> HttpResponse:
    _assert_sat_access(request)
    # Install before CSRF parses multipart; the inner view still enforces CSRF.
    request.upload_handlers = [EfirmaUploadHandler(request)]
    return _panel_fiscal(request)


@csrf_protect
@sensitive_post_parameters("password")
def _panel_fiscal(request: HttpRequest) -> HttpResponse:
    can_manage = has_any_role(request.user, ROLE_DG)
    form = EfirmaForm()
    if request.method == "POST":
        action = request.POST.get("accion")
        if action in ("actualizar_efirma", "probar_efirma"):
            if not can_manage:
                raise PermissionDenied("Solo Dirección puede administrar la e.firma.")
            ok = False
            if action == "actualizar_efirma":
                form = EfirmaForm(request.POST, request.FILES)
            try:
                if action == "actualizar_efirma":
                    if not form.is_valid():
                        raise ValidationError([f"{form.fields[name].label if name in form.fields else 'e.firma'}: {error}"
                                               for name, errors in form.errors.items() for error in errors])
                    activar_efirma(form.credentials, form.certificate, usuario=request.user)
                    message = "e.firma activada. Autenticación SAT verificada; las próximas descargas usarán esta e.firma."
                else:
                    probar_efirma(usuario=request.user)
                    message = "Autenticación SAT verificada. No se solicitaron descargas."
                ok = True
            except (ValidationError, SatConfigurationError) as exc:
                message = " ".join(exc.messages) if isinstance(exc, ValidationError) else str(exc)
            if "application/json" in request.headers.get("Accept", ""):
                payload = {"ok": ok, "toast": {"type": "success" if ok else "error", "message": message}}
                if ok:
                    updating = action == "actualizar_efirma"
                    payload.update(target="#sat-efirma" if updating else "#sat-efirma-status", html=render_to_string(
                        "sat_client/efirma.html" if updating else "sat_client/efirma_estado.html",
                        {"efirma": estado_efirma(), "efirma_form": EfirmaForm(), "efirma_expandida": True}, request=request))
                return JsonResponse(payload, status=200 if ok else 400)
            if ok:
                messages.success(request, message)
                return redirect(reverse("sat_client:panel_fiscal") + "#sat-efirma")
            messages.error(request, message)
            if action == "actualizar_efirma" and not form.non_field_errors():
                form.add_error(None, message)
        else:
            tipo = request.POST.get("tipo")
            if tipo in dict(SolicitudDocumentoSat.TIPO_CHOICES):
                solicitar_documento_sat(tipo=tipo, usuario=request.user)
                messages.warning(request, "Solicitud registrada. Falta conectar la descarga real en SAT.")
            return redirect(reverse("sat_client:panel_fiscal"))

    return render(
        request,
        "sat_client/panel_fiscal.html",
        {
            "estado_sat": estado_sat(),
            "documentos_sat": documentos_sat_estado(),
            "can_manage_efirma": can_manage,
            "efirma": estado_efirma() if can_manage else None,
            "efirma_form": form,
        },
    )
