from __future__ import annotations

from django.contrib import messages
from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods

from conciliacion.services.importador import (
    ImportacionBancariaError,
    PreviewImportacion,
    confirmar_importacion,
    generar_preview,
    periodo_default_conciliacion,
    resumen_periodo_conciliacion,
    resumen_conciliacion,
)
from core.access import is_admin_or_dg
from core.audit import log_event
from sat_client.models import CfdiDescargado, LogDescargaSat
from syncfy_client.models import CuentaBancaria


SESSION_PREVIEW_KEY = "conciliacion_bancaria_preview"


def _assert_conciliacion_access(request: HttpRequest) -> None:
    if not request.user.is_authenticated:
        raise PermissionDenied("Debes iniciar sesion.")
    if not is_admin_or_dg(request.user):
        raise PermissionDenied("No tienes permisos para conciliacion bancaria.")


@require_http_methods(["GET", "POST"])
def conciliacion_bancaria_view(request: HttpRequest) -> HttpResponse:
    if not request.user.is_authenticated:
        return redirect("/login/?next=/conciliacion/bancaria/")
    _assert_conciliacion_access(request)

    preview: PreviewImportacion | None = None
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "preview":
            preview = _handle_preview(request)
        elif action == "confirm":
            _handle_confirm(request)
            return redirect("conciliacion:bancaria")

    if preview is None:
        preview_payload = request.session.get(SESSION_PREVIEW_KEY)
        if isinstance(preview_payload, dict):
            try:
                preview = PreviewImportacion.from_session_payload(preview_payload)
            except (KeyError, TypeError, ValueError):
                request.session.pop(SESSION_PREVIEW_KEY, None)

    contexto = resumen_conciliacion()
    periodo_year, periodo_month = _periodo_from_request(request)
    periodo_resumen = resumen_periodo_conciliacion(year=periodo_year, month=periodo_month)
    sat_ultimo_log = LogDescargaSat.objects.order_by("-creado_en").first()
    sat_descarga_enabled = getattr(settings, "SAT_DESCARGA_ENABLED", False)
    if sat_ultimo_log and sat_ultimo_log.nivel == LogDescargaSat.NIVEL_ERROR:
        sat_estado_label = "Descarga SAT con error"
        sat_estado_tone = "is-warn"
    elif sat_descarga_enabled:
        sat_estado_label = "Descarga SAT activa"
        sat_estado_tone = "is-ok"
    else:
        sat_estado_label = "Descarga SAT pausada"
        sat_estado_tone = "is-muted"

    contexto.update(
        {
            "cuentas": CuentaBancaria.objects.filter(activa=True).order_by("banco"),
            "sat_cfdis_total": CfdiDescargado.objects.count(),
            "sat_estado_label": sat_estado_label,
            "sat_estado_tone": sat_estado_tone,
            "sat_ultimo_log": sat_ultimo_log,
            "periodo_resumen": periodo_resumen,
            "preview": preview,
            "preview_rows": preview.movimientos[:50] if preview else [],
            "movimiento_rows": _movimiento_rows(periodo_resumen["movimientos_rows"], periodo_resumen["candidatos"]),
        }
    )
    return render(request, "conciliacion/bancaria.html", contexto)


def _periodo_from_request(request: HttpRequest) -> tuple[int, int]:
    periodo = str(request.GET.get("periodo") or "")
    if len(periodo) == 7 and periodo[4] == "-":
        try:
            year = int(periodo[:4])
            month = int(periodo[5:])
        except ValueError:
            return periodo_default_conciliacion()
        if 1 <= month <= 12:
            return year, month
    return periodo_default_conciliacion()


def _handle_preview(request: HttpRequest) -> PreviewImportacion | None:
    cuenta_id = request.POST.get("cuenta")
    archivo = request.FILES.get("archivo")
    if not cuenta_id or not archivo:
        messages.error(request, "Selecciona una cuenta y un archivo.")
        return None
    try:
        cuenta = CuentaBancaria.objects.get(pk=cuenta_id, activa=True)
        preview = generar_preview(cuenta=cuenta, uploaded_file=archivo)
    except CuentaBancaria.DoesNotExist:
        messages.error(request, "Cuenta bancaria no valida.")
        return None
    except ImportacionBancariaError as exc:
        messages.error(request, str(exc))
        return None

    request.session[SESSION_PREVIEW_KEY] = preview.to_session_payload()
    request.session.modified = True
    messages.success(
        request,
        f"Revision lista: {len(preview.movimientos)} movimientos validos y {len(preview.errores)} filas con error.",
    )
    return preview


def _handle_confirm(request: HttpRequest) -> None:
    preview_payload = request.session.get(SESSION_PREVIEW_KEY)
    if not isinstance(preview_payload, dict):
        messages.error(request, "No hay revision pendiente para importar.")
        return
    try:
        preview = PreviewImportacion.from_session_payload(preview_payload)
        importacion = confirmar_importacion(preview=preview, user=request.user)
    except (ImportacionBancariaError, KeyError, TypeError, ValueError) as exc:
        messages.error(request, f"No se pudo confirmar la importacion: {exc}")
        return

    request.session.pop(SESSION_PREVIEW_KEY, None)
    request.session.modified = True
    log_event(
        request.user,
        "CREATE",
        "conciliacion.ImportacionBancaria",
        str(importacion.pk),
        {
            "cuenta": importacion.cuenta_id,
            "archivo_hash": importacion.archivo_hash,
            "nuevos": importacion.movimientos_nuevos,
            "duplicados": importacion.movimientos_duplicados,
        },
    )
    messages.success(
        request,
        (
            f"Importacion aplicada: {importacion.movimientos_nuevos} movimientos nuevos, "
            f"{importacion.movimientos_duplicados} duplicados."
        ),
    )


def _movimiento_rows(movimientos, candidatos: dict[int, list]) -> list[dict]:
    rows = []
    for movimiento in movimientos:
        rows.append({"movimiento": movimiento, "candidatos": candidatos.get(movimiento.pk, [])})
    return rows
