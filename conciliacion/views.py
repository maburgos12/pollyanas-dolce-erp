from __future__ import annotations

from datetime import timedelta

from django.contrib import messages
from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator
from django.db.models import Q
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from conciliacion.services.importador import (
    ImportacionBancariaError,
    PreviewImportacion,
    confirmar_importacion,
    generar_preview,
    periodo_default_conciliacion,
    resumen_periodo_conciliacion,
    resumen_conciliacion,
    sugerir_cfdis_para_movimientos,
)
from conciliacion.services.reglas_fiscales import regla_para_movimiento
from core.access import is_admin_or_dg
from core.audit import log_event
from sat_client.models import CfdiDescargado, LogDescargaSat
from syncfy_client.models import CuentaBancaria, MovimientoBancario


SESSION_PREVIEW_KEY = "conciliacion_bancaria_preview"
MOVIMIENTOS_PAGE_SIZE = 100


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
        elif action == "conciliar_movimiento":
            redirect_url = _handle_conciliar_movimiento(request)
            return redirect(redirect_url)

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
    movimientos_trabajo = _movimientos_trabajo_context(request, periodo_resumen)
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
            "movimiento_rows": movimientos_trabajo["rows"],
            "movimientos_page": movimientos_trabajo["page"],
            "movimientos_total_filtrado": movimientos_trabajo["total"],
            "movimientos_filtros": movimientos_trabajo["filtros"],
            "movimientos_querystring": movimientos_trabajo["querystring"],
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


def _handle_conciliar_movimiento(request: HttpRequest) -> str:
    periodo = str(request.POST.get("periodo") or "").strip()
    redirect_url = f"/conciliacion/bancaria/?periodo={periodo}#mesa-movimientos" if periodo else "/conciliacion/bancaria/#mesa-movimientos"
    movimiento_id = str(request.POST.get("movimiento_id") or "").strip()
    tipo_conciliacion = str(request.POST.get("tipo_conciliacion") or "").strip()
    nota = str(request.POST.get("nota_conciliacion") or "").strip()

    if tipo_conciliacion not in dict(MovimientoBancario.CONCILIACION_CHOICES):
        messages.error(request, "Selecciona una accion de conciliacion valida.")
        return redirect_url
    try:
        movimiento = MovimientoBancario.objects.select_related("cuenta").get(pk=movimiento_id)
    except (MovimientoBancario.DoesNotExist, ValueError):
        messages.error(request, "Movimiento bancario no encontrado.")
        return redirect_url

    if tipo_conciliacion == MovimientoBancario.CONCILIACION_CFDI:
        cfdi_uuid = str(request.POST.get("cfdi_uuid") or "").strip()
        try:
            cfdi = CfdiDescargado.objects.get(uuid=cfdi_uuid)
        except CfdiDescargado.DoesNotExist:
            messages.error(request, "Selecciona un CFDI valido para relacionar.")
            return redirect_url
        _marcar_movimiento_conciliado(
            movimiento,
            tipo_conciliacion=tipo_conciliacion,
            user=request.user,
            nota=nota,
            cfdi=cfdi,
        )
        cfdi.conciliado = True
        cfdi.save(update_fields=["conciliado"])
        messages.success(request, "Movimiento conciliado contra CFDI.")
        return redirect_url

    if tipo_conciliacion == MovimientoBancario.CONCILIACION_TRASPASO:
        contraparte_id = str(request.POST.get("contraparte_id") or "").strip()
        if not contraparte_id:
            _marcar_movimiento_conciliado(
                movimiento,
                tipo_conciliacion=tipo_conciliacion,
                user=request.user,
                nota=nota or "Traspaso entre cuentas propias sin contraparte importada o localizada.",
            )
            messages.success(request, "Movimiento marcado como traspaso entre cuentas sin contraparte ligada.")
            return redirect_url
        try:
            contraparte = MovimientoBancario.objects.select_related("cuenta").get(pk=contraparte_id)
        except (MovimientoBancario.DoesNotExist, ValueError):
            messages.error(request, "Selecciona una contraparte valida o deja el campo vacio si no esta importada.")
            return redirect_url
        if not _es_contraparte_valida(movimiento, contraparte):
            messages.error(request, "La contraparte debe ser otro movimiento con monto igual y tipo opuesto.")
            return redirect_url
        _marcar_movimiento_conciliado(
            movimiento,
            tipo_conciliacion=tipo_conciliacion,
            user=request.user,
            nota=nota,
            movimiento_relacionado=contraparte,
        )
        _marcar_movimiento_conciliado(
            contraparte,
            tipo_conciliacion=tipo_conciliacion,
            user=request.user,
            nota=nota or f"Contraparte de movimiento {movimiento.pk}",
            movimiento_relacionado=movimiento,
        )
        messages.success(request, "Traspaso entre cuentas conciliado con su contraparte.")
        return redirect_url

    _marcar_movimiento_conciliado(
        movimiento,
        tipo_conciliacion=tipo_conciliacion,
        user=request.user,
        nota=nota,
    )
    messages.success(request, "Movimiento clasificado y marcado como conciliado.")
    return redirect_url


def _marcar_movimiento_conciliado(
    movimiento: MovimientoBancario,
    *,
    tipo_conciliacion: str,
    user,
    nota: str = "",
    cfdi: CfdiDescargado | None = None,
    movimiento_relacionado: MovimientoBancario | None = None,
) -> None:
    movimiento.conciliado = True
    movimiento.tipo_conciliacion = tipo_conciliacion
    movimiento.nota_conciliacion = nota
    movimiento.cfdi_relacionado = cfdi
    movimiento.movimiento_relacionado = movimiento_relacionado
    movimiento.conciliado_por = user if getattr(user, "is_authenticated", False) else None
    movimiento.conciliado_en = timezone.now()
    movimiento.save(
        update_fields=[
            "conciliado",
            "tipo_conciliacion",
            "nota_conciliacion",
            "cfdi_relacionado",
            "movimiento_relacionado",
            "conciliado_por",
            "conciliado_en",
        ]
    )


def _es_contraparte_valida(movimiento: MovimientoBancario, contraparte: MovimientoBancario) -> bool:
    return (
        movimiento.pk != contraparte.pk
        and movimiento.tipo != contraparte.tipo
        and movimiento.monto == contraparte.monto
    )


def _movimientos_trabajo_context(request: HttpRequest, periodo_resumen: dict) -> dict:
    qs = MovimientoBancario.objects.select_related("cuenta").filter(
        fecha_transaccion__date__gte=periodo_resumen["periodo_inicio"],
        fecha_transaccion__date__lte=periodo_resumen["periodo_fin"],
    )
    cuenta = str(request.GET.get("cuenta") or "").strip()
    tipo = str(request.GET.get("tipo") or "").strip()
    busqueda = str(request.GET.get("q") or "").strip()

    if cuenta and cuenta.isdigit():
        qs = qs.filter(cuenta_id=cuenta)
    else:
        cuenta = ""
    if tipo in {MovimientoBancario.TIPO_ABONO, MovimientoBancario.TIPO_CARGO}:
        qs = qs.filter(tipo=tipo)
    else:
        tipo = ""
    if busqueda:
        qs = qs.filter(
            Q(descripcion__icontains=busqueda)
            | Q(cuenta__nombre_display__icontains=busqueda)
            | Q(cuenta__numero_cuenta__icontains=busqueda)
        )

    qs = qs.order_by("-fecha_transaccion", "cuenta__banco", "id")
    paginator = Paginator(qs, MOVIMIENTOS_PAGE_SIZE)
    page = paginator.get_page(request.GET.get("page") or 1)
    movimientos = list(page.object_list)
    candidatos = sugerir_cfdis_para_movimientos([mov for mov in movimientos if not mov.conciliado])
    query = request.GET.copy()
    query.pop("page", None)
    return {
        "rows": _movimiento_rows(movimientos, candidatos),
        "page": page,
        "total": paginator.count,
        "filtros": {"cuenta": cuenta, "tipo": tipo, "q": busqueda},
        "querystring": query.urlencode(),
    }


def _movimiento_rows(movimientos, candidatos: dict[int, list]) -> list[dict]:
    rows = []
    contrapartes = _contrapartes_por_movimiento(movimientos)
    for movimiento in movimientos:
        rows.append(
            {
                "movimiento": movimiento,
                "candidatos": candidatos.get(movimiento.pk, []),
                "contrapartes": contrapartes.get(movimiento.pk, []),
                "regla": regla_para_movimiento(movimiento),
            }
        )
    return rows


def _contrapartes_por_movimiento(movimientos) -> dict[int, list[MovimientoBancario]]:
    result = {}
    for movimiento in movimientos:
        opposite = MovimientoBancario.TIPO_ABONO if movimiento.tipo == MovimientoBancario.TIPO_CARGO else MovimientoBancario.TIPO_CARGO
        start = movimiento.fecha_transaccion - timedelta(days=5)
        end = movimiento.fecha_transaccion + timedelta(days=5)
        result[movimiento.pk] = list(
            MovimientoBancario.objects.select_related("cuenta")
            .filter(
                conciliado=False,
                tipo=opposite,
                monto=movimiento.monto,
                fecha_transaccion__gte=start,
                fecha_transaccion__lte=end,
            )
            .exclude(pk=movimiento.pk)
            .order_by("fecha_transaccion", "cuenta__banco")[:5]
        )
    return result
