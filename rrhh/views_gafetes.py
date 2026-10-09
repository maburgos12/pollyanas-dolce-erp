"""Emisión privada y verificación pública de gafetes de RRHH."""
import csv
import io
import uuid
from zipfile import ZIP_DEFLATED, ZipFile

import segno
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.text import slugify
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_POST, require_safe

from core.access import can_manage_submodule, can_view_submodule
from core.models import AuditLog
from core.notificaciones import PUBLIC_BASE_URL
from .models import Empleado
from .views import _module_tabs


def _can_manage(user):
    return can_manage_submodule(user, "rrhh", "empleados")


def _url(empleado):
    return PUBLIC_BASE_URL + reverse("verificar_gafete", args=[empleado.gafete_token])


def _qr(empleado):
    buffer = io.BytesIO()
    segno.make_qr(_url(empleado), error="h").save(buffer, kind="svg", scale=8, border=4)
    return buffer.getvalue()


def _filename(empleado):
    return f"qr-{slugify(empleado.codigo) or 'colaborador'}-{empleado.pk}.svg"


def _csv_text(value):
    # Evitar fórmulas al abrir nombres o códigos capturados en una hoja de cálculo.
    value = str(value)
    return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value


@never_cache
@require_safe
def verificar_gafete(request, token):
    empleado = Empleado.objects.filter(gafete_token=token, activo=True).values(
        "nombre", "codigo", "fecha_ingreso"
    ).first()
    response = render(request, "rrhh/gafete_publico.html", {"empleado": empleado}, status=200 if empleado else 404)
    response["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    response["Referrer-Policy"] = "no-referrer"
    return response


@login_required
@never_cache
@require_safe
def gafetes(request):
    if not can_view_submodule(request.user, "rrhh", "empleados"):
        raise PermissionDenied()
    empleados = Empleado.objects.only("nombre", "codigo", "fecha_ingreso", "activo", "gafete_token").order_by("nombre", "pk")
    query = request.GET.get("q", "").strip()
    if query:
        empleados = empleados.filter(Q(nombre__icontains=query) | Q(codigo__icontains=query))
    return render(request, "rrhh/gafetes.html", {
        "empleados": empleados, "q": query, "can_manage": _can_manage(request.user),
        "module_tabs": _module_tabs("gafetes", request.user),
    })


@login_required
@never_cache
@require_safe
def descargar_qr(request, pk):
    if not _can_manage(request.user):
        raise PermissionDenied()
    empleado = get_object_or_404(Empleado, pk=pk, activo=True, gafete_token__isnull=False)
    response = HttpResponse(_qr(empleado), content_type="image/svg+xml")
    response["Content-Disposition"] = f'attachment; filename="{_filename(empleado)}"'
    return response


@login_required
@never_cache
@require_safe
def descargar_gafetes(request):
    if not _can_manage(request.user):
        raise PermissionDenied()
    empleados = Empleado.objects.filter(activo=True, gafete_token__isnull=False).only(
        "codigo", "nombre", "fecha_ingreso", "gafete_token"
    ).order_by("nombre", "pk")
    buffer = io.BytesIO()
    manifest = io.StringIO(newline="")
    writer = csv.writer(manifest)
    writer.writerow(["codigo_colaborador", "nombre", "fecha_ingreso", "archivo_qr", "liga_verificacion"])
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        for empleado in empleados:
            filename = _filename(empleado)
            archive.writestr(filename, _qr(empleado))
            writer.writerow([_csv_text(empleado.codigo), _csv_text(empleado.nombre), empleado.fecha_ingreso.isoformat(), filename, _url(empleado)])
        archive.writestr("colaboradores.csv", manifest.getvalue().encode("utf-8-sig"))
    response = HttpResponse(buffer.getvalue(), content_type="application/zip")
    response["Content-Disposition"] = 'attachment; filename="gafetes-pollyanas.zip"'
    return response


@login_required
@require_POST
@transaction.atomic
def accion_gafete(request, pk):
    if not _can_manage(request.user):
        raise PermissionDenied()
    empleado = get_object_or_404(Empleado.objects.select_for_update(), pk=pk)
    action = request.POST.get("accion")
    if action not in {"revocar", "emitir"}:
        return HttpResponse("Acción inválida.", status=400)
    if request.POST.get("token_actual") != str(empleado.gafete_token or ""):
        messages.error(request, "El gafete cambió. Revisa su estado antes de continuar.")
        return redirect("rrhh:gafetes")
    if action == "emitir" and (not empleado.activo or empleado.gafete_token):
        messages.error(request, "Solo puedes emitir un gafete para un colaborador activo sin QR vigente.")
        return redirect("rrhh:gafetes")
    token = uuid.uuid4() if action == "emitir" else None
    Empleado.objects.filter(pk=empleado.pk).update(gafete_token=token, updated_at=timezone.now())
    AuditLog.objects.create(user=request.user, action="UPDATE", model="rrhh.Empleado", object_id=str(empleado.pk), payload={"gafete": action})
    messages.success(request, "Gafete emitido. Descarga el nuevo QR." if token else "Gafete revocado. El QR anterior ya no tiene vigencia.")
    return redirect("rrhh:gafetes")
