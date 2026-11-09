import json

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.core.serializers import serialize
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.http import require_http_methods

from core.audit import log_event

from .access_departamentales import _areas_usuario, puede_gestionar_compras_departamentales
from .forms_gestion_solicitud import EditarArticulosFormSet, EditarSolicitudForm, EliminarSolicitudForm
from .models import SolicitudCompraDepartamental
from .services_solicitudes import (
    BLOQUEO_SOLICITUD, bloquear_items_solicitud, puede_modificar_solicitud,
    retirar_solicitud, solicitud_sin_atencion,
)
from .views_departamentales import _respuesta_accion


def _gestionar_solicitud(request, pk, *, editar):
    solicitudes = SolicitudCompraDepartamental.objects.all()
    solicitud = get_object_or_404(solicitudes, pk=pk)
    if not puede_modificar_solicitud(request.user, solicitud):
        raise PermissionDenied("Solo puedes modificar las solicitudes que creaste.")

    if request.method == "POST":
        bloquear_items_solicitud(pk)
        solicitud = solicitudes.select_for_update().get(pk=pk)
        if not puede_modificar_solicitud(request.user, solicitud):
            raise PermissionDenied("Ya no tienes permiso para modificar esta solicitud.")

    if not solicitud_sin_atencion(solicitud):
        return HttpResponse(BLOQUEO_SOLICITUD, status=409)
    version = solicitud.actualizado_en.isoformat()
    if request.method == "POST" and request.POST.get("version") != version:
        return HttpResponse("La solicitud cambió. Vuelve al detalle y revisa la versión actual.", status=409)

    detalle = reverse("compras:departamental_detalle", args=[pk])
    bound = request.POST if request.method == "POST" else None
    form = EliminarSolicitudForm(bound, initial={"version": version})
    items = None
    if editar:
        form = EditarSolicitudForm(bound, instance=solicitud, initial={"version": version})
        areas = form.fields["area"].queryset
        if not puede_gestionar_compras_departamentales(request.user):
            areas = areas.filter(pk__in=_areas_usuario(request.user))
        form.fields["area"].queryset = areas.filter(Q(activa=True) | Q(pk=solicitud.area_id))
        items = EditarArticulosFormSet(bound, request.FILES if bound is not None else None, instance=solicitud)
        if bound is not None:
            previo = json.loads(serialize("json", [solicitud, *solicitud.items.all()]))
            if form.is_valid() and items.is_valid():
                form.save()
                items.save()
                log_event(request.user, "COMPRA_SOLICITUD_EDITADA", "compras.SolicitudCompraDepartamental",
                          pk, {"folio": solicitud.folio, "antes": previo})
                return _respuesta_accion(request, message=f"Solicitud {solicitud.folio} actualizada.", redirect_url=detalle)
    elif bound is not None and form.is_valid():
        borrador = retirar_solicitud(solicitud, actor=request.user, motivo=form.cleaned_data["motivo"])
        accion = "eliminada" if borrador else "retirada"
        return _respuesta_accion(
            request, message=f"Solicitud {solicitud.folio} {accion}.",
            redirect_url=reverse("compras:departamental_mis_solicitudes") if borrador else detalle,
        )
    return render(request, "compras/departamentales/gestionar_solicitud.html", {
        "solicitud": solicitud, "form": form, "items": items, "editar": editar, "detalle": detalle,
    }, status=400 if bound is not None else 200)


@login_required
@require_http_methods(["GET", "POST"])
@transaction.atomic
def departamental_editar(request, pk):
    return _gestionar_solicitud(request, pk, editar=True)


@login_required
@require_http_methods(["GET", "POST"])
@transaction.atomic
def departamental_eliminar(request, pk):
    return _gestionar_solicitud(request, pk, editar=False)
