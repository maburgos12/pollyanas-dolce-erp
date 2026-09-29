"""Formularios progresivos para cancelar intentos, registrar reembolsos y cerrar artículos."""

from django import forms
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from .access_departamentales import puede_gestionar_compras_departamentales
from .forms_intentos_compra import CancelarIntentoCompraForm, RegistrarReembolsoCompraForm
from .models import IntentoCompraDepartamental, ItemCompraDepartamental
from .services_intentos_compra import (
    cancelar_articulo_definitivamente, cancelar_intento_compra, registrar_reembolso_compra,
)
from .views_departamentales import _respuesta_accion


class CancelarArticuloForm(forms.Form):
    version = forms.CharField(widget=forms.HiddenInput)
    motivo = forms.CharField(
        label="Motivo de cancelación", max_length=2000,
        widget=forms.Textarea(attrs={"rows": 3}),
    )

    def __init__(self, *args, item, **kwargs):
        super().__init__(*args, **kwargs)
        self.initial.setdefault("version", item.actualizado_en.isoformat())

    def clean_motivo(self):
        motivo = self.cleaned_data["motivo"].strip()
        if not motivo:
            raise ValidationError("Explica por qué ya no se comprará el artículo.")
        return motivo


def _destino(item):
    return reverse("compras:departamental_detalle", args=[item.solicitud_id]) + f"#item-{item.pk}"


def _es_async(request):
    return request.headers.get("x-requested-with") == "XMLHttpRequest" or "application/json" in request.headers.get("accept", "")


def _respuesta_exito(request, *, message, item):
    destino = _destino(item)
    if _es_async(request):
        return JsonResponse({
            "ok": True, "message": message,
            "toast": {"type": "success", "message": message},
            "redirect": destino, "redirect_url": destino, "reload": True,
        })
    return _respuesta_accion(request, message=message, redirect_url=destino, reload=True)


def _formulario(request, *, item, form, titulo, explicacion, estado, action, confirmacion="", status=200):
    if status >= 400 and _es_async(request):
        mensaje = "; ".join(str(error) for errores in form.errors.values() for error in errores)
        return JsonResponse({
            "ok": False, "message": mensaje,
            "toast": {"type": "error", "message": mensaje, "persistent": True},
            "errors": form.errors.get_json_data(),
        }, status=status)
    return render(request, "compras/departamentales/partials/accion_intento_form.html", {
        "item": item, "form": form, "titulo": titulo, "explicacion": explicacion,
        "estado": estado, "action": action, "confirmacion": confirmacion,
        "destino": _destino(item), "volver_adjuntar": status >= 400 and bool(request.FILES),
    }, status=status)


def _error_servicio(request, *, error, item, form, titulo, explicacion, estado, action, confirmacion=""):
    mensaje = "; ".join(error.messages)
    conflicto = "Otra persona actualizó" in mensaje or mensaje in {
        "Solo se puede cancelar el intento vigente.",
        "El intento no tiene un reembolso pendiente.",
        "El artículo ya está cancelado.",
    }
    if conflicto and "Otra persona actualizó" not in mensaje:
        mensaje = f"Otra persona actualizó este registro. Recarga y revisa los cambios. {mensaje}"
    form.add_error(None, mensaje)
    status = 409 if conflicto else 400
    return _formulario(
        request, item=item, form=form, titulo=titulo, explicacion=explicacion,
        estado=estado, action=action, confirmacion=confirmacion, status=status,
    )


@login_required
@require_http_methods(["GET", "POST"])
def departamental_intento_cancelar(request, pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    intento = get_object_or_404(
        IntentoCompraDepartamental.objects.select_related("item__solicitud", "cotizacion__proveedor"), pk=pk,
    )
    item = intento.item
    form = CancelarIntentoCompraForm(request.POST or None, request.FILES or None, intento=intento)
    config = {
        "item": item, "titulo": "Cancelar intento con proveedor",
        "explicacion": "El historial de la compra permanece visible. Si ya se pagó, solicita aquí el reembolso.",
        "estado": intento.get_estado_display(),
        "action": reverse("compras:departamental_intento_cancelar", args=[intento.pk]),
        "confirmacion": "Se cancelará este intento con el proveedor y se conservará todo su historial. ¿Continuar?",
    }
    if request.method == "POST":
        if not form.is_valid():
            return _formulario(request, form=form, status=400, **config)
        try:
            cancelar_intento_compra(
                intento, actor=request.user, **form.cleaned_data,
            )
        except ValidationError as exc:
            return _error_servicio(request, error=exc, form=form, **config)
        return _respuesta_exito(request, message="Intento cancelado; el historial permanece disponible.", item=item)
    return _formulario(request, form=form, **config)


@login_required
@require_http_methods(["GET", "POST"])
def departamental_reembolso_registrar(request, pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    intento = get_object_or_404(IntentoCompraDepartamental.objects.select_related("item__solicitud"), pk=pk)
    item = intento.item
    form = RegistrarReembolsoCompraForm(
        request.POST or None, request.FILES or None, intento=intento,
        initial={"fecha": timezone.localdate()},
    )
    config = {
        "item": item, "titulo": "Registrar reembolso recibido",
        "explicacion": "Registra únicamente el dinero ya devuelto por el proveedor. El saldo pendiente seguirá visible.",
        "estado": intento.get_estado_display(),
        "action": reverse("compras:departamental_reembolso_registrar", args=[intento.pk]),
    }
    if request.method == "POST":
        try:
            version_enviada = form.fields["version"].clean(request.POST.get("version"))
        except ValidationError:
            version_enviada = None
        if version_enviada is not None and version_enviada != intento.version:
            return _error_servicio(
                request,
                error=ValidationError("Otra persona actualizó este intento. Recarga y revisa el saldo."),
                form=form, **config,
            )
        if not form.is_valid():
            return _formulario(request, form=form, status=400, **config)
        try:
            registrar_reembolso_compra(intento, actor=request.user, **form.cleaned_data)
        except ValidationError as exc:
            return _error_servicio(request, error=exc, form=form, **config)
        return _respuesta_exito(request, message="Reembolso registrado en el historial.", item=item)
    return _formulario(request, form=form, **config)


@login_required
@require_http_methods(["GET", "POST"])
def departamental_articulo_cancelar(request, item_pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    item = get_object_or_404(ItemCompraDepartamental.objects.select_related("solicitud"), pk=item_pk)
    form = CancelarArticuloForm(request.POST or None, item=item)
    config = {
        "item": item, "titulo": "Cancelar artículo definitivamente",
        "explicacion": "Se conservarán los intentos anteriores y el motivo de cierre.",
        "estado": item.get_estado_display(),
        "action": reverse("compras:departamental_articulo_cancelar", args=[item.pk]),
        "confirmacion": "Este artículo dejará de buscarse y la solicitud puede cerrarse. ¿Continuar?",
    }
    if request.method == "POST":
        if not form.is_valid():
            return _formulario(request, form=form, status=400, **config)
        try:
            with transaction.atomic():
                bloqueado = ItemCompraDepartamental.objects.select_for_update().get(pk=item.pk)
                if form.cleaned_data["version"] != bloqueado.actualizado_en.isoformat():
                    raise ValidationError("Otra persona actualizó este artículo. Recarga y revisa los cambios.")
                cancelar_articulo_definitivamente(bloqueado, motivo=form.cleaned_data["motivo"], actor=request.user)
        except ValidationError as exc:
            return _error_servicio(request, error=exc, form=form, **config)
        return _respuesta_exito(request, message="Artículo cancelado; se conserva su historial.", item=item)
    return _formulario(request, form=form, **config)
