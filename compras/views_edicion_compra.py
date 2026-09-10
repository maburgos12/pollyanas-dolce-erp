"""Edición y registro de compra con el mismo contrato progresivo de acciones."""
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import FileResponse, Http404, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from .access_departamentales import puede_gestionar_compras_departamentales
from .forms_edicion_compra import EditarCotizacionDepartamentalForm, RegistrarCompraDepartamentalForm
from .models import (
    AvisoCompraDepartamental, CompraRealizadaDepartamental, CotizacionCompraDepartamental,
    ItemCompraDepartamental,
)
from .services_avisos_compra import enviar_aviso, reconciliar_incierto
from .services_edicion_compra import editar_cotizacion, registrar_compra_realizada, validar_edicion
from .views_departamentales import _es_direccion, _puede_ver_solicitud, _respuesta_accion


def _destino(item):
    return reverse('compras:departamental_detalle', args=[item.solicitud_id]) + f'#item-{item.pk}'


def _mostrar_formulario(request, item, form, *, titulo, explicacion, status=200):
    if status >= 400 and (request.headers.get('x-requested-with') == 'XMLHttpRequest' or 'application/json' in request.headers.get('accept', '')):
        message = ' '.join(str(error) for errors in form.errors.values() for error in errors)
        return JsonResponse({'ok': False, 'toast': {'type': 'error', 'message': message, 'persistent': True}}, status=status)
    return render(request, 'compras/departamentales/editar_compra_form.html', {
        'item': item, 'form': form, 'titulo': titulo, 'explicacion': explicacion, 'destino': _destino(item),
        'volver_adjuntar': status >= 400 and bool(request.FILES),
    }, status=status)


@login_required
@require_http_methods(['GET', 'POST'])
def departamental_cotizacion_editar(request, quote_pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    quote = get_object_or_404(CotizacionCompraDepartamental.objects.select_related('item__solicitud', 'proveedor'), pk=quote_pk)
    form = EditarCotizacionDepartamentalForm(request.POST or None, request.FILES or None, item=quote.item, instance=quote)
    config = {'titulo': 'Editar cotización', 'explicacion': 'Cada cambio conserva su versión anterior, autor y motivo. Si aumenta el importe de la cotización seleccionada, Dirección General debe volver a autorizarla.'}
    try:
        validar_edicion(quote.item)
    except ValidationError as exc:
        return _respuesta_accion(request, message='; '.join(exc.messages), redirect_url=_destino(quote.item), status=409)
    if request.method == 'POST':
        if form.is_valid():
            try:
                editar_cotizacion(quote, datos=form.cleaned_data, version=form.cleaned_data['version'], motivo=form.cleaned_data['motivo'], actor=request.user)
            except ValidationError as exc:
                form.add_error(None, '; '.join(exc.messages))
                return _mostrar_formulario(request, quote.item, form, status=409, **config)
            return _respuesta_accion(request, message='Cotización actualizada con historial. Revisa el estado de autorización del artículo.', redirect_url=_destino(quote.item))
        return _mostrar_formulario(request, quote.item, form, status=400, **config)
    return _mostrar_formulario(request, quote.item, form, **config)


@login_required
@require_http_methods(['GET', 'POST'])
def departamental_compra_registrar(request, item_pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    item = get_object_or_404(ItemCompraDepartamental.objects.select_related('solicitud'), pk=item_pk)
    seleccionada = item.cotizaciones.filter(seleccionada=True).first()
    form = RegistrarCompraDepartamentalForm(request.POST or None, request.FILES or None, initial={
        'fecha_compra': timezone.localdate(), 'importe_final': seleccionada.total_adquisicion.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if seleccionada else None,
        'cotizacion_id': seleccionada.pk if seleccionada else None, 'version': seleccionada.version if seleccionada else None,
    })
    config = {'titulo': 'Registrar compra realizada', 'explicacion': 'Registra una compra que ya realizaste y adjunta su comprobante. La entrega se registra por separado cuando llegue el artículo.'}
    if request.method == 'POST':
        if form.is_valid():
            try:
                registrar_compra_realizada(item, **form.cleaned_data, actor=request.user)
            except ValidationError as exc:
                form.add_error(None, '; '.join(exc.messages))
                return _mostrar_formulario(request, item, form, status=409, **config)
            return _respuesta_accion(request, message='Compra registrada. El artículo queda pendiente de entrega.', redirect_url=_destino(item))
        return _mostrar_formulario(request, item, form, status=400, **config)
    return _mostrar_formulario(request, item, form, **config)


@login_required
@require_http_methods(['GET'])
def departamental_compra_comprobante(request, pk):
    compra = get_object_or_404(CompraRealizadaDepartamental.objects.select_related('item__solicitud'), pk=pk)
    if not _puede_ver_solicitud(request.user, compra.item.solicitud):
        raise PermissionDenied
    try:
        response = FileResponse(compra.comprobante.open('rb'), as_attachment=True, filename=Path(compra.comprobante.name).name)
    except FileNotFoundError:
        raise Http404('No se encontró el comprobante.')
    response['Cache-Control'] = 'private, no-store'
    return response


@login_required
@require_http_methods(['POST'])
def departamental_aviso_reintentar(request, pk):
    """Reintenta un aviso fallido. Solo Compras o Dirección; nunca reenvía uno aceptado."""
    aviso = get_object_or_404(
        AvisoCompraDepartamental.objects.select_related('compra__item__solicitud', 'destinatario'), pk=pk
    )
    if not (puede_gestionar_compras_departamentales(request.user) or _es_direccion(request.user)):
        raise PermissionDenied
    destino = _destino(aviso.compra.item) + '-avisos'
    if aviso.estado == AvisoCompraDepartamental.ESTADO_ENVIADO:
        return _respuesta_accion(request, message=f'El aviso por {aviso.get_canal_display().lower()} ya se había enviado. No se reenvió.',
                                 redirect_url=destino, reload=True)
    if aviso.estado == AvisoCompraDepartamental.ESTADO_INCIERTO:
        aviso = reconciliar_incierto(aviso)
        if aviso.estado == AvisoCompraDepartamental.ESTADO_ENVIADO:
            return _respuesta_accion(request, message=f'El aviso por {aviso.get_canal_display().lower()} sí se había entregado al proveedor. No se reenvió.',
                                     redirect_url=destino, reload=True)
    aviso = enviar_aviso(aviso, actor=request.user)
    if aviso.estado == AvisoCompraDepartamental.ESTADO_ENVIADO:
        mensaje = f'Aviso por {aviso.get_canal_display().lower()} enviado al solicitante.'
        return _respuesta_accion(request, message=mensaje, redirect_url=destino, reload=True)
    mensaje = f'El aviso por {aviso.get_canal_display().lower()} sigue sin salir: {aviso.detalle or aviso.get_estado_display()}'
    return _respuesta_accion(request, message=mensaje, redirect_url=destino, status=409)
