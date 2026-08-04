from datetime import date
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Count, Q, Sum
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.access import ROLE_DG, has_any_role
from maestros.models import Proveedor
from reportes.models import AreaPresupuesto, AreaPresupuestoResponsable, RubroPresupuesto

from .models import (
    CompromisoCompraDepartamental,
    CotizacionCompraDepartamental,
    ItemCompraDepartamental,
    RecepcionItemDepartamental,
    SolicitudCompraDepartamental,
)
from .access_departamentales import puede_gestionar_compras_departamentales
from .services_departamentales import (
    confirmar_recepcion_departamental,
    decidir_exceso,
    evaluar_presupuesto_item,
    generar_ordenes_departamentales,
    seleccionar_cotizacion,
)


def _areas_usuario(user):
    return AreaPresupuesto.objects.filter(
        activa=True,
        responsables__usuario=user,
        responsables__puede_capturar=True,
    ).distinct()


def _es_direccion(user):
    return (
        user.is_superuser
        or has_any_role(user, ROLE_DG)
        or user.has_perm("compras.decidir_exceso_compra_departamental")
    )


def _puede_ver_solicitud(user, solicitud):
    return (
        puede_gestionar_compras_departamentales(user)
        or _es_direccion(user)
        or AreaPresupuestoResponsable.objects.filter(
            area=solicitud.area, usuario=user, puede_capturar=True
        ).exists()
    )


def _respuesta_accion(request, *, message, redirect_url, status=200):
    if request.headers.get("x-requested-with") == "XMLHttpRequest" or "application/json" in request.headers.get("accept", ""):
        return JsonResponse(
            {
                "ok": status < 400,
                "toast": {"type": "success" if status < 400 else "error", "message": message},
                "redirect": redirect_url,
            },
            status=status,
        )
    if status < 400:
        messages.success(request, message)
        return redirect(redirect_url)
    messages.error(request, message)
    return redirect(redirect_url)


def _error_nueva(request, message, areas):
    if request.headers.get("x-requested-with") == "XMLHttpRequest" or "application/json" in request.headers.get("accept", ""):
        return JsonResponse(
            {"ok": False, "toast": {"type": "error", "message": message, "persistent": True}},
            status=400,
        )
    messages.error(request, message)
    return render(
        request,
        "compras/departamentales/nueva.html",
        {"areas": areas, "tipos": SolicitudCompraDepartamental.TIPO_CHOICES, "form_data": request.POST},
        status=400,
    )


@login_required
def departamental_inicio(request):
    if puede_gestionar_compras_departamentales(request.user):
        return redirect("compras:departamental_bandeja")
    if _es_direccion(request.user):
        return redirect("compras:departamental_direccion")
    return redirect("compras:departamental_mis_solicitudes")


@login_required
def departamental_mis_solicitudes(request):
    areas = _areas_usuario(request.user)
    if not areas.exists() and not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied("No tienes un área asignada para solicitudes departamentales.")
    solicitudes = (
        SolicitudCompraDepartamental.objects.filter(area__in=areas)
        .select_related("area", "solicitante", "comprador_asignado")
        .prefetch_related("items")
    )
    return render(
        request,
        "compras/departamentales/lista.html",
        {
            "solicitudes": solicitudes,
            "areas": areas,
            "vista": "area",
            "puede_solicitar": areas.exists(),
            "es_compras": puede_gestionar_compras_departamentales(request.user),
            "es_direccion": _es_direccion(request.user),
        },
    )


@login_required
def departamental_nueva(request):
    areas = _areas_usuario(request.user)
    if not areas.exists() and not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied("No tienes un área asignada para solicitar compras.")
    if request.method == "GET":
        return render(
            request,
            "compras/departamentales/nueva.html",
            {"areas": areas, "tipos": SolicitudCompraDepartamental.TIPO_CHOICES},
        )

    area = get_object_or_404(AreaPresupuesto, pk=request.POST.get("area"), activa=True)
    if not puede_gestionar_compras_departamentales(request.user) and not areas.filter(pk=area.pk).exists():
        raise PermissionDenied("Solo puedes solicitar compras para tu área asignada.")
    try:
        periodo = date.fromisoformat(f"{request.POST.get('periodo')}-01")
    except (TypeError, ValueError):
        return _error_nueva(request, "Selecciona el mes de planeación.", areas)

    tipo = request.POST.get("tipo") or SolicitudCompraDepartamental.TIPO_MENSUAL
    accion = request.POST.get("accion") or "borrador"
    if accion == "enviar" and tipo == SolicitudCompraDepartamental.TIPO_MENSUAL:
        today = timezone.localdate()
        if not 20 <= today.day <= 25:
            return _error_nueva(request, "La solicitud mensual se envía del día 20 al 25. Puedes guardarla como borrador o marcarla extraordinaria.", areas)
        siguiente_mes = date(today.year + (today.month == 12), 1 if today.month == 12 else today.month + 1, 1)
        if periodo != siguiente_mes:
            return _error_nueva(request, "La solicitud mensual debe corresponder al mes siguiente.", areas)

    descripciones = request.POST.getlist("descripcion")
    if not any(value.strip() for value in descripciones):
        return _error_nueva(request, "Agrega al menos un artículo.", areas)

    with transaction.atomic():
        solicitud = SolicitudCompraDepartamental(
            area=area,
            solicitante=request.user,
            tipo=tipo,
            periodo=periodo,
            motivo=request.POST.get("motivo", "").strip(),
            justificacion_extraordinaria=request.POST.get("justificacion_extraordinaria", "").strip(),
            estado=(SolicitudCompraDepartamental.ESTADO_ENVIADA if accion == "enviar" else SolicitudCompraDepartamental.ESTADO_BORRADOR),
            enviada_en=timezone.now() if accion == "enviar" else None,
        )
        try:
            solicitud.full_clean()
        except ValidationError as exc:
            transaction.set_rollback(True)
            return _error_nueva(request, "; ".join(exc.messages), areas)
        solicitud.save()
        cantidades = request.POST.getlist("cantidad")
        unidades = request.POST.getlist("unidad")
        categorias = request.POST.getlist("categoria")
        estimados = request.POST.getlist("costo_unitario_estimado")
        prioridades = request.POST.getlist("prioridad")
        fechas = request.POST.getlist("fecha_requerida")
        for index, descripcion in enumerate(descripciones):
            if not descripcion.strip():
                continue
            try:
                cantidad = Decimal(cantidades[index] or "1")
                estimado = Decimal(estimados[index]) if index < len(estimados) and estimados[index] else None
            except (InvalidOperation, IndexError):
                transaction.set_rollback(True)
                return _error_nueva(request, "Cantidad o costo estimado inválido.", areas)
            if cantidad <= 0 or (estimado is not None and estimado < 0):
                transaction.set_rollback(True)
                return _error_nueva(request, "La cantidad debe ser mayor a cero y el costo no puede ser negativo.", areas)
            item = ItemCompraDepartamental(
                solicitud=solicitud,
                descripcion=descripcion.strip(),
                cantidad=cantidad,
                unidad=unidades[index] if index < len(unidades) and unidades[index] else "pieza",
                categoria=categorias[index].strip() if index < len(categorias) else "",
                costo_unitario_estimado=estimado,
                prioridad=prioridades[index] if index < len(prioridades) and prioridades[index] else ItemCompraDepartamental.PRIORIDAD_NORMAL,
                fecha_requerida=date.fromisoformat(fechas[index]) if index < len(fechas) and fechas[index] else None,
                imagen=request.FILES.get(f"imagen_{index}"),
                estado=ItemCompraDepartamental.ESTADO_POR_REVISAR,
            )
            item.full_clean()
            item.save()
    return _respuesta_accion(
        request,
        message=f"Solicitud {solicitud.folio} {'enviada a Administración' if accion == 'enviar' else 'guardada como borrador'}.",
        redirect_url=reverse("compras:departamental_detalle", args=[solicitud.pk]),
    )


@login_required
def departamental_detalle(request, pk):
    solicitud = get_object_or_404(
        SolicitudCompraDepartamental.objects.select_related("area", "solicitante", "comprador_asignado").prefetch_related(
            "items__cotizaciones__proveedor", "items__eventos"
        ),
        pk=pk,
    )
    if not _puede_ver_solicitud(request.user, solicitud):
        raise PermissionDenied("No puedes consultar esta solicitud.")
    rubros = RubroPresupuesto.objects.filter(area=solicitud.area, activo=True).order_by("concepto")
    proveedores = Proveedor.objects.filter(activo=True).order_by("nombre")
    total_solicitado = Decimal("0")
    total_cotizado = Decimal("0")
    total_comprometido = Decimal("0")
    total_gastado = Decimal("0")
    for item in solicitud.items.all():
        if item.subtotal_estimado is not None:
            total_solicitado += item.subtotal_estimado
        seleccionada = next((quote for quote in item.cotizaciones.all() if quote.seleccionada), None)
        if seleccionada:
            total_cotizado += seleccionada.total_adquisicion
            item.evaluacion_presupuesto = evaluar_presupuesto_item(item, seleccionada.total_adquisicion)
        compromiso = CompromisoCompraDepartamental.objects.filter(
            item=item, activo=True, formalizado_en__isnull=False
        ).first()
        if compromiso:
            total_comprometido += compromiso.monto
        total_gastado += item.monto_gastado
    return render(
        request,
        "compras/departamentales/detalle.html",
        {
            "solicitud": solicitud,
            "rubros": rubros,
            "proveedores": proveedores,
            "es_compras": puede_gestionar_compras_departamentales(request.user),
            "es_direccion": _es_direccion(request.user),
            "total_solicitado": total_solicitado,
            "total_cotizado": total_cotizado,
            "total_comprometido": total_comprometido,
            "total_gastado": total_gastado,
        },
    )


@login_required
def departamental_bandeja(request):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied("La bandeja compartida corresponde a Administración.")
    items = ItemCompraDepartamental.objects.select_related(
        "solicitud__area", "solicitud__solicitante", "solicitud__comprador_asignado"
    ).exclude(estado__in=[ItemCompraDepartamental.ESTADO_RECIBIDO_CONFORME, ItemCompraDepartamental.ESTADO_RECHAZADO, ItemCompraDepartamental.ESTADO_CANCELADO])
    return render(
        request,
        "compras/departamentales/bandeja.html",
        {
            "items": items,
            "vista": "compras",
            "puede_solicitar": _areas_usuario(request.user).exists(),
            "es_compras": True,
            "es_direccion": _es_direccion(request.user),
        },
    )


@login_required
def departamental_direccion(request):
    if not _es_direccion(request.user):
        raise PermissionDenied("Esta bandeja corresponde a Dirección General.")
    items = ItemCompraDepartamental.objects.filter(
        estado=ItemCompraDepartamental.ESTADO_ESPERANDO_DG
    ).select_related("solicitud__area", "solicitud__solicitante")
    return render(
        request,
        "compras/departamentales/direccion.html",
        {
            "items": items,
            "vista": "direccion",
            "puede_solicitar": _areas_usuario(request.user).exists(),
            "es_compras": puede_gestionar_compras_departamentales(request.user),
            "es_direccion": True,
        },
    )


@login_required
@require_POST
def departamental_asignar(request, pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    solicitud = get_object_or_404(SolicitudCompraDepartamental, pk=pk)
    solicitud.comprador_asignado = request.user
    solicitud.estado = SolicitudCompraDepartamental.ESTADO_EN_ATENCION
    solicitud.save(update_fields=["comprador_asignado", "estado", "actualizado_en"])
    solicitud.items.filter(estado=ItemCompraDepartamental.ESTADO_POR_REVISAR).update(
        estado=ItemCompraDepartamental.ESTADO_POR_COTIZAR,
        siguiente_responsable=ItemCompraDepartamental.RESPONSABLE_COMPRAS,
    )
    return _respuesta_accion(request, message="Solicitud asignada; los artículos quedaron por cotizar.", redirect_url=reverse("compras:departamental_detalle", args=[pk]))


@login_required
@require_POST
def departamental_cotizar(request, item_pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    item = get_object_or_404(ItemCompraDepartamental.objects.select_related("solicitud"), pk=item_pk)
    cotizacion = CotizacionCompraDepartamental.objects.create(
        item=item,
        proveedor=get_object_or_404(Proveedor, pk=request.POST.get("proveedor"), activo=True),
        documento=request.FILES.get("documento"),
        cantidad_ofertada=Decimal(request.POST.get("cantidad_ofertada") or item.cantidad),
        costo_unitario=Decimal(request.POST.get("costo_unitario")),
        descuento=Decimal(request.POST.get("descuento") or "0"),
        impuestos=Decimal(request.POST.get("impuestos") or "0"),
        envio=Decimal(request.POST.get("envio") or "0"),
        instalacion=Decimal(request.POST.get("instalacion") or "0"),
        otros_cargos=Decimal(request.POST.get("otros_cargos") or "0"),
        garantia_observaciones=request.POST.get("garantia_observaciones", "").strip(),
    )
    if request.POST.get("seleccionar"):
        seleccionar_cotizacion(cotizacion, actor=request.user)
    else:
        item.estado = ItemCompraDepartamental.ESTADO_COTIZANDO
        item.save(update_fields=["estado", "actualizado_en"])
    return _respuesta_accion(request, message="Cotización guardada.", redirect_url=reverse("compras:departamental_detalle", args=[item.solicitud_id]))


@login_required
@require_POST
def departamental_decidir(request, item_pk):
    if not _es_direccion(request.user):
        raise PermissionDenied
    item = get_object_or_404(ItemCompraDepartamental, pk=item_pk)
    decidir_exceso(item, decision=request.POST.get("decision", ""), comentario=request.POST.get("comentario", ""), actor=request.user)
    return _respuesta_accion(request, message="Decisión registrada con trazabilidad.", redirect_url=reverse("compras:departamental_detalle", args=[item.solicitud_id]))


@login_required
@require_POST
def departamental_generar_ordenes(request, pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    solicitud = get_object_or_404(SolicitudCompraDepartamental, pk=pk)
    items = list(solicitud.items.filter(estado=ItemCompraDepartamental.ESTADO_AUTORIZADO))
    if not items:
        raise ValidationError("No hay artículos autorizados pendientes de orden.")
    ordenes = generar_ordenes_departamentales(items, actor=request.user)
    return _respuesta_accion(request, message=f"Se generaron {len(ordenes)} órdenes agrupadas por proveedor.", redirect_url=reverse("compras:departamental_detalle", args=[pk]))


@login_required
@require_POST
def departamental_recibir(request, item_pk):
    if not puede_gestionar_compras_departamentales(request.user):
        raise PermissionDenied
    item = get_object_or_404(ItemCompraDepartamental, pk=item_pk)
    linea = item.linea_orden
    RecepcionItemDepartamental.objects.create(
        linea_orden=linea,
        cantidad_recibida=Decimal(request.POST.get("cantidad_recibida")),
        observaciones=request.POST.get("observaciones", "").strip(),
        registrado_por=request.user,
    )
    return _respuesta_accion(request, message="Recepción registrada; el pendiente continuará visible hasta confirmación del área.", redirect_url=reverse("compras:departamental_detalle", args=[item.solicitud_id]))


@login_required
@require_POST
def departamental_confirmar(request, item_pk):
    item = get_object_or_404(ItemCompraDepartamental.objects.select_related("solicitud__area"), pk=item_pk)
    if not AreaPresupuestoResponsable.objects.filter(area=item.solicitud.area, usuario=request.user, puede_capturar=True).exists():
        raise PermissionDenied
    confirmar_recepcion_departamental(
        item,
        conforme=request.POST.get("conforme") == "1",
        comentario=request.POST.get("comentario", ""),
        actor=request.user,
    )
    return _respuesta_accion(request, message="Confirmación de recepción registrada.", redirect_url=reverse("compras:departamental_detalle", args=[item.solicitud_id]))
