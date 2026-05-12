from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Count, Q, Sum
from django.http import HttpResponseBadRequest
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from core.access import ACCESS_MANAGE, ACCESS_VIEW
from core.models import Sucursal, UserModuleAccess, sucursales_operativas
from logistica.models import Repartidor
from recetas.models import Receta

from .models import MermaEvidencia, MermaProducto, MermaRegistro, PersonalEnviosSucursal


def _explicit_access(user, *scopes: str) -> str:
    if not user or not user.is_authenticated:
        return UserModuleAccess.ACCESS_NONE
    if user.is_superuser:
        return ACCESS_MANAGE
    modules = ["mermas", *[f"mermas.{scope}" for scope in scopes]]
    access_levels = list(
        UserModuleAccess.objects.filter(user=user, module__in=modules).values_list("access", flat=True)
    )
    if ACCESS_MANAGE in access_levels:
        return ACCESS_MANAGE
    if ACCESS_VIEW in access_levels:
        return ACCESS_VIEW
    return UserModuleAccess.ACCESS_NONE


def _can_dashboard(user) -> bool:
    return _explicit_access(user, "dashboard") in {ACCESS_VIEW, ACCESS_MANAGE}


def _can_capture(user) -> bool:
    return _explicit_access(user, "captura") in {ACCESS_VIEW, ACCESS_MANAGE}


def _can_receive(user) -> bool:
    if _explicit_access(user, "recepcion") == ACCESS_MANAGE:
        return True
    return PersonalEnviosSucursal.objects.filter(user=user, activo=True).exists()


def _can_manage_mermas(user) -> bool:
    return _explicit_access(user) == ACCESS_MANAGE


def _require_dashboard(user):
    if not (_can_dashboard(user) or _can_receive(user)):
        raise PermissionDenied("No tienes acceso al panel de mermas.")


def _require_capture(user):
    if not _can_capture(user):
        raise PermissionDenied("No tienes acceso a la app de captura de mermas.")


def _require_mermas(user):
    if not (_can_dashboard(user) or _can_capture(user) or _can_receive(user)):
        raise PermissionDenied("No tienes acceso al módulo de mermas.")


def _require_receive(user):
    if not _can_receive(user):
        raise PermissionDenied("No tienes permiso para recepción CEDIS de mermas.")


def _sucursal_usuario(user):
    profile = getattr(user, "userprofile", None)
    return getattr(profile, "sucursal", None)


def _registros_visibles(user):
    qs = (
        MermaRegistro.objects.select_related(
            "sucursal",
            "registrado_por",
            "repartidor__user",
            "enviado_por",
            "recibido_por",
        )
        .prefetch_related("productos", "evidencias")
        .all()
    )
    if _can_dashboard(user) or _can_receive(user) or _can_manage_mermas(user):
        return qs
    sucursal = _sucursal_usuario(user)
    if sucursal:
        return qs.filter(sucursal=sucursal)
    return qs.filter(registrado_por=user)


@login_required
def dashboard(request):
    _require_dashboard(request.user)
    registros = _registros_visibles(request.user)
    estatus = request.GET.get("estatus", "").strip()
    sucursal_id = request.GET.get("sucursal", "").strip()
    if estatus:
        registros = registros.filter(estatus=estatus)
    if sucursal_id:
        registros = registros.filter(sucursal_id=sucursal_id)

    hoy = timezone.localdate()
    base = _registros_visibles(request.user)
    summary = {
        "hoy": base.filter(iniciado_en__date=hoy).count(),
        "enviado": base.filter(estatus=MermaRegistro.ESTATUS_ENVIADO_CEDIS).count(),
        "diferencias": base.filter(estatus=MermaRegistro.ESTATUS_RECIBIDO_DIFERENCIA).count(),
        "productos": base.aggregate(total=Count("productos"))["total"] or 0,
        "cantidad": base.aggregate(total=Sum("productos__cantidad_enviada"))["total"] or Decimal("0"),
    }
    cola_cedis = base.filter(estatus=MermaRegistro.ESTATUS_ENVIADO_CEDIS).order_by("enviado_en")[:8]
    diferencias = base.filter(alerta_ventas=True).order_by("-recibido_en")[:8]
    sucursales_foco = (
        base.values("sucursal__id", "sucursal__nombre")
        .annotate(registros=Count("id"), productos=Count("productos"))
        .order_by("-registros", "sucursal__nombre")[:10]
    )

    return render(
        request,
        "mermas/dashboard.html",
        {
            "registros": registros[:80],
            "summary": summary,
            "cola_cedis": cola_cedis,
            "diferencias": diferencias,
            "sucursales_foco": sucursales_foco,
            "sucursales": sucursales_operativas(),
            "estatus_choices": MermaRegistro.ESTATUS_CHOICES,
            "filters": {"estatus": estatus, "sucursal": sucursal_id},
            "can_manage_mermas": _can_manage_mermas(request.user),
            "can_capture_mermas": _can_capture(request.user),
        },
    )


def _producto_rows_from_post(post):
    rows = []
    receta_ids = post.getlist("receta_id[]")
    textos = post.getlist("producto_texto[]")
    cantidades = post.getlist("cantidad[]")
    max_len = max(len(receta_ids), len(textos), len(cantidades), 0)
    for idx in range(max_len):
        receta_id = receta_ids[idx].strip() if idx < len(receta_ids) else ""
        texto = textos[idx].strip() if idx < len(textos) else ""
        cantidad_raw = cantidades[idx].strip() if idx < len(cantidades) else ""
        if not receta_id and not texto and not cantidad_raw:
            continue
        try:
            cantidad = Decimal(cantidad_raw)
        except (InvalidOperation, TypeError):
            raise ValidationError("La cantidad debe ser numérica.")
        if cantidad <= Decimal("0"):
            raise ValidationError("La cantidad debe ser mayor a cero.")
        rows.append({"receta_id": receta_id or None, "producto_texto": texto, "cantidad": cantidad})
    if not rows:
        raise ValidationError("Agrega al menos un producto a la merma.")
    return rows


@login_required
def crear_registro(request):
    _require_capture(request.user)
    sucursal_usuario = _sucursal_usuario(request.user)
    sucursales = sucursales_operativas()
    if sucursal_usuario and not _can_manage_mermas(request.user):
        sucursales = Sucursal.objects.filter(pk=sucursal_usuario.pk)

    if request.method == "POST":
        try:
            rows = _producto_rows_from_post(request.POST)
            sucursal = get_object_or_404(Sucursal, pk=request.POST.get("sucursal"))
            ticket_files = request.FILES.getlist("ticket_fotos")
            producto_files = request.FILES.getlist("producto_fotos")
            if not ticket_files:
                raise ValidationError("Toma o sube la foto del ticket Point.")
            if not producto_files:
                raise ValidationError("Toma o sube al menos una foto del producto.")
            if sucursal_usuario and not _can_manage_mermas(request.user) and sucursal != sucursal_usuario:
                raise PermissionDenied("No puedes registrar merma de otra sucursal.")
            with transaction.atomic():
                registro = MermaRegistro.objects.create(
                    sucursal=sucursal,
                    ticket_point=request.POST.get("ticket_point", "").strip(),
                    registrado_por=request.user,
                    nota_sucursal=request.POST.get("nota_sucursal", "").strip(),
                )
                recetas = {
                    str(receta.id): receta
                    for receta in Receta.objects.filter(id__in=[row["receta_id"] for row in rows if row["receta_id"]])
                }
                for row in rows:
                    MermaProducto.objects.create(
                        registro=registro,
                        receta=recetas.get(str(row["receta_id"])),
                        producto_texto=row["producto_texto"],
                        cantidad_enviada=row["cantidad"],
                    )
                for archivo in ticket_files:
                    MermaEvidencia.objects.create(
                        registro=registro,
                        tipo=MermaEvidencia.TIPO_TICKET,
                        archivo=archivo,
                        subido_por=request.user,
                    )
                for archivo in producto_files:
                    MermaEvidencia.objects.create(
                        registro=registro,
                        tipo=MermaEvidencia.TIPO_PRODUCTO_SUCURSAL,
                        archivo=archivo,
                        subido_por=request.user,
                    )
            messages.success(request, f"Merma {registro.folio} registrada. Queda abierta hasta asignar repartidor.")
            return redirect("mermas:detalle", pk=registro.pk)
        except (ValidationError, PermissionDenied) as exc:
            messages.error(request, exc.messages[0] if hasattr(exc, "messages") else str(exc))

    productos_iniciales = Receta.objects.order_by("nombre")
    return render(
        request,
        "mermas/form.html",
        {
            "sucursales": sucursales,
            "productos_iniciales": productos_iniciales,
            "now": timezone.localtime(),
        },
    )


@login_required
def detalle(request, pk):
    _require_mermas(request.user)
    registro = get_object_or_404(_registros_visibles(request.user), pk=pk)
    repartidores = Repartidor.objects.select_related("user", "unidad_asignada").order_by("user__first_name", "user__username")
    return render(
        request,
        "mermas/detalle.html",
        {
            "registro": registro,
            "repartidores": repartidores,
            "can_manage_mermas": _can_manage_mermas(request.user),
        },
    )


@login_required
@require_POST
def enviar_cedis(request, pk):
    _require_capture(request.user)
    registro = get_object_or_404(_registros_visibles(request.user), pk=pk)
    if registro.estatus != MermaRegistro.ESTATUS_CAPTURA:
        return HttpResponseBadRequest("La merma ya no está abierta para envío.")
    repartidor = get_object_or_404(Repartidor, pk=request.POST.get("repartidor"))
    try:
        registro.marcar_enviado(repartidor, request.user)
    except ValidationError as exc:
        messages.error(request, exc.messages[0])
    else:
        messages.success(request, f"{registro.folio} marcado como enviado a CEDIS.")
    return redirect("mermas:detalle", pk=registro.pk)


@login_required
@require_POST
def recibir_cedis(request, pk):
    _require_receive(request.user)
    registro = get_object_or_404(MermaRegistro.objects.prefetch_related("productos"), pk=pk)
    if registro.estatus != MermaRegistro.ESTATUS_ENVIADO_CEDIS:
        return HttpResponseBadRequest("Solo se pueden recibir mermas enviadas a CEDIS.")
    nota = request.POST.get("nota_recepcion", "").strip()
    repartidor_confirmado = request.POST.get("repartidor_confirmado") == "on"
    try:
        with transaction.atomic():
            for producto in registro.productos.all():
                cantidad_raw = request.POST.get(f"cantidad_recibida_{producto.pk}", "").strip()
                correcto = request.POST.get(f"correcto_{producto.pk}") == "on"
                if correcto and not cantidad_raw:
                    cantidad = producto.cantidad_enviada
                else:
                    try:
                        cantidad = Decimal(cantidad_raw)
                    except (InvalidOperation, TypeError):
                        raise ValidationError("Captura la cantidad correcta recibida.")
                producto.cantidad_recibida = cantidad
                producto.conforme = correcto and cantidad == producto.cantidad_enviada
                producto.nota_recepcion = request.POST.get(f"nota_producto_{producto.pk}", "").strip()
                producto.save(update_fields=["cantidad_recibida", "conforme", "nota_recepcion"])
            for archivo in request.FILES.getlist("cedis_fotos"):
                MermaEvidencia.objects.create(
                    registro=registro,
                    tipo=MermaEvidencia.TIPO_PRODUCTO_CEDIS,
                    archivo=archivo,
                    subido_por=request.user,
                )
            registro.marcar_recibido(
                user=request.user,
                repartidor_confirmado=repartidor_confirmado,
                nota=nota,
            )
    except ValidationError as exc:
        messages.error(request, exc.messages[0])
        return redirect("mermas:detalle", pk=registro.pk)
    messages.success(request, f"{registro.folio} recibido en CEDIS.")
    return redirect("mermas:detalle", pk=registro.pk)


@login_required
@require_GET
def buscar_productos(request):
    _require_capture(request.user)
    q = request.GET.get("q", "").strip()
    productos = Receta.objects.all()
    if q:
        productos = productos.filter(Q(nombre__icontains=q) | Q(codigo_point__icontains=q))
    productos = productos.order_by("nombre")[:200]
    return render(request, "mermas/partials/product_options.html", {"productos": productos})
