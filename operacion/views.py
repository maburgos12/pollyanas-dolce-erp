from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.contrib.staticfiles import finders
from django.db.models.functions import Trim
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.cache import never_cache

from core.access import can_manage_module, can_view_module, can_view_submodule, is_mermas_only
from core.models import Sucursal
from inventario.models import ALMACEN_CHOICES, LoteProduccion
from maestros.models import Insumo, UnidadMedida
from maestros.utils.canonical_catalog import canonicalized_active_insumos
from recetas.models import Receta
from recetas.utils.costeo_snapshot import resolve_preparation_recipe_for_insumo

from .bitacoras_config import BITACORA_CONFIG
from .models import BitacoraOperativa, BitacoraOperativaLinea
from .services import build_operacion_context
from .services_bitacoras_inventory import (
    registrar_apertura_inicial,
    cerrar_hornos,
    cerrar_armado,
    guardar_corte_ciego,
    entregar_a_armado,
    autorizar_correccion_bitacora,
)


PRODUCTION_BITACORA_TYPES = {
    BitacoraOperativa.TIPO_HORNOS,
    BitacoraOperativa.TIPO_ARMADO,
}

DECIMAL_FIELDS = {
    "cantidad",
    "cedis",
    "devolucion",
    "existencia",
    "salida",
    "entrada",
    "pastel_entero",
    "total_rebanadas",
    "merma_rebanadas",
    "preparacion",
    "existencia_fisica",
    "salida_armado",
    "consumo_real",
    "producto_terminado",
}


@login_required
def app_home(request):
    return render(request, "operacion/app_home.html", build_operacion_context(request.user))


@never_cache
def app_sw(request):
    path = finders.find("operacion/sw.js")
    if not path:
        raise Http404("Service worker de App Operativa no encontrado")
    with open(path, encoding="utf-8") as service_worker:
        return HttpResponse(service_worker.read(), content_type="application/javascript")


def _can_use_bitacoras(user) -> bool:
    if user.is_superuser:
        return True
    if is_mermas_only(user):
        return False
    return (
        can_view_module(user, "produccion")
        or can_view_module(user, "logistica")
        or can_view_submodule(user, "mermas", "captura")
        or can_view_submodule(user, "mermas", "recepcion")
    )


def _can_use_bitacora_type(user, tipo: str) -> bool:
    if not _can_use_bitacoras(user):
        return False
    if tipo in PRODUCTION_BITACORA_TYPES:
        return user.is_superuser or can_view_module(user, "produccion")
    return True


def _decimal(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValidationError("Ingresa una cantidad numérica válida.")
    if not decimal_value.is_finite():
        raise ValidationError("Ingresa una cantidad numérica válida.")
    return str(decimal_value)


def _recetas_for_config(config):
    recetas = Receta.objects.filter(pasa_modulo_produccion=True)
    if config.get("receta_tipo"):
        recetas = recetas.filter(tipo=config["receta_tipo"])
    if config.get("requiere_codigo_point"):
        recetas = recetas.annotate(codigo_point_limpio=Trim("codigo_point")).exclude(codigo_point_limpio="")
    return recetas.order_by("nombre")


def _insumos_for_config(config):
    if not config.get("usa_insumos"):
        return []
    return [
        row["canonical"]
        for row in canonicalized_active_insumos(limit=3000)
        if row["canonical"].tipo_item == Insumo.TIPO_INTERNO
        and (row["canonical"].codigo_point or "").strip()
    ]


def _lineas_from_post(request, config):
    lineas = []
    allowed_insumos = _insumos_for_config(config) if config.get("usa_insumos") else []
    allowed_insumos_by_id = {str(item.id): item for item in allowed_insumos}
    indices = range(len(allowed_insumos)) if config.get("usa_insumos") else range(8)
    for index in indices:
        receta = None
        insumo = None
        datos = {}
        observaciones = (request.POST.get(f"observaciones_{index}") or "").strip()
        if config.get("usa_insumos"):
            insumo_id = request.POST.get(f"insumo_{index}")
            if not insumo_id:
                continue
            insumo = allowed_insumos_by_id.get(str(insumo_id))
            if not insumo:
                raise ValidationError("Selecciona un producto válido con identidad Point.")
        elif not config.get("sin_producto"):
            receta_id = request.POST.get(f"receta_{index}")
            if not receta_id:
                continue
            receta = _recetas_for_config(config).filter(pk=receta_id).first()
            if not receta:
                if config.get("requiere_codigo_point"):
                    raise ValidationError("Selecciona un producto válido con identidad Point.")
                raise ValidationError("Selecciona un producto válido.")
        for campo in config["campos"]:
            raw = (request.POST.get(f"{campo}_{index}") or "").strip()
            if campo in DECIMAL_FIELDS:
                raw = _decimal(raw) or ""
            if raw:
                datos[campo] = raw
        if config.get("usa_sucursales"):
            cantidades = {}
            prefix = f"sucursal_{index}_"
            for key, raw in request.POST.items():
                if key.startswith(prefix):
                    value = _decimal(raw)
                    if value:
                        cantidades[key.removeprefix(prefix)] = value
            if cantidades:
                datos["sucursales"] = cantidades
        if insumo and not datos and not observaciones:
            if request.POST.get("accion") == "guardar_existencia":
                raise ValidationError(f"Captura la existencia de {insumo.nombre}.")
            continue
        if receta and not datos and not observaciones:
            raise ValidationError("Captura al menos una cantidad u observación.")
        if receta or insumo or datos or observaciones:
            lineas.append((receta, insumo, datos, observaciones))
    if config.get("requiere_codigo_point") and not lineas:
        raise ValidationError("Selecciona un producto válido con identidad Point.")
    return lineas


@login_required
def bitacoras_home(request):
    if not _can_use_bitacoras(request.user):
        raise PermissionDenied
    tipos = [choice for choice in BitacoraOperativa.TIPO_CHOICES if _can_use_bitacora_type(request.user, choice[0])]
    recientes = BitacoraOperativa.objects.select_related("creado_por").prefetch_related("lineas")
    if not (request.user.is_superuser or can_view_module(request.user, "produccion")):
        recientes = recientes.exclude(tipo__in=PRODUCTION_BITACORA_TYPES)
    return render(
        request,
        "operacion/bitacoras_home.html",
        {
            "tipos": tipos,
            "config": BITACORA_CONFIG,
            "recientes": recientes[:8],
            "can_manage_produccion": can_manage_module(request.user, "produccion"),
        },
    )


def _productos_apertura():
    productos = []
    for row in canonicalized_active_insumos(limit=5000):
        insumo = row["canonical"]
        if (
            insumo.tipo_item != Insumo.TIPO_INTERNO
            or not (insumo.codigo_point or "").strip()
            or not insumo.unidad_base_id
        ):
            continue
        receta = resolve_preparation_recipe_for_insumo(insumo)
        if not receta or not (receta.codigo_point or "").strip():
            continue
        productos.append({"insumo": insumo, "receta": receta})
    return productos


@login_required
def bitacoras_apertura(request):
    if not can_manage_module(request.user, "produccion"):
        raise PermissionDenied
    productos = _productos_apertura()
    productos_por_insumo = {str(item["insumo"].pk): item for item in productos}
    if request.method == "POST":
        try:
            producto = productos_por_insumo.get(request.POST.get("insumo", ""))
            if not producto:
                raise ValidationError("Selecciona un producto canónico de Point.")
            unidad = UnidadMedida.objects.filter(pk=request.POST.get("unidad")).first()
            if not unidad:
                raise ValidationError("Selecciona una unidad válida.")
            raw_fecha = (request.POST.get("fecha_elaboracion") or "").strip()
            fecha_elaboracion = None
            if raw_fecha:
                try:
                    fecha_elaboracion = timezone.datetime.strptime(raw_fecha, "%Y-%m-%d").date()
                except ValueError as exc:
                    raise ValidationError("La fecha de elaboración no es válida.") from exc
            lote = registrar_apertura_inicial(
                receta=producto["receta"],
                insumo=producto["insumo"],
                cantidad=request.POST.get("cantidad"),
                unidad=unidad,
                ubicacion=request.POST.get("ubicacion", ""),
                fecha_elaboracion=fecha_elaboracion,
                actor=request.user,
                observaciones=request.POST.get("observaciones", ""),
            )
        except ValidationError as exc:
            messages.error(request, exc.messages[0])
        else:
            messages.success(request, f"Apertura aplicada: {lote.codigo}")
            return redirect("operacion:bitacoras_apertura")

    aperturas = (
        LoteProduccion.objects.filter(es_apertura=True)
        .select_related("insumo", "unidad", "creado_por")
        .prefetch_related("movimientos")
        .order_by("-id")[:50]
    )
    return render(
        request,
        "operacion/bitacoras_home.html",
        {
            "modo_apertura": True,
            "productos_apertura": productos,
            "unidades_apertura": UnidadMedida.objects.order_by("nombre"),
            "ubicaciones": ALMACEN_CHOICES,
            "aperturas": aperturas,
        },
    )


@login_required
def bitacora_captura(request, tipo):
    if tipo not in BITACORA_CONFIG or not _can_use_bitacora_type(request.user, tipo):
        raise PermissionDenied
    config = BITACORA_CONFIG[tipo]
    sucursales = list(Sucursal.objects.filter(activa=True).order_by("codigo"))
    recetas = _recetas_for_config(config)[:120]
    insumos = _insumos_for_config(config)[:240]
    capture_rows = (
        [{"index": index, "insumo": insumo} for index, insumo in enumerate(insumos)]
        if config.get("usa_insumos")
        else [{"index": index, "insumo": None} for index in range(8)]
    )

    # Si hay parámetro revision, cargar la bitácora sellada
    revision_id = request.GET.get("revision")
    sealed_bitacora = None
    if revision_id:
        try:
            sealed_bitacora = BitacoraOperativa.objects.get(id=revision_id, tipo=tipo)
        except BitacoraOperativa.DoesNotExist:
            sealed_bitacora = None

    if request.method == "POST":
        try:
            lineas = _lineas_from_post(request, config)
        except ValidationError as exc:
            messages.error(request, exc.messages[0])
            return render(
                request,
                "operacion/bitacora_captura.html",
                {
                    "tipo": tipo,
                    "config": config,
                    "recetas": recetas,
                    "insumos": insumos,
                    "sucursales": sucursales,
                    "capture_rows": capture_rows,
                    "today": timezone.localdate(),
                    "submitted_values": request.POST,
                },
            )
        bitacora = BitacoraOperativa.objects.create(
            tipo=tipo,
            fecha=request.POST.get("fecha") or timezone.localdate(),
            sucursal_id=request.POST.get("sucursal") or None,
            notas=(request.POST.get("notas") or "").strip(),
            creado_por=request.user,
        )
        for receta, insumo, datos, observaciones in lineas:
            BitacoraOperativaLinea.objects.create(
                bitacora=bitacora,
                receta=receta,
                insumo=insumo,
                datos=datos,
                observaciones=observaciones,
            )
        accion = request.POST.get("accion") or ""

        # Acción de corrección: requiere movimiento_original_id bloqueado + validaciones
        if accion == "corregir":
            try:
                movimiento_original_id = request.POST.get("movimiento_original_id")
                if not movimiento_original_id:
                    raise ValidationError("ID del movimiento a corregir es obligatorio.")
                movimiento_original_id = int(movimiento_original_id)

                nueva_cantidad_raw = (request.POST.get("cantidad_corregida") or "").strip()
                if not nueva_cantidad_raw:
                    raise ValidationError("Ingresa la cantidad corregida.")

                motivo = (request.POST.get("motivo_correccion") or "").strip()
                if not motivo:
                    raise ValidationError("El motivo de la corrección es obligatorio.")

                # Obtener la línea desde el movimiento original para validación
                from inventario.models import MovimientoInventario
                try:
                    mov_original = MovimientoInventario.objects.get(id=movimiento_original_id)
                except MovimientoInventario.DoesNotExist:
                    raise ValidationError("Movimiento a corregir no encontrado.")

                linea = mov_original.linea_bitacora
                if not linea or linea.bitacora_id != bitacora.id:
                    raise ValidationError("El movimiento no pertenece a esta bitácora.")

                autorizar_correccion_bitacora(
                    movimiento_original_id=movimiento_original_id,
                    bitacora=bitacora,
                    linea=linea,
                    nueva_cantidad=nueva_cantidad_raw,
                    motivo=motivo,
                    actor=request.user,
                )
                messages.success(request, "Corrección registrada y auditable.")
                return redirect(f"{request.path}?revision={bitacora.id}")
            except ValidationError as exc:
                messages.error(request, exc.messages[0] if exc.messages else str(exc))
                return redirect("operacion:bitacoras_home")
            except PermissionDenied as exc:
                messages.error(request, str(exc))
                return redirect("operacion:bitacoras_home")

        if accion == "guardar_existencia":
            if tipo == BitacoraOperativa.TIPO_CFP11:
                guardar_corte_ciego(bitacora, request.user)
                messages.success(request, "Existencia guardada. Se selló el corte ciego.")
                return redirect(f"{request.path}?revision={bitacora.id}")
            else:
                messages.error(request, "Esta acción solo aplica a CFP 1.1.")
                return redirect("operacion:bitacoras_home")
        if accion == "entregar_armado":
            if tipo == BitacoraOperativa.TIPO_CFP11 and bitacora.conteo_guardado_en:
                # Minimal transfer action: get first line for transfer context
                linea = bitacora.lineas.first()
                if linea and (linea.insumo_id or linea.receta_id):
                    try:
                        insumo_resuelto = linea.insumo or resolve_preparation_recipe_for_insumo(linea.receta)
                        if insumo_resuelto:
                            cantidad_raw = (request.POST.get("cantidad_entregar") or "").strip()
                            if cantidad_raw:
                                entregar_a_armado(
                                    insumo=insumo_resuelto,
                                    cantidad=cantidad_raw,
                                    linea=linea,
                                    actor=request.user,
                                )
                                messages.success(request, "Lotes transferidos a Armado.")
                            else:
                                messages.error(request, "Ingresa una cantidad válida.")
                        else:
                            messages.error(request, "No se resolvió el insumo para transferencia.")
                    except ValidationError as exc:
                        messages.error(request, exc.messages[0])
                    except PermissionDenied as exc:
                        messages.error(request, str(exc))
                return redirect(f"{request.path}?revision={bitacora.id}")
            else:
                messages.error(request, "Esta acción solo aplica a CFP 1.1 sellada.")
                return redirect("operacion:bitacoras_home")
        cerrar_accion = accion == "cerrar_produccion" or accion == "cerrar_armado" or request.POST.get("cerrar") == "1"
        if cerrar_accion:
            if tipo == BitacoraOperativa.TIPO_HORNOS:
                cerrar_hornos(bitacora, request.user)
                messages.success(request, f"Hornos cerrados. Se generaron lotes de producción.")
                return redirect(f"{request.path}?revision={bitacora.id}")
            elif tipo == BitacoraOperativa.TIPO_ARMADO and accion == "cerrar_armado":
                try:
                    result = cerrar_armado(bitacora, request.user)
                    messages.success(
                        request,
                        f"Armado cerrado. Producto terminado: {result.lote_terminado.codigo}. "
                        f"Stock en CEDIS: {result.cantidad_terminada} unidades."
                    )
                except ValidationError as exc:
                    messages.error(request, exc.messages[0] if exc.messages else str(exc))
                except PermissionDenied as exc:
                    messages.error(request, str(exc))
                return redirect(f"{request.path}?revision={bitacora.id}")
            else:
                bitacora.cerrar()
                bitacora.save(update_fields=["estatus", "cerrado_en", "actualizado_en"])
        messages.success(request, "Bitácora guardada.")
        return redirect("operacion:bitacoras_home")
    context = {
        "tipo": tipo,
        "config": config,
        "recetas": recetas,
        "insumos": insumos,
        "sucursales": sucursales,
        "capture_rows": capture_rows,
        "today": timezone.localdate(),
        "sealed_bitacora": sealed_bitacora,
    }
    return render(
        request,
        "operacion/bitacora_captura.html",
        context,
    )
