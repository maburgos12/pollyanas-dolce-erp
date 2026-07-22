from __future__ import annotations

import json
import logging
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.contrib.staticfiles import finders
from django.db import IntegrityError, transaction
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from activos.models import Activo
from core.access import can_view_module, can_view_submodule, is_mermas_only
from core.models import Sucursal
from core.notificaciones import crear_notificacion, crear_notificaciones
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla
from mermas.models import MermaInsumo, OrdenAjustePoint
from mantenimiento.evidence_validation import EvidenceValidationError, validate_evidence_files
from mantenimiento.services_access import can_access_mantenimiento
from mermas.services_insumos import (
    decidir_merma_insumo, enviar_merma_insumo, insumos_elegibles_para_sucursal,
    simular_orden_ajuste_point,
)
from recetas.models import Receta

from .models import BitacoraOperativa, BitacoraOperativaLinea
from .services import build_operacion_context


BITACORA_CONFIG = {
    BitacoraOperativa.TIPO_SALIDAS_CFP1: {
        "titulo": "Salidas CFP1",
        "ayuda": "Cantidades enviadas por producto a cada sucursal.",
        "campos": ["cantidad"],
        "usa_sucursales": True,
    },
    BitacoraOperativa.TIPO_INVENTARIO_CFP1: {
        "titulo": "Inventario CFP1",
        "ayuda": "Existencia CEDIS y devolución del día.",
        "campos": ["cedis", "devolucion"],
    },
    BitacoraOperativa.TIPO_PLAGAS: {
        "titulo": "Control de plagas",
        "ayuda": "Registro de detección o aplicación.",
        "campos": ["plaga", "area", "metodo", "fecha_deteccion"],
        "sin_producto": True,
    },
    BitacoraOperativa.TIPO_CFP11: {
        "titulo": "Inventario CFP 1.1",
        "ayuda": "Bloques de existencia, salida y entrada.",
        "campos": ["bloque", "tamano", "existencia", "salida", "entrada"],
    },
    BitacoraOperativa.TIPO_ROTACION: {
        "titulo": "Rotación producto",
        "ayuda": "Producto, cantidad y fecha del producto.",
        "campos": ["cantidad", "fecha_producto"],
    },
    BitacoraOperativa.TIPO_REBANADO: {
        "titulo": "Producto rebanado",
        "ayuda": "Enteros, rebanadas y merma.",
        "campos": ["pastel_entero", "total_rebanadas", "merma_rebanadas", "fecha_producto", "motivo_merma"],
    },
}

logger = logging.getLogger(__name__)


def _on_commit_seguro(callback):
    def wrapped():
        try:
            callback()
        except Exception:
            logger.exception("No fue posible emitir una notificación de App Operativa")
    transaction.on_commit(wrapped)


@login_required
def app_home(request):
    return render(request, "operacion/app_home.html", build_operacion_context(request.user))


@login_required
def sucursal_tools(request):
    sucursal = _sucursal_operativa_usuario(request.user)
    pendientes = MermaInsumo.objects.filter(
        jefe_inmediato=request.user, estatus=MermaInsumo.ESTATUS_ENVIADA
    ).select_related("sucursal", "reportado_por")
    solo_aprobacion = False
    if not sucursal and pendientes.exists():
        sucursal = pendientes.first().sucursal
        solo_aprobacion = True
    if not sucursal:
        raise PermissionDenied("Tu sesión no tiene una sucursal operativa asignada.")
    return render(
        request,
        "operacion/sucursal_tools.html",
        {
            "sucursal": sucursal,
            "tab_activa": request.GET.get("tab") or "fallas",
            "activos": Activo.objects.filter(sucursal=sucursal, activo=True).order_by("nombre", "id"),
            "categorias_equipo": CategoriaFalla.objects.filter(
                activo=True, tipo=CategoriaFalla.TIPO_EQUIPO
            ).order_by("orden", "nombre"),
            "categorias_instalacion": CategoriaFalla.objects.filter(
                activo=True, tipo=CategoriaFalla.TIPO_INSTALACION
            ).order_by("orden", "nombre"),
            "insumos": [] if solo_aprobacion else insumos_elegibles_para_sucursal(sucursal),
            "mermas_pendientes": pendientes,
            "solo_aprobacion": solo_aprobacion,
            "mis_mermas": MermaInsumo.objects.filter(reportado_por=request.user).order_by("-creado_en")[:8],
            "mis_fallas": ReporteFalla.objects.filter(
                reportado_por=request.user, sucursal=sucursal
            ).select_related("categoria", "activo_relacionado").order_by("-fecha_reporte")[:8],
        },
    )


def _sucursal_operativa_usuario(user):
    profile = getattr(user, "userprofile", None)
    sucursal = getattr(profile, "sucursal", None)
    if not sucursal or not sucursal.esta_operativa():
        return None
    return sucursal


def _usuarios_mantenimiento():
    return [
        user for user in get_user_model().objects.filter(is_active=True).prefetch_related("groups", "module_access")
        if can_access_mantenimiento(user)
    ]


@login_required
@require_GET
def fallas_activos_api(request):
    sucursal = _sucursal_operativa_usuario(request.user)
    if not sucursal:
        return JsonResponse({"error": "Tu sesión no tiene una sucursal operativa asignada."}, status=403)
    activos = Activo.objects.filter(sucursal=sucursal, activo=True).order_by("nombre", "id")
    return JsonResponse(
        {
            "activos": [
                {"id": activo.id, "codigo": activo.codigo, "nombre": activo.nombre, "categoria": activo.categoria}
                for activo in activos
            ]
        }
    )


@login_required
@require_POST
def fallas_crear_api(request):
    sucursal = _sucursal_operativa_usuario(request.user)
    if not sucursal:
        return JsonResponse({"error": "Tu sesión no tiene una sucursal operativa asignada."}, status=403)
    try:
        data = json.loads(request.body or "{}") if request.content_type == "application/json" else request.POST
    except json.JSONDecodeError:
        return JsonResponse({"error": "La solicitud no contiene JSON válido."}, status=400)

    tipo = (data.get("tipo_objetivo") or "").strip().upper()
    categoria = CategoriaFalla.objects.filter(pk=data.get("categoria_id"), activo=True).first()
    if not categoria:
        return JsonResponse({"error": "Selecciona una categoría activa."}, status=400)

    activo = None
    if tipo == ReporteFalla.OBJETIVO_EQUIPO:
        activo = Activo.objects.filter(pk=data.get("activo_id"), sucursal=sucursal, activo=True).first()
        if not activo:
            return JsonResponse({"error": "El equipo no pertenece a tu sucursal."}, status=400)

    reporte = ReporteFalla(
        sucursal=sucursal,
        activo_relacionado=activo,
        categoria=categoria,
        tipo_objetivo=tipo,
        area_instalacion=(data.get("area_instalacion") or "").strip(),
        titulo=(data.get("titulo") or "").strip(),
        descripcion=(data.get("descripcion") or "").strip(),
        prioridad=(data.get("prioridad") or ReporteFalla.PRIORIDAD_MEDIA).strip(),
        foto_evidencia=None,
        justificacion_sin_foto=(data.get("justificacion_sin_foto") or "").strip(),
        reportado_por=request.user,
    )
    try:
        fotos = validate_evidence_files(
            [request.FILES["foto_evidencia"]] if request.FILES.get("foto_evidencia") else [], images_only=True
        )
        reporte.foto_evidencia = fotos[0] if fotos else None
        reporte.full_clean()
    except (ValidationError, EvidenceValidationError) as exc:
        if isinstance(exc, EvidenceValidationError):
            return JsonResponse({"error": str(exc)}, status=400)
        return JsonResponse({"error": "Revisa la captura.", "fields": exc.message_dict}, status=400)
    with transaction.atomic():
        reporte.save()
        BitacoraFalla.objects.create(
            reporte=reporte, usuario=request.user, estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
            comentario="Reporte creado desde App Operativa y enviado a Mantenimiento.",
        )
    _on_commit_seguro(lambda: crear_notificaciones(
        _usuarios_mantenimiento(),
        titulo=f"Nueva falla en {sucursal.nombre}",
        mensaje=reporte.titulo,
        url=f"/fallas/reportes/{reporte.pk}/",
        actor=request.user,
        objeto_tipo="ReporteFalla",
        objeto_id=reporte.pk,
    ))
    return JsonResponse({"id": reporte.id, "estatus": reporte.estatus, "destino": "Mantenimiento"}, status=201)


@login_required
@require_GET
def mermas_insumos_catalogo_api(request):
    sucursal = _sucursal_operativa_usuario(request.user)
    if not sucursal:
        return JsonResponse({"error": "Tu sesión no tiene una sucursal operativa asignada."}, status=403)
    rows = insumos_elegibles_para_sucursal(sucursal)
    return JsonResponse(
        {
            "sucursal": {"id": sucursal.id, "nombre": sucursal.nombre},
            "insumos": [
                {
                    "codigo_point": row.codigo_point,
                    "nombre": row.nombre_point,
                    "unidad": row.unidad_point,
                    "existencia": str(row.existencia),
                    "snapshot_en": row.snapshot_capturado_en.isoformat(),
                }
                for row in rows
            ],
        }
    )


@login_required
@require_POST
def mermas_insumos_crear_api(request):
    sucursal = _sucursal_operativa_usuario(request.user)
    if not sucursal:
        return JsonResponse({"error": "Tu sesión no tiene una sucursal operativa asignada."}, status=403)
    try:
        data = json.loads(request.body or "{}") if request.content_type == "application/json" else request.POST
    except json.JSONDecodeError:
        return JsonResponse({"error": "La solicitud no contiene JSON válido."}, status=400)

    eligible = {row.codigo_point: row for row in insumos_elegibles_para_sucursal(sucursal)}
    row = eligible.get((data.get("codigo_point") or "").strip())
    if not row:
        return JsonResponse({"error": "El insumo no está habilitado para esta sucursal."}, status=400)
    try:
        cantidad = Decimal(str(data.get("cantidad") or ""))
    except (InvalidOperation, ValueError):
        return JsonResponse({"error": "Captura una cantidad válida."}, status=400)
    if not cantidad.is_finite() or cantidad <= 0 or cantidad > row.existencia:
        return JsonResponse({"error": "La cantidad debe ser positiva y no superar la existencia de Point."}, status=400)

    try:
        fotos = validate_evidence_files(
            [request.FILES["foto_evidencia"]] if request.FILES.get("foto_evidencia") else [], images_only=True
        )
        with transaction.atomic():
            merma = MermaInsumo(
                sucursal=sucursal, reportado_por=request.user, codigo_point=row.codigo_point,
                nombre_point=row.nombre_point, unidad_point=row.unidad_point, cantidad_reportada=cantidad,
                motivo=(data.get("motivo") or "").strip(), comentario=(data.get("comentario") or "").strip(),
                foto_evidencia=fotos[0] if fotos else None,
                justificacion_sin_foto=(data.get("justificacion_sin_foto") or "").strip(),
            )
            merma.full_clean()
            merma.save()
            merma = enviar_merma_insumo(merma_id=merma.id, usuario=request.user)
    except EvidenceValidationError as exc:
        return JsonResponse({"error": str(exc)}, status=400)
    except ValidationError as exc:
        return JsonResponse({"error": "Revisa la captura.", "fields": exc.message_dict}, status=400)
    if merma.jefe_inmediato_id:
        _on_commit_seguro(lambda: crear_notificacion(
            usuario=merma.jefe_inmediato, titulo=f"Merma por aprobar · {sucursal.nombre}",
            mensaje=f"{merma.nombre_point}: {merma.cantidad_reportada} {merma.unidad_point}",
            url="/app/sucursal/?tab=mermas", actor=request.user,
            objeto_tipo="MermaInsumo", objeto_id=merma.pk,
        ))
    return JsonResponse({"id": merma.id, "estatus": merma.estatus}, status=201)


@login_required
@require_POST
def mermas_insumos_aprobar_api(request, merma_id):
    try:
        data = json.loads(request.body or "{}") if request.content_type == "application/json" else request.POST
        cantidad = Decimal(str(data.get("cantidad") or ""))
    except (json.JSONDecodeError, InvalidOperation, ValueError):
        return JsonResponse({"error": "Captura una cantidad válida."}, status=400)
    if not cantidad.is_finite():
        return JsonResponse({"error": "Captura una cantidad válida."}, status=400)
    with transaction.atomic():
        merma = MermaInsumo.objects.filter(pk=merma_id, jefe_inmediato=request.user).first()
        if not merma:
            return JsonResponse({"error": "La merma no está asignada a este jefe inmediato."}, status=403)
        if merma.estatus == MermaInsumo.ESTATUS_APROBADA and hasattr(merma, "orden_point"):
            orden = merma.orden_point
            return JsonResponse({"id": merma.id, "estatus": merma.estatus, "orden_id": orden.id})
        try:
            aprobada = merma.aprobar(jefe=request.user, cantidad=cantidad, motivo=data.get("motivo") or "")
            orden = OrdenAjustePoint.crear_desde_merma(aprobada)
            orden = simular_orden_ajuste_point(orden.id)
        except (ValidationError, IntegrityError) as exc:
            message = "; ".join(exc.messages) if isinstance(exc, ValidationError) else "La orden ya existe."
            return JsonResponse({"error": message}, status=400)
    aprobada.refresh_from_db()
    return JsonResponse({"id": merma.id, "estatus": aprobada.estatus, "orden_id": orden.id, "orden_estatus": orden.estatus})


@login_required
@require_POST
def mermas_insumos_decidir_api(request, merma_id):
    try:
        data = json.loads(request.body or "{}") if request.content_type == "application/json" else request.POST
        merma = decidir_merma_insumo(
            merma_id=merma_id, jefe=request.user, accion=data.get("accion"), motivo=data.get("motivo")
        )
    except json.JSONDecodeError:
        return JsonResponse({"error": "La solicitud no contiene JSON válido."}, status=400)
    except ValidationError as exc:
        return JsonResponse({"error": "; ".join(exc.messages)}, status=400)
    _on_commit_seguro(lambda: crear_notificacion(
        usuario=merma.reportado_por, titulo=f"Merma {merma.get_estatus_display().lower()}",
        mensaje=(data.get("motivo") or "").strip(), url="/app/sucursal/?tab=mermas",
        actor=request.user, objeto_tipo="MermaInsumo", objeto_id=merma.pk,
    ))
    return JsonResponse({"id": merma.id, "estatus": merma.estatus})


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


def _decimal(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return str(Decimal(value))
    except (InvalidOperation, ValueError):
        return None


def _lineas_from_post(request, config):
    lineas = []
    for index in range(8):
        receta = None
        datos = {}
        observaciones = (request.POST.get(f"observaciones_{index}") or "").strip()
        if not config.get("sin_producto"):
            receta_id = request.POST.get(f"receta_{index}")
            if not receta_id:
                continue
            receta = Receta.objects.filter(pk=receta_id).first()
            if not receta:
                continue
        for campo in config["campos"]:
            raw = (request.POST.get(f"{campo}_{index}") or "").strip()
            if campo in {
                "cantidad",
                "cedis",
                "devolucion",
                "existencia",
                "salida",
                "entrada",
                "pastel_entero",
                "total_rebanadas",
                "merma_rebanadas",
            }:
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
        if receta or datos or observaciones:
            lineas.append((receta, datos, observaciones))
    return lineas


@login_required
def bitacoras_home(request):
    if not _can_use_bitacoras(request.user):
        raise PermissionDenied
    recientes = BitacoraOperativa.objects.select_related("creado_por").prefetch_related("lineas")[:8]
    return render(
        request,
        "operacion/bitacoras_home.html",
        {"tipos": BitacoraOperativa.TIPO_CHOICES, "config": BITACORA_CONFIG, "recientes": recientes},
    )


@login_required
def bitacora_captura(request, tipo):
    if not _can_use_bitacoras(request.user) or tipo not in BITACORA_CONFIG:
        raise PermissionDenied
    config = BITACORA_CONFIG[tipo]
    sucursales = list(Sucursal.objects.filter(activa=True).order_by("codigo"))
    recetas = Receta.objects.filter(pasa_modulo_produccion=True).order_by("nombre")[:120]
    if request.method == "POST":
        bitacora = BitacoraOperativa.objects.create(
            tipo=tipo,
            fecha=request.POST.get("fecha") or timezone.localdate(),
            sucursal_id=request.POST.get("sucursal") or None,
            notas=(request.POST.get("notas") or "").strip(),
            creado_por=request.user,
        )
        for receta, datos, observaciones in _lineas_from_post(request, config):
            BitacoraOperativaLinea.objects.create(
                bitacora=bitacora,
                receta=receta,
                datos=datos,
                observaciones=observaciones,
            )
        if request.POST.get("cerrar") == "1":
            bitacora.cerrar()
            bitacora.save(update_fields=["estatus", "cerrado_en", "actualizado_en"])
        messages.success(request, "Bitácora guardada.")
        return redirect("operacion:bitacoras_home")
    return render(
        request,
        "operacion/bitacora_captura.html",
        {
            "tipo": tipo,
            "config": config,
            "recetas": recetas,
            "sucursales": sucursales,
            "row_range": range(8),
            "today": timezone.localdate(),
        },
    )
