from __future__ import annotations

import json
import logging
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from django.conf import settings
from django.core.mail import send_mail
from django.core.exceptions import PermissionDenied, ValidationError
from django.contrib.staticfiles import finders
from django.db import IntegrityError, OperationalError, connection, transaction
from django.db.models.functions import Trim
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from activos.models import Activo
from core.access import can_manage_module, can_view_module, can_view_submodule, is_admin_or_dg, is_mermas_only
from core.models import Sucursal
from core.notificaciones import crear_notificacion, crear_notificaciones
from fallas.models import BitacoraFalla, CategoriaFalla, ReporteFalla
from inventario.models import ALMACEN_CHOICES, LoteProduccion
from maestros.models import Insumo, UnidadMedida
from maestros.utils.canonical_catalog import canonicalized_active_insumos
from mermas.models import MermaInsumo, OrdenAjustePoint
from mantenimiento.evidence_validation import EvidenceValidationError, validate_evidence_files
from mantenimiento.services_access import can_access_mantenimiento
from mermas.services_insumos import (
    consultar_existencia_insumo_point, decidir_merma_insumo, enviar_merma_insumo,
    insumos_recibidos_para_sucursal,
    reasignar_merma_sin_responsable, reenviar_merma_aclarada, simular_orden_ajuste_point,
)
from pos_bridge.services.live_inventory_lookup_service import PointLiveInventoryLookupError
from recetas.models import Receta
from recetas.utils.costeo_snapshot import resolve_preparation_recipe_for_insumo
from rrhh.models import Empleado

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

logger = logging.getLogger(__name__)


def _cargar_insumos_sucursal(sucursal):
    """Evita que una réplica Point lenta consuma un worker web completo."""
    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = %s", [5000])
            return insumos_recibidos_para_sucursal(sucursal), False
    except OperationalError:
        logger.exception("Timeout al cargar el catálogo local de insumos Point para sucursal=%s", sucursal.pk)
        return [], True


def _on_commit_seguro(callback):
    def wrapped():
        try:
            callback()
        except Exception:
            logger.exception("No fue posible emitir una notificación de App Operativa")
    transaction.on_commit(wrapped)


def _notificar_usuario_operacion(*, usuario, titulo, mensaje, actor, objeto_id):
    crear_notificacion(
        usuario=usuario, titulo=titulo, mensaje=mensaje, url="/app/sucursal/?tab=mermas",
        actor=actor, objeto_tipo="MermaInsumo", objeto_id=objeto_id,
    )
    email = (getattr(usuario, "email", "") or "").strip()
    if email:
        send_mail(
            subject=titulo, message=mensaje,
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "") or None,
            recipient_list=[email], fail_silently=False,
        )


def _respuesta_creacion(request, *, payload, tab, anchor, mensaje):
    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.content_type == "application/json":
        return JsonResponse(payload, status=201)
    messages.success(request, mensaje)
    return redirect(f"{reverse('operacion:sucursal_tools')}?tab={tab}#{anchor}")


def _respuesta_error(request, *, error, tab, anchor, fields=None):
    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.content_type == "application/json":
        payload = {"error": error}
        if fields:
            payload["fields"] = fields
        return JsonResponse(payload, status=400)
    request.session[f"operacion_draft_{tab}"] = {
        key: value for key, value in request.POST.items() if key != "csrfmiddlewaretoken"
    }
    messages.error(request, error)
    return redirect(f"{reverse('operacion:sucursal_tools')}?tab={tab}#{anchor}")


def _respuesta_accion_merma(request, *, merma_id, payload=None, error="", mensaje="", status=400):
    async_request = request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.content_type == "application/json"
    if async_request:
        return JsonResponse(payload or {"error": error}, status=status if error else 200)
    if error:
        messages.error(request, error)
    else:
        messages.success(request, mensaje)
    return redirect(f"{reverse('operacion:sucursal_tools')}?tab=mermas#merma-{merma_id}")


@login_required
def app_home(request):
    return render(request, "operacion/app_home.html", build_operacion_context(request.user))


@login_required
def sucursal_tools(request):
    sucursal = _sucursal_operativa_usuario(request.user)
    es_direccion = is_admin_or_dg(request.user)
    pendientes = MermaInsumo.objects.filter(
        jefe_inmediato=request.user, estatus=MermaInsumo.ESTATUS_ENVIADA
    ).select_related("sucursal", "reportado_por")
    solo_aprobacion = False
    sin_responsable = MermaInsumo.objects.none()
    if es_direccion:
        sin_responsable = MermaInsumo.objects.filter(
            estatus=MermaInsumo.ESTATUS_SIN_RESPONSABLE
        ).select_related("sucursal", "reportado_por")
        if not sucursal and sin_responsable.exists():
            sucursal = sin_responsable.first().sucursal
            solo_aprobacion = True
    if not sucursal and pendientes.exists():
        sucursal = pendientes.first().sucursal
        solo_aprobacion = True
    if not sucursal and es_direccion:
        solo_aprobacion = True
    if not sucursal and not es_direccion:
        raise PermissionDenied("Tu sesión no tiene una sucursal operativa asignada.")
    insumos, catalogo_insumos_no_disponible = (
        ([], False) if solo_aprobacion else _cargar_insumos_sucursal(sucursal)
    )
    return render(
        request,
        "operacion/sucursal_tools.html",
        {
            "sucursal": sucursal,
            "tab_activa": request.GET.get("tab") or ("mermas" if solo_aprobacion else "fallas"),
            "activos": Activo.objects.filter(sucursal=sucursal, activo=True).order_by("nombre", "id"),
            "categorias_equipo": CategoriaFalla.objects.filter(
                activo=True, tipo=CategoriaFalla.TIPO_EQUIPO
            ).order_by("orden", "nombre"),
            "categorias_instalacion": CategoriaFalla.objects.filter(
                activo=True, tipo=CategoriaFalla.TIPO_INSTALACION
            ).order_by("orden", "nombre"),
            "insumos": insumos,
            "catalogo_insumos_no_disponible": catalogo_insumos_no_disponible,
            "mermas_pendientes": pendientes,
            "solo_aprobacion": solo_aprobacion,
            "mermas_sin_responsable": sin_responsable,
            "jefes_candidatos": Empleado.objects.filter(
                activo=True, usuario_erp__is_active=True
            ).select_related("usuario_erp", "sucursal_ref").order_by("nombre") if es_direccion else [],
            "draft_fallas": request.session.pop("operacion_draft_fallas", {}),
            "draft_mermas": request.session.pop("operacion_draft_mermas", {}),
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


def _usuarios_direccion():
    return [user for user in get_user_model().objects.filter(is_active=True) if is_admin_or_dg(user)]


def _notificar_merma_sin_responsable(merma, actor):
    for usuario in _usuarios_direccion():
        _notificar_usuario_operacion(
            usuario=usuario, titulo=f"Sin responsable · merma en {merma.sucursal.nombre}",
            mensaje=f"{merma.nombre_point}: requiere asignación de responsable RRHH.",
            actor=actor, objeto_id=merma.pk,
        )


def _notificar_falla_mantenimiento(reporte, actor):
    usuarios = _usuarios_mantenimiento()
    crear_notificaciones(
        usuarios, titulo=f"Nueva falla en {reporte.sucursal.nombre}", mensaje=reporte.titulo,
        url="/mantenimiento/", actor=actor, objeto_tipo="ReporteFalla", objeto_id=reporte.pk,
    )
    emails = sorted({(usuario.email or "").strip() for usuario in usuarios if (usuario.email or "").strip()})
    if emails:
        send_mail(
            subject=f"Nueva falla en {reporte.sucursal.nombre}",
            message=f"{reporte.titulo}\n\n{reporte.descripcion}\n\nAbrir Mantenimiento: /mantenimiento/",
            from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "") or None,
            recipient_list=emails, fail_silently=False,
        )


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
        return _respuesta_error(request, error="Selecciona una categoría activa.", tab="fallas", anchor="falla-form")

    activo = None
    if tipo == ReporteFalla.OBJETIVO_EQUIPO:
        activo = Activo.objects.filter(pk=data.get("activo_id"), sucursal=sucursal, activo=True).first()
        if not activo:
            return _respuesta_error(request, error="El equipo no pertenece a tu sucursal.", tab="fallas", anchor="falla-form")

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
            return _respuesta_error(request, error=str(exc), tab="fallas", anchor="falla-form")
        return _respuesta_error(
            request, error="Revisa la captura.", tab="fallas", anchor="falla-form", fields=exc.message_dict
        )
    with transaction.atomic():
        reporte.save()
        BitacoraFalla.objects.create(
            reporte=reporte, usuario=request.user, estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
            comentario="Reporte creado desde App Operativa y enviado a Mantenimiento.",
        )
    _on_commit_seguro(lambda: _notificar_falla_mantenimiento(reporte, request.user))
    return _respuesta_creacion(
        request, payload={"id": reporte.id, "estatus": reporte.estatus, "destino": "Mantenimiento"},
        tab="fallas", anchor="falla-form", mensaje="Reporte enviado a Mantenimiento.",
    )


@login_required
@require_GET
def mermas_insumos_catalogo_api(request):
    sucursal = _sucursal_operativa_usuario(request.user)
    if not sucursal:
        return JsonResponse({"error": "Tu sesión no tiene una sucursal operativa asignada."}, status=403)
    codigo_point = (request.GET.get("codigo_point") or "").strip()
    if codigo_point:
        try:
            row = consultar_existencia_insumo_point(sucursal, codigo_point)
        except ValidationError as exc:
            return JsonResponse({"error": str(exc)}, status=400)
        except PointLiveInventoryLookupError:
            logger.exception(
                "No fue posible consultar existencia en vivo para sucursal=%s codigo=%s",
                sucursal.pk,
                codigo_point,
            )
            return JsonResponse(
                {"error": "No pudimos consultar la existencia vigente en Point. Reintenta."},
                status=503,
            )
        return JsonResponse(
            {
                "sucursal": {"id": sucursal.id, "nombre": sucursal.nombre},
                "insumo": {
                    "codigo_point": row.codigo_point,
                    "nombre": row.nombre_point,
                    "unidad": row.unidad_point,
                    "existencia": str(row.existencia),
                    "snapshot_en": row.snapshot_capturado_en.isoformat(),
                },
            }
        )
    rows, no_disponible = _cargar_insumos_sucursal(sucursal)
    if no_disponible:
        return JsonResponse(
            {"error": "No pudimos cargar las existencias de Point. Reintenta en unos momentos."},
            status=503,
        )
    return JsonResponse(
        {
            "sucursal": {"id": sucursal.id, "nombre": sucursal.nombre},
            "insumos": [
                {
                    "codigo_point": row.codigo_point,
                    "nombre": row.nombre_point,
                    "unidad": row.unidad_point,
                    "existencia": None,
                    "snapshot_en": None,
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

    rows, no_disponible = _cargar_insumos_sucursal(sucursal)
    if no_disponible:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.content_type == "application/json":
            return JsonResponse(
                {"error": "No pudimos validar las existencias de Point. No se registró ninguna merma."},
                status=503,
            )
        messages.error(request, "No pudimos validar las existencias de Point. No se registró ninguna merma.")
        return redirect(f"{reverse('operacion:sucursal_tools')}?tab=mermas#merma-form")
    eligible = {item.codigo_point: item for item in rows}
    codigo_point = (data.get("codigo_point") or "").strip()
    if codigo_point not in eligible:
        return _respuesta_error(
            request, error="El insumo no está habilitado para esta sucursal.", tab="mermas", anchor="merma-form"
        )
    try:
        row = consultar_existencia_insumo_point(
            sucursal,
            codigo_point,
            insumo_recibido=eligible[codigo_point],
        )
    except ValidationError as exc:
        return _respuesta_error(
            request, error=str(exc), tab="mermas", anchor="merma-form"
        )
    except PointLiveInventoryLookupError:
        logger.exception(
            "No fue posible revalidar existencia Point para sucursal=%s codigo=%s",
            sucursal.pk,
            codigo_point,
        )
        if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.content_type == "application/json":
            return JsonResponse(
                {"error": "No pudimos validar la existencia vigente en Point. No se registró ninguna merma."},
                status=503,
            )
        messages.error(
            request,
            "No pudimos validar la existencia vigente en Point. No se registró ninguna merma.",
        )
        return redirect(f"{reverse('operacion:sucursal_tools')}?tab=mermas#merma-form")
    try:
        cantidad = Decimal(str(data.get("cantidad") or ""))
    except (InvalidOperation, ValueError):
        return _respuesta_error(request, error="Captura una cantidad válida.", tab="mermas", anchor="merma-form")
    if not cantidad.is_finite() or cantidad <= 0 or cantidad > row.existencia:
        return _respuesta_error(
            request, error="La cantidad debe ser positiva y no superar la existencia de Point.",
            tab="mermas", anchor="merma-form",
        )

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
        return _respuesta_error(request, error=str(exc), tab="mermas", anchor="merma-form")
    except ValidationError as exc:
        return _respuesta_error(
            request, error="Revisa la captura.", tab="mermas", anchor="merma-form", fields=exc.message_dict
        )
    if merma.jefe_inmediato_id:
        _on_commit_seguro(lambda: _notificar_usuario_operacion(
            usuario=merma.jefe_inmediato, titulo=f"Merma por aprobar · {sucursal.nombre}",
            mensaje=f"{merma.nombre_point}: {merma.cantidad_reportada} {merma.unidad_point}",
            actor=request.user, objeto_id=merma.pk,
        ))
    else:
        _on_commit_seguro(lambda: _notificar_merma_sin_responsable(merma, request.user))
    return _respuesta_creacion(
        request, payload={"id": merma.id, "estatus": merma.estatus}, tab="mermas", anchor="merma-form",
        mensaje=(
            "Merma enviada a tu jefe inmediato."
            if merma.jefe_inmediato_id else "Merma registrada sin responsable; Dirección fue notificada."
        ),
    )


@login_required
@require_POST
def mermas_insumos_aprobar_api(request, merma_id):
    try:
        data = json.loads(request.body or "{}") if request.content_type == "application/json" else request.POST
        cantidad = Decimal(str(data.get("cantidad") or ""))
    except (json.JSONDecodeError, InvalidOperation, ValueError):
        return _respuesta_accion_merma(request, merma_id=merma_id, error="Captura una cantidad válida.")
    if not cantidad.is_finite():
        return _respuesta_accion_merma(request, merma_id=merma_id, error="Captura una cantidad válida.")
    with transaction.atomic():
        merma = MermaInsumo.objects.filter(pk=merma_id, jefe_inmediato=request.user).first()
        if not merma:
            return _respuesta_accion_merma(
                request, merma_id=merma_id, error="La merma no está asignada a este jefe inmediato."
            )
        if merma.estatus == MermaInsumo.ESTATUS_APROBADA and hasattr(merma, "orden_point"):
            orden = merma.orden_point
            return _respuesta_accion_merma(
                request, merma_id=merma.id,
                payload={"id": merma.id, "estatus": merma.estatus, "orden_id": orden.id},
                mensaje="La merma ya estaba aprobada.",
            )
        try:
            aprobada = merma.aprobar(jefe=request.user, cantidad=cantidad, motivo=data.get("motivo") or "")
            orden = OrdenAjustePoint.crear_desde_merma(aprobada)
            orden = simular_orden_ajuste_point(orden.id)
        except (ValidationError, IntegrityError) as exc:
            message = "; ".join(exc.messages) if isinstance(exc, ValidationError) else "La orden ya existe."
            return _respuesta_accion_merma(request, merma_id=merma_id, error=message)
    aprobada.refresh_from_db()
    _on_commit_seguro(lambda: _notificar_usuario_operacion(
        usuario=aprobada.reportado_por,
        titulo=f"Merma {aprobada.get_estatus_display().lower()} · {aprobada.sucursal.nombre}",
        mensaje=f"{aprobada.nombre_point}: {aprobada.cantidad_aprobada} {aprobada.unidad_point}",
        actor=request.user, objeto_id=aprobada.pk,
    ))
    return _respuesta_accion_merma(
        request, merma_id=merma.id,
        payload={"id": merma.id, "estatus": aprobada.estatus, "orden_id": orden.id, "orden_estatus": orden.estatus},
        mensaje="Merma aprobada y orden de ajuste simulada.",
    )


@login_required
@require_POST
def mermas_insumos_decidir_api(request, merma_id):
    try:
        data = json.loads(request.body or "{}") if request.content_type == "application/json" else request.POST
        merma = decidir_merma_insumo(
            merma_id=merma_id, jefe=request.user, accion=data.get("accion"), motivo=data.get("motivo")
        )
    except json.JSONDecodeError:
        return _respuesta_accion_merma(
            request, merma_id=merma_id, error="La solicitud no contiene JSON válido."
        )
    except ValidationError as exc:
        return _respuesta_accion_merma(request, merma_id=merma_id, error="; ".join(exc.messages))
    _on_commit_seguro(lambda: _notificar_usuario_operacion(
        usuario=merma.reportado_por, titulo=f"Merma {merma.get_estatus_display().lower()}",
        mensaje=(data.get("motivo") or "").strip(), actor=request.user, objeto_id=merma.pk,
    ))
    return _respuesta_accion_merma(
        request, merma_id=merma.id, payload={"id": merma.id, "estatus": merma.estatus},
        mensaje=f"Merma {merma.get_estatus_display().lower()}.",
    )


@login_required
@require_POST
def mermas_insumos_reenviar_api(request, merma_id):
    try:
        data = json.loads(request.body or "{}") if request.content_type == "application/json" else request.POST
        merma = reenviar_merma_aclarada(
            merma_id=merma_id, usuario=request.user, cantidad=data.get("cantidad"),
            comentario=data.get("comentario"), motivo=data.get("motivo"),
        )
    except (json.JSONDecodeError, InvalidOperation, ValueError):
        return _respuesta_accion_merma(
            request, merma_id=merma_id, error="Captura una cantidad válida."
        )
    except (ValidationError, MermaInsumo.DoesNotExist) as exc:
        message = "; ".join(exc.messages) if isinstance(exc, ValidationError) else "La merma no existe."
        return _respuesta_accion_merma(request, merma_id=merma_id, error=message)
    _on_commit_seguro(lambda: _notificar_usuario_operacion(
        usuario=merma.jefe_inmediato, titulo=f"Merma aclarada · {merma.sucursal.nombre}",
        mensaje=f"{merma.nombre_point}: {merma.cantidad_reportada} {merma.unidad_point}",
        actor=request.user, objeto_id=merma.pk,
    ))
    return _respuesta_accion_merma(
        request, merma_id=merma.id, payload={"id": merma.id, "estatus": merma.estatus},
        mensaje="Aclaración enviada nuevamente a tu jefe.",
    )


@login_required
@require_POST
def mermas_insumos_reasignar_api(request, merma_id):
    try:
        data = json.loads(request.body or "{}") if request.content_type == "application/json" else request.POST
        jefe = Empleado.objects.select_related("usuario_erp").get(pk=data.get("jefe_empleado_id"))
        merma = reasignar_merma_sin_responsable(
            merma_id=merma_id, actor=request.user, jefe_empleado=jefe, motivo=data.get("motivo")
        )
    except json.JSONDecodeError:
        return _respuesta_accion_merma(
            request, merma_id=merma_id, error="La solicitud no contiene JSON válido."
        )
    except (ValidationError, Empleado.DoesNotExist, MermaInsumo.DoesNotExist) as exc:
        message = "; ".join(exc.messages) if isinstance(exc, ValidationError) else "La merma o responsable no existe."
        return _respuesta_accion_merma(request, merma_id=merma_id, error=message)
    _on_commit_seguro(lambda: _notificar_usuario_operacion(
        usuario=merma.jefe_inmediato, titulo=f"Merma reasignada · {merma.sucursal.nombre}",
        mensaje=f"{merma.nombre_point}: {merma.cantidad_reportada} {merma.unidad_point}",
        actor=request.user, objeto_id=merma.pk,
    ))
    return _respuesta_accion_merma(
        request, merma_id=merma.id, payload={"id": merma.id, "estatus": merma.estatus},
        mensaje="Responsable asignado y notificado.",
    )


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
