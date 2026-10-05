from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.template.loader import render_to_string
from django.views.decorators.http import require_http_methods
from activos.models import Activo
from core.access import can_manage_inventario, can_view_inventario
from .access_departamentales import (
    _puede_ver_solicitud,
    puede_gestionar_compras_departamentales,
)
from .models import RecepcionItemDepartamental
from .services_procedencia_adquisicion import (
    actor_actual,
    confirmar_procedencia,
    procedencias_visibles,
)


@login_required
@require_http_methods(["GET", "POST"])
def procedencia_adquisicion(request, recepcion_pk):
    actor = actor_actual(request.user)
    recepcion = get_object_or_404(
        RecepcionItemDepartamental.objects.select_related(
            "linea_orden__item__solicitud__area", "linea_orden__intento"
        ),
        pk=recepcion_pk,
    )
    item = recepcion.linea_orden.item
    if not can_view_inventario(actor) or not _puede_ver_solicitud(
        actor, item.solicitud
    ):
        raise PermissionDenied(
            "Necesitas consultar la solicitud de origen y el equipo."
        )
    datos = request.POST if request.method == "POST" else request.GET
    mensaje, ok, status = "", True, 200
    fuente_disponible = True
    if request.method == "POST":
        try:
            _, creado = confirmar_procedencia(
                user=request.user,
                recepcion_id=recepcion.pk,
                activo_id=datos.get("activo_id"),
                version=datos.get("version"),
                referencia_unidad=datos.get("referencia_unidad"),
                motivo=datos.get("motivo"),
                evidencia=datos.get("evidencia"),
                confirmado=datos.get("confirmado") == "on",
            )
            mensaje = (
                "Procedencia documental confirmada."
                if creado
                else "La confirmación ya existía. Se conserva su autor y evidencia originales."
            )
            status = 201 if creado else 200
        except ValidationError as exc:
            mensaje, ok = " ".join(exc.messages), False
            status = 409 if exc.code == "conflict" else 400
        # Mostrar el origen actual sin sustituir la versión enviada en el formulario.
        actual = (
            RecepcionItemDepartamental.objects.select_related(
                "linea_orden__item__solicitud__area", "linea_orden__intento"
            )
            .filter(pk=recepcion.pk)
            .first()
        )
        fuente_disponible = actual is not None
        if actual is not None:
            recepcion = actual
        else:
            mensaje, ok, status = (
                "La recepción original ya no está disponible.",
                False,
                409,
            )
    puede_crear = (
        fuente_disponible
        and puede_gestionar_compras_departamentales(actor)
        and can_manage_inventario(actor)
    )
    es_async = request.method == "POST" and (
        "application/json" in request.headers.get("Accept", "")
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    )
    contexto = {
        "mostrar_mensaje": not es_async,
        "recepcion": recepcion,
        "item": item,
        "equipos": Activo.objects.all() if puede_crear else Activo.objects.none(),
        "puede_crear": puede_crear,
        "vinculos": procedencias_visibles(actor, recepcion_id=recepcion.pk),
        "mensaje": mensaje,
        "ok": ok,
        "id_elegido": str(datos.get("activo_id", "")),
        "version": datos.get("version", recepcion.linea_orden.intento.version),
        "referencia_unidad": datos.get("referencia_unidad", ""),
        "motivo": datos.get("motivo", ""),
        "evidencia": datos.get("evidencia", ""),
        "confirmado": datos.get("confirmado") == "on",
    }
    if es_async:
        return JsonResponse(
            {
                "ok": ok,
                "toast": {"type": "success" if ok else "error", "message": mensaje},
                "target": "#procedencia-adquisicion",
                "html": render_to_string(
                    "compras/departamentales/_procedencia_adquisicion.html",
                    contexto,
                    request=request,
                ),
            },
            status=status,
        )
    return render(
        request,
        "compras/departamentales/procedencia_adquisicion.html",
        contexto,
        status=status,
    )


@login_required
@require_http_methods(["GET"])
def procedencia_del_activo(request, activo_pk):
    actor = actor_actual(request.user)
    if not can_view_inventario(actor):
        raise PermissionDenied
    vinculos = procedencias_visibles(actor, activo_id=activo_pk)
    if not vinculos:
        raise PermissionDenied(
            "No hay procedencias consultables con tus permisos actuales."
        )
    return render(
        request,
        "compras/departamentales/procedencia_del_activo.html",
        {"activo": vinculos[0].activo, "vinculos": vinculos},
    )
