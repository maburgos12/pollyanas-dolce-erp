from django import forms
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.paginator import Paginator
from django.db.models import Q
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import get_object_or_404, render
from django.template.loader import render_to_string
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_http_methods

from .services_documentos_financieros import (TIPOS, actor_actual, confirmar_documento, detalle_fuente,
    documentos_visibles, fuentes_autorizadas, puede_confirmar, trabajos_autorizados)


@login_required
@never_cache
@require_http_methods(["GET", "POST"])
def documentos_trabajo(request, tipo, pk):
    actor = actor_actual(request.user)
    if tipo not in {"orden", "falla"}:
        return HttpResponseBadRequest("Tipo de trabajo inválido.")
    trabajo = get_object_or_404(trabajos_autorizados(actor, tipo), pk=pk)
    datos = request.POST if request.method == "POST" else request.GET
    doc_tipo = datos.get("tipo_documento", "obligacion")
    if doc_tipo not in dict(TIPOS):
        return HttpResponseBadRequest("Tipo documental inválido.")
    mensaje, ok, status = "", True, 200
    error_documento_id = ""
    documento_id = None
    try:
        documento_id = forms.IntegerField(min_value=1, max_value=2**63 - 1,
            required=request.method == "POST" or bool(datos.get("documento_id"))).clean(datos.get("documento_id"))
    except ValidationError as exc:
        error_documento_id = "ID de documento inválido: " + " ".join(exc.messages)
        mensaje, ok, status = error_documento_id, False, 400
    if request.method == "POST" and not error_documento_id:
        try:
            _, creado = confirmar_documento(user=request.user, tipo_trabajo=tipo, trabajo_id=pk,
                tipo_documento=doc_tipo, documento_id=documento_id,
                motivo=datos.get("motivo"), evidencia=datos.get("evidencia"), confirmado=datos.get("confirmado") == "on")
            mensaje = "Soporte documental confirmado." if creado else "Este soporte ya estaba confirmado. Se conservó su autor y evidencia originales."
            status = 201 if creado else 200
        except ValidationError as exc:
            mensaje, ok = " ".join(exc.messages), False
            status = 409 if exc.code == "conflict" else 400
    fuentes = fuentes_autorizadas(actor, doc_tipo) if not error_documento_id else []
    busqueda = str(datos.get("q", "")).strip()[:200]
    if busqueda and not error_documento_id:
        campo = {"obligacion": "concepto", "gasto": "comentario", "cfdi": "uuid", "movimiento": "descripcion"}[doc_tipo]
        filtro = Q(**{f"{campo}__icontains": busqueda})
        if busqueda.isdecimal():
            filtro |= Q(pk=int(busqueda))
        fuentes = fuentes.filter(filtro)
    pagina = Paginator(fuentes.order_by("-pk") if not error_documento_id else [], 20).get_page(datos.get("page"))
    opciones = [{"pk": f.pk, "detalle": detalle_fuente(doc_tipo, f, actor), "puede_confirmar": puede_confirmar(actor, doc_tipo, f)} for f in pagina]
    elegida = fuentes_autorizadas(actor, doc_tipo).filter(pk=documento_id).first() if documento_id is not None else None
    if documento_id is not None and elegida is None and request.method == "GET":
        raise PermissionDenied("El documento no está disponible en tu alcance actual.")
    vinculos = []
    for v in documentos_visibles(actor, tipo, pk) if not error_documento_id else []:
        v.detalle = detalle_fuente(v.tipo_documento, getattr(v, v.tipo_documento), actor)
        vinculos.append(v)
    async_request = request.method == "POST" and ("application/json" in request.headers.get("Accept", "") or request.headers.get("X-Requested-With") == "XMLHttpRequest")
    contexto = {"error_documento_id": error_documento_id, "mostrar_mensaje": not async_request, "trabajo": trabajo, "tipo": tipo, "trabajo_id": pk, "tipos": TIPOS,
        "tipo_documento": doc_tipo, "q": busqueda, "pagina": pagina, "opciones": opciones,
        "elegida": elegida, "detalle_elegida": detalle_fuente(doc_tipo, elegida, actor) if elegida else None,
        "puede_crear": elegida is not None and puede_confirmar(actor, doc_tipo, elegida),
        "documento_id": str(datos.get("documento_id", "")), "motivo": datos.get("motivo", ""),
        "evidencia": datos.get("evidencia", ""), "confirmado": datos.get("confirmado") == "on",
        "vinculos": vinculos, "mensaje": mensaje, "ok": ok}
    if async_request:
        return JsonResponse({"ok": ok, "toast": {"type": "success" if ok else "error", "message": mensaje},
            "target": "#documentos-trabajo", "html": render_to_string("mantenimiento/_documentos_financieros.html", contexto, request=request)}, status=status)
    return render(request, "mantenimiento/documentos_financieros.html", contexto, status=status)
