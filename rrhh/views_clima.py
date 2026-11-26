"""Preparación interna de campañas de clima y accesos individuales."""

import base64
import json
import logging
import os
from collections import Counter
from io import BytesIO
from pathlib import Path

import segno
from django import forms
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core import signing
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.validators import validate_email
from django.db import transaction
from django.db.models import Count
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_POST

from core.access import can_manage_rrhh
from core.contactos import normalizar_telefono

from .models import AvisoClima, CampanaClima, Empleado, EventoCampanaClima, ParticipanteClima, RespuestaClima


QUESTIONNAIRE = json.loads((Path(__file__).parent / "data" / "clima_laboral_2026.json").read_text())
logger = logging.getLogger(__name__)


def _encolar_confirmacion(aviso_id):
    from .tasks import enviar_confirmacion_clima
    try:
        enviar_confirmacion_clima.delay(aviso_id)
    except Exception:
        logger.exception("No se pudo encolar confirmación de clima; queda pendiente para el barrido diario")


class CampanaClimaForm(forms.Form):
    titulo = forms.CharField(max_length=160, label="Nombre de la campaña")
    fecha_cierre = forms.DateField(
        label="Fecha límite prevista",
        widget=forms.DateInput(attrs={"type": "date"}, format="%Y-%m-%d"),
        input_formats=["%Y-%m-%d"],
    )

    def clean_fecha_cierre(self):
        value = self.cleaned_data["fecha_cierre"]
        if value <= timezone.localdate():
            raise forms.ValidationError("La fecha límite debe ser posterior a hoy.")
        return value


class RespuestasClimaForm(forms.Form):
    def __init__(self, cuestionario, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for indice, pregunta in enumerate(q for s in cuestionario["sections"] for q in s["questions"]):
            if pregunta["type"] == "choice":
                self.fields[f"q_{indice}"] = forms.ChoiceField(
                    label=pregunta["text"], choices=[(opcion, opcion) for opcion in pregunta["options"]],
                    widget=forms.RadioSelect,
                )
            else:
                self.fields[f"q_{indice}"] = forms.CharField(
                    label=pregunta["text"], max_length=2000,
                    widget=forms.Textarea(attrs={"rows": 4}),
                )


def _cobertura_contacto(pares, solo_pendientes=False):
    filas = []
    for persona, empleado in pares:
        correo = (empleado.email or "").strip().lower()
        try:
            validate_email(correo)
        except ValidationError:
            correo = ""
        telefono = normalizar_telefono(empleado.telefono)
        persona.contacto_estado = "Correo y teléfono" if correo and telefono else "Correo" if correo else "Teléfono" if telefono else "Sin contacto"
        persona.contacto_inactivo = not empleado.activo
        filas.append((persona, correo, telefono))
    correos = Counter(correo for _, correo, _ in filas if correo)
    telefonos = Counter(telefono for _, _, telefono in filas if telefono)
    for persona, correo, telefono in filas:
        persona.contacto_compartido = (bool(correo) and correos[correo] > 1) or (bool(telefono) and telefonos[telefono] > 1)
        persona.contacto_listo = persona.contacto_estado != "Sin contacto" and not persona.contacto_compartido and not persona.contacto_inactivo
    contadas = [persona for persona, _, _ in filas if not solo_pendientes or not persona.respondio]
    return {
        "total": len(contadas),
        "listos": sum(persona.contacto_listo for persona in contadas),
        "sin_contacto": sum(persona.contacto_estado == "Sin contacto" for persona in contadas),
        "compartidos": sum(persona.contacto_compartido for persona in contadas),
        "inactivos": sum(persona.contacto_inactivo for persona in contadas),
    }


@login_required
def campanas_clima(request):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied
    empleados = list(
        Empleado.objects.filter(activo=True, tipo_personal=Empleado.TIPO_POLLYANA)
        .select_related("sucursal_ref").order_by("nombre", "id")
    )
    form = CampanaClimaForm(request.POST or None)
    selected = set(request.POST.getlist("empleados")) if request.method == "POST" else {str(e.pk) for e in empleados}
    cobertura = _cobertura_contacto((empleado, empleado) for empleado in empleados)
    for empleado in empleados:
        empleado.seleccionado = str(empleado.pk) in selected
    if request.method == "POST" and form.is_valid():
        eligible = {str(e.pk): e for e in empleados}
        if not selected or not selected <= eligible.keys():
            form.add_error(None, "Selecciona al menos una persona activa del padrón mostrado.")
        else:
            with transaction.atomic():
                campana = CampanaClima.objects.create(
                    titulo=form.cleaned_data["titulo"],
                    fecha_cierre=form.cleaned_data["fecha_cierre"],
                    cuestionario=QUESTIONNAIRE,
                    creada_por=request.user,
                )
                ParticipanteClima.objects.bulk_create([
                    ParticipanteClima(
                        campana=campana, empleado=eligible[pk], nombre=eligible[pk].nombre,
                        departamento=eligible[pk].departamento,
                        sucursal=eligible[pk].sucursal_display,
                    ) for pk in sorted(selected)
                ])
            return redirect("rrhh:clima_detalle", pk=campana.pk)
    return render(request, "rrhh/clima.html", {
        "form": form, "empleados": empleados, "selected": selected,
        "cobertura": cobertura,
        "campanas": CampanaClima.objects.select_related("creada_por").annotate(total=Count("participantes"))[:20],
        "cuestionario": QUESTIONNAIRE,
    })


@login_required
def detalle_clima(request, pk):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied
    campana = get_object_or_404(CampanaClima, pk=pk)
    participantes = list(campana.participantes.select_related("empleado"))
    cobertura = _cobertura_contacto(((persona, persona.empleado) for persona in participantes), solo_pendientes=True)
    from .services_clima_avisos import _habilitado, digest_telefono
    avisos_todos = list(AvisoClima.objects.filter(participante__campana=campana))
    avisos = {a.participante_id: a for a in avisos_todos if a.tipo == AvisoClima.INVITACION}
    recordatorios = {a.participante_id: a for a in avisos_todos if a.tipo == AvisoClima.RECORDATORIO}
    confirmaciones = {a.participante_id: a for a in avisos_todos if a.tipo == AvisoClima.CONFIRMACION}
    telefonos = {persona.pk: normalizar_telefono(persona.empleado.telefono) for persona in participantes}
    frecuencia = Counter(telefono for telefono in telefonos.values() if telefono)
    for persona in participantes:
        telefono = telefonos[persona.pk] if frecuencia[telefonos[persona.pk]] == 1 else None
        persona.telefono_sufijo = telefono[-4:] if telefono else ""
        persona.whatsapp_listo = bool(telefono and persona.empleado.activo and
                                      persona.empleado.tipo_personal == Empleado.TIPO_POLLYANA)
        persona.autorizacion_vigente = bool(persona.envio_whatsapp_autorizado and telefono and
                                           persona.telefono_autorizado_digest == digest_telefono(telefono))
        persona.aviso_invitacion = avisos.get(persona.pk)
        persona.aviso_recordatorio = recordatorios.get(persona.pk)
        persona.aviso_confirmacion = confirmaciones.get(persona.pk)
    activos_omitidos = list(Empleado.objects.filter(activo=True, tipo_personal=Empleado.TIPO_POLLYANA).exclude(
        pk__in=[p.empleado_id for p in participantes]
    ).order_by("nombre").values("id", "nombre", "departamento"))
    seleccionados = {p.empleado_id for p in participantes}
    padron_actual = list(Empleado.objects.filter(activo=True, tipo_personal=Empleado.TIPO_POLLYANA)
                         .order_by("nombre").values("id", "nombre", "departamento"))
    for empleado in padron_actual:
        empleado["incluido"] = empleado["id"] in seleccionados
    autorizados = sum(p.autorizacion_vigente for p in participantes if not p.respondio)
    whatsapp_listos = sum(p.whatsapp_listo for p in participantes if not p.respondio)
    lote_autorizables = sum(p.whatsapp_listo and not p.autorizacion_vigente and not p.respondio
                           and not p.aviso_invitacion for p in participantes)
    lote_revisar = sum(not p.whatsapp_listo and not p.respondio for p in participantes)
    avisos_resumen = Counter(a.estado for a in avisos.values())
    entrega_resumen = Counter(a.entrega_estado for a in avisos.values() if a.estado == "ENVIADO")
    sin_intento = sum(p.autorizacion_vigente and not p.respondio and p.pk not in avisos for p in participantes)
    respondieron = len(participantes) - cobertura["total"]
    respuestas = list(campana.respuestas.values("departamento", "respuestas"))
    # No publicar un desglose que permita deducir el grupo pequeño por diferencia.
    areas = {}
    for respuesta in respuestas:
        areas.setdefault(respuesta["departamento"] or "Sin departamento", []).append(respuesta["respuestas"])
    cerrada = not campana.recepcion_habilitada or campana.fecha_cierre < timezone.localdate()
    grupos = {"Toda la campaña": [r["respuestas"] for r in respuestas]} if cerrada and len(respuestas) >= 5 else {}
    if cerrada and areas and all(len(valores) >= 5 for valores in areas.values()):
        grupos.update(areas)
    resultados = []
    for nombre, valores in grupos.items():
        preguntas = []
        for indice, pregunta in enumerate(q for s in campana.cuestionario["sections"] for q in s["questions"]):
            if pregunta["type"] == "choice":
                preguntas.append({"texto": pregunta["text"], "opciones": [
                    {"texto": opcion, "total": sum(r.get(f"q_{indice}") == opcion for r in valores),
                     "porcentaje": round(100 * sum(r.get(f"q_{indice}") == opcion for r in valores) / len(valores))}
                    for opcion in pregunta["options"]
                ]})
        resultados.append({"nombre": nombre, "total": len(valores), "preguntas": preguntas})
    return render(request, "rrhh/clima_detalle.html", {
        "campana": campana,
        "participantes": participantes,
        "cobertura": cobertura,
        "respondieron": respondieron,
        "cerrada": cerrada,
        "resultados": resultados,
        "cuestionario": campana.cuestionario,
        "activos_omitidos": activos_omitidos,
        "padron_actual": padron_actual,
        "autorizados": autorizados,
        "tiene_autorizaciones": any(p.envio_whatsapp_autorizado for p in participantes),
        "tiene_decisiones_participante": campana.eventos.filter(participante__isnull=False).exists(),
        "whatsapp_listos": whatsapp_listos,
        "lote_autorizables": lote_autorizables,
        "lote_revisar": lote_revisar,
        "avisos_resumen": avisos_resumen,
        "entrega_resumen": entrega_resumen,
        "webhook_configurado": bool(os.getenv("META_APP_SECRET") and os.getenv("CLIMA_META_WEBHOOK_VERIFY_TOKEN")
                                    and os.getenv("META_WHATSAPP_PHONE_NUMBER_ID")),
        "sin_intento": sin_intento,
        "envios_habilitados": _habilitado(),
        "puede_decidir": request.user.is_superuser or campana.creada_por_id == request.user.pk,
        "eventos": campana.eventos.select_related("actor")[:12],
    })


@login_required
@require_POST
def accion_clima(request, pk):
    """Decisiones separadas y auditadas; ninguna acción implícita al crear borrador."""
    if not can_manage_rrhh(request.user):
        raise PermissionDenied
    from .services_clima_avisos import _habilitado, _telefono_unico, digest_telefono
    from .tasks import procesar_avisos_clima_campana

    progressive = "application/json" in request.headers.get("Accept", "") or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    destino = reverse("rrhh:clima_detalle", args=[pk]) + "#preparacion-clima"

    def responder(mensaje, *, error=False):
        if progressive:
            return JsonResponse({"ok": not error, "toast": {"type": "error" if error else "success", "message": mensaje},
                                 "redirect": destino, "reload": not error}, status=400 if error else 200)
        (messages.error if error else messages.success)(request, mensaje)
        return redirect(destino)

    accion = request.POST.get("accion")
    with transaction.atomic():
        campana = get_object_or_404(CampanaClima.objects.select_for_update(), pk=pk)
        if not (request.user.is_superuser or campana.creada_por_id == request.user.pk):
            raise PermissionDenied
        if campana.fecha_cierre < timezone.localdate():
            return responder("La fecha límite ya venció.", error=True)
        if accion == "padron":
            if (campana.recepcion_habilitada or campana.envio_solicitado_en or
                    campana.participantes.filter(envio_whatsapp_autorizado=True).exists() or
                    campana.participantes.filter(respondio=True).exists() or
                    campana.eventos.filter(participante__isnull=False).exists() or
                    AvisoClima.objects.filter(participante__campana=campana).exists()):
                return responder("El padrón ya tiene autorizaciones, respuestas o envíos; no puede editarse.", error=True)
            ids = request.POST.getlist("empleados")
            if not ids or len(ids) != len(set(ids)):
                return responder("Selecciona personas activas sin duplicados.", error=True)
            elegibles = {str(e.pk): e for e in Empleado.objects.filter(activo=True, tipo_personal=Empleado.TIPO_POLLYANA)
                         .select_related("sucursal_ref")}
            if not set(ids) <= elegibles.keys():
                return responder("La selección incluye personas que ya no están activas en el padrón.", error=True)
            actuales = set(campana.participantes.values_list("empleado_id", flat=True))
            nuevos = {int(pk) for pk in ids}
            campana.participantes.exclude(empleado_id__in=nuevos).delete()
            ParticipanteClima.objects.bulk_create([
                ParticipanteClima(campana=campana, empleado=elegibles[str(pk)], nombre=elegibles[str(pk)].nombre,
                                 departamento=elegibles[str(pk)].departamento, sucursal=elegibles[str(pk)].sucursal_display)
                for pk in sorted(nuevos - actuales)
            ])
            EventoCampanaClima.objects.create(campana=campana, accion="PADRON", actor=request.user,
                                              detalle=f"{len(nuevos)} incluidos; {len(nuevos - actuales)} altas; {len(actuales - nuevos)} retiros")
            return responder(f"Padrón guardado: {len(nuevos)} personas incluidas.")
        if accion == "autorizar_lote":
            if campana.envio_solicitado_en:
                return responder("El envío ya fue solicitado; no se puede cambiar el lote.", error=True)
            if request.POST.get("titularidad_confirmada") != "si":
                return responder("Confirma que los WhatsApp del lote pertenecen a las personas incluidas.", error=True)
            personas = list(campana.participantes.select_for_update().select_related("empleado"))
            if not personas:
                return responder("La campaña no tiene personas incluidas.", error=True)
            telefonos = {p.pk: normalizar_telefono(p.empleado.telefono) for p in personas}
            frecuencia = Counter(t for t in telefonos.values() if t)
            con_intento = set(AvisoClima.objects.filter(
                participante__campana=campana, tipo=AvisoClima.INVITACION,
            ).values_list("participante_id", flat=True))
            nuevas = []
            revisar = 0
            ya_autorizadas = 0
            ahora = timezone.now()
            for persona in personas:
                if persona.respondio or persona.pk in con_intento:
                    continue
                telefono = telefonos[persona.pk]
                if (not persona.empleado.activo or persona.empleado.tipo_personal != Empleado.TIPO_POLLYANA
                        or not telefono or frecuencia[telefono] != 1):
                    revisar += 1
                    continue
                digest = digest_telefono(telefono)
                if persona.envio_whatsapp_autorizado and persona.telefono_autorizado_digest == digest:
                    ya_autorizadas += 1
                    continue
                persona.envio_whatsapp_autorizado = True
                persona.telefono_autorizado_digest = digest
                persona.autorizado_por = request.user
                persona.autorizado_en = ahora
                nuevas.append(persona)
            if not nuevas:
                return responder(f"No hay nuevos contactos aptos. Ya autorizados: {ya_autorizadas}; por revisar: {revisar}.", error=True)
            ParticipanteClima.objects.bulk_update(
                nuevas, ["envio_whatsapp_autorizado", "telefono_autorizado_digest", "autorizado_por", "autorizado_en"],
            )
            EventoCampanaClima.objects.create(
                campana=campana, accion="AUTORIZAR_LOTE", actor=request.user,
                detalle=f"{len(nuevas)} autorizados; {ya_autorizadas} ya autorizados; {revisar} por revisar",
            )
            return responder(f"Autorizados {len(nuevas)} contactos. Ya autorizados: {ya_autorizadas}; por revisar: {revisar}. La campaña sigue cerrada y no se enviaron mensajes.")
        if accion in {"autorizar", "revocar"}:
            if campana.envio_solicitado_en:
                return responder("El envío ya fue solicitado; no se puede cambiar el lote.", error=True)
            persona = get_object_or_404(ParticipanteClima.objects.select_for_update().select_related("empleado"),
                                        pk=request.POST.get("participante_id") or 0, campana=campana)
            if persona.respondio:
                return responder("Esta persona ya respondió.", error=True)
            if AvisoClima.objects.filter(participante=persona, tipo=AvisoClima.INVITACION).exists():
                return responder("Ya existe un intento de invitación para esta persona.", error=True)
            if accion == "autorizar":
                if request.POST.get("titularidad_confirmada") != "si":
                    return responder("Confirma que el WhatsApp pertenece a esta persona.", error=True)
                telefono = _telefono_unico(persona)
                if not telefono or not persona.empleado.activo or persona.empleado.tipo_personal != Empleado.TIPO_POLLYANA:
                    return responder("El empleado no está activo o su WhatsApp falta o está compartido.", error=True)
                persona.envio_whatsapp_autorizado = True
                persona.telefono_autorizado_digest = digest_telefono(telefono)
                persona.autorizado_por = request.user
                persona.autorizado_en = timezone.now()
                detalle = f"WhatsApp terminado en {telefono[-4:]}"
            else:
                persona.envio_whatsapp_autorizado = False
                persona.telefono_autorizado_digest = ""
                persona.autorizado_por = None
                persona.autorizado_en = None
                detalle = "Autorización retirada"
            persona.save(update_fields=["envio_whatsapp_autorizado", "telefono_autorizado_digest", "autorizado_por", "autorizado_en"])
            EventoCampanaClima.objects.create(campana=campana, participante=persona, accion=accion.upper(), actor=request.user, detalle=detalle)
            return responder(f"{persona.nombre}: {detalle}.")
        if accion not in {"abrir", "enviar"}:
            return responder("Acción no reconocida.", error=True)
        personas = list(campana.participantes.select_related("empleado"))
        autorizadas = [p for p in personas if p.envio_whatsapp_autorizado and not p.respondio]
        if not autorizadas:
            return responder("Autoriza al menos un destinatario pendiente antes de continuar.", error=True)
        telefonos = {p.pk: normalizar_telefono(p.empleado.telefono) for p in personas}
        frecuencia = Counter(telefono for telefono in telefonos.values() if telefono)
        invalidas = [p for p in autorizadas if not p.empleado.activo or p.empleado.tipo_personal != Empleado.TIPO_POLLYANA
                    or not (telefono := telefonos[p.pk]) or frecuencia[telefono] != 1
                    or p.telefono_autorizado_digest != digest_telefono(telefono)]
        if invalidas:
            return responder(f"Revisa {len(invalidas)} autorización(es): hubo bajas o cambios de teléfono.", error=True)
        if accion == "abrir":
            if campana.recepcion_habilitada:
                return responder("La campaña ya está abierta.")
            campana.recepcion_habilitada = True
            campana.abierta_por = request.user
            campana.abierta_en = timezone.now()
            campana.save(update_fields=["recepcion_habilitada", "abierta_por", "abierta_en"])
            EventoCampanaClima.objects.create(campana=campana, accion="ABRIR", actor=request.user,
                                              detalle=f"{len(autorizadas)} autorizados; {len(personas)} participantes")
            return responder("Campaña abierta para respuestas. Aún no se solicitó el envío.")
        if not campana.recepcion_habilitada or not campana.abierta_en:
            return responder("Abre la campaña antes de solicitar invitaciones.", error=True)
        if not _habilitado():
            return responder("El canal Meta está deshabilitado; no se solicitó ningún envío.", error=True)
        if campana.envio_solicitado_en:
            return responder("El envío de esta campaña ya fue solicitado.")
        campana.envio_solicitado_en = timezone.now()
        campana.envio_solicitado_por = request.user
        campana.save(update_fields=["envio_solicitado_en", "envio_solicitado_por"])
        EventoCampanaClima.objects.create(campana=campana, accion="ENVIAR", actor=request.user,
                                          detalle=f"{len(autorizadas)} destinatarios autorizados")
        transaction.on_commit(lambda: _encolar_campana(procesar_avisos_clima_campana, campana.pk))
    return responder("Envío solicitado. Revisa los estados de Meta en esta pantalla.")


def _encolar_campana(task, campana_id):
    try:
        task.delay(campana_id)
    except Exception:
        logger.exception("No se pudo encolar clima %s; el barrido diario retomará la campaña", campana_id)


@login_required
def acceso_individual_clima(request, pk, participante_id):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied
    participante = get_object_or_404(ParticipanteClima, pk=participante_id, campana_id=pk)
    token = signing.Signer(salt="rrhh.clima").sign(str(participante.pk))
    liga = request.build_absolute_uri(reverse("rrhh:clima_acceso_publico", args=[token]))
    qr = BytesIO()
    segno.make_qr(liga, error="h").save(qr, kind="svg", scale=6, border=4)
    return render(request, "rrhh/clima_acceso.html", {
        "participante": participante, "liga": liga,
        "qr_data": base64.b64encode(qr.getvalue()).decode("ascii"),
    })


@never_cache
def acceso_publico_clima(request, token):
    try:
        pk = int(signing.Signer(salt="rrhh.clima").unsign(token))
    except (signing.BadSignature, ValueError):
        raise Http404
    participante = get_object_or_404(ParticipanteClima.objects.select_related("campana"), pk=pk)
    headers = {"X-Robots-Tag": "noindex", "Referrer-Policy": "same-origin"}
    if participante.campana.fecha_cierre < timezone.localdate():
        return HttpResponse("Esta liga ya venció.", status=410, headers=headers)
    if not participante.campana.recepcion_habilitada:
        return HttpResponse("La encuesta todavía no está disponible. Conserva esta liga para cuando se abra la campaña.", headers=headers)
    if participante.respondio:
        return HttpResponse("Tu respuesta ya fue registrada. Gracias.", headers=headers)
    form = RespuestasClimaForm(participante.campana.cuestionario, request.POST or None)
    if request.method == "POST" and form.is_valid():
        with transaction.atomic():
            locked = ParticipanteClima.objects.select_for_update().select_related("campana").get(pk=participante.pk)
            if not locked.campana.recepcion_habilitada or locked.campana.fecha_cierre < timezone.localdate():
                return HttpResponse("Esta encuesta ya no está disponible.", status=410, headers=headers)
            if locked.respondio:
                return HttpResponse("Tu respuesta ya fue registrada. Gracias.", headers=headers)
            RespuestaClima.objects.create(
                campana=locked.campana, departamento=locked.departamento,
                respuestas=form.cleaned_data,
            )
            locked.respondio = True
            locked.save(update_fields=["respondio"])
            from .services_clima_avisos import _habilitado
            from .models import AvisoClima
            if locked.envio_whatsapp_autorizado and _habilitado():
                aviso, _ = AvisoClima.objects.get_or_create(
                    participante=locked, tipo=AvisoClima.CONFIRMACION,
                )
                transaction.on_commit(lambda: _encolar_confirmacion(aviso.pk))
        return HttpResponse("¡Gracias por tus respuestas! Tu encuesta completa quedó registrada correctamente.", headers=headers)
    response = render(request, "rrhh/clima_responder.html", {"campana": participante.campana, "form": form})
    for name, value in headers.items():
        response[name] = value
    return response
