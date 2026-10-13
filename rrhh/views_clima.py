"""Preparación interna de campañas de clima y accesos individuales."""

import base64
import json
from io import BytesIO
from pathlib import Path

import segno
from django import forms
from django.contrib.auth.decorators import login_required
from django.core import signing
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.validators import validate_email
from django.db import transaction
from django.db.models import Count
from django.http import Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.cache import never_cache

from core.access import can_manage_rrhh
from core.contactos import normalizar_telefono

from .models import CampanaClima, Empleado, ParticipanteClima, RespuestaClima


QUESTIONNAIRE = json.loads((Path(__file__).parent / "data" / "clima_laboral_2026.json").read_text())


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
                    label=pregunta["text"], required=False, max_length=2000,
                    widget=forms.Textarea(attrs={"rows": 4}),
                )


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
    for empleado in empleados:
        empleado.seleccionado = str(empleado.pk) in selected
        try:
            validate_email(empleado.email)
            correo = True
        except ValidationError:
            correo = False
        telefono = bool(normalizar_telefono(empleado.telefono))
        empleado.contacto_estado = "Correo y teléfono" if correo and telefono else "Correo" if correo else "Teléfono" if telefono else "Sin contacto"
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
        "campanas": CampanaClima.objects.select_related("creada_por").annotate(total=Count("participantes"))[:20],
        "cuestionario": QUESTIONNAIRE,
    })


@login_required
def detalle_clima(request, pk):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied
    campana = get_object_or_404(CampanaClima, pk=pk)
    participantes = campana.participantes.all()
    respondieron = participantes.filter(respondio=True).count()
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
        "respondieron": respondieron,
        "cerrada": cerrada,
        "resultados": resultados,
        "cuestionario": campana.cuestionario,
    })


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
        return HttpResponse("Tu respuesta quedó registrada. Gracias.", headers=headers)
    response = render(request, "rrhh/clima_responder.html", {"campana": participante.campana, "form": form})
    for name, value in headers.items():
        response[name] = value
    return response
