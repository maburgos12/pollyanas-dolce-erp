"""Preparación interna de campañas de clima; todavía no emite accesos."""

import json
from pathlib import Path

from django import forms
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.validators import validate_email
from django.db import transaction
from django.db.models import Count
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from core.access import can_manage_rrhh
from core.contactos import normalizar_telefono

from .models import CampanaClima, Empleado, ParticipanteClima


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
    return render(request, "rrhh/clima_detalle.html", {
        "campana": campana,
        "participantes": campana.participantes.select_related("empleado"),
        "cuestionario": campana.cuestionario,
    })
