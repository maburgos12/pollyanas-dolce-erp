"""Calendario de cumpleaños y captura verificada de Capital Humano."""
import calendar
import re
from datetime import date, timedelta
from urllib.parse import urlencode

from django import forms
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import Http404, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from core.models import AuditLog
from .models import AvisoCumpleanos
from .services_cumpleanos import (
    empleados_visibles, eventos_entre, puede_gestionar_cumpleanos,
    puede_ver_cumpleanos, vista_global_cumpleanos,
)
from .views import _wants_progressive_response


class CapturaCumpleanosForm(forms.Form):
    fecha_nacimiento = forms.DateField(
        label="Fecha de nacimiento verificada", input_formats=["%Y-%m-%d"],
        widget=forms.DateInput(attrs={"type": "date"}, format="%Y-%m-%d"),
    )
    motivo = forms.CharField(
        label="Fuente de verificación o motivo de corrección", max_length=500,
        widget=forms.Textarea(attrs={"rows": 3, "placeholder": "Ej. Confirmada con la persona o con su documento de identidad"}),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["fecha_nacimiento"].widget.attrs["max"] = timezone.localdate().isoformat()

    def clean_fecha_nacimiento(self):
        value = self.cleaned_data["fecha_nacimiento"]
        raw = self.data.get("fecha_nacimiento", "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            raise forms.ValidationError("Usa una fecha válida en formato año-mes-día.")
        if value > timezone.localdate():
            raise forms.ValidationError("La fecha de nacimiento no puede estar en el futuro.")
        return value


def _context(request, *, selected=None, form=None):
    today = timezone.localdate()
    params = request.POST if request.method == "POST" else request.GET
    month_value = params.get("mes", today.strftime("%Y-%m"))
    try:
        if not re.fullmatch(r"\d{4}-\d{2}", month_value):
            raise ValueError
        year, month = map(int, month_value.split("-"))
        # Keep navigation away from date overflow at the supported limits.
        if not 1901 <= year <= 9998:
            raise ValueError
        first = date(year, month, 1)
    except (ValueError, TypeError):
        first = today.replace(day=1)
    last = first.replace(day=calendar.monthrange(first.year, first.month)[1])
    scope = empleados_visibles(request.user).select_related("sucursal_ref")
    departments = list(scope.exclude(departamento="").values_list("departamento", flat=True).distinct().order_by("departamento"))
    branches = sorted({e.sucursal_display for e in scope if e.sucursal_display})
    department = params.get("departamento", "")
    branch = params.get("sucursal", "")
    if department:
        scope = scope.filter(departamento=department)
    # Canonical display preserves FK identity while supporting legacy employees.
    if branch:
        scope = scope.filter(pk__in=[e.pk for e in scope if e.sucursal_display == branch])
    events = eventos_entre(scope, first, last)
    filter_values = {"mes": first.strftime("%Y-%m"), "departamento": department, "sucursal": branch}
    base_url = reverse("rrhh:rrhh_cumpleanos")
    can_manage = puede_gestionar_cumpleanos(request.user)
    for event in events:
        event["capture_url"] = base_url + "?" + urlencode({**filter_values, "empleado": event["empleado"].pk}) + "#captura-cumpleanos"
    by_day = {}
    for event in events:
        by_day.setdefault(event["fecha"].day, []).append(event)
    weeks = [[{"numero": day, "eventos": by_day.get(day, []), "hoy": bool(day and date(first.year, first.month, day) == today)} for day in week] for week in calendar.Calendar(firstweekday=0).monthdayscalendar(first.year, first.month)]
    pending = []
    if vista_global_cumpleanos(request.user):
        for employee in scope.filter(fecha_nacimiento__isnull=True).order_by("nombre"):
            pending.append({"nombre": employee.nombre, "url": base_url + "?" + urlencode({**filter_values, "empleado": employee.pk}) + "#captura-cumpleanos"})
    def month_url(day):
        return base_url + "?" + urlencode({**filter_values, "mes": day.strftime("%Y-%m")})
    return {
        "global_scope": vista_global_cumpleanos(request.user), "can_manage": can_manage,
        "historial_avisos": list(AvisoCumpleanos.objects.select_related("usuario").order_by("-creado_en", "-pk")[:50]) if vista_global_cumpleanos(request.user) else [],
        "mes": first, "mes_value": first.strftime("%Y-%m"), "departamento": department,
        "sucursal": branch, "departamentos": departments, "sucursales": branches,
        "eventos": events, "semanas": weeks, "hoy": today,
        "eventos_hoy": eventos_entre(scope, today, today),
        "eventos_proximos": eventos_entre(scope, today, today + timedelta(days=6)),
        "total_hoy": len(eventos_entre(scope, today, today)),
        "total_semana": len(eventos_entre(scope, today, today + timedelta(days=6))),
        "total_mes": len(events), "pendientes": pending,
        "anterior_url": month_url(first - timedelta(days=1)),
        "siguiente_url": month_url(last + timedelta(days=1)),
        "selected": selected, "captura_form": form,
    }


@login_required
@require_GET
def calendario_cumpleanos(request):
    if not puede_ver_cumpleanos(request.user):
        raise PermissionDenied
    selected = None
    form = None
    if request.GET.get("empleado") and puede_gestionar_cumpleanos(request.user):
        if not request.GET["empleado"].isdigit():
            raise Http404
        selected = get_object_or_404(empleados_visibles(request.user), pk=request.GET["empleado"])
        form = CapturaCumpleanosForm(initial={"fecha_nacimiento": selected.fecha_nacimiento})
    return render(request, "rrhh/cumpleanos.html", _context(request, selected=selected, form=form))


@login_required
@require_POST
def guardar_cumpleanos(request):
    if not puede_gestionar_cumpleanos(request.user):
        raise PermissionDenied
    if not request.POST.get("empleado_id", "").isdigit():
        raise Http404
    form = CapturaCumpleanosForm(request.POST)
    with transaction.atomic():
        employee = get_object_or_404(empleados_visibles(request.user).select_for_update(of=("self",)), pk=request.POST.get("empleado_id"))
        if form.is_valid():
            previous = employee.fecha_nacimiento
            employee.fecha_nacimiento = form.cleaned_data["fecha_nacimiento"]
            employee.updated_at = timezone.now()
            employee.save(update_fields=["fecha_nacimiento", "updated_at"])
            AuditLog.objects.create(user=request.user, action="UPDATE", model="rrhh.Empleado", object_id=str(employee.pk), payload={"campo": "fecha_nacimiento", "anterior": previous.isoformat() if previous else None, "nuevo": employee.fecha_nacimiento.isoformat(), "motivo": form.cleaned_data["motivo"]})
        else:
            error = " ".join(message for errors in form.errors.values() for message in errors)
            if _wants_progressive_response(request):
                return JsonResponse({"ok": False, "toast": {"type": "error", "message": error, "persistent": True}}, status=400)
            messages.error(request, error)
            return render(request, "rrhh/cumpleanos.html", _context(request, selected=employee, form=form), status=400)
    params = {key: request.POST.get(key, "") for key in ("mes", "departamento", "sucursal")}
    params["empleado"] = employee.pk
    target = reverse("rrhh:rrhh_cumpleanos") + "?" + urlencode(params) + "#captura-cumpleanos"
    success = f"Fecha de nacimiento de {employee.nombre} guardada con su verificación."
    if _wants_progressive_response(request):
        return JsonResponse({"ok": True, "toast": {"type": "success", "message": success}, "redirect": target, "reload": True})
    messages.success(request, success)
    return redirect(target)
