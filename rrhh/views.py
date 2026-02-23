from __future__ import annotations

from datetime import date as dt_date
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.db.models import Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from core.access import can_manage_rrhh, can_view_rrhh
from core.audit import log_event

from .models import Empleado, NominaLinea, NominaPeriodo


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _parse_date(raw: str | None):
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return dt_date.fromisoformat(value)
    except ValueError:
        return None


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Empleados", "url_name": "rrhh:empleados", "active": active == "empleados"},
        {"label": "Nómina", "url_name": "rrhh:nomina", "active": active == "nomina"},
    ]


@login_required
def empleados(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver RRHH")

    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para gestionar RRHH")

        nombre = (request.POST.get("nombre") or "").strip()
        if not nombre:
            messages.error(request, "Nombre del empleado es obligatorio.")
        else:
            empleado = Empleado.objects.create(
                nombre=nombre,
                area=(request.POST.get("area") or "").strip(),
                puesto=(request.POST.get("puesto") or "").strip(),
                tipo_contrato=(request.POST.get("tipo_contrato") or Empleado.CONTRATO_FIJO).strip(),
                fecha_ingreso=request.POST.get("fecha_ingreso") or timezone.localdate(),
                salario_diario=_parse_decimal(request.POST.get("salario_diario")),
                telefono=(request.POST.get("telefono") or "").strip(),
                email=(request.POST.get("email") or "").strip(),
                sucursal=(request.POST.get("sucursal") or "").strip(),
            )
            log_event(
                request.user,
                "CREATE",
                "rrhh.Empleado",
                str(empleado.id),
                {
                    "codigo": empleado.codigo,
                    "nombre": empleado.nombre,
                    "salario_diario": str(empleado.salario_diario),
                },
            )
            messages.success(request, f"Empleado {empleado.nombre} registrado.")
            return redirect("rrhh:empleados")

    q = (request.GET.get("q") or "").strip()
    estado = (request.GET.get("estado") or "activos").strip().lower()

    qs = Empleado.objects.all().annotate(total_lineas_nomina=Count("lineas_nomina"))
    if q:
        qs = qs.filter(
            Q(nombre__icontains=q)
            | Q(codigo__icontains=q)
            | Q(area__icontains=q)
            | Q(puesto__icontains=q)
        )
    if estado == "activos":
        qs = qs.filter(activo=True)
    elif estado == "inactivos":
        qs = qs.filter(activo=False)

    context = {
        "module_tabs": _module_tabs("empleados"),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "empleados": qs.order_by("nombre")[:600],
        "q": q,
        "estado": estado,
        "total_empleados": Empleado.objects.count(),
        "total_activos": Empleado.objects.filter(activo=True).count(),
        "total_nominas": NominaPeriodo.objects.count(),
        "contrato_choices": Empleado.CONTRATO_CHOICES,
    }
    return render(request, "rrhh/empleados.html", context)


@login_required
def nomina(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver RRHH")

    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para gestionar RRHH")

        fecha_inicio = request.POST.get("fecha_inicio")
        fecha_fin = request.POST.get("fecha_fin")
        fecha_inicio_obj = _parse_date(fecha_inicio)
        fecha_fin_obj = _parse_date(fecha_fin)
        if not fecha_inicio_obj or not fecha_fin_obj:
            messages.error(request, "Fecha inicio y fin son obligatorias.")
        elif fecha_fin_obj < fecha_inicio_obj:
            messages.error(request, "La fecha fin no puede ser menor a fecha inicio.")
        else:
            nomina = NominaPeriodo.objects.create(
                tipo_periodo=(request.POST.get("tipo_periodo") or NominaPeriodo.TIPO_QUINCENAL).strip(),
                fecha_inicio=fecha_inicio_obj,
                fecha_fin=fecha_fin_obj,
                estatus=(request.POST.get("estatus") or NominaPeriodo.ESTATUS_BORRADOR).strip(),
                notas=(request.POST.get("notas") or "").strip(),
                created_by=request.user,
            )
            log_event(
                request.user,
                "CREATE",
                "rrhh.NominaPeriodo",
                str(nomina.id),
                {
                    "folio": nomina.folio,
                    "tipo_periodo": nomina.tipo_periodo,
                    "fecha_inicio": str(nomina.fecha_inicio),
                    "fecha_fin": str(nomina.fecha_fin),
                },
            )
            messages.success(request, f"Nómina {nomina.folio} creada.")
            return redirect("rrhh:nomina_detail", pk=nomina.id)

    estatus = (request.GET.get("estatus") or "").strip().upper()
    tipo = (request.GET.get("tipo") or "").strip().upper()

    nominas_qs = NominaPeriodo.objects.all()
    if estatus:
        nominas_qs = nominas_qs.filter(estatus=estatus)
    if tipo:
        nominas_qs = nominas_qs.filter(tipo_periodo=tipo)

    context = {
        "module_tabs": _module_tabs("nomina"),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "nominas": nominas_qs.order_by("-fecha_fin", "-id")[:120],
        "estatus": estatus,
        "tipo": tipo,
        "tipo_choices": NominaPeriodo.TIPO_CHOICES,
        "estatus_choices": NominaPeriodo.ESTATUS_CHOICES,
        "total_nominas": NominaPeriodo.objects.count(),
        "nominas_borrador": NominaPeriodo.objects.filter(estatus=NominaPeriodo.ESTATUS_BORRADOR).count(),
    }
    return render(request, "rrhh/nomina.html", context)


@login_required
def nomina_detail(request, pk: int):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver RRHH")

    periodo = get_object_or_404(NominaPeriodo, pk=pk)

    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para gestionar RRHH")

        action = (request.POST.get("action") or "add_line").strip()
        if action == "add_line":
            empleado_id = (request.POST.get("empleado_id") or "").strip()
            if not empleado_id.isdigit():
                messages.error(request, "Selecciona un empleado válido.")
                return redirect("rrhh:nomina_detail", pk=periodo.id)
            empleado = get_object_or_404(Empleado, pk=int(empleado_id), activo=True)
            with transaction.atomic():
                linea, _ = NominaLinea.objects.get_or_create(periodo=periodo, empleado=empleado)
                linea.dias_trabajados = _parse_decimal(request.POST.get("dias_trabajados"))
                linea.salario_base = _parse_decimal(request.POST.get("salario_base"))
                linea.bonos = _parse_decimal(request.POST.get("bonos"))
                linea.descuentos = _parse_decimal(request.POST.get("descuentos"))
                linea.observaciones = (request.POST.get("observaciones") or "").strip()
                linea.save()
                periodo.recompute_totals()
                periodo.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])
            log_event(
                request.user,
                "UPDATE",
                "rrhh.NominaLinea",
                str(linea.id),
                {
                    "periodo": periodo.folio,
                    "empleado": empleado.nombre,
                    "neto": str(linea.neto_calculado),
                },
            )
            messages.success(request, f"Línea de nómina guardada para {empleado.nombre}.")
            return redirect("rrhh:nomina_detail", pk=periodo.id)

        if action == "delete_line":
            line_id = (request.POST.get("line_id") or "").strip()
            if line_id.isdigit():
                line = NominaLinea.objects.filter(pk=int(line_id), periodo=periodo).first()
                if line:
                    line.delete()
                    periodo.recompute_totals()
                    periodo.save(update_fields=["total_bruto", "total_descuentos", "total_neto", "updated_at"])
                    messages.success(request, "Línea eliminada.")
            return redirect("rrhh:nomina_detail", pk=periodo.id)

    context = {
        "module_tabs": _module_tabs("nomina"),
        "can_manage_rrhh": can_manage_rrhh(request.user),
        "periodo": periodo,
        "lineas": periodo.lineas.select_related("empleado").order_by("empleado__nombre", "id"),
        "empleados": Empleado.objects.filter(activo=True).order_by("nombre")[:1000],
        "estatus_choices": NominaPeriodo.ESTATUS_CHOICES,
    }
    return render(request, "rrhh/nomina_detail.html", context)


@login_required
def nomina_status(request, pk: int, estatus: str):
    if request.method != "POST":
        return redirect("rrhh:nomina")
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para gestionar RRHH")

    estatus = (estatus or "").strip().upper()
    if estatus not in {choice[0] for choice in NominaPeriodo.ESTATUS_CHOICES}:
        messages.error(request, "Estatus inválido.")
        return redirect("rrhh:nomina")

    periodo = get_object_or_404(NominaPeriodo, pk=pk)
    from_status = periodo.estatus
    if from_status == estatus:
        return redirect("rrhh:nomina_detail", pk=periodo.id)

    periodo.estatus = estatus
    periodo.save(update_fields=["estatus", "updated_at"])
    log_event(
        request.user,
        "UPDATE",
        "rrhh.NominaPeriodo",
        str(periodo.id),
        {"folio": periodo.folio, "from": from_status, "to": estatus},
    )
    messages.success(request, f"Nómina {periodo.folio} actualizada a {estatus}.")
    return redirect("rrhh:nomina_detail", pk=periodo.id)
