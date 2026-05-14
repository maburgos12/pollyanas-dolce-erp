from __future__ import annotations

import calendar
from datetime import date
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from core.access import can_manage_rrhh, can_view_rrhh

from .models import Empleado, ImportacionNominaContpaq, Prestamo, PrestamoCuota
from .services_prestamos import aplicar_cobro_manual, generar_cuotas
from .views import _module_tabs


def _require_rrhh_view(user):
    if not can_view_rrhh(user):
        raise PermissionDenied("No tienes acceso a préstamos de Capital Humano.")


def _require_rrhh_manage(user):
    if not can_manage_rrhh(user):
        raise PermissionDenied("No tienes permisos para modificar préstamos de Capital Humano.")


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0")).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0.00")


def _parse_date(raw: str | None) -> date | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


@login_required
def prestamos_lista(request):
    _require_rrhh_view(request.user)
    activos = Prestamo.objects.filter(estado=Prestamo.ESTADO_ACTIVO).select_related("empleado")
    pendientes = Prestamo.objects.filter(
        estado__in=[Prestamo.ESTADO_SOLICITADO, Prestamo.ESTADO_AUTORIZADO]
    ).select_related("empleado")
    liquidados = Prestamo.objects.filter(estado=Prestamo.ESTADO_LIQUIDADO).select_related("empleado")
    return render(
        request,
        "rrhh/prestamos_lista.html",
        {
            "activos": activos,
            "pendientes": pendientes,
            "liquidados": liquidados,
            "module_tabs": _module_tabs("prestamos"),
        },
    )


@login_required
def prestamo_nuevo(request):
    _require_rrhh_manage(request.user)
    if request.method == "POST":
        importe = _parse_decimal(request.POST.get("importe"))
        try:
            quincenas = max(int(request.POST.get("num_quincenas", "1")), 1)
        except (TypeError, ValueError):
            quincenas = 1
        descuento = (importe / Decimal(str(quincenas))).quantize(Decimal("0.01"))

        prestamo = Prestamo.objects.create(
            empleado_id=request.POST.get("empleado"),
            concepto=request.POST.get("concepto", ""),
            metodo_pago=request.POST.get("metodo_pago", Prestamo.METODO_TRANSFERENCIA),
            fecha_solicitud=_parse_date(request.POST.get("fecha_solicitud")) or date.today(),
            fecha_deposito=_parse_date(request.POST.get("fecha_deposito")),
            importe=importe,
            num_quincenas=quincenas,
            descuento_quincenal=descuento,
            saldo_actual=importe,
            estado=Prestamo.ESTADO_SOLICITADO,
            creado_por=request.user,
        )
        messages.success(request, f"Préstamo {prestamo.folio} creado. Pendiente de autorización.")
        return redirect("rrhh:rrhh_prestamos_lista")

    empleados = Empleado.objects.filter(activo=True).order_by("nombre")
    return render(
        request,
        "rrhh/prestamo_nuevo.html",
        {
            "empleados": empleados,
            "metodos": Prestamo.METODO_CHOICES,
            "module_tabs": _module_tabs("prestamos"),
        },
    )


@login_required
def prestamo_detalle(request, pk):
    _require_rrhh_view(request.user)
    prestamo = get_object_or_404(Prestamo.objects.select_related("empleado"), pk=pk)
    cuotas = prestamo.cuotas.all()
    progreso = 0
    if prestamo.importe:
        progreso = int(((prestamo.importe - prestamo.saldo_actual) / prestamo.importe) * 100)
    return render(
        request,
        "rrhh/prestamo_detalle.html",
        {
            "prestamo": prestamo,
            "cuotas": cuotas,
            "progreso": max(0, min(progreso, 100)),
            "module_tabs": _module_tabs("prestamos"),
        },
    )


@login_required
def prestamo_autorizar_jefe(request, pk):
    _require_rrhh_manage(request.user)
    prestamo = get_object_or_404(Prestamo, pk=pk)
    if request.method == "POST":
        prestamo.firma_jefe = True
        prestamo.autorizado_jefe = request.user
        prestamo.fecha_auth_jefe = timezone.now()
        prestamo.estado = Prestamo.ESTADO_AUTORIZADO
        prestamo.save(update_fields=["firma_jefe", "autorizado_jefe", "fecha_auth_jefe", "estado", "actualizado_en"])
        messages.success(request, f"Préstamo {prestamo.folio} autorizado por jefe.")
    return redirect("rrhh:rrhh_prestamo_detalle", pk=pk)


@login_required
def prestamo_autorizar_dg(request, pk):
    _require_rrhh_manage(request.user)
    prestamo = get_object_or_404(Prestamo, pk=pk)
    if request.method == "POST" and prestamo.estado == Prestamo.ESTADO_AUTORIZADO:
        prestamo.firma_direccion = True
        prestamo.autorizado_dg = request.user
        prestamo.fecha_auth_dg = timezone.now()
        prestamo.estado = Prestamo.ESTADO_ACTIVO
        prestamo.save(
            update_fields=["firma_direccion", "autorizado_dg", "fecha_auth_dg", "estado", "actualizado_en"]
        )
        generar_cuotas(prestamo)
        messages.success(request, f"Préstamo {prestamo.folio} aprobado. Cuotas generadas automáticamente.")
    return redirect("rrhh:rrhh_prestamo_detalle", pk=pk)


@login_required
def prestamo_cobro_manual(request, cuota_pk):
    _require_rrhh_manage(request.user)
    cuota = get_object_or_404(PrestamoCuota.objects.select_related("prestamo"), pk=cuota_pk)
    if request.method == "POST":
        monto = _parse_decimal(request.POST.get("monto_cobrado"))
        nota = request.POST.get("nota", "")
        aplicar_cobro_manual(cuota, monto, request.user, nota)
        messages.success(request, f"Cobro de ${monto} registrado. Saldo restante: ${cuota.prestamo.saldo_actual}")
    return redirect("rrhh:rrhh_prestamo_detalle", pk=cuota.prestamo.pk)


@login_required
def importar_contpaq(request):
    _require_rrhh_manage(request.user)
    if request.method == "POST":
        archivo = request.FILES.get("archivo")
        periodo_ini = _parse_date(request.POST.get("periodo_inicio"))
        periodo_fin = _parse_date(request.POST.get("periodo_fin"))
        try:
            quincena_num = int(request.POST.get("quincena_num", "1"))
        except (TypeError, ValueError):
            quincena_num = 1

        if not archivo or not periodo_ini or not periodo_fin:
            messages.error(request, "Selecciona un archivo XLS y captura el periodo completo.")
        else:
            from .importers_contpaq import importar_lista_raya_contpaq

            resultado = importar_lista_raya_contpaq(archivo, request.user, periodo_ini, periodo_fin, quincena_num)
            messages.success(
                request,
                f"Importación completa: {resultado['aplicados']} cobros aplicados, "
                f"{resultado['sin_match']} sin match, {resultado['diferencias']} diferencias detectadas.",
            )
        return redirect("rrhh:rrhh_importar_contpaq")

    historial = ImportacionNominaContpaq.objects.order_by("-creado_en")[:10]
    return render(
        request,
        "rrhh/importar_contpaq.html",
        {"historial": historial, "module_tabs": _module_tabs("prestamos")},
    )


@login_required
def quincena_cobros(request):
    _require_rrhh_view(request.user)
    hoy = date.today()
    if hoy.day <= 15:
        desde = date(hoy.year, hoy.month, 1)
        hasta = date(hoy.year, hoy.month, 15)
    else:
        ultimo = calendar.monthrange(hoy.year, hoy.month)[1]
        desde = date(hoy.year, hoy.month, 16)
        hasta = date(hoy.year, hoy.month, ultimo)

    cuotas_pendientes = (
        PrestamoCuota.objects.filter(
            estado__in=[PrestamoCuota.ESTADO_PENDIENTE, PrestamoCuota.ESTADO_PARCIAL],
            fecha_quincena__range=[desde, hasta],
        )
        .select_related("prestamo__empleado")
        .order_by("prestamo__empleado__nombre")
    )

    return render(
        request,
        "rrhh/quincena_cobros.html",
        {
            "cuotas": cuotas_pendientes,
            "desde": desde,
            "hasta": hasta,
            "module_tabs": _module_tabs("prestamos"),
        },
    )
