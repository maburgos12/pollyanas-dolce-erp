from __future__ import annotations

import calendar
from datetime import date
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.views.decorators.cache import never_cache
from django.shortcuts import get_object_or_404, redirect, render

from core.access import can_manage_rrhh, can_view_rrhh
from core.notificaciones import (
    notificar_prestamo_aprobado,
    notificar_prestamo_para_direccion,
    notificar_prestamo_solicitado,
)

from .models import Empleado, ImportacionNominaContpaq, Prestamo, PrestamoCuota
from .services_prestamos import (
    aplicar_cobro_manual,
    aprobar_prestamo_direccion,
    autorizar_prestamo_jefe,
    can_autorizar_prestamo_direccion,
    can_autorizar_prestamo_jefe,
    prestamos_jefe_q,
    usuario_equivale_jefe_prestamo,
)
from .views import _module_tabs


ESTADOS_DEUDA_VIGENTE = [
    Prestamo.ESTADO_SOLICITADO,
    Prestamo.ESTADO_AUTORIZADO,
    Prestamo.ESTADO_APROBADO,
    Prestamo.ESTADO_ACTIVO,
]


def _prestamos_por_autorizar(user):
    if not user or not user.is_authenticated:
        return Prestamo.objects.none()
    return Prestamo.objects.filter(
        prestamos_jefe_q(user),
        estado=Prestamo.ESTADO_SOLICITADO,
    ).select_related("empleado", "jefe_directo", "autorizado_jefe")


def _prestamos_asignados(user):
    if not user or not user.is_authenticated:
        return Prestamo.objects.none()
    return Prestamo.objects.filter(prestamos_jefe_q(user)).select_related("empleado", "jefe_directo", "autorizado_jefe")


def _can_view_prestamos(user) -> bool:
    return bool(can_view_rrhh(user) or _prestamos_asignados(user).exists())


def _require_rrhh_view(user):
    if not _can_view_prestamos(user):
        raise PermissionDenied("No tienes acceso a préstamos de Capital Humano.")


def _require_rrhh_manage(user):
    if not can_manage_rrhh(user):
        raise PermissionDenied("No tienes permisos para modificar préstamos de Capital Humano.")


def _can_approve_direccion(user) -> bool:
    return can_autorizar_prestamo_direccion(user)


def _require_direccion(user):
    if not _can_approve_direccion(user):
        raise PermissionDenied("Solo Dirección puede aprobar préstamos y generar cuotas.")


def _is_jefe_asignado(user, prestamo: Prestamo) -> bool:
    return usuario_equivale_jefe_prestamo(user, prestamo)


def _can_view_prestamo(user, prestamo: Prestamo) -> bool:
    return bool(can_view_rrhh(user) or _is_jefe_asignado(user, prestamo))


def _can_authorize_jefe(user, prestamo: Prestamo) -> bool:
    return can_autorizar_prestamo_jefe(user, prestamo)


def _deuda_vigente(empleado_id: str | int | None):
    if not empleado_id:
        return Prestamo.objects.none()
    return Prestamo.objects.filter(
        empleado_id=empleado_id,
        estado__in=ESTADOS_DEUDA_VIGENTE,
        saldo_actual__gt=0,
    ).order_by("-fecha_solicitud", "-id")


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
    if can_view_rrhh(request.user):
        base_qs = Prestamo.objects.select_related("empleado", "jefe_directo", "autorizado_jefe")
    else:
        base_qs = Prestamo.objects.filter(prestamos_jefe_q(request.user)).select_related("empleado", "jefe_directo", "autorizado_jefe")
    por_autorizar = _prestamos_por_autorizar(request.user)
    activos = base_qs.filter(estado=Prestamo.ESTADO_ACTIVO)
    pendientes = base_qs.filter(estado__in=[Prestamo.ESTADO_SOLICITADO, Prestamo.ESTADO_AUTORIZADO])
    liquidados = base_qs.filter(estado=Prestamo.ESTADO_LIQUIDADO)
    return render(
        request,
        "rrhh/prestamos_lista.html",
        {
            "activos": activos,
            "pendientes": pendientes,
            "liquidados": liquidados,
            "por_autorizar": por_autorizar,
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "show_rrhh_tabs": can_view_rrhh(request.user),
            "module_tabs": _module_tabs("prestamos", request.user),
        },
    )


@login_required
def prestamo_nuevo(request):
    _require_rrhh_manage(request.user)
    if request.method == "POST":
        empleado_id = request.POST.get("empleado")
        deuda = _deuda_vigente(empleado_id).first()
        if deuda:
            messages.error(
                request,
                f"{deuda.empleado.nombre} no puede solicitar un nuevo préstamo porque aún cuenta con un monto "
                f"de crédito no cubierto: {deuda.folio} · saldo ${deuda.saldo_actual}.",
            )
            return redirect("rrhh:rrhh_prestamo_nuevo")

        importe = _parse_decimal(request.POST.get("importe"))
        try:
            quincenas = max(int(request.POST.get("num_quincenas", "1")), 1)
        except (TypeError, ValueError):
            quincenas = 1
        descuento = (importe / Decimal(str(quincenas))).quantize(Decimal("0.01"))

        prestamo = Prestamo.objects.create(
            empleado_id=empleado_id,
            concepto=request.POST.get("concepto", ""),
            metodo_pago=request.POST.get("metodo_pago", Prestamo.METODO_TRANSFERENCIA),
            fecha_solicitud=_parse_date(request.POST.get("fecha_solicitud")) or date.today(),
            fecha_deposito=_parse_date(request.POST.get("fecha_deposito")),
            importe=importe,
            num_quincenas=quincenas,
            descuento_quincenal=descuento,
            saldo_actual=importe,
            estado=Prestamo.ESTADO_SOLICITADO,
            jefe_directo_id=request.POST.get("jefe_directo") or None,
            creado_por=request.user,
        )
        notificar_prestamo_solicitado(prestamo, actor=request.user)
        destino = f" Pendiente de autorización por {prestamo.jefe_directo.get_full_name() or prestamo.jefe_directo.username}." if prestamo.jefe_directo else " Pendiente de asignar/autorización de jefe."
        messages.success(request, f"Préstamo {prestamo.folio} creado.{destino}")
        return redirect("rrhh:rrhh_prestamo_detalle", pk=prestamo.pk)

    empleados = Empleado.objects.filter(activo=True).order_by("nombre")
    User = get_user_model()
    autorizadores = User.objects.filter(is_active=True).order_by("first_name", "last_name", "username")
    return render(
        request,
        "rrhh/prestamo_nuevo.html",
        {
            "empleados": empleados,
            "autorizadores": autorizadores,
            "metodos": Prestamo.METODO_CHOICES,
            "module_tabs": _module_tabs("prestamos", request.user),
        },
    )


@login_required
@never_cache
def prestamo_detalle(request, pk):
    _require_rrhh_view(request.user)
    prestamo = get_object_or_404(
        Prestamo.objects.select_related("empleado", "jefe_directo", "autorizado_jefe", "autorizado_dg"),
        pk=pk,
    )
    if not _can_view_prestamo(request.user, prestamo):
        raise PermissionDenied("No tienes acceso a este préstamo.")
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
            "module_tabs": _module_tabs("prestamos", request.user),
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "show_rrhh_tabs": can_view_rrhh(request.user),
            "can_authorize_jefe": _can_authorize_jefe(request.user, prestamo),
            "can_approve_direccion": _can_approve_direccion(request.user),
        },
    )


@login_required
def prestamo_autorizar_jefe(request, pk):
    prestamo = get_object_or_404(Prestamo.objects.select_related("empleado", "jefe_directo"), pk=pk)
    if not _can_authorize_jefe(request.user, prestamo):
        raise PermissionDenied("Solo el jefe directo asignado puede autorizar este préstamo.")
    if request.method == "POST":
        autorizar_prestamo_jefe(prestamo, request.user)
        notificar_prestamo_para_direccion(prestamo, actor=request.user)
        messages.success(request, f"Préstamo {prestamo.folio} autorizado por jefe.")
    return redirect("rrhh:rrhh_prestamo_detalle", pk=pk)


@login_required
def prestamo_imprimir(request, pk):
    _require_rrhh_view(request.user)
    prestamo = get_object_or_404(
        Prestamo.objects.select_related("empleado", "jefe_directo", "autorizado_jefe", "autorizado_dg", "creado_por"),
        pk=pk,
    )
    if not _can_view_prestamo(request.user, prestamo):
        raise PermissionDenied("No tienes acceso a este préstamo.")
    return render(
        request,
        "rrhh/prestamo_imprimir.html",
        {"prestamo": prestamo, "cuotas": prestamo.cuotas.all()},
    )


@login_required
def prestamo_autorizar_dg(request, pk):
    _require_direccion(request.user)
    prestamo = get_object_or_404(Prestamo.objects.select_related("empleado"), pk=pk)
    if request.method == "POST" and prestamo.estado == Prestamo.ESTADO_AUTORIZADO:
        aprobar_prestamo_direccion(prestamo, request.user)
        notificar_prestamo_aprobado(prestamo, actor=request.user)
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
        {"historial": historial, "module_tabs": _module_tabs("prestamos", request.user)},
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
            "module_tabs": _module_tabs("prestamos", request.user),
        },
    )
