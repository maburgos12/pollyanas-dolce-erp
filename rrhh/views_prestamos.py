from __future__ import annotations

import calendar
from datetime import date
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.http import JsonResponse
from rest_framework.exceptions import ValidationError

from django.views.decorators.cache import never_cache
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse

from core.access import can_manage_rrhh, can_view_rrhh
from core.notificaciones import (
    notificar_prestamo_aprobado,
    notificar_prestamo_para_direccion,
)

from .models import ImportacionNominaContpaq, Prestamo, PrestamoCuota
from .serializers import PrestamoSerializer
from .services_identidad import empleado_de_usuario
from .services_prestamos import (
    aplicar_cobro_manual,
    crear_solicitud_prestamo,
    can_capturar_prestamos_equipo,
    empleados_solicitables_prestamo,
    aprobar_prestamo_direccion,
    autorizar_prestamo_jefe,
    can_autorizar_prestamo_direccion,
    can_autorizar_prestamo_jefe,
    prestamos_jefe_q,
    prestamos_por_autorizar_q,
    usuario_equivale_jefe_prestamo,
)
from .views import _module_tabs


def _prestamos_por_autorizar(user):
    if not user or not user.is_authenticated:
        return Prestamo.objects.none()
    return Prestamo.objects.filter(
        prestamos_por_autorizar_q(user),
    ).select_related("empleado", "jefe_directo", "autorizado_jefe")


def _prestamos_asignados(user):
    if not user or not user.is_authenticated:
        return Prestamo.objects.none()
    return Prestamo.objects.filter(
        prestamos_jefe_q(user) | Q(empleado=empleado_de_usuario(user)) | Q(creado_por=user)
    ).select_related("empleado", "jefe_directo", "autorizado_jefe")


def _can_view_prestamos(user) -> bool:
    return bool(can_view_rrhh(user) or can_capturar_prestamos_equipo(user) or _prestamos_asignados(user).exists())


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
    return bool(can_view_rrhh(user) or _is_jefe_asignado(user, prestamo)
                or prestamo.creado_por_id == user.pk
                or prestamo.empleado_id == getattr(empleado_de_usuario(user), "pk", None))


def _can_authorize_jefe(user, prestamo: Prestamo) -> bool:
    return can_autorizar_prestamo_jefe(user, prestamo)


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
        base_qs = _prestamos_asignados(request.user)
    por_autorizar = _prestamos_por_autorizar(request.user)
    activos = base_qs.filter(estado=Prestamo.ESTADO_ACTIVO)
    pendientes = base_qs.filter(
        estado__in=[Prestamo.ESTADO_SOLICITADO, Prestamo.ESTADO_AUTORIZADO],
    ).exclude(pk__in=por_autorizar.values("pk"))
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
            "can_capture_loans": can_capturar_prestamos_equipo(request.user),
            "show_rrhh_tabs": can_view_rrhh(request.user),
            "module_tabs": _module_tabs("prestamos", request.user),
        },
    )


@login_required
def prestamo_nuevo(request):
    if not can_capturar_prestamos_equipo(request.user):
        raise PermissionDenied("No tienes colaboradores directos activos ni permisos de captura RRHH.")
    empleados = empleados_solicitables_prestamo(request.user).select_related("jefe_directo__usuario_erp").order_by("nombre")
    errores = None
    if request.method == "POST":
        payload = request.POST.dict()
        payload["fecha_deposito"] = payload.get("fecha_deposito") or None
        datos = PrestamoSerializer(data=payload, context={"request": request})
        try:
            datos.is_valid(raise_exception=True)
            empleado = datos.validated_data.get("empleado")
            if not empleado or not empleados.filter(pk=empleado.pk).exists():
                raise PermissionDenied("Empleado fuera de tu alcance de captura.")
            prestamo = crear_solicitud_prestamo(
                empleado=empleado,
                actor=request.user,
                concepto=datos.validated_data["concepto"],
                metodo_pago=datos.validated_data["metodo_pago"],
                importe=datos.validated_data["importe"],
                num_quincenas=datos.validated_data["num_quincenas"],
                fecha_deposito=datos.validated_data.get("fecha_deposito"),
                fecha_solicitud=_parse_date(request.POST.get("fecha_solicitud")),
            )
        except ValidationError as exc:
            errores = [str(mensaje) for valor in exc.detail.values()
                       for mensaje in (valor if isinstance(valor, list) else [valor])]
        else:
            destino = "Dirección" if prestamo.estado == Prestamo.ESTADO_AUTORIZADO else "su jefe directo"
            messages.success(request, f"Préstamo {prestamo.folio} creado. Pendiente de autorización de {destino}.")
            return redirect("rrhh:rrhh_prestamo_detalle", pk=prestamo.pk)
    return render(
        request, "rrhh/prestamo_nuevo.html",
        {
            "empleados": empleados,
            "metodos": Prestamo.METODO_CHOICES,
            "module_tabs": _module_tabs("prestamos", request.user),
            "show_rrhh_tabs": can_view_rrhh(request.user),
            "errores": errores,
            "valores": request.POST,
        },
        status=400 if errores else 200,
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
            "can_approve_direccion": can_autorizar_prestamo_direccion(request.user, prestamo),
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
        success_message = f"Préstamo {prestamo.folio} aprobado. Cuotas generadas automáticamente."
        if "application/json" in request.headers.get("Accept", ""):
            return JsonResponse(
                {
                    "ok": True,
                    "toast": {"type": "success", "message": success_message},
                    "redirect": f"{reverse('rrhh:rrhh_prestamos_lista')}#prestamo-{prestamo.pk}",
                    "reload": True,
                }
            )
        messages.success(request, success_message)
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
