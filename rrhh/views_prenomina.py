from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime

from core.access import can_manage_rrhh, can_view_rrhh
from rrhh.exporters.contpaqi_prenomina import export_movimientos_contpaqi_xlsx, export_revision_xlsx
from rrhh.models import AjusteAsistencia, PrenominaCorte, PrenominaEmpleadoResumen, PrenominaMovimiento
from rrhh.services_ajustes_asistencia import (
    TIPOS_A_CAMPOS,
    aprobar_ajuste_asistencia,
    crear_ajuste_asistencia,
    rechazar_ajuste_asistencia,
)
from rrhh.services_prenomina import crear_corte_prenomina, recalcular_corte_prenomina
from rrhh.views import _module_tabs

TIPOS_AJUSTE_PRENOMINA = [
    (AjusteAsistencia.TIPO_ENTRADA, "Entrada"),
    (AjusteAsistencia.TIPO_SALIDA, "Salida"),
    (AjusteAsistencia.TIPO_SALIDA_COMIDA, "Salida comida"),
    (AjusteAsistencia.TIPO_REGRESO_COMIDA, "Regreso comida"),
]


def _parse_fecha(value, default=None):
    return parse_date((value or "").strip()) or default


def _parse_datetime_local(value):
    parsed = parse_datetime((value or "").strip())
    if parsed and timezone.is_aware(parsed):
        parsed = timezone.localtime(parsed)
    return parsed


def _validation_message(exc: ValidationError) -> str:
    return "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc)


def _recalcular_corte_con_mensaje(request, corte: PrenominaCorte) -> PrenominaCorte:
    try:
        return recalcular_corte_prenomina(corte)
    except ValidationError as exc:
        messages.warning(request, f"El ajuste se guardó, pero el corte no se pudo recalcular: {_validation_message(exc)}")
        return corte


def _contador_resumen(resumen: dict, key: str) -> int:
    try:
        return int(resumen.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _blocked_export_reasons(corte: PrenominaCorte) -> list[str]:
    resumen = corte.resumen or {}
    checks = [
        ("bloqueados", "resúmenes bloqueados"),
        ("ajustes_pendientes", "ajustes pendientes"),
        ("movimientos_pendientes_configuracion", "movimientos sin clave CONTPAQi"),
        ("movimientos_bloqueados", "movimientos bloqueados"),
        ("movimientos_exportados", "movimientos ya exportados"),
    ]
    reasons = [label for key, label in checks if _contador_resumen(resumen, key) > 0]
    if corte.movimientos.exclude(estado=PrenominaMovimiento.ESTADO_LISTO).exists():
        reasons.append("movimientos fuera de estado listo")
    return reasons


@login_required
def prenomina(request):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenómina.")

    hoy = timezone.localdate()
    if request.method == "POST":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para generar prenómina.")
        inicio = _parse_fecha(request.POST.get("fecha_inicio"))
        fin = _parse_fecha(request.POST.get("fecha_fin"))
        fecha_corte = _parse_fecha(request.POST.get("fecha_corte"), hoy)
        if not inicio or not fin or fin < inicio:
            messages.error(request, "Captura un rango de prenómina válido.")
            return redirect("rrhh:prenomina")
        corte = crear_corte_prenomina(
            fecha_inicio=inicio,
            fecha_fin=fin,
            fecha_corte=fecha_corte,
            creado_por=request.user,
            tipo_periodo=(request.POST.get("tipo_periodo") or PrenominaCorte.TIPO_QUINCENAL).strip(),
            sucursal=(request.POST.get("sucursal") or "").strip(),
            area=(request.POST.get("area") or "").strip(),
            notas=(request.POST.get("notas") or "").strip(),
        )
        messages.success(request, f"Corte {corte.folio} generado.")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)

    return render(
        request,
        "rrhh/prenomina.html",
        {
            "module_tabs": _module_tabs("prenomina", request.user),
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "hoy": hoy.isoformat(),
            "cortes": PrenominaCorte.objects.select_related("creado_por").order_by("-fecha_fin", "-id")[:80],
            "tipo_choices": PrenominaCorte.TIPO_CHOICES,
        },
    )


@login_required
def prenomina_detail(request, pk):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenómina.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)

    if request.method == "POST" and request.POST.get("action") == "recalcular":
        if not can_manage_rrhh(request.user):
            raise PermissionDenied("No tienes permisos para recalcular prenómina.")
        try:
            corte = recalcular_corte_prenomina(corte)
        except ValidationError as exc:
            messages.error(request, "; ".join(exc.messages))
            return redirect("rrhh:prenomina_detail", pk=corte.pk)
        messages.success(request, f"Corte {corte.folio} recalculado.")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)

    return render(
        request,
        "rrhh/prenomina_detail.html",
        {
            "module_tabs": _module_tabs("prenomina", request.user),
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "corte": corte,
            "resumenes": corte.resumenes.select_related("empleado").order_by("empleado__nombre", "empleado__codigo"),
            "movimientos": corte.movimientos.select_related("empleado").order_by("empleado__nombre", "fecha", "id")[:400],
            "export_blockers": _blocked_export_reasons(corte),
        },
    )


@login_required
def prenomina_persona(request, pk, empleado_id):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para ver prenómina.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    resumen = get_object_or_404(
        PrenominaEmpleadoResumen.objects.select_related("empleado"),
        corte=corte,
        empleado_id=empleado_id,
    )
    return render(
        request,
        "rrhh/prenomina_persona.html",
        {
            "module_tabs": _module_tabs("prenomina", request.user),
            "corte": corte,
            "resumen": resumen,
            "can_manage_rrhh": can_manage_rrhh(request.user),
            "tipo_ajuste_choices": TIPOS_AJUSTE_PRENOMINA,
            "ajustes": AjusteAsistencia.objects.filter(
                empleado_id=empleado_id,
                fecha__gte=corte.fecha_inicio,
                fecha__lte=corte.fecha_fin,
            ).select_related("solicitado_por", "autorizado_por").order_by("-fecha", "-id"),
            "movimientos": corte.movimientos.filter(empleado_id=empleado_id).order_by("fecha", "tipo_movimiento_erp", "id"),
        },
    )


@login_required
def prenomina_ajuste_crear(request, pk, empleado_id):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para crear ajustes de asistencia.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    resumen = get_object_or_404(
        PrenominaEmpleadoResumen.objects.select_related("empleado"),
        corte=corte,
        empleado_id=empleado_id,
    )
    redirect_url = redirect("rrhh:prenomina_persona", pk=corte.pk, empleado_id=resumen.empleado_id)
    if request.method != "POST":
        return redirect_url

    fecha = _parse_fecha(request.POST.get("fecha"))
    tipo_ajuste = (request.POST.get("tipo_ajuste") or "").strip()
    valor_propuesto = (request.POST.get("valor_propuesto") or "").strip()
    motivo = (request.POST.get("motivo") or "").strip()

    if not fecha or fecha < corte.fecha_inicio or fecha > corte.fecha_fin:
        messages.error(request, "Captura una fecha dentro del periodo del corte.")
        return redirect_url
    if tipo_ajuste not in TIPOS_A_CAMPOS:
        messages.error(request, "Selecciona un tipo de ajuste válido.")
        return redirect_url
    if not valor_propuesto:
        messages.error(request, "Captura el valor propuesto del ajuste.")
        return redirect_url
    valor_datetime = _parse_datetime_local(valor_propuesto)
    if not valor_datetime or valor_datetime.date() != fecha:
        messages.error(request, "El valor propuesto debe corresponder a la misma fecha del ajuste.")
        return redirect_url

    try:
        ajuste = crear_ajuste_asistencia(
            resumen.empleado,
            fecha,
            tipo_ajuste,
            {TIPOS_A_CAMPOS[tipo_ajuste]: valor_propuesto},
            motivo,
            request.user,
        )
    except ValidationError as exc:
        messages.error(request, _validation_message(exc))
        return redirect_url

    _recalcular_corte_con_mensaje(request, corte)
    messages.success(request, f"Ajuste de asistencia solicitado para {ajuste.fecha:%Y-%m-%d}.")
    return redirect_url


@login_required
def prenomina_ajuste_aprobar(request, pk, ajuste_id):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para aprobar ajustes de asistencia.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    ajuste = get_object_or_404(
        AjusteAsistencia.objects.select_related("empleado"),
        pk=ajuste_id,
        fecha__gte=corte.fecha_inicio,
        fecha__lte=corte.fecha_fin,
    )
    redirect_url = redirect("rrhh:prenomina_persona", pk=corte.pk, empleado_id=ajuste.empleado_id)
    if request.method != "POST":
        return redirect_url

    try:
        aprobar_ajuste_asistencia(ajuste, request.user, comentario=request.POST.get("comentario", ""))
    except ValidationError as exc:
        messages.error(request, _validation_message(exc))
        return redirect_url

    _recalcular_corte_con_mensaje(request, corte)
    messages.success(request, "Ajuste de asistencia aprobado y aplicado.")
    return redirect_url


@login_required
def prenomina_ajuste_rechazar(request, pk, ajuste_id):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para rechazar ajustes de asistencia.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    ajuste = get_object_or_404(
        AjusteAsistencia.objects.select_related("empleado"),
        pk=ajuste_id,
        fecha__gte=corte.fecha_inicio,
        fecha__lte=corte.fecha_fin,
    )
    redirect_url = redirect("rrhh:prenomina_persona", pk=corte.pk, empleado_id=ajuste.empleado_id)
    if request.method != "POST":
        return redirect_url

    try:
        rechazar_ajuste_asistencia(ajuste, request.user, comentario=request.POST.get("comentario", ""))
    except ValidationError as exc:
        messages.error(request, _validation_message(exc))
        return redirect_url

    _recalcular_corte_con_mensaje(request, corte)
    messages.success(request, "Ajuste de asistencia rechazado.")
    return redirect_url


@login_required
def prenomina_export_revision(request, pk):
    if not can_view_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para exportar prenómina.")
    return export_revision_xlsx(get_object_or_404(PrenominaCorte, pk=pk))


@login_required
def prenomina_export_contpaqi(request, pk):
    if not can_manage_rrhh(request.user):
        raise PermissionDenied("No tienes permisos para exportar movimientos CONTPAQi.")
    corte = get_object_or_404(PrenominaCorte, pk=pk)
    reasons = _blocked_export_reasons(corte)
    if reasons:
        messages.error(request, "No se puede exportar CONTPAQi: " + ", ".join(reasons) + ".")
        return redirect("rrhh:prenomina_detail", pk=corte.pk)
    return export_movimientos_contpaqi_xlsx(corte)
