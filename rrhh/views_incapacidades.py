from __future__ import annotations

from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_date
from django.views.decorators.http import require_POST

from core.access import can_manage_submodule, can_view_submodule

from .models import Empleado, IncapacidadCambio, IncapacidadEmpleado, SolicitudVacaciones
from .services_asistencia_reglas import evaluar_rango_asistencia
from .views import _module_tabs


VIGENCIA_CHOICES = [
    ("programada", "Programada"),
    ("vigente", "Vigente"),
    ("finalizada", "Finalizada"),
    ("cancelada", "Cancelada"),
]
VIGENCIA_LABELS = dict(VIGENCIA_CHOICES)
VIGENCIA_BADGE_CLASSES = {
    "programada": "bg-warning",
    "vigente": "bg-success",
    "finalizada": "",
    "cancelada": "bg-danger",
}


def _vigencia_incapacidad(incapacidad: IncapacidadEmpleado, hoy):
    if incapacidad.estado == IncapacidadEmpleado.ESTADO_CANCELADA:
        return "cancelada"
    if incapacidad.fecha_inicio > hoy:
        return "programada"
    if incapacidad.fecha_fin < hoy:
        return "finalizada"
    return "vigente"


def _filtrar_vigencia(queryset, vigencia: str, hoy):
    if vigencia == "cancelada":
        return queryset.filter(estado=IncapacidadEmpleado.ESTADO_CANCELADA)
    if not vigencia:
        return queryset

    queryset = queryset.exclude(estado=IncapacidadEmpleado.ESTADO_CANCELADA)
    if vigencia == "programada":
        return queryset.filter(fecha_inicio__gt=hoy)
    if vigencia == "vigente":
        return queryset.filter(fecha_inicio__lte=hoy, fecha_fin__gte=hoy)
    if vigencia == "finalizada":
        return queryset.filter(fecha_fin__lt=hoy)
    return queryset


CAMPOS_EDITABLES = [
    ("empleado", "Empleado"),
    ("fecha_inicio", "Fecha inicio"),
    ("fecha_fin", "Fecha fin"),
    ("tipo", "Tipo"),
    ("folio", "Folio"),
    ("estado", "Estado administrativo"),
    ("notas", "Notas"),
]


def _valor_legible(incapacidad: IncapacidadEmpleado, campo: str) -> str:
    if campo == "empleado":
        return str(incapacidad.empleado) if incapacidad.empleado_id else ""
    if campo in {"fecha_inicio", "fecha_fin"}:
        valor = getattr(incapacidad, campo)
        return valor.isoformat() if valor else ""
    if campo in {"tipo", "estado"}:
        return getattr(incapacidad, f"get_{campo}_display")()
    return getattr(incapacidad, campo) or ""


def _diff_incapacidad(antes: dict[str, str], incapacidad: IncapacidadEmpleado) -> list[dict[str, str]]:
    cambios = []
    for campo, etiqueta in CAMPOS_EDITABLES:
        despues = _valor_legible(incapacidad, campo)
        if antes[campo] != despues:
            cambios.append({"campo": etiqueta, "antes": antes[campo], "despues": despues})
    return cambios


def _dias_cubiertos(empleado_id, fecha_inicio, fecha_fin, estado) -> set:
    """Días que la incapacidad marca como cubiertos, junto al empleado al que aplican.

    Una incapacidad cancelada no cubre nada. El tope de MAX_DIAS acota los dos
    registros heredados con el año mal tecleado, que abarcan cientos de miles de días.
    """
    if not (empleado_id and fecha_inicio and fecha_fin):
        return set()
    if estado == IncapacidadEmpleado.ESTADO_CANCELADA:
        return set()
    inicio = max(fecha_inicio, fecha_fin - timedelta(days=IncapacidadEmpleado.MAX_DIAS))
    dias = set()
    cursor = inicio
    while cursor <= fecha_fin:
        dias.add((empleado_id, cursor))
        cursor += timedelta(days=1)
    return dias


def _reevaluar_cambio_cobertura(antes: set, despues: set) -> None:
    """Reevalúa solo los días que cambiaron de cobertura.

    Recalcular el rango completo reescribe historia que nadie pidió: al corregir el
    año de un registro heredado el motor le fabricó faltas —y hasta un escalamiento
    de baja— en la semana de inducción de la empleada. Extender una incapacidad
    larga toca ahora solo los días agregados, y cancelarla sí toca todos, que es lo
    correcto.
    """
    # Nunca hacia el futuro: un día que deja de estar cubierto y todavía no llega no
    # tiene checadas, y evaluarlo generaría una falta por adelantado. El job diario
    # los evalúa cuando toca.
    hoy = timezone.localdate()
    afectados = {(emp, dia) for emp, dia in antes ^ despues if dia <= hoy}
    if not afectados:
        return

    por_empleado: dict[int, list] = {}
    for empleado_id, dia in afectados:
        por_empleado.setdefault(empleado_id, []).append(dia)

    empleados = Empleado.objects.in_bulk(por_empleado.keys())
    for empleado_id, dias in por_empleado.items():
        empleado = empleados.get(empleado_id)
        if empleado is None:
            continue
        for dia in sorted(dias):
            evaluar_rango_asistencia(dia, dia, empleados=[empleado])


def _require_manage_rrhh(user):
    if not can_manage_submodule(user, "rrhh", "nomina"):
        raise PermissionDenied("Solo Capital Humano puede capturar incapacidades.")


@login_required
def rrhh_incapacidades(request):
    if not can_view_submodule(request.user, "rrhh", "nomina"):
        raise PermissionDenied("No tienes permisos para ver incapacidades.")

    empleado_id = (request.GET.get("empleado") or "").strip()
    estado = (request.GET.get("estado") or "").strip()
    vigencia = (request.GET.get("vigencia") or "").strip()
    if vigencia not in VIGENCIA_LABELS:
        vigencia = ""
    hoy = timezone.localdate()
    incapacidades_qs = (
        IncapacidadEmpleado.objects.select_related("empleado", "registrada_por")
        .order_by("-fecha_inicio", "-id")
    )
    if empleado_id.isdigit():
        incapacidades_qs = incapacidades_qs.filter(empleado_id=int(empleado_id))
    if estado in {choice[0] for choice in IncapacidadEmpleado.ESTADO_CHOICES}:
        incapacidades_qs = incapacidades_qs.filter(estado=estado)
    incapacidades_qs = _filtrar_vigencia(incapacidades_qs, vigencia, hoy)

    rows = []
    for incapacidad in incapacidades_qs[:300]:
        vigencia_registro = _vigencia_incapacidad(incapacidad, hoy)
        vacaciones = SolicitudVacaciones.objects.filter(
            empleado=incapacidad.empleado,
            estado__in=[
                SolicitudVacaciones.ESTADO_SOLICITADA,
                SolicitudVacaciones.ESTADO_PREAUTORIZADA,
                SolicitudVacaciones.ESTADO_APROBADA,
            ],
            fecha_inicio__lte=incapacidad.fecha_fin,
            fecha_fin__gte=incapacidad.fecha_inicio,
        ).order_by("-creado_en", "-id")[:5]
        rows.append(
            {
                "incapacidad": incapacidad,
                "vacaciones": vacaciones,
                "vigencia": vigencia_registro,
                "vigencia_label": VIGENCIA_LABELS[vigencia_registro],
                "vigencia_badge_class": VIGENCIA_BADGE_CLASSES[vigencia_registro],
            }
        )

    empleados = Empleado.objects.filter(activo=True).order_by("nombre", "id")
    return render(
        request,
        "rrhh/incapacidades.html",
        {
            "module_tabs": _module_tabs("incapacidades", request.user),
            "rows": rows,
            "empleados": empleados,
            "tipo_choices": IncapacidadEmpleado.TIPO_CHOICES,
            "estado_choices": IncapacidadEmpleado.ESTADO_CHOICES,
            "vigencia_choices": VIGENCIA_CHOICES,
            "empleado_id": empleado_id,
            "estado": estado,
            "vigencia": vigencia,
            "can_manage_rrhh": can_manage_submodule(request.user, "rrhh", "nomina"),
        },
    )


@login_required
@require_POST
def crear_incapacidad(request):
    _require_manage_rrhh(request.user)
    empleado_id = (request.POST.get("empleado") or "").strip()
    if not empleado_id.isdigit():
        messages.error(request, "Selecciona un empleado activo.")
        return redirect("rrhh:rrhh_incapacidades")
    empleado = get_object_or_404(Empleado, pk=int(empleado_id), activo=True)
    fecha_inicio = parse_date((request.POST.get("fecha_inicio") or "").strip())
    fecha_fin = parse_date((request.POST.get("fecha_fin") or "").strip())
    tipo = (request.POST.get("tipo") or "").strip()
    estado = (request.POST.get("estado") or IncapacidadEmpleado.ESTADO_ACTIVA).strip()
    if not fecha_inicio or not fecha_fin or not tipo:
        messages.error(request, "Empleado, fechas y tipo son obligatorios.")
        return redirect("rrhh:rrhh_incapacidades")
    if tipo not in {choice[0] for choice in IncapacidadEmpleado.TIPO_CHOICES}:
        messages.error(request, "Tipo de incapacidad inválido.")
        return redirect("rrhh:rrhh_incapacidades")
    if estado not in {IncapacidadEmpleado.ESTADO_ACTIVA, IncapacidadEmpleado.ESTADO_CERRADA}:
        messages.error(request, "Solo se puede crear una incapacidad activa o cerrada.")
        return redirect("rrhh:rrhh_incapacidades")
    incapacidad = IncapacidadEmpleado(
        empleado=empleado,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        tipo=tipo,
        folio=(request.POST.get("folio") or "").strip(),
        estado=estado,
        notas=(request.POST.get("notas") or "").strip(),
        registrada_por=request.user,
    )
    try:
        incapacidad.full_clean(validate_constraints=False)
        incapacidad.save()
    except (IntegrityError, ValidationError) as exc:
        messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
        return redirect("rrhh:rrhh_incapacidades")

    _reevaluar_cambio_cobertura(
        set(), _dias_cubiertos(empleado.pk, fecha_inicio, fecha_fin, incapacidad.estado)
    )
    messages.success(request, "Incapacidad registrada.")
    return redirect("rrhh:rrhh_incapacidades")


@login_required
@require_POST
def cancelar_incapacidad(request, incapacidad_id):
    _require_manage_rrhh(request.user)
    incapacidad = get_object_or_404(IncapacidadEmpleado, pk=incapacidad_id)
    comentario = (request.POST.get("comentario_cancelacion") or "").strip()
    if not comentario:
        messages.error(request, "El comentario de cancelación es obligatorio.")
        return redirect("rrhh:rrhh_incapacidades")

    estado_anterior = incapacidad.get_estado_display()
    cubiertos_antes = _dias_cubiertos(
        incapacidad.empleado_id, incapacidad.fecha_inicio, incapacidad.fecha_fin, incapacidad.estado
    )
    incapacidad.estado = IncapacidadEmpleado.ESTADO_CANCELADA
    incapacidad.comentario_cancelacion = comentario
    incapacidad.save(update_fields=["estado", "comentario_cancelacion", "actualizado_en"])
    IncapacidadCambio.objects.create(
        incapacidad=incapacidad,
        accion=IncapacidadCambio.ACCION_CANCELAR,
        motivo=comentario,
        cambios=[
            {
                "campo": "Estado administrativo",
                "antes": estado_anterior,
                "despues": incapacidad.get_estado_display(),
            }
        ],
        realizado_por=request.user,
    )
    _reevaluar_cambio_cobertura(cubiertos_antes, set())
    messages.success(request, "Incapacidad cancelada.")
    return redirect("rrhh:rrhh_incapacidades")


@login_required
def editar_incapacidad(request, incapacidad_id):
    _require_manage_rrhh(request.user)
    incapacidad = get_object_or_404(
        IncapacidadEmpleado.objects.select_related("empleado", "registrada_por"), pk=incapacidad_id
    )
    if request.method != "POST":
        return render(
            request,
            "rrhh/incapacidad_editar.html",
            {
                "module_tabs": _module_tabs("incapacidades", request.user),
                "incapacidad": incapacidad,
                "empleados": Empleado.objects.filter(activo=True).order_by("nombre", "id"),
                "tipo_choices": IncapacidadEmpleado.TIPO_CHOICES,
                "bitacora": incapacidad.cambios.select_related("realizado_por"),
            },
        )

    motivo = (request.POST.get("motivo") or "").strip()
    if not motivo:
        messages.error(request, "El motivo de la corrección es obligatorio.")
        return redirect("rrhh:rrhh_incapacidad_editar", incapacidad_id=incapacidad.id)

    empleado_id = (request.POST.get("empleado") or "").strip()
    if not empleado_id.isdigit():
        messages.error(request, "Selecciona un empleado activo.")
        return redirect("rrhh:rrhh_incapacidad_editar", incapacidad_id=incapacidad.id)
    empleado = get_object_or_404(Empleado, pk=int(empleado_id), activo=True)
    fecha_inicio = parse_date((request.POST.get("fecha_inicio") or "").strip())
    fecha_fin = parse_date((request.POST.get("fecha_fin") or "").strip())
    tipo = (request.POST.get("tipo") or "").strip()
    estado = (request.POST.get("estado") or "").strip()
    if not fecha_inicio or not fecha_fin or not tipo:
        messages.error(request, "Empleado, fechas y tipo son obligatorios.")
        return redirect("rrhh:rrhh_incapacidad_editar", incapacidad_id=incapacidad.id)
    if tipo not in {choice[0] for choice in IncapacidadEmpleado.TIPO_CHOICES}:
        messages.error(request, "Tipo de incapacidad inválido.")
        return redirect("rrhh:rrhh_incapacidad_editar", incapacidad_id=incapacidad.id)
    if estado not in {IncapacidadEmpleado.ESTADO_ACTIVA, IncapacidadEmpleado.ESTADO_CERRADA}:
        messages.error(
            request,
            "La corrección solo puede dejar la incapacidad activa o cerrada; "
            "para cancelarla usa el botón Cancelar.",
        )
        return redirect("rrhh:rrhh_incapacidad_editar", incapacidad_id=incapacidad.id)

    antes = {campo: _valor_legible(incapacidad, campo) for campo, _ in CAMPOS_EDITABLES}
    cubiertos_antes = _dias_cubiertos(
        incapacidad.empleado_id, incapacidad.fecha_inicio, incapacidad.fecha_fin, incapacidad.estado
    )

    incapacidad.empleado = empleado
    incapacidad.fecha_inicio = fecha_inicio
    incapacidad.fecha_fin = fecha_fin
    incapacidad.tipo = tipo
    incapacidad.folio = (request.POST.get("folio") or "").strip()
    incapacidad.estado = estado
    incapacidad.notas = (request.POST.get("notas") or "").strip()
    incapacidad.comentario_cancelacion = ""

    try:
        incapacidad.full_clean(validate_constraints=False)
        incapacidad.save()
    except (IntegrityError, ValidationError) as exc:
        messages.error(request, "; ".join(exc.messages) if hasattr(exc, "messages") else str(exc))
        return redirect("rrhh:rrhh_incapacidad_editar", incapacidad_id=incapacidad.id)

    cambios = _diff_incapacidad(antes, incapacidad)
    if not cambios:
        messages.info(request, "No hubo cambios que registrar.")
        return redirect("rrhh:rrhh_incapacidades")

    IncapacidadCambio.objects.create(
        incapacidad=incapacidad,
        accion=IncapacidadCambio.ACCION_EDITAR,
        motivo=motivo,
        cambios=cambios,
        realizado_por=request.user,
    )

    _reevaluar_cambio_cobertura(
        cubiertos_antes,
        _dias_cubiertos(empleado.pk, fecha_inicio, fecha_fin, incapacidad.estado),
    )
    messages.success(request, "Incapacidad corregida.")
    return redirect("rrhh:rrhh_incapacidades")
