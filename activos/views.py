from datetime import timedelta
from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Q, Sum
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from core.access import can_manage_inventario, can_view_inventario
from core.audit import log_event

from .models import Activo, BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento


def _safe_decimal(value) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except Exception:
        return Decimal("0")


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Dashboard", "url_name": "activos:dashboard", "active": active == "dashboard"},
        {"label": "Órdenes", "url_name": "activos:ordenes", "active": active == "ordenes"},
        {"label": "Calendario", "url_name": "activos:calendario", "active": active == "calendario"},
    ]


@login_required
def dashboard(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    today = timezone.localdate()
    week_limit = today + timedelta(days=7)
    month_limit = today + timedelta(days=30)

    activos_qs = Activo.objects.filter(activo=True)
    ordenes_abiertas_qs = OrdenMantenimiento.objects.filter(
        estatus__in=[OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO]
    )
    planes_activos_qs = PlanMantenimiento.objects.filter(estatus=PlanMantenimiento.ESTATUS_ACTIVO, activo=True)

    proximos = list(
        planes_activos_qs.filter(proxima_ejecucion__isnull=False, proxima_ejecucion__lte=month_limit)
        .select_related("activo_ref")
        .order_by("proxima_ejecucion", "id")[:30]
    )
    ordenes_recientes = list(
        OrdenMantenimiento.objects.select_related("activo_ref", "plan_ref")
        .order_by("-fecha_programada", "-id")[:20]
    )

    costo_mes = (
        OrdenMantenimiento.objects.filter(
            fecha_cierre__year=today.year,
            fecha_cierre__month=today.month,
            estatus=OrdenMantenimiento.ESTATUS_CERRADA,
        ).aggregate(
            rep=Sum("costo_repuestos"),
            mo=Sum("costo_mano_obra"),
            otros=Sum("costo_otros"),
        )
    )
    costo_mes_total = _safe_decimal(costo_mes.get("rep")) + _safe_decimal(costo_mes.get("mo")) + _safe_decimal(
        costo_mes.get("otros")
    )

    criticidad_rows = list(
        activos_qs.values("criticidad")
        .annotate(total=Count("id"))
        .order_by()
    )
    criticidad = {
        "ALTA": 0,
        "MEDIA": 0,
        "BAJA": 0,
    }
    for row in criticidad_rows:
        criticidad[row["criticidad"]] = int(row["total"] or 0)

    context = {
        "module_tabs": _module_tabs("dashboard"),
        "activos_total": activos_qs.count(),
        "activos_operativos": activos_qs.filter(estado=Activo.ESTADO_OPERATIVO).count(),
        "activos_mantenimiento": activos_qs.filter(estado=Activo.ESTADO_MANTENIMIENTO).count(),
        "activos_fuera_servicio": activos_qs.filter(estado=Activo.ESTADO_FUERA_SERVICIO).count(),
        "ordenes_abiertas": ordenes_abiertas_qs.count(),
        "ordenes_en_proceso": ordenes_abiertas_qs.filter(estatus=OrdenMantenimiento.ESTATUS_EN_PROCESO).count(),
        "planes_vencidos": planes_activos_qs.filter(proxima_ejecucion__lt=today).count(),
        "planes_proxima_semana": planes_activos_qs.filter(
            proxima_ejecucion__gte=today,
            proxima_ejecucion__lte=week_limit,
        ).count(),
        "costo_mes_total": costo_mes_total,
        "criticidad": criticidad,
        "proximos": proximos,
        "ordenes_recientes": ordenes_recientes,
    }
    return render(request, "activos/dashboard.html", context)


@login_required
def ordenes(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para gestionar órdenes de mantenimiento.")
        activo_id = (request.POST.get("activo_id") or "").strip()
        plan_id = (request.POST.get("plan_id") or "").strip()
        tipo = (request.POST.get("tipo") or OrdenMantenimiento.TIPO_PREVENTIVO).strip().upper()
        prioridad = (request.POST.get("prioridad") or OrdenMantenimiento.PRIORIDAD_MEDIA).strip().upper()
        descripcion = (request.POST.get("descripcion") or "").strip()
        responsable = (request.POST.get("responsable") or "").strip()
        fecha_programada_raw = (request.POST.get("fecha_programada") or "").strip()
        try:
            fecha_programada = (
                timezone.datetime.fromisoformat(fecha_programada_raw).date()
                if fecha_programada_raw
                else timezone.localdate()
            )
        except ValueError:
            fecha_programada = timezone.localdate()
        if not activo_id.isdigit():
            messages.error(request, "Selecciona un activo válido.")
            return redirect("activos:ordenes")
        activo_obj = get_object_or_404(Activo, pk=int(activo_id))
        plan_obj = None
        if plan_id.isdigit():
            plan_obj = PlanMantenimiento.objects.filter(pk=int(plan_id), activo_ref=activo_obj).first()
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo_obj,
            plan_ref=plan_obj,
            tipo=tipo if tipo in {x[0] for x in OrdenMantenimiento.TIPO_CHOICES} else OrdenMantenimiento.TIPO_PREVENTIVO,
            prioridad=(
                prioridad
                if prioridad in {x[0] for x in OrdenMantenimiento.PRIORIDAD_CHOICES}
                else OrdenMantenimiento.PRIORIDAD_MEDIA
            ),
            descripcion=descripcion,
            responsable=responsable,
            fecha_programada=fecha_programada,
            creado_por=request.user,
        )
        BitacoraMantenimiento.objects.create(
            orden=orden,
            accion="CREADA",
            comentario="Orden creada desde UI",
            usuario=request.user,
        )
        log_event(
            request.user,
            "CREATE",
            "activos.OrdenMantenimiento",
            orden.id,
            {
                "folio": orden.folio,
                "activo_id": orden.activo_ref_id,
                "tipo": orden.tipo,
                "prioridad": orden.prioridad,
                "estatus": orden.estatus,
            },
        )
        messages.success(request, f"Orden {orden.folio} creada.")
        return redirect("activos:ordenes")

    estado = (request.GET.get("estatus") or "abiertas").strip().upper()
    qs = OrdenMantenimiento.objects.select_related("activo_ref", "plan_ref", "creado_por").order_by("-fecha_programada", "-id")
    if estado == "ABIERTAS":
        qs = qs.filter(estatus__in=[OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO])
    elif estado in {x[0] for x in OrdenMantenimiento.ESTATUS_CHOICES}:
        qs = qs.filter(estatus=estado)

    context = {
        "module_tabs": _module_tabs("ordenes"),
        "ordenes": list(qs[:120]),
        "activos": list(Activo.objects.filter(activo=True).order_by("nombre")[:800]),
        "planes": list(
            PlanMantenimiento.objects.filter(
                estatus=PlanMantenimiento.ESTATUS_ACTIVO,
                activo=True,
                activo_ref__activo=True,
            )
            .select_related("activo_ref")
            .order_by("activo_ref__nombre", "nombre")[:1200]
        ),
        "estado": estado,
        "can_manage_activos": can_manage_inventario(request.user),
    }
    return render(request, "activos/ordenes.html", context)


@login_required
def actualizar_orden_estatus(request, pk: int, estatus: str):
    if request.method != "POST":
        return redirect("activos:ordenes")
    if not can_manage_inventario(request.user):
        raise PermissionDenied("No tienes permisos para gestionar órdenes de mantenimiento.")

    estatus = (estatus or "").strip().upper()
    if estatus not in {x[0] for x in OrdenMantenimiento.ESTATUS_CHOICES}:
        messages.error(request, "Estatus inválido.")
        return redirect("activos:ordenes")

    orden = get_object_or_404(OrdenMantenimiento, pk=pk)
    from_status = orden.estatus
    if from_status == estatus:
        return redirect("activos:ordenes")
    orden.estatus = estatus
    today = timezone.localdate()
    if estatus == OrdenMantenimiento.ESTATUS_EN_PROCESO and not orden.fecha_inicio:
        orden.fecha_inicio = today
    if estatus == OrdenMantenimiento.ESTATUS_CERRADA:
        orden.fecha_cierre = today
        if orden.plan_ref_id:
            plan = orden.plan_ref
            plan.ultima_ejecucion = today
            plan.recompute_next_date()
            plan.save(update_fields=["ultima_ejecucion", "proxima_ejecucion", "actualizado_en"])
    orden.save(update_fields=["estatus", "fecha_inicio", "fecha_cierre", "actualizado_en"])
    BitacoraMantenimiento.objects.create(
        orden=orden,
        accion="ESTATUS",
        comentario=f"{from_status} -> {estatus}",
        usuario=request.user,
    )
    log_event(
        request.user,
        "UPDATE",
        "activos.OrdenMantenimiento",
        orden.id,
        {"from": from_status, "to": estatus, "folio": orden.folio},
    )
    messages.success(request, f"Orden {orden.folio} actualizada a {estatus}.")
    return redirect("activos:ordenes")


@login_required
def calendario(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    date_from_raw = (request.GET.get("from") or "").strip()
    date_to_raw = (request.GET.get("to") or "").strip()
    try:
        date_from = timezone.datetime.fromisoformat(date_from_raw).date() if date_from_raw else timezone.localdate()
    except ValueError:
        date_from = timezone.localdate()
    try:
        date_to = timezone.datetime.fromisoformat(date_to_raw).date() if date_to_raw else (date_from + timedelta(days=45))
    except ValueError:
        date_to = date_from + timedelta(days=45)
    if date_to < date_from:
        date_to = date_from + timedelta(days=45)

    planes = list(
        PlanMantenimiento.objects.select_related("activo_ref")
        .filter(
            activo=True,
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            proxima_ejecucion__isnull=False,
            proxima_ejecucion__gte=date_from,
            proxima_ejecucion__lte=date_to,
        )
        .order_by("proxima_ejecucion", "id")
    )
    ordenes_qs = (
        OrdenMantenimiento.objects.select_related("activo_ref", "plan_ref")
        .filter(fecha_programada__gte=date_from, fecha_programada__lte=date_to)
        .order_by("fecha_programada", "id")
    )

    events = []
    for plan in planes:
        events.append(
            {
                "fecha": plan.proxima_ejecucion,
                "tipo": "Plan",
                "referencia": f"Plan #{plan.id}",
                "activo": plan.activo_ref.nombre,
                "detalle": plan.nombre,
                "estado": plan.estatus,
            }
        )
    for orden in ordenes_qs:
        events.append(
            {
                "fecha": orden.fecha_programada,
                "tipo": "Orden",
                "referencia": orden.folio,
                "activo": orden.activo_ref.nombre,
                "detalle": orden.descripcion or orden.get_tipo_display(),
                "estado": orden.estatus,
            }
        )
    events.sort(key=lambda r: (r["fecha"], r["tipo"], r["referencia"]))

    context = {
        "module_tabs": _module_tabs("calendario"),
        "date_from": date_from,
        "date_to": date_to,
        "events": events,
    }
    return render(request, "activos/calendario.html", context)
