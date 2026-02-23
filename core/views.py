import logging
import os
import calendar
from datetime import date
from datetime import timedelta
from decimal import Decimal

from django.db import OperationalError, ProgrammingError
from django.db.models import F
from django.db.models import Avg, Sum
from django.core.paginator import Paginator
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.utils import timezone
from core.access import (
    can_manage_crm,
    can_view_audit,
    can_manage_compras,
    can_manage_inventario,
    can_manage_rrhh,
    can_view_crm,
    can_view_compras,
    can_view_inventario,
    can_view_maestros,
    can_view_recetas,
    can_view_rrhh,
    can_view_reportes,
)
from maestros.models import Insumo, Proveedor, PointPendingMatch
from maestros.models import CostoInsumo
from compras.models import PresupuestoCompraPeriodo, SolicitudCompra, OrdenCompra
from recetas.models import PlanProduccionItem, PronosticoVenta, Receta, LineaReceta
from inventario.models import AlmacenSyncRun, ExistenciaInsumo
from core.models import AuditLog
from core.audit import log_event

logger = logging.getLogger(__name__)


def _budget_period_bounds(periodo_tipo: str, periodo_mes: str) -> tuple[date, date]:
    year, month = periodo_mes.split("-")
    y = int(year)
    m = int(month)
    start = date(y, m, 1)
    end = date(y, m, calendar.monthrange(y, m)[1])
    if periodo_tipo == "q1":
        end = date(y, m, 15)
    elif periodo_tipo == "q2":
        start = date(y, m, 16)
    return start, end


def _compute_budget_semaforo(periodo_tipo: str, periodo_mes: str) -> dict:
    start, end = _budget_period_bounds(periodo_tipo, periodo_mes)
    objetivo_obj = PresupuestoCompraPeriodo.objects.filter(
        periodo_tipo=periodo_tipo,
        periodo_mes=periodo_mes,
    ).first()
    objetivo = objetivo_obj.monto_objetivo if objetivo_obj else Decimal("0")

    solicitudes = list(
        SolicitudCompra.objects.filter(fecha_requerida__range=(start, end)).only("insumo_id", "cantidad")
    )
    insumo_ids = [s.insumo_id for s in solicitudes]
    latest_cost_by_insumo: dict[int, Decimal] = {}
    if insumo_ids:
        for c in CostoInsumo.objects.filter(insumo_id__in=insumo_ids).order_by("insumo_id", "-fecha", "-id"):
            if c.insumo_id not in latest_cost_by_insumo:
                latest_cost_by_insumo[c.insumo_id] = c.costo_unitario

    estimado = sum(
        ((s.cantidad or Decimal("0")) * (latest_cost_by_insumo.get(s.insumo_id, Decimal("0"))))
        for s in solicitudes
    )
    ejecutado = (
        OrdenCompra.objects.exclude(estatus=OrdenCompra.STATUS_BORRADOR)
        .filter(fecha_emision__range=(start, end))
        .aggregate(total=Sum("monto_estimado"))
        .get("total")
        or Decimal("0")
    )

    base = max(estimado, ejecutado)
    ratio_pct = ((base * Decimal("100")) / objetivo) if objetivo > 0 else None
    if objetivo <= 0:
        estado = "SIN_OBJETIVO"
        badge = "bg-warning"
        estado_label = "Sin objetivo"
    elif ratio_pct <= Decimal("90"):
        estado = "VERDE"
        badge = "bg-success"
        estado_label = "Verde"
    elif ratio_pct <= Decimal("100"):
        estado = "AMARILLO"
        badge = "bg-warning"
        estado_label = "Amarillo"
    else:
        estado = "ROJO"
        badge = "bg-danger"
        estado_label = "Rojo"

    if periodo_tipo == "mes":
        periodo_label = f"Mensual {periodo_mes}"
    elif periodo_tipo == "q1":
        periodo_label = f"1ra quincena {periodo_mes}"
    else:
        periodo_label = f"2da quincena {periodo_mes}"

    return {
        "periodo_tipo": periodo_tipo,
        "periodo_mes": periodo_mes,
        "periodo_label": periodo_label,
        "objetivo": objetivo,
        "estimado": estimado,
        "ejecutado": ejecutado,
        "ratio_pct": ratio_pct,
        "estado": estado,
        "estado_label": estado_label,
        "estado_badge": badge,
        "sobre_objetivo_estimado": bool(objetivo > 0 and estimado > objetivo),
        "sobre_objetivo_ejecutado": bool(objetivo > 0 and ejecutado > objetivo),
    }


def _log_budget_alert_once(alert_data: dict, kind: str) -> None:
    if kind not in {"ESTIMADO", "EJECUTADO"}:
        return
    object_id = f"{alert_data['periodo_tipo']}:{alert_data['periodo_mes']}:{kind}"
    today = timezone.localdate()
    exists = AuditLog.objects.filter(
        action="ALERT",
        model="compras.PresupuestoCompraPeriodo",
        object_id=object_id,
        timestamp__date=today,
    ).exists()
    if exists:
        return

    valor = alert_data["estimado"] if kind == "ESTIMADO" else alert_data["ejecutado"]
    log_event(
        None,
        "ALERT",
        "compras.PresupuestoCompraPeriodo",
        object_id,
        {
            "periodo_tipo": alert_data["periodo_tipo"],
            "periodo_mes": alert_data["periodo_mes"],
            "periodo_label": alert_data["periodo_label"],
            "kind": kind,
            "objetivo": str(alert_data["objetivo"]),
            "valor": str(valor),
            "ratio_pct": str(alert_data["ratio_pct"] or Decimal("0")),
            "mensaje": f"{kind} supera objetivo del periodo",
        },
    )


def _compute_plan_forecast_semaforo(periodo_mes: str) -> dict:
    try:
        year, month = periodo_mes.split("-")
        y = int(year)
        m = int(month)
    except Exception:
        today = timezone.localdate()
        y = today.year
        m = today.month
        periodo_mes = f"{y:04d}-{m:02d}"

    # In local or partially migrated environments, forecast tables may not exist yet.
    pron_unavailable = False
    plan_unavailable = False
    try:
        pron_rows = list(
            PronosticoVenta.objects.filter(periodo=periodo_mes)
            .values("receta_id", "receta__nombre")
            .annotate(total=Sum("cantidad"))
        )
    except (OperationalError, ProgrammingError):
        pron_rows = []
        pron_unavailable = True

    try:
        plan_rows = list(
            PlanProduccionItem.objects.filter(plan__fecha_produccion__year=y, plan__fecha_produccion__month=m)
            .values("receta_id", "receta__nombre")
            .annotate(total=Sum("cantidad"))
        )
    except (OperationalError, ProgrammingError):
        plan_rows = []
        plan_unavailable = True

    merged: dict[int, dict] = {}
    for row in pron_rows:
        receta_id = int(row["receta_id"])
        merged[receta_id] = {
            "receta_id": receta_id,
            "receta": row["receta__nombre"],
            "pronostico": Decimal(str(row["total"] or 0)),
            "plan": Decimal("0"),
        }
    for row in plan_rows:
        receta_id = int(row["receta_id"])
        current = merged.setdefault(
            receta_id,
            {
                "receta_id": receta_id,
                "receta": row["receta__nombre"],
                "pronostico": Decimal("0"),
                "plan": Decimal("0"),
            },
        )
        current["plan"] = Decimal(str(row["total"] or 0))

    rows = []
    con_desviacion = 0
    for row in merged.values():
        delta = row["plan"] - row["pronostico"]
        if delta != 0:
            con_desviacion += 1
        row["delta"] = delta
        if row["pronostico"] > 0:
            row["delta_pct"] = (delta * Decimal("100")) / row["pronostico"]
        else:
            row["delta_pct"] = None
        rows.append(row)

    rows = sorted(rows, key=lambda x: (abs(x["delta"]), x["receta"]), reverse=True)
    total_plan = sum((r["plan"] for r in rows), Decimal("0"))
    total_pronostico = sum((r["pronostico"] for r in rows), Decimal("0"))
    delta_total = total_plan - total_pronostico
    if total_pronostico > 0:
        desviacion_abs_pct = (abs(delta_total) * Decimal("100")) / total_pronostico
    else:
        desviacion_abs_pct = None

    if total_pronostico <= 0 and total_plan <= 0:
        semaforo = "Sin datos"
        semaforo_badge = "bg-warning"
    elif total_pronostico <= 0 and total_plan > 0:
        semaforo = "Rojo"
        semaforo_badge = "bg-danger"
    elif desviacion_abs_pct is not None and desviacion_abs_pct <= Decimal("10"):
        semaforo = "Verde"
        semaforo_badge = "bg-success"
    elif desviacion_abs_pct is not None and desviacion_abs_pct <= Decimal("25"):
        semaforo = "Amarillo"
        semaforo_badge = "bg-warning"
    else:
        semaforo = "Rojo"
        semaforo_badge = "bg-danger"

    return {
        "periodo_mes": periodo_mes,
        "total_plan": total_plan,
        "total_pronostico": total_pronostico,
        "delta_total": delta_total,
        "desviacion_abs_pct": desviacion_abs_pct,
        "recetas_total": len(rows),
        "recetas_con_desviacion": con_desviacion,
        "semaforo_label": semaforo,
        "semaforo_badge": semaforo_badge,
        "rows_top": rows[:8],
        "data_unavailable": pron_unavailable or plan_unavailable,
    }


def login_view(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        username = request.POST.get("username", "")
        password = request.POST.get("password", "")
        
        logger.info(f"Login attempt: username={username}")
        
        if username and password:
            user = authenticate(request, username=username, password=password)
            logger.info(f"Authentication result: user={user}")
            if user is not None and user.is_active:
                login(request, user)
                logger.info(f"Login successful for user={username}")
                return redirect("dashboard")
            else:
                logger.warning(f"Authentication failed for username={username}")
        else:
            logger.warning(f"Missing username or password")
        
        return render(request, "core/login.html", {"error": "Credenciales inválidas"})
    
    return render(request, "core/login.html")

def logout_view(request: HttpRequest) -> HttpResponse:
    logout(request)
    return redirect("login")

def dashboard(request: HttpRequest) -> HttpResponse:
    if not request.user.is_authenticated:
        return redirect("/login/")

    u = request.user
    ctx = {
        "can_view_recetas": False,
        "can_import": False,
        "can_review_matching": False,
        "can_view_crm": False,
        "can_manage_crm": False,
        "can_view_rrhh": False,
        "can_manage_rrhh": False,
        "insumos_count": 0,
        "recetas_count": 0,
        "proveedores_count": 0,
        "alertas_count": 0,
        "criticos_count": 0,
        "bajo_reorden_count": 0,
        "latest_almacen_sync": None,
        "auto_sync_enabled": False,
        "auto_sync_interval_hours": 24,
        "next_sync_eta": None,
        "next_sync_state_label": "",
        "next_sync_state_class": "bg-warning",
        "budget_semaforo_mes": None,
        "budget_semaforo_quincena": None,
        "budget_alerts_active": 0,
        "latest_budget_alert": None,
        "plan_forecast_semaforo": None,
        "point_pending_total": 0,
        "point_pending_insumos": 0,
        "point_pending_productos": 0,
        "point_pending_proveedores": 0,
        "recetas_pending_matching_count": 0,
        "inventario_last_unmatched_count": 0,
        "homologacion_total_pending": 0,
    }
    try:
        existencias_qs = ExistenciaInsumo.objects.all()
        inventario_total_count = existencias_qs.count()
        stock_min_config_count = existencias_qs.exclude(stock_minimo=0).count()
        stock_max_config_count = existencias_qs.exclude(stock_maximo=0).count()
        inv_prom_config_count = existencias_qs.exclude(inventario_promedio=0).count()
        punto_reorden_config_count = existencias_qs.exclude(punto_reorden=0).count()

        stock_bajo_min_count = existencias_qs.filter(stock_minimo__gt=0, stock_actual__lt=F("stock_minimo")).count()
        stock_sobre_max_count = existencias_qs.filter(stock_maximo__gt=0, stock_actual__gt=F("stock_maximo")).count()

        agg = existencias_qs.aggregate(
            avg_dias_llegada=Avg("dias_llegada_pedido"),
            avg_consumo_diario=Avg("consumo_diario_promedio"),
            total_consumo_diario=Sum("consumo_diario_promedio"),
        )

        lead_time_risk_count = 0
        cobertura_total = Decimal("0")
        cobertura_items = 0
        for e in existencias_qs.only("stock_actual", "consumo_diario_promedio", "dias_llegada_pedido"):
            consumo = e.consumo_diario_promedio or Decimal("0")
            dias_llegada = Decimal(e.dias_llegada_pedido or 0)
            if consumo <= 0:
                continue
            cobertura_dias = e.stock_actual / consumo
            cobertura_total += cobertura_dias
            cobertura_items += 1
            if dias_llegada > 0 and cobertura_dias < dias_llegada:
                lead_time_risk_count += 1

        cobertura_promedio_dias = (cobertura_total / cobertura_items) if cobertura_items else Decimal("0")

        ctx.update(
            {
                "can_view_recetas": u.has_perm("recetas.view_receta"),
                "can_import": u.is_superuser or u.groups.filter(name__in=["ADMIN", "COMPRAS"]).exists(),
                "can_review_matching": u.is_superuser or u.groups.filter(name__in=["ADMIN"]).exists(),
                "insumos_count": Insumo.objects.filter(activo=True).count(),
                "recetas_count": Receta.objects.count(),
                "proveedores_count": Proveedor.objects.count(),
                "alertas_count": ExistenciaInsumo.objects.filter(stock_actual__lt=F("punto_reorden")).count(),
                "criticos_count": ExistenciaInsumo.objects.filter(stock_actual__lte=0).count(),
                "bajo_reorden_count": ExistenciaInsumo.objects.filter(stock_actual__gt=0, stock_actual__lt=F("punto_reorden")).count(),
                "can_view_maestros": can_view_maestros(u),
                "can_view_recetas": can_view_recetas(u),
                "can_view_compras": can_view_compras(u),
                "can_manage_compras": can_manage_compras(u),
                "can_view_crm": can_view_crm(u),
                "can_manage_crm": can_manage_crm(u),
                "can_view_rrhh": can_view_rrhh(u),
                "can_manage_rrhh": can_manage_rrhh(u),
                "can_view_inventario": can_view_inventario(u),
                "can_manage_inventario": can_manage_inventario(u),
                "can_view_reportes": can_view_reportes(u),
                "inventario_total_count": inventario_total_count,
                "stock_min_config_count": stock_min_config_count,
                "stock_max_config_count": stock_max_config_count,
                "inv_prom_config_count": inv_prom_config_count,
                "punto_reorden_config_count": punto_reorden_config_count,
                "stock_bajo_min_count": stock_bajo_min_count,
                "stock_sobre_max_count": stock_sobre_max_count,
                "lead_time_risk_count": lead_time_risk_count,
                "avg_dias_llegada": agg.get("avg_dias_llegada") or Decimal("0"),
                "avg_consumo_diario": agg.get("avg_consumo_diario") or Decimal("0"),
                "total_consumo_diario": agg.get("total_consumo_diario") or Decimal("0"),
                "cobertura_promedio_dias": cobertura_promedio_dias,
            }
        )
        latest_sync = (
            AlmacenSyncRun.objects.select_related("triggered_by")
            .order_by("-started_at", "-id")
            .first()
        )
        ctx["latest_almacen_sync"] = latest_sync
        inventario_last_unmatched_count = int(latest_sync.unmatched or 0) if latest_sync else 0
        point_pending_insumos = PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_INSUMO).count()
        point_pending_productos = PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_PRODUCTO).count()
        point_pending_proveedores = PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_PROVEEDOR).count()
        point_pending_total = point_pending_insumos + point_pending_productos + point_pending_proveedores
        recetas_pending_matching_count = (
            LineaReceta.objects.exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .filter(match_status__in=[LineaReceta.STATUS_NEEDS_REVIEW, LineaReceta.STATUS_REJECTED])
            .count()
        )
        ctx.update(
            {
                "point_pending_total": point_pending_total,
                "point_pending_insumos": point_pending_insumos,
                "point_pending_productos": point_pending_productos,
                "point_pending_proveedores": point_pending_proveedores,
                "recetas_pending_matching_count": recetas_pending_matching_count,
                "inventario_last_unmatched_count": inventario_last_unmatched_count,
                "homologacion_total_pending": (
                    point_pending_total + recetas_pending_matching_count + inventario_last_unmatched_count
                ),
            }
        )

        auto_sync_enabled = os.getenv("ENABLE_AUTO_SYNC_ALMACEN", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        try:
            auto_sync_interval_hours = int((os.getenv("AUTO_SYNC_INTERVAL_HOURS", "24") or "24").strip())
        except ValueError:
            auto_sync_interval_hours = 24
        auto_sync_interval_hours = max(auto_sync_interval_hours, 1)

        next_sync_eta = None
        next_sync_state_label = "Pendiente"
        next_sync_state_class = "bg-warning"
        if auto_sync_enabled:
            latest_scheduled = (
                AlmacenSyncRun.objects.filter(source=AlmacenSyncRun.SOURCE_SCHEDULED)
                .order_by("-started_at", "-id")
                .first()
            )
            if latest_scheduled:
                delta = timedelta(hours=auto_sync_interval_hours)
                next_sync_eta = latest_scheduled.started_at + delta
                now = timezone.now()
                while next_sync_eta <= now:
                    next_sync_eta += delta
                hours_to_next = (next_sync_eta - now).total_seconds() / 3600
                if hours_to_next <= 2:
                    next_sync_state_label = "Próximo"
                    next_sync_state_class = "bg-danger"
                elif hours_to_next <= 8:
                    next_sync_state_label = "Hoy"
                    next_sync_state_class = "bg-warning"
                else:
                    next_sync_state_label = "Programado"
                    next_sync_state_class = "bg-success"
            else:
                next_sync_state_label = "Pendiente"
                next_sync_state_class = "bg-warning"
        else:
            next_sync_state_label = "Desactivado"
            next_sync_state_class = "bg-danger"

        ctx.update(
            {
                "auto_sync_enabled": auto_sync_enabled,
                "auto_sync_interval_hours": auto_sync_interval_hours,
                "next_sync_eta": next_sync_eta,
                "next_sync_state_label": next_sync_state_label,
                "next_sync_state_class": next_sync_state_class,
            }
        )

        today = timezone.localdate()
        periodo_mes = f"{today.year:04d}-{today.month:02d}"
        periodo_quincena = "q1" if today.day <= 15 else "q2"
        semaforo_mes = _compute_budget_semaforo("mes", periodo_mes)
        semaforo_quincena = _compute_budget_semaforo(periodo_quincena, periodo_mes)

        for semaforo in (semaforo_mes, semaforo_quincena):
            if semaforo["sobre_objetivo_estimado"]:
                _log_budget_alert_once(semaforo, "ESTIMADO")
            if semaforo["sobre_objetivo_ejecutado"]:
                _log_budget_alert_once(semaforo, "EJECUTADO")

        ctx.update(
            {
                "budget_semaforo_mes": semaforo_mes,
                "budget_semaforo_quincena": semaforo_quincena,
                "budget_alerts_active": sum(
                    1
                    for s in (semaforo_mes, semaforo_quincena)
                    if s["sobre_objetivo_estimado"] or s["sobre_objetivo_ejecutado"]
                ),
                "latest_budget_alert": AuditLog.objects.filter(
                    action="ALERT",
                    model="compras.PresupuestoCompraPeriodo",
                ).first(),
            }
        )
        ctx["plan_forecast_semaforo"] = _compute_plan_forecast_semaforo(periodo_mes)
    except Exception:
        logger.exception("Dashboard failed to build full context")

    return render(request, "core/dashboard.html", ctx)


def health_check(_request: HttpRequest) -> JsonResponse:
    return JsonResponse({"status": "ok"})


def audit_log_view(request: HttpRequest) -> HttpResponse:
    if not request.user.is_authenticated:
        return redirect("/login/")
    if not can_view_audit(request.user):
        raise PermissionDenied("No tienes permisos para ver la bitácora.")

    logs = AuditLog.objects.select_related("user").all()

    model = (request.GET.get("model") or "").strip()
    action = (request.GET.get("action") or "").strip()
    username = (request.GET.get("username") or "").strip()
    date_from = (request.GET.get("date_from") or "").strip()
    date_to = (request.GET.get("date_to") or "").strip()
    q = (request.GET.get("q") or "").strip()

    if model:
        logs = logs.filter(model=model)
    if action:
        logs = logs.filter(action=action)
    if username:
        logs = logs.filter(user__username__icontains=username)
    if date_from:
        logs = logs.filter(timestamp__date__gte=date_from)
    if date_to:
        logs = logs.filter(timestamp__date__lte=date_to)
    if q:
        logs = logs.filter(object_id__icontains=q)

    page = Paginator(logs, 30).get_page(request.GET.get("page"))
    context = {
        "page": page,
        "models": AuditLog.objects.order_by("model").values_list("model", flat=True).distinct(),
        "actions": AuditLog.objects.order_by("action").values_list("action", flat=True).distinct(),
        "filters": {
            "model": model,
            "action": action,
            "username": username,
            "date_from": date_from,
            "date_to": date_to,
            "q": q,
        },
    }
    return render(request, "core/auditoria.html", context)
