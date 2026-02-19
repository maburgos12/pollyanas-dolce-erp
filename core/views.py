import logging
import os
from datetime import timedelta
from decimal import Decimal

from django.db.models import F
from django.db.models import Avg, Sum
from django.core.paginator import Paginator
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.utils import timezone
from core.access import (
    can_view_audit,
    can_manage_compras,
    can_manage_inventario,
    can_view_compras,
    can_view_inventario,
    can_view_maestros,
    can_view_recetas,
    can_view_reportes,
)
from maestros.models import Insumo, Proveedor
from recetas.models import Receta
from inventario.models import AlmacenSyncRun, ExistenciaInsumo
from core.models import AuditLog

logger = logging.getLogger(__name__)

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
