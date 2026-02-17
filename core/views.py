import logging

from django.db.models import F
from django.core.paginator import Paginator
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
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
from inventario.models import ExistenciaInsumo
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
    }
    try:
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
