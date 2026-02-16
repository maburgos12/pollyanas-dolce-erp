import logging

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from maestros.models import Insumo, Proveedor
from recetas.models import Receta

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
            }
        )
    except Exception:
        logger.exception("Dashboard failed to build full context")

    return render(request, "core/dashboard.html", ctx)


def health_check(_request: HttpRequest) -> JsonResponse:
    return JsonResponse({"status": "ok"})
