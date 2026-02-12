from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from maestros.models import Insumo, Proveedor
from recetas.models import Receta

def login_view(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        username = request.POST.get("username", "")
        password = request.POST.get("password", "")
        
        if username and password:
            user = authenticate(request, username=username, password=password)
            if user is not None and user.is_active:
                login(request, user)
                return redirect("dashboard")
        
        return render(request, "core/login.html", {"error": "Credenciales inválidas"})
    
    return render(request, "core/login.html")

def logout_view(request: HttpRequest) -> HttpResponse:
    logout(request)
    return redirect("login")

# @login_required(login_url="login")  # Temporarily disabled for testing
def dashboard(request: HttpRequest) -> HttpResponse:
    u = request.user if request.user.is_authenticated else None
    insumos_count = Insumo.objects.filter(activo=True).count()
    recetas_count = Receta.objects.all().count()
    proveedores_count = Proveedor.objects.all().count()
    
    ctx = {
        "can_view_recetas": u.has_perm("recetas.view_receta"),
        "can_import": u.is_superuser or u.groups.filter(name__in=["ADMIN", "COMPRAS"]).exists(),
        "can_review_matching": u.is_superuser or u.groups.filter(name__in=["ADMIN"]).exists(),
        "insumos_count": insumos_count,
        "recetas_count": recetas_count,
        "proveedores_count": proveedores_count,
        "alertas_count": 0,
    }
    return render(request, "core/dashboard.html", ctx)
