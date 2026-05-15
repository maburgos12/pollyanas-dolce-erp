from django.contrib.auth.decorators import login_required
from django.shortcuts import render


@login_required
def bonos_ventas_pwa(request):
    return render(request, "bonos_ventas/index.html")
