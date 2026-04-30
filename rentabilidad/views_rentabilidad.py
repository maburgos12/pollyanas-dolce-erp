"""
sucursales/views_rentabilidad.py

Vista principal del dashboard de rentabilidad.
URL: /sucursales/rentabilidad/
URL detalle: /sucursales/rentabilidad/<pk>/
URL API analizar: POST /sucursales/rentabilidad/<pk>/analizar/
"""

from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required, permission_required
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.db.models import Sum, Avg, Count, Q
from django.utils import timezone
from datetime import date
import calendar

from .models_rentabilidad import SucursalRentabilidad, EstadoRentabilidad
from core.access import can_manage_rentabilidad, can_view_rentabilidad


def _require_view_rentabilidad(user):
    if not can_view_rentabilidad(user):
        raise PermissionDenied("No tienes permisos para ver rentabilidad")


def _require_manage_rentabilidad(user):
    if not can_manage_rentabilidad(user):
        raise PermissionDenied("No tienes permisos para gestionar rentabilidad")


def _get_periodo(request):
    """Lee ?periodo=YYYY-MM o usa el último mes con datos."""
    param = request.GET.get("periodo")
    if param:
        try:
            year, month = map(int, param.split("-"))
            return date(year, month, 1)
        except (ValueError, AttributeError):
            pass
    ultimo = SucursalRentabilidad.objects.order_by("-periodo").first()
    if ultimo:
        return ultimo.periodo
    hoy = date.today()
    return hoy.replace(day=1)


def _colores_estado(estado):
    mapa = {
        EstadoRentabilidad.SUBSIDIADA:   {"bg": "#FEE2E2", "text": "#991B1B", "badge": "#EF4444"},
        EstadoRentabilidad.EQUILIBRIO:   {"bg": "#FEF9C3", "text": "#854D0E", "badge": "#EAB308"},
        EstadoRentabilidad.RECUPERACION: {"bg": "#DBEAFE", "text": "#1E3A5F", "badge": "#3B82F6"},
        EstadoRentabilidad.RENTABLE:     {"bg": "#DCFCE7", "text": "#14532D", "badge": "#22C55E"},
        EstadoRentabilidad.ESTRELLA:     {"bg": "#FEF3C7", "text": "#78350F", "badge": "#C9A84C"},
        EstadoRentabilidad.SIN_DATOS:    {"bg": "#F3F4F6", "text": "#6B7280", "badge": "#9CA3AF"},
    }
    return mapa.get(estado, mapa[EstadoRentabilidad.SIN_DATOS])


@login_required
def dashboard_rentabilidad(request):
    _require_view_rentabilidad(request.user)
    periodo = _get_periodo(request)

    # Periodos disponibles para el selector
    periodos_disponibles = (
        SucursalRentabilidad.objects
        .values_list("periodo", flat=True)
        .distinct()
        .order_by("-periodo")[:24]
    )

    # Registros del periodo seleccionado
    registros = (
        SucursalRentabilidad.objects
        .filter(periodo=periodo)
        .select_related("sucursal")
        .exclude(sucursal=None)
        .order_by("-ventas_brutas")
    )

    # Enriquecer con colores y datos calculados para el template
    sucursales_data = []
    for r in registros:
        colores = _colores_estado(r.estado)
        sucursales_data.append({
            "obj": r,
            "colores": colores,
            "semaforo_margen":  "verde" if r.porcentaje_margen_bruto >= 55 else ("amarillo" if r.porcentaje_margen_bruto >= 40 else "rojo"),
            "semaforo_pe":      "verde" if r.porcentaje_avance_pe >= 100 else ("amarillo" if r.porcentaje_avance_pe >= 85 else "rojo"),
            "semaforo_utilidad":"verde" if r.utilidad_operativa > 0 else "rojo",
            "semaforo_roi":     "verde" if (r.roi_anualizado or 0) >= 25 else ("amarillo" if (r.roi_anualizado or 0) >= 10 else "rojo"),
        })

    totales = {
        "ventas_brutas":      sum(r["obj"].ventas_brutas        for r in sucursales_data),
        "ventas_netas":       sum(r["obj"].ventas_netas         for r in sucursales_data),
        "costo_variable":     sum(r["obj"].costo_variable_total for r in sucursales_data),
        "gasto_fijo":         sum(r["obj"].gasto_fijo_total     for r in sucursales_data),
        "utilidad_operativa": sum(r["obj"].utilidad_operativa   for r in sucursales_data),
    }
    if totales["ventas_netas"]:
        totales["pct_utilidad"] = round(totales["utilidad_operativa"] / totales["ventas_netas"] * 100, 2)
    else:
        totales["pct_utilidad"] = 0

    conteo_estados = {}
    for estado in EstadoRentabilidad:
        conteo_estados[estado.value] = sum(1 for r in sucursales_data if r["obj"].estado == estado)

    # Alertas urgentes (alerta_nivel == 2)
    alertas_urgentes = [r for r in sucursales_data if r["obj"].alerta_nivel == 2]

    context = {
        "periodo":              periodo,
        "periodos_disponibles": periodos_disponibles,
        "sucursales_data":      sucursales_data,
        "totales":              totales,
        "conteo_estados":       conteo_estados,
        "alertas_urgentes":     alertas_urgentes,
        "EstadoRentabilidad":   EstadoRentabilidad,
    }
    return render(request, "rentabilidad/dashboard.html", context)


@login_required
def detalle_sucursal(request, pk):
    _require_view_rentabilidad(request.user)
    rent = get_object_or_404(SucursalRentabilidad, pk=pk)

    # Historial de los últimos 12 meses para gráfica de tendencia
    historial = (
        SucursalRentabilidad.objects
        .filter(sucursal=rent.sucursal, periodo__lte=rent.periodo)
        .order_by("-periodo")[:12]
    )

    context = {
        "rent":     rent,
        "colores":  _colores_estado(rent.estado),
        "historial": list(reversed(list(historial))),
    }
    return render(request, "rentabilidad/detalle.html", context)


@login_required
@require_POST
def analizar_con_ia(request, pk):
    """Endpoint AJAX: lanza el agente IA para una sucursal y devuelve el resultado."""
    _require_manage_rentabilidad(request.user)
    from .agente_rentabilidad import analizar_sucursal

    rent = get_object_or_404(SucursalRentabilidad, pk=pk)
    try:
        resultado = analizar_sucursal(rent, guardar=True)
        return JsonResponse({"ok": True, "resultado": resultado})
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)


@login_required
@require_POST
def analizar_todas(request):
    """Endpoint AJAX: lanza el agente IA para todas las sucursales del periodo."""
    _require_manage_rentabilidad(request.user)
    from .agente_rentabilidad import analizar_todas_sucursales
    periodo = _get_periodo(request)
    resultados = analizar_todas_sucursales(periodo=periodo)
    return JsonResponse({"ok": True, "resultados": resultados})
