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
from django.db.models import Sum, Avg, Count, Q, Max
from django.utils import timezone
from datetime import date
from decimal import Decimal
import calendar

from .models_rentabilidad import SucursalRentabilidad, EstadoRentabilidad
from core.access import can_manage_rentabilidad, can_view_rentabilidad

# Palabras clave que identifican productos de tipo anticipo/pasivo
# (tarjetas de regalo, vales). Contablemente son un pasivo hasta redención,
# no una venta con margen normal. Se excluyen de alertas de "sin costo".
_ANTICIPO_KEYWORDS = ("tarjeta de regalo", "gift card", "vale de regalo", "e-gift")


def _es_anticipo(nombre: str) -> bool:
    """Retorna True si el nombre del producto corresponde a un anticipo/pasivo."""
    lower = (nombre or "").lower()
    return any(kw in lower for kw in _ANTICIPO_KEYWORDS)


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


def _pct(numerador, denominador):
    if not denominador:
        return Decimal("0")
    return (Decimal(numerador or 0) / Decimal(denominador or 0) * Decimal("100")).quantize(Decimal("0.01"))


def _periodo_bounds(periodo):
    _, ultimo_dia = calendar.monthrange(periodo.year, periodo.month)
    return periodo, date(periodo.year, periodo.month, ultimo_dia)


def _costos_recetas_para_periodo(receta_ids, periodo, fin_periodo):
    if not receta_ids:
        return {}

    from django.db.models import Case, IntegerField, When
    from recetas.models import RecetaCostoVersion
    from reportes.models import RecetaCostoHistoricoMensual

    costos = {}
    historicos = RecetaCostoHistoricoMensual.objects.filter(
        receta_id__in=receta_ids,
        periodo=periodo,
    )
    for historico in historicos:
        costos[historico.receta_id] = Decimal(historico.costo_total or 0)

    recetas_sin_historico = [receta_id for receta_id in receta_ids if receta_id not in costos]
    if recetas_sin_historico:
        versiones = (
            RecetaCostoVersion.objects
            .filter(receta_id__in=recetas_sin_historico, creado_en__date__lte=fin_periodo)
            .annotate(
                fuente_prioridad=Case(
                    When(fuente="POINT_PRODUCTION_REPORT", then=0),
                    When(fuente="POINT_COST_CAPTURE", then=1),
                    When(fuente="POINT_COST_CAPTURE_FIX", then=2),
                    default=3,
                    output_field=IntegerField(),
                )
            )
            .order_by("receta_id", "fuente_prioridad", "-version_num")
        )
        for version in versiones:
            if version.receta_id not in costos:
                costos[version.receta_id] = Decimal(version.costo_total or 0)
    return costos


def _costos_reventa_para_periodo(producto_ids, periodo, fin_periodo):
    if not producto_ids:
        return {}

    from reportes.models import ProductoReventaCosto, ProductoReventaCostoHistoricoMensual

    costos = {}
    historicos = ProductoReventaCostoHistoricoMensual.objects.filter(
        producto_point_id__in=producto_ids,
        periodo=periodo,
    )
    for historico in historicos:
        costos[historico.producto_point_id] = Decimal(historico.costo_promedio or 0)

    productos_sin_historico = [producto_id for producto_id in producto_ids if producto_id not in costos]
    if productos_sin_historico:
        vigentes = (
            ProductoReventaCosto.objects
            .filter(producto_point_id__in=productos_sin_historico, fecha_vigencia__lte=fin_periodo)
            .order_by("producto_point_id", "-fecha_vigencia", "-id")
        )
        for vigente in vigentes:
            if vigente.producto_point_id not in costos:
                costos[vigente.producto_point_id] = Decimal(vigente.costo_unitario or 0)
    return costos


def _build_productos_panel(periodo, fecha_inicio, fecha_fin):
    from pos_bridge.models.sales import PointDailySale

    rows = list(
        PointDailySale.objects
        .filter(sale_date__gte=fecha_inicio, sale_date__lte=fecha_fin)
        .values("product_id", "product__name", "product__category", "receta_id")
        .annotate(
            venta=Sum("gross_amount"),
            cantidad=Sum("quantity"),
            sucursales=Count("branch_id", distinct=True),
        )
        .order_by("-venta")[:80]
    )
    receta_ids = [row["receta_id"] for row in rows if row["receta_id"]]
    producto_reventa_ids = [row["product_id"] for row in rows if row["product_id"] and not row["receta_id"]]
    costos_recetas = _costos_recetas_para_periodo(receta_ids, periodo, fecha_fin)
    costos_reventa = _costos_reventa_para_periodo(producto_reventa_ids, periodo, fecha_fin)

    productos = []
    costo_faltante = 0
    costo_faltante_reventa = 0
    costo_faltante_fabricado = 0
    costo_faltante_anticipo = 0  # tarjetas/vales: pasivo, no alerta de costo
    for row in rows:
        cantidad = Decimal(row["cantidad"] or 0)
        venta = Decimal(row["venta"] or 0)
        costo_unitario = Decimal("0")
        tipo = "Sin clasificar"
        tiene_costo = False
        nombre_producto = row["product__name"] or "Producto sin nombre"
        es_anticipo = _es_anticipo(nombre_producto)

        if row["receta_id"]:
            costo_unitario = costos_recetas.get(row["receta_id"], Decimal("0"))
            tipo = "Fabricado"
            tiene_costo = costo_unitario > 0
        elif row["product_id"]:
            costo_unitario = costos_reventa.get(row["product_id"], Decimal("0"))
            tipo = "Anticipo" if es_anticipo else "Reventa"
            tiene_costo = costo_unitario > 0

        costo_total = (costo_unitario * cantidad).quantize(Decimal("0.01"))
        utilidad = venta - costo_total
        margen = _pct(utilidad, venta)
        if not tiene_costo and venta > 0:
            costo_faltante += 1
            if es_anticipo:
                costo_faltante_anticipo += 1
                # Anticipos no se alertan como "sin costo de adquisición"
            elif tipo == "Reventa":
                costo_faltante_reventa += 1
            elif tipo == "Fabricado":
                costo_faltante_fabricado += 1
        productos.append({
            "nombre": nombre_producto,
            "categoria": row["product__category"] or "Sin categoría",
            "tipo": tipo,
            "es_anticipo": es_anticipo,
            "cantidad": cantidad,
            "venta": venta,
            "costo_unitario": costo_unitario,
            "costo_total": costo_total,
            "utilidad": utilidad,
            "margen": margen,
            "sucursales": row["sucursales"],
            "tiene_costo": tiene_costo,
        })

    return {
        "top_utilidad": sorted(productos, key=lambda row: row["utilidad"], reverse=True)[:12],
        "riesgo_margen": sorted(
            [row for row in productos if row["venta"] > 0],
            key=lambda row: (row["tiene_costo"], row["margen"], -row["venta"]),
        )[:12],
        "costo_faltante": costo_faltante,
        "costo_faltante_reventa": costo_faltante_reventa,
        "costo_faltante_fabricado": costo_faltante_fabricado,
        "costo_faltante_anticipo": costo_faltante_anticipo,
        "productos_revisados": len(productos),
    }


def _build_gastos_panel(periodo):
    from reportes.models import GastoOperativoMensual

    gastos = (
        GastoOperativoMensual.objects
        .filter(periodo=periodo, tipo_dato="REAL")
        .select_related("centro_costo", "categoria_gasto")
    )
    por_categoria = (
        gastos
        .values("categoria_gasto__codigo", "categoria_gasto__nombre")
        .annotate(total=Sum("monto"), registros=Count("id"))
        .order_by("-total")
    )
    por_sucursal = (
        gastos
        .filter(centro_costo__sucursal__isnull=False)
        .values("centro_costo__sucursal__nombre")
        .annotate(total=Sum("monto"), registros=Count("id"))
        .order_by("-total")
    )
    return {
        "total": gastos.aggregate(t=Sum("monto"))["t"] or Decimal("0"),
        "registros": gastos.count(),
        "por_categoria": list(por_categoria[:16]),
        "por_sucursal": list(por_sucursal[:12]),
    }


def _build_alertas_panel(sucursales_data, productos_panel, gastos_panel, max_sale_date, periodo):
    alertas = []
    for item in sucursales_data:
        r = item["obj"]
        if r.ventas_netas > 0 and r.gasto_fijo_total == 0:
            alertas.append({
                "nivel": "alto",
                "titulo": f"{r.sucursal.nombre}: sin gastos reales",
                "detalle": "Tiene ventas en el periodo, pero gasto fijo total en cero. Revisar captura/importación de gastos.",
            })
        if r.ventas_netas > 0 and r.porcentaje_margen_bruto < 40:
            alertas.append({
                "nivel": "alto",
                "titulo": f"{r.sucursal.nombre}: margen bruto bajo",
                "detalle": f"Margen bruto {r.porcentaje_margen_bruto}%. Revisar mezcla de venta, costos de producción/reventa y descuentos.",
            })
        if r.punto_equilibrio_mensual > 0 and r.porcentaje_avance_pe < 100:
            alertas.append({
                "nivel": "medio",
                "titulo": f"{r.sucursal.nombre}: debajo del punto de equilibrio",
                "detalle": f"Avance PE {r.porcentaje_avance_pe}%. Faltan ${r.brecha_pe.quantize(Decimal('0.01')):,.2f} aprox. para cubrir estructura fija.",
            })

    if productos_panel["costo_faltante_reventa"]:
        n = productos_panel["costo_faltante_reventa"]
        n_anticipo = productos_panel["costo_faltante_anticipo"]
        nota_anticipo = (
            f" ({n_anticipo} tarjeta{'s' if n_anticipo > 1 else ''} de regalo excluida{'s' if n_anticipo > 1 else ''} — son anticipos/pasivos)"
            if n_anticipo else ""
        )
        alertas.append({
            "nivel": "alto",
            "titulo": f"{n} producto{'s' if n > 1 else ''} de reventa sin costo de adquisición",
            "detalle": (
                f"{n} productos de reventa vendidos no tienen costo en ProductoReventaCosto "
                f"(velas, pirotecnia, refrescos, decorativos, bebidas, etc.). "
                f"Capturar costo real de factura/proveedor en /maestros/costos-adquisicion/.{nota_anticipo}"
            ),
        })
    if productos_panel["costo_faltante_fabricado"]:
        n = productos_panel["costo_faltante_fabricado"]
        alertas.append({
            "nivel": "alto",
            "titulo": f"{n} producto{'s' if n > 1 else ''} fabricado{'s' if n > 1 else ''} sin costo de receta",
            "detalle": (
                f"{n} productos fabricados del ranking no tienen RecetaCostoVersion vigente. "
                f"Revisar costeo de recetas."
            ),
        })
    if gastos_panel["registros"] == 0:
        alertas.append({
            "nivel": "medio",
            "titulo": "Sin gastos reales cargados",
            "detalle": "El periodo no tiene registros REAL en GastoOperativoMensual; el punto de equilibrio puede aparecer en cero.",
        })
    if max_sale_date and max_sale_date < periodo:
        alertas.append({
            "nivel": "alto",
            "titulo": "Ventas fuente fuera de periodo",
            "detalle": "La última venta fuente es anterior al periodo seleccionado. Revisar sync de Point.",
        })
    return alertas


@login_required
def dashboard_rentabilidad(request):
    _require_view_rentabilidad(request.user)
    periodo = _get_periodo(request)
    active_tab = request.GET.get("tab", "resumen")
    tabs_validos = {"resumen", "diagnostico", "productos", "gastos", "alertas"}
    if active_tab not in tabs_validos:
        active_tab = "resumen"
    fecha_inicio, fecha_fin = _periodo_bounds(periodo)

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
        costo_variable_pct = _pct(r.costo_variable_total, r.ventas_netas)
        gasto_fijo_pct = _pct(r.gasto_fijo_total, r.ventas_netas)
        sucursales_data.append({
            "obj": r,
            "colores": colores,
            "semaforo_margen":  "verde" if r.porcentaje_margen_bruto >= 55 else ("amarillo" if r.porcentaje_margen_bruto >= 40 else "rojo"),
            "semaforo_pe":      "verde" if r.porcentaje_avance_pe >= 100 else ("amarillo" if r.porcentaje_avance_pe >= 85 else "rojo"),
            "semaforo_utilidad":"verde" if r.utilidad_operativa > 0 else "rojo",
            "semaforo_roi":     "verde" if (r.roi_anualizado or 0) >= 25 else ("amarillo" if (r.roi_anualizado or 0) >= 10 else "rojo"),
            "costo_variable_pct": costo_variable_pct,
            "gasto_fijo_pct": gasto_fijo_pct,
            "peso_venta": Decimal("0"),
        })

    totales = {
        "ventas_brutas":      sum(r["obj"].ventas_brutas        for r in sucursales_data),
        "ventas_netas":       sum(r["obj"].ventas_netas         for r in sucursales_data),
        "costo_variable":     sum(r["obj"].costo_variable_total for r in sucursales_data),
        "gasto_fijo":         sum(r["obj"].gasto_fijo_total     for r in sucursales_data),
        "utilidad_operativa": sum(r["obj"].utilidad_operativa   for r in sucursales_data),
        "costo_produccion":   sum(r["obj"].costo_materia_prima  for r in sucursales_data),
        "costo_reventa":      sum(r["obj"].costo_reventa        for r in sucursales_data),
        "empaque":            sum(r["obj"].empaque              for r in sucursales_data),
    }
    if totales["ventas_netas"]:
        totales["pct_utilidad"] = round(totales["utilidad_operativa"] / totales["ventas_netas"] * 100, 2)
        totales["pct_margen_bruto"] = round((totales["ventas_netas"] - totales["costo_variable"]) / totales["ventas_netas"] * 100, 2)
        totales["pct_costo_variable"] = round(totales["costo_variable"] / totales["ventas_netas"] * 100, 2)
        totales["pct_gasto_fijo"] = round(totales["gasto_fijo"] / totales["ventas_netas"] * 100, 2)
    else:
        totales["pct_utilidad"] = 0
        totales["pct_margen_bruto"] = 0
        totales["pct_costo_variable"] = 0
        totales["pct_gasto_fijo"] = 0

    for item in sucursales_data:
        item["peso_venta"] = _pct(item["obj"].ventas_netas, totales["ventas_netas"])

    conteo_estados = {}
    for estado in EstadoRentabilidad:
        conteo_estados[estado.value] = sum(1 for r in sucursales_data if r["obj"].estado == estado)

    # Alertas urgentes (alerta_nivel == 2)
    alertas_urgentes = [r for r in sucursales_data if r["obj"].alerta_nivel == 2]

    from pos_bridge.models.sales import PointDailySale
    ventas_fuente = PointDailySale.objects.filter(sale_date__gte=fecha_inicio, sale_date__lte=fecha_fin)
    fuente_estado = {
        "max_sale_date": ventas_fuente.aggregate(max_date=Max("sale_date"))["max_date"],
        "rows": ventas_fuente.count(),
        "total": ventas_fuente.aggregate(total=Sum("gross_amount"))["total"] or Decimal("0"),
        "rentabilidad_total": totales["ventas_brutas"],
        "diferencia": (ventas_fuente.aggregate(total=Sum("gross_amount"))["total"] or Decimal("0")) - totales["ventas_brutas"],
        "max_calculado_en": registros.aggregate(max_calc=Max("calculado_en"))["max_calc"],
    }
    fuente_estado["cuadra"] = abs(fuente_estado["diferencia"]) < Decimal("1.00")

    diagnostico = {
        "ranking_margen": sorted(sucursales_data, key=lambda item: item["obj"].porcentaje_margen_bruto),
        "ranking_utilidad": sorted(sucursales_data, key=lambda item: item["obj"].utilidad_operativa, reverse=True),
        "ranking_pe": sorted(sucursales_data, key=lambda item: item["obj"].porcentaje_avance_pe),
        "interpretacion": [
            {
                "titulo": "Margen bruto",
                "valor": f"{totales['pct_margen_bruto']}%",
                "detalle": "Mide cuánto queda después de producción, reventa y empaque.",
            },
            {
                "titulo": "Carga fija",
                "valor": f"{totales['pct_gasto_fijo']}%",
                "detalle": "Parte de la venta absorbida por renta, nómina, servicios y estructura.",
            },
            {
                "titulo": "Resultado operativo",
                "valor": f"{totales['pct_utilidad']}%",
                "detalle": "Utilidad después de costo variable y gasto fijo.",
            },
        ],
    }
    productos_panel = _build_productos_panel(periodo, fecha_inicio, fecha_fin)
    gastos_panel = _build_gastos_panel(periodo)
    alertas_panel = _build_alertas_panel(
        sucursales_data,
        productos_panel,
        gastos_panel,
        fuente_estado["max_sale_date"],
        periodo,
    )

    context = {
        "periodo":              periodo,
        "active_tab":           active_tab,
        "periodos_disponibles": periodos_disponibles,
        "sucursales_data":      sucursales_data,
        "totales":              totales,
        "conteo_estados":       conteo_estados,
        "alertas_urgentes":     alertas_urgentes,
        "fuente_estado":        fuente_estado,
        "diagnostico":          diagnostico,
        "productos_panel":      productos_panel,
        "gastos_panel":         gastos_panel,
        "alertas_panel":        alertas_panel,
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
