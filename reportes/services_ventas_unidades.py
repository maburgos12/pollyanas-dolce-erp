"""Comparativo de ventas por UNIDADES: proyección mensual vs venta real POS.

Regla de negocio (definida por dirección): la proyección de ventas se compara
en unidades propuestas por producto×mes contra las unidades reales vendidas;
el importe proyectado se calcula con el PRECIO ACTUAL, no con los pesos del
Excel de proyección.

- Unidades proyectadas: ``recetas.PronosticoVenta`` (receta × YYYY-MM).
- Unidades/importe real: ``pos_bridge.PointSalesDailyProductFact`` (FK receta).
- Precio actual por receta: ASP de los últimos 30 días de venta
  (venta_neta/cantidad); si no hubo venta reciente, precio de lista activo de
  ``PointProduct``; si tampoco, ASP del propio mes comparado.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Sum

ZERO = Decimal("0")
VENTANA_PRECIO_DIAS = 30


def _precio_actual_por_receta(hoy: date) -> dict[int, Decimal]:
    """ASP (venta_neta/cantidad) de la ventana reciente, por receta."""
    from pos_bridge.models.sales_pipeline import PointSalesDailyProductFact

    desde = hoy - timedelta(days=VENTANA_PRECIO_DIAS)
    filas = (
        PointSalesDailyProductFact.objects.filter(
            sale_date__gte=desde, sale_date__lte=hoy, receta__isnull=False
        )
        .values("receta_id")
        .annotate(venta=Sum("total_venta_neta"), cantidad=Sum("total_cantidad"))
    )
    precios: dict[int, Decimal] = {}
    for fila in filas:
        cantidad = fila["cantidad"] or ZERO
        if cantidad > 0:
            precios[fila["receta_id"]] = (fila["venta"] / cantidad).quantize(Decimal("0.01"))
    return precios


def _precio_lista_por_receta(receta_ids: list[int]) -> dict[int, Decimal]:
    """Precio de lista activo de PointProduct, vía los productos ligados a receta."""
    from pos_bridge.models.sales_pipeline import PointSalesDailyProductFact

    filas = (
        PointSalesDailyProductFact.objects.filter(
            receta_id__in=receta_ids,
            point_product__isnull=False,
            point_product__precio_activo=True,
            point_product__precio__isnull=False,
        )
        .order_by()
        .values_list("receta_id", "point_product__precio")
        .distinct()
    )
    precios: dict[int, Decimal] = {}
    for receta_id, precio in filas:
        precios.setdefault(receta_id, precio)
    return precios


def comparativo_ventas_unidades(periodo: date, *, hoy: date | None = None) -> dict[str, object]:
    """Filas por producto: unidades proyectadas vs reales y $ a precio actual."""
    from pos_bridge.models.sales_pipeline import PointSalesDailyProductFact
    from recetas.models import PronosticoVenta

    periodo = periodo.replace(day=1)
    hoy = hoy or date.today()
    clave_periodo = f"{periodo.year}-{periodo.month:02d}"

    proyeccion = {
        p.receta_id: p
        for p in PronosticoVenta.objects.filter(periodo=clave_periodo).select_related("receta")
    }
    reales = {
        fila["receta_id"]: fila
        for fila in (
            PointSalesDailyProductFact.objects.filter(
                sale_date__year=periodo.year, sale_date__month=periodo.month, receta__isnull=False
            )
            .values("receta_id")
            .annotate(cantidad=Sum("total_cantidad"), venta=Sum("total_venta"))
        )
    }

    receta_ids = list(proyeccion.keys())
    precios_asp = _precio_actual_por_receta(hoy)
    precios_lista = _precio_lista_por_receta(receta_ids)

    filas = []
    totales = {
        "unidades_proyectadas": ZERO,
        "unidades_reales": ZERO,
        "importe_proyectado": ZERO,
        "importe_real": ZERO,
        "sin_precio": 0,
    }
    for receta_id, pronostico in proyeccion.items():
        real = reales.get(receta_id) or {}
        u_proy = pronostico.cantidad or ZERO
        u_real = real.get("cantidad") or ZERO
        importe_real = real.get("venta") or ZERO

        precio = precios_asp.get(receta_id) or precios_lista.get(receta_id)
        precio_fuente = "ASP 30 días" if receta_id in precios_asp else ("Lista Point" if precio else "")
        if precio is None and u_real > 0:
            precio = (importe_real / u_real).quantize(Decimal("0.01"))
            precio_fuente = "ASP del mes"

        importe_proy = (u_proy * precio).quantize(Decimal("0.01")) if precio is not None else None
        if precio is None:
            totales["sin_precio"] += 1

        cumplimiento = (
            (u_real / u_proy * Decimal("100")).quantize(Decimal("0.1")) if u_proy > 0 else None
        )
        filas.append(
            {
                "receta_id": receta_id,
                "producto": pronostico.receta.nombre,
                "unidades_proyectadas": u_proy,
                "unidades_reales": u_real,
                "cumplimiento_pct": cumplimiento,
                "precio_actual": precio,
                "precio_fuente": precio_fuente,
                "importe_proyectado": importe_proy,
                "importe_real": importe_real,
                "varianza": (importe_real - importe_proy) if importe_proy is not None else None,
                "tone": (
                    "neutral"
                    if cumplimiento is None
                    else ("success" if cumplimiento >= 100 else ("warning" if cumplimiento >= 85 else "danger"))
                ),
            }
        )
        totales["unidades_proyectadas"] += u_proy
        totales["unidades_reales"] += u_real
        if importe_proy is not None:
            totales["importe_proyectado"] += importe_proy
        totales["importe_real"] += importe_real

    filas.sort(key=lambda f: -(f["importe_proyectado"] or ZERO))
    totales["cumplimiento_pct"] = (
        (totales["unidades_reales"] / totales["unidades_proyectadas"] * Decimal("100")).quantize(Decimal("0.1"))
        if totales["unidades_proyectadas"] > 0
        else None
    )

    # ---- Ventas fuera de la proyección (productos nuevos / temporada) ------
    # Trajeron venta real pero nadie los proyectó: deben verse, no perderse.
    fuera = _ventas_fuera_de_proyeccion(periodo, recetas_proyectadas=set(proyeccion.keys()))

    return {
        "periodo": periodo,
        "filas": filas,
        "totales": totales,
        "con_proyeccion": bool(filas),
        "fuera_proyeccion": fuera,
    }


FUERA_PROYECCION_TOP = 15


def _ventas_fuera_de_proyeccion(periodo: date, *, recetas_proyectadas: set[int]) -> dict[str, object]:
    """Ventas del mes de productos SIN unidades proyectadas (nuevos/temporada).

    Incluye productos con receta no proyectada y productos sin receta ligada
    (se agrupan por su nombre histórico de Point). Top N por venta + resto.
    """
    from pos_bridge.models.sales_pipeline import PointSalesDailyProductFact

    filas_raw = (
        PointSalesDailyProductFact.objects.filter(
            sale_date__year=periodo.year, sale_date__month=periodo.month
        )
        .exclude(receta_id__in=recetas_proyectadas)
        .values("receta_id", "receta__nombre", "producto_nombre_historico")
        .annotate(cantidad=Sum("total_cantidad"), venta=Sum("total_venta"))
    )
    agregado: dict[str, dict] = {}
    for fila in filas_raw:
        nombre = fila["receta__nombre"] or fila["producto_nombre_historico"] or "(sin nombre)"
        bucket = agregado.setdefault(nombre, {"producto": nombre, "unidades": ZERO, "venta": ZERO})
        bucket["unidades"] += fila["cantidad"] or ZERO
        bucket["venta"] += fila["venta"] or ZERO

    ordenadas = sorted(agregado.values(), key=lambda f: -f["venta"])
    top = ordenadas[:FUERA_PROYECCION_TOP]
    resto = ordenadas[FUERA_PROYECCION_TOP:]
    total_unidades = sum((f["unidades"] for f in ordenadas), ZERO)
    total_venta = sum((f["venta"] for f in ordenadas), ZERO)
    return {
        "top": top,
        "resto_productos": len(resto),
        "resto_venta": sum((f["venta"] for f in resto), ZERO),
        "total_unidades": total_unidades,
        "total_venta": total_venta,
    }
