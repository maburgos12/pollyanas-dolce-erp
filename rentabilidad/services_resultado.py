"""Proyección mensual de lectura. Conserva los snapshots y las fuentes operativas."""
from calendar import monthrange
from collections import defaultdict
from datetime import date
from decimal import Decimal

from django.db.models import Sum, Max, Count
from unidecode import unidecode
from pos_bridge.models.sales import PointDailySale
from control.models import MermaMensualSucursal
from reportes.models import LineaPresupuestoMensual, ReglaFuenteRubro
from .services_costos import _costos_recetas_para_periodo, _costos_reventa_para_periodo

ZERO = Decimal("0")
CENTRALES = {"produccion", "logistica", "administracion"}


def leer_resultado_mensual(periodo):
    fin = date(periodo.year, periodo.month, monthrange(periodo.year, periodo.month)[1])
    ventas = PointDailySale.objects.filter(sale_date__range=(periodo, fin))
    filas = list(ventas.values("branch__erp_branch_id", "receta_id", "product_id")
                 .annotate(brutas=Sum("gross_amount"), impuestos=Sum("tax_amount"),
                           descuentos=Sum("discount_amount"), cantidad=Sum("quantity")))
    recetas = _costos_recetas_para_periodo({f["receta_id"] for f in filas if f["receta_id"]}, periodo, fin)
    reventa = _costos_reventa_para_periodo({f["product_id"] for f in filas if not f["receta_id"]}, periodo, fin)
    sucursales = defaultdict(lambda: {"ventas_brutas": ZERO, "descuentos": ZERO,
        "costo_materia_prima": ZERO, "costo_reventa": ZERO, "merma": ZERO, "pendientes": []})
    for fila in filas:
        suc = sucursales[fila["branch__erp_branch_id"]]
        suc["ventas_brutas"] += (fila["brutas"] or ZERO) - (fila["impuestos"] or ZERO)
        suc["descuentos"] += fila["descuentos"] or ZERO
        costo = recetas.get(fila["receta_id"], ZERO) if fila["receta_id"] else reventa.get(fila["product_id"], ZERO)
        campo = "costo_materia_prima" if fila["receta_id"] else "costo_reventa"
        suc[campo] += costo * (fila["cantidad"] or ZERO)
        neto = (fila["brutas"] or ZERO) - (fila["impuestos"] or ZERO) - (fila["descuentos"] or ZERO)
        if neto > 0 and costo * (fila["cantidad"] or ZERO) > neto:
            suc["pendientes"].append(f"Revisar costo/unidad superior a venta: producto {fila['product_id']}")
        if costo <= 0 and fila["cantidad"]:
            suc["pendientes"].append(f"Costo sin confirmar: producto {fila['product_id']}, receta {fila['receta_id']}")
    for merma in MermaMensualSucursal.objects.filter(periodo=periodo):
        suc = sucursales[merma.sucursal_id]
        suc["merma"] += merma.costo_merma
        if merma.unidades_merma and not merma.costo_merma:
            suc["pendientes"].append(f"Merma sin costo: {merma.nombre_producto}")
    # Una única versión por rubro: nunca sumar presupuesto, revisado o consumo MP
    # encima del material de las unidades vendidas y de la merma.
    rubros_mp = set(ReglaFuenteRubro.objects.filter(activa=True, tipo_fuente="CONSUMO_MP").values_list("rubro_id", flat=True))
    from maestros.models import Insumo
    nombres_mp = set(Insumo.objects.filter(activo=True, tipo_item=Insumo.TIPO_MATERIA_PRIMA).values_list("nombre_normalizado", flat=True))
    nombres_mp.add("costo de produccion")
    centrales = []
    for linea in LineaPresupuestoMensual.objects.filter(
        periodo=periodo, version="ORIGINAL", rubro__activo=True,
        rubro__area__codigo__in=CENTRALES, rubro__tipo__in=("COSTO", "EGRESO"),
    ).select_related("rubro__area"):
        meta = linea.metadata or {}
        if " ".join(unidecode(linea.rubro.concepto).lower().split()) in nombres_mp or linea.rubro_id in rubros_mp or "CONSUMO_MP" in linea.fuente_real or any(
            x.get("tipo_fuente") == "CONSUMO_MP" for x in meta.get("real_breakdown", [])
        ):
            continue
        centrales.append({"id": linea.pk, "area": linea.rubro.area.nombre,
            "concepto": linea.rubro.concepto, "monto": linea.monto_real,
            "fuente": linea.fuente_real, "pendiente": linea.monto_real is None or bool(meta.get("sin_datos_fuente"))})
    return {"sucursales": dict(sucursales), "centrales": centrales,
        "central_registrado": sum((f["monto"] for f in centrales if not f["pendiente"]), ZERO),
        "fuente": ventas.aggregate(max_sale_date=Max("sale_date"), rows=Count("id"))}


def aplicar_resultado(rent, resultado):
    suc = resultado["sucursales"].get(rent.sucursal_id)
    rent.en_perimetro_fuente = suc is not None
    rent.costos_pendientes = list(suc["pendientes"]) if suc else ["Sin ventas o merma fuente del mes"]
    for campo in ("ventas_brutas", "descuentos", "costo_materia_prima", "costo_reventa"):
        setattr(rent, campo, (suc[campo] if suc else ZERO).quantize(Decimal("0.01")))
    rent.devoluciones = ZERO  # Point entrega los descuentos en discount_amount.
    rent.merma_registrada = suc["merma"] if suc else ZERO
    rent.otros_costos_variables = rent.merma_registrada
    rent.empaque = ZERO  # El empaque de receta ya forma parte del costo material.
    rent.resultado_integral_pendiente = True
    rent.costos_centrales = resultado["centrales"]
    rent.central_registrado = resultado["central_registrado"]


def registros_del_mes(periodo, costos=None):
    from reportes.services_rentabilidad_mensual import leer_costos_mensuales, aplicar_costos_en_memoria
    from .models_rentabilidad import SucursalRentabilidad
    costos = costos if costos is not None else leer_costos_mensuales(periodo)
    registros = list(SucursalRentabilidad.objects.filter(periodo=periodo).select_related("sucursal"))
    from core.models import Sucursal
    existentes = {r.sucursal_id for r in registros}
    faltantes = set(costos["resultado"]["sucursales"]) - existentes - {None}
    registros.extend(SucursalRentabilidad(sucursal=s, periodo=periodo)
                     for s in Sucursal.objects.filter(pk__in=faltantes))
    for rent in registros:
        aplicar_costos_en_memoria(rent, costos)
    return [r for r in registros if r.en_perimetro_fuente or (r.sucursal.activa and r.gasto_fijo_total)]
