"""Reporte de resultados parciales por sucursal; horas extra informativas sin doble suma."""

from __future__ import annotations

from calendar import monthrange
from datetime import date
from decimal import Decimal

from django.db.models import Sum

TWO_PLACES = Decimal("0.01")

SIN_SUCURSAL_KEY = None
SIN_SUCURSAL_NOMBRE = "Sin sucursal asignada"


def _month_add(periodo: date, delta: int) -> date:
    total = periodo.year * 12 + (periodo.month - 1) + delta
    return date(total // 12, total % 12 + 1, 1)


def _quantize(value: Decimal) -> Decimal:
    return (value or Decimal("0")).quantize(TWO_PLACES)


def _pct(numerador: Decimal, denominador: Decimal) -> Decimal | None:
    if not denominador:
        return None
    return (numerador / denominador * Decimal("100")).quantize(TWO_PLACES)


def _yoy(actual: Decimal, anterior: Decimal | None) -> Decimal | None:
    """Variación % vs el mismo mes del año anterior.

    Con base negativa (mes anterior en pérdida) se usa el valor absoluto como
    denominador para que el signo del resultado siga la dirección real del
    cambio (mejora → positivo, deterioro → negativo).
    """
    if anterior is None or anterior == 0:
        return None
    return ((actual - anterior) / abs(anterior) * Decimal("100")).quantize(TWO_PLACES)


def hora_extra_estados_nomina() -> list[str]:
    from rrhh.models import HoraExtra

    return [HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO]


def _rentabilidad_por_mes(desde: date, hasta: date, sucursal_id: int | None) -> dict:
    """{periodo: {sucursal_id: {nombre, ingresos, nomina, utilidad}}}"""
    from rentabilidad.models_rentabilidad import SucursalRentabilidad

    from .services_resultado import registros_del_mes
    from pos_bridge.models.sales import PointDailySale
    periodos = set(SucursalRentabilidad.objects.filter(periodo__gte=desde, periodo__lte=hasta).order_by().values_list("periodo", flat=True))
    fin = date(hasta.year, hasta.month, monthrange(hasta.year, hasta.month)[1])
    periodos.update(PointDailySale.objects.filter(sale_date__range=(desde, fin)).dates("sale_date", "month"))
    data = {}
    for periodo in periodos:
        mes = data.setdefault(periodo, {})
        for row in registros_del_mes(periodo):
            if sucursal_id and row.sucursal_id != sucursal_id:
                continue
            mes[row.sucursal_id] = {
                "nombre": row.sucursal.nombre, "ingresos": _quantize(row.ventas_netas),
                "nomina": _quantize(row.nomina_directa), "utilidad": _quantize(row.utilidad_operativa),
                "central_registrado": getattr(row, "central_registrado", None),
            }
    return data


def _horas_extra_por_mes(desde: date, hasta: date, sucursal_id: int | None) -> dict:
    """{periodo: {sucursal_id|None: monto}} — solo estados autorizado/pagado."""
    from rrhh.models import HoraExtra

    fin_mes = date(hasta.year, hasta.month, monthrange(hasta.year, hasta.month)[1])
    qs = HoraExtra.objects.filter(
        fecha__gte=desde,
        fecha__lte=fin_mes,
        estado__in=hora_extra_estados_nomina(),
    )
    if sucursal_id:
        qs = qs.filter(empleado__sucursal_ref_id=sucursal_id)

    agregado = (
        qs.values("fecha__year", "fecha__month", "empleado__sucursal_ref_id")
        .annotate(total=Sum("monto_calculado"))
        .order_by()
    )
    data: dict[date, dict] = {}
    for fila in agregado:
        periodo = date(fila["fecha__year"], fila["fecha__month"], 1)
        mes = data.setdefault(periodo, {})
        clave = fila["empleado__sucursal_ref_id"]
        mes[clave] = _quantize((mes.get(clave) or Decimal("0")) + (fila["total"] or Decimal("0")))
    return data


def _fila_mes(periodo: date, rent_mes: dict, he_mes: dict) -> dict:
    ingresos = sum((s["ingresos"] for s in rent_mes.values()), Decimal("0"))
    nomina = sum((s["nomina"] for s in rent_mes.values()), Decimal("0"))
    utilidad = sum((s["utilidad"] for s in rent_mes.values()), Decimal("0"))
    horas_extra = sum(he_mes.values(), Decimal("0"))
    nomina_total = nomina

    central = next((s.get("central_registrado") for s in rent_mes.values()), None)
    sucursales = []
    claves = set(rent_mes) | set(he_mes)
    for clave in claves:
        datos = rent_mes.get(clave) or {
            "nombre": SIN_SUCURSAL_NOMBRE if clave is SIN_SUCURSAL_KEY else f"Sucursal {clave}",
            "ingresos": Decimal("0"),
            "nomina": Decimal("0"),
            "utilidad": Decimal("0"),
        }
        he = he_mes.get(clave, Decimal("0"))
        nomina_total_suc = datos["nomina"]
        sucursales.append(
            {
                "sucursal_id": clave,
                "sucursal": datos["nombre"],
                "ingresos": datos["ingresos"],
                "nomina": datos["nomina"],
                "horas_extra": _quantize(he),
                "nomina_total": _quantize(nomina_total_suc),
                "nomina_pct_ventas": _pct(nomina_total_suc, datos["ingresos"]),
                "utilidad_neta": datos["utilidad"],  # Alias legado; alcance explícito en exportación.
                "alcance": "RESULTADO_PARCIAL_SUCURSAL",
            }
        )
    sucursales.sort(key=lambda s: (s["sucursal_id"] is None, s["sucursal"]))

    return {
        "periodo": periodo,
        "ingresos": _quantize(ingresos),
        "nomina": _quantize(nomina),
        "horas_extra": _quantize(horas_extra),
        "nomina_total": _quantize(nomina_total),
        "nomina_pct_ventas": _pct(nomina_total, ingresos),
        "utilidad_neta": _quantize(utilidad),
        "alcance": "RESULTADO_PARCIAL_SUCURSAL",
        "central_registrado": central,
        "resultado_empresa_parcial": utilidad - central if central is not None else None,
        "sucursales": sucursales,
    }


def ultimo_periodo_con_datos() -> date:
    """Último mes con P&L registrado; si no hay datos, el mes anterior al actual."""
    from django.utils import timezone

    from rentabilidad.models_rentabilidad import SucursalRentabilidad

    ultimo = (
        SucursalRentabilidad.objects.order_by("-periodo")
        .values_list("periodo", flat=True)
        .first()
    )
    if ultimo:
        return ultimo.replace(day=1)
    hoy = timezone.localdate()
    return _month_add(hoy.replace(day=1), -1)


def build_reporte_mensual_consolidado(
    *,
    hasta: date | None = None,
    meses: int = 12,
    sucursal_id: int | None = None,
) -> dict:
    """Arma el reporte mensual consolidado de los últimos `meses` meses.

    Regresa dict con `filas` (una por mes, de la más antigua a la más
    reciente); cada fila trae totales consolidados, % de nómina sobre ventas,
    variación YoY y el desglose por sucursal.
    """
    meses = max(1, int(meses))
    hasta = (hasta or ultimo_periodo_con_datos()).replace(day=1)
    desde = _month_add(hasta, -(meses - 1))
    # Se consulta un año extra hacia atrás para poder calcular YoY.
    desde_con_yoy = _month_add(desde, -12)

    rent = _rentabilidad_por_mes(desde_con_yoy, hasta, sucursal_id)
    horas = _horas_extra_por_mes(desde_con_yoy, hasta, sucursal_id)

    filas_por_periodo: dict[date, dict] = {}
    for offset in range(meses + 12):
        periodo = _month_add(desde_con_yoy, offset)
        if periodo > hasta:
            break
        rent_mes = rent.get(periodo, {})
        he_mes = horas.get(periodo, {})
        if not rent_mes and not he_mes:
            continue
        filas_por_periodo[periodo] = _fila_mes(periodo, rent_mes, he_mes)

    filas = []
    for offset in range(meses):
        periodo = _month_add(desde, offset)
        fila = filas_por_periodo.get(periodo) or _fila_mes(periodo, {}, {})
        anterior = filas_por_periodo.get(_month_add(periodo, -12))
        if sucursal_id:
            fila["central_registrado"] = None
            fila["resultado_empresa_parcial"] = None
        fila["yoy"] = {
            "ingresos": _yoy(fila["ingresos"], anterior["ingresos"] if anterior else None),
            "nomina_total": _yoy(fila["nomina_total"], anterior["nomina_total"] if anterior else None),
            "utilidad_neta": _yoy(fila["utilidad_neta"], anterior["utilidad_neta"] if anterior else None),
        }
        filas.append(fila)

    return {
        "hasta": hasta,
        "desde": desde,
        "meses": meses,
        "sucursal_id": sucursal_id,
        "filas": filas,
        "totales": {
            "ingresos": _quantize(sum((f["ingresos"] for f in filas), Decimal("0"))),
            "nomina": _quantize(sum((f["nomina"] for f in filas), Decimal("0"))),
            "horas_extra": _quantize(sum((f["horas_extra"] for f in filas), Decimal("0"))),
            "nomina_total": _quantize(sum((f["nomina_total"] for f in filas), Decimal("0"))),
            "utilidad_neta": _quantize(sum((f["utilidad_neta"] for f in filas), Decimal("0"))),
        },
    }


def reporte_a_json(reporte: dict) -> dict:
    """Versión serializable (str para Decimal, ISO para fechas)."""

    def _num(value):
        return str(value) if value is not None else None

    filas = []
    for fila in reporte["filas"]:
        filas.append(
            {
                "periodo": fila["periodo"].isoformat(),
                "alcance": fila["alcance"],
                "central_registrado": _num(fila["central_registrado"]),
                "resultado_empresa_parcial": _num(fila["resultado_empresa_parcial"]),
                "utilidad_neta_confirmada": None,
                "ingresos": _num(fila["ingresos"]),
                "nomina": _num(fila["nomina"]),
                "horas_extra": _num(fila["horas_extra"]),
                "nomina_total": _num(fila["nomina_total"]),
                "nomina_pct_ventas": _num(fila["nomina_pct_ventas"]),
                "utilidad_neta": _num(fila["utilidad_neta"]),
                "yoy": {k: _num(v) for k, v in fila["yoy"].items()},
                "sucursales": [
                    {
                        "sucursal_id": s["sucursal_id"],
                        "alcance": s["alcance"],
                        "utilidad_neta_confirmada": None,
                        "sucursal": s["sucursal"],
                        "ingresos": _num(s["ingresos"]),
                        "nomina": _num(s["nomina"]),
                        "horas_extra": _num(s["horas_extra"]),
                        "nomina_total": _num(s["nomina_total"]),
                        "nomina_pct_ventas": _num(s["nomina_pct_ventas"]),
                        "utilidad_neta": _num(s["utilidad_neta"]),
                    }
                    for s in fila["sucursales"]
                ],
            }
        )
    return {
        "hasta": reporte["hasta"].isoformat(),
        "desde": reporte["desde"].isoformat(),
        "meses": reporte["meses"],
        "sucursal_id": reporte["sucursal_id"],
        "filas": filas,
        "totales": {k: _num(v) for k, v in reporte["totales"].items()},
        "alcance": "RESULTADO_PARCIAL_SUCURSAL",
        "nota": "utilidad_neta es un alias legado del resultado parcial antes de costos centrales. Las horas extra son informativas; no se suman otra vez a nómina.",
    }
