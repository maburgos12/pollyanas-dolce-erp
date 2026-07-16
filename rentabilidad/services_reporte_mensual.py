"""
Reporte mensual consolidado (últimos N meses) para las sucursales.

Cruza, mes a mes:
1. Ingresos totales (ventas netas del P&L canónico: SucursalRentabilidad).
2. Nómina del P&L (línea nomina_directa) con HoraExtra (RRHH) como línea aparte.
3. Nómina total (nómina P&L + horas extra) como % de ventas.
4. Utilidad neta del P&L de 6 líneas de gasto (utilidad operativa:
   margen bruto − renta − nómina − servicios − mantenimiento − admin − otros).
5. Variación % contra el mismo mes del año anterior (ingresos, nómina, utilidad).

Fuentes:
- SucursalRentabilidad (rentabilidad histórica / P&L canónico por sucursal-mes).
- rrhh.HoraExtra con estado autorizado o pagado (costo real comprometido);
  la sucursal se resuelve por Empleado.sucursal_ref (FK canónico).

Nota: nomina_directa proviene de GastoOperativoMensual (categoría NOMINA);
las horas extra de RRHH se reportan como línea separada, por lo que si el
importe de nómina importado ya incluyera horas extra, la línea "nómina total"
debe leerse como techo, no como suma contable auditada.
"""

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

    qs = (
        SucursalRentabilidad.objects.filter(periodo__gte=desde, periodo__lte=hasta)
        .exclude(sucursal=None)
        .select_related("sucursal")
    )
    if sucursal_id:
        qs = qs.filter(sucursal_id=sucursal_id)

    data: dict[date, dict] = {}
    for row in qs:
        mes = data.setdefault(row.periodo, {})
        mes[row.sucursal_id] = {
            "nombre": row.sucursal.nombre,
            "ingresos": _quantize(row.ventas_netas),
            "nomina": _quantize(row.nomina_directa),
            "utilidad": _quantize(row.utilidad_operativa),
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
    nomina_total = nomina + horas_extra

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
        nomina_total_suc = datos["nomina"] + he
        sucursales.append(
            {
                "sucursal_id": clave,
                "sucursal": datos["nombre"],
                "ingresos": datos["ingresos"],
                "nomina": datos["nomina"],
                "horas_extra": _quantize(he),
                "nomina_total": _quantize(nomina_total_suc),
                "nomina_pct_ventas": _pct(nomina_total_suc, datos["ingresos"]),
                "utilidad_neta": datos["utilidad"],
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
    }
