"""Tablero Presupuesto vs Real — comparativo por área/sucursal/mes.

Lee ``LineaPresupuestoMensual`` (presupuesto + real consolidado por
``PresupuestoRealConsolidacionService``) y presenta la matriz área×mes,
el detalle por rubro con semáforos y el estado de cobertura del mapeo.
"""

from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal
from io import BytesIO

from django.core.exceptions import PermissionDenied
from django.db.models import Prefetch
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils import timezone

from core.access import can_view_reportes

from .models import AreaPresupuesto, LineaPresupuestoMensual, ReglaFuenteRubro, RubroPresupuesto
from .services_budget_vs_actual import budget_tone, variance_pct
from .services_presupuesto_maestro import (
    MONTH_COLUMNS,
    normalize_area_code,
    normalize_version,
)

ZERO = Decimal("0")

# El área "Nómina" replica los sueldos que ya viven dentro de cada área
# (Gastos de Venta, Administración, Logística, Producción). Se muestra como
# vista de control pero NO se suma a los KPI globales para no duplicar dinero.
AREAS_NO_SUMABLES = {"nomina"}


def _parse_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _fuente_display(fuente_real: str) -> dict[str, str]:
    """Traduce el namespace de fuente_real a un badge legible."""
    valor = str(fuente_real or "").strip()
    if valor.startswith("MANUAL:"):
        usuario = valor.split(":", 1)[1]
        return {"kind": "manual", "label": f"Capturado · {usuario}"}
    if valor.startswith("AUTO:"):
        fuentes = valor.split(":", 1)[1]
        etiquetas = {
            "GASTO_OPERATIVO": "Gasto operativo",
            "NOMINA": "Nómina",
            "VENTA_POS": "Ventas Point",
            "BONO_PRODUCCION": "Bonos producción",
            "BONO_VENTAS": "Bonos ventas",
            "CONSUMO_MP": "Consumo MP",
            "LEGADO": "Importado",
        }
        partes = [etiquetas.get(parte, parte) for parte in fuentes.split("+")]
        return {"kind": "auto", "label": "Automático · " + " + ".join(partes)}
    if valor:
        return {"kind": "otro", "label": valor}
    return {"kind": "pendiente", "label": "Pendiente"}


def _cobertura_por_area() -> list[dict[str, object]]:
    """Rubros activos por área: automatizados, manuales explícitos y sin mapear."""
    rubros = (
        RubroPresupuesto.objects.filter(activo=True)
        .select_related("area")
        .prefetch_related(
            Prefetch("reglas_fuente", queryset=ReglaFuenteRubro.objects.filter(activa=True))
        )
    )
    acumulado: dict[str, dict[str, object]] = {}
    for rubro in rubros:
        bucket = acumulado.setdefault(
            rubro.area.codigo,
            {"area": rubro.area.nombre, "codigo": rubro.area.codigo, "orden": rubro.area.orden,
             "total": 0, "auto": 0, "manual": 0, "sin_mapear": 0},
        )
        bucket["total"] += 1
        reglas = list(rubro.reglas_fuente.all())
        if any(r.tipo_fuente != ReglaFuenteRubro.FUENTE_MANUAL for r in reglas):
            bucket["auto"] += 1
        elif reglas:
            bucket["manual"] += 1
        else:
            bucket["sin_mapear"] += 1
    filas = sorted(acumulado.values(), key=lambda item: item["orden"])
    for fila in filas:
        fila["pct_auto"] = int(round(fila["auto"] * 100 / fila["total"])) if fila["total"] else 0
    return filas


def _build_context(request: HttpRequest) -> dict[str, object]:
    from .views import _reportes_module_tabs  # import tardío: views.py es pesado

    hoy = timezone.localdate()
    selected_year = max(2020, min(_parse_int(request.GET.get("year"), hoy.year), 2035))
    selected_month = max(1, min(_parse_int(request.GET.get("month"), hoy.month), 12))
    selected_version = normalize_version(request.GET.get("version"))
    selected_area = normalize_area_code(request.GET.get("area") or "")
    periodo = date(selected_year, selected_month, 1)

    lineas = (
        LineaPresupuestoMensual.objects.filter(periodo__year=selected_year, version=selected_version)
        .select_related("rubro", "rubro__area", "rubro__sucursal")
        .order_by("rubro__area__orden", "rubro__concepto", "rubro__sucursal__codigo")
    )

    # ---- matriz área × mes -------------------------------------------------
    matriz: dict[str, dict[str, object]] = {}
    detalle: list[dict[str, object]] = []
    for linea in lineas:
        area = linea.rubro.area
        fila = matriz.setdefault(
            area.codigo,
            {
                "area": area.nombre,
                "codigo": area.codigo,
                "orden": area.orden,
                "meses": {m: {"presupuesto": ZERO, "real": ZERO, "con_real": False} for _, m in MONTH_COLUMNS},
                "anual_presupuesto": ZERO,
                "anual_real": ZERO,
            },
        )
        mes = linea.periodo.month
        celda = fila["meses"][mes]
        celda["presupuesto"] += linea.monto_presupuesto or ZERO
        fila["anual_presupuesto"] += linea.monto_presupuesto or ZERO
        if linea.monto_real is not None:
            celda["real"] += linea.monto_real
            celda["con_real"] = True
            fila["anual_real"] += linea.monto_real

        # ---- detalle del mes seleccionado ---------------------------------
        if linea.periodo == periodo and (not selected_area or area.codigo == selected_area):
            presupuesto = linea.monto_presupuesto or ZERO
            real = linea.monto_real
            varianza = (real - presupuesto) if real is not None else None
            metadata = linea.metadata or {}
            detalle.append(
                {
                    "area": area.nombre,
                    "area_codigo": area.codigo,
                    "concepto": linea.rubro.concepto,
                    "sucursal": linea.rubro.sucursal.codigo if linea.rubro.sucursal_id else "",
                    "tipo": linea.rubro.tipo,
                    "presupuesto": presupuesto,
                    "real": real,
                    "varianza": varianza,
                    "varianza_pct": variance_pct(varianza, presupuesto) if varianza is not None else None,
                    "tone": budget_tone(linea.rubro.tipo, varianza) if varianza is not None else "neutral",
                    "fuente": _fuente_display(linea.fuente_real),
                    "sin_datos_fuente": bool(metadata.get("sin_datos_fuente")),
                    "breakdown": metadata.get("real_breakdown") or [],
                }
            )

    filas_matriz = sorted(matriz.values(), key=lambda item: item["orden"])
    for fila in filas_matriz:
        fila["meses_lista"] = [
            {"mes": m, **fila["meses"][m]} for _, m in MONTH_COLUMNS
        ]
        celda = fila["meses"][selected_month]
        fila["mes_presupuesto"] = celda["presupuesto"]
        fila["mes_real"] = celda["real"] if celda["con_real"] else None
        varianza = (celda["real"] - celda["presupuesto"]) if celda["con_real"] else None
        fila["mes_varianza"] = varianza
        fila["mes_varianza_pct"] = variance_pct(varianza, celda["presupuesto"]) if varianza is not None else None

    # ---- KPIs del mes ------------------------------------------------------
    # Sin área seleccionada, las áreas de control (Nómina) no se suman: sus
    # sueldos ya están dentro de las demás áreas y duplicarían el total.
    kpis = {"presupuesto": ZERO, "real": ZERO, "capturado": 0, "pendiente": 0}
    for row in detalle:
        if not selected_area and row["area_codigo"] in AREAS_NO_SUMABLES:
            continue
        kpis["presupuesto"] += row["presupuesto"]
        if row["real"] is not None:
            kpis["real"] += row["real"]
            kpis["capturado"] += 1
        else:
            kpis["pendiente"] += 1
    kpis["varianza"] = kpis["real"] - kpis["presupuesto"]
    kpis["varianza_pct"] = variance_pct(kpis["varianza"], kpis["presupuesto"])

    return {
        "module_tabs": _reportes_module_tabs("presupuesto_vs_real"),
        "areas": AreaPresupuesto.objects.filter(activa=True).order_by("orden", "nombre"),
        "versions": [v for v, _ in LineaPresupuestoMensual.VERSION_CHOICES],
        "month_options": MONTH_COLUMNS,
        "selected_year": selected_year,
        "selected_month": selected_month,
        "selected_month_name": dict((m, name) for name, m in MONTH_COLUMNS)[selected_month],
        "selected_version": selected_version,
        "selected_area": selected_area,
        "matriz": filas_matriz,
        "detalle": detalle,
        "kpis": kpis,
        "cobertura": _cobertura_por_area(),
    }


EXPORT_HEADERS = [
    "Área", "Concepto", "Sucursal", "Tipo", "Presupuesto", "Real", "Varianza", "Varianza %", "Fuente",
]


def _celda_segura(valor: object) -> str:
    """Neutraliza inyección de fórmulas en hojas de cálculo (=, +, -, @, tab, CR)."""
    texto = str(valor or "")
    if texto.startswith(("=", "+", "-", "@", "\t", "\r")):
        return "'" + texto
    return texto


def _export_rows(detalle: list[dict[str, object]]) -> list[list[object]]:
    rows = []
    for row in detalle:
        rows.append(
            [
                _celda_segura(row["area"]),
                _celda_segura(row["concepto"]),
                _celda_segura(row["sucursal"]),
                _celda_segura(row["tipo"]),
                f"{row['presupuesto']:.2f}",
                f"{row['real']:.2f}" if row["real"] is not None else "",
                f"{row['varianza']:.2f}" if row["varianza"] is not None else "",
                f"{row['varianza_pct']:.2f}" if row["varianza_pct"] is not None else "",
                _celda_segura(row["fuente"]["label"]),
            ]
        )
    return rows


def _export_csv(context: dict[str, object]) -> HttpResponse:
    filename = f"presupuesto_vs_real_{context['selected_year']}-{context['selected_month']:02d}.csv"
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    writer = csv.writer(response)
    writer.writerow(EXPORT_HEADERS)
    writer.writerows(_export_rows(context["detalle"]))
    return response


def _export_xlsx(context: dict[str, object]) -> HttpResponse:
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = f"{context['selected_year']}-{context['selected_month']:02d}"
    sheet.append(EXPORT_HEADERS)
    for row in _export_rows(context["detalle"]):
        sheet.append(row)
    buffer = BytesIO()
    workbook.save(buffer)
    filename = f"presupuesto_vs_real_{context['selected_year']}-{context['selected_month']:02d}.xlsx"
    response = HttpResponse(
        buffer.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def presupuesto_vs_real(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")

    context = _build_context(request)
    export = (request.GET.get("export") or "").strip().lower()
    if export == "csv":
        return _export_csv(context)
    if export == "xlsx":
        return _export_xlsx(context)
    return render(request, "reportes/presupuesto_vs_real.html", context)
