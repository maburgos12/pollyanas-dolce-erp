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

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Prefetch
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_POST

from core.access import can_view_reportes

from .models import (
    AreaPresupuesto,
    AreaPresupuestoResponsable,
    LineaPresupuestoMensual,
    ReglaFuenteRubro,
    RubroPresupuesto,
)
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
AREAS_NO_SUMABLES = {"nomina", "resultados"}


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
        # Con fuente = automatizado + manual declarado (un rubro MANUAL está
        # resuelto por diseño, no pendiente — ej. CAPEX se captura a mano).
        fila["pct_con_fuente"] = (
            int(round((fila["auto"] + fila["manual"]) * 100 / fila["total"])) if fila["total"] else 0
        )
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
        LineaPresupuestoMensual.objects.filter(
            periodo__year=selected_year, version=selected_version, rubro__activo=True
        )
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
    # Ingresos y egresos NUNCA se suman juntos: un "total general" que los
    # mezcla cuadra al peso pero no significa nada para el negocio (hallazgo
    # de dirección: mayo mostraba $8.9M "presupuestados" = venta + gasto).
    kpis = {
        "ppto_ingresos": ZERO, "real_ingresos": ZERO,
        "ppto_egresos": ZERO, "real_egresos": ZERO,
        "capturado": 0, "pendiente": 0, "retenidos": 0,
    }
    for row in detalle:
        if not selected_area and row["area_codigo"] in AREAS_NO_SUMABLES:
            continue
        es_ingreso = row["tipo"] == RubroPresupuesto.TIPO_INGRESO
        kpis["ppto_ingresos" if es_ingreso else "ppto_egresos"] += row["presupuesto"]
        if row["real"] is not None:
            kpis["real_ingresos" if es_ingreso else "real_egresos"] += row["real"]
            kpis["capturado"] += 1
            if row["sin_datos_fuente"]:
                # Valor retenido de una consolidación previa (fuente sin datos
                # este mes): se suma, pero se advierte que puede estar viejo.
                kpis["retenidos"] += 1
        else:
            kpis["pendiente"] += 1
    kpis["dif_real"] = kpis["real_ingresos"] - kpis["real_egresos"]
    kpis["dif_ppto"] = kpis["ppto_ingresos"] - kpis["ppto_egresos"]

    # ---- ventas por unidades (regla de dirección: unidades × precio actual) --
    ventas_unidades = None
    if not selected_area or selected_area == "ventas":
        from .services_ventas_unidades import comparativo_ventas_unidades

        ventas_unidades = comparativo_ventas_unidades(periodo)

    return {
        "module_tabs": _reportes_module_tabs("presupuesto_vs_real"),
        "ventas_unidades": ventas_unidades,
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


@login_required
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


# ---------------------------------------------------------------------- #
# Captura distribuida por área                                            #
# ---------------------------------------------------------------------- #


def _areas_capturables(user):
    """Áreas donde el usuario puede capturar; None = todas (perfil reportes)."""
    if can_view_reportes(user):
        return None
    codigos = list(
        AreaPresupuestoResponsable.objects.filter(
            usuario=user, puede_capturar=True, area__activa=True
        ).values_list("area__codigo", flat=True)
    )
    if not codigos:
        raise PermissionDenied("No eres responsable de ningún área de presupuesto.")
    return codigos


def _linea_es_capturable(linea: LineaPresupuestoMensual) -> bool:
    """Editable solo lo que NO llena el sistema: sin regla automática activa.

    Una línea consolidada por AUTO nunca se captura a mano (el motor la
    recalcularía); las MANUAL o pendientes sí.
    """
    tiene_regla_auto = any(
        r.activa and r.tipo_fuente != ReglaFuenteRubro.FUENTE_MANUAL
        for r in linea.rubro.reglas_fuente.all()
    )
    return not tiene_regla_auto


def _wants_json(request: HttpRequest) -> bool:
    return (
        "application/json" in request.headers.get("Accept", "")
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    )


def _es_admin_presupuesto(user) -> bool:
    from core.access import can_manage_module

    return bool(user and user.is_authenticated and (user.is_superuser or can_manage_module(user, "reportes")))


@login_required
def presupuesto_real_captura(request: HttpRequest) -> HttpResponse:
    from .views import _reportes_module_tabs

    areas_permitidas = _areas_capturables(request.user)
    es_admin = _es_admin_presupuesto(request.user)

    hoy = timezone.localdate()
    selected_year = max(2020, min(_parse_int(request.GET.get("year"), hoy.year), 2035))
    selected_month = max(1, min(_parse_int(request.GET.get("month"), hoy.month), 12))
    periodo = date(selected_year, selected_month, 1)

    areas_qs = AreaPresupuesto.objects.filter(activa=True).order_by("orden", "nombre")
    if areas_permitidas is not None:
        areas_qs = areas_qs.filter(codigo__in=areas_permitidas)
    areas = list(areas_qs)
    selected_area = normalize_area_code(request.GET.get("area") or "")
    if selected_area not in {a.codigo for a in areas}:
        selected_area = areas[0].codigo if areas else ""

    lineas = (
        LineaPresupuestoMensual.objects.filter(
            periodo=periodo,
            version=LineaPresupuestoMensual.VERSION_ORIGINAL,
            rubro__area__codigo=selected_area,
            rubro__activo=True,
        )
        .select_related("rubro", "rubro__sucursal")
        .prefetch_related("rubro__reglas_fuente")
        .order_by("rubro__concepto", "rubro__sucursal__codigo")
    )

    filas = []
    completos = 0
    for linea in lineas:
        capturable = _linea_es_capturable(linea)
        con_real = linea.monto_real is not None
        if con_real:
            completos += 1
        filas.append(
            {
                "linea": linea,
                "capturable": capturable,
                # Dirección puede sobrescribir un automático (motivo obligatorio)
                # y liberar una captura manual para que vuelva al automático.
                "puede_sobrescribir": es_admin and not capturable,
                "puede_liberar": es_admin and str(linea.fuente_real or "").startswith("MANUAL:"),
                "con_real": con_real,
                "fuente": _fuente_display(linea.fuente_real),
            }
        )

    return render(
        request,
        "reportes/presupuesto_real_captura.html",
        {
            "module_tabs": _reportes_module_tabs("presupuesto_vs_real") if can_view_reportes(request.user) else [],
            "areas": areas,
            "selected_area": selected_area,
            "selected_year": selected_year,
            "selected_month": selected_month,
            "selected_month_name": dict((m, name) for name, m in MONTH_COLUMNS)[selected_month],
            "month_options": MONTH_COLUMNS,
            "filas": filas,
            "completos": completos,
            "total_filas": len(filas),
            "es_admin": es_admin,
        },
    )


@login_required
@require_POST
def presupuesto_real_captura_guardar(request: HttpRequest) -> HttpResponse:
    areas_permitidas = _areas_capturables(request.user)
    wants_json = _wants_json(request)

    def responder(ok: bool, mensaje: str, *, tipo: str = "success", status: int = 200):
        if wants_json:
            return JsonResponse(
                {"ok": ok, "toast": {"type": tipo, "message": mensaje, "persistent": not ok}},
                status=status,
            )
        from django.contrib import messages

        (messages.success if ok else messages.error)(request, mensaje)
        destino = request.POST.get("return_to") or reverse("reportes:presupuesto_real_captura")
        if not destino.startswith("/") or destino.startswith("//"):
            destino = reverse("reportes:presupuesto_real_captura")
        fragmento = f"#linea-{request.POST.get('linea_id', '')}"
        return redirect(destino + fragmento)

    monto_raw = (request.POST.get("monto") or "").strip().replace(",", "").replace("$", "")
    try:
        monto = Decimal(monto_raw).quantize(Decimal("0.01"))
    except Exception:  # noqa: BLE001
        return responder(False, "Captura un monto válido (ej. 1250.50).", tipo="error", status=400)
    if monto < 0:
        return responder(False, "El monto no puede ser negativo.", tipo="error", status=400)

    from django.db import transaction

    # Transacción + bloqueo de fila (recomendación de auditoría): la captura,
    # la consolidación Celery y otro guardado simultáneo no pueden intercalarse.
    with transaction.atomic():
        try:
            linea = (
                LineaPresupuestoMensual.objects.select_for_update()
                .select_related("rubro", "rubro__area")
                .get(pk=_parse_int(request.POST.get("linea_id"), 0))
            )
        except LineaPresupuestoMensual.DoesNotExist:
            return responder(False, "La línea de presupuesto no existe.", tipo="error", status=404)

        if areas_permitidas is not None and linea.rubro.area.codigo not in areas_permitidas:
            return responder(False, "No eres responsable de esta área.", tipo="error", status=403)
        motivo = (request.POST.get("motivo") or "").strip()
        if not _linea_es_capturable(linea):
            # Sobrescribir un automático: solo dirección y con motivo (queda
            # como MANUAL:<usuario>, que el motor respeta — auditoría).
            if not _es_admin_presupuesto(request.user):
                return responder(
                    False,
                    "Este concepto se llena automáticamente desde el sistema; no se captura a mano.",
                    tipo="error",
                    status=409,
                )
            if not motivo:
                return responder(
                    False,
                    "Para sobrescribir un dato automático escribe el motivo (obligatorio).",
                    tipo="error",
                    status=400,
                )

        metadata = dict(linea.metadata or {})
        historial = list(metadata.get("capturas") or [])
        historial.append(
            {
                "usuario": request.user.get_username(),
                "monto": str(monto),
                "anterior": str(linea.monto_real) if linea.monto_real is not None else None,
                "fecha": timezone.now().isoformat(),
                **({"motivo": motivo[:200]} if motivo else {}),
            }
        )
        metadata["capturas"] = historial[-20:]  # historial acotado, nunca se borra lo capturado
        metadata.pop("sin_datos_fuente", None)
        metadata.pop("fuente_sin_datos_en", None)

        linea.monto_real = monto
        linea.fuente_real = f"MANUAL:{request.user.get_username()}"[:100]
        linea.metadata = metadata
        linea.save(update_fields=["monto_real", "fuente_real", "metadata", "actualizado_en"])

    return responder(
        True,
        f"Capturado {linea.rubro.concepto}: ${monto:,.2f}.",
    )


# ---------------------------------------------------------------------- #
# Cédulas IMSS / SIPARE                                                   #
# ---------------------------------------------------------------------- #


def _puede_subir_cedulas(user) -> bool:
    """Administración de reportes, o responsable del área Nómina."""
    from core.access import can_manage_module

    if can_manage_module(user, "reportes"):
        return True
    return AreaPresupuestoResponsable.objects.filter(
        usuario=user, puede_capturar=True, area__codigo="nomina", area__activa=True
    ).exists()


@login_required
def cedula_imss_importar(request: HttpRequest) -> HttpResponse:
    from .views import _reportes_module_tabs

    if not _puede_subir_cedulas(request.user):
        raise PermissionDenied("No tienes permisos para subir cédulas del IMSS.")

    resumen = None
    error = None
    fue_dry_run = False
    if request.method == "POST":
        from .services_cedula_imss import procesar_cedula_subida

        archivo = request.FILES.get("cedula")
        fue_dry_run = bool(request.POST.get("previsualizar"))
        if archivo is None:
            error = "Selecciona el archivo .xls de la cédula (SUA/SIPARE)."
        elif not archivo.name.lower().endswith(".xls"):
            error = "El archivo debe ser el .xls que genera el SUA/SIPARE."
        else:
            try:
                resumen = procesar_cedula_subida(archivo, dry_run=fue_dry_run)
            except ValueError as exc:
                error = str(exc)

    return render(
        request,
        "reportes/cedula_imss_importar.html",
        {
            "module_tabs": _reportes_module_tabs("presupuesto_vs_real")
            if can_view_reportes(request.user)
            else [],
            "resumen": resumen,
            "error": error,
            "fue_dry_run": fue_dry_run,
        },
    )


@login_required
@require_POST
def presupuesto_real_liberar(request: HttpRequest) -> HttpResponse:
    """Devuelve una captura manual al flujo automático (solo dirección).

    Limpia fuente_real: la consolidación nocturna (o la siguiente corrida)
    vuelve a llenar el dato desde su fuente. El historial se conserva.
    """
    from django.contrib import messages
    from django.db import transaction

    if not _es_admin_presupuesto(request.user):
        raise PermissionDenied("Solo dirección puede liberar capturas.")

    wants_json = _wants_json(request)

    def responder(ok: bool, mensaje: str, *, status: int = 200):
        if wants_json:
            return JsonResponse(
                {"ok": ok, "toast": {"type": "success" if ok else "error", "message": mensaje, "persistent": not ok}},
                status=status,
            )
        (messages.success if ok else messages.error)(request, mensaje)
        destino = request.POST.get("return_to") or reverse("reportes:presupuesto_real_captura")
        if not destino.startswith("/") or destino.startswith("//"):
            destino = reverse("reportes:presupuesto_real_captura")
        return redirect(destino + f"#linea-{request.POST.get('linea_id', '')}")

    with transaction.atomic():
        try:
            linea = LineaPresupuestoMensual.objects.select_for_update().select_related("rubro").get(
                pk=_parse_int(request.POST.get("linea_id"), 0)
            )
        except LineaPresupuestoMensual.DoesNotExist:
            return responder(False, "La línea no existe.", status=404)
        if not str(linea.fuente_real or "").startswith("MANUAL:"):
            return responder(False, "Esta línea no tiene captura manual que liberar.", status=409)

        metadata = dict(linea.metadata or {})
        historial = list(metadata.get("capturas") or [])
        historial.append(
            {
                "usuario": request.user.get_username(),
                "accion": "liberado_a_automatico",
                "anterior": str(linea.monto_real) if linea.monto_real is not None else None,
                "fecha": timezone.now().isoformat(),
            }
        )
        metadata["capturas"] = historial[-20:]
        linea.fuente_real = ""
        linea.metadata = metadata
        linea.save(update_fields=["fuente_real", "metadata", "actualizado_en"])

    return responder(True, f"{linea.rubro.concepto}: liberado — el automático lo rellenará en la próxima consolidación.")


# ---------------------------------------------------------------------------
# Estado de resultados (P&L empresa completa)
# ---------------------------------------------------------------------------
# Réplica honesta de la pestaña "GENERAL" del Excel de administración:
# Ingresos − Costos = Utilidad bruta − Egresos por área = Utilidad operativa
# − Inversiones (CAPEX) = Resultado final. A diferencia del Excel, los
# egresos incluyen TODAS las áreas de gasto (no solo administración), y a
# producción se le excluye la materia prima porque ya está en "Costos".

_ER_AREAS_EGRESO = [
    ("gastos-venta", "Gastos de venta"),
    ("administracion", "Administración"),
    ("produccion", "Producción (sin materia prima)"),
    ("logistica", "Logística"),
]


def _er_bucket() -> dict[int, dict[str, object]]:
    return {m: {"ppto": ZERO, "real": ZERO, "con_real": False} for m in range(1, 13)}


def _er_fila(label: str, bucket, kind: str = "linea", area: str = "") -> dict[str, object]:
    meses = []
    anual_ppto = ZERO
    anual_real = ZERO
    hay_real = False
    for m in range(1, 13):
        celda = bucket[m]
        ppto = celda["ppto"]
        real = celda["real"] if celda["con_real"] else None
        anual_ppto += ppto
        if real is not None:
            anual_real += real
            hay_real = True
        meses.append({
            "ppto": ppto,
            "real": real,
            "var": (real - ppto) if real is not None else None,
        })
    return {
        "label": label,
        "kind": kind,
        "area": area,
        "meses": meses,
        "anual_ppto": anual_ppto,
        "anual_real": anual_real if hay_real else None,
        "anual_var": (anual_real - anual_ppto) if hay_real else None,
    }


def _er_resta(bucket_a, bucket_b) -> dict[int, dict[str, object]]:
    """a − b por mes; el real solo existe donde a lo tiene (gate: ingresos)."""
    out = _er_bucket()
    for m in range(1, 13):
        out[m]["ppto"] = bucket_a[m]["ppto"] - bucket_b[m]["ppto"]
        if bucket_a[m]["con_real"]:
            real_b = bucket_b[m]["real"] if bucket_b[m]["con_real"] else ZERO
            out[m]["real"] = bucket_a[m]["real"] - real_b
            out[m]["con_real"] = True
    return out


def _er_suma(*buckets) -> dict[int, dict[str, object]]:
    out = _er_bucket()
    for m in range(1, 13):
        for b in buckets:
            out[m]["ppto"] += b[m]["ppto"]
            if b[m]["con_real"]:
                out[m]["real"] += b[m]["real"]
                out[m]["con_real"] = True
    return out


@login_required
def estado_resultados(request: HttpRequest) -> HttpResponse:
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para ver Reportes.")

    from .views import _reportes_module_tabs  # import tardío: views.py es pesado

    hoy = timezone.localdate()
    selected_year = max(2020, min(_parse_int(request.GET.get("year"), hoy.year), 2035))
    selected_version = normalize_version(request.GET.get("version"))

    rubros_mp = set(
        ReglaFuenteRubro.objects.filter(
            tipo_fuente=ReglaFuenteRubro.FUENTE_CONSUMO_MP, activa=True
        ).values_list("rubro_id", flat=True)
    )
    # La hoja de producción del Excel traía su roll-up "Costo de producción"
    # (idéntico al renglón Costos del P&L) MÁS el detalle por insumo; ambos
    # duplicarían "Costos", así que también se excluyen los rubros cuyo nombre
    # es un insumo de materia prima del catálogo.
    from unidecode import unidecode

    from maestros.models import Insumo

    def _norm(texto: str) -> str:
        return " ".join(unidecode(texto or "").lower().strip().split())

    nombres_mp = set(
        Insumo.objects.filter(
            activo=True, tipo_item=Insumo.TIPO_MATERIA_PRIMA
        ).values_list("nombre_normalizado", flat=True)
    )
    nombres_mp.add("costo de produccion")

    def _es_materia_prima(linea) -> bool:
        return (
            linea.rubro_id in rubros_mp
            or _norm(linea.rubro.concepto) in nombres_mp
        )

    buckets = {clave: _er_bucket() for clave, _ in _ER_AREAS_EGRESO}
    # Las inversiones NO se revuelven (regla de dirección): los proyectos de
    # apertura (rubros "CAPEX <proyecto> ...") se muestran aparte del equipo
    # ordinario (adquisiciones/mesas/hornos de producción).
    buckets.update(
        ingresos=_er_bucket(), costos=_er_bucket(),
        capex_proyectos=_er_bucket(), capex_equipo=_er_bucket(),
    )
    egreso_keys = {clave for clave, _ in _ER_AREAS_EGRESO}
    # Desglose por concepto (pestaña GENERAL del Excel): cada concepto sumado
    # entre sucursales, agrupado por bloque del P&L, para auditar renglón a
    # renglón de dónde sale cada total.
    detalle: dict[tuple[str, str], dict] = {}

    lineas = LineaPresupuestoMensual.objects.filter(
        periodo__year=selected_year, version=selected_version, rubro__activo=True
    ).select_related("rubro", "rubro__area")
    for linea in lineas:
        area = linea.rubro.area.codigo
        if area == "resultados":
            clave = "ingresos" if linea.rubro.tipo == RubroPresupuesto.TIPO_INGRESO else "costos"
        elif area == "produccion" and _es_materia_prima(linea):
            continue  # la materia prima ya está en "Costos" del P&L
        elif area == "capex":
            clave = (
                "capex_proyectos"
                if linea.rubro.concepto.upper().startswith("CAPEX")
                else "capex_equipo"
            )
        elif area in egreso_keys:
            clave = area
        else:
            continue  # ventas por producto ya vive en Ingresos; nómina es control
        celda = buckets[clave][linea.periodo.month]
        celda["ppto"] += linea.monto_presupuesto or ZERO
        if str(linea.fuente_real or "").strip():
            celda["real"] += linea.monto_real or ZERO
            celda["con_real"] = True
        celda_det = detalle.setdefault((clave, linea.rubro.concepto), _er_bucket())[linea.periodo.month]
        celda_det["ppto"] += linea.monto_presupuesto or ZERO
        if str(linea.fuente_real or "").strip():
            celda_det["real"] += linea.monto_real or ZERO
            celda_det["con_real"] = True

    utilidad_bruta = _er_resta(buckets["ingresos"], buckets["costos"])
    egresos_total = _er_suma(*(buckets[clave] for clave in egreso_keys))
    utilidad_operativa = _er_resta(utilidad_bruta, egresos_total)
    inversion_total = _er_suma(buckets["capex_proyectos"], buckets["capex_equipo"])
    resultado_final = _er_resta(utilidad_operativa, inversion_total)

    filas = [
        _er_fila("Ingresos", buckets["ingresos"], area="resultados"),
        _er_fila("Costos", buckets["costos"], area="resultados"),
        _er_fila("Utilidad bruta", utilidad_bruta, kind="total"),
    ]
    filas.extend(
        _er_fila(nombre, buckets[clave], area=clave) for clave, nombre in _ER_AREAS_EGRESO
    )
    filas.append(_er_fila("Utilidad operativa", utilidad_operativa, kind="total"))
    filas.append(_er_fila("Inversión en proyectos (aperturas)", buckets["capex_proyectos"], area="capex"))
    filas.append(_er_fila("Compras de equipo", buckets["capex_equipo"], area="capex"))
    filas.append(_er_fila("Resultado final", resultado_final, kind="total"))

    # ---- desglose por concepto (orden y nombres de los bloques del P&L) ----
    NOMBRES_GRUPO = {
        "ingresos": "Ingresos",
        "costos": "Costos",
        **dict(_ER_AREAS_EGRESO),
        "capex_proyectos": "Inversión en proyectos (aperturas)",
        "capex_equipo": "Compras de equipo",
    }
    orden_grupos = ["ingresos", "costos"] + [c for c, _ in _ER_AREAS_EGRESO] + [
        "capex_proyectos", "capex_equipo",
    ]
    desglose = []
    for grupo in orden_grupos:
        conceptos = [
            _er_fila(concepto, bucket)
            for (g, concepto), bucket in detalle.items()
            if g == grupo
        ]
        if not conceptos:
            continue
        conceptos.sort(key=lambda f: -(f["anual_real"] if f["anual_real"] is not None else ZERO))
        desglose.append({"grupo": NOMBRES_GRUPO[grupo], "conceptos": conceptos})

    por_label = {fila["label"]: fila for fila in filas}
    kpis = {
        "ingresos_real": por_label["Ingresos"]["anual_real"],
        "utilidad_bruta_real": por_label["Utilidad bruta"]["anual_real"],
        "utilidad_operativa_real": por_label["Utilidad operativa"]["anual_real"],
        "resultado_final_real": por_label["Resultado final"]["anual_real"],
    }
    if kpis["ingresos_real"]:
        kpis["margen_operativo_pct"] = (
            (kpis["utilidad_operativa_real"] or ZERO) / kpis["ingresos_real"] * 100
        )
    else:
        kpis["margen_operativo_pct"] = None

    return render(request, "reportes/estado_resultados.html", {
        "module_tabs": _reportes_module_tabs("estado_resultados"),
        "filas": filas,
        "desglose": desglose,
        "kpis": kpis,
        "month_options": MONTH_COLUMNS,
        "selected_year": selected_year,
        "selected_version": selected_version,
        "versions": [v for v, _ in LineaPresupuestoMensual.VERSION_CHOICES],
    })
