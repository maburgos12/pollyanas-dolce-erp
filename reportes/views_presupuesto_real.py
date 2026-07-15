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
    kpis = {"presupuesto": ZERO, "real": ZERO, "capturado": 0, "pendiente": 0, "retenidos": 0}
    for row in detalle:
        if not selected_area and row["area_codigo"] in AREAS_NO_SUMABLES:
            continue
        kpis["presupuesto"] += row["presupuesto"]
        if row["real"] is not None:
            kpis["real"] += row["real"]
            kpis["capturado"] += 1
            if row["sin_datos_fuente"]:
                # Valor retenido de una consolidación previa (fuente sin datos
                # este mes): se suma, pero se advierte que puede estar viejo.
                kpis["retenidos"] += 1
        else:
            kpis["pendiente"] += 1
    kpis["varianza"] = kpis["real"] - kpis["presupuesto"]
    kpis["varianza_pct"] = variance_pct(kpis["varianza"], kpis["presupuesto"])

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
