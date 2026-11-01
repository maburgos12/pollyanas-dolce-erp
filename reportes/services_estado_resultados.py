"""Fondos del estado de resultados compartidos con Rentabilidad, sin escrituras."""
from decimal import Decimal
from .models import LineaPresupuestoMensual, ReglaFuenteRubro, RubroPresupuesto

ZERO = Decimal("0")

_ER_AREAS_EGRESO = [
    ("gastos-venta", "Gastos de venta"),
    ("administracion", "Administración"),
    ("produccion", "Producción (sin materia prima)"),
    ("logistica", "Logística"),
]


def _er_bucket() -> dict[int, dict[str, object]]:
    return {m: {"ppto": ZERO, "real": ZERO, "con_real": False} for m in range(1, 13)}


def leer_estado_resultados(selected_year, selected_version):
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

    return buckets, detalle, lineas
