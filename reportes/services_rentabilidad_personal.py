"""Lectura del costo laboral, sin modificar nóminas ni asignaciones de RRHH.

Las nóminas cerradas o pagadas usan el total oficial de percepciones y la
sucursal vigente del expediente de RRHH, tal como se capturó en el ERP.
"""
from collections import defaultdict
from datetime import date
from decimal import Decimal

from core.models import Sucursal
from rrhh.models import NominaLinea, NominaPeriodo

from .models import LineaPresupuestoMensual
from .services_presupuesto_maestro import normalize_header_text

ZERO = Decimal("0")


def _fila(periodo, *, origen, registro_id, sucursal_id, familia, concepto, monto=ZERO,
          estado="PENDIENTE", detalle="", soporte=""):
    return {
        "origen": origen, "registro_id": registro_id,
        "clave": f"{origen}:{registro_id}:{sucursal_id}:{familia}",
        "sucursal_id": sucursal_id, "area": "gastos-venta", "familia": familia,
        "concepto": concepto, "monto_original": monto, "monto_mensual": monto,
        "cobertura_inicio": periodo, "cobertura_fin": periodo,
        "regla_id": None, "porcentaje": None, "soporte": soporte,
        "estado": estado, "detalle": detalle,
    }


def leer_personal_mensual(periodo: date) -> dict:
    """Importes agregados por sucursal; no devuelve nombres o salarios individuales."""
    periodo = periodo.replace(day=1)
    filas, pendientes = [], []
    importes = defaultdict(lambda: ZERO)
    periodos = defaultdict(set)
    discrepancias = defaultdict(set)
    nominas = (
        NominaLinea.objects.filter(
            periodo__fecha_fin__year=periodo.year, periodo__fecha_fin__month=periodo.month,
            periodo__estatus__in=(NominaPeriodo.ESTATUS_CERRADA, NominaPeriodo.ESTATUS_PAGADA),
        ).select_related("empleado", "periodo").prefetch_related("conceptos")
    )
    for linea in nominas:
        # Producción ya pertenece al costo de fabricación, no a ventas.
        departamento = (linea.empleado.departamento or "").upper()
        if departamento in {"PRODUCCION", "LOGISTICA", "ADMINISTRACION"}:
            continue
        sucursal_id = linea.empleado.sucursal_ref_id
        if departamento != "VENTAS" or sucursal_id is None:
            pendientes.append(_fila(
                periodo, origen="NOMINA", registro_id=linea.periodo_id,
                sucursal_id=None, familia="nomina", concepto="Asignación de nómina",
                detalle="Nómina ERP con personal sin departamento de ventas o sucursal inequívocos; revisar en RRHH.",
            ))
            continue
        # total_percepciones ya incluye sueldo, bonos y prestaciones. El importador
        # lista_raya conserva el total oficial y almacena sus conceptos aparte.
        importes[sucursal_id] += linea.total_percepciones
        periodos[sucursal_id].add(linea.periodo_id)
        percepciones = [c.importe for c in linea.conceptos.all() if c.tipo == "PERCEPCION"]
        if percepciones and sum(percepciones, ZERO) != linea.total_percepciones:
            discrepancias[sucursal_id].add(linea.periodo_id)
    for sucursal_id, monto in sorted(importes.items()):
        ids = sorted(periodos[sucursal_id])
        tiene_discrepancia = bool(discrepancias[sucursal_id])
        fila = _fila(
            periodo, origen="NOMINA", registro_id=ids[0], sucursal_id=sucursal_id,
            familia="nomina", concepto="Nómina ERP · percepciones completas", monto=monto,
            estado="COMPLETO", detalle=(
                "Se usa el total oficial de percepciones de la nómina cerrada o pagada y la "
                "sucursal asignada en RRHH. Incluye sueldo, bonos y prestaciones una sola vez, "
                "antes de descuentos."
                + (" El desglose de conceptos no cuadra con el total oficial; se conserva el total "
                   "de la línea sin sumar los conceptos." if tiene_discrepancia else "")
            ),
        )
        fila["periodos_nomina"] = ids
        filas.append(fila)

    cargas_encontradas = defaultdict(set)
    # El flujo SIPARE ya mensualiza IMSS/RCV. Se lee su resultado ORIGINAL,
    # igual que el consolidado de Reportes; no se vuelve a dividir el bimestre.
    cargas = LineaPresupuestoMensual.objects.filter(
        periodo=periodo, version=LineaPresupuestoMensual.VERSION_ORIGINAL,
        rubro__activo=True, rubro__area__codigo="gastos-venta",
    ).select_related("rubro", "rubro__sucursal")
    for linea in cargas:
        concepto = normalize_header_text(linea.rubro.concepto)
        tipo = "IMSS" if concepto == "imss" else "RCV" if concepto in {"infonavit", "infonavit rcv"} else None
        if tipo is None:
            continue
        sid = linea.rubro.sucursal_id
        meta = linea.metadata or {}
        trazable = (
            linea.fuente_real == "AUTO:SIPARE" and bool(meta.get("cedula_imss"))
        ) or linea.fuente_real.startswith("MANUAL:")
        if sid is None or linea.monto_real is None or not trazable or meta.get("sin_datos_fuente"):
            pendientes.append(_fila(
                periodo, origen="PRESUPUESTO", registro_id=linea.pk, sucursal_id=sid,
                familia="cargas_patronales", concepto=linea.rubro.concepto,
                detalle=f"{tipo}: falta importe patronal vigente, soporte o asignación de sucursal.",
            ))
            continue
        if tipo in cargas_encontradas[sid]:
            pendientes.append(_fila(
                periodo, origen="PRESUPUESTO", registro_id=linea.pk, sucursal_id=sid,
                familia="cargas_patronales", concepto=linea.rubro.concepto,
                detalle=f"Más de un rubro {tipo} en la sucursal; conciliar antes de sumar.",
            ))
            continue
        cargas_encontradas[sid].add(tipo)
        es_sipare = linea.fuente_real == "AUTO:SIPARE"
        filas.append(_fila(
            periodo, origen=linea.fuente_real, registro_id=linea.pk, sucursal_id=sid,
            familia="cargas_patronales", concepto=f"{tipo} · parte patronal mensual",
            monto=linea.monto_real, estado="COMPLETO" if es_sipare else "PARCIAL", detalle=(
                "Importe mensual ya distribuido por Reportes; no incluye retenciones al trabajador."
                if es_sipare else "Captura manual: falta comprobar que corresponde exclusivamente a la parte patronal."
            ),
        ))
    for sid in Sucursal.objects.filter(activa=True).values_list("id", flat=True):
        if sid not in importes:
            pendientes.append(_fila(
                periodo, origen="NOMINA", registro_id=None, sucursal_id=sid, familia="nomina",
                concepto="Nómina ERP", detalle="No hay nómina cerrada o pagada de ventas asignada a esta sucursal en el mes.",
            ))
        for tipo in ("IMSS", "RCV"):
            if tipo not in cargas_encontradas[sid]:
                pendientes.append(_fila(
                    periodo, origen="SIPARE", registro_id=None, sucursal_id=sid,
                    familia="cargas_patronales", concepto=f"{tipo} patronal",
                    detalle=f"Pendiente {tipo}: falta la cédula o su importe patronal mensual trazable.",
                ))
    return {"filas": filas, "pendientes": pendientes}
