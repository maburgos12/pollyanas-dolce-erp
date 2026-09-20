"""Lectura del costo laboral, sin modificar nóminas ni asignaciones de RRHH.

Las nóminas cerradas o pagadas usan el total oficial de percepciones y la
sucursal vigente del expediente de RRHH, tal como se capturó en el ERP.
"""
from collections import defaultdict
from datetime import date
from decimal import Decimal
import re

from core.models import Sucursal
from rrhh.models import NominaLinea, NominaPeriodo

from .models import ExpedienteCedulaIMSS, LineaPresupuestoMensual
from .services_presupuesto_maestro import normalize_header_text

ZERO = Decimal("0")
REGISTRO_PATRONAL_AUTORIZADO = "E5240157100"


def _normalizar_registro_patronal(valor) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(valor or "").upper())


def _mes_siguiente(periodo: date) -> date:
    if periodo.month == 12:
        return date(periodo.year + 1, 1, 1)
    return periodo.replace(month=periodo.month + 1)


def _expediente_id(metadata) -> int | None:
    if not isinstance(metadata, dict):
        return None
    try:
        return int(metadata.get("expediente_cedula_imss_id"))
    except (TypeError, ValueError):
        return None


def _metadata_cedula_valida(metadata, tipo_esperado: str, *, exigir_registro: bool) -> bool:
    if not isinstance(metadata, dict):
        return False
    cedula = metadata.get("cedula_imss", {})
    documento = metadata.get("cedula_imss_documento", {})
    if not isinstance(cedula, dict) or not isinstance(documento, dict):
        return False
    if exigir_registro and cedula.get("tipo") != tipo_esperado:
        return False
    if cedula.get("tipo") and cedula["tipo"] != tipo_esperado:
        return False
    registros = {
        _normalizar_registro_patronal(registro)
        for registro in (
            cedula.get("registro_patronal"),
            documento.get("registro_patronal"),
        )
        if registro
    }
    if exigir_registro and not registros:
        return False
    return not registros or registros == {REGISTRO_PATRONAL_AUTORIZADO}


def _expediente_certifica_linea(expediente, *, tipo_esperado: str, periodo: date) -> bool:
    if expediente is None:
        return False
    if expediente.tipo != tipo_esperado:
        return False
    if _normalizar_registro_patronal(expediente.registro_patronal) != REGISTRO_PATRONAL_AUTORIZADO:
        return False
    if tipo_esperado == ExpedienteCedulaIMSS.TIPO_MENSUAL:
        return expediente.periodo == periodo
    return expediente.periodo in {periodo, _mes_siguiente(periodo)}


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
    cargas = list(LineaPresupuestoMensual.objects.filter(
        periodo=periodo, version=LineaPresupuestoMensual.VERSION_ORIGINAL,
        rubro__activo=True, rubro__area__codigo="gastos-venta",
    ).select_related("rubro", "rubro__sucursal"))
    expedientes_enlazados = {
        expediente_id
        for linea in cargas
        if (expediente_id := _expediente_id(linea.metadata)) is not None
    }
    expedientes_aplicados = (
        ExpedienteCedulaIMSS.objects.filter(
            pk__in=expedientes_enlazados,
            estado=ExpedienteCedulaIMSS.ESTADO_APLICADO,
        ).in_bulk()
    )
    for linea in cargas:
        concepto = normalize_header_text(linea.rubro.concepto)
        tipo = "IMSS" if concepto == "imss" else "RCV" if concepto in {"infonavit", "infonavit rcv"} else None
        if tipo is None:
            continue
        sid = linea.rubro.sucursal_id
        meta = linea.metadata or {}
        expediente_id = _expediente_id(meta)
        tipo_esperado = (
            ExpedienteCedulaIMSS.TIPO_MENSUAL
            if tipo == "IMSS"
            else ExpedienteCedulaIMSS.TIPO_BIMESTRAL
        )
        expediente_valido = (
            _expediente_certifica_linea(
                expedientes_aplicados.get(expediente_id),
                tipo_esperado=tipo_esperado,
                periodo=periodo,
            )
            and _metadata_cedula_valida(meta, tipo_esperado, exigir_registro=False)
        )
        legado_verificado = _metadata_cedula_valida(
            meta, tipo_esperado, exigir_registro=True,
        )
        trazable = (
            linea.fuente_real == "AUTO:SIPARE"
            and (
                expediente_valido
                or (expediente_id is None and legado_verificado)
            )
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
