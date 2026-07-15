"""Import de cédulas IMSS/SIPARE al presupuesto real, sin captura manual.

Diseño acordado con dirección (2026-07-15):
- La cédula mensual (obrero-patronal) llena los rubros IMSS por área con la
  CUOTA PATRONAL (la parte obrera es retención al trabajador, no gasto).
- La cédula bimestral (Retiro/Cesantía/Infonavit) llena los rubros
  Infonavit (o "Infonavit-RCV" si el área lo tiene) con la parte patronal
  (retiro + cesantía patronal + aportación de vivienda), partida 50/50
  entre los dos meses del bimestre (devengado).
- El cruce es por NSS contra el expediente de RRHH (Empleado.nss) y el
  reparto por departamento/sucursal del empleado.
- Escribe con fuente AUTO:SIPARE: re-subir una cédula corregida re-escribe;
  una captura MANUAL:* jamás se pisa. NSS sin cruce se reportan.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation

from django.db import transaction
from django.utils import timezone

from .models import AreaPresupuesto, LineaPresupuestoMensual, RubroPresupuesto
from .services_presupuesto_maestro import normalize_header_text

FUENTE_SIPARE = "AUTO:SIPARE"

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11,
    "diciembre": 12,
}

# Departamento del empleado → área del presupuesto. Ventas reparte por
# sucursal del empleado; departamentos sin área propia van a administración.
DEPARTAMENTO_A_AREA = {
    "VENTAS": "gastos-venta",
    "PRODUCCION": "produccion",
    "ADMINISTRACION": "administracion",
    "LOGISTICA": "logistica",
}
AREA_RESPALDO = "administracion"

NSS_RE = re.compile(r"^[\d\s-]{11,20}$")


def _nss_digits(valor: object) -> str:
    digitos = re.sub(r"[^0-9]", "", str(valor or ""))
    return digitos if len(digitos) == 11 else ""


def _decimal(valor: object) -> Decimal:
    try:
        return Decimal(str(valor)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


@dataclass
class TrabajadorCedula:
    nss: str
    nombre: str
    patronal: Decimal


@dataclass
class CedulaParseada:
    tipo: str  # MENSUAL | BIMESTRAL
    periodo: date  # mes de proceso (para bimestral: el segundo mes del bimestre)
    registro_patronal: str
    trabajadores: list[TrabajadorCedula] = field(default_factory=list)

    @property
    def meses(self) -> list[date]:
        if self.tipo == "BIMESTRAL":
            # "Bimestre de Proceso: Abril-2026" cubre marzo-abril. Un bimestre
            # solo puede cerrar en mes par; otro valor es un archivo mal
            # formado y se rechaza (no se escribe en diciembre del año previo).
            if self.periodo.month % 2 != 0:
                raise ValueError(
                    f"Bimestre inválido: cierra en {self.periodo:%B-%Y}; "
                    "los bimestres IMSS cierran en mes par (feb/abr/jun/ago/oct/dic)."
                )
            primero = self.periodo.replace(month=self.periodo.month - 1)
            return [primero, self.periodo]
        return [self.periodo]


def cargar_filas_xls(path: str) -> list[list[object]]:
    """Lee la cédula .xls (SUA/SIPARE) como matriz de celdas."""
    import xlrd

    libro = xlrd.open_workbook(path)
    hoja = libro.sheet_by_index(0)
    return [[hoja.cell_value(r, c) for c in range(hoja.ncols)] for r in range(hoja.nrows)]


def parsear_cedula(filas: list[list[object]]) -> CedulaParseada:
    """Convierte la matriz de la cédula en trabajadores con su cuota patronal."""
    tipo = ""
    periodo = None
    registro = ""
    for fila in filas[:16]:
        for celda in fila:
            texto = str(celda or "")
            m = re.search(r"(Per[ií]odo|Bimestre) de Proceso:\s*([A-Za-zÁÉÍÓÚáéíóú]+)-(\d{4})", texto)
            if m:
                tipo = "BIMESTRAL" if m.group(1).lower().startswith("bimestre") else "MENSUAL"
                mes = MESES.get(normalize_header_text(m.group(2)))
                if mes:
                    periodo = date(int(m.group(3)), mes, 1)
            if "Registro Patronal:" in texto:
                idx = fila.index(celda)
                registro = str(fila[idx + 1] if idx + 1 < len(fila) else "").strip() or texto.split(":", 1)[1].strip()
    if not tipo or periodo is None:
        raise ValueError("No se encontró 'Período/Bimestre de Proceso: <Mes>-<Año>' en la cédula.")

    columnas = _columnas_montos(filas, tipo)
    trabajadores: list[TrabajadorCedula] = []
    n = len(filas)
    for i, fila in enumerate(filas):
        nss = _nss_digits(fila[0]) if fila and NSS_RE.match(str(fila[0] or "").strip()) else ""
        if not nss:
            continue
        nombre = next((str(v).strip() for v in fila[1:8] if str(v or "").strip()), "")
        # La fila de montos es la siguiente (hasta +3) con valor NUMÉRICO en
        # TODAS las columnas objetivo (una fila basura o subtotal con texto se
        # rechaza); si aparece otro NSS antes, el trabajador queda sin montos.
        for j in range(i + 1, min(i + 4, n)):
            montos = filas[j]
            if montos and NSS_RE.match(str(montos[0] or "").strip()) and _nss_digits(montos[0]):
                break
            celdas = [montos[c] if c < len(montos) else None for c in columnas.values()]
            if all(isinstance(v, (int, float)) for v in celdas):
                patronal = sum((_decimal(v) for v in celdas), Decimal("0"))
                trabajadores.append(TrabajadorCedula(nss=nss, nombre=nombre, patronal=patronal))
                break

    if not trabajadores:
        raise ValueError("La cédula no trae trabajadores reconocibles (formato inesperado).")
    return CedulaParseada(tipo=tipo, periodo=periodo, registro_patronal=registro, trabajadores=trabajadores)


def _columnas_montos(filas: list[list[object]], tipo: str) -> dict[str, int]:
    """Localiza por ETIQUETA las columnas de cuota patronal (resiliente a
    corrimientos de columnas entre versiones del SUA)."""
    objetivo = (
        {"patronal": ["patronal"]}
        if tipo == "MENSUAL"
        else {"retiro": ["retiro"], "cv_patronal": ["patronal"], "aportacion": ["aportacion pa", "aportación pa"]}
    )
    for fila in filas[:30]:
        etiquetas = [normalize_header_text(v) for v in fila]
        if "clave" not in etiquetas:
            continue
        encontradas: dict[str, int] = {}
        for clave, patrones in objetivo.items():
            for idx, etiqueta in enumerate(etiquetas):
                if any(etiqueta.startswith(p) for p in patrones) and idx not in encontradas.values():
                    encontradas[clave] = idx
                    break
        if len(encontradas) == len(objetivo):
            return encontradas
    raise ValueError(f"No se localizaron las columnas de cuota patronal ({tipo}).")


@dataclass
class ResumenCedula:
    tipo: str
    meses: list[str]
    total_patronal: Decimal
    empleados_cruzados: int
    nss_sin_cruce: list[str]
    lineas_actualizadas: int
    protegidas_manual: int
    avisos: list[str]


def aplicar_cedula(parseada: CedulaParseada, *, dry_run: bool = False) -> ResumenCedula:
    from rrhh.models import Empleado

    por_nss: dict[str, list] = {}
    for e in Empleado.objects.filter(activo=True).select_related("sucursal_ref"):
        digitos = _nss_digits(e.nss)
        if digitos:
            por_nss.setdefault(digitos, []).append(e)
    nss_cedula = {t.nss for t in parseada.trabajadores}
    ambiguos = {nss: emps for nss, emps in por_nss.items() if len(emps) > 1 and nss in nss_cedula}
    if ambiguos:
        detalle = "; ".join(
            f"{nss}: {', '.join(e.codigo or e.nombre for e in emps)}" for nss, emps in ambiguos.items()
        )
        raise ValueError(
            f"NSS duplicados en RRHH — el dinero no puede asignarse sin ambigüedad: {detalle}. "
            "Corrige los expedientes y reintenta."
        )
    indice_nss = {nss: emps[0] for nss, emps in por_nss.items()}

    totales: dict[tuple[str, int | None], Decimal] = {}
    total_cedula = sum((t.patronal for t in parseada.trabajadores), Decimal("0"))
    total_asignado = Decimal("0")
    sin_cruce: list[str] = []
    avisos: list[str] = []
    cruzados = 0
    for trabajador in parseada.trabajadores:
        empleado = indice_nss.get(trabajador.nss)
        if empleado is None:
            sin_cruce.append(f"{trabajador.nss} {trabajador.nombre} (${trabajador.patronal})")
            continue
        cruzados += 1
        area = DEPARTAMENTO_A_AREA.get((empleado.departamento or "").upper())
        if area is None:
            area = AREA_RESPALDO
            avisos.append(
                f"{empleado.nombre}: departamento '{empleado.departamento}' sin área propia → administración"
            )
        sucursal_id = empleado.sucursal_ref_id if area == "gastos-venta" else None
        clave = (area, sucursal_id)
        totales[clave] = totales.get(clave, Decimal("0")) + trabajador.patronal
        total_asignado += trabajador.patronal

    # El área Nómina (vista de control) recibe el total ÍNTEGRO de la cédula
    # (incluye NSS sin cruce): la diferencia contra la suma de áreas queda
    # visible y reportada, nunca se esconde dinero.
    totales[("nomina", None)] = total_cedula
    if total_cedula != total_asignado:
        avisos.append(
            f"${total_cedula - total_asignado} de la cédula quedaron SIN asignar a áreas "
            f"({len(sin_cruce)} NSS sin cruce) — el total de control (Nómina) sí los incluye"
        )

    conceptos_objetivo = (
        ["imss"] if parseada.tipo == "MENSUAL" else ["infonavit rcv", "infonavit"]
    )
    meses = parseada.meses

    actualizadas = 0
    protegidas = 0
    conflictos = 0
    with transaction.atomic():
        # Orden None-safe: una misma área puede mezclar claves con y sin sucursal.
        for (area_codigo, sucursal_id), total in sorted(
            totales.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0)
        ):
            rubro = _rubro_destino(area_codigo, sucursal_id, conceptos_objetivo, avisos)
            if rubro is None:
                continue
            # Reparto que conserva el total al centavo: el primer mes lleva la
            # mitad redondeada y el último la diferencia exacta.
            primera_mitad = (total / Decimal(len(meses))).quantize(Decimal("0.01"))
            montos_mes = [primera_mitad] * (len(meses) - 1) + [total - primera_mitad * (len(meses) - 1)]
            for mes, monto_mes in zip(meses, montos_mes):
                linea, _ = LineaPresupuestoMensual.objects.get_or_create(
                    rubro=rubro,
                    periodo=mes,
                    version=LineaPresupuestoMensual.VERSION_ORIGINAL,
                    defaults={"monto_presupuesto": Decimal("0")},
                )
                fuente_actual = str(linea.fuente_real or "")
                if fuente_actual.startswith("MANUAL:"):
                    protegidas += 1
                    continue
                # Precedencia: SIPARE solo escribe sobre vacío, sobre sí mismo
                # o sobre el legado del Excel (la cédula oficial es más
                # autoritativa que lo tecleado). Otra fuente AUTO = conflicto.
                if fuente_actual and fuente_actual not in (FUENTE_SIPARE, "AUTO:LEGADO"):
                    conflictos += 1
                    avisos.append(
                        f"{rubro} {mes:%Y-%m}: ya lo llena {fuente_actual}; no se pisó (conflicto de fuentes)"
                    )
                    continue
                metadata = dict(linea.metadata or {})
                metadata["cedula_imss"] = {
                    "tipo": parseada.tipo,
                    "registro_patronal": parseada.registro_patronal,
                    "trabajadores": cruzados,
                    "importado_en": timezone.now().isoformat(),
                }
                metadata.pop("sin_datos_fuente", None)
                metadata.pop("fuente_sin_datos_en", None)
                if dry_run:
                    actualizadas += 1
                    continue
                escritas = LineaPresupuestoMensual.objects.filter(
                    pk=linea.pk, fuente_real=linea.fuente_real
                ).update(
                    monto_real=monto_mes,
                    fuente_real=FUENTE_SIPARE,
                    metadata=metadata,
                    actualizado_en=timezone.now(),
                )
                if escritas == 1:
                    actualizadas += 1
                else:
                    conflictos += 1
                    avisos.append(f"{rubro} {mes:%Y-%m}: captura concurrente detectada; no se pisó")
        if dry_run:
            transaction.set_rollback(True)

    return ResumenCedula(
        tipo=parseada.tipo,
        meses=[m.isoformat() for m in meses],
        total_patronal=total_cedula,
        empleados_cruzados=cruzados,
        nss_sin_cruce=sin_cruce,
        lineas_actualizadas=actualizadas,
        protegidas_manual=protegidas,
        avisos=avisos,
    )


def _rubro_destino(
    area_codigo: str, sucursal_id: int | None, conceptos: list[str], avisos: list[str]
) -> RubroPresupuesto | None:
    area = AreaPresupuesto.objects.filter(codigo=area_codigo, activa=True).first()
    if area is None:
        avisos.append(f"área '{area_codigo}' no existe; monto no asignado")
        return None
    candidatos = [
        r
        for r in RubroPresupuesto.objects.filter(area=area, activo=True)
        if normalize_header_text(r.concepto) in conceptos
    ]
    con_sucursal = [r for r in candidatos if r.sucursal_id == sucursal_id]
    if con_sucursal:
        return con_sucursal[0]
    sin_sucursal = [r for r in candidatos if r.sucursal_id is None]
    if sucursal_id is not None and sin_sucursal:
        avisos.append(
            f"área '{area_codigo}': sin rubro {conceptos[0]} para la sucursal {sucursal_id}; "
            "se asignó al rubro general del área"
        )
        return sin_sucursal[0]
    if sin_sucursal:
        return sin_sucursal[0]
    avisos.append(f"área '{area_codigo}': no existe rubro {'/'.join(conceptos)}; monto no asignado")
    return None
