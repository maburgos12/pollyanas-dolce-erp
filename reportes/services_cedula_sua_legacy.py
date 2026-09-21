"""Recuperación acotada de las dos cédulas SUA truncadas de febrero de 2026.

No modifica los bytes de origen. Reconstruye etiquetas solo en memoria y exige
que detalle, pie de cédula y resumen SUA concilien antes de permitir un expediente.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from .services_cedula_imss import CedulaParseada, _nss_digits, parsear_cedula
from .services_presupuesto_maestro import normalize_header_text


@dataclass(frozen=True)
class RecuperacionSUA:
    filas_validadas: list[list[object]]
    parseada: CedulaParseada
    total_patronal: Decimal


def _texto(fila: list[object]) -> str:
    return normalize_header_text(" ".join(str(valor or "") for valor in fila))


def _importe(valor: object) -> Decimal:
    if isinstance(valor, bool) or not isinstance(valor, (int, float, Decimal)):
        raise ValueError("Resumen SUA sin importe numérico inequívoco.")
    try:
        return Decimal(str(valor)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Resumen SUA sin importe numérico inequívoco.") from exc


def _fila_unica(filas: list[list[object]], frase: str) -> list[object]:
    candidatas = [fila for fila in filas if frase in _texto(fila)]
    if len(candidatas) != 1:
        raise ValueError(f"Resumen SUA sin etiqueta única: {frase}.")
    return candidatas[0]


def _total_seccion(
    filas: list[list[object]], inicio: str, fin: str | None
) -> Decimal:
    comienzos = [i for i, fila in enumerate(filas) if inicio in _texto(fila)]
    if len(comienzos) != 1:
        raise ValueError(f"Resumen SUA sin sección única: {inicio}.")
    desde = comienzos[0] + 1
    fin_compacto = re.sub(r"\s+", "", fin or "")
    hasta = next(
        (
            i for i in range(desde, len(filas))
            if fin and (
                fin in _texto(filas[i])
                or fin_compacto in re.sub(r"\s+", "", _texto(filas[i]))
            )
        ),
        len(filas),
    )
    if fin and hasta == len(filas):
        raise ValueError(f"Resumen SUA sin cierre de sección: {inicio}.")
    totales = [
        _importe(fila[7]) for fila in filas[desde:hasta]
        if len(fila) > 7 and re.sub(r"\s+", "", _texto(fila)).startswith("total")
        and isinstance(fila[7], (int, float, Decimal))
    ]
    if len(totales) != 1:
        raise ValueError(f"Resumen SUA sin total único en sección: {inicio}.")
    return totales[0]


def _importe_etiqueta(filas: list[list[object]], frase: str) -> Decimal:
    fila = _fila_unica(filas, frase)
    if len(fila) <= 5:
        raise ValueError(f"Resumen SUA sin importe: {frase}.")
    return _importe(fila[5])


def recuperar_febrero_sin_encabezado(
    filas: list[list[object]], resumen: list[list[object]]
) -> RecuperacionSUA:
    """Acepta únicamente los layouts comprobados de febrero de 2026."""
    mes = _texto(_fila_unica(resumen, "mes de proceso"))
    bimestre = _texto(_fila_unica(resumen, "bimestre de proceso"))
    registro_fila = " ".join(str(v or "") for v in _fila_unica(resumen, "registro patronal"))
    registros = set(re.findall(r"[A-Z]\d{2}-\d{5}-\d{2}-\d", registro_fila.upper()))
    if (
        re.search(r"\bfebrero\s+2026\b", mes) is None
        or re.search(r"\b01\s+2026\b", bimestre) is None
        or len(registros) != 1
        or not filas
        or not _nss_digits(filas[0][0] if filas[0] else "")
    ):
        raise ValueError("La recuperación histórica solo admite febrero de 2026 con identidad inequívoca.")
    ancho = len(filas[0])
    if any(len(fila) != ancho for fila in filas):
        raise ValueError("Cédula histórica con ancho de columnas inconsistente.")
    if ancho == 21:
        tipo = "MENSUAL"
        columnas = {"patronal": 18}
    elif ancho == 20:
        tipo = "BIMESTRAL"
        columnas = {"retiro": 7, "patronal": 8, "aportacion patronal": 11}
    else:
        raise ValueError("Cédula histórica sin layout mensual o bimestral conocido.")

    encabezado_identidad = [""] * ancho
    encabezado_identidad[0] = f"{'Período' if tipo == 'MENSUAL' else 'Bimestre'} de Proceso: Febrero-2026"
    encabezado_identidad[1] = f"Registro Patronal: {registros.pop()}"
    encabezado_columnas = [""] * ancho
    encabezado_columnas[0], encabezado_columnas[2], encabezado_columnas[3] = "Clave", "Días", "SDI"
    for etiqueta, posicion in columnas.items():
        encabezado_columnas[posicion] = etiqueta.capitalize()
    filas_validadas = [encabezado_identidad, encabezado_columnas, *filas]
    parseada = parsear_cedula(filas_validadas)
    if parseada.periodo != date(2026, 2, 1) or parseada.tipo != tipo:
        raise ValueError("La identidad recuperada no coincide con febrero de 2026.")
    total_detalle = sum((t.patronal for t in parseada.trabajadores), Decimal("0")).quantize(Decimal("0.01"))

    imss_resumen = _total_seccion(
        resumen, "para abono en cuenta del imss", "para abono en cuenta individual"
    )
    if tipo == "MENSUAL":
        filas_pago = [f for f in filas if "total a pagar" in _texto(f)]
        if len(filas_pago) != 1 or _importe(filas_pago[0][11]) != imss_resumen:
            raise ValueError("Total a pagar IMSS no concilia con resumen SUA.")
        indice = filas.index(filas_pago[0])
        anteriores = [f for f in filas[:indice] if len(f) > 10 and isinstance(f[8], (int, float, Decimal))]
        if not anteriores or _importe(anteriores[-1][8]) != total_detalle:
            raise ValueError("El control patronal impreso no concilia con el detalle.")
        if _importe(anteriores[-1][10]) != imss_resumen:
            raise ValueError("El pie mensual no concilia con resumen SUA.")
    else:
        rcv_resumen = _total_seccion(
            resumen, "para abono en cuenta individual", "para abono en cuenta del infonavit"
        )
        vivienda_resumen = _total_seccion(
            resumen, "para abono en cuenta del infonavit", "total a pagar"
        )
        rcv_pie = _fila_unica(filas, "total a pagar de rcv")
        vivienda_pie = _fila_unica(filas, "total a pagar de infonavit")
        if _importe(rcv_pie[1]) != rcv_resumen or _importe(vivienda_pie[4]) != vivienda_resumen:
            raise ValueError("Los totales bimestrales impresos no concilian con resumen SUA.")
        retiro = sum((t.retiro for t in parseada.trabajadores), Decimal("0")).quantize(Decimal("0.01"))
        vivienda = sum((t.aportacion_vivienda for t in parseada.trabajadores), Decimal("0")).quantize(Decimal("0.01"))
        vivienda_patronal = _importe_etiqueta(resumen, "aportacion patronal sin credito") + _importe_etiqueta(
            resumen, "aportacion patronal con credito"
        )
        if retiro != _importe_etiqueta(resumen, "retiro") or vivienda != vivienda_patronal:
            raise ValueError("Componentes patronales bimestrales no concilian con resumen SUA.")
        if total_detalle > rcv_resumen + vivienda_resumen:
            raise ValueError("Detalle patronal excede el total a pagar bimestral.")

    return RecuperacionSUA(filas_validadas, parseada, total_detalle)
