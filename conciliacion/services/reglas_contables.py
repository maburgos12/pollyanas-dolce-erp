from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from conciliacion.models import (
    CuentaBancariaPropia,
    InstrumentoFinancieroConciliacion,
    ReglaClasificacionMovimiento,
)
from syncfy_client.models import MovimientoBancario


@dataclass(frozen=True)
class PropuestaClasificacion:
    regla: ReglaClasificacionMovimiento
    confianza: int
    razon: str
    evidencia_requerida: list[str]


def propuestas_para_movimiento(movimiento: MovimientoBancario) -> list[PropuestaClasificacion]:
    descripcion_normalizada = normalizar_texto(movimiento.descripcion)
    reglas = (
        ReglaClasificacionMovimiento.objects.select_related("concepto", "cuenta_debe_sugerida", "cuenta_haber_sugerida")
        .filter(activa=True)
        .order_by("prioridad", "nombre")
    )
    propuestas = []
    for regla in reglas:
        if not _tipo_movimiento_compatible(regla, movimiento):
            continue
        if not _patrones_coinciden(regla.patrones_descripcion, descripcion_normalizada):
            continue
        if regla.requiere_cuenta_propia_destino and not _descripcion_contiene_cuenta_propia(descripcion_normalizada):
            continue
        if regla.instrumento_tipo and not _descripcion_contiene_instrumento(regla.instrumento_tipo, descripcion_normalizada):
            continue
        propuestas.append(
            PropuestaClasificacion(
                regla=regla,
                confianza=min(regla.confianza_base + 10, 100),
                razon=_razon(regla),
                evidencia_requerida=list(regla.evidencia_requerida or regla.concepto.evidencia_requerida or []),
            )
        )
    return propuestas


def normalizar_texto(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto or "")
    texto = "".join(char for char in texto if not unicodedata.combining(char))
    texto = re.sub(r"\s+", " ", texto.upper()).strip()
    return texto


def _tipo_movimiento_compatible(regla: ReglaClasificacionMovimiento, movimiento: MovimientoBancario) -> bool:
    return regla.tipo_movimiento == ReglaClasificacionMovimiento.TIPO_AMBOS or regla.tipo_movimiento == movimiento.tipo


def _patrones_coinciden(patrones: list[str], descripcion_normalizada: str) -> bool:
    if not patrones:
        return True
    return any(normalizar_texto(patron) in descripcion_normalizada for patron in patrones)


def _descripcion_contiene_cuenta_propia(descripcion_normalizada: str) -> bool:
    cuentas = CuentaBancariaPropia.objects.filter(activa=True).only("clabe", "ultimos_digitos")
    for cuenta in cuentas:
        identificadores = [cuenta.clabe, cuenta.ultimos_digitos]
        for identificador in identificadores:
            if identificador and identificador in descripcion_normalizada:
                return True
    return False


def _descripcion_contiene_instrumento(instrumento_tipo: str, descripcion_normalizada: str) -> bool:
    instrumentos = InstrumentoFinancieroConciliacion.objects.filter(tipo=instrumento_tipo, activo=True)
    for instrumento in instrumentos:
        candidatos = [instrumento.numero_referencia, instrumento.nombre, instrumento.institucion]
        candidatos.extend(instrumento.patrones_descripcion or [])
        if any(normalizar_texto(candidato) in descripcion_normalizada for candidato in candidatos if candidato):
            return True
    return False


def _razon(regla: ReglaClasificacionMovimiento) -> str:
    partes = [f"Regla {regla.nombre}"]
    if regla.requiere_cuenta_propia_destino:
        partes.append("cuenta destino reconocida como propia")
    if regla.instrumento_tipo:
        partes.append(f"instrumento {regla.get_instrumento_tipo_display()}")
    return "; ".join(partes)
