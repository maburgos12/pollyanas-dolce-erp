from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from lxml import etree

from conciliacion.models import CfdiSucursalResolucion, SucursalIdentificadorFiscal
from sat_client.models import CfdiDescargado


@dataclass(frozen=True)
class SucursalMatch:
    sucursal_id: int | None
    sucursal_codigo: str
    fuente: str
    confianza: int
    texto_detectado: str
    detalles: dict


def normalizar_texto(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    upper = without_accents.upper()
    upper = re.sub(r"[^A-Z0-9]+", " ", upper)
    return re.sub(r"\s+", " ", upper).strip()


def extraer_textos_cfdi(xml_raw: str | None) -> list[str]:
    if not xml_raw:
        return []

    try:
        root = etree.fromstring(xml_raw.encode("utf-8"))
    except etree.XMLSyntaxError:
        return []

    textos: list[str] = []
    for node in root.iter():
        local_name = etree.QName(node).localname
        if local_name == "Concepto":
            descripcion = node.get("Descripcion")
            no_identificacion = node.get("NoIdentificacion")
            if descripcion:
                textos.append(descripcion)
            if no_identificacion:
                textos.append(no_identificacion)
        elif local_name in {"Emisor", "Receptor"}:
            nombre = node.get("Nombre")
            if nombre:
                textos.append(nombre)
    return textos


def _coincide_identificador(identificador: SucursalIdentificadorFiscal, texto_normalizado: str) -> bool:
    patron = normalizar_texto(identificador.patron)
    if not patron:
        return False
    if identificador.tipo == SucursalIdentificadorFiscal.TIPO_REGEX:
        try:
            return re.search(identificador.patron, texto_normalizado, flags=re.IGNORECASE) is not None
        except re.error:
            return False
    return patron in texto_normalizado


def resolver_sucursal_cfdi(cfdi: CfdiDescargado) -> SucursalMatch:
    textos = extraer_textos_cfdi(cfdi.xml_raw)
    texto_unido = " | ".join(textos)
    texto_normalizado = normalizar_texto(texto_unido)
    if not texto_normalizado:
        return SucursalMatch(
            sucursal_id=None,
            sucursal_codigo="",
            fuente=CfdiSucursalResolucion.FUENTE_SIN_COINCIDENCIA,
            confianza=0,
            texto_detectado="",
            detalles={"motivo": "sin_texto_xml"},
        )

    matches: list[tuple[SucursalIdentificadorFiscal, int]] = []
    identificadores = (
        SucursalIdentificadorFiscal.objects.select_related("sucursal")
        .filter(activo=True, sucursal__activa=True)
        .order_by("prioridad", "sucursal__codigo", "patron")
    )
    for identificador in identificadores:
        if _coincide_identificador(identificador, texto_normalizado):
            matches.append((identificador, identificador.prioridad))

    if not matches:
        return SucursalMatch(
            sucursal_id=None,
            sucursal_codigo="",
            fuente=CfdiSucursalResolucion.FUENTE_SIN_COINCIDENCIA,
            confianza=0,
            texto_detectado=texto_unido[:255],
            detalles={"motivo": "sin_patron", "texto_normalizado": texto_normalizado[:500]},
        )

    best_priority = min(priority for _, priority in matches)
    best_matches = [identificador for identificador, priority in matches if priority == best_priority]
    sucursales = {identificador.sucursal_id for identificador in best_matches}
    if len(sucursales) > 1:
        return SucursalMatch(
            sucursal_id=None,
            sucursal_codigo="",
            fuente=CfdiSucursalResolucion.FUENTE_AMBIGUA,
            confianza=40,
            texto_detectado=texto_unido[:255],
            detalles={
                "motivo": "multiples_sucursales",
                "candidatos": [
                    {
                        "sucursal": identificador.sucursal.codigo,
                        "patron": identificador.patron,
                        "prioridad": identificador.prioridad,
                    }
                    for identificador in best_matches
                ],
            },
        )

    identificador = best_matches[0]
    return SucursalMatch(
        sucursal_id=identificador.sucursal_id,
        sucursal_codigo=identificador.sucursal.codigo,
        fuente=CfdiSucursalResolucion.FUENTE_XML_CONCEPTO,
        confianza=95 if best_priority <= 20 else 85,
        texto_detectado=identificador.patron,
        detalles={
            "patron": identificador.patron,
            "tipo": identificador.tipo,
            "prioridad": identificador.prioridad,
            "texto_normalizado": texto_normalizado[:500],
        },
    )


def guardar_resolucion_sucursal_cfdi(cfdi: CfdiDescargado) -> CfdiSucursalResolucion:
    match = resolver_sucursal_cfdi(cfdi)
    resolucion, _ = CfdiSucursalResolucion.objects.update_or_create(
        cfdi=cfdi,
        defaults={
            "sucursal_id": match.sucursal_id,
            "fuente": match.fuente,
            "confianza": match.confianza,
            "texto_detectado": match.texto_detectado,
            "detalles": match.detalles,
        },
    )
    return resolucion
