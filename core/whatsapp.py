"""Envío de WhatsApp por Meta Cloud API con verificación real de la respuesta.

Por qué no se reutilizan los helpers de ``seguimiento/tasks.py`` y
``fallas/tasks.py``: ambos hacen ``httpx.post`` contra
``https://api.pollyanasdolce.com/api/send-message/`` y solo registran algo si
se levanta una excepción. Ese endpoint no existe (responde 404), así que esas
llamadas nunca envían nada y jamás lo reportan. Aquí se habla directo con la
Graph API, se revisa el código HTTP y se conserva el identificador del mensaje.

Requisitos vigentes del canal: un aviso iniciado por el negocio fuera de la
ventana de 24 horas solo puede enviarse con una plantilla aprobada por Meta.
Por eso este módulo solo envía ``type: template`` y queda inactivo mientras no
existan credenciales y nombre de plantilla en el ambiente.
"""
from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

GRAPH_VERSION = "v21.0"

RESULTADO_ENVIADO = "ENVIADO"
RESULTADO_FALLIDO = "FALLIDO"
RESULTADO_INCIERTO = "INCIERTO"
RESULTADO_SIN_CANAL = "SIN_CANAL"


class ResultadoWhatsApp:
    __slots__ = ("estado", "referencia", "detalle")

    def __init__(self, estado: str, *, referencia: str = "", detalle: str = ""):
        self.estado = estado
        self.referencia = referencia
        self.detalle = detalle

    def __repr__(self) -> str:  # pragma: no cover - ayuda de depuración
        return f"ResultadoWhatsApp({self.estado!r}, referencia={self.referencia!r}, detalle={self.detalle!r})"


def _env(nombre: str) -> str:
    return (os.getenv(nombre) or "").strip()


def canal_habilitado() -> bool:
    return _env("WHATSAPP_ENABLED").lower() in {"1", "true", "yes", "on", "si", "sí"}


def configuracion_faltante() -> str:
    """Describe qué falta para que el canal pueda enviar; vacío si está listo."""
    if not canal_habilitado():
        return "Canal de WhatsApp desactivado en este ambiente (WHATSAPP_ENABLED)."
    faltantes = [
        nombre
        for nombre in ("META_WHATSAPP_TOKEN", "META_WHATSAPP_PHONE_NUMBER_ID")
        if not _env(nombre)
    ]
    if faltantes:
        return "WhatsApp sin credenciales: falta " + ", ".join(faltantes) + "."
    return ""


def _idioma() -> str:
    codigo = _env("WHATSAPP_TEMPLATE_LANGUAGE") or "es_MX"
    if not re.fullmatch(r"[a-z]{2}(_[A-Z]{2})?", codigo):
        raise ValueError("Código de idioma de plantilla inválido. Ejemplo: es_MX")
    return codigo


def _validar_plantilla(nombre: str) -> str:
    limpio = (nombre or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9_]+", limpio or ""):
        raise ValueError("Nombre de plantilla inválido. Usa minúsculas, números y guion bajo.")
    return limpio


def _detalle_error(cuerpo: str) -> str:
    try:
        datos = json.loads(cuerpo or "{}")
    except ValueError:
        datos = {}
    mensaje = ""
    if isinstance(datos, dict):
        error = datos.get("error")
        if isinstance(error, dict):
            mensaje = error.get("message") or ""
    mensaje = mensaje or (cuerpo or "").strip()
    mensaje = re.sub(r"\s+", " ", mensaje)
    return mensaje[:400]


def enviar_plantilla(*, telefono: str, plantilla: str, parametros: list[str], timeout: float = 20.0) -> ResultadoWhatsApp:
    """Envía una plantilla aprobada y reporta el resultado real del canal.

    ``ENVIADO`` solo se devuelve cuando Meta respondió 2xx con identificador de
    mensaje. Un rechazo con respuesta es ``FALLIDO``; una respuesta que no llegó
    (timeout, error de red o 2xx sin identificador) es ``INCIERTO`` y no debe
    reenviarse sin reconciliar.
    """
    faltante = configuracion_faltante()
    if faltante:
        return ResultadoWhatsApp(RESULTADO_SIN_CANAL, detalle=faltante)
    nombre_plantilla = _validar_plantilla(plantilla or _env("WHATSAPP_TEMPLATE_COMPRA_REALIZADA"))
    payload = {
        "messaging_product": "whatsapp",
        "to": telefono,
        "type": "template",
        "template": {
            "name": nombre_plantilla,
            "language": {"code": _idioma()},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": str(valor)} for valor in parametros],
                }
            ],
        },
    }
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{_env('META_WHATSAPP_PHONE_NUMBER_ID')}/messages"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {_env('META_WHATSAPP_TOKEN')}",
            "Content-Type": "application/json",
            "User-Agent": "PollyanasERP/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            codigo = response.getcode()
            cuerpo = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        cuerpo = exc.read().decode("utf-8", errors="replace")
        # Meta ya decidió: el mensaje no se aceptó y reintentar no duplica.
        return ResultadoWhatsApp(RESULTADO_FALLIDO, detalle=f"HTTP {exc.code}: {_detalle_error(cuerpo)}")
    except Exception as exc:  # timeout, DNS, TLS: no sabemos si Meta lo recibió
        logger.warning("[compras] WhatsApp resultado incierto: %s", exc)
        return ResultadoWhatsApp(RESULTADO_INCIERTO, detalle=f"Sin respuesta del canal: {exc}"[:400])

    if codigo >= 400:
        return ResultadoWhatsApp(RESULTADO_FALLIDO, detalle=f"HTTP {codigo}: {_detalle_error(cuerpo)}")
    try:
        datos = json.loads(cuerpo or "{}")
    except ValueError:
        datos = {}
    mensajes = datos.get("messages") if isinstance(datos, dict) else None
    referencia = ""
    if isinstance(mensajes, list) and mensajes and isinstance(mensajes[0], dict):
        referencia = mensajes[0].get("id") or ""
    if not referencia:
        return ResultadoWhatsApp(RESULTADO_INCIERTO, detalle="Meta respondió sin identificador de mensaje.")
    return ResultadoWhatsApp(RESULTADO_ENVIADO, referencia=referencia)
