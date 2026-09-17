"""Lectura de tickets de combustible con el modelo de visión que ya usa el ERP."""

import base64
import json
import logging
from decimal import Decimal, InvalidOperation
from io import BytesIO

from django.conf import settings
from PIL import Image, ImageOps, UnidentifiedImageError

logger = logging.getLogger(__name__)

MODELO = "gpt-4o"
# ponytail: 1280px de lado largo alcanza para leer un ticket térmico y recorta
# el costo por imagen. Subir si aparecen tickets ilegibles por resolución.
LADO_LARGO_MAX = 1280

PROMPT = """Eres un auditor de tickets de gasolinera mexicanos.
Analiza la foto y responde SOLO con JSON, sin texto adicional.

{
  "es_ticket_combustible": true|false,
  "legible": true|false,
  "litros": número o null,
  "importe_total": número o null,
  "precio_por_litro": número o null,
  "estacion": "texto" o null,
  "fecha": "YYYY-MM-DD" o null,
  "observaciones": "texto breve en español"
}

Reglas:
- es_ticket_combustible: true solo si es un comprobante de compra de combustible
  (ticket de bomba, nota de gasolinera, vale de gasolina canjeado). Una foto de
  otra cosa, una pantalla, un recibo distinto o una imagen vacía es false.
- legible: false si la foto es un ticket pero no logras leer los importes.
- No inventes cifras. Si un dato no se ve, ponlo en null.
- importe_total es el total pagado, no el subtotal ni el IVA.
"""


class TicketOCRNoDisponible(RuntimeError):
    """El servicio de lectura no está configurado o no respondió."""


def leer_ticket(field_file) -> dict:
    """Devuelve lo que el modelo leyó del ticket. Lanza TicketOCRNoDisponible si falla."""
    api_key = getattr(settings, "OPENAI_API_KEY", "")
    if not api_key:
        raise TicketOCRNoDisponible("OPENAI_API_KEY no está configurada")

    try:
        from openai import OpenAI
    except ImportError as exc:  # pragma: no cover - el paquete está en requirements
        raise TicketOCRNoDisponible("paquete openai no disponible") from exc

    imagen_b64 = _imagen_para_modelo(field_file)

    try:
        respuesta = OpenAI(api_key=api_key).chat.completions.create(
            model=MODELO,
            temperature=0,
            max_tokens=400,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": PROMPT},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{imagen_b64}"},
                        },
                    ],
                }
            ],
        )
        crudo = respuesta.choices[0].message.content
        datos = json.loads(crudo)
    except json.JSONDecodeError as exc:
        raise TicketOCRNoDisponible(f"respuesta no es JSON: {exc}") from exc
    except Exception as exc:
        raise TicketOCRNoDisponible(f"error al llamar al modelo: {exc}") from exc

    uso = getattr(respuesta, "usage", None)
    return {
        "es_ticket": bool(datos.get("es_ticket_combustible")),
        "legible": bool(datos.get("legible")),
        "litros": _a_decimal(datos.get("litros")),
        "importe_total": _a_decimal(datos.get("importe_total")),
        "precio_por_litro": _a_decimal(datos.get("precio_por_litro")),
        "estacion": (datos.get("estacion") or "")[:120],
        "fecha": datos.get("fecha") or None,
        "observaciones": (datos.get("observaciones") or "")[:300],
        "modelo": MODELO,
        "tokens": getattr(uso, "total_tokens", None) if uso else None,
    }


def _imagen_para_modelo(field_file) -> str:
    """Normaliza orientación, reduce y codifica en base64."""
    field_file.open("rb")
    try:
        with Image.open(field_file) as original:
            imagen = ImageOps.exif_transpose(original).convert("RGB")
            imagen.thumbnail((LADO_LARGO_MAX, LADO_LARGO_MAX), Image.LANCZOS)
            buffer = BytesIO()
            imagen.save(buffer, format="JPEG", quality=85)
    except (UnidentifiedImageError, OSError) as exc:
        raise TicketOCRNoDisponible(f"no se pudo abrir la imagen: {exc}") from exc
    finally:
        field_file.close()
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _a_decimal(valor) -> Decimal | None:
    if valor is None or valor == "":
        return None
    try:
        return Decimal(str(valor).replace(",", "").replace("$", "").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
