from __future__ import annotations

import json
import logging

from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)


MARKET_STUDY_SYSTEM_PROMPT = """
Eres un analista de mercado especializado en viabilidad de negocios de alimentos
y repostería artesanal en México. Tu análisis debe ser realista, basado en datos
demográficos y de mercado para la zona indicada, y enfocado en la toma de
decisiones de inversión.

Responde únicamente con un objeto JSON válido con esta estructura exacta:
{
  "score_viabilidad": <int 0-100>,
  "veredicto": "<VIABLE|VIABLE_CON_RESERVAS|NO_VIABLE>",
  "resumen_ejecutivo": "<2-3 oraciones clave>",
  "perfil_demografico": {
    "poblacion_estimada_radio_2km": <int>,
    "nivel_socioeconomico_predominante": "<A/B|C+|C|D+|D>",
    "edad_promedio_cliente_objetivo": "<rango>",
    "descripcion": "<texto breve>"
  },
  "analisis_ubicacion": {
    "trafico_peatonal": "<Alto|Medio|Bajo>",
    "trafico_vehicular": "<Alto|Medio|Bajo>",
    "accesibilidad": "<texto breve>",
    "visibilidad": "<Alta|Media|Baja>",
    "factores_positivos": ["<factor1>", "<factor2>"],
    "factores_negativos": ["<factor1>", "<factor2>"]
  },
  "competencia": {
    "nivel_competencia": "<Alto|Medio|Bajo>",
    "competidores_directos_estimados": <int>,
    "principales_competidores": ["<nombre o tipo>"],
    "diferenciadores_clave": ["<diferenciador>"]
  },
  "proyeccion_demanda": {
    "clientes_potenciales_dia": <int>,
    "ticket_promedio_estimado": <float>,
    "ventas_mensuales_estimadas_mes1": <float>,
    "ventas_mensuales_estimadas_mes6": <float>,
    "ventas_mensuales_estimadas_anio1": <float>,
    "tasa_crecimiento_mensual_estimada_pct": <float>
  },
  "foda": {
    "fortalezas": ["<item>"],
    "oportunidades": ["<item>"],
    "debilidades": ["<item>"],
    "amenazas": ["<item>"]
  },
  "recomendaciones": ["<recomendacion1>", "<recomendacion2>", "<recomendacion3>"],
  "advertencias": ["<advertencia si aplica>"]
}
"""


def generar_estudio_mercado(
    *,
    ciudad: str,
    colonia: str,
    descripcion_ubicacion: str,
    tipo_negocio: str = "pastelería artesanal y repostería",
    m2_local: float | None = None,
    renta_mensual: float | None = None,
    ticket_promedio_cadena: float | None = None,
    ventas_promedio_cadena: float | None = None,
    competidores_conocidos: str = "",
) -> dict:
    """
    Genera un estudio de mercado con OpenAI y regresa un dict serializable.
    La vista decide dónde persistirlo; este servicio no toca modelos.
    """
    api_key = getattr(settings, "OPENAI_API_KEY", "")
    if not api_key:
        logger.error("OPENAI_API_KEY no configurado")
        return {"error": "OPENAI_API_KEY no configurado", "score_viabilidad": 0}

    try:
        from openai import OpenAI
    except ImportError:
        logger.error("openai package no disponible")
        return {"error": "openai package no disponible", "score_viabilidad": 0}

    model = getattr(settings, "POS_BRIDGE_AGENT_MODEL", "gpt-4o-mini")
    contexto_erp = []
    if ticket_promedio_cadena:
        contexto_erp.append(f"Ticket promedio real de la cadena: ${ticket_promedio_cadena:,.2f} MXN")
    if ventas_promedio_cadena:
        contexto_erp.append(f"Ventas mensuales promedio de sucursales existentes: ${ventas_promedio_cadena:,.2f} MXN")

    user_prompt = f"""
Genera un estudio de mercado para abrir una {tipo_negocio} en {colonia}, {ciudad}, Sinaloa, México.

Descripción del local: {descripcion_ubicacion or "Local comercial en zona urbana"}
Superficie: {m2_local or "No especificada"} m2
Renta mensual: {f"${renta_mensual:,.0f} MXN" if renta_mensual else "No especificada"}
Competidores conocidos: {competidores_conocidos or "No especificados"}
Benchmark ERP: {"; ".join(contexto_erp) if contexto_erp else "No disponible"}

Sé específico para la ciudad y colonia indicadas. No uses datos nacionales genéricos.
"""

    raw = ""
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": MARKET_STUDY_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=2000,
        )
        raw = (response.choices[0].message.content or "").strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(raw)
        result["_model_usado"] = model
        result["_generado_en"] = timezone.now().isoformat()
        return result
    except json.JSONDecodeError as exc:
        logger.error("JSON inválido de OpenAI: %s | raw=%s", exc, raw[:500])
        return {"error": f"Respuesta JSON inválida: {exc}", "score_viabilidad": 0, "_raw": raw[:500]}
    except Exception as exc:
        logger.error("Error OpenAI market study: %s", exc)
        return {"error": str(exc), "score_viabilidad": 0}
