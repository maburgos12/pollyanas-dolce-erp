from __future__ import annotations

import json
import logging
from datetime import date

from django.conf import settings
from django.db.models import Sum
from django.utils import timezone
from openai import OpenAI

from rentabilidad.agente_rentabilidad import _construir_contexto
from rentabilidad.models import SucursalRentabilidad
from reportes.dashboard_full_dataset import get_materialized_dashboard_full_payload
from reportes.models import ProductoSucursalContribucionMensual

from .models import ConsejoConsulta

logger = logging.getLogger(__name__)

_DASHBOARD_KEYS_RELEVANTES = [
    "daily_sales_snapshot",
    "forecast_panel",
    "production_summary",
    "waste_executive_summary",
    "budget_semaforo_mes",
    "budget_semaforo_quincena",
]


def _restar_meses(fecha: date, meses: int) -> date:
    total = fecha.year * 12 + (fecha.month - 1) - meses
    year, month = divmod(total, 12)
    return date(year, month + 1, 1)


def _texto_dashboard(payload: dict) -> str:
    lines = []
    for key in _DASHBOARD_KEYS_RELEVANTES:
        value = payload.get(key)
        if not value:
            continue
        lines.append(f"{key}: {json.dumps(value, ensure_ascii=False, default=str)[:800]}")
    return "\n".join(lines)


def _texto_sucursales() -> str:
    """Snapshot más reciente de cada sucursal, reutilizando el mismo bloque de
    texto que ya usa el agente de rentabilidad por sucursal."""
    vistos: set[int] = set()
    bloques = []
    for fila in SucursalRentabilidad.objects.select_related("sucursal").order_by("-periodo"):
        if fila.sucursal_id in vistos:
            continue
        vistos.add(fila.sucursal_id)
        bloques.append(_construir_contexto(fila))
    return "\n\n---\n\n".join(bloques)


def _texto_productos(months_window: int) -> str:
    inicio = _restar_meses(timezone.localdate().replace(day=1), months_window)
    agregados = list(
        ProductoSucursalContribucionMensual.objects.filter(periodo__gte=inicio)
        .values("receta_id", "receta__nombre")
        .annotate(contribucion=Sum("contribucion_total"))
        .order_by("-contribucion")
    )
    if not agregados:
        return ""
    lines = ["TOP 10 productos por contribución total del periodo:"]
    for row in agregados[:10]:
        lines.append(f"- {row['receta__nombre']}: ${row['contribucion']:,.2f}")
    if len(agregados) > 10:
        lines.append("BOTTOM 10 productos por contribución total del periodo:")
        for row in agregados[-10:]:
            lines.append(f"- {row['receta__nombre']}: ${row['contribucion']:,.2f}")
    return "\n".join(lines)


def construir_snapshot(*, months_window: int = 6) -> dict:
    """Contexto real de la empresa para alimentar a los 9 roles. Cada sección
    declara honestamente si el dato está disponible — nunca se omite en
    silencio ni se inventa."""
    dashboard_payload = get_materialized_dashboard_full_payload(months_window=months_window)
    if dashboard_payload:
        dashboard = {"disponible": True, "texto": _texto_dashboard(dashboard_payload)}
    else:
        dashboard = {
            "disponible": False,
            "texto": "Dashboard ejecutivo no disponible (vista materializada sin poblar).",
        }

    texto_sucursales = _texto_sucursales()
    rentabilidad_sucursal = {
        "disponible": bool(texto_sucursales),
        "texto": texto_sucursales or "Sin snapshots de rentabilidad por sucursal disponibles.",
    }

    texto_productos = _texto_productos(months_window)
    rentabilidad_producto = {
        "disponible": bool(texto_productos),
        "texto": texto_productos or "Sin datos de contribución por producto disponibles.",
    }

    return {
        "dashboard_ejecutivo": dashboard,
        "rentabilidad_sucursal": rentabilidad_sucursal,
        "rentabilidad_producto": rentabilidad_producto,
    }


def _snapshot_a_texto(snapshot: dict) -> str:
    etiquetas = {
        "dashboard_ejecutivo": "DASHBOARD EJECUTIVO",
        "rentabilidad_sucursal": "RENTABILIDAD POR SUCURSAL",
        "rentabilidad_producto": "RENTABILIDAD POR PRODUCTO",
    }
    partes = []
    for clave, titulo in etiquetas.items():
        seccion = snapshot.get(clave) or {}
        disponible = "SÍ" if seccion.get("disponible") else "NO"
        partes.append(f"=== {titulo} (disponible: {disponible}) ===\n{seccion.get('texto', '')}")
    return "\n\n".join(partes)


CONTEXTO_EMPRESA = (
    "Eres parte del Consejo Estratégico de Pollyana's Dolce, cadena de "
    "pastelería y repostería en Guasave, Sinaloa, México. Ventas anuales "
    "aproximadas: $40 millones MXN. Objetivo estratégico: crecer a "
    "$100-120 millones anuales y sostener retiros de $500,000 a "
    "$1,000,000 mensuales para los socios. Operan 8-9 sucursales con "
    "producción centralizada."
)

REGLA_NO_INVENTAR = (
    "Regla obligatoria: NO inventes cifras. Usa solo los datos reales que se "
    "te dan en el contexto. Si necesitas un dato que no está presente, "
    "decláralo explícitamente en datos_faltantes en vez de asumir un número."
)

FORMATO_ROL = (
    "Responde SOLO con JSON válido, sin texto adicional ni markdown, con "
    "este formato exacto:\n"
    "{\n"
    '  "analisis": "Tu análisis desde tu rol, máximo 4 oraciones.",\n'
    '  "supuestos": ["supuesto 1", "supuesto 2"],\n'
    '  "datos_faltantes": ["dato que te haría falta para responder con más precisión"]\n'
    "}"
)


def _prompt_rol(nombre_rol: str, enfoque: str) -> str:
    return f"{CONTEXTO_EMPRESA}\n\nRol: {nombre_rol}.\n{enfoque}\n\n{REGLA_NO_INVENTAR}\n\n{FORMATO_ROL}"


ROLES = [
    {
        "codigo": "cfo",
        "nombre": "CFO / Finanzas",
        "system_prompt": _prompt_rol(
            "CFO / Finanzas",
            "Analiza inversión, flujo de efectivo, utilidad, ROI, recuperación "
            "de inversión y riesgo financiero de la pregunta planteada.",
        ),
    },
    {
        "codigo": "coo",
        "nombre": "COO / Operaciones",
        "system_prompt": _prompt_rol(
            "COO / Operaciones",
            "Analiza capacidad de producción, logística, personal necesario, "
            "horarios e inventarios requeridos.",
        ),
    },
    {
        "codigo": "cmo",
        "nombre": "CMO / Marketing",
        "system_prompt": _prompt_rol(
            "CMO / Marketing",
            "Analiza marca, posicionamiento, mercado, experiencia del cliente "
            "y promociones relevantes a la pregunta.",
        ),
    },
    {
        "codigo": "comercial",
        "nombre": "Comercial",
        "system_prompt": _prompt_rol(
            "Comercial",
            "Analiza ventas esperadas, ticket promedio, tráfico, canal de "
            "venta y comportamiento de clientes.",
        ),
    },
    {
        "codigo": "rh",
        "nombre": "RH",
        "system_prompt": _prompt_rol(
            "RH / Recursos Humanos",
            "Analiza personal requerido, capacitación, sueldos, rotación y "
            "productividad implicados.",
        ),
    },
    {
        "codigo": "cto",
        "nombre": "CTO / Tecnología",
        "system_prompt": _prompt_rol(
            "CTO / Tecnología",
            "Analiza si el ERP y los sistemas actuales pueden soportar el "
            "proyecto, qué automatización o integración haría falta.",
        ),
    },
    {
        "codigo": "innovacion",
        "nombre": "Innovación",
        "system_prompt": _prompt_rol(
            "Innovación",
            "Analiza si el proyecto abre una nueva unidad de negocio, ventaja "
            "competitiva o diferenciación relevante.",
        ),
    },
    {
        "codigo": "riesgos",
        "nombre": "Riesgos",
        "system_prompt": _prompt_rol(
            "Riesgos",
            "Analiza amenazas, sensibilidad del negocio, dependencia de "
            "proveedores, competencia y posibles errores operativos.",
        ),
    },
]

CEO_SYSTEM_PROMPT = (
    f"{CONTEXTO_EMPRESA}\n\n"
    "Rol: CEO / Dirección General. Recibirás la pregunta original, el "
    "contexto real de la empresa, y el análisis de 8 roles (CFO, COO, CMO, "
    "Comercial, RH, CTO, Innovación, Riesgos).\n\n"
    "Da la conclusión ejecutiva final, considerando los 8 análisis. Tu "
    "veredicto debe ser exactamente uno de: APROBAR, RECHAZAR, POSPONER, "
    "PILOTO, PEDIR_DATOS.\n\n"
    f"{REGLA_NO_INVENTAR}\n\n"
    "Responde SOLO con JSON válido, sin texto adicional ni markdown, con "
    "este formato exacto:\n"
    "{\n"
    '  "veredicto": "APROBAR|RECHAZAR|POSPONER|PILOTO|PEDIR_DATOS",\n'
    '  "resumen_ejecutivo": "Una sola oración con la conclusión para el DG.",\n'
    '  "conclusion": "Explicación breve (máximo 4 oraciones), referenciando los análisis de los roles."\n'
    "}"
)


def _llamar_rol(system_prompt: str, contexto_usuario: str) -> dict:
    """Mismo patrón que rentabilidad/agente_rentabilidad.py: JSON forzado por
    prompt, con fallback si el JSON sale inválido o la llamada falla — un rol
    fallido no debe tumbar toda la consulta."""
    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.3,
            max_tokens=800,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": contexto_usuario},
            ],
        )
        raw = response.choices[0].message.content.strip()
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("[ConsejoIA] JSON inválido: %s", exc)
        return {"error": "No se pudo generar un análisis válido. Revisar manualmente."}
    except Exception as exc:  # noqa: BLE001 - mismo criterio que agente_rentabilidad.py
        logger.exception("[ConsejoIA] Error al llamar al rol: %s", exc)
        return {"error": f"Error del agente: {exc}"}


def analizar_pregunta(pregunta: str, *, usuario) -> ConsejoConsulta:
    snapshot = construir_snapshot()
    snapshot_texto = _snapshot_a_texto(snapshot)

    respuestas: dict[str, dict] = {}
    for rol in ROLES:
        contexto_usuario = f"{snapshot_texto}\n\nPREGUNTA: {pregunta}"
        respuestas[rol["codigo"]] = _llamar_rol(rol["system_prompt"], contexto_usuario)

    contexto_ceo = (
        f"{snapshot_texto}\n\nPREGUNTA: {pregunta}\n\n"
        f"ANÁLISIS DE LOS ROLES:\n{json.dumps(respuestas, ensure_ascii=False)}"
    )
    resultado_ceo = _llamar_rol(CEO_SYSTEM_PROMPT, contexto_ceo)

    veredicto = resultado_ceo.get("veredicto") or ConsejoConsulta.VEREDICTO_PEDIR_DATOS
    if veredicto not in dict(ConsejoConsulta.VEREDICTO_CHOICES):
        veredicto = ConsejoConsulta.VEREDICTO_PEDIR_DATOS
    respuestas["ceo"] = resultado_ceo

    return ConsejoConsulta.objects.create(
        pregunta=pregunta,
        snapshot_json=snapshot,
        respuestas_json=respuestas,
        veredicto_ceo=veredicto,
        resumen_ejecutivo_ceo=resultado_ceo.get("resumen_ejecutivo", ""),
        creado_por=usuario if getattr(usuario, "is_authenticated", False) else None,
    )
