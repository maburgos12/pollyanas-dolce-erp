"""
sucursales/agente_rentabilidad.py

Agente GPT-4o-mini que analiza cada SucursalRentabilidad y genera:
  - Diagnóstico en español natural
  - Lista de recomendaciones priorizadas
  - Alertas específicas

Se ejecuta desde:
  1. Celery Beat (automático, cierre de mes)
  2. Botón "Analizar con IA" en el dashboard
  3. Cuando se guarda un nuevo registro vía API
"""

import json
import logging
from decimal import Decimal
from django.conf import settings

logger = logging.getLogger(__name__)

# Umbral de margen mínimo aceptable para pastelería (ajustable en settings)
MARGEN_BRUTO_MINIMO    = getattr(settings, "RENT_MARGEN_BRUTO_MIN",    55.0)
MARGEN_NETO_MINIMO     = getattr(settings, "RENT_MARGEN_NETO_MIN",     15.0)
ROI_OBJETIVO_ANUAL     = getattr(settings, "RENT_ROI_OBJETIVO",        25.0)
PAYBACK_MAXIMO_MESES   = getattr(settings, "RENT_PAYBACK_MAX_MESES",   36)


SYSTEM_PROMPT = """
Eres el analista financiero interno de Pollyana's Dolce, una cadena de pastelerías en Sinaloa, México.
Tu tarea es analizar el desempeño mensual de UNA sucursal y entregar:

1. Un diagnóstico claro y directo (máximo 4 oraciones).
2. Una lista de máximo 5 recomendaciones priorizadas, ordenadas de mayor a menor impacto.
3. Un nivel de alerta: 0 (ok), 1 (atención), 2 (urgente).

Contexto del negocio:
- Margen bruto objetivo: {margen_bruto_min}%+ (pastelería artesanal con productos premium)
- Margen neto objetivo: {margen_neto_min}%+
- ROI anual objetivo: {roi_obj}%+
- Payback máximo tolerable: {payback_max} meses
- Las sucursales subsidiadas deben tener un plan de 90 días para llegar a equilibrio o considerar cierre

Reglas:
- Sé específico con los números del análisis, no genérico.
- Si algo es crítico, dilo claramente. No suavices malas noticias.
- Las recomendaciones deben ser accionables por el Director General esta semana.
- Responde SOLO con JSON válido, sin texto adicional, sin markdown.

Formato de respuesta:
{{
  "diagnostico": "Texto del diagnóstico aquí.",
  "recomendaciones": [
    {{"prioridad": 1, "accion": "Texto de la acción", "impacto": "alto|medio|bajo"}},
    ...
  ],
  "alerta_nivel": 0,
  "resumen_ejecutivo": "Una sola oración para el DG."
}}
""".format(
    margen_bruto_min=MARGEN_BRUTO_MINIMO,
    margen_neto_min=MARGEN_NETO_MINIMO,
    roi_obj=ROI_OBJETIVO_ANUAL,
    payback_max=PAYBACK_MAXIMO_MESES,
)


def _construir_contexto(rent) -> str:
    """Convierte el objeto SucursalRentabilidad en un bloque de texto para el prompt."""
    lines = [
        f"SUCURSAL: {rent.sucursal.nombre if hasattr(rent.sucursal, 'nombre') else rent.sucursal}",
        f"PERIODO: {rent.periodo.strftime('%B %Y')}",
        f"MESES OPERANDO: {rent.meses_operando or 'N/D'}",
        "",
        "=== INGRESOS ===",
        f"Ventas brutas:         ${rent.ventas_brutas:,.2f}",
        f"Descuentos/devoluc.:   ${rent.descuentos + rent.devoluciones:,.2f}",
        f"Ventas netas:          ${rent.ventas_netas:,.2f}",
        "",
        "=== COSTOS VARIABLES ===",
        f"Materia prima (CMV):   ${rent.costo_materia_prima:,.2f}",
        f"Reventa:               ${rent.costo_reventa:,.2f}",
        f"Empaque:               ${rent.empaque:,.2f}",
        f"Otros variables:       ${rent.otros_costos_variables:,.2f}",
        f"Total costos variables:${rent.costo_variable_total:,.2f}",
        f"Margen bruto:          ${rent.margen_bruto:,.2f}  ({rent.porcentaje_margen_bruto}%)",
        "",
        "=== GASTOS FIJOS ===",
        f"Renta:                 ${rent.renta:,.2f}",
        f"Nómina directa:        ${rent.nomina_directa:,.2f}",
        f"Servicios:             ${rent.servicios_luz_agua:,.2f}",
        f"Mantenimiento:         ${rent.mantenimiento:,.2f}",
        f"Admin prorrateado:     ${rent.gastos_admin_prorrateados:,.2f}",
        f"Otros fijos:           ${rent.otros_gastos_fijos:,.2f}",
        f"Total gastos fijos:    ${rent.gasto_fijo_total:,.2f}",
        "",
        "=== RENTABILIDAD ===",
        f"Utilidad operativa:    ${rent.utilidad_operativa:,.2f}  ({rent.porcentaje_utilidad_operativa}%)",
        f"Punto de equilibrio:   ${rent.punto_equilibrio_mensual:,.2f}",
        f"Avance sobre PE:       {rent.porcentaje_avance_pe}%",
        f"Brecha al PE:          ${rent.brecha_pe:,.2f} {'(FALTA)' if rent.brecha_pe > 0 else '(EXCEDE)'}",
        f"Estado actual:         {rent.estado}",
        "",
        "=== INVERSIÓN Y RECUPERACIÓN ===",
        f"Inversión inicial:     ${rent.inversion_inicial:,.2f}",
        f"Utilidad acumulada:    ${rent.utilidad_acumulada:,.2f}",
        f"Inversión recuperada:  {rent.porcentaje_recuperacion_inversion}%",
        f"Inversión pendiente:   ${rent.inversion_pendiente:,.2f}",
        f"Payback estimado:      {rent.payback_meses_estimados or 'No calculable'} meses",
        f"ROI anualizado:        {rent.roi_anualizado or 'N/D'}%",
    ]

    if rent.es_subsidiada:
        lines += [
            "",
            "=== ALERTA: SUBSIDIO ===",
            f"Esta sucursal está siendo subsidiada.",
            f"Monto subsidio implícito este mes: ${rent.monto_subsidio_implicito:,.2f}",
        ]

    return "\n".join(lines)


def analizar_sucursal(rent, guardar=True) -> dict:
    """
    Llama al agente IA para diagnosticar una SucursalRentabilidad.

    Args:
        rent: instancia de SucursalRentabilidad
        guardar: si True, persiste el diagnóstico en el objeto

    Returns:
        dict con diagnostico, recomendaciones, alerta_nivel, resumen_ejecutivo
    """
    from openai import OpenAI

    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    contexto = _construir_contexto(rent)

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.3,
            max_tokens=800,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": contexto},
            ],
        )
        raw = response.choices[0].message.content.strip()
        resultado = json.loads(raw)

    except json.JSONDecodeError as e:
        logger.error(f"[AgentRentabilidad] JSON inválido para {rent}: {e}")
        resultado = {
            "diagnostico": "No se pudo generar diagnóstico automático. Revisa manualmente.",
            "recomendaciones": [],
            "alerta_nivel": 1,
            "resumen_ejecutivo": "Diagnóstico no disponible.",
        }
    except Exception as e:
        logger.exception(f"[AgentRentabilidad] Error para {rent}: {e}")
        resultado = {
            "diagnostico": f"Error del agente: {str(e)}",
            "recomendaciones": [],
            "alerta_nivel": 1,
            "resumen_ejecutivo": "Error en diagnóstico.",
        }

    if guardar:
        rent.diagnostico_ia     = resultado.get("diagnostico", "")
        rent.recomendaciones_ia = resultado.get("recomendaciones", [])
        rent.alerta_nivel       = resultado.get("alerta_nivel", 0)
        rent.calculado_por_agente = True
        rent.save(update_fields=[
            "diagnostico_ia", "recomendaciones_ia",
            "alerta_nivel", "calculado_por_agente",
        ])

    return resultado


def analizar_todas_sucursales(periodo=None):
    """
    Analiza todas las sucursales para un periodo dado.
    Si periodo es None, usa el mes actual.

    Llamada desde Celery:
        analizar_todas_sucursales.apply_async(args=[periodo])
    """
    from .models import SucursalRentabilidad  # import local para evitar circular
    from datetime import date

    if periodo is None:
        hoy = date.today()
        periodo = hoy.replace(day=1)

    qs = SucursalRentabilidad.objects.filter(periodo=periodo)
    resultados = []
    for rent in qs:
        try:
            r = analizar_sucursal(rent, guardar=True)
            resultados.append({"sucursal": str(rent.sucursal), "estado": rent.estado, "ok": True})
        except Exception as e:
            resultados.append({"sucursal": str(rent.sucursal), "error": str(e), "ok": False})
            logger.error(f"[AgentRentabilidad] Fallo en {rent}: {e}")

    return resultados
