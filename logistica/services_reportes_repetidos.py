"""Evita que la inspección diaria abra un ticket nuevo por el mismo desperfecto.

La inspección de salida se captura todos los días. Una condición que no se ha
reparado (un testigo encendido, unas llantas lisas) se vuelve a marcar cada
mañana, y hasta ahora cada captura abría un `ReporteUnidad` nuevo: la bandeja de
mantenimiento terminó con decenas de copias del mismo pendiente.

Cuando la observación de hoy equivale a un reporte que sigue abierto para esa
unidad, se registra una reafirmación sobre el existente —el modelo ya existía
para el reporte manual— en lugar de crear otro ticket.
"""

import re
import unicodedata

from django.db import transaction
from rapidfuzz import fuzz

from core.audit import log_event
from core.duplicados import enlazar_duplicado

from .models import ReporteUnidad, ReporteUnidadReafirmacion

PREFIJO_INSPECCION = "Falla detectada en inspección diaria:"

# Umbral de token_set_ratio para considerar que dos observaciones hablan del
# mismo desperfecto. Alto a propósito: preferimos dejar pasar un duplicado antes
# que fundir dos problemas distintos en un solo ticket. Con 92 "check encendido"
# y "check engine encendido" quedan juntos, y "luz baja no enciende" contra
# "las luces delanteras fallan" quedan separados.
UMBRAL_EQUIVALENCIA = 92


def normalizar(texto: str) -> str:
    """Minúsculas, sin acentos ni puntuación, espacios colapsados."""

    limpio = (texto or "").replace(PREFIJO_INSPECCION, " ")
    limpio = unicodedata.normalize("NFKD", limpio).encode("ascii", "ignore").decode("ascii")
    limpio = re.sub(r"[^a-zA-Z0-9\s]", " ", limpio).lower()
    return " ".join(limpio.split())


def son_equivalentes(observacion: str, descripcion: str) -> bool:
    a, b = normalizar(observacion), normalizar(descripcion)
    if not a or not b:
        return False
    if a == b:
        return True
    return fuzz.token_set_ratio(a, b) >= UMBRAL_EQUIVALENCIA


def buscar_reporte_abierto_equivalente(unidad, observaciones: str):
    """Devuelve el reporte abierto de esa unidad que ya describe lo mismo, o None."""

    if not (observaciones or "").strip():
        return None
    abiertos = (
        ReporteUnidad.objects.filter(unidad=unidad)
        .exclude(estatus=ReporteUnidad.ESTATUS_CERRADO)
        .filter(duplicado_de__isnull=True)
        .order_by("-fecha_reporte")
    )
    for reporte in abiertos:
        if son_equivalentes(observaciones, reporte.descripcion):
            return reporte
    return None


def reafirmar_desde_inspeccion(reporte, repartidor, observaciones, *, latitud=None, longitud=None, ip=None):
    """Registra que el desperfecto sigue presente, sin abrir otro ticket."""

    return ReporteUnidadReafirmacion.objects.create(
        reporte=reporte,
        repartidor=repartidor,
        comentario=f"Sigue presente en la inspección diaria: {observaciones}".strip(),
        latitud=latitud or None,
        longitud=longitud or None,
        ip_registro=ip,
    )


@transaction.atomic
def marcar_duplicado_unidad(reporte, principal, usuario):
    """Liga un reporte de unidad repetido al que ya se está atendiendo.

    A diferencia de fallas, aquí no hay bitácora por reporte: el rastro queda en
    el AuditLog y en el contador de repetidos que muestra la bandeja.
    """

    reporte = ReporteUnidad.objects.select_for_update().get(pk=reporte.pk)
    principal = ReporteUnidad.objects.select_for_update().get(pk=principal.pk)
    destino = enlazar_duplicado(reporte, principal, estatus_cerrados=(ReporteUnidad.ESTATUS_CERRADO,))

    log_event(
        usuario,
        "UPDATE",
        "logistica.ReporteUnidad",
        str(reporte.pk),
        {"duplicado_de": destino.pk, "unidad": destino.unidad.codigo},
    )
    return destino


def propagar_cierre_unidad(principal, estatus):
    """Arrastra a los repetidos cuando el principal se cierra o avanza."""

    duplicados = list(ReporteUnidad.objects.filter(duplicado_de=principal).exclude(estatus=estatus))
    for duplicado in duplicados:
        duplicado.estatus = estatus
        duplicado.save(update_fields=["estatus"])
    return [d.pk for d in duplicados]
