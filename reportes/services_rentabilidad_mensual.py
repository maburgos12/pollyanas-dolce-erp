"""Resultado mensual compartido por la tarea y las vistas de Rentabilidad."""
from collections import defaultdict
from decimal import Decimal
from urllib.parse import urlsplit

from .services_rentabilidad_gastos import leer_gastos_mensuales
from .services_rentabilidad_personal import leer_personal_mensual

ZERO = Decimal("0")
ETIQUETAS = {
    "renta": "Renta", "electricidad": "Electricidad (CFE)",
    "telefono": "Teléfono e internet", "sistemas": "Sistemas",
    "alarmas": "Alarmas y seguridad", "mantenimiento": "Mantenimiento",
    "otros": "Otros gastos recurrentes", "nomina": "Nómina ERP",
    "cargas_patronales": "Cargas patronales",
}
CAMPO_FAMILIA = {
    "renta": "renta", "nomina": "nomina_directa", "cargas_patronales": "nomina_directa",
    "electricidad": "servicios_luz_agua", "telefono": "servicios_luz_agua",
    "sistemas": "otros_gastos_fijos", "alarmas": "otros_gastos_fijos", "otros": "otros_gastos_fijos",
    "mantenimiento": "mantenimiento",
}
REQUERIDAS = {"renta", "electricidad", "telefono", "sistemas", "alarmas", "nomina", "cargas_patronales"}
# Familias sin fuente automatizada en el ERP: se siguen reportando como pendientes,
# pero no cuentan en el denominador de cobertura ni apagan el punto de equilibrio.
# Retirar de aquí en cuanto SIPARE alimente monto_real/cedula_imss por sucursal.
SIN_FUENTE_ACTIVA = {"cargas_patronales"}
AREAS_EXCLUIDAS = {"produccion", "nomina", "resultados", "logistica"}


def _campo(fila):
    if fila.get("area") == "administracion":
        return "gastos_admin_prorrateados"
    return CAMPO_FAMILIA.get(fila.get("familia"))


def leer_costos_mensuales(periodo):
    """Sin efectos laterales; las dos fuentes se consultan una vez por mes."""
    gastos, personal = leer_gastos_mensuales(periodo), leer_personal_mensual(periodo)
    return {
        "periodo": periodo.replace(day=1),
        "filas": gastos["filas"] + personal["filas"],
        "pendientes": gastos["pendientes"] + personal["pendientes"],
    }


def costos_de_sucursal(resultado, sucursal_id):
    """Cobertura por sucursal: mide qué se pudo cargar en vez de aprobar o reprobar.

    Un pendiente sin sucursal asignada es un problema del catálogo, no de esta
    sucursal: se informa aparte y no reduce su cobertura.
    """
    filas = [dict(f) for f in resultado["filas"]
             if f["sucursal_id"] == sucursal_id and f["area"] not in AREAS_EXCLUIDAS]
    pendientes = [dict(f) for f in resultado["pendientes"]
                  if f["sucursal_id"] == sucursal_id and f["area"] not in AREAS_EXCLUIDAS]
    pendientes_globales = [dict(f) for f in resultado["pendientes"]
                           if f["sucursal_id"] is None and f["area"] not in AREAS_EXCLUIDAS]
    presentes = {f["familia"] for f in filas}
    faltantes = REQUERIDAS - presentes
    ya_pendientes = {f["familia"] for f in pendientes}
    for familia in sorted(faltantes - ya_pendientes):
        pendientes.append({
            "familia": familia, "concepto": ETIQUETAS[familia], "sucursal_id": sucursal_id,
            "origen": "COBERTURA", "registro_id": None, "estado": "PENDIENTE",
            "detalle": "Falta confirmar el gasto del mes o documentar que no aplica a esta sucursal.",
        })
    totales = defaultdict(lambda: ZERO)
    campos = defaultdict(lambda: ZERO)
    for fila in filas:
        familia = fila["familia"]
        soporte = fila.get("soporte") or {}
        archivo = soporte.get("archivo", "") if isinstance(soporte, dict) else soporte
        fila["soporte_referencia"] = archivo
        try:
            url = urlsplit(archivo)
            fila["soporte_url"] = archivo if url.scheme in {"http", "https"} and url.netloc else ""
        except ValueError:
            fila["soporte_url"] = ""
        fila["familia_display"] = ETIQUETAS.get(familia, "Pendiente de clasificación")
        totales[familia] += fila["monto_mensual"]
        campo = _campo(fila)
        if campo:
            campos[campo] += fila["monto_mensual"]
    exigibles = REQUERIDAS - SIN_FUENTE_ACTIVA
    cubiertas = exigibles & presentes
    faltantes_exigibles = exigibles - presentes
    bloqueantes = [p for p in pendientes if p["familia"] not in SIN_FUENTE_ACTIVA]
    return {
        "filas": filas, "pendientes": pendientes, "pendientes_globales": pendientes_globales,
        "totales": dict(totales), "campos": dict(campos), "total": sum(totales.values(), ZERO),
        "familias_cubiertas": sorted(cubiertas),
        "familias_faltantes": sorted(faltantes_exigibles),
        "familias_faltantes_display": [ETIQUETAS[f] for f in sorted(faltantes_exigibles)],
        "familias_sin_fuente": sorted(SIN_FUENTE_ACTIVA & faltantes),
        "cobertura_pct": int(round(100 * len(cubiertas) / len(exigibles))) if exigibles else 0,
        "completo": bool(filas) and not bloqueantes and not faltantes_exigibles
        and all(f["estado"] == "COMPLETO" for f in filas),
    }


def aplicar_costos_en_memoria(rentabilidad, resultado):
    """Único adaptador del snapshot. Nunca guarda; el caller decide la escritura.

    Una familia completamente ausente conserva el valor previo y se etiqueta
    como no actualizado. Los importes parciales actuales se muestran como tales.
    """
    resumen = costos_de_sucursal(resultado, rentabilidad.sucursal_id)
    conservados = []
    campos_incompletos = {_campo(p) for p in resumen["pendientes"]}
    for campo in set(CAMPO_FAMILIA.values()) | {"gastos_admin_prorrateados"}:
        if getattr(rentabilidad, campo) and campo in campos_incompletos:
            conservados.append(campo)
        elif campo in resumen["campos"]:
            setattr(rentabilidad, campo, resumen["campos"][campo])
        elif getattr(rentabilidad, campo):
            conservados.append(campo)
    resumen["campos_previos_conservados"] = conservados
    resumen["completo"] = resumen["completo"] and not conservados
    rentabilidad.gastos_mensuales = resumen
    rentabilidad._fuente_gastos_completa = resumen["completo"]
    rentabilidad.calcular_estado()
    return resumen
