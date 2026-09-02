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
    filas = [dict(f) for f in resultado["filas"]
             if f["sucursal_id"] == sucursal_id and f["area"] not in AREAS_EXCLUIDAS]
    pendientes = [dict(f) for f in resultado["pendientes"]
                  if f["sucursal_id"] in (sucursal_id, None) and f["area"] not in AREAS_EXCLUIDAS]
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
    return {
        "filas": filas, "pendientes": pendientes, "totales": dict(totales), "campos": dict(campos),
        "total": sum(totales.values(), ZERO), "completo": bool(filas) and not pendientes
        and not faltantes and all(f["estado"] == "COMPLETO" for f in filas),
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
