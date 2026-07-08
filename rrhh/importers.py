from __future__ import annotations

import logging
from datetime import datetime, time

import pandas as pd
from django.db.models import Q

from core.branch_catalog import resolver_sucursal_por_texto
from rrhh.models import AsistenciaEmpleado, Empleado, ImportacionChecador
from rrhh.services import generar_horas_extra_automatico
from rrhh.services_asistencia_reglas import evaluar_dia_empleado
from rrhh.services_bonos_checador import programar_sincronizacion_bonos_desde_checador


COLUMNAS_ESPERADAS = ["id_empleado", "nombre", "fecha", "hora_entrada", "hora_salida"]
log = logging.getLogger(__name__)


def _normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    aliases = {
        "employee_id": "id_empleado",
        "no_empleado": "id_empleado",
        "numero_empleado": "id_empleado",
        "employee_name": "nombre",
        "name": "nombre",
        "date": "fecha",
        "check_in": "hora_entrada",
        "entrada": "hora_entrada",
        "check_out": "hora_salida",
        "salida": "hora_salida",
    }
    return df.rename(columns={source: target for source, target in aliases.items() if source in df.columns})


def _datetime_from_row(fecha, raw_value):
    if pd.isna(raw_value):
        return None
    fecha_obj = pd.to_datetime(fecha).date()
    if isinstance(raw_value, time):
        return datetime.combine(fecha_obj, raw_value)
    parsed = pd.to_datetime(raw_value)
    if parsed.date() == datetime.today().date() and isinstance(raw_value, str) and len(raw_value.strip()) <= 8:
        return datetime.combine(fecha_obj, parsed.time())
    return parsed.to_pydatetime()


def _buscar_empleado(row) -> Empleado:
    raw_id = str(row.get("id_empleado", "") or "").strip()
    nombre = str(row.get("nombre", "") or "").strip()
    qs = Empleado.objects.all()
    if raw_id:
        empleado = qs.filter(Q(codigo=raw_id) | Q(codigo__iexact=raw_id)).first()
        if empleado:
            return empleado
    if nombre:
        empleado = qs.filter(nombre__iexact=nombre).first()
        if empleado:
            return empleado
    raise Empleado.DoesNotExist(raw_id or nombre)


def _sucursal_de_empleado(empleado: Empleado):
    # Preferir el FK canónico (FASE 2); resolver de texto solo como respaldo.
    return empleado.sucursal_ref or resolver_sucursal_por_texto(empleado.sucursal)


def importar_excel_hikconnect(archivo, user, fecha_inicio, fecha_fin):
    """
    Lee un Excel exportado desde Hik-Connect y crea/actualiza AsistenciaEmpleado.
    Retorna dict con contadores.
    """
    df = _normalizar_columnas(pd.read_excel(archivo))

    procesados = 0
    errores = 0
    log_lines: list[str] = []

    for index, row in df.iterrows():
        try:
            empleado = _buscar_empleado(row)
            fecha = pd.to_datetime(row["fecha"]).date()
            entrada = _datetime_from_row(row["fecha"], row.get("hora_entrada"))
            salida = _datetime_from_row(row["fecha"], row.get("hora_salida"))

            minutos = 0
            if entrada and salida:
                delta = salida - entrada
                minutos = max(int(delta.total_seconds() / 60), 0)

            asistencia, _ = AsistenciaEmpleado.objects.update_or_create(
                empleado=empleado,
                fecha=fecha,
                defaults={
                    "entrada": entrada,
                    "salida": salida,
                    "minutos_trabajados": minutos,
                    "fuente": AsistenciaEmpleado.FUENTE_HIKCONNECT_EXCEL,
                    "sucursal": _sucursal_de_empleado(empleado),
                },
            )
            generar_horas_extra_automatico(asistencia)
            try:
                evaluar_dia_empleado(empleado, fecha)
            except Exception as exc:
                log.warning("Error evaluando reglas de asistencia para %s %s: %s", empleado, fecha, exc)
            else:
                programar_sincronizacion_bonos_desde_checador(empleado.id, fecha)
            procesados += 1
        except Empleado.DoesNotExist:
            errores += 1
            log_lines.append(f"Empleado no encontrado: {row.get('id_empleado') or row.get('nombre')}")
        except Exception as exc:
            errores += 1
            log_lines.append(f"Error fila {index}: {exc}")

    if hasattr(archivo, "seek"):
        archivo.seek(0)
    ImportacionChecador.objects.create(
        metodo=ImportacionChecador.METODO_EXCEL,
        archivo=archivo,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        registros_procesados=procesados,
        errores=errores,
        log="\n".join(log_lines),
        creado_por=user,
    )
    return {"procesados": procesados, "errores": errores, "log": log_lines}
