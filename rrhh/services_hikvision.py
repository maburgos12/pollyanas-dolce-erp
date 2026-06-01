from __future__ import annotations

import logging
from datetime import datetime, time as dtime
from typing import Any

import requests
from django.utils import timezone

from .models import AsistenciaEmpleado, Empleado, ImportacionChecador, Turno
from .services import generar_horas_extra_automatico

log = logging.getLogger("rrhh.hikvision")

ESTADO_ENTRADA = {"checkIn", "overtimeIn"}
ESTADO_SALIDA = {"checkOut", "overtimeOut"}


def _parse_hik_time(time_str: str):
    if not time_str:
        return None
    try:
        dt = datetime.fromisoformat(time_str)
        if timezone.is_naive(dt):
            return timezone.make_aware(dt, timezone.get_current_timezone())
        return dt
    except Exception:
        log.warning("No se pudo parsear tiempo Hikvision: %s", time_str)
        return None


def _resolver_sucursal(empleado: Empleado):
    raw = getattr(empleado, "sucursal", None)
    if not raw:
        return None
    if hasattr(raw, "pk"):
        return raw
    nombre = str(raw).strip()
    if not nombre:
        return None
    try:
        from core.models import Sucursal

        return Sucursal.objects.filter(codigo__iexact=nombre).first() or Sucursal.objects.filter(nombre__iexact=nombre).first()
    except Exception:
        return None


def _detectar_turno(hora_entrada: dtime) -> Turno | None:
    mejor = None
    menor_diff = None
    base = timezone.localdate()
    dt_evento = datetime.combine(base, hora_entrada)
    for turno in Turno.objects.filter(activo=True):
        dt_turno = datetime.combine(base, turno.hora_entrada)
        diff = abs((dt_evento - dt_turno).total_seconds() / 60)
        if diff <= 90 and (menor_diff is None or diff < menor_diff):
            menor_diff = diff
            mejor = turno
    return mejor


def _aplicar_evento(asistencia: AsistenciaEmpleado, status: str, dt) -> str:
    if status in ESTADO_ENTRADA:
        if asistencia.entrada:
            return "entrada duplicada"
        asistencia.entrada = dt
        asistencia.fuente = AsistenciaEmpleado.FUENTE_HIKCONNECT_API
        return "entrada registrada"

    if status in ESTADO_SALIDA:
        asistencia.salida = dt
        asistencia.fuente = AsistenciaEmpleado.FUENTE_HIKCONNECT_API
        return "salida registrada"

    obs = asistencia.observacion or ""
    hora = timezone.localtime(dt).strftime("%H:%M")
    asistencia.observacion = f"{obs} | {status}@{hora}".strip(" |")
    asistencia.fuente = AsistenciaEmpleado.FUENTE_HIKCONNECT_API
    return "observacion registrada"


def _status_desde_label(label: str) -> str:
    label_norm = (label or "").strip().lower()
    if label_norm in {"entrada", "check in", "checkin"}:
        return "checkIn"
    if label_norm in {"salida", "check out", "checkout"}:
        return "checkOut"
    return ""


def normalizar_eventos_isapi(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Convierte la respuesta ISAPI AcsEvent en eventos canónicos del receptor RRHH.
    Se omiten filas técnicas sin empleado, fecha o estado de asistencia.
    """
    info_list = ((payload or {}).get("AcsEvent") or {}).get("InfoList") or []
    eventos: list[dict[str, Any]] = []
    for item in info_list:
        employee_no = str(item.get("employeeNoString") or item.get("employeeNo") or "").strip()
        time_s = str(item.get("time") or "").strip()
        status = str(item.get("attendanceStatus") or "").strip() or _status_desde_label(str(item.get("label") or ""))
        if not employee_no or not time_s or not status:
            continue
        eventos.append(
            {
                "employee_no": employee_no,
                "name": str(item.get("name") or "").strip(),
                "attendance_status": status,
                "time": time_s,
                "serial_no": item.get("serialNo"),
                "source": "hikvision_isapi",
            }
        )
    return eventos


def procesar_eventos_hik(eventos: list[dict[str, Any]]) -> dict[str, Any]:
    procesados = 0
    errores = 0
    duplicados = 0
    detalle = []

    for ev in eventos:
        emp_no = str(ev.get("employee_no", "")).strip()
        status = str(ev.get("attendance_status", "")).strip()
        time_s = str(ev.get("time", "")).strip()
        if not emp_no or not status or not time_s:
            errores += 1
            detalle.append({"employee_no": emp_no, "resultado": "campos faltantes"})
            continue

        empleado = Empleado.objects.filter(codigo=emp_no).first()
        if not empleado:
            errores += 1
            log.warning("Empleado no encontrado desde Hikvision: codigo=%s nombre=%s", emp_no, ev.get("name", ""))
            detalle.append({"employee_no": emp_no, "resultado": "empleado no encontrado"})
            continue

        dt = _parse_hik_time(time_s)
        if not dt:
            errores += 1
            detalle.append({"employee_no": emp_no, "resultado": "fecha invalida"})
            continue

        local_dt = timezone.localtime(dt)
        fecha = local_dt.date()
        asistencia, creada = AsistenciaEmpleado.objects.get_or_create(
            empleado=empleado,
            fecha=fecha,
            defaults={
                "fuente": AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
                "sucursal": _resolver_sucursal(empleado),
            },
        )

        resultado = _aplicar_evento(asistencia, status, dt)
        if resultado == "entrada duplicada":
            duplicados += 1
            detalle.append({"employee_no": emp_no, "fecha": str(fecha), "resultado": resultado})
            continue

        if asistencia.entrada and asistencia.salida:
            delta = asistencia.salida - asistencia.entrada
            asistencia.minutos_trabajados = max(int(delta.total_seconds() / 60), 0)

        if not asistencia.turno_id:
            turno = _detectar_turno(local_dt.time())
            if turno:
                asistencia.turno = turno

        asistencia.save()
        if asistencia.salida and asistencia.turno_id:
            try:
                generar_horas_extra_automatico(asistencia)
            except Exception as exc:
                log.warning("Error generando horas extra para %s: %s", empleado, exc)

        procesados += 1
        detalle.append(
            {
                "employee_no": emp_no,
                "fecha": str(fecha),
                "status": status,
                "resultado": "creado" if creada else resultado,
            }
        )

    log.info("Eventos Hikvision: procesados=%d errores=%d duplicados=%d", procesados, errores, duplicados)
    return {"procesados": procesados, "errores": errores, "duplicados": duplicados, "detalle": detalle}


def obtener_eventos_isapi(
    *,
    fecha_inicio,
    fecha_fin,
    base_url: str,
    username: str,
    password: str,
    session=None,
    page_size: int = 200,
    max_pages: int = 200,
) -> list[dict[str, Any]]:
    session = session or requests.Session()
    base = base_url.rstrip("/")
    url = f"{base}/ISAPI/AccessControl/AcsEvent?format=json"
    auth = requests.auth.HTTPDigestAuth(username, password)
    eventos: list[dict[str, Any]] = []
    position = 0

    for page in range(max_pages):
        payload = {
            "AcsEventCond": {
                "searchID": f"erp-hik-{fecha_inicio:%Y%m%d}-{fecha_fin:%Y%m%d}-{page}",
                "searchResultPosition": position,
                "maxResults": page_size,
                "major": 0,
                "minor": 0,
                "startTime": f"{fecha_inicio.isoformat()}T00:00:00-07:00",
                "endTime": f"{fecha_fin.isoformat()}T23:59:59-07:00",
            }
        }
        response = session.post(url, json=payload, auth=auth, timeout=30)
        response.raise_for_status()
        data = response.json()
        eventos.extend(normalizar_eventos_isapi(data))

        acs = data.get("AcsEvent") or {}
        num_matches = int(acs.get("numOfMatches") or 0)
        if acs.get("responseStatusStrg") != "MORE" or num_matches <= 0:
            break
        position += num_matches
    else:
        log.warning("Consulta ISAPI alcanzó max_pages=%s para %s a %s", max_pages, fecha_inicio, fecha_fin)

    return eventos


def importar_asistencia_isapi(
    *,
    fecha_inicio,
    fecha_fin,
    base_url: str,
    username: str,
    password: str,
    user=None,
    session=None,
) -> dict[str, Any]:
    eventos = obtener_eventos_isapi(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        base_url=base_url,
        username=username,
        password=password,
        session=session,
    )
    resultado = procesar_eventos_hik(eventos)
    ImportacionChecador.objects.create(
        metodo=ImportacionChecador.METODO_API,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        registros_procesados=resultado["procesados"],
        errores=resultado["errores"],
        log=(
            "ISAPI/IP checador principal. "
            f"Eventos leidos={len(eventos)} duplicados={resultado['duplicados']}."
        ),
        creado_por=user,
    )
    return {**resultado, "eventos_leidos": len(eventos), "fuente": "hikvision_isapi"}
