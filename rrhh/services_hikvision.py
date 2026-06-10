from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Any

import requests
from django.utils import timezone

from .models import AsistenciaEmpleado, Empleado, EmpleadoIdentidadPendiente, ImportacionChecador, Turno
from .services import generar_horas_extra_automatico
from .services_asistencia_reglas import evaluar_dia_empleado
from .services_identidad import buscar_empleado_por_codigo, registrar_identidad_pendiente

log = logging.getLogger("rrhh.hikvision")

ESTADO_ENTRADA = {"checkIn", "overtimeIn"}
ESTADO_SALIDA = {"checkOut", "overtimeOut"}
VENTANA_DUPLICADO_MINUTOS = 5
OBS_REVISION_TRES_MARCAJES = "REVISIÓN: 3 marcajes"
OBS_MARCAJES_EXTRA = "Marcajes extra"
OBS_TECNICAS_HIK_PREFIXES = ("breakOut@", "breakIn@", "checkIn@", "checkOut@", "overtimeIn@", "overtimeOut@")


@dataclass(frozen=True)
class MarcaHik:
    dt: Any
    status: str
    serial_no: Any = None
    nueva: bool = True


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


def _minutos_entre(inicio, fin) -> int:
    if not inicio or not fin:
        return 0
    return max(int((fin - inicio).total_seconds() / 60), 0)


def _marcas_existentes(asistencia: AsistenciaEmpleado) -> list[MarcaHik]:
    marcas = []
    if asistencia.entrada:
        marcas.append(MarcaHik(asistencia.entrada, "checkIn", nueva=False))
    if asistencia.salida_comida:
        marcas.append(MarcaHik(asistencia.salida_comida, "checkIn", nueva=False))
    if asistencia.regreso_comida:
        marcas.append(MarcaHik(asistencia.regreso_comida, "checkIn", nueva=False))
    if asistencia.salida:
        marcas.append(MarcaHik(asistencia.salida, "checkOut", nueva=False))
    return marcas


def _es_marca_cercana(a, b) -> bool:
    return abs((timezone.localtime(a) - timezone.localtime(b)).total_seconds()) <= VENTANA_DUPLICADO_MINUTOS * 60


def _filtrar_marcas_cercanas(marcas: list[MarcaHik]) -> tuple[list[MarcaHik], int]:
    aceptadas: list[MarcaHik] = []
    duplicados = 0
    for marca in sorted(marcas, key=lambda item: timezone.localtime(item.dt)):
        if any(_es_marca_cercana(marca.dt, aceptada.dt) for aceptada in aceptadas):
            if marca.nueva:
                duplicados += 1
            continue
        aceptadas.append(marca)
    return aceptadas, duplicados


def _actualizar_observacion_hik(asistencia: AsistenciaEmpleado, notas: list[str]) -> None:
    partes_actuales = [
        parte.strip()
        for parte in (asistencia.observacion or "").split(" | ")
        if parte.strip()
        and not parte.strip().startswith(OBS_REVISION_TRES_MARCAJES)
        and not parte.strip().startswith(OBS_MARCAJES_EXTRA)
        and not parte.strip().startswith(OBS_TECNICAS_HIK_PREFIXES)
    ]
    asistencia.observacion = " | ".join([*partes_actuales, *notas])


def _aplicar_marcajes(asistencia: AsistenciaEmpleado, marcas_nuevas: list[MarcaHik]) -> tuple[int, str]:
    marcas, duplicados = _filtrar_marcas_cercanas([*_marcas_existentes(asistencia), *marcas_nuevas])
    marcas = sorted(marcas, key=lambda item: timezone.localtime(item.dt))
    notas: list[str] = []

    asistencia.entrada = marcas[0].dt if len(marcas) >= 1 else None
    asistencia.salida_comida = None
    asistencia.regreso_comida = None
    asistencia.salida = None

    if len(marcas) == 2:
        if marcas[1].status in ESTADO_SALIDA:
            asistencia.salida = marcas[1].dt
        else:
            asistencia.salida_comida = marcas[1].dt
    elif len(marcas) == 3:
        asistencia.salida_comida = marcas[1].dt
        asistencia.regreso_comida = marcas[2].dt
        notas.append(OBS_REVISION_TRES_MARCAJES)
    elif len(marcas) >= 4:
        asistencia.salida_comida = marcas[1].dt
        asistencia.regreso_comida = marcas[2].dt
        asistencia.salida = marcas[-1].dt
        if len(marcas) > 4:
            extras = ", ".join(timezone.localtime(marca.dt).strftime("%H:%M") for marca in marcas[3:-1])
            if extras:
                notas.append(f"{OBS_MARCAJES_EXTRA}: {extras}")

    asistencia.minutos_comida = _minutos_entre(asistencia.salida_comida, asistencia.regreso_comida)
    if asistencia.entrada and asistencia.salida:
        if asistencia.salida_comida and asistencia.regreso_comida:
            asistencia.minutos_trabajados = _minutos_entre(asistencia.entrada, asistencia.salida_comida) + _minutos_entre(
                asistencia.regreso_comida,
                asistencia.salida,
            )
        else:
            asistencia.minutos_trabajados = _minutos_entre(asistencia.entrada, asistencia.salida)
    else:
        asistencia.minutos_trabajados = 0

    asistencia.fuente = AsistenciaEmpleado.FUENTE_HIKCONNECT_API
    _actualizar_observacion_hik(asistencia, notas)
    return duplicados, f"{len(marcas)} marcajes aplicados"


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
    grupos = defaultdict(list)

    for ev in eventos:
        emp_no = str(ev.get("employee_no", "")).strip()
        status = str(ev.get("attendance_status", "")).strip()
        time_s = str(ev.get("time", "")).strip()
        if not emp_no or not status or not time_s:
            errores += 1
            detalle.append({"employee_no": emp_no, "resultado": "campos faltantes"})
            continue

        empleado = buscar_empleado_por_codigo(emp_no)
        if not empleado:
            registrar_identidad_pendiente(
                fuente=EmpleadoIdentidadPendiente.FUENTE_HIKVISION,
                codigo_externo=emp_no,
                nombre_externo=str(ev.get("name", "")).strip(),
                notas="Detectado automáticamente desde checador.",
            )
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
        grupos[(empleado.pk, fecha)].append((empleado, ev, MarcaHik(dt=dt, status=status, serial_no=ev.get("serial_no"))))

    for (_, fecha), registros in grupos.items():
        empleado = registros[0][0]
        asistencia, creada = AsistenciaEmpleado.objects.get_or_create(
            empleado=empleado,
            fecha=fecha,
            defaults={
                "fuente": AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
                "sucursal": _resolver_sucursal(empleado),
            },
        )

        marcas_nuevas = [registro[2] for registro in registros]
        duplicados_grupo, resultado = _aplicar_marcajes(asistencia, marcas_nuevas)
        duplicados += duplicados_grupo
        procesados_grupo = max(len(marcas_nuevas) - duplicados_grupo, 0)
        procesados += procesados_grupo

        if not asistencia.turno_id:
            turno = _detectar_turno(timezone.localtime(asistencia.entrada).time()) if asistencia.entrada else None
            if turno:
                asistencia.turno = turno

        asistencia.save()
        if asistencia.salida and asistencia.turno_id:
            try:
                generar_horas_extra_automatico(asistencia)
            except Exception as exc:
                log.warning("Error generando horas extra para %s: %s", empleado, exc)
        try:
            evaluar_dia_empleado(empleado, fecha)
        except Exception as exc:
            log.warning("Error evaluando reglas de asistencia para %s %s: %s", empleado, fecha, exc)

        for _, ev, marca in registros:
            marca_duplicada = any(_es_marca_cercana(marca.dt, existente.dt) for existente in _marcas_existentes(asistencia) if existente.dt != marca.dt)
            detalle.append(
                {
                    "employee_no": str(ev.get("employee_no", "")).strip(),
                    "fecha": str(fecha),
                    "status": marca.status,
                    "resultado": "marca duplicada" if marca_duplicada else ("creado" if creada else resultado),
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
