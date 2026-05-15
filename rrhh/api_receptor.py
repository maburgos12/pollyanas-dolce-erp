from __future__ import annotations

import hmac
import json
import logging
from datetime import datetime, time as dtime

from django.conf import settings
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .models import AsistenciaEmpleado, Empleado, Turno
from .services import generar_horas_extra_automatico

log = logging.getLogger("rrhh.receptor_hik")

ESTADO_ENTRADA = {"checkIn", "overtimeIn"}
ESTADO_SALIDA = {"checkOut", "overtimeOut"}


def _auth_ok(request) -> bool:
    key = (request.headers.get("X-API-Key") or "").strip()
    if not key:
        return False

    expected = (getattr(settings, "ERP_PUBLIC_API_KEY", "") or "").strip()
    if expected and hmac.compare_digest(key, expected):
        return True

    try:
        from integraciones.models import PublicApiClient

        client = PublicApiClient.objects.filter(clave_prefijo=key[:12], activo=True).first()
        if client and client.validate(key):
            client.mark_used()
            return True
    except Exception as exc:
        log.warning("No se pudo validar PublicApiClient para receptor Hik: %s", exc)
    return False


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


@csrf_exempt
@require_POST
def receptor_asistencia_hik(request):
    if not _auth_ok(request):
        return JsonResponse({"error": "No autorizado"}, status=401)

    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "JSON invalido"}, status=400)

    eventos = body.get("eventos", [])
    if not isinstance(eventos, list):
        return JsonResponse({"error": "eventos debe ser una lista"}, status=400)

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

    log.info("Receptor Hik: procesados=%d errores=%d duplicados=%d", procesados, errores, duplicados)
    return JsonResponse(
        {
            "procesados": procesados,
            "errores": errores,
            "duplicados": duplicados,
            "detalle": detalle,
        }
    )
