from __future__ import annotations

import os
from datetime import date, timedelta

from celery import shared_task
from django.utils import timezone


@shared_task
def sync_asistencia_point():
    """
    Futura integración: jalar checadas de PointMeUp via pos_bridge.
    """
    return {"ok": True, "procesados": 0, "fuente": "point_placeholder"}


@shared_task
def sync_asistencia_hikvision_isapi(dias: int = 1):
    """
    Sincroniza asistencia desde el checador por ISAPI/IP.
    Hik-Connect Excel queda como respaldo manual desde el comando/pantalla de carga.
    """
    from .services_hikvision import importar_asistencia_isapi

    hasta = date.today()
    desde = hasta - timedelta(days=max(int(dias), 1))
    password = os.getenv("HIKVISION_ISAPI_PASSWORD", "")
    if not password:
        return {"ok": False, "error": "HIKVISION_ISAPI_PASSWORD no configurado"}

    resultado = importar_asistencia_isapi(
        fecha_inicio=desde,
        fecha_fin=hasta,
        base_url=os.getenv("HIKVISION_ISAPI_URL", "http://127.0.0.1:28073"),
        username=os.getenv("HIKVISION_ISAPI_USER", "admin"),
        password=password,
    )
    return {"ok": True, **resultado}


@shared_task
def reconciliar_bonos_asistencia_periodo_actual():
    """Reconcilia los bonos del periodo vigente con la asistencia real (checador/Point).

    Red de seguridad automática: el trigger por-día (transaction.on_commit) puede
    saltarse días si `evaluar_dia_empleado` falla. Este recompute lee TODA la
    asistencia del periodo y reconstruye los registros diarios. Solo toca bonos
    BORRADOR; no pisa bono_extra/ajustes capturados. Idempotente.
    """
    hoy = timezone.localdate()
    resultado: dict[str, object] = {}

    from bonos_ventas.models import ConfigBonoVentasPeriodo
    from bonos_ventas.services_checador import sincronizar_asistencia_desde_checador as _sync_ventas

    periodo_ventas = ConfigBonoVentasPeriodo.objects.filter(mes=hoy.month, anio=hoy.year).first()
    resultado["ventas"] = _sync_ventas(periodo_ventas) if periodo_ventas else "sin periodo"

    from bonos_produccion.models import ConfigBonoPeriodo
    from bonos_produccion.services_checador import sincronizar_asistencia_desde_checador as _sync_prod

    periodo_prod = ConfigBonoPeriodo.objects.filter(mes=hoy.month, anio=hoy.year).first()
    resultado["produccion"] = _sync_prod(periodo_prod) if periodo_prod else "sin periodo"

    return {"ok": True, **resultado}


@shared_task
def alertar_he_pendientes():
    """
    Envía email si hay horas extra en estado pendiente por más de 24 horas.
    """
    from datetime import timedelta

    from django.core.mail import send_mail

    from .models import HoraExtra

    umbral = timezone.now() - timedelta(hours=24)
    count = HoraExtra.objects.filter(estado=HoraExtra.ESTADO_PENDIENTE, creado_en__lt=umbral).count()
    if count <= 0:
        return {"ok": True, "pendientes": 0}

    send_mail(
        subject=f"[ERP] {count} horas extra pendientes de autorización",
        message=f"Hay {count} registros de horas extra esperando autorización en el ERP.",
        from_email="no-reply@pollyanasdolce.com",
        recipient_list=["mauricio@pollyanasdolce.com"],
    )
    return {"ok": True, "pendientes": count}


@shared_task
def alertar_cuotas_quincena():
    """
    Notifica cuotas de préstamo pendientes de cobrar en la quincena actual.
    Programar en beat los días 14 y 29.
    """
    from datetime import date

    from django.core.mail import send_mail

    from .models import PrestamoCuota

    hoy = date.today()
    cuotas = PrestamoCuota.objects.filter(
        estado=PrestamoCuota.ESTADO_PENDIENTE,
        fecha_quincena__month=hoy.month,
        fecha_quincena__year=hoy.year,
    ).select_related("prestamo__empleado")

    count = cuotas.count()
    if count <= 0:
        return {"ok": True, "pendientes": 0}

    lineas = "\n".join(
        f"- {c.prestamo.empleado} | Folio {c.prestamo.folio} | ${c.monto_esperado} | Q{c.numero_quincena}"
        for c in cuotas
    )
    send_mail(
        subject=f"[ERP] {count} cuotas de préstamo pendientes esta quincena",
        message=f"Cuotas pendientes:\n\n{lineas}",
        from_email="no-reply@pollyanasdolce.com",
        recipient_list=["mauricio@pollyanasdolce.com"],
    )
    return {"ok": True, "pendientes": count}
