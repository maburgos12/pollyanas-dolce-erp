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
def consumir_goce_vacaciones_completado():
    """Convierte reservas aprobadas en goce consumido al día siguiente de su fin."""
    from .services_vacaciones import consumir_solicitudes_vacaciones_completadas

    fecha_corte = timezone.localdate()
    consumidas = consumir_solicitudes_vacaciones_completadas(fecha_corte=fecha_corte)
    return {
        "ok": True,
        "consumidas": consumidas,
        "fecha_corte": fecha_corte.isoformat(),
    }


@shared_task
def evaluar_asistencia_diaria(dias: int = 1):
    """Barrido diario del motor de asistencia: evalúa también a quien NO checó.

    Sustituye la evaluación de ausentes que hacía el polling ISAPI al NAS
    (retirado): las checadas llegan por el receptor API de Hik-Connect, pero
    solo este barrido genera la falta de quien no tiene registro. Las
    solicitudes de vacaciones vigentes (reservadas o aprobadas) concilian la
    falta automáticamente.
    """
    from .services_asistencia_reglas import evaluar_rango_asistencia

    hasta = timezone.localdate() - timedelta(days=1)
    desde = hasta - timedelta(days=max(int(dias), 1) - 1)
    resultado = evaluar_rango_asistencia(desde, hasta)
    return {
        "ok": True,
        "desde": desde.isoformat(),
        "hasta": hasta.isoformat(),
        "evaluados": resultado.evaluados,
        "creados": resultado.creados,
        "actualizados": resultado.actualizados,
        "resueltos": resultado.resueltos,
    }


def _hallazgos_auditoria_vacaciones() -> list[str]:
    """Detecta anomalías del control de vacaciones por bolsas (auditoría diaria)."""
    from .models import (
        AplicacionGoceVacaciones,
        PeriodoVacacional,
        SolicitudVacaciones,
    )
    from .services_vacaciones_saldos import saldo_periodo_vacacional

    hoy = timezone.localdate()
    hallazgos: list[str] = []

    for periodo in PeriodoVacacional.objects.select_related("empleado"):
        saldo = saldo_periodo_vacacional(periodo)
        usado = saldo.reservado + saldo.gozado
        if usado > periodo.dias_generados:
            hallazgos.append(
                f"Bolsa sobregirada: {periodo.empleado.nombre} · periodo "
                f"{periodo.aniversario} usa {usado} de {periodo.dias_generados} días."
            )

    colgadas = SolicitudVacaciones.objects.filter(
        estado=SolicitudVacaciones.ESTADO_APROBADA,
        fecha_fin__lt=hoy - timedelta(days=1),
        aplicaciones_goce__estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
    ).select_related("empleado").distinct()
    for solicitud in colgadas:
        hallazgos.append(
            f"Reserva colgada: {solicitud.folio} ({solicitud.empleado.nombre}) "
            f"terminó el {solicitud.fecha_fin} y su goce sigue reservado."
        )

    # Patrón Carmina: reserva pendiente de aprobar descontando de un periodo
    # que no corresponde a las fechas de la vacación. Se avisa ANTES de aprobar.
    reservas = AplicacionGoceVacaciones.objects.filter(
        estado=AplicacionGoceVacaciones.ESTADO_RESERVADA,
        solicitud__estado__in=[
            SolicitudVacaciones.ESTADO_SOLICITADA,
            SolicitudVacaciones.ESTADO_PREAUTORIZADA,
        ],
    ).select_related("solicitud__empleado", "periodo")
    for aplicacion in reservas:
        inicio_periodo = aplicacion.periodo.aniversario
        try:
            fin_periodo = inicio_periodo.replace(year=inicio_periodo.year + 1)
        except ValueError:  # 29 de febrero
            fin_periodo = inicio_periodo.replace(year=inicio_periodo.year + 1, day=28)
        fecha = aplicacion.solicitud.fecha_inicio
        if fecha < inicio_periodo or fecha >= fin_periodo:
            hallazgos.append(
                f"Cruce de periodo por revisar: {aplicacion.solicitud.folio} "
                f"({aplicacion.solicitud.empleado.nombre}, vacación del "
                f"{aplicacion.solicitud.fecha_inicio}) descuenta {aplicacion.dias} días "
                f"del periodo {inicio_periodo.year}. Verificar antes de aprobar."
            )

    return hallazgos


@shared_task
def auditar_vacaciones_diaria():
    """Audita saldos de vacaciones y avisa por correo solo si hay hallazgos."""
    from django.contrib.auth import get_user_model
    from django.core.mail import send_mail

    hallazgos = _hallazgos_auditoria_vacaciones()
    if not hallazgos:
        return {"ok": True, "hallazgos": 0}

    destinatarios = ["mauricio@pollyanasdolce.com"]
    paula = get_user_model().objects.filter(username="paula.lugo").exclude(email="").first()
    if paula:
        destinatarios.append(paula.email)

    cuerpo = (
        "Auditoría diaria del control de vacaciones. Casos que requieren revisión:\n\n- "
        + "\n- ".join(hallazgos)
        + "\n\nEste aviso solo se envía cuando hay algo que revisar."
    )
    send_mail(
        subject=f"[ERP] Vacaciones: {len(hallazgos)} caso(s) por revisar",
        message=cuerpo,
        from_email="maburgos12@pollyanasdolce.com",
        recipient_list=destinatarios,
    )
    return {"ok": True, "hallazgos": len(hallazgos), "destinatarios": destinatarios}


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
