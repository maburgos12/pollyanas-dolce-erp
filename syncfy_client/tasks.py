from __future__ import annotations

import time

from celery import shared_task
from django.conf import settings
from django.core.mail import send_mail
from django.utils import timezone

from syncfy_client.models import CuentaBancaria, LogSyncfy
from syncfy_client.services.auth import obtener_token
from syncfy_client.services.base import SyncfyAuthError, SyncfyConfigurationError, SyncfyServiceError
from syncfy_client.services.cuentas import actualizar_cuenta_desde_syncfy, obtener_cuentas, seleccionar_account
from syncfy_client.services.transacciones import descargar_transacciones, guardar_transacciones, rango_unix_syncfy


def _alertar_error(mensaje: str) -> None:
    recipients = [
        email.strip()
        for email in getattr(settings, "SYNCFY_ALERT_EMAILS", ["maburgos12@pollyanasdolce.com"])
        if email and email.strip()
    ]
    if not recipients:
        return
    send_mail(
        subject="Error Syncfy ERP",
        message=mensaje,
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "erp@pollyanasdolce.com"),
        recipient_list=recipients,
        fail_silently=True,
    )


def _sincronizar_cuenta(cuenta: CuentaBancaria, *, token: str) -> dict[str, int | str]:
    if not cuenta.id_credential:
        LogSyncfy.objects.create(
            nivel=LogSyncfy.NIVEL_WARN,
            cuenta=cuenta,
            mensaje="Cuenta bancaria sin id_credential de Syncfy; se omite.",
        )
        return {"status": "sin_credential", "total": 0, "nuevos": 0}
    if not cuenta.id_account:
        LogSyncfy.objects.create(
            nivel=LogSyncfy.NIVEL_WARN,
            cuenta=cuenta,
            mensaje="Cuenta bancaria sin id_account de Syncfy; se omite.",
        )
        return {"status": "sin_account", "total": 0, "nuevos": 0}

    accounts = obtener_cuentas(id_credential=cuenta.id_credential, token=token)
    account = seleccionar_account(cuenta, accounts)
    if account:
        actualizar_cuenta_desde_syncfy(cuenta, account)

    dt_from, dt_to = rango_unix_syncfy()
    transacciones = descargar_transacciones(
        id_credential=cuenta.id_credential,
        id_account=cuenta.id_account,
        token=token,
        dt_refresh_from=dt_from,
        dt_refresh_to=dt_to,
    )
    total, nuevos = guardar_transacciones(cuenta=cuenta, transacciones=transacciones)
    cuenta.ultima_sync = timezone.now()
    cuenta.save(update_fields=["ultima_sync", "actualizado_en"])
    return {"status": "ok", "total": total, "nuevos": nuevos}


@shared_task(name="syncfy_client.sincronizar_movimientos_bancarios", bind=True, max_retries=2, default_retry_delay=300)
def sincronizar_movimientos_bancarios(self):
    inicio = time.monotonic()
    if not getattr(settings, "SYNCFY_ENABLED", False):
        return {"status": "deshabilitada"}

    cuentas = list(
        CuentaBancaria.objects.filter(
            activa=True,
            origen=CuentaBancaria.ORIGEN_SYNCFY,
        ).order_by("banco")
    )
    if not cuentas:
        LogSyncfy.objects.create(
            nivel=LogSyncfy.NIVEL_WARN,
            mensaje="No hay cuentas bancarias activas para sincronizar con Syncfy.",
            duracion_segundos=0,
        )
        return {"status": "sin_cuentas", "cuentas": 0, "total": 0, "nuevos": 0}

    try:
        token = obtener_token()
    except SyncfyConfigurationError as exc:
        mensaje = f"Syncfy no configurado: {exc}"
        LogSyncfy.objects.create(
            nivel=LogSyncfy.NIVEL_ERROR,
            mensaje=mensaje,
            duracion_segundos=int(time.monotonic() - inicio),
        )
        _alertar_error(mensaje)
        return {"status": "configuracion_incompleta", "error": str(exc)}
    except SyncfyServiceError as exc:
        mensaje = f"Error al autenticar Syncfy: {exc}"
        LogSyncfy.objects.create(
            nivel=LogSyncfy.NIVEL_ERROR,
            mensaje=mensaje,
            duracion_segundos=int(time.monotonic() - inicio),
        )
        _alertar_error(mensaje)
        raise self.retry(exc=exc)

    total = 0
    nuevos = 0
    errores = 0
    for cuenta in cuentas:
        cuenta_inicio = time.monotonic()
        try:
            resultado = _sincronizar_cuenta(cuenta, token=token)
        except SyncfyAuthError:
            token = obtener_token()
            resultado = _sincronizar_cuenta(cuenta, token=token)
        except SyncfyServiceError as exc:
            errores += 1
            mensaje = f"Error Syncfy en {cuenta}: {exc}"
            LogSyncfy.objects.create(
                nivel=LogSyncfy.NIVEL_ERROR,
                cuenta=cuenta,
                mensaje=mensaje,
                duracion_segundos=int(time.monotonic() - cuenta_inicio),
            )
            continue

        cuenta_total = int(resultado.get("total", 0))
        cuenta_nuevos = int(resultado.get("nuevos", 0))
        total += cuenta_total
        nuevos += cuenta_nuevos
        LogSyncfy.objects.create(
            nivel=LogSyncfy.NIVEL_INFO,
            cuenta=cuenta,
            mensaje=f"Syncfy sincronizo {cuenta_nuevos}/{cuenta_total} movimientos nuevos.",
            movimientos_nuevos=cuenta_nuevos,
            movimientos_total=cuenta_total,
            duracion_segundos=int(time.monotonic() - cuenta_inicio),
        )

    duracion = int(time.monotonic() - inicio)
    if errores:
        mensaje = f"Syncfy finalizo con {errores} cuenta(s) en error; {nuevos}/{total} movimientos nuevos."
        LogSyncfy.objects.create(
            nivel=LogSyncfy.NIVEL_WARN,
            mensaje=mensaje,
            movimientos_nuevos=nuevos,
            movimientos_total=total,
            duracion_segundos=duracion,
        )
        _alertar_error(mensaje)
        return {"status": "parcial", "cuentas": len(cuentas), "errores": errores, "total": total, "nuevos": nuevos}

    LogSyncfy.objects.create(
        nivel=LogSyncfy.NIVEL_INFO,
        mensaje=f"Syncfy nocturno finalizado: {nuevos}/{total} movimientos nuevos.",
        movimientos_nuevos=nuevos,
        movimientos_total=total,
        duracion_segundos=duracion,
    )
    return {"status": "ok", "cuentas": len(cuentas), "total": total, "nuevos": nuevos, "duracion_segundos": duracion}
