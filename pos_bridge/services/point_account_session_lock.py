from __future__ import annotations

from contextlib import contextmanager
import time

from django.db import connection


# Point invalida la sesión anterior cuando la misma cuenta inicia sesión de
# nuevo. Este candado debe envolver operaciones que mantengan una sesión Point
# abierta durante varios requests (navegador o HTTP).
POINT_ACCOUNT_SESSION_LOCK_ID = 7_532_026_080_700_001


@contextmanager
def point_account_session_lock(*, wait: bool, timeout_seconds: float | None = None):
    if connection.vendor != "postgresql":
        raise RuntimeError("La coordinación de sesiones Point requiere PostgreSQL.")

    from pos_bridge.services.catalog_recipe_execution import remaining_seconds
    bounded = wait and (timeout_seconds is not None or remaining_seconds() is not None)
    query = "SELECT pg_advisory_lock(%s)" if wait and not bounded else "SELECT pg_try_advisory_lock(%s)"
    lock_deadline = time.monotonic() + (60 if timeout_seconds is None else timeout_seconds)
    while True:
        with connection.cursor() as cursor:
            cursor.execute(query, [POINT_ACCOUNT_SESSION_LOCK_ID])
            acquired = True if wait and not bounded else bool(cursor.fetchone()[0])
        if acquired or not bounded:
            break
        remaining_seconds()
        remaining = lock_deadline - time.monotonic()
        if remaining <= 0:
            if timeout_seconds is not None:
                break
            raise TimeoutError("Point está ocupado con otra sincronización. Reintenta en un minuto.")
        time.sleep(min(remaining, 0.25 if timeout_seconds is not None else 1))

    try:
        yield acquired
    finally:
        if acquired:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    [POINT_ACCOUNT_SESSION_LOCK_ID],
                )
