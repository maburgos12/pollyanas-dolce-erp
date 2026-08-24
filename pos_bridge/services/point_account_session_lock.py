from __future__ import annotations

from contextlib import contextmanager

from django.db import connection


# Point invalida la sesión anterior cuando la misma cuenta inicia sesión de
# nuevo. Este candado debe envolver operaciones que mantengan una sesión Point
# abierta durante varios requests (navegador o HTTP).
POINT_ACCOUNT_SESSION_LOCK_ID = 7_532_026_080_700_001


@contextmanager
def point_account_session_lock(*, wait: bool):
    if connection.vendor != "postgresql":
        raise RuntimeError("La coordinación de sesiones Point requiere PostgreSQL.")

    query = "SELECT pg_advisory_lock(%s)" if wait else "SELECT pg_try_advisory_lock(%s)"
    with connection.cursor() as cursor:
        cursor.execute(query, [POINT_ACCOUNT_SESSION_LOCK_ID])
        acquired = True if wait else bool(cursor.fetchone()[0])

    try:
        yield acquired
    finally:
        if acquired:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    [POINT_ACCOUNT_SESSION_LOCK_ID],
                )
