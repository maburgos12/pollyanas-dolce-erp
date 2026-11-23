#!/usr/bin/env python3
"""Read-only idle hint inside web; Django alone authorizes and leases real work."""
from contextlib import closing
import os
import sys

import psycopg2


def has_pending_audio():
    with closing(psycopg2.connect(os.environ['DATABASE_URL'], connect_timeout=2,
            options='-c default_transaction_read_only=on -c statement_timeout=1000 -c lock_timeout=500')) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT EXISTS(SELECT 1 FROM horarios_especiales_telegramhoursupdate WHERE state = 'AUDIO_WAIT')")
            return cursor.fetchone()[0]


def main():
    try:
        print('1' if has_pending_audio() else '0')
    except Exception:
        # Credentials and database errors must never reach the SSH response or logs.
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
