from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path


def iter_month_ranges(start: date, end: date):
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        if cursor.month == 12:
            next_month = date(cursor.year + 1, 1, 1)
        else:
            next_month = date(cursor.year, cursor.month + 1, 1)
        month_end = min(end, next_month - timedelta(days=1))
        yield cursor, month_end
        cursor = next_month


def main() -> int:
    root_dir = Path(__file__).resolve().parents[1]
    python_bin = Path(os.getenv("PYTHON_BIN", sys.executable))
    django_settings_module = os.getenv("DJANGO_SETTINGS_MODULE", "config.settings")
    backfill_start = date.fromisoformat(os.getenv("BACKFILL_START", "2022-01-01"))
    backfill_end = date.fromisoformat(os.getenv("BACKFILL_END", "2025-12-31"))

    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = django_settings_module

    for start_date, end_date in iter_month_ranges(backfill_start, backfill_end):
        print(f">>> Backfill ventas Point {start_date.isoformat()}..{end_date.isoformat()}", flush=True)
        command = [
            str(python_bin),
            "manage.py",
            "run_sales_history_sync",
            "--start-date",
            start_date.isoformat(),
            "--end-date",
            end_date.isoformat(),
        ]
        completed = subprocess.run(command, cwd=root_dir, env=env)
        if completed.returncode != 0:
            print(
                f"!!! Backfill detenido en {start_date.isoformat()}..{end_date.isoformat()} con código {completed.returncode}",
                flush=True,
            )
            return completed.returncode
    print(">>> Backfill ventas Point finalizado", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
