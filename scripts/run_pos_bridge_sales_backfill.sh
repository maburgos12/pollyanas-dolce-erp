#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
DJANGO_SETTINGS_MODULE="${DJANGO_SETTINGS_MODULE:-config.settings}"
BACKFILL_START="${BACKFILL_START:-2022-01-01}"
BACKFILL_END="${BACKFILL_END:-2025-12-31}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python virtualenv not found at $PYTHON_BIN" >&2
  exit 1
fi

export DJANGO_SETTINGS_MODULE
export BACKFILL_START
export BACKFILL_END

"$PYTHON_BIN" - <<'PY' | while read -r START_DATE END_DATE; do
from datetime import date, timedelta
import os

start = date.fromisoformat(os.environ["BACKFILL_START"])
end = date.fromisoformat(os.environ["BACKFILL_END"])
cursor = date(start.year, start.month, 1)

while cursor <= end:
    if cursor.month == 12:
        next_month = date(cursor.year + 1, 1, 1)
    else:
        next_month = date(cursor.year, cursor.month + 1, 1)
    month_end = min(end, next_month - timedelta(days=1))
    print(cursor.isoformat(), month_end.isoformat())
    cursor = next_month
PY
  echo ">>> Backfill ventas Point ${START_DATE}..${END_DATE}"
  "$PYTHON_BIN" manage.py run_sales_history_sync --start-date "$START_DATE" --end-date "$END_DATE"
done
