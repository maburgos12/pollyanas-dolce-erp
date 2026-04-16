#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Falta .venv/bin/python. Crea o activa la venv local antes de correr pruebas." >&2
  exit 1
fi

if [[ "${SKIP_POINTDAILYSALE_GUARD:-0}" != "1" ]]; then
  ./.venv/bin/python scripts/check_pointdailysale_usage.py
fi

if [[ "${SKIP_PROTECTED_SALES_READER_GUARD:-0}" != "1" ]]; then
  ./.venv/bin/python scripts/check_protected_sales_readers.py
fi

if [[ -z "${TEST_DB_NAME:-}" ]]; then
  SAFE_USER="$(printf '%s' "${USER:-local}" | tr -cs 'a-zA-Z0-9_' '_')"
  export TEST_DB_NAME="test_pastelerias_erp_${SAFE_USER}_$$"
fi

exec ./.venv/bin/python manage.py test --settings=config.settings_test --noinput "$@"
