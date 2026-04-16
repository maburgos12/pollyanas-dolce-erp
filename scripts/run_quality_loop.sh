#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Falta .venv/bin/python. Crea o activa la venv local antes de correr el loop de calidad." >&2
  exit 1
fi

MODE="${1:-full}"
BASE_DIR="${ROOT_DIR}"

case "$MODE" in
  quick)
    ./.venv/bin/python scripts/check_pointdailysale_usage.py --base-dir "$BASE_DIR"
    ./.venv/bin/python scripts/check_protected_sales_readers.py --base-dir "$BASE_DIR"
    ;;
  persist)
    ./.venv/bin/python manage.py run_quality_guards --base-dir "$BASE_DIR"
    ;;
  validate)
    ./.venv/bin/python manage.py run_quality_guards --base-dir "$BASE_DIR" --no-persist
    ./scripts/run_tests_local.sh orquestacion.tests_quality_loop
    ;;
  full)
    ./.venv/bin/python manage.py run_quality_guards --base-dir "$BASE_DIR"
    ./scripts/run_tests_local.sh orquestacion.tests_quality_loop
    ;;
  *)
    echo "Uso: ./scripts/run_quality_loop.sh [quick|persist|validate|full]" >&2
    exit 2
    ;;
esac
