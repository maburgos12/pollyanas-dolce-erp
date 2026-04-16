#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "ERROR: no existe .venv/bin/python en el repo."
  exit 1
fi

echo "repo: $ROOT_DIR"
echo "fecha_local: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo

./.venv/bin/python manage.py diagnose_erp_runtime_context "$@"
