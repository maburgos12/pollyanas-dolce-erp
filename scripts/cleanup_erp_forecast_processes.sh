#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1"
PATTERN='(manage.py shell -c|manage.py test|manage.py check).*pastelerias_erp_sprint1'

PIDS=()
while IFS= read -r pid; do
  [[ -n "$pid" ]] && PIDS+=("$pid")
done < <(
  ps -axo pid=,command= | awk -v root="$ROOT" '
    $0 ~ /manage\.py (shell -c|test|check)/ && index($0, root) { print $1 }
  '
)

if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "No hay procesos pesados activos del ERP para limpiar."
  exit 0
fi

echo "Procesos del ERP a terminar:"
ps -p "$(IFS=,; echo "${PIDS[*]}")" -o pid=,etime=,%cpu=,%mem=,command=
kill "${PIDS[@]}"
echo "Procesos terminados: ${PIDS[*]}"
