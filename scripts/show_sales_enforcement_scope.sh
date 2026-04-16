#!/usr/bin/env bash
set -euo pipefail

SCOPE=(
  "orquestacion/services/pointdailysale_guard.py"
  "orquestacion/services/protected_sales_reader_guard.py"
  "orquestacion/services/sales_publication_guard.py"
  "orquestacion/services/quality_guard_runner.py"
  "orquestacion/services/quality_findings.py"
  "orquestacion/management/commands/run_quality_guards.py"
  "orquestacion/views.py"
  "orquestacion/tests_quality_loop.py"
  "scripts/check_pointdailysale_usage.py"
  "scripts/check_protected_sales_readers.py"
  "scripts/run_tests_local.sh"
  "scripts/run_quality_loop.sh"
  "ventas/services/financials.py"
  "ventas/services/event_detail_snapshot.py"
  "ventas/views.py"
  "ventas/tests.py"
  "templates/orquestacion/quality_findings.html"
  "templates/orquestacion/quality_finding_detail.html"
  "docs/ALCANCE_ENFORCEMENT_VENTAS.md"
  "docs/LOOP_CALIDAD_Y_MEMORIA_ERP.md"
  "docs/CRITERIO_MEMORIA_OPERATIVA_ERP.md"
  "docs/CIERRE_AUDITORIA_Y_REPORTE_CONSOLIDADO_AGENTES_ERP.md"
  "docs/CANONICIDAD_VENTAS_ERP.md"
  "memory.md"
)

if [[ "${1:-}" == "--pathspec" ]]; then
  printf '%s\n' "${SCOPE[@]}"
  exit 0
fi

echo "Bloque de enforcement de ventas:"
for path in "${SCOPE[@]}"; do
  echo " - ${path}"
done
