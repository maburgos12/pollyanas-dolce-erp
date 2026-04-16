#!/bin/bash
set -euo pipefail

cat <<'EOF'
Este script quedó retirado.

Motivo:
- PostgreSQL es la única base canónica soportada por el ERP.
- No se permite reintroducir flujos de exportación o migración desde SQLite sin una validación explícita de Dirección General y una reconciliación viva contra PostgreSQL.

Acción requerida:
- usa el entorno PostgreSQL vigente del ERP
- valida cobertura con consultas y snapshots sobre PostgreSQL
- si existe un legado SQLite que deba rescatarse, trátalo como evidencia histórica aislada y documenta la conciliación antes de cualquier carga
EOF

exit 1
