# Operación API - Aliases Inventario

Este runbook cubre validación rápida de endpoints de homologación de nombres en inventario.

## 1) Variables recomendadas

```bash
export ERP_BASE_URL="https://pollyanas-dolce-erp-production.up.railway.app"
# Opción A: token fijo
export ERP_API_TOKEN="<TOKEN_DRF>"
# Opción B: credenciales para obtener token automático
export ERP_API_USER="admin"
export ERP_API_PASSWORD="<PASSWORD>"
```

## 2) Smoke operativo

```bash
./scripts/smoke_aliases_api.sh
```

Si hay problema TLS/certificados en macOS:

```bash
./scripts/smoke_aliases_api.sh --insecure
```

El smoke valida:
- `GET /api/inventario/aliases/pendientes/` (estructura, filtros y `recent_runs`)
- `GET /api/inventario/aliases/pendientes/?q=...&source=POINT`
- `GET /api/inventario/aliases/pendientes-unificados/?source=POINT&min_sources=1`
- `GET /api/inventario/aliases/pendientes/?export=csv|xlsx`
- `GET /api/inventario/aliases/pendientes-unificados/` (filtros y paginación)
- `GET /api/inventario/aliases/pendientes-unificados/?export=csv|xlsx`

## 3) Ejecución directa del comando Django

```bash
.venv/bin/python manage.py smoke_aliases_api \
  --base-url "$ERP_BASE_URL" \
  --token "$ERP_API_TOKEN"
```

O con usuario/contraseña:

```bash
.venv/bin/python manage.py smoke_aliases_api \
  --base-url "$ERP_BASE_URL" \
  --username "$ERP_API_USER" \
  --password "$ERP_API_PASSWORD"
```
