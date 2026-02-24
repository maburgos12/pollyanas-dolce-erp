# Operación API Integraciones (Runbook)

Este runbook cubre operación diaria y smoke de endpoints de integraciones Point.

## 1) Endpoints operativos

- `GET /api/integraciones/point/resumen/`
  - KPI operativo (API 24h/7d, clientes, alertas, estado de operaciones).
- `POST /api/integraciones/point/clientes/desactivar-inactivos/`
  - Parámetros: `idle_days`, `limit`, `dry_run`.
- `POST /api/integraciones/point/logs/purgar/`
  - Parámetros: `retain_days`, `max_delete`, `dry_run`.
- `POST /api/integraciones/point/mantenimiento/ejecutar/`
  - Parámetros: `idle_days`, `idle_limit`, `retain_days`, `max_delete`, `dry_run`.
- `GET /api/integraciones/point/operaciones/historial/`
  - Filtros: `action`, `date_from`, `date_to`, `limit`, `export=csv`.

Todos requieren autenticación `Token` y rol `ADMIN`/`DG` para operación.

## 2) Smoke operativo (recomendado diario)

### Opción rápida (script)

```bash
BASE_URL=https://pollyanas-dolce-erp-production.up.railway.app \
TOKEN=<TOKEN_DRF> \
./scripts/smoke_integraciones_api.sh
```

Si aparece error de certificados TLS en Python/macOS, ejecutar con:

```bash
./scripts/smoke_integraciones_api.sh --insecure
```

### Opción comando Django

```bash
.venv/bin/python manage.py smoke_integraciones_api \
  --base-url https://pollyanas-dolce-erp-production.up.railway.app \
  --token <TOKEN_DRF>
```

El smoke valida:
- `health`
- `resumen`
- `historial`
- operaciones `dry_run` (desactivación, purga, mantenimiento combinado)

## 3) Ejecución live (controlada)

Para ejecutar mantenimiento con efectos reales:

```bash
.venv/bin/python manage.py smoke_integraciones_api \
  --base-url https://pollyanas-dolce-erp-production.up.railway.app \
  --token <TOKEN_DRF> \
  --live --confirm-live YES \
  --idle-days 30 --idle-limit 100 \
  --retain-days 90 --max-delete 5000
```

Si `--confirm-live YES` no está presente, el comando corta con error.

## 4) Validación posterior a operación

1. Consultar historial:
```bash
curl -H "Authorization: Token <TOKEN_DRF>" \
  "https://pollyanas-dolce-erp-production.up.railway.app/api/integraciones/point/operaciones/historial/?limit=20"
```

2. Exportar historial CSV:
```bash
curl -L -H "Authorization: Token <TOKEN_DRF>" \
  "https://pollyanas-dolce-erp-production.up.railway.app/api/integraciones/point/operaciones/historial/?limit=200&export=csv"
```

3. Revisar en respuesta:
- acción ejecutada (`RUN_API_MAINTENANCE`, `PURGE_API_LOGS`, etc.)
- timestamp
- payload con totales (`deactivated`, `deleted`, `remaining_candidates`)

## 5) Criterio de éxito operacional

- Smoke diario sin errores HTTP.
- Historial con entradas de operación esperadas.
- Sin alertas críticas inesperadas en `resumen`.
