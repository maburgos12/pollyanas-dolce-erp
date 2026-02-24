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
  - Filtros: `action`, `date_from`, `date_to`, `limit`, `offset`, `sort_by`, `sort_dir`, `export=csv|xlsx`.
  - `sort_by`: `timestamp|action|model|object_id|user|id`
  - `sort_dir`: `asc|desc`
  - Respuesta JSON incluye `pagination` (`has_next`, `next_offset`, `has_prev`, `prev_offset`).

Todos requieren autenticación `Token` y rol `ADMIN`/`DG` para operación.

## 2) Smoke operativo (recomendado diario)

### Opción rápida (script)

```bash
BASE_URL=https://pollyanas-dolce-erp-production.up.railway.app \
TOKEN=<TOKEN_DRF> \
./scripts/smoke_integraciones_api.sh
```

Si no tienes token DRF, el mismo script acepta usuario/contraseña y obtiene token automáticamente:

```bash
BASE_URL=https://pollyanas-dolce-erp-production.up.railway.app \
USERNAME=admin \
PASSWORD='<TU_PASSWORD>' \
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

Alternativa equivalente sin token pre-generado:

```bash
.venv/bin/python manage.py smoke_integraciones_api \
  --base-url https://pollyanas-dolce-erp-production.up.railway.app \
  --username admin \
  --password '<TU_PASSWORD>'
```

El smoke valida:
- `health`
- `resumen`
- `historial` (incluye validación de `limit`/`offset`, `sort_by`/`sort_dir` y bloque `pagination`)
- exportables de historial (`csv` y `xlsx`)
- operaciones `dry_run` (desactivación, purga, mantenimiento combinado)

## 2.1) Comando de mantenimiento directo (cron-friendly)

Preview (sin cambios):

```bash
.venv/bin/python manage.py run_integraciones_maintenance --dry-run
```

Live (con cambios):

```bash
.venv/bin/python manage.py run_integraciones_maintenance --confirm-live YES
```

Parámetros disponibles:
- `--idle-days`
- `--idle-limit`
- `--retain-days`
- `--max-delete`
- `--actor-username` (opcional para bitácora con usuario explícito)

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
  "https://pollyanas-dolce-erp-production.up.railway.app/api/integraciones/point/operaciones/historial/?limit=20&offset=0&sort_by=timestamp&sort_dir=desc"
```

2. Exportar historial CSV:
```bash
curl -L -H "Authorization: Token <TOKEN_DRF>" \
  "https://pollyanas-dolce-erp-production.up.railway.app/api/integraciones/point/operaciones/historial/?limit=200&export=csv"
```

3. Exportar historial XLSX:
```bash
curl -L -H "Authorization: Token <TOKEN_DRF>" \
  "https://pollyanas-dolce-erp-production.up.railway.app/api/integraciones/point/operaciones/historial/?limit=200&export=xlsx" \
  -o integraciones_operaciones_historial.xlsx
```
4. Revisar en respuesta:
- acción ejecutada (`RUN_API_MAINTENANCE`, `PURGE_API_LOGS`, etc.)
- timestamp
- payload con totales (`deactivated`, `deleted`, `remaining_candidates`)

## 5) Criterio de éxito operacional

- Smoke diario sin errores HTTP.
- Historial con entradas de operación esperadas.
- Sin alertas críticas inesperadas en `resumen`.

## 6) Scheduler opcional en contenedor

Si deseas ejecución automática periódica dentro del contenedor:

- `ENABLE_AUTO_MAINT_INTEGRACIONES=1`
- `AUTO_MAINT_INTEGRACIONES_INTERVAL_HOURS=24`
- `AUTO_MAINT_INTEGRACIONES_DRY_RUN=1` (recomendado para arranque)
- `AUTO_MAINT_INTEGRACIONES_IDLE_DAYS=30`
- `AUTO_MAINT_INTEGRACIONES_IDLE_LIMIT=100`
- `AUTO_MAINT_INTEGRACIONES_RETAIN_DAYS=90`
- `AUTO_MAINT_INTEGRACIONES_MAX_DELETE=5000`

Cuando pases a live automático:
- `AUTO_MAINT_INTEGRACIONES_DRY_RUN=0`
- `AUTO_MAINT_INTEGRACIONES_CONFIRM_LIVE=YES`
