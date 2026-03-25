# Despliegue Pos Bridge En Railway + Redis

## Objetivo

Operar `pos_bridge` en servidor usando:

- `web` Django
- `worker` Celery
- `beat` Celery Beat
- `Redis`

Sin correr `launchd` ni schedulers locales dentro del mismo entorno.

## Arquitectura

### Servicio web

Comando:

```bash
bash -lc 'python manage.py migrate && gunicorn config.wsgi:application --bind 0.0.0.0:$PORT'
```

Responsabilidad:

- servir ERP
- servir API pública pickup
- servir API interna `pos_bridge`

### Servicio worker

Comando:

```bash
celery -A config worker -l info
```

Responsabilidad:

- ejecutar jobs `pos_bridge`
- retry de tareas

### Servicio beat

Comando:

```bash
bash -lc 'python manage.py setup_celery_schedules && celery -A config beat -l info'
```

Responsabilidad:

- programar ventas
- inventario
- mermas
- producción
- transferencias
- realtime inventory
- recipes sync
- retry jobs

### Redis

Usar `REDIS_URL` del servicio Redis de Railway.

## Variables mínimas

```bash
REDIS_URL=redis://...
CELERY_BROKER_URL=${REDIS_URL}
CELERY_RESULT_BACKEND=${REDIS_URL}

POINT_BASE_URL=https://app.pointmeup.com
POINT_USERNAME=...
POINT_PASSWORD=...

POINT_SALES_EXCLUDED_BRANCHES=CEDIS,ALMACEN,PRODUCCION CRUCERO,DEVOLUCIONES
POINT_PRODUCTION_STORAGE_BRANCHES=CEDIS
POINT_TRANSFER_STORAGE_BRANCHES=CEDIS

ENABLE_AUTO_SYNC_POS_BRIDGE=0
AUTO_SYNC_POS_BRIDGE_INCLUDE_INVENTORY=0
AUTO_SYNC_POS_BRIDGE_INCLUDE_SALES=0
POS_BRIDGE_REALTIME_INTERVAL_MINUTES=5
POS_BRIDGE_REALTIME_BRANCHES=
```

## Schedules esperados

Registrados por `python manage.py setup_celery_schedules`:

- ventas cerradas `01:30`
- inventario completo `02:15`
- mermas `02:45`
- producción `03:00`
- transferencias `03:15`
- inventario realtime cada `5` minutos
- retry jobs fallidos cada `6` horas
- recetas semanal
- auditoría de recetas semanal

## Regla crítica

No correr dos schedulers en el mismo entorno.

### Local macOS

Usar:

- `launchd`

### Railway / servidor

Usar:

- `beat`

## Checklist previo a activar

1. `python manage.py migrate`
2. `python manage.py check`
3. `python manage.py setup_celery_schedules`
4. validar que `PeriodicTask` contiene 9 tasks
5. confirmar que `launchd` no se ejecuta dentro del servidor
6. validar un ciclo manual:
   - ventas
   - inventario
   - mermas
   - producción
   - transferencias

## Smoke tests recomendados

```bash
python manage.py run_daily_sales_sync --days 1 --lag-days 1
python manage.py run_inventory_sync
python manage.py run_waste_sync --start-date 2026-03-20 --end-date 2026-03-20
python manage.py run_production_entry_sync --start-date 2026-03-20 --end-date 2026-03-20
python manage.py run_transfer_sync --start-date 2026-03-20 --end-date 2026-03-20
```

## Rollback

Si el scheduler servidor falla:

1. deshabilitar `beat`
2. conservar `worker` y `web`
3. ejecutar manualmente los jobs críticos
4. no activar `launchd` dentro del servidor

## Nota operativa

Este documento deja lista la ruta de despliegue, pero no implica activar producción todavía.
