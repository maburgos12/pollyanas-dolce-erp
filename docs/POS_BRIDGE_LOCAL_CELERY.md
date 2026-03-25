# Pos Bridge Local Con Celery + Beat + Redis

## Objetivo

Operar `pos_bridge` localmente sin `launchd`, usando la misma topología lógica que se publicará:

- `redis`
- `worker`
- `beat`
- web Django local

## Cuándo usarlo

Usar esta ruta cuando:

- el repo está en `Downloads` o en otra carpeta donde `launchd` falla por permisos en macOS
- se quiere alinear la automatización local con la publicación final
- se quiere dejar un único scheduler real y visible

## Supuesto local actual

La base activa sigue siendo `db.sqlite3` local.  
Para no partir el entorno ni perder datos, `worker` y `beat` montan el mismo workspace y operan sobre esa base con:

- `celery --pool=solo`
- `concurrency=1`

Esto prioriza estabilidad sobre paralelismo.

## Arranque

```bash
./scripts/start_pos_bridge_celery_local.sh
```

Esto hace:

1. desinstala `launchd` legado de ventas e inventario
2. levanta `redis`
3. registra `django-celery-beat`
4. levanta `worker` y `beat`

## Intervalo realtime recomendado

Para el estado actual de Pollyana's:

- `POS_BRIDGE_REALTIME_INTERVAL_MINUTES=10`

La corrida completa de todas las sucursales tarda más de `5` minutos, así que `10` evita cola innecesaria y hace la publicación más estable.

## Paro

```bash
./scripts/stop_pos_bridge_celery_local.sh
```

## Estado

```bash
./scripts/show_pos_bridge_sync_status.sh
```

## Servicios docker

Definidos en [docker-compose.yml](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docker-compose.yml):

- `redis`
- `worker`
- `beat`

El `web` puede seguir corriendo fuera de Docker sobre el mismo `db.sqlite3` mientras el equipo termina la transición visual/local.

## Regla operativa

No mezclar:

- `launchd`
- `auto_sync_pos_bridge.sh`
- `celery beat`

El scheduler válido debe ser uno solo.

## Smoke test

```bash
DJANGO_SETTINGS_MODULE=config.settings .venv/bin/python manage.py setup_celery_schedules
DJANGO_SETTINGS_MODULE=config.settings .venv/bin/python manage.py run_daily_sales_sync --days 1 --lag-days 1
DJANGO_SETTINGS_MODULE=config.settings .venv/bin/python manage.py run_inventory_sync
```

## Nota de publicación

En publicación final la recomendación sigue siendo:

- `web`
- `worker`
- `beat`
- `redis`

pero sobre PostgreSQL, no SQLite.
