# pos_bridge

Módulo Django para integrar POS Point mediante browser automation determinística con Playwright.

## Objetivo de esta primera fase

- Login con usuario Point de solo lectura.
- Navegación al módulo de inventario.
- Extracción base por sucursal usando tabla HTML y selectores versionados.
- Extracción histórica diaria de ventas por producto y sucursal desde el reporte autenticado de Point.
- Persistencia de snapshots, jobs y logs en PostgreSQL/Django.
- Staging idempotente de ventas en `PointDailySale` y materialización segura a `recetas.VentaHistorica`.
- Screenshots y raw exports para diagnóstico.
- Comandos manuales y worker periódico.

## Decisiones de arquitectura

- Framework: Django, porque el ERP actual ya está montado sobre Django 5 y el acoplamiento más limpio es una app nueva con migraciones propias.
- Aislamiento: `pos_bridge` no toca flujos existentes de `integraciones` ni `inventario`; consume `core.Sucursal` solo para mapeo opcional.
- Determinismo: la navegación usa Page Object Model + selectores centralizados. No hay IA dentro del flujo browser.
- Auditabilidad: cada corrida crea `PointSyncJob`, `PointExtractionLog`, snapshots y entrada en `core.AuditLog`.
- Alcance de sucursales: el robot abre un workspace válido y luego controla la sucursal real desde el dropdown de inventario, para soportar cuentas donde una sola sesión ve múltiples sucursales.
- Ventas históricas: se usa el reporte autenticado `Report/VentasCategorias` dentro de la sesión web de Point. No es API pública, pero sí una fuente determinística del portal ya autenticado.
- Seguridad de materialización: `VentaHistorica` sólo se actualiza con fuente `POINT_BRIDGE_SALES` y no pisa otros importadores.

## Estructura

- `pos_bridge/models/`: catálogos Point, snapshots, ventas diarias, jobs y logs.
- `pos_bridge/browser/`: cliente Playwright, waits, screenshots y page objects.
- `pos_bridge/selectors/`: selectores desacoplados por dominio UI.
- `pos_bridge/services/`: login, parser, normalización, sync y alertas.
- `pos_bridge/tasks/`: wrappers listos para scheduler.
- `pos_bridge/tests/`: pruebas unitarias/base de persistencia.
- `storage/pos_bridge/`: logs, screenshots y raw exports.

## Variables de entorno

Variables obligatorias:

- `POINT_BASE_URL`
- `POINT_USERNAME`
- `POINT_PASSWORD`

Variables operativas:

- `POINT_HEADLESS`
- `POINT_TIMEOUT`
- `POINT_BROWSER_SLOW_MO`
- `POINT_SYNC_INTERVAL_HOURS`
- `POINT_RETRY_ATTEMPTS`
- `POINT_SYNC_MAX_BRANCHES`
- `POINT_SYNC_MAX_PAGES_PER_BRANCH`
- `POINT_SALES_EXCLUDED_BRANCHES`
- `POINT_BRIDGE_STORAGE_ROOT`
- `POINT_SELECTOR_OVERRIDES_JSON`
- `AUTO_SYNC_POS_BRIDGE_INCLUDE_INVENTORY`
- `AUTO_SYNC_POS_BRIDGE_INCLUDE_SALES`
- `AUTO_SYNC_POS_BRIDGE_SALES_LOOKBACK_DAYS`
- `AUTO_SYNC_POS_BRIDGE_SALES_LAG_DAYS`

Para Pollyana's Dolce, `POINT_SALES_EXCLUDED_BRANCHES` debe incluir sucursales operativas sin venta al público, por ejemplo:

```bash
POINT_SALES_EXCLUDED_BRANCHES=CEDIS,ALMACEN,PRODUCCION CRUCERO,DEVOLUCIONES
```

## Instalación

```bash
pip install -r requirements.txt
python -m playwright install chromium
python manage.py migrate
```

## Ejecución manual

```bash
python manage.py run_inventory_sync
python manage.py run_inventory_sync --branch SUC-01
python manage.py run_daily_sales_sync --days 3 --lag-days 1
python manage.py run_sales_history_sync --start-date 2022-01-01 --end-date 2025-12-31
python manage.py run_sales_history_sync --start-date 2025-12-31 --end-date 2025-12-31 --branch MATRIZ
python manage.py measure_sales_day_close --sale-date 2026-03-17 --probes 3 --interval-minutes 60
python manage.py reconcile_unresolved_sales_matches --start-date 2022-01-01 --end-date 2025-12-31 --create-missing-recipes
python manage.py export_unresolved_sales_matches --start-date 2022-01-01 --end-date 2025-12-31
python manage.py retry_failed_jobs --limit 3
python manage.py run_pos_bridge_scheduler --once --run-inventory --run-sales
```

## Backfill de ventas históricas

Comando principal:

```bash
python manage.py run_sales_history_sync \
  --start-date 2022-01-01 \
  --end-date 2026-03-13
```

Notas operativas:

- Por default excluye el rango ya incorporado del `2026-01-01` al `2026-03-13`.
- Si necesitas incluir ese rango otra vez, usa `--no-default-skip-range`.
- Puedes limitar una ventana para validación con `--max-days`.
- Cada día/sucursal genera un raw export JSON bajo `storage/pos_bridge/raw_exports/`.
- `PointDailySale` conserva staging aunque el producto todavía no tenga match a `Receta`.
- `VentaHistorica` sólo se materializa cuando existe match determinístico a receta y sucursal ERP.
- Las sucursales sin venta se excluyen del extractor con `POINT_SALES_EXCLUDED_BRANCHES`.

## Sync incremental diario de ventas

Comando recomendado para operación diaria:

```bash
python manage.py run_daily_sales_sync --days 3 --lag-days 1
```

Notas:

- Reprocesa una ventana móvil e idempotente de los últimos `N` días cerrados.
- `--lag-days 1` evita tomar el día aún abierto.
- Por default sigue respetando el rango ya cargado manualmente `2026-01-01` a `2026-03-13`.
- Puedes activar el worker periódico con `AUTO_SYNC_POS_BRIDGE_INCLUDE_SALES=1`.

## Validación de cierre del día

Para medir a qué hora la venta deja de cambiar y fijar una hora confiable de corte ERP:

```bash
python manage.py measure_sales_day_close \
  --sale-date 2026-03-17 \
  --probes 3 \
  --interval-minutes 60 \
  --stable-after 2
```

Notas:

- Cada medición consulta Point para la fecha indicada y luego resume el staging `PointDailySale`.
- El reporte se guarda en `storage/pos_bridge/reports/sales_close_validation_<fecha>_<sucursal>.json`.
- `stable-after 2` significa que se requieren dos mediciones consecutivas sin cambios para considerar estable la venta del día.
- Si ya tienes la venta cargada y solo quieres recalcular el análisis sin volver a consultar Point, usa `--skip-sync`.
- Recomendación operativa: medir al menos tres puntos nocturnos, por ejemplo `11:30 PM`, `12:30 AM` y `2:00 AM`.

## Reporte de ventas sin match a receta

Para revisar productos vendidos en Point que aún no materializan a `VentaHistorica`:

```bash
python manage.py export_unresolved_sales_matches \
  --start-date 2022-01-01 \
  --end-date 2025-12-31
```

Genera un CSV agregado por producto/sucursal bajo `storage/pos_bridge/reports/`.

## Reconciliación de ventas sin match

Para crear recetas placeholder seguras a partir de productos Point descriptivos y rematerializar staging existente:

```bash
python manage.py reconcile_unresolved_sales_matches \
  --start-date 2022-01-01 \
  --end-date 2025-12-31 \
  --create-missing-recipes
```

Notas:

- Crea recetas `PRODUCTO_FINAL` con `sheet_name=AUTO_POINT_SALES`.
- Las recetas creadas pueden clasificarse con `temporalidad` y `temporalidad_detalle` para forecast y reporting.
- Solo crea placeholders para nombres descriptivos; deja fuera códigos ambiguos como `800`, `801`, `03SPOREB`.
- Reusa el staging `PointDailySale` ya cargado y actualiza `VentaHistorica` con fuente `POINT_BRIDGE_SALES`.

Para reclasificar temporalidad de las recetas `AUTO_POINT_SALES` ya existentes:

```bash
python manage.py classify_auto_point_sales_recipes
```

Ejemplo de validación controlada:

```bash
python manage.py run_sales_history_sync \
  --start-date 2025-12-31 \
  --end-date 2025-12-31 \
  --branch MATRIZ \
  --no-default-skip-range
```

## Calibración inicial requerida

Falta un insumo externo que no está en el repositorio: el DOM real de Point.

Ajuste esperado:

1. Abrir Point y validar login real.
2. Confirmar selectores de login, menú inventario, selector de sucursal y tabla.
3. Si Point no coincide con los defaults, ajustar `pos_bridge/selectors/*.py` o `POINT_SELECTOR_OVERRIDES_JSON`.
4. Ejecutar `python manage.py run_inventory_sync --branch <sucursal>` y revisar:
   - `storage/pos_bridge/logs/`
   - `storage/pos_bridge/screenshots/`
   - `storage/pos_bridge/raw_exports/`

## Scheduler

Hay dos caminos:

- `python manage.py run_pos_bridge_scheduler --run-inventory --run-sales`
- `scripts/auto_sync_pos_bridge.sh`

## Programación fija en macOS para ventas cerradas

Para dejar la sincronización oficial de ventas cerradas todos los días a la `1:30 AM` hora local en macOS:

```bash
./scripts/install_pos_bridge_sales_close_launchd.sh
```

Archivos operativos:

- Runner diario: `scripts/run_pos_bridge_daily_sales_close.sh`
- Instalador launchd: `scripts/install_pos_bridge_sales_close_launchd.sh`
- Rollback: `scripts/uninstall_pos_bridge_sales_close_launchd.sh`

Variables opcionales:

- `POS_BRIDGE_SALES_CLOSE_HOUR` default `1`
- `POS_BRIDGE_SALES_CLOSE_MINUTE` default `30`
- `POS_BRIDGE_SALES_CLOSE_LOOKBACK_DAYS` default `3`
- `POS_BRIDGE_SALES_CLOSE_LAG_DAYS` default `1`
- `POS_BRIDGE_SALES_CLOSE_BRANCH_FILTER` default vacío

Logs:

- `storage/pos_bridge/logs/launchd_pos_bridge_sales_close.log`
- `storage/pos_bridge/logs/launchd_pos_bridge_sales_close.error.log`
- Estado rápido: `./scripts/show_pos_bridge_sync_status.sh`

Rollback seguro:

```bash
./scripts/uninstall_pos_bridge_sales_close_launchd.sh
```

## Programación fija en macOS para inventario

Para dejar el snapshot diario de inventario de Point separado de ventas, a la `2:15 AM` hora local en macOS:

```bash
./scripts/install_pos_bridge_inventory_launchd.sh
```

Archivos operativos:

- Runner diario: `scripts/run_pos_bridge_inventory_sync.sh`
- Instalador launchd: `scripts/install_pos_bridge_inventory_launchd.sh`
- Rollback: `scripts/uninstall_pos_bridge_inventory_launchd.sh`

Variables opcionales:

- `POS_BRIDGE_INVENTORY_HOUR` default `2`
- `POS_BRIDGE_INVENTORY_MINUTE` default `15`
- `POS_BRIDGE_INVENTORY_BRANCH_FILTER` default vacío
- `POS_BRIDGE_INVENTORY_LIMIT_BRANCHES` default vacío

Logs:

- `storage/pos_bridge/logs/launchd_pos_bridge_inventory.log`
- `storage/pos_bridge/logs/launchd_pos_bridge_inventory.error.log`
- Estado rápido: `./scripts/show_pos_bridge_sync_status.sh`

Rollback seguro:

```bash
./scripts/uninstall_pos_bridge_inventory_launchd.sh
```

## Esquema recomendado

- Ventas cerradas: `1:30 AM` con `run_daily_sales_sync`
- Inventario: `2:15 AM` con `run_inventory_sync`
- Worker genérico `auto_sync_pos_bridge.sh`: dejarlo apagado para no duplicar corridas

## Checklist de despliegue seguro

- Crear usuario Point exclusivo de lectura.
- Validar que el inventario mostrado en Point coincide con la sucursal seleccionada.
- Ejecutar primero una corrida por una sola sucursal.
- Comparar snapshot ERP vs Point en al menos 10 SKUs críticos.
- Para ventas, validar al menos un día cerrado por sucursal antes del backfill masivo.
- Activar worker periódico solo después de la conciliación inicial.

## Rollback

- Desactivar `ENABLE_AUTO_SYNC_POS_BRIDGE`.
- Desinstalar `launchd` de ventas cerradas con `./scripts/uninstall_pos_bridge_sales_close_launchd.sh`.
- Desinstalar `launchd` de inventario con `./scripts/uninstall_pos_bridge_inventory_launchd.sh`.
- Revertir migración `pos_bridge`.
- Quitar `pos_bridge` de `INSTALLED_APPS` si se decide retirar el módulo completo.
