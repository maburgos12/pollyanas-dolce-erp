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
- `POINT_PRODUCTION_STORAGE_BRANCHES`
- `POINT_TRANSFER_STORAGE_BRANCHES`
- `POINT_BRIDGE_STORAGE_ROOT`
- `POINT_SELECTOR_OVERRIDES_JSON`
- `POS_BRIDGE_REALTIME_INTERVAL_MINUTES` (`10` recomendado cuando recorre todas las sucursales)
- `POS_BRIDGE_REALTIME_BRANCHES`
- `POS_BRIDGE_ECOMMERCE_WEBHOOK_URL`
- `AUTO_SYNC_POS_BRIDGE_INCLUDE_INVENTORY`
- `AUTO_SYNC_POS_BRIDGE_INCLUDE_SALES`
- `AUTO_SYNC_POS_BRIDGE_SALES_LOOKBACK_DAYS`
- `AUTO_SYNC_POS_BRIDGE_SALES_LAG_DAYS`

## API interna DRF

`pos_bridge` ahora expone una capa interna autenticada bajo:

```bash
/api/pos-bridge/
```

Endpoints principales:

- `GET /api/pos-bridge/sales/`
- `GET /api/pos-bridge/sales/summary/`
- `GET /api/pos-bridge/sales/by-branch/`
- `GET /api/pos-bridge/sales/by-product/`
- `GET /api/pos-bridge/sales/trends/`
- `GET /api/pos-bridge/inventory/`
- `GET /api/pos-bridge/inventory/current/`
- `GET /api/pos-bridge/inventory/availability/`
- `GET /api/pos-bridge/inventory/low-stock/`
- `GET /api/pos-bridge/products/`
- `GET /api/pos-bridge/products/<id>/recipe/`
- `GET /api/pos-bridge/sync-jobs/`
- `POST /api/pos-bridge/sync-jobs/trigger/`
- `POST /api/pos-bridge/agent/query/`

Notas:

- Esta capa es interna y autenticada; la tienda en línea debe seguir usando la API pública de pickup ya existente.
- `inventory/availability/` es un agregado interno de snapshots; no reemplaza `pickup-availability`.
- `sync-jobs/trigger/` ejecuta sincronización de forma síncrona sobre los wrappers actuales de `pos_bridge`.
- `agent/query/` es privado y queda restringido a usuarios staff o con permiso operativo de `pos_bridge`.

## Agente conversacional interno

Comando de terminal:

```bash
python manage.py ask_agent "Cuanto vendimos en Matriz en febrero"
python manage.py ask_agent "Dame la receta de Tres Leches" --json
```

Endpoint interno:

```bash
POST /api/pos-bridge/agent/query/
```

Body:

```json
{
  "query": "Cuanto vendimos en Matriz en febrero",
  "context": {}
}
```

Comportamiento:

- primero intenta clasificar por reglas y keywords locales
- si la consulta es ambigua y existe `OPENAI_API_KEY`, usa OpenAI solo como fallback de clasificacion
- deja traza en `core.AuditLog`
- no expone esta capacidad en la API publica de tienda

Para Pollyana's Dolce, `POINT_SALES_EXCLUDED_BRANCHES` debe incluir sucursales operativas sin venta al público, por ejemplo:

```bash
POINT_SALES_EXCLUDED_BRANCHES=CEDIS,ALMACEN,PRODUCCION CRUCERO,DEVOLUCIONES
POINT_PRODUCTION_STORAGE_BRANCHES=CEDIS
POINT_TRANSFER_STORAGE_BRANCHES=CEDIS
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
python manage.py run_realtime_inventory --force
python manage.py run_inventory_sync --branch SUC-01
python manage.py run_waste_sync --start-date 2026-03-20 --end-date 2026-03-20
python manage.py run_production_entry_sync --start-date 2026-03-20 --end-date 2026-03-20
python manage.py run_transfer_sync --start-date 2026-03-20 --end-date 2026-03-20
python manage.py run_movement_history_backfill --start-date 2026-01-01 --end-date 2026-01-31
python manage.py run_product_recipe_sync --branch-hint MATRIZ --product-code 01BLN01
python manage.py run_product_recipe_sync --branch-hint MATRIZ --product-code 00445 --product-code 00446 --include-without-recipe
python manage.py run_recipe_gap_audit --branch-hint MATRIZ --product-code 00445 --product-code 00446
python manage.py sync_point_derived_presentations
python manage.py run_daily_sales_sync --days 3 --lag-days 1
python manage.py run_sales_history_sync --start-date 2022-01-01 --end-date 2025-12-31
python manage.py run_sales_history_sync --start-date 2025-12-31 --end-date 2025-12-31 --branch MATRIZ
python manage.py measure_sales_day_close --sale-date 2026-03-17 --probes 3 --interval-minutes 60
python manage.py reconcile_unresolved_sales_matches --start-date 2022-01-01 --end-date 2025-12-31 --create-missing-recipes
python manage.py export_unresolved_sales_matches --start-date 2022-01-01 --end-date 2025-12-31
python manage.py retry_failed_jobs --limit 3
python manage.py run_pos_bridge_scheduler --once --run-inventory --run-sales
```

## Sync de inventario en alta frecuencia

Comando manual:

```bash
python manage.py run_realtime_inventory --force
```

Comportamiento:

- reutiliza `PointSyncService`
- si `POS_BRIDGE_REALTIME_BRANCHES` está vacío, recorre todas las sucursales activas detectadas en Point
- permite filtrar sucursales prioritarias con `POS_BRIDGE_REALTIME_BRANCHES`
- si no se usa `--force`, solo corre dentro del horario operativo configurado en el servicio
- puede notificar al frontend con `POS_BRIDGE_ECOMMERCE_WEBHOOK_URL` después de jobs exitosos

## Mermas y entrada por producción

Comandos:

```bash
python manage.py run_waste_sync --start-date 2026-03-20 --end-date 2026-03-20
python manage.py run_production_entry_sync --start-date 2026-03-20 --end-date 2026-03-20
python manage.py run_transfer_sync --start-date 2026-03-20 --end-date 2026-03-20
```

Comportamiento:

- `run_waste_sync` consume `Mermas/get_mermas`, `Mermas/get_detalle` y `Mermas/get_justificacion`.
- Cada línea queda auditada en `PointWasteLine` y se materializa idempotentemente a `control.MermaPOS`.
- La merma cubre sucursales de venta, `CEDIS` y, cuando exista en Point, `Devoluciones`.
- `run_production_entry_sync` consume `Produccion/getProduccionGeneral` y `Produccion/getProduccionDetalle`.
- Cada línea queda auditada en `PointProductionLine`.
- Si la línea producida se homologa a `Insumo`, se registra como `MovimientoInventario` tipo `ENTRADA`.
- Si la línea producida se homologa a `Receta` y la sucursal pertenece a `POINT_PRODUCTION_STORAGE_BRANCHES`, se registra como `MovimientoProductoCedis` tipo `ENTRADA`.
- `run_transfer_sync` consume `Transfer/GetTransfer` y `Transfer/GetDetalle`.
- Cada línea recibida queda auditada en `PointTransferLine`.
- Si la transferencia recibida llega a una sucursal incluida en `POINT_TRANSFER_STORAGE_BRANCHES`, se registra como entrada a inventario central:
  - `MovimientoInventario` para insumos
  - `MovimientoProductoCedis` para producto terminado homologado
- `Produccion Crucero` no debe agregarse a `POINT_PRODUCTION_STORAGE_BRANCHES`; su impacto en `CEDIS` debe entrar por `Transferencias` para no duplicar stock.

## Backfill histórico de movimientos

Comando:

```bash
python manage.py run_movement_history_backfill \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

Opciones:

- `--waste`
- `--production`
- `--transfers`
- `--branch`

Notas:

- Si no indicas ningún flag, procesa los tres dominios.
- Corre por día y deja un reporte JSON bajo `storage/pos_bridge/reports/`.
- Está pensado para cerrar históricos por bloques, no para reprocesar ciegamente todo `2022-2025`.

## Celery/Beat para local o servidor

La recomendación operativa actual ya no es depender de `launchd` si el repo vive en carpetas protegidas de macOS como `Downloads`. Para local y para servidor queda listo el camino con Celery:

```bash
REDIS_URL=redis://localhost:6379/0
python manage.py migrate
python manage.py setup_celery_schedules
celery -A config worker -l info
celery -A config beat -l info
```

Notas:

- `django_celery_beat` registra el calendario en base de datos; el comando `setup_celery_schedules` es idempotente.
- La recomendación es usar una sola estrategia por entorno:
  - local actual de este repo: Celery + Beat + Redis
  - Linux/servidor: Celery + Beat + Redis
- guía local: [POS_BRIDGE_LOCAL_CELERY.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/POS_BRIDGE_LOCAL_CELERY.md)
- guía Railway: [POS_BRIDGE_RAILWAY_REDIS.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/POS_BRIDGE_RAILWAY_REDIS.md)
- Schedules registrados por defecto:
  - ventas cerradas `01:30`
  - inventario completo `02:15`
  - mermas `02:45`
  - producción `03:00`
  - transferencias `03:15`
  - inventario realtime cada `5` minutos
  - retry de jobs fallidos cada `6` horas
  - sync de recetas semanal
  - auditoría de recetas semanal

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

## Sync de recetas de productos

Point expone la receta/BOM de productos desde el flujo:

- `Configuración`
- `Productos`
- `Editar`
- `Siguiente`
- `Siguiente`
- `Receta`

Para no depender del navegador en ese caso, `pos_bridge` usa endpoints internos autenticados y determinísticos:

- `POST /Account/SignIn_click`
- `POST /Account/get_workSpaces`
- `POST /Account/get_acctok`
- `GET /Catalogos/get_productos`
- `GET /Catalogos/get_producto_byID`
- `GET /Catalogos/getBomsByProducts`

Comando:

```bash
python manage.py run_product_recipe_sync --branch-hint MATRIZ
```

Notas operativas:

- Solo sincroniza productos que en Point vienen con `hasReceta=true`, a menos que uses `--include-without-recipe`.
- No crea recetas vacías en ERP cuando Point no trae BOM.
- Guarda raw export en `storage/pos_bridge/raw_exports/`.
- Crea o actualiza `Receta` con `sheet_name=POINT_PRODUCT_BOM`.
- Reconstruye `LineaReceta` desde el BOM de Point.
- Hace matching automático de insumos por `Insumo.codigo_point`; si no existe, cae a alias/nombre/fuzzy del ERP.
- Si un producto sigue sin receta en Point, aparecerá en el summary como `products_without_recipe_in_point`.

## Auditoría de recetas faltantes contra catálogos de insumos

Cuando un producto no trae BOM en `Configuración -> Productos`, Point todavía puede exponer evidencia en:

- `Catálogos`
- `Insumos`
- `Editar`
- `Receta`

`pos_bridge` audita esos casos buscando candidatos internos en `Catalogos/get_articulos` y corroborando su `BOM` vía `Catalogos/ArticuloGetbyid`.

Comando:

```bash
python manage.py run_recipe_gap_audit --branch-hint MATRIZ
```

Notas operativas:

- Solo audita productos cuyo BOM de producto viene vacío en Point.
- Genera un CSV resumido y un JSON detallado bajo `storage/pos_bridge/reports/`.
- Clasifica cada faltante como:
  - `DERIVED_PRESENTATION`
  - `CORROBORATED_FROM_INSUMO_CATALOG`
  - `POSSIBLE_MATCH_REQUIRES_REVIEW`
  - `INTERNAL_CANDIDATE_WITHOUT_BOM`
  - `MISSING_IN_POINT`
- `DERIVED_PRESENTATION` se usa para SKUs como `rebanadas`: se ligan a la receta padre y aún pueden requerir componentes directos como empaque o etiqueta.
- No modifica recetas del ERP; sirve para corroborar qué falta realmente en origen antes de capturar o sincronizar más datos.

## Sync de presentaciones derivadas de producto

Para convertir los hallazgos `DERIVED_PRESENTATION` en relaciones persistidas dentro del ERP:

```bash
python manage.py sync_point_derived_presentations
```

Notas operativas:

- Lee el último `*_point_recipe_gap_audit.json` por default; puedes pasar `--report-path` para fijar uno específico.
- Crea relaciones `receta padre -> receta derivada` para rebanadas.
- Si la receta derivada no existe todavía en ERP, crea un placeholder `PRODUCTO_FINAL` con `sheet_name=AUTO_POINT_DERIVED_PRESENTATION`.
- Marca `requiere_componentes_directos=true` para recordar que el SKU derivado puede llevar empaque, etiqueta u otros componentes de salida además de la conversión desde el padre.
- Hoy quedan precargadas estas reglas de negocio:
  - pays grandes: `8` rebanadas por entero
  - pasteles medianos: `10` rebanadas por entero
  - `3 leches` mediano: `6` rebanadas por entero

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
