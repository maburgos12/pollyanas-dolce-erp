# Point Sales Rebuild V2

## Objetivo

Reconstruir el histórico de ventas Point por `sucursal + día + categoría` y, cuando el reporte lo permite, también por producto, sin confiar ciegamente en `PointDailySale` ni en `VentaHistorica`.

## Qué cambia

- Se agrega una cola de tareas por `sucursal + día + credito_scope`.
- Se agrega staging crudo inmutable con `source_file`, `source_hash`, `row_hash` y payload original.
- Se agrega normalización separada con `match_catalogo_status`.
- Se agregan facts finales:
  - `pos_bridge_sales_daily_category_fact`
  - `pos_bridge_sales_daily_product_fact`
- Se agregan alertas de calidad y reportes de conciliación.
- Se reutiliza la descarga oficial real de Point en `/Report/PrintReportes?idreporte=3`.

## Tablas nuevas

- `pos_bridge_sales_extraction_tasks`
- `pos_bridge_sales_raw_staging`
- `pos_bridge_sales_normalized`
- `pos_bridge_sales_daily_category_fact`
- `pos_bridge_sales_daily_product_fact`
- `pos_bridge_sales_quality_alerts`

## Flujo

1. Plan del job y creación de tasks por shard.
2. Claim seguro de tasks con `select_for_update(skip_locked=True)`.
3. Descarga real del XLS desde Point con retry.
4. Validación básica del archivo descargado.
5. Carga a staging crudo.
6. Normalización y matching a catálogo.
7. Reemplazo idempotente de facts del branch-day.
8. Promoción opcional a `ventas.VentaAutoritativaPoint`.
9. Reporte de conciliación contra la base legacy.

## Comandos

### Backfill histórico

```bash
./.venv/bin/python manage.py run_point_sales_rebuild_backfill \
  --start-date 2022-01-01 \
  --end-date 2026-04-04 \
  --batch-size 12 \
  --build-report
```

### Reanudar job existente

```bash
./.venv/bin/python manage.py run_point_sales_rebuild_backfill \
  --job-id <JOB_ID> \
  --batch-size 12 \
  --build-report
```

### Incremental futuro

```bash
./.venv/bin/python manage.py run_point_sales_rebuild_incremental \
  --lookback-days 3 \
  --lag-days 1 \
  --build-report
```

### Validación posterior

```bash
./.venv/bin/python manage.py validate_point_sales_rebuild --job-id <JOB_ID>
```

## Variables de entorno relevantes

- `POINT_BASE_URL`
- `POINT_USERNAME`
- `POINT_PASSWORD`
- `POINT_TIMEOUT`
- `POINT_RETRY_ATTEMPTS`
- `POINT_BRIDGE_STORAGE_ROOT`

No se hardcodean credenciales en el código.

## Estrategia de reanudación

- Cada task representa una única combinación `sucursal + día + scope`.
- Si un proceso cae, las tasks `RUNNING` stale se reencolan.
- Reprocesar una task reemplaza su staging y sus facts del branch-day.
- El job no se marca `SUCCESS` mientras existan tasks `PENDING` o `RUNNING`.

## Estrategia de migración segura

- El pipeline viejo no se elimina en este cambio.
- La nueva reconstrucción vive en tablas nuevas y separadas.
- La promoción a `VentaAutoritativaPoint` se hace dentro del pipeline v2.
- La promoción a `VentaHistorica` queda explícita y opcional con `POINT_BRIDGE_SALES_V2`.
- No se toca automáticamente `POINT_BRIDGE_SALES` hasta validar conciliación.

## Métricas registradas por task

- `download`
- `checksum`
- `parse`
- `persist`
- `total`

Se guardan en `pos_bridge_sales_extraction_tasks.timings_ms`.
