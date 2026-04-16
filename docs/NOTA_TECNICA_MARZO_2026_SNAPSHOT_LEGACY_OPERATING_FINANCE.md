# Nota Tecnica - Marzo 2026 Snapshot Legacy En Operating Finance

## Resumen Ejecutivo

`EmpresaResultadoMensual` del periodo `2026-03-01` fue generado con un snapshot previo de `PointDailySale` que ya no coincide con el stage actual.

La evidencia tecnica indica que:

- el mensual historico de marzo no se construyo con `PointMonthlySalesOfficial`
- el mensual historico de marzo tampoco se construyo con la capa canonica actual (`VentaAutoritativaPoint` o facts v2)
- el mensual historico de marzo fue actualizado antes de la reconstruccion masiva posterior del stage `PointDailySale`
- el valor `metadata.sales_total_source = "POINT_DAILY_SALE"` no puede ser emitido por el codigo actual, por lo que marzo tambien apunta a una version anterior del servicio

Conclusion operativa:

- marzo 2026 debe tratarse como **mes con snapshot legacy**
- no es reproducible exactamente con el sistema actual
- si se desea consistencia tecnica actual, marzo debe regenerarse y aceptarse como nuevo baseline
- si se desea preservar el historico ya presentado, marzo debe dejarse como legado documentado

## Evidencia Tecnica

### 1. Flujo que materializa `EmpresaResultadoMensual`

El flujo actual es:

- comando: [snapshot_operating_finance.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/management/commands/snapshot_operating_finance.py)
- servicio: [services_operating_finance.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/services_operating_finance.py)
- metodo: `OperatingFinanceSnapshotService.build_snapshot(...)`

El servicio actual llena en `metadata`:

- `period_end`
- `sales_total_source`
- `venta_costeada_total`
- `venta_sin_mapear_total`
- `venta_no_receta_total`
- `venta_receta_sin_match_total`
- `sales_mapping_coverage_pct`

El codigo actual solo puede emitir:

- `POINT_MONTHLY_OFFICIAL`
- `SALES_READ_<source>`

No puede emitir `POINT_DAILY_SALE`.

### 2. Timeline de marzo 2026

`EmpresaResultadoMensual` para marzo 2026:

- `creado_en`: `2026-03-29 23:17:02 UTC`
- `actualizado_en`: `2026-04-03 07:19:18 UTC`
- `venta_total`: `3,449,037.19`
- `metadata.sales_total_source`: `POINT_DAILY_SALE`

`PointMonthlySalesOfficial` para marzo 2026:

- `created_at`: `2026-04-03 18:00:02 UTC`
- `updated_at`: `2026-04-03 18:00:02 UTC`
- `total_amount`: `3,326,094.25`

Esto significa que el oficial mensual fue creado **despues** del ultimo update del `EmpresaResultadoMensual`.

### 3. Estado del stage `PointDailySale` actual para marzo

Estadisticas actuales:

- filas: `9,592`
- total: `3,303,505.19`
- `min_created`: `2026-03-21 17:48:17 UTC`
- `max_created`: `2026-04-05 21:31:50 UTC`
- `max_updated`: `2026-04-05 21:31:50 UTC`

Al corte de `EmpresaResultadoMensual.actualizado_en = 2026-04-03 07:19:18 UTC`, el `PointDailySale` actual solo contenia:

- filas: `1,273`
- total: `433,031.99`

Despues de ese corte se escribieron:

- filas: `8,319`
- total: `2,870,473.20`

Distribucion por dia de `updated_at`:

- `2026-03-21`: `678` filas, `208,623.98`
- `2026-03-26`: `299` filas, `118,471.01`
- `2026-03-29`: `88` filas, `25,880.00`
- `2026-03-30`: `115` filas, `53,158.00`
- `2026-03-31`: `46` filas, `19,033.00`
- `2026-04-01`: `27` filas, `5,215.00`
- `2026-04-02`: `20` filas, `2,651.00`
- `2026-04-03`: `72` filas, `36,975.00`
- `2026-04-04`: `84` filas, `33,080.00`
- `2026-04-05`: `8,163` filas, `2,800,418.20`

### 4. Jobs de reconstruccion detectados

Jobs `sales` relevantes para marzo en `PointDailySale`:

- `sync_job_id = 553`
  - `started_at`: `2026-04-03 08:30 UTC`
  - `finished_at`: `2026-04-03 08:32 UTC`
  - `72` filas
  - `36,975.00`

- `sync_job_id = 1746`
  - `started_at`: `2026-04-05 18:19 UTC`
  - `finished_at`: `2026-04-05 18:43 UTC`
  - `1,477` filas
  - `524,336.94`

- `sync_job_id = 1765`
  - `started_at`: `2026-04-05 21:06 UTC`
  - `finished_at`: `2026-04-05 21:32 UTC`
  - `6,686` filas
  - `2,276,081.26`

La reconstruccion masiva principal ocurrio el `2026-04-05`, es decir, **despues** del mensual historico.

### 5. Estado de fuentes canonicas en marzo 2026

Para marzo 2026 hoy no existen filas en:

- `VentaAutoritativaPoint`
- `PointSalesDailyCategoryFact`
- `PointSalesDailyProductFact`

Esto descarta que marzo historico se haya construido desde la capa canonica actual.

## Impacto

### Diferencias observadas hoy

Comparando auditoria actual vs `EmpresaResultadoMensual.metadata` historico:

- `venta_total`
  - auditoria actual: `3,303,505.19`
  - historico: `3,449,037.19`
  - diferencia: `-145,532.00`

- `venta_receta_sin_match_total`
  - auditoria actual: `320.00`
  - historico: `220,555.00`
  - diferencia: `-220,235.00`

- `venta_sin_mapear_total`
  - auditoria actual: `105,721.00`
  - historico: `326,171.00`
  - diferencia: `-220,450.00`

Estas diferencias ya no se explican por la clasificacion comercial reciente.
Se explican por drift entre snapshots y por una version anterior del flujo de materializacion.

### Marca recomendada para trazabilidad

Se recomienda documentar marzo 2026 con una marca de control como:

```json
{
  "historical_snapshot_legacy": true
}
```

Uso recomendado:

- solo para `periodo = 2026-03-01`
- como metadata o marca documental de reconciliacion
- sin cambiar semantica productiva actual

## Decision Recomendada

### Decision

Marzo 2026 debe considerarse **historico legado construido con snapshot previo de `PointDailySale`**.

### Implicacion

- no intentar reconciliarlo al centavo contra el stage actual
- no asumir que el mensual historico de marzo es reproducible con el sistema vigente
- usar marzo 2026 solo como referencia historica cerrada, no como baseline tecnico de comparacion directa

### Opciones futuras

- **Opcion A: dejar marzo como legado documentado**
  - recomendada si se quiere preservar el historico ya reportado

- **Opcion B: regenerar marzo**
  - valida solo si Direccion decide reabrir marzo y aceptar un nuevo baseline tecnico
  - implicaria cambiar `venta_total`, `venta_sin_mapear_total` y demas metricas del mensual

### Estado recomendado

- `historical_snapshot_legacy = true`
- estado: `documentado`
- accion: `no reconciliar contra snapshot viejo`
