# Diagnóstico técnico de ventas Point

## Hallazgos principales

- La base actual mezcla dos fuentes distintas en `pos_bridge_daily_sales`:
  - oficial `/Report/PrintReportes?idreporte=3`
  - legacy `/Report/VentasCategorias`
- Existen muchos jobs de ventas fallidos, parciales y `RUNNING` stale.
- La sesión HTTP de Point usa un `DEFAULT_ACCOUNT_ID` fijo y no resuelve correctamente el workspace/cuenta.
- El retry de `sync_service.retry_failed_jobs()` hoy reintenta fallidos como inventario, no como ventas.
- El backfill oficial actual opera en un solo job grande y no tiene shard queue por branch-day.
- La reparación a `VentaHistorica` borra y reconstruye todo el rango, lo que dificulta paralelizar seguro.
- La cobertura actual inicia en `2022-01-02`, no en `2022-01-01`.
- Los totales oficiales mensuales no coinciden con la suma diaria actual en varios meses, y los datos mixtos incluso sobrecuentan.

## Qué se conserva

- Módulo `pos_bridge`
- `PointSyncJob`
- `PointExtractionLog`
- Descarga oficial real con `PointSalesCategoryReportService`
- Matching actual como base

## Qué se reemplaza o aísla

- Staging de ventas autoritativas
- Cola de tasks por shard
- Facts por categoría y producto
- Alertas de calidad
- Reporte de conciliación

## Riesgos que se atacan con V2

- Duplicidad funcional por mezcla de fuentes
- Reescritura masiva no controlada de histórico
- Falta de `source_hash` y `row_hash`
- Baja auditabilidad de cambios
- Dificultad para reanudar el backfill histórico
