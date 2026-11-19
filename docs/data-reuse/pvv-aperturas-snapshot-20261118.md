# Ficha de fuentes — aperturas documentales de octubre

Fecha y ambiente consultado 18/11/2026, PostgreSQL de producción en solo lectura. Metadatos locales mediante inventario_fuentes_datos para inventario, saldo y snapshot, con PostgreSQL 16 aislado.

## Necesidad y unidad de análisis

Saldo Point acreditado al inicio del mes por sucursal y producto. Ocho pares tienen snapshot documental fuera del manifiesto de apertura, pero el suplemento actual exige un historial original archivado.

## Fuentes candidatas

| Concepto | Modelo | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Existencia observada | PointInventorySnapshot | Sync inventario Point | PK, sucursal, producto y captura | Ocho snapshots, jobs 80315/80348 y logs correspondientes | Balance mensual, trazabilidad y auditor |
| Procedencia | PointSyncJob y PointExtractionLog | Extracción inventario | Job y sucursal seleccionada | Estado SUCCESS y sede exacta | Validador _snapshot_historical_boundaries |
| Movimiento retenido | PointProductHistoryImport y PointProductHistoryRow | Importación Stock | Producto, sucursal e ID movimiento | Ocho contrastes favorables | _snapshot_canonical_consistency |
| Cierre histórico | PointHistoricalInventoryClosing y sus líneas | Captura documental | Fecha y manifiesto | #7 del 30/09, #8 del 31/10 | Ambos lectores de balance |
| Proyección visible | ProductInventoryAuditRun y ProductInventoryAuditCase | InventoryAuditMaterializer | Mes y par sucursal-producto | 65 aperturas sin acreditar, 11 también sin cierre | Reporte Producido vs Vendido |

## Alias y equivalencias

Los ocho pares usan el producto ERP 568 / artículo Point 70, con IDs de sucursal exactos y sus propios logs. La sucursal histórica Leyva del caso 5060 es distinta de Sinaloa de Leyva. No se unifican identidades official por SKU o nombre. Sinaloa de Leyva tiene apertura operativa observada al 03/10 y mantiene sin acreditar el cierre del 30/09.

## Decisión de diseño

Extender el suplemento documental existente. Reutilizar los validadores de snapshot y su contraste con los movimientos retenidos, preservando precedencia de fronteras existentes y evidencia de origen. Una captura posterior solo acredita el corte cuando el último movimiento verificable es anterior y el contraste retenido resulta favorable. No se promueve cobertura de historial ni verificación de conteo físico.

Se conserva el manifiesto #7 íntegro. El código lee fuentes existentes. Una reconstrucción real posterior actualizaría la proyección e investigaría los casos mediante el flujo existente, y requiere revisión de impacto. El preview dry_run se ejecuta en READ ONLY sin aplicar esa reconstrucción. La lectura de casos existentes usa bloqueo de filas únicamente en aplicación real, conservando el bloqueo mensual en simulación. La regresión reproduce el acceso dentro de una transacción PostgreSQL READ ONLY.

## Evidencia reproducible

Casos 4201, 4365, 4537, 4894, 5060, 5241, 5411 y 5573. Snapshots 28807271, 28807981, 28807626, 28809046, 28809401, 28809756, 28810111 y 28810466. Todos acreditan saldo cero a la frontera de apertura 01/10 00:00 America/Mazatlan. La consulta y sus firmas se archivan externamente en el expediente de tarea.

Siete no presentan choque entre venta comercial y salida Stock. Matriz #5241 conserva comercial cero frente a Stock uno y el candidato de venta #5226 exige vínculo documental. Acreditar su apertura mantiene esa discrepancia. El cambio no resuelve las otras 57 aperturas por inferencia.

## Riesgos y pendientes

Probar ambos lectores, materializador dry_run, evidencia de pantalla, ausencia de escrituras y callbacks, historial contradictorio, par ya excluido, procedencia incompleta y contrato insuficiente. CI, revisión independiente y preview productivo preceden cualquier reconstrucción real.
