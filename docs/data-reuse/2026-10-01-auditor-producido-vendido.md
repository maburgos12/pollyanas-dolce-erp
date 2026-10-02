# Ficha de fuentes — auditor y Producido vs Vendido

Fecha y ambiente consultado: 2026-10-01; revisión previa de PostgreSQL de producción y código en main 2c5d7a62. Esta etapa documenta el diseño, sin nuevas escrituras operativas.

## Necesidad y unidad de análisis

Compartir saldos, pendientes y vigencia por mes/producto/sucursal, usando los expedientes existentes. Separar maestros, movimientos, snapshots y proyecciones calculadas.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Conciliación mensual | reportes.ProductInventoryAuditRun / ProductInventoryAuditCase / ProductInventoryAuditEvent | InventoryAuditMaterializer e InventoryAuditAgent | mes, branch, product; expediente y fingerprint | Septiembre: 1,958 casos, 1,259 BALANCED, 220 NEEDS_EXPLANATION, 479 SOURCE_INCOMPLETE | auditor, detalle; integrar reporte y exportaciones |
| Historial del producto | pos_bridge.PointProductHistoryImport / PointProductHistoryRow | captura Point existente; AuditStockHistoryService lo lee | identidad Point, ubicación, intervalo y movimiento | Preview de 127 diferencias pendientes: 76 sin historial suficiente, 51 sin saldo explicado | conciliación y trazabilidad |
| Producción, merma, conversiones | pos_bridge.PointProductionLine / PointWasteLine / PointConversionLine | sincronización Point existente | identificadores y fecha operativa | fuentes ya consumidas por servicios y reporte; comprobar cobertura por mes en implementación | auditor, producido/vendido, cierre mensual |
| Transferencias y evidencia logística | pos_bridge.PointTransferLine y registros de Logística | sincronización Point y carga/recepción existentes | transferencia/detalle y relaciones explícitas | fuentes vigentes documentadas en ficha de clasificación conservadora | Logística y auditor |
| Ventas y cierres | fuente canónica de ventas, snapshots Point y cierres existentes | flujos actuales de importación/sincronización | producto, ubicación y día operativo | consumidos por MonthlyPointProductBalanceService y trazabilidad; no duplicar con histórico | reporte, auditor, cierre |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Sucursal Point / sucursal ERP | Reutilizar equivalencias vigentes | canonical_point_branch_identity; nombre por sí solo no acredita identidad | toda equivalencia nueva requiere revisión |
| Producto Point / receta | Reutilizar enlaces comprobados | productos sin enlace no se omiten ni se fusionan por semejanza | conservar ambiguos pendientes |
| Rebanadas / pastel Chico o Mediano | No resuelta cuando falta origen | la entrada de rebanadas no prueba una salida de un tamaño concreto | no inferir origen ni dividir automáticamente por rendimiento |
| Historial / movimientos importados | Representaciones potencialmente superpuestas | historial confirma trazabilidad, no es una cantidad adicional | impedir doble conteo |

## Decisión de diseño

Extender proyección, disparadores y consumidores existentes. No crear maestros, tablas ni capturas adicionales. El reporte leerá la conciliación persistida y conservará información de costos separada de los saldos auditados.

Procedimiento reproducible: inventario_fuentes_datos --term auditoria --term cierre --term inventario con PostgreSQL; consultas acotadas al mes 2026-09 y preview AuditStockHistoryService.reconcile_many en lotes de 50, sin HTTP. El catálogo arroja candidatos léxicos, no prueba equivalencias.

Riesgos y pendientes: cobertura insuficiente en septiembre; orígenes de conversión no identificados. La revisión diaria recupera omisiones. El job mensual de Point desactivado refresca fuentes y no sustituye al auditor; no se activa. Responsables faltantes y resoluciones humanas se preservan.

## Escritores y disparadores comprobados en implementación

| Fuente | Finalización reutilizada | Fecha y meses afectados |
| --- | --- | --- |
| Ventas oficiales | PointDailySale y PointSyncJob finalizado (incluye bulk) | sale_date / parameters.start_date y end_date |
| Producción y merma | PointMovementSyncService y signals existentes; estado terminal del job | production_date / movement_at, mes operativa Mazatlán |
| Conversiones | PointConversionLine y job finalizado | movement_at; no inferir producto origen |
| Transferencias | persist_transfer_lines: reutilizar scope_dates, movement_dates y historical_dates, después del commit | registered_at, sent_at, received_at; incluye el mes anterior al mover una transferencia |
| Snapshots | persist_branch_inventory, después de bulk_create; signal individual | snapshot_affected_months: cierre y apertura siguiente, ventana canónica existente |
| Cierres históricos | guardado/verificación de PointHistoricalInventoryClosing | operational_date: mes de cierre y siguiente |
| Historial local | guardado final de PointProductHistoryImport, después de filas y cobertura | intervalo de movement_at intersectado con meses auditados existentes |
| Logística | carga de RutaCargaChecklistLinea, recepción de ParadaRuta y DiscrepanciaLogistica | ruta.fecha_ruta; respaldo diario detecta actualizaciones bulk |

No se crean tablas ni PointSyncJob. Se extiende la programación existente con una revisión diaria local a las 04:15 America/Mazatlan. El reporte y CSV/XLSX/PDF leen ProductInventoryAuditCase; la consulta ligera de vigencia no costea ni reconstruye. El checksum previo al despliegue para las 1,958 cantidades de septiembre fue c860e77381192a23356c2ec01fcb7ef7fc4c87f3189188a7144d70c5257d5a11 (campos de saldo/movimiento, orden branch_id/product_id); status 1,259/220/479 y is_locked=False, verificados directamente en PostgreSQL del VPS el 1 de octubre.
