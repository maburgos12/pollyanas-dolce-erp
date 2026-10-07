# Ficha de fuentes — cierre documental por producto y sucursal

Fecha y ambiente consultado: 2026-10-06; código en worktree aislado y conteos de producción en solo lectura.

## Necesidad y unidad de análisis

Un resultado documental para septiembre de 2026 por producto Point final y sucursal Point canónica. No representa conteo físico ni el bloqueo mensual. Una conversión puede tener movimientos de entrada y salida independientes sin folio común: cada efecto se acredita por identidad, tipo, cantidad, fecha y continuidad de existencias, sin emparejar por proximidad horaria.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Expediente por par | `reportes.ProductInventoryAuditCase` | Materializador de auditoría | `(month, branch_id, product_id)` único | 1,684 casos de productos vendidos de septiembre en lectura de producción; estado mensual aún incompleto | Auditoría, agente ERP |
| Apertura y cierre | `pos_bridge.PointHistoricalInventoryClosingLine` y originales Stock | Importador y lector documental | fecha operativa, sucursal, producto, huella y límite | Apertura y cierre se validan por separado; ausencia no equivale a cero | Trazabilidad, cierre mensual |
| Movimientos | Ventas oficiales, producción, mermas, transferencias, conversiones Point | Sincronizaciones oficiales | tipo, sucursal, producto, cantidad, fecha y origen íntegro | Fuentes mensuales verificadas en checkpoint; la entrada por conversión no implica por sí sola una salida inferida | Trazabilidad, cierre mensual |
| Historial de inventario | `AuditStockHistoryService` y originales de Point | Captura protegida | par producto/sucursal, movimientos y saldo anterior/posterior | Point ofrece historial por producto y sucursal aun sin folio común; la captura de pantalla no acredita movimientos de septiembre | Auditoría de movimientos |

## Alias y equivalencias

No se homologan productos por nombre, SKU aislado, cercanía de hora ni regla general de rebanadas. Las claves de sucursal/producto Point y la relación ERP se resuelven con identidad documentada. `Extra 10` y `Cake Topper` conservan su clasificación comercial ya decidida; no se transforman en producción.

## Decisión de diseño

Reutilizar el expediente único y las fuentes originales. Una lectura parcial puede mostrar pares probados cuando otros carecen de frontera, sin modificar `build()` estricto ni el cierre mensual. El cierre documental individual exige controles propios y no se deduce de saldo cero, `BALANCED`, ausencia de folio, ni de una pantalla de Point. No crear una segunda tabla de captura de movimientos.

Consultas reproducibles: `python3 manage.py inventario_fuentes_datos --term 'cierre producto sucursal'`, `--term 'auditoria inventario'` y `--term 'ProductInventoryAuditCase'` en PostgreSQL 16; las dos primeras consultas léxicas devolvieron cero candidatos, la tercera identificó el expediente exacto. Lectura de producción acotada a mes 2026-09.

Riesgos y pendientes: la corrida vigente es `SOURCE_INCOMPLETE`; sus casos no constituyen cierres actuales. Verificar por par fronteras, autoridad de movimientos, cobertura histórica y huella antes de sellar cualquiera. Mantener conteo físico y aprobación humana separados.
