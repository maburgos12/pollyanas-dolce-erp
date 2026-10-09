# Ficha de fuentes — fronteras documentales independientes

Fecha: 4 octubre 2026. Ambiente local PostgreSQL16 aislado, tarea
`auditor-fronteras-independientes`; diagnóstico previo de producción READ ONLY.

## Necesidad y unidad de análisis

Acreditar la existencia documental de un producto en una sucursal y un corte
operativo. Una frontera no es cobertura de movimientos del mes ni conteo físico.
La autorización humana adjunta permite evaluar snapshots independientes junto a
canónicas INCOMPLETE, sin ocultarlas ni modificar originales. No autoriza aceptar
las 374 fronteras candidatas indiscriminadamente.

## Fuentes candidatas

|Concepto|Modelo / tabla|Fuente que crea / actualiza|Identificador y ámbito|Evidencia|Consumidores|
|---|---|---|---|---|---|
|Existencia documental original|PointInventorySnapshot / pos_bridge_inventory_snapshots|Sync inventario Point|PK snapshot, FK branch/product, job, raw row0/row9|Diagnóstico post174: 374 fronteras INCOMPLETE con candidato que pasa contrato snapshot previo|Lector compartido, balance mensual y trazabilidad sucursal|
|Procedencia de captura|PointSyncJob / PointExtractionLog|Extractor de inventario|Job INVENTORY SUCCESS, log original de sucursal y claves externas|Snapshots 28799456,28605158,28797254 y logs en diagnóstico íntegro|Calificador snapshot y huellas|
|Historia canónica|PointProductHistoryImport / PointProductHistoryRow|AuditStockHistoryService.capture|Import único del par, source POINT_STOCK_HISTORY_API, raw FK_Movimiento, membresía de lote|Import454 500 descargadas/499 retenidas; import721 saturado500 sin IDs: conteo solo no prueba lote exacto|Reconcile, auditoría y lectores de fronteras|
|Manifiesto de apertura/cierre|PointHistoricalInventoryClosing / Line|Captura/consolidación oficial|Closing6/7, pares esperados completos, fecha y retrieved_at|1520/1570 líneas; 959 fronteras sin prueba en diagnóstico post174|Cierre mensual y reconstrucción|
|Saldo calculado|SnapshotLedgerInventarioMensual / SnapshotFlujoCentralMensual|Servicios de reportes|Mes y fuentes consumidas|Candidato léxico; NO fuente original alternativa|Reportes, no sustituto de fronteras|

## Alias y equivalencias

|Término|Estado|Evidencia / contraejemplo|Revisión|
|---|---|---|---|
|Ult_Mov / ÚltimoMovimiento / UIt_Mov|Campo temporal UTC confirmado para tablaProductosPA|Frontend tab_almacen SHA8a0516c8bd2b2d3735ec8f65d92ca5305ebab8fb902fc9b7270bd218bfb829a4|No nuevo offset ni timezone global|
|Snapshot producto / snapshot insumo|Dominios distintos|row9 explícito; FK product no se sustituye por insumo del mismo código|No equivalencia nueva|
|Frontera / historia COMPLETE / conteo físico|Hechos distintos|Snapshot prueba existencia en corte, no cadena mensual ni conteo humano|Mantener flags separados|
|Bamoa / Crucero histórico|No fusionar|Sucursal Point2 y PK5 de los seis casos; contexto temporal separado|No renombrar ni reasignar|

## Decisión de diseño

Extender `documentary_historical_boundary`, consumido tanto por balance mensual
como trazabilidad sucursal. Reutilizar snapshots, jobs/logs y raws existentes; no
crear tabla, importación ni consulta Point. La canónica COMPLETE mantiene su
precedencia. Una INCOMPLETE exige inspección exhaustiva de los originales retenidos y ausencia de
desconocidos, raw inválido o contradicción; la acreditación independiente no
completa la historia ni autoriza el cierre por sí misma.

Evaluar también todos los candidatos calificados: igual stock con distinto
ÚltimoMovimiento no acredita consenso. Un evento posterior al ÚltimoMovimiento y
anterior a captura contradice la prueba aunque su cancelación/reversión netee cero.
Preservar membresía, originales y señales de integridad; incluir evidencia conjunta
en huellas. Leer bajo exclusión mensual compartida, sin invertir orden de locks.

Los originales retenidos se usan como vetos conocidos, no como corroboración de
todo el historial. Ausencia legacy de metadata del lote, saturación y diferencia entre filas
descargadas/retenidas no invalidan por sí mismas el snapshot independiente:
`canonical_membership_verified=False` permanece explícito. IDs presentes inválidos,
referencias ausentes, row_count distinto al número real retenido o raw incoherente
sí impiden aceptación. No imponer continuidad inexistente a un historial parcial
ni compensar una contradicción temporal sumando cancelaciones. Varios imports API
del mismo par constituyen identidad canónica ambigua: no elegir por orden ni
permitir que una prueba válida esconda el veto de otro import. Conservar todas
las huellas para resolución posterior por el flujo oficial.
La coherencia raw/persistido respeta la escala DecimalField existente y el
redondeo PostgreSQL; raw permanece completo en la firma. Deltas y comparación
documental en ÚltimoMovimiento usan precisión original, no redondeo de fuentes.

## Procedimiento y evidencia reproducible

- `inventario_fuentes_datos --term snapshot`, `--term historial`, y
  `--term history --term closing --term snapshot --limit 5`, ejecutados con
  DATABASE_URL PostgreSQL5474: tablas originales existen; candidatos léxicos no
  acreditan equivalencia.
- Grafo indexado del worktree: lector compartido, helper snapshot, reconcile_many
  y pruebas de consumidores inspeccionados; no duplicar reconciliación.
- Diagnóstico íntegro previo: `diagnostico-fronteras-no-probadas-post174-resultado-20261004.json`,
  SHA payload `6daa12bf279c4e1018a6515f6c7e369bd2fededbebe7cf07fcb3fbc72ba3bbd0`.
  554 fronteras INCOMPLETE, 374 con candidato snapshot; no se ha probado aún
  ausencia de contradicción de sus lotes completos.
- Baseline local: migraciones aplicadas, migrate --check0, check0 y 16 pruebas
  snapshot PASS. Pruebas reales de canónicas/raw preceden implementación.
- Regresión final: 468 pruebas PASS en 36.177s, incluidos lectores mensuales y
  sucursal, cierre, historiales/captura, mutex, materializador y agente nativo.
  41 pruebas snapshot y dos regresiones de cierre tuvieron aceptación focalizada.
  Los presupuestos de datos se conservan y el mutex se verifica por separado.
  Revisión independiente del diff estable: ningún bloqueo. La base test
  preservada emite W001 tras flush de TransactionTestCase; development check0,
  sin seed ni supresión de la advertencia.
- Consulta fresca VPS acotada READ ONLY, statement_timeout10s, imports454/721 y
  snapshots28799456/28605158/28797254: ambas canónicas carecen de fetched IDs;
  454 conserva499/fetched500 y721 500/fetched500. Snapshots mantienen FK exactas
  y job77519/35042. No HTTP ni escrituras. La inspección independiente debe
  distinguir contradicción conocida de cobertura parcial; no certificar lote
  íntegro ni exigir artificialmente COMPLETE para leer un snapshot independiente.

## Riesgos y pendientes

No inferir membresía del último lote a partir de cantidades saturadas, eliminar
guardias para alcanzar un número objetivo ni atribuir autoridad a totales
INCOMPLETE. Validación de CI, despliegue, navegador autenticado y lectura de
producción permanecen pendientes hasta completar esta tarea. No presentar esta
ficha como publicación ni cierre de septiembre.
