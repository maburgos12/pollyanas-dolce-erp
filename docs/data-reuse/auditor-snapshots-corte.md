# Apertura y cierre desde snapshots originales Point — 4 octubre 2026

## Requerimiento y autorización

Mauricio autorizó integrar al lector compartido los snapshots investigados para
553 pares producto/sucursal con ambos extremos documentales. No se autorizó
inventar cobertura histórica, cambiar originales, vender insumos como productos,
ni dar por contado físicamente el inventario. No hay migración o maestro nuevo.

## Fuentes existentes y unidad de análisis

|Fuente|Unidad / identificadores|Crea y actualiza|Consumidores / decisión|
|---|---|---|---|
|`pos_bridge.PointInventorySnapshot` / `pos_bridge_inventory_snapshots`|PK snapshot, FK branch/product/job, stock, captured_at, raw row|Extractor inventario Point; conservar original|Reutilizar para prueba de frontera, no historia integral|
|`PointExtractionLog`|job + context branch_id/branch_external_id, mensaje original|Extracción por sucursal|Reutilizar para corroborar procedencia sucursal|
|`PointSyncJob`|PK, tipo inventory, status SUCCESS|Sincronizador existente|Reutilizar autoridad de captura; SUCCESS no prueba cobertura de historia|
|`PointProductHistoryImport/Row`|FK canónica del par, FK_Movimiento, Fecha rawUTC|Captura histórica existente|Precedencia; cualquier canónica existente impide este fallback|
|`PointHistoricalInventoryClosing/Line`|cierre/fecha/método/manifiesto + branch/product|Servicio cierres existente|Releer valores efectivos sin editar stock/evidence originales|
|`PointInsumoInventorySnapshot`|FK insumo, quantity_base|Extractor insumos|Dominio distinto, NO reutilizar como producto|

`inventario_fuentes_datos --term snapshot --term existencia` ejecutado en
PostgreSQL de producción (solo lectura):33 candidatos léxicos,15 mostrados;
snapshot producto e insumo existen separadamente. La búsqueda no prueba identidad.
No usar `ExistenciaInsumo`, CRM, ledger o resúmenes históricos como equivalentes.

## Evidencia original y contrato

Snapshot28797567: branchPK4/external5, productPK106/external116, stock1,
job77519 SUCCESS, captured_at2026-10-01T09:16:45.591722Z. Row original10 celdas,
headers visibles8: row0=`116`, row4=`1`, row8=`2026-10-01T02:18:44.487`,
row9=`false`. Log original: `Sucursal procesada 5.`, context branch_id4,
branch_external_id5, products_seen354/snapshots_created354. No identidad por SKU.

Frontend Point `/Stock/tab_almacen` SHA256
`8a0516c8bd2b2d3735ec8f65d92ca5305ebab8fb902fc9b7270bd218bfb829a4`
usa `moment.utc(data,"YYYY-MM-DD HH:mm:ss")` para ÚltimoMovimiento/UIt_Mov.
No aplicar este contrato a notas comerciales ni cambiar America/Mazatlan.

Fuentes investigadas:576 pares sin canónica,559 aperturas y553 cierres
documentales,553 con ambos. Cifras de investigación, no criterios hardcoded.
690 snapshots adicionales de23 pares CEDIS no aportaron17 aperturas/23 cierres;
último movimiento posterior al corte no prueba el saldo en ese corte.

## Contrato mínimo de extensión

Mantener `documentary_historical_boundary` como lector único para auditor y
balance mensual. Ventana bulk desde corte a corte+3d; captura posterior, último
movimiento UTC anterior al corte y no posterior a captura. Exigir stock raw=stock
persistido, row externalID=FK producto externalID, dominio explícito producto,
job inventory SUCCESS y procedencia original de sucursal. Stock contradictorio
entre candidatos válidos deja frontera sin prueba; no escoger el conveniente.

Aceptación de producción PR1458 detectó apertura0 porque Closing6 VERIFIED usa
`consolidated_point_stock_history_attempts`; el helper sí acredita sus snapshots
originales Sep1 dentro3d. Corrección acotada: separar gate de snapshot independiente
del gate legacyzero. Ambos exigen manifiesto reconocido, VERIFIED, fecha y pares
completos, retrievedpostcorte. Consolidación NO autoriza stock/cero de sus líneas.
No ampliar ventana: el universo vigente supera553, validar por FK/prueba, no cifra.

Canónica API `POINT_STOCK_HISTORY_API` completa/incompleta/desconocida tiene precedencia; snapshot
no la oculta. Documento admite `snapshot_boundary_verified`, pero conserva
`canonical_history_verified=False`, cobertura MISSING y físico no acreditado.
Agregar PK/job/pair/captura/lastMove/stock/contrato/huella a la evidencia efectiva.

La firma de refresh debe cambiar por raw, captured_at, job status/tipo y log
de procedencia, incluso updates sin signals. Consultas bulk/cache por corte,
sin N+1, HTTP, nuevas tablas o imports.

## Riesgos, pruebas y aceptación

Rechazar identidad/dominio ausente o erróneo, fecha incompleta, FAILED/RUNNING,
captura pre-corte, lastMove posterior, ventana fuera de límite, raw/stock
contradictorio y canónica incompleta. Probar consumidores compartidos, cache y
firma, HTTP prohibido, originales intactos, comparación repetida y guard cierre.
CI/deploy/authUI pendientes hasta que se registren resultados reales; esta ficha
no acredita publicación, cierre mensual, físico ni resolución humana.

## Recursos temporales

Tarea `auditor-snapshots-corte`, propietario codex, rama
`codex/auditor-snapshots-corte`, worktree registrado del mismo nombre,
base4900b18d. ComposeLOCAL `erp_auditor_snapshots_corte`, PG5472/Redis6472,
volúmenes exclusivos postgres_data/redis_data con prefijo Compose. PostgreSQL16
migrate/check0 antes de escribir. Retirar solo al cerrar entrega, con helper y
respaldo verificado; no servicios Docker de producción ni entornos ajenos.
