# Ficha de fuentes — fronteras independientes Bamoa con canónica COMPLETE

Fecha y ambiente: 4oct2026; diagnóstico original PostgreSQL producción ya conservado, implementación/pruebas en PostgreSQL16 aislado 5476. No nueva consulta Point ni recaptura COMPLETE.

## Necesidad y unidad de análisis

Frontera por producto/sucursal/fecha operativa, independiente de cobertura mensual canónica y conteo físico. Seis canónicas Bamoa retienen500 filas sin membresía inmutable del último lote. Un estado COMPLETE no inventa esa membresía; snapshots originales acreditados pueden probar el extremo concreto ausente, sin alterar historia ni cobertura.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente escritora | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Historia original | PointProductHistoryImport / PointProductHistoryRow | AuditStockHistoryService | Import/par/FK_Movimiento scoped | 721/778/785/197/816/817:500retenidas,unknown0; no fetchedIDs | Balance/cierre/materializador/investigación/firma |
| Snapshot producto | PointInventorySnapshot | Extracción inventory Point | PK/par/domainPRODUCT/raw/captura/job | Aperturas28605158/28605320/28605343/28605349/28605322/28605261; cierres28797254/28797422/28797446/28797452/28797424/28797363 | documentary_historical_boundary |
| Procedencia | PointSyncJob / PointExtractionLog | inventory pipeline | job/par sucursal/externalID | Apertura35042/log260894; cierre77519/log299172 SUCCESS | Lector y firmas |
| Cierre original | PointHistoricalInventoryClosing/Line | Captura/consolidación original | Closing6/7/par/fecha/manifiesto | VERIFIED exacto no prueba lote íntegro por sí solo | Ledger/cierre oficial |

## Alias y equivalencias

BranchPK5 corresponde Bamoa Point2. No equiparar Crucero histórico. Snapshot insumo no producto; FK_Movimiento no global entre productos. No nuevas equivalencias ni maestros. Casos2574/2580 excluidos por roles conservan su condición.

## Decisión de diseño

Extender el mismo lector independiente y sus vetos raw para COMPLETE unknown0 únicamente cuando la frontera canónica concreta sigue ausente; canónica existente conserva precedencia. Reutilizar snapshots exactos con dominio/job/log/par/captura postcorte/Ult_MovUTC precorte y consistencia. Desconocidos, metadata presente inválida, identidad o raws contradictorios abortan prueba. Conservar coverage_status=COMPLETE; snapshot_boundary_verified=True/canonical_history_verified=False si el extremo procede sólo de snapshot. No editar fetchedIDs/row_count/imports/cierre, ni cambiar roles o habilitar cierre sin otros guards.

Autorización: adjunto humano Texto pegado.txt punto3. Contrato compartido afecta balance mensual, servicio cierre, materializador, auditor/investigador, firma y habilidad; no nuevas tablas/importaciones/modelos/migraciones/API/agents/scheduler. Archivos permitidos: monthly_product_balance_service.py, pruebas afectadas, esta ficha y3referencias nativas. Prueba negativa descubrió TypeError previo de _covers_month ante fetched_movement_ids string: extensión quirúrgica anunciada a audit_stock_history_service.py y test dedicado, rechazando membresía presente inválida sin alterar el comportamiento legacy de clave ausente ni datos. Sin nuevas consultas ni parser temporal/captura/ingreso distintos. Publicación/aceptación pendientes hasta CI SHA actual/deploy oficial/UI reales.

## Procedimiento reproducible

inventario_fuentes_datos --term snapshot y --term historial ejecutados en PostgreSQL local; candidatos léxicos no identidad. Fuentes reales: contrato-bamoa-snapshot-complete-20261004.md y diagnóstico post174 original; no volver a leer Point. Regresiones: COMPLETE sin membresía, frontera presente, unknown, raw/domain/quantity/fecha/metadata contradictorios, manifest/captura/job inválidos, caché/firma y bulk. Después deploy:12extremos exactos/two reads/HTTP prohibido/hashoriginales sin cambios, review nativa de un candidato nuevo con UI autenticada. No materialización ni cierre en aceptación lectora.
