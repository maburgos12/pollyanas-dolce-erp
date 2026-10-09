# Membresía original de historial Stock

Fecha: 5 octubre 2026. Fuentes originales de producción verificadas offline;
metadatos Django/PostgreSQL16 local aislado, puerto5483, tarea registrada
auditor-membresia-original, base721f5c84. No HTTP en esta implementación.

## Necesidad y unidad de análisis

Separar ocurrencia original de respuesta, movimiento único por import/par/dominio
y cobertura mensual. Quinientas ocurrencias con499identidades siguen saturadas.

## Fuentes candidatas

|Concepto|Modelo/fuente|Writer|Identidad|Evidencia|Consumidores|
|---|---|---|---|---|---|
|Respuesta íntegra|original_responses en PointProductHistoryImport.raw_metadata|AuditStockHistoryService.ingest_original_response/_persist_response|SHA/envelope/par/receipt/request|Seis wires completos SHA verificados, 500/499 o285/284|Cobertura, fronteras y firmas|
|Ledger canónico|PointProductHistoryRow|Persistencia del mismo servicio|import+row_number/FK_Movimiento|2779identidades,2732raw comunes exactos y47nuevos de octubre|Reconcile, balance mensual, trazabilidad|
|Frontera|PointHistoricalInventoryClosing/Line|Captura histórica existente|fecha operativa/par/dominio|Doce fronteras vetadas por membresía documentada|MonthlyPointProductBalanceService, BranchInventoryTraceabilityService, ProductMonthClosureService|

## Alias y equivalencias

Ocurrencias repetidas con todos los campos y tipos idénticos acreditan repetición
documental, no dos efectos operativos. MismoFK con raw diferente rechaza lote.
FK no es identidad global entre productos;499únicos no acredita consulta inferior
al límite500. Names/SKU/hora no intervienen.

## Decisión

Extender validación compartida y archivo existente, sin tabla/importador paralelo.
Conservar orden y multiplicidad del original, conteo/límite y fecha de consulta.
Ledger único; filas retenidas fuera del lote no heredan su receipt/cobertura.
Archivo/SHA/membership/incidencias deben participar en firmas de consumidores.
Discontinuidad anterior a septiembre permanece visible; contradicción dentro del
mes o en los enlaces de corte impide frontera documental. No fabricar COMPLETE.

Procedimiento reproducible: inventario_fuentes_datos --term historial, y
--term PointProductHistory --term raw_metadata --term closing, PostgreSQL16 local
migrado/check0/migratecheck0. Búsqueda lexical identifica8modelos candidatos;
no demuestra equivalencias. Grafo actualizado del worktree traza consumidores.
Fuentes offline completas: consulta-integridad-seis-lotes-resultado-20261005.jsonl
y ficha-fuentes-membresia-seis-lotes-20261005.md del expediente de este hilo.
Seis pares exactos Point:8/100,8/108,5/117,2/118,2/60,13/108.

TDD observado RED→GREEN para duplicados exactos/conflictivos, corrupción de archivo,
dominio/par antes de escritura, huecos en cortes, firma de consumidor y promoción
LIVE. Batería compartida final460PASS32.697s; revisión independiente resuelta,
check0/migratecheck0/makemigrations sin cambios. TestDB preservada temporalmente;
sus reglas críticas vacías por flush generan W001, no errores ni cambio productivo.
Skill contrastada con escenario baseline y GREEN independiente.
Pendiente: CI SHA actual/deploy, ingreso oficial y aceptación autenticada.
La ficha no acredita publicación ni cierre mensual, físico o responsabilidad.
