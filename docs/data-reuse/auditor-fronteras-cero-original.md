# Ficha de fuentes — fronteras documentales vacío/cero

Fecha/ambiente: 5oct2026, PostgreSQL16 aislado5482 y producción READ ONLY.
Unidad: extremo operacional/producto/sucursal/dominio PRODUCT, no conteo físico.
Autorización: orden adjunta eaf19bc3-0aab-44ad-95aa-8142645a6401; incorpora
prueba original vacía/cero e independencia de historia INCOMPLETE sin promoción.

|Fuente|Modelo/clave|Writer|Consumidores|Evidencia|
|---|---|---|---|---|
|Manifiesto y línea original|PointHistoricalInventoryClosing/Line, closing/par/fecha|HistoricalInventoryCaptureService|Balance mensual y BranchInventoryTraceabilityService|7 directo;6 consolidado con intentos4/5 directos DRAFT; línea0/método no_history_current_zero/rows0/limit500/fecha propia postcorte|
|Historia canónica|PointProductHistoryImport/Row, import/par/FK_Movimiento|AuditStockHistoryService capture/ingest_original_response|Reconcile, vetos, firma y cierre|INCOMPLETE no implica contradicción; raws originales anteriores a captura vacía sí la contradicen|
|Snapshot con ÚltimoMovimiento|PointInventorySnapshot/par/job/log|Inventory sync|Mismo lector compartido|Contrato publicado distinto, no se generaliza al cero por ausencia|

Inventario léxico local: inventario_fuentes_datos --term cierre --term historial
--term snapshot:42 candidatos, no identidad semántica por ese resultado.
Grafo indexado actual; documentary_historical_boundary tiene dos consumidores
directos: balance mensual y trazabilidad por sucursal. No nueva tabla/captura.

## Decisión y protecciones

Extender el lector existente. Para consolidado leer bulk las fuentes declaradas y
sus líneas exactas; no aceptar sólo una lista de IDs ni prestarle fecha del consolidado.
DRAFT por fallos en otros pares no invalida una línea original íntegra; REJECTED,
origen distinto, fecha distinta, contradicción o fuente ausente sí la invalidan.
Canónica presente exige sus vetos de integridad/raw: no rebajar membresía ni ocultar
filas conocidas. Mantener INCOMPLETE/MISSING y físicoFalse. Guardar prueba y firma
de procedencia usada; no modificar manifiestos, ventas, stock o datos originales.

## Entorno y entrega

Task auditor-fronteras-cero-original ownercodex, basea80341b4; Compose único
erp_auditor_fronteras_cero, PG5482/Redis6482, subnet10.239.82/24,
volúmenes db_data/redis_data exclusivos. Venv raíz reutilizado, no retirar.
Migrate inicial y check0 verificados. Entorno pendiente de retiro al completar
CI/deploy/aceptación; backup verificado y helper oficial antes de borrar volúmenes.
TDD:2 fallos RED esperados de3 pruebas iniciales; lector previo rechaza
consolidado e INCOMPLETE vacío. Regresiones incluyen fuente ausente/rechazada,
bool/float, fechas prestadas, movimiento cancelado contradictorio y raw posterior.
372pruebas compartidas PASS, check0/migratecheck0/sin migraciones nuevas.
Revisión independiente: P2 tipos copia consolidada corregido, sin otros bloqueantes.
Referencia nativa RED antes de editar (contrato directo exclusivo), GREEN después
(fuentes4/5 por par e INCOMPLETE independiente, review sin capacidades nuevas).
Estado: implementación probada; CI/publicación/aceptación pendientes, mes no cerrado.
