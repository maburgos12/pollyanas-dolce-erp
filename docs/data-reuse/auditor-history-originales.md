# Ficha de fuentes — ingreso de historiales originales Point

Fecha y ambiente: 2026-10-04; PostgreSQL 16 local aislado (5475), base main e9b34147. Evidencia documental de producción reutilizada del expediente de septiembre; no nueva consulta Point.

## Necesidad y unidad de análisis

Incorporar una respuesta original completa de `/Stock/GetHistorial` en la importación canónica existente del par sucursal/producto. La respuesta, cada movimiento y una frontera documental son unidades distintas. Conservar fecha original de consulta, petición, SHA y procedencia; la fecha de ingreso no acredita frescura Point.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Importación canónica | PointProductHistoryImport / pos_bridge_product_history_imports | AuditStockHistoryService.capture | file_hash único del par externo; FK branch/product | Inventario de fuentes confirma unicidad; base aislada: 0 importaciones antes de pruebas | Conciliación, balance mensual, materializador, agente y captura de cierre histórico |
| Movimiento original | PointProductHistoryRow / pos_bridge_product_history_rows | Persistencia del mismo servicio | (import_record, row_number FK_Movimiento) | Base aislada: 0 filas antes de pruebas; manifiesto CEDIS conserva raws y claves de 14 respuestas completas | Reconciliación y fronteras históricas |
| Respuesta original guardada | Artefactos originales JSONL y salida de sesión | Consulta Point protegida ya realizada | SHA del JSON original, request, fecha de consulta y localizador | Manifiesto de 17 aperturas: 14 respuestas completas verificadas; 3 resúmenes sin raw íntegro excluidos | Nueva entrada controlada del servicio canónico |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| FK_Movimiento dentro de un par | Confirmada sólo en ámbito de importación | El mismo FK aparece entre productos diferentes | No deduplicar globalmente |
| SHA de respuesta y file_hash canónico | Distinta | SHA identifica contenido original; file_hash identifica sucursal/producto | Conservar ambas funciones |
| Frontera probada / historia COMPLETE / conteo físico | Distinta | Dos filas no acreditan respuesta íntegra ni conteo | Guardas vigentes sin bypass |

## Decisión de diseño

Extender AuditStockHistoryService con una entrada explícita para respuesta original completa y una persistencia compartida con capture. No crear tabla, importador paralelo ni cliente HTTP ficticio. Serializar primero la importación canónica (incluida su creación), después los meses afectados. Validar identidad documental por dominio, SHA, count, limit, fechas y raw antes de escritura. Evitar sobrescribir filas de procedencia más reciente o desconocida con evidencia antigua; conservar membresía por respuesta y fecha original. La segunda ejecución idéntica no cambia filas, importaciones ni metadata.

Las respuestas JSON no incluyen petición literal: conservar explícitamente su reconstrucción desde los scripts originales de adquisición. request_provenance archiva source_code completo, source_file, source_sha256, kind DERIVED_FROM_ACQUISITION_SCRIPT y contrato PointHttpSessionClient.get_stock_history. El preflight offline verifica SPEC o argumentos literales contra los 14 pares y límites; el servicio comprueba integridad del texto y envelope exacto sin ejecutar código ni leer rutas del Mac en el VPS. No presentar una petición derivada como petición literal capturada.

Procedimiento reproducible: `inventario_fuentes_datos --term historial` y `--term PointProductHistory`; consulta local acotada de conteos de ambos modelos. Grafo: consumidores directos de AuditStockHistoryService, incluyendo documentary_historical_boundary, InventoryAuditMaterializer, HistoricalPointInventoryClosingCapture y observe_review. Baseline migrate/check: 0; makemigrations: sin cambios.

Riesgos: concurrencia entre captura e ingreso; respuestas antiguas frente a filas retenidas; lote saturado sin membresía; resumen parcial; cobertura mensual y cortes UTC. Las pruebas deben conservar parser legado y lectores raw UTC, demostrar rollback y exclusión mutua, y no producir materialización ni cierre automático. La aceptación en producción requerirá CI del SHA actual, despliegue oficial, ingreso exacto de originales elegibles, idempotencia y pantalla autenticada. Los 3 resúmenes parciales no serán ingresados como respuestas completas.
