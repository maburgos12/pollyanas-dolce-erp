# Ficha de fuentes — corte documental Stock de respuesta original parcial

Fecha y ambiente consultado: 2026-10-05; código `main` publicado y PostgreSQL 16 local aislado del worktree `auditor-corte-original-parcial`. La aceptación en producción corresponde a una etapa posterior.

## Necesidad y unidad de análisis

Acreditar el saldo en el instante de apertura o cierre de septiembre de 2026 por par exacto sucursal/producto Point, aunque la respuesta original completa de `/Stock/GetHistorial` no cubra **todo el mes**. Un corte probado no convierte `INCOMPLETE` en `COMPLETE` ni acredita conteo físico.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Respuesta Stock original y sus ocurrencias | `PointProductHistoryImport` / `pos_bridge_product_history_imports` | `AuditStockHistoryService.ingest_original_response` | import, branch/product FK, fingerprint, petición, recibo, SHA y archivo íntegro | Ingresos aceptados PR1483: 6 membresías, 5 residuales y 4 cierres; segunda entrada 0 filas | `AuditStockHistoryService`, balance mensual, cierre |
| Movimiento canónico y raw retenido | `PointProductHistoryRow` / `pos_bridge_product_history_rows` | ingreso original / captura oficial | `import_record`, `row_number=FK_Movimiento` dentro del par | Filas de esos ingresos conservan raw y dedup por import; no identidad global por FK | Reconcile, balance, fuente histórica |
| Manifiesto del corte | `PointHistoricalInventoryClosing` y `PointHistoricalInventoryClosingLine` | captura histórica Point ya guardada | fecha operativa, sucursal/producto y fuente | Manifiestos verificados de 31 agosto / 30 septiembre; no son conteo humano | balance y `ProductMonthClosureService` |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `FK_Movimiento` en respuesta y `row_number` en import | Confirmada sólo dentro de import/sucursal/producto | El mismo FK puede aparecer en otro producto; archivo original valida membresía y dominio | Ninguna equivalencia global |
| `Fecha` Stock y corte operativo | Confirmada como UTC Stock | Parser `point_stock_history_instant` y frontend Point `moment.utc`; notas comerciales son locales | No offset aproximado |
| Respuesta parcial y cobertura mensual | Distinta | Dos movimientos adyacentes pueden probar un corte aunque falte mes anterior | Mantener `INCOMPLETE` |

## Decisión de diseño

Extender el lector compartido `documentary_historical_boundary` con una prueba de corte independiente sobre el archivo original **ya guardado**. Reutilizar `_original_batch` para petición, recibo, SHA, membresía, dominio y cadena, incluso si una captura LIVE posterior ya no lo marca como respuesta más reciente. La prueba exige un único archivo original para evitar elegir entre versiones ambiguas; comprueba raw y campos canónicos, vecinos inmediatos a ambos lados del corte, ausencia de movimiento retenido intercalado y de contradicción de stock. No crear tabla, importación ni consulta Point nueva. Devolver firma/evidencia propia, conservando el estado mensual y físico. Consumidores afectados: balance histórico y guard del cierre oficial.

Consultas o procedimiento reproducible: `manage.py inventario_fuentes_datos --term historial` y `--term cierre` con PostgreSQL 16 local; búsquedas son léxicas y no equivalen a identidad. Inspeccionar imports/rows del par exacto y manifiesto mediante consultas acotadas de sólo lectura. Ejecutar test de corte parcial con HTTP prohibido y veto de raw/canónica contradictoria.

Riesgos y pendientes: respuestas sin vecinos a ambos lados, lote corrupto, hueco en el enlace, retención canónica intercalada o múltiples imports mantienen el corte sin acreditar. Publicación y aceptación autenticada aún pendientes; otros bloqueos de conversiones y clasificación comercial son independientes.
