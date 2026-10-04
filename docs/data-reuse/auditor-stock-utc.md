# Ficha de fuentes — lector documental Stock UTC

Fecha: 4 octubre 2026. Inspección PostgreSQL local aislado `erp_auditor_stock_utc` (5471/6471), y evidencia de producción previa fechada del mismo expediente. La prueba local no sustituye publicación ni cierre mensual.

## Necesidad y unidad de análisis

Releer cada movimiento documental de Stock por importación canónica, producto y sucursal; atribuir su instante al día/mes America/Mazatlan sin reescribir las fechas derivadas originales. Apertura, cierre y cobertura deben usar el mismo contrato que las cantidades.

## Fuentes candidatas

| Concepto | Modelo / tabla | Escritor | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Historial canónico | PointProductHistoryImport / pos_bridge_product_history_imports | AuditStockHistoryService.capture | file_hash único; producto y sucursal exactos; source POINT_STOCK_HISTORY_API | metadata fetched_at, fetched_rows, history_limit, frontera y IDs del último lote | conciliación, cierre histórico, auditor y habilidad ERP |
| Movimiento Stock | PointProductHistoryRow / pos_bridge_product_history_rows | capture deduplica | UNIQUE(import_record,row_number); row_number es FK_Movimiento dentro del dominio | raw_payload.Fecha original; movimiento1686312 tiene raw Oct1 02:01UTC, pertenece a Sep30 local | reconcile/reconcile_many, cobertura, extremos, firma mensual |
| Cierre documental | PointHistoricalInventoryClosing / Line | captura histórica | fecha operativa, branch/product exactos, manifiesto | originales de cierre6/7 y evidence conservados | BranchInventoryTraceabilityService y MonthlyPointProductBalanceService |
| Notas comerciales | detalle original Point; PointNoteDetailService | consultas comerciales | PK_NOTA y folio, no FK_Movimiento equivalente | folios102345/102352/102362 documentan1+1+2 ventas de3430 | evidencia comercial; parser independiente, no modificado |

## Alias y equivalencias

| Términos | Estado | Evidencia / caso contrario | Revisión |
| --- | --- | --- | --- |
| Stock Fecha naive → UTC | confirmada | frontend /Stock/tab_historial usa moment.utc(data).toDate(); SHA8755e73fc393236ec56f26354904ea5731022f6b4004c90f826e11c2f2e3e55c | autorización humana explícita del lector UTC |
| Nota Fecha_Hora naive → UTC | distinta, prohibida | contrato comercial local; no generalizar el parser Stock | no modificación |
| FK_Movimiento → PK_NOTA | no equivalencia | dominio distinto;3430 se enlaza por folio explícito de header, no por hora | conservar identidad documental |
| Fecha derivada legacy → evidencia original | distinta | timestamps +7h existentes se conservan; la lectura exacta usa raw | no migración ni actualización masiva |

## Decisión

Extender los lectores existentes y la habilidad nativa; ninguna nueva tabla/importación/captura. Helper Stock exclusivo devuelve instante UTC aware, conserva offsets explícitos y rechaza raw inválido. SQL recupera candidatos con ventana legacy acotada y filtro exacto en memoria; no aplica una resta heurística al raw. Lote500 exige frontera documental del último lote, no filas antiguas retenidas. Extremos mensuales requieren cobertura, continuidad y ecuación documental válida; un mes vacío no prueba cero. Los lectores de apertura/cierre no pueden certificar un saldo legacy cuyo corte no esté acreditado.

Captura conserva dedup original y protege también la cobertura global de la importación: meses retenidos legacy/efectivos, intervalo intermedio sin movimientos, cierres/casos existentes, fuentes de cierre histórico del par exacto aun antes del primer cierre mensual y el mes siguiente por apertura heredada. Los mutex existentes se adquieren ordenados dentro de la transacción antes de escribir. Firma/cache cambian versión y ventana; signals invalidan ambos ámbitos posibles. No alteración de ventas, inventario, mermas, maestros, timezone general ni aprobaciones.

## Procedimiento y pruebas

`inventario_fuentes_datos --term historial` y `--term PointProductHistory` ejecutados con PostgreSQL configurado. El primero encontró solo candidatos léxicos ajenos; el segundo confirmó ambas tablas e identificadores, no identidad semántica nueva. Grafo MCP usado para lectores y consumidores. Pruebas rojas reprodujeron corte erróneo, mixed aware/naive, firma que omitía filas de septiembre almacenadas octubre, signal sin invalidación septiembre, serialización de evidencia en materialización y cobertura concurrente de meses retenidos. La revisión independiente exigió cubrir además el primer cierre histórico sin expediente previo; regresión roja→verde incorporada. Suite compartida final:328 pruebas PASS, incluyendo runtime/plan del agente (26.683s). Check0, migratecheck0 y makemigrations sin cambios; diffcheck0, revisión independiente final sin bloqueantes. El test runner advierte reglas comerciales ausentes en sus fixtures; el check de la base local migrada no tiene advertencias ni errores.

## Riesgos y pendientes de aceptación

No reclamar frescura Point por una firma, ni cobertura por aritmética. Capturas previas al fin mensual siguen INCOMPLETE. No recapturar COMPLETE ni inventar conteo físico. Publicación exige pruebas compartidas, CI completo, despliegue oficial, revisión de datos/UI e idempotencia sin HTTP antes de cierre por servicio oficial y guards reales. Este documento no declara septiembre cerrado.

## Entorno temporal y propietario

Propietario codex; tarea auditor-stock-utc, worktree dedicado, Compose erp_auditor_stock_utc, PostgreSQL5471/Redis6471 y volúmenes exclusivos. No reutiliza otro entorno. Al terminar publicación y aceptación, detener recursos exactos y aplicar helper de retiro con registro terminal/plan/backup verificado; no limpiar producción ni Docker global.
