# Ficha de fuentes — efectos documentales de merma cancelada

Fecha: 4 octubre2026 Mazatlán /5octUTC. Fuente original de producción ya investigada; implementación y pruebas PostgreSQL16 aisladas, tarea registrada auditor-merma-reversion-historica base63a3c57d. No consulta Point adicional.

## Necesidad y unidad de análisis

Un evento de stock original, delimitado por importación, sucursal, producto y FK_Movimiento, acredita su propio débito o reverso administrativo. No representa una merma vigente, una pérdida física ni una relación inventada con otro evento. La cadena debe conservar ambos efectos y cortarse por el instante original StockUTC en America/Mazatlan.

## Fuentes candidatas

|Concepto|Modelo/tabla|Fuente que crea/actualiza|Identificador y ámbito|Evidencia reutilizada|Consumidores|
|---|---|---|---|---|---|
|Historial canónico|PointProductHistoryImport /pos_bridge_product_history_imports|AuditStockHistoryService capture/ingreso original autorizado|import,point_branch,point_product,metadata petición y membresía|import43, rows5776/5775; original íntegro2169 de300filas SHA0aec23799d6405e161942f478dc870ec130b281cd2a681a17981e06d4978b2fe|reconcile_many, lectores de fronteras|
|Evento original|PointProductHistoryRow /pos_bridge_product_history_rows|mismo servicio existente|import_record, row_number=FK_Movimiento; raw_payload y campos canónicos|import43/product120:1680267tipo5MERMA qty4,4→0,CanceladoTrue;1680271tipo15CANCELACION DE MERMA qty4,0→4,CanceladoFalse|historial, resolución cierre, consistencia snapshot|
|Otro ámbito mismoFK|mismo historial, importación original recuperada para2169|servicio canónico oficial|CEDISbranch3/product263, no unir globalmenteFK|1680267qty1,10→9,24sept16:20:16.27UTC;1680271qty1,9→10,16:26:48.103UTC,iscargoTrue/False|regresión identidad y segunda ejecución|
|Merma vigente|PointWasteLine /pos_bridge_waste_lines yMermaPOS /control_mermapos|sync protegido81612 previamente aceptado|source_hash, dominio e identidad original|267filas mensuales/mirrors1:1 ya acreditadas; no vuelve a sincronizarse|autoridad mensual independiente, no netear mediante este lector|
|Clasificación del tipo|frontend Point Stock/tab_historial|frontend original comprobado|FK_Tipo_Movimiento5MERMA/15CANCELACION DE MERMA|SHA8755e73fc393236ec56f26354904ea5731022f6b4004c90f826e11c2f2e3e55c|contrato compartido puro de efecto histórico|

## Alias e identidades

|Identificadores|Estado|Evidencia /caso contrario|Revisión|
|---|---|---|---|
|tipo5,MERMA,CanceladoTrue|confirmada débito histórico sólo si delta negativo igualqty y dirección original coherente|import43 y2169; ordinaryMERMA CanceladoFalse mantiene tratamiento actual|reparación autorizada adjunto Texto pegado|
|tipo15,CANCELACION DE MERMA,CanceladoFalse|confirmada reversión histórica sólo si delta positivo igualqty y dirección coherente|originales exactos, no alias por similitud textual|misma autorización|
|FK1680267/1680271 entreproducto120 y263|distinta|cantidades4vs1/imports diferentes|no relación porFK global|
|Débito/reverso|NO relación FK confirmada entre sí|cada evento tiene su propia ecuación; conjunto0 sólo si ambos caen en intervalo|no fabricar vínculo ni restar fuera del mes|

## Decisión de diseño

Extender tres lectores existentes mediante helper puro compartido en historical_inventory_capture: resultado no-especial/válido-especial/inválido-especial. El helper interpreta estrictamente FK_Movimiento original positivo int/ASCII entero, tipo, nombre, Cancelado, cantidad, existencia anterior/nueva y dominio/isCargo cuando presentes. El wrapper comprueba representación persistida y timestamps aware en UTC exacto. Especial inválido queda desconocido/veto; otros cancelados conservan tratamiento previo. No nuevo modelo, importación, snapshot o tabla.

Consumidores afectados: AuditStockHistoryService candidatos/reconciliación, resolve_stock_at_close y _snapshot_canonical_consistency. Los servicios superiores de materialización/agente/cierre reciben sus resultados existentes sin nuevos permisos. Signedwaste +qty débito/−qty reverso; expected_closing resta waste. Conservar cronología, cobertura, desconocidos, gaps, firmas/raw y evidencia documental.

Inventario de fuentes ejecutado en PostgreSQL16 local (PG5478/Redis6478 propios) con --term merma --term historial --term cancelacion:25candidatos léxicos, no prueba semántica. Búsqueda específica --term PointProductHistory complementa el inventario. Grafo indexado y consumidores leídos; se reutiliza ficha propuesta-lectores-reventa-cancelacion-20261004.md del expediente, no repetir diagnóstico de producción ni Point.

## Riesgos y aceptación

TDD: débito/reverso y cadena exactos, ámbitos repetidos, cortes entre eventos y cruce agosto/septiembre, especiales malformados, otros cancelados y ordinarymerma sin cambio. Regresión compartida de reconciliación/fronteras/snapshot y queries constantes. No tolerancia nueva a cantidades, no transformaciones timezoneglobal.

Publicación requiere CI SHAactual, merge/deploy oficial, lectura original doble HTTP0/huellas iguales, revisión nativa acotada nueva y UI autenticada. No se declara publicada en esta ficha antes de esa aceptación. Físico/custodia/conversiones y curaciónCakeTopper permanecen independientes; no cerrar por neto0.

Resultados locales: RED acreditado antes del cambio; GREEN final402regresiones compartidas/orquestación (33.808s) y117consumidores agente/materializador/pantallas/Producido vsVendido (11.374s). Once pruebas puras posteriores de identidad/malformación PASS. Bulk con identidad raw explícita mantiene dos consultas. Check0/migrate--check0/makemigrations sin cambios; diffcheck0. Revisión técnica y documental independientes sin bloqueantes. WarningW001 sólo en testDB conservada tras flush, no en check de la base local migrada; no seed global ni nuevas reglas.
