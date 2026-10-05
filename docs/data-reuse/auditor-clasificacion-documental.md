# Ficha de fuentes — clasificación documental de movimientos no fabricados

Fecha/ambiente:5oct2026UTC. PostgreSQL16 local5480 recién migrado/check0;
producción READONLY previamente acreditada, sin HTTP Point ni escrituras.

## Necesidad y unidad de análisis

Una fila de merma Point o agregado de conversión mensual puede corresponder a
reventa/accesorio, no receta fabricada. Clasificar documentalmente no asigna FK
producto inexistente ni prueba compra, stock, conteo o ejecución histórica AGG.

## Fuentes candidatas

|Concepto|Modelo/tabla|Escritor|Identificador/ámbito|Evidencia|Consumidores|
|---|---|---|---|---|---|
|Merma|PointWasteLine/pos_bridge_waste_lines|importador protegido/PointSyncJob|source_hash único/ID/branch/movimiento|1492/mov1666364/Colosio/1PZA; autoridad81612 sobre267|MonthlyPointProductBalanceService, cierre, auditor/materializador|
|Conversión|PointConversionLine/pos_bridge_conversion_lines|extractor Report/crea_Reporte_Largo/job77629|source_hash único/ID/branch/AGG|193/Matriz/code875/11PZA;26fuentes completas|balance mensual/cierre/agente|
|Regla comercial|ProductBusinessRule/reportes_productbusinessrule|curación y migraciones existentes|normalized_name único strip.upper|regla3 COCA-COLA 450 ML fija REVENTA desde17abril|finanzas y nueva lectura compartida; sin invocar privados financieros|
|Catálogo original|PointRecipeNode/pos_bridge_recipe_nodes|extracción productos/insumos|run/identity_key; no unicidad globalSKU|2431run79PRODUCT850 COCA450;2332run72PRODUCT1001 875|lectura documental/firma del cierre|
|Cierre|ProductoMonthClosure/Line|ProductMonthClosureService|mes/status/fuentes/firma|guard real de build/lock|pantalla/ledger/agente|

Nombres de tabla corroborados por modelos/grafo; ningún alias lexical acredita
transacción. Inventario local ejecutado: `inventario_fuentes_datos --term reventa
--term clasificacion --term conversion --term merma`:25modelos candidatos,
15mostrados; consulta metadata sin valores. Reglas/nodos se localizaron también
en grafo indexado del worktree. Registros de producción se reutilizan del resultado
`fuente-original-coca-vela-resultado-20261005.json`, consulta acotada cinco filas,
SHA emitida269699d9ff3a44daec66fcea8027c2981bfe210ad31eb1f74e13aaebd68d443a.
Ese SHA corresponde a la serialización de la consulta, no al archivo reformatado.
Raw de modelos sigue fuente operativa; no ingresar este artefacto como Stockraw.

## Alias y equivalencias

|Términos/identificadores|Estado|Evidencia/caso contrario|Decisión|
|---|---|---|---|
|Coca450/regla3/nodoPRODUCT850|clasificación comercial confirmada, noFKtransaccional|raw1492 Articulo exacto/cantidad/unidad/sucursal; PK850 está en nodo, no en1492|reutilizar clasificación, no linkPointProduct|
|Point119external850 frente540external235|distintos|sin equivalencia ni FK compartida|no vincular235|
|Vela875/nodoPRODUCT1001|corroboración catálogo de accesorio autorizada|AGG193 código/nombre completos/familiaVelas/Alegría; sin origen/fecha ejecución|excluir sólo dominio fabricado preservando AGG|
|Extra10/23AGGfabricados|no resuelto|configuración/factor/ProduccionFalse no ejecución|conservar issues, no ampliar tokens|
|MERMAcancelada/reverso|lector1476 entregado independiente|cada delta/fecha/par propio, noFKpareja|no repetir cambio/diagnóstico|

## Decisión de diseño

Extender lectores mensuales existentes con clasificación documental estricta
compartida/cache bulk por lectura. No tablas, maestros, ingestión o segunda captura.
Validar autoridad sobre TODAS267/26 antes de excluir; preservar cantidad/unidad/
branch/raw/sourcehash. Metadata ordenada `excluded_documentary_rows` conserva
regla/nodos/runs/rawSHA/criterio efectivamente usado; firma mensual ya consume
waste/conversions, sin digest global de catálogos no utilizados.

Contrato compartido afectado: ProductMonthClosureService.lock adquiere SHARE
NOWAIT sobre ambos catálogos dentro de atomic y antes de preview/sellado, evitando
mutaciones y phantoms. Lecturas permitidas, escritores globales detenidos durante
preview/sellado. Catalog ocupado falla cerrado; no se modifica autorización del
cierre ni se fuerza lock_ready. Build anidado puede sostener mutex previo;
NOWAIT impide espera/ciclo, no promete orden global universal. Medir duración en
pruebas/aceptación, no afirmar techo que no existe.

Consumidores de regresión: balance/cierre/firma/materializador/auditor/goal
reconciliation_guard/UI. Review/plan permanecen sin HTTP/capture/sync/ops/avisos.

## Riesgos y pendientes

Raw/fields inconsistentes, dominio insumo, nodos contradictorios, regla no fija o
FABRICADO deben conservar unresolved. Regla no prueba FK/ejecución; Vela
execution_origin_verifiedFalse. Fuente incompleta no se vuelve autoritativa por
exclusión. Concurrencia requiere test INSERT/UPDATE/DELETE/SELECT/NOWAIT/rollback.
Publicación pendiente en esta tarea: TDD/revisión/CI/PR/deploy/auth/idempotencia/
retirada propia5480/6480 con respaldo; no declarar septiembre cerrado por entregar.
