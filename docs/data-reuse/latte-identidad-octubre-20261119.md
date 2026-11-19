# Ficha de fuentes — identidad de Latte en confianza de ventas

Fecha y ambiente: 19/11/2026 UTC, ERP productivo `56f3dcec`, consultas PostgreSQL REPEATABLE READ READ ONLY. Validación local PostgreSQL 16 aislado, tarea `rentabilidad-latte-identidad-20261119`.

## Necesidad y unidad de análisis

Una fila de confianza de octubre acredita costo de una receta de otro producto. El lector mensual de Rentabilidad mantiene pendiente esa misma venta porque usa su identidad Point original. Corregir únicamente la lectura de la pareja documentada, sin modificar hechos, recetas, ventas ni asignar costos por semejanza del nombre.

## Fuentes candidatas

|Concepto|Modelo / tabla|Creador|Identificador y ámbito|Evidencia|Consumidores|
|---|---|---|---|---|---|
|Venta nativa|PointDailySale / pos_bridge_daily_sales|Reporte oficial Point|1515268, 10/10/2026, sucursal Point 28, producto ERP 1001|SKU 010408, Latte Frans Coffie, 1 unidad, $100; sin receta|Rentabilidad mensual|
|Hecho derivado|FactVentaDiaria / reportes_factventadiaria|Pipeline V2|3694462, 10/10, sucursal ERP 18, point_product_id 1001|Clave receta:139, nombre Latte, receta 139, costo estimado 0|Confianza general y financiera|
|Origen del hecho|PointSalesDailyProductFact / pos_bridge_sales_daily_product_fact|Pipeline Point|737827, job 95690, 10/10, sucursal Point 28|Misma pareja producto 1001 / receta 139, match EXACT_CODE|FactVentaDiaria|
|Producto de la venta|PointProduct|Catálogo/importación oficial|ERP 1001, external_id official:010408, SKU 010408|Candidato nativo 1004 / Point 1064; snapshot 29363708 con costo 0|Inventarios, ventas y costos|
|Receta ajena|Receta, RecetaCodigoPointAlias, PointRecipeNode|Extracción de receta Point|Receta 139, alias 139, nodos 1326/2836 y posteriores|Código 0-2, Point PK 1013; BOM agua 455 ml y cuatro empaques. No acredita Point 1064|Costeo del Latte original|
|Costo de esa receta|RecetaCostoHistoricoMensual|Constructor mensual|1191, octubre 2026, receta 139|$5.609361, cobertura 100%; costo válido para la receta, identidad distinta de la venta pendiente|Confianza y costeo|

## Alias y equivalencias

- **Distintas:** Point 1013 / SKU 0-2 y Point 1064 / SKU 010408. El alias existente de receta 139 acredita solamente 0-2. No crear equivalencia por nombre.
- **Confirmado como conflicto:** pareja derivada producto ERP 1001 / receta 139 en octubre V2, frente al reporte nativo SKU 010408. La etiqueta EXACT_CODE del pipeline no reemplaza la prueba de identidad original.
- Las demás 18 unidades de Latte Frans Coffie siguen pendientes. Este cambio no acredita su receta ni costo.

## Decisión de diseño

Reutilizar `_cost_evidence` y la precedencia de `asignar_costos_filas`. La validación de costos de reventa existente compara la identidad de una adquisición; no valida una FK de receta. La asignación de recetas identificadas valida nuevas selecciones, pero no rechaza la FK recibida de un hecho. Ninguna puede reutilizarse para aceptar esta pareja ajena.

Antes de buscar evidencia de costo, quitar solo en memoria la receta de la pareja estable 1001/139, con external_id `official:010408`, SKU `010408`, origen V2 y mes octubre de 2026. Conservar el ID original en el grupo de lectura como `source_recipe_id` y marcar conflicto. Descartar también un costo materializado que intentase acreditar esa misma pareja. No depender del nombre mutable ni crear maestro, alias, migración o escritura correctiva.

El Latte correcto con SKU 0-2 conserva la receta 139 y su costo. Otros meses, identidades y fuentes quedan fuera del guard.

## Consultas y verificación

Inventario léxico local: `inventario_fuentes_datos --term costo --term receta --term identidad`; evidencia fuera del repositorio en el expediente de la tarea. Grafo no disponible para el checkout vigente; se inspeccionaron las definiciones acotadas y sus consumidores.

Expediente diagnóstico: `~/.codex-task-artifacts/rentabilidad-74-costos-20261119/`: `confianza.json`, `desglose.json`, `fuentes.json`, `latte-traza.json`. Contienen consultas de solo lectura y los IDs exactos. No se repitieron los 269 CFDI ya auditados.

Regresión: rechazo de receta ajena en confianza general/financiera, rechazo del costo materializado ajeno, conservación del producto 0-2 correcto, conservación de otros meses y FK histórica intacta.

## Riesgos y pendientes

El indicador se vuelve más estricto: se esperan 75 filas pendientes y $4,409 de venta sin costo, frente a 74/$4,309, sin cambiar ventas ni costo material mensual. No se pretende completar octubre. La corrección del origen histórico del pipeline, si se requiere, es una escritura separada y no está incluida. El guard permanece deliberadamente acotado al conflicto documentado; no constituye una auditoría de todas las FK del pipeline.
