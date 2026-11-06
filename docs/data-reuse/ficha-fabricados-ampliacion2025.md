# Ficha de fuentes — ampliación documental de fabricados 2025

## Necesidad y unidad de análisis

Completar 56 pares producto/mes de 21 productos Point sin receta ERP: 237 detalles nativos de producción, cotejados contra sus entradas originales. Cubren 152 grupos de ventas por $133,377.54 netos. El costo es el promedio ponderado por unidades producidas del mismo mes, no una certificación del costo de existencias vendidas ni de utilidad anual.

## Fuentes existentes y decisión

Se reutiliza el contrato de `ficha-produccion-documental2025.md`, `PointProduct`, `PointBranch`, `PointProductionLine` y el lector común `asignar_costos_filas`. No se crean modelos, equivalencias de recetas, adquisiciones ni movimientos de inventario. El inventario de fuentes se ejecutó en PostgreSQL 16 aislado con términos production/costo/producto; las identidades y ausencia de pares se verificaron mediante consulta de sólo lectura a producción el 6 de noviembre de 2026.

| Fuente | Identificador / ámbito | Creador / actualizador | Evidencia | Consumidor |
|---|---|---|---|---|
| PointProduct | external_id nativo + SKU + nombre de catálogo | Sincronización Point | 21 productos, identidad nativa exacta | Ventas y rentabilidad |
| Órdenes nativas 2025 | FK_Produccion + PK_Produccion_detalle | Point | 237 pares activos/finales, cantidades y costo positivos | Staging documental |
| Entradas de producción | Movimiento + línea + fecha + centro | Point | Cantidad, unidad, nombre y costo unitario exactos contra orden; SHA256 de originales | Evidencia del importador |
| Reducciones por venta 2025 | Día + sucursal histórica + SKU | Point | 528 grupos diarios y 565 filas: cantidades coincidentes y unidades idénticas | Contraste de unidad vendida |
| PointProductionLine | Par nativo + source_hash | Integración/importador auditado | 0 filas existentes para los 237 pares antes de importar | Lector y consumidores de staging ya documentados |

## Identidad, unidad y procedencia

- Cada ID nativo coincide con `PointProduct.external_id`, SKU y nombre. No se confunde PK ERP con ID Point ni se crea una receta aproximada.
- U y PZA se conservan literalmente por grupo; no se convierten ni se consideran equivalentes. Todas las reducciones vendidas de cada grupo usan la misma unidad que su producción.
- 236 líneas proceden de CEDIS (Point 8 / PK ERP 3) y una de Matriz (Point 1 / PK ERP 10). Dona Chocolate julio agrega dos unidades producidas en CEDIS y cinco en Matriz. Cada línea conserva su sucursal nativa; no se reasigna una a la otra ni se rellena `erp_branch_id` en el staging documental.
- Dos productos Halloween (Point 941/SKU1684 y Point 942/SKU6911) tienen un espacio final en el nombre nativo que el catálogo no conserva. El manifiesto fija ambos literales, con ID y SKU exactos. La guarda sólo admite esa diferencia de espacios finales; no hay normalización general ni alias nuevo.
- Se excluye Dona Chocolate junio porque una orden tiene estado 2. No se promedia únicamente el subconjunto finalizado. Siete grupos sin ventas pendientes no se agregan al manifiesto.

## Alcance y contratos

El cambio mínimo amplía el manifiesto con 56 grupos y permite unidad explícita, nombre nativo literal y sucursal por línea en el helper existente. Los 20 grupos de galletas y sus 216 hashes anteriores permanecen intactos. Total esperado: 76 grupos, 453 pares únicos. No cambia la precedencia de adquisición, receta o decisiones DG, ni se habilita otro año o un mes parcial. Todas las filas continúan con `financial_close=False`.

El importador auditado usa `bulk_create` únicamente en `PointProductionLine`, sin señales que creen inventario o reconstruyan analítica. Registra AuditLog y paquete de fuentes; exige precondiciones, respaldo, manifiesto desplegado, invariancia de datos protegidos y repetición idempotente. No sobrescribe registros existentes ni permite lotes parciales.

Consumidores afectados: panel de productos, resultado mensual y confianza de ventas a través del lector compartido. Los consumidores de staging conservan sus límites: receta NULL sigue pendiente de identidad ERP y no acredita inventario físico completo.

## Evidencia y aceptación

Los originales y archivos auditables permanecen fuera del repositorio en `continuacion2025-costos-20261106`: `fabricados-pares-validados.json`, `identidades-y-staging-actual.json`, `unidades-venta-verificadas.json`, `staging-payloads.json` y los scripts reproducibles. El manifiesto versionado guarda hashes, no datos personales de responsables.

Pruebas: promedio nativo con precisión Decimal; U/PZA mixtas o unidad incorrecta rechazadas; nombre diferente sin literal revisado rechazado; sucursal incorrecta rechazada; mismo producto producido en dos centros ponderado sin duplicar; duplicados, falta de líneas, otro mes/año y costo no positivo rechazados. Mantener pruebas de precedencia y consumidores existentes. Comparar lector completo 2025/2026 antes y después, ventas y otros costos intactos, y verificar panel real antes de cerrar.
