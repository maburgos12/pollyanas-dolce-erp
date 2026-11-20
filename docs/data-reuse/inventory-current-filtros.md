# Inventory/current: limitar la lectura al ámbito solicitado

## Necesidad y fuentes

El GET JSON de `inventory/current/?product=0160&branch=1` respondió correctamente
con 330 bytes, pero tardó 46.8065 s en la comprobación HTTPS del 19 de noviembre.
Esa observación no aísla por sí sola el costo de SQL. La consulta anterior
seleccionaba el último snapshot de todas las parejas antes de aplicar los filtros
externos. No se añade otra fuente, captura, tabla, índice ni caché.

| Concepto | Fuente existente | Identidad y consumidores |
| --- | --- | --- |
| Inventario registrado | `PointInventorySnapshot` / `pos_bridge_inventory_snapshots` | Pareja branch/product; orden captured_at DESC, id DESC. Current, availability y low-stock. |
| Identidad de producto | `PointProduct` | Nombre contiene, SKU exacto o external_id exacto; categoría contiene. |
| Identidad de sede | `PointBranch` y su `Sucursal` | Nombre contiene, external_id exacto o código ERP exacto; FK ERP puede ser nula. |
| Contrato de respuesta | `CurrentStockSerializer`, `LargePagination` | Mismos productos, sedes, decimales, fechas, orden y paginación. |

El índice compuesto `pb_inv_latest_idx` ya está declarado en el modelo y la
migración 0014. No se modifican modelos, migraciones ni los snapshots existentes.
La existencia y uso físico del índice en producción no se infieren del código.

## Reutilización compatible

Los filtros de identidad son constantes dentro de una pareja branch/product:
filtrar antes del último registro conserva el mismo ganador que filtrar después.
`_latest_snapshot_qs` recibe opcionalmente un queryset privado, sin evaluarlo, y
reutiliza ese ámbito dentro y fuera de DISTINCT ON. Los predicados OR tienen una
sola definición y también conservan los filtros del listado histórico.

No se empujan filtros de stock, fecha o job antes del ganador. Current conserva
productos inactivos y parejas con captura antigua aunque falten en el job nuevo.
No se sustituye el historial por un único job ni se inventa un cero al faltar una
captura. Availability y low-stock aplican sus condiciones al último registro;
list/retrieve mantienen su contrato. No cambian API pública, autenticación,
serializers, fórmulas ni la cantidad de consultas por fila.

## Verificación y límites

La regresión PostgreSQL con ruido histórico mostró que el DISTINCT anterior
resolvía 73 parejas para devolver una. Se verifica el número de parejas resueltas
en el plan real, no un umbral de milisegundos de unittest. También se comparan
JSON completo, filtros OR y combinados, desempate por ID, 70 parejas históricas,
ceros, paginación, lectura lazy y consumidores relacionados.

La aceptación de rendimiento requiere comparar el lector completo y un GET JSON
HTTPS después de publicar, con datos, fechas y condiciones declaradas. El baseline
real ya existe y no se repite sin necesidad. No se declara una mejora productiva
solo por los tests o por el SQL generado. Las mediciones de aplicación privada y
de navegador se registran por separado; nunca se vacía una caché compartida.

La ingesta #1719 y sus datos quedan fuera de esta corrección. La publicación de
esta tarea espera al mensual #1766 y se coordina antes de los rótulos de Cierre.
