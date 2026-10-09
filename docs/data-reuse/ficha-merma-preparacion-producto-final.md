# Ficha de fuentes — merma de preparación frente a producto final

Fecha: 9oct2026. Fuentes originales Point ya guardadas; revisión local PostgreSQL16 aislado. No nueva consulta Point.

## Necesidad y unidad de análisis

Una merma conserva su dominio original: preparación/insumo no equivale al producto final homónimo. El reporte Producido vs Vendido no debe heredar una merma de preparación por coincidencia de nombre o código.

## Fuentes candidatas

| Concepto | Modelo | Creador/actualizador | Identidad y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Merma original | PointWasteLine | sincronizador mensual Point | movimiento, sucursal, fecha, receta e insumo | 1460/mov1662740:42PZA, receta501 PREPARACION, insumo623 | lectores mensual y por sucursal, auditor, reporte |
| Dominio de receta | Receta, Insumo, PointRecipeNode | fuentes existentes | FK y tipo, no nombre | nodo7649:INSUMO565/01GCHM01/PREPARED_INPUT | lectores de movimientos |
| Producto final distinto | PointProduct | catálogo Point | PRODUCTO1039/01GCM01, no INSUMO565 | expediente2232 tenía producción0/venta0 y merma42 resuelta por nombre | reporte y exportaciones |
| Proyección persistida | ProductInventoryAuditCase | InventoryAuditMaterializer | mes/sucursal/producto | conserva evidencia anterior al retirar un caso del rebuild | informe, agente y expediente |

## Alias y equivalencias

Galleta Chocolate Mini (preparación INSUMO565) y producto1039 son **distintos**, aunque coincida el nombre. No crear alias, cambiar maestros ni importar el historial INSUMO como PRODUCTO. El historial original de preparación da 0+618−42−576=0; las576 salidas de producción no prueban paquetes vendidos ni una equivalencia de seis piezas.

## Decisión

Extender el lector existente por sucursal para omitir la merma con FK insumo y receta PREPARACION, como ya separa el lector mensual. Mantener mermas de PRODUCTO_FINAL aun cuando tengan FK insumo. Preservar originales y autoridad del job mensual; no fabricar cero histórico ni COMPLETE.

Después del rebuild oficial, omitir de la lectura del reporte parcial únicamente una proyección retirada originada exclusivamente por esas mermas preparadas comprobadas. Rechazar retiro si hay otras actividades, fronteras o historial propios, filas faltantes, otro mes/sucursal o cantidad contradictoria. El caso histórico no se borra ni se aprueba.

## Verificación

Inventario de fuentes: `inventario_fuentes_datos --term 'merma preparación' --term 'insumo preparado'`, PostgreSQL propio. Regresiones del lector y reporte: homónimo/código, producto final dual, proyección retirada, cantidad y mes incompatibles. Aceptación: rebuild oficial dos veces sin HTTP, originales intactos y reporte autenticado sin42PZA atribuidas al producto1039. Publicación no equivale a cierre del mes.
