# Ficha de fuentes — identidad del costo mensual de 2025

Fecha y ambiente: 2026-11-05, PostgreSQL de producción en solo lectura y PostgreSQL 16 aislado para pruebas.

## Necesidad y unidad de análisis

El reporte deja sin costo ventas de productos cuyo costo mensual ya existe, porque la venta no tiene receta y el catálogo actual de Point ya no contiene el nodo correspondiente. Una identidad representa el producto nativo, código y nombre documentados para una receta en un mes de 2025. No representa una receta vigente en cualquier fecha.

## Fuentes candidatas

| Concepto | Modelo | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Costo e identidad documentales | RecetaCostoHistoricoMensual | Importaciones auditadas 43030 y 43036 de movimientos y exportaciones originales | Mes, receta; metadata con ID nativo, código, nombre, unidad y fuentes | 593 registros de 2025; 14 productos con 171 grupos de ventas sin receta y $242,208 de venta neta | Rentabilidad, resultado mensual y confianza de ventas |
| Producto nativo | PointProduct | Sincronización Point | external_id numérico; PK interna distinta | Los 14 códigos tienen ID nativo coincidente con el histórico; también existen espejos official que no son ese ID | Lectores de ventas |
| Receta | Receta | Catálogo existente | PK vinculada en el histórico | Código y nombre verificados en la importación y nuevamente al leer | Costeo |
| Identidad actual | PointRecipeNode, RecetaCodigoPointAlias | Extracción Point y catálogo | Código, nombre y receta | La ausencia de nodos actuales omite productos históricos; los aliases actuales solos no acreditan 2025 | Resolución existente, que conserva su precedencia |

## Alias y equivalencias

Se reutiliza exclusivamente la relación receta-producto ya acreditada por el costo mensual importado. Se exige coincidencia simultánea de ID externo nativo, código y nombre normalizado, mes exacto, fuente documental específica, análisis por unidad de producto, referencias originales y cobertura completa. Un espejo `official:1015` no equivale al ID nativo `409` por compartir código. Dos recetas para la misma identidad permanecen ambiguas. Los aliases actuales no habilitan costos de 2025 por sí solos.

## Decisión de diseño

Extender el lector compartido `asignar_costos_filas`, sin escribir vínculos en ventas ni crear tablas, aliases, recetas o costos. Solo resuelve filas sin receta y sin adquisición positiva o decisión DG prioritaria. Conserva la regla independiente de aliases de enero-septiembre de 2026 y la selección de costos vigente.

Consumidores revisados: `rentabilidad.services_resultado`, `rentabilidad.views_rentabilidad` y `reportes.sales_confidence`. No cambia contratos de API, modelos, configuración ni exportaciones; todos reciben el resultado del lector compartido.

Procedimiento reproducible: `inventario_fuentes_datos --term costo --term receta --term historico --term producto`; consultas acotadas de PointProduct, PointRecipeNode, RecetaCodigoPointAlias y los históricos; contraste de ID nativo con los originales. Las evidencias completas quedan en el expediente privado `cierre-integral`, archivos `identidades-costos2025-historicas-no-consumidas.json` y `productos-identidades2025-comprobacion-id-nativo.json`.

## Límites y validación

Esta corrección permite consumir costos documentados; no valida inventario físico, costo integral ni utilidad anual. Pruebas: recuperación en el resultado mensual, ausencia de escrituras, identidad incompleta/distinta/ambigua, exclusión de otro mes y ejercicio, conservación de receta registrada y precedencia de adquisición. Debe verificarse también el reporte de producción después del despliegue.

## Costos de conversión de 2025

Se reutiliza el mismo modelo mensual para seis recetas existentes con código y nombre
idénticos al producto nativo. La fuente `POINT_CONVERSION_UNIT_VALUATION_2025`
representa una pieza resultante (`ONE_NATIVE_CONVERTED_PRODUCT_UNIT`): suma del valor
documentado de los productos de origen dividida entre las piezas realmente obtenidas
en el mes. No se aplica un rendimiento fijo ni se recuperan fórmulas actuales de 2026.
El lector admite exclusivamente cada pareja correcta de fuente y unidad.

Se recuperaron 1,168 entradas nativas tipo 21 en 84 historiales de seis productos y
14 ubicaciones. Sus originales de entrada y salida comparten cabecera, y las salidas
se contrastan con historiales tipo 22 por producto, ubicación, cantidad y costo.
La fecha de Stock es UTC y se convierte a America/Mazatlan antes de agrupar por mes;
enero coincide con los ocho grupos comparables de la exportación mensual nativa.

De 67 combinaciones producto/mes, 43 tienen todos sus movimientos valorados y
verificados, importados mediante la auditoría 43069 sin modificar los 1,804
históricos anteriores, incluidos los 593 de producción de 2025.
Las otras 24 conservan excepciones: 19 orígenes sin costo, seis
rendimientos que no acreditan fraccionamiento y diez movimientos en Devoluciones
sin trazabilidad a venta. No se completan con otros meses ni se omiten movimientos
para declarar cobertura completa. Tampoco se acredita empaque adicional, mano de
obra ni utilidad integral por haber valorado el producto de origen.

Evidencia privada congelada: `costos-conversiones2025-lote43-validado.json` y archivo
de soportes SHA256 `395eb887d3b4dbb9ce0d04360770dd7032472e9ab54a25000784f0d03993c97e`.
La consulta de fuentes usa `inventario_fuentes_datos` con costo, conversión, receta
e histórico. Se mantienen los consumidores y contratos ya descritos; las mismas
pruebas de identidad, mes, ejercicio, cobertura, ambigüedad, precedencia y ausencia
de escritura se ejecutan para ambas fuentes.
