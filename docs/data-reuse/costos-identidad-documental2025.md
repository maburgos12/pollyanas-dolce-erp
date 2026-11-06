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
