# Ficha de fuentes — análisis de producto del agente ERP

Fecha y ambiente: 2026-10-18; código a2200b2a, PostgreSQL de producción consultado en transacción de solo lectura; pruebas PostgreSQL local sintético.

## Necesidad y unidad de análisis

Ventas por producto/rango/sucursal y contribución mensual por receta/sucursal. Cada lectura mantiene su periodo, cobertura y fecha de cálculo. Una consulta nueva no certifica que un cierre guardado esté actualizado.

## Fuentes candidatas

| Concepto | Modelo / servicio | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta | ventas.services.sales_read_service.get_sales_range_grouped | Fuentes Point, hechos analíticos, selección canónica por cobertura | Producto/receta de la fuente; fechas y sucursales | Servicio devuelve identidad explícita cuando puede acreditarla, no por similitud de nombre | Reportes, inventario, API |
| Ingredientes | Receta, LineaReceta | Flujos existentes de recetas | receta_id, línea, insumo, unidad | Herramientas de recetas ya incorporadas | Producción, costeo, agente |
| Fabricación | ProductoCostoOperativoMensual | OperatingFinanceSnapshotService | periodo + receta (únicos), ámbito empresa | Septiembre: 77; agosto: 74; julio: 77; junio: 75 | Rentabilidad, monitor de márgenes |
| Contribución | ProductoSucursalContribucionMensual | OperatingFinanceSnapshotService | periodo + receta + sucursal (únicos) | Septiembre: 521; agosto: 500; julio: 474; junio: 469 | Rentabilidad, monitor de márgenes |

## Alias y equivalencias

| Relación | Estado | Evidencia | Revisión |
| --- | --- | --- | --- |
| Fila de venta → recipe_id devuelto por servicio canónico | Declarada en fuente; no nuevo matching | FK existente y metadatos de la fuente | No crear equivalencias nuevas |
| Nombre parecido → receta | No resuelta | Helper legacy elige primer resultado aproximado | No usar para atribuir costos |
| Costo actual → venta histórica | Distinta | Fechas, unidades y contexto pueden diferir | No mezclar silenciosamente |
| Contribución → utilidad neta | Distinta | No certifica todos los gastos ni cobertura contable | No presentar como utilidad neta |

## Decisión de diseño

Dos contratos aditivos de lectura, con permisos y alcance del puente existente: ventas por producto y contribución mensual por receta/sucursal. Reutilizar búsqueda y detalle de recetas para ingredientes. Reutilizar tablas calculadas; no ejecutar recalculadores, crear tablas maestras ni nuevas capturas. No llamar la vista de precio sugerido: es amplia, usa heurísticas/fallbacks y no constituye un contrato acotado por sucursal.

Los importes guardados de contribución se exponen únicamente con contexto de costo consistente. Ausencia de costo, componentes sin importe, guardrail o discrepancia entre costo guardado y costo aplicado se marcan; no producir un margen alto por costo cero. Un componente cero no demuestra que el gasto real sea cero. La fracción margen_contribucion_pct se convierte a porcentaje multiplicando por 100; no se cambia el cálculo operativo.

El costo unitario de fabricación es empresarial; no se exponen ventas ni unidades empresariales del registro de costo a un usuario limitado a sucursal. Las filas de contribución y ventas se filtran con el alcance del servidor. El acceso a margen exige los permisos existentes de recetas y rentabilidad. No se conceden roles ni participantes.

Procedimiento: inventario_fuentes_datos --term margen --term costo --term ventas (PGOPTIONS read-only, timeout 15s); conteos agrupados por periodo de ambas fuentes con límite de cuatro meses. Evidencia: task-artifacts/ai-product-margin-20261018/source-inventory.txt. Las consultas de evidencia no ejecutan el agente ni API pagada.

Riesgos: snapshots incompletos o desactualizados; nombres sin identidad; vistas históricas con reglas diferentes. Mantener null, límites, advertencias y fecha de cálculo. Integración local probada con PostgreSQL, proveedor simulado y bloqueo de conexiones externas. Se validaron descubrimiento, lectura, revocación de historial y presentación en navegador. Publicación y validación en producción se registran por separado en el cierre de la tarea; este documento no declara ejecución con un modelo pagado.
