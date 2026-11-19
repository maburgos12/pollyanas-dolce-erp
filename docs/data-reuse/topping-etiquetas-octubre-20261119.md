# Ficha de fuentes — etiquetas incrementales de Topping

Consultas productivas READ ONLY del 18/11/2026 y pruebas PostgreSQL 16 aisladas.

## Necesidad y unidad de análisis

Material vendido incremental por sucursal y mes. Dirección autorizó reconocer las dos etiquetas de Topping Zanahoria C en octubre, conservando los 20 g de crumble en la receta base. Se revisan 123 filas, 167 unidades y diez sucursales históricas. El importe autorizado es $167.69 con redondeo por sucursal.

## Fuentes candidatas

| Concepto | Fuente existente | Identidad y evidencia | Consumidores |
|---|---|---|---|
| Relación aprobada de complemento | RecetaAgrupacionAddon | Regla 26, base 55, complemento 516, APPROVED | Resolución de identidad y costeo agrupado |
| Identidad Point | PointProduct | ERP 186, Point 961, SKU 1254 | Ventas y Rentabilidad |
| Componentes | LineaReceta | Crumble 384, 20 g en ambas recetas; etiquetas 98 y 99 solo en complemento | Costeo completo de receta |
| Precio de etiquetas | CostoInsumo | Cuatro fuentes de 98 y ocho de 99 en octubre, normalizadas en piezas | Algoritmo mensual existente |
| Ventas | ventas_para_costeo | 123 filas, 167 piezas, importe cero, diez sucursales históricas; base suficiente el mismo día/sucursal | Resultado mensual y panel de productos |
| Costo total histórico de receta | RecetaCostoHistoricoMensual | Base 55 octubre, snapshot 1145, $139.957253. Complemento septiembre 1095, $2.914887. Octubre sin snapshot del complemento | Contrato de recetas, semanal, logística, mano de obra y merma |
| Merma | MermaMensualSucursal | Cero registros productivos de receta 516 en octubre, consulta acotada | Lector de merma con costo total de receta |

## Alias y equivalencias

SKU 1254 también identifica Crunch Mini, producto distinto. El tratamiento exige PK ERP 186, external_id 961, SKU 1254, regla 26 y recetas 55/516. El nombre auditado se conserva como referencia. Un cambio legítimo del nombre de catálogo conserva la identidad basada en las claves.

## Decisión de diseño

Extender la relación aprobada existente con metadata JSON, default vacío y sin carga de datos en migración. La carga autorizada ocurre mediante `aplicar_etiquetas_topping_octubre`, por defecto en READ ONLY. `--apply` modifica exclusivamente metadata de la fila 26, conservando el resto de sus claves. Repetir una carga idéntica conserva la misma revisión y rechaza tratamientos distintos.

El documento mensual queda en `monthly_sold_material_treatments[2026-10-01]` con contrato, ámbito, autorización, identidad, componentes, precios y filas fuente, ventas comprobadas, componentes absorbidos y fecha de aplicación. El comando reutiliza la normalización y los pesos del algoritmo mensual existente. No crea una segunda captura de ingredientes o ventas.

El costo de Etiqueta G es $0.435795 y el de Etiqueta Rectangular $0.568304. Una unidad consume una pieza de cada etiqueta. El importe de sucursal es `round(cantidad de sucursal × 1.004099, 2)`. Se añade al material previamente cuantizado a centavos una vez, conservando el importe anterior también en los empates de medio centavo. La suma de las diez sucursales es $167.69. El panel de productos suma esos mismos importes por sucursal y el reporte mensual/CSV utiliza el mismo lector de resultados.

El lector de material vendido activa explícitamente este tratamiento solo en Rentabilidad. La etiqueta del panel es Complemento incremental. Los consumidores de receta completa, costeo semanal, merma, logística, mano de obra y confianza general conservan su lectura existente. No se guarda el importe incremental en un histórico que represente el costo total de una receta.

La banda de confianza financiera del dashboard también recibe el opt-in explícito. Su consulta conserva cantidades por sucursal únicamente para el producto candidato y agrega los grupos de recetas antes de redondear, con cantidades convertidas a texto para conservar precisión decimal. La confianza general conserva su comportamiento por defecto. Las filas autorizadas desaparecen del contador de costos faltantes de Rentabilidad y los restantes faltantes conservan su estado.

## Comprobaciones y límites

El comando valida fuentes de precio exactas, costos finitos positivos, unidades, 20 g de crumble compartidos, venta gratuita, absorción diaria por base y cantidades aprobadas. Usa REPEATABLE READ y el mutex mensual. El tratamiento rechaza metadata de otro mes, identidad, ámbito, costo o componentes. No altera otras relaciones ni períodos, ventas o mermas. La autorización de este costo conserva los restantes pendientes de octubre y el estado financiero de cierre.

Evidencia externa en `~/.codex-task-artifacts/rentabilidad-fuentes-recuperables-20261118` y `~/.codex-task-artifacts/topping-etiquetas-octubre-20261119`. Se ejecutó el inventario de fuentes con los términos costo, compra y receta en el diagnóstico previo. La publicación se coordina después de las aperturas y de la equivalencia Stock.
