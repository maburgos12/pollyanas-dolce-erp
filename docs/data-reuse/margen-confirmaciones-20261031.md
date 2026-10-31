# Ficha de fuentes — cierre de costos de septiembre

Fecha y ambiente: 2026-10-31, consultas acotadas a producción ERP y Point.

## Necesidad y unidad de análisis

Costo directo por producto y mes, con reglas explícitas para cargos y regalos. Resolver los 15 conceptos pendientes sin reescribir ventas ni duplicar costos centrales.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Identidad | PointProduct | Point / sincronización | PK ERP + external_id + nombre | 186/961 topping, 87/1001 vela, 994/1059 topper, 299/879 humo | Confianza, rentabilidad, productos |
| Costo adquirido / regla DG | ProductoReventaCosto | Point y confirmación Mauricio | Producto, fecha, source_hash | 7432 vela espiral; compras 1636574/936988/524177 | Mismos tres lectores |
| Complemento | Receta, LineaReceta, RecetaAgrupacionAddon | BOM Point y aprobación DG | Point 961, regla 26, SKU 1254 + nombre | 20g crumble 384, etiquetas 98/99; relación antigua apuntaba 472 Crunch Mini | Mismos tres lectores |
| Costos insumos | CostoInsumo | Fuentes ERP existentes | Insumo, fecha | 10227:96.244360/kg; 9836:0.43;10459:0.56 | Referencia mensual de receta |
| Venta | FactVentaDiaria / PointDailySale | Sincronización existente | Fuente, fecha, sucursal, producto | Septiembre 2026; lectura sin mutaciones | Confianza / rentabilidad |

## Alias y equivalencias

| Identificadores | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Topping 961/1254 y receta C | Confirmada | BOM directa; Crunch Mini comparte SKU pero es distinto | Mauricio pidió obtener receta Point |
| Topper estrella negro y Feliz Cumpleaños Negro | Precio confirmado | Mauricio confirmó mismo precio, factura AA16060 a 39.44/pza | Autorizado en chat |
| Vela individual y 7190 espiral estrella | Confirmada, costo de referencia | Imagen corregida; paquete de 8 a 24.90; sustituye referencia previa de 5 piezas | Autorizado en chat |
| Humo azul ERP299 y ERP496 | Distintas identidades sin fusión | Mismo SKU no prueba equivalencia de registro | No se modifica 496 |

## Decisión

Reutilizar tablas de costos, recetas y relación aprobada existentes. Agregar registros de costo trazables, conservar anteriores. Metadata de regla manual requiere autorización, identidad exacta y tratamiento válido; cero sin regla continúa faltante. Domicilios mantienen costo sin asignar y margen individual N/D. El indicador global es provisional. Los costos superiores a venta son advertencias, no costos ausentes.

Procedimiento: `inventario_fuentes_datos --term costo --term receta --term reventa`; lecturas acotadas y simulación transaccional de las 14 reglas/costos más receta. Evidencia y respaldo en Documents/Codex/2026-10-31/margen-cierre-confirmado. Aplicación idempotente por hash.

Limitaciones: receta actual de Point aplicada como referencia autorizada de septiembre, no reconstrucción diaria. Vela y humo arrastran compras históricas. Gastos centrales/fijos conservan sus faltantes. No se certifica utilidad neta; no se modifica servilleta ni datos de venta.
