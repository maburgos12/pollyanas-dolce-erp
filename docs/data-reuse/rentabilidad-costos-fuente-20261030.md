# Ficha de fuentes — Costos de rentabilidad mensual

Fecha y ambiente consultado: 2026-10-30; producción en solo lectura, septiembre de 2026. Validación de código en PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Costo por unidad de producto vendido, complemento consumido o merma de una sucursal y mes. No confundir el identificador interno de un insumo de Point con el de un producto vendible.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Ventas | PointDailySale | Sincronización Point | Producto, sucursal, fecha | Ventas netas de septiembre conciliadas con Point | Rentabilidad y finanzas operativas |
| Complementos | RecetaAgrupacionAddon | Revisión de recetas | Código y nombre exactos, relación activa APPROVED | 29 relaciones aprobadas; una relación de Zanahoria C apunta a Crunch Mini y debe rechazarse | Resultado mensual y panel de productos |
| Recetas | RecetaCostoHistoricoMensual | Costeo histórico | Receta y mes, cobertura 100%, costo por unidad | Costos de complementos y cinco recetas de merma disponibles | Costos de venta y merma |
| Merma | MermaMensualSucursal | Captura/importación operativa | Sucursal, producto, receta, mes | 38 filas sin importe recuperables por receta: $3,542.68; Guamúchil $298.88 | Resultado mensual |
| Reventa | ProductoReventaCosto / ProductoReventaCostoHistoricoMensual | Compras y captura de existencias | Producto, vigencia y source_rows | Siete históricos de septiembre contienen capturas de otro artículo | Rentabilidad, costeo histórico y finanzas operativas |
| Evidencia alternativa | Compras e historial de movimientos Point; CFDI recibidos; indicadores de almacén | Point, SAT y Administración | Producto exacto, fecha, cantidad y unidad | 1,205 compras, 2,412 detalles y 2,374 CFDI examinados; movimientos con costos de Caja G, Tarjeta de regalo y Vela humo rosa | Investigación; no se importaron nuevas equivalencias |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Insumo interno / producto interno Point | Distintos | Extra 10 recibió costo de Sanitas; Caja G recibió costo de manga para decorar | No reutilizar coincidencia numérica entre espacios |
| Complemento / receta aprobada | Confirmada por fuente existente | Código y nombre normalizados deben coincidir; código 1254 también aparece en Crunch Mini y no autoriza usar Zanahoria | No se crean relaciones nuevas |
| Caja G, Tarjeta de regalo, Vela humo rosa | Evidencia externa localizada | Movimientos del producto exacto anteriores al cierre: $19.86, $2 y $20 respectivamente | Importación operativa pendiente; no incluida en este cambio de lectura |
| Extras, estuche y demás artículos sin costo | No resuelta | Point conserva costo cero; los CFDI candidatos no prueban identidad o rendimiento | Confirmar artículo/consumo real antes de equivalencias nuevas |

## Decisión de diseño

Reutilizar las relaciones de complementos ya aprobadas y las identidades producto/receta existentes en PointRecipeNode, exigiendo código y nombre inequívocos y nombre de receta consistente. Reutilizar los costos históricos o versiones anteriores al cierre. Empaque Pay/Pastel, Crunch Mini y Fresa Rebanada tienen composición en Point y receta ERP existente; la comprobación nominal evita heredar la relación equivocada de Zanahoria C. Reconstruir solamente en lectura el importe de merma sin costo; conservar las capturas existentes. Excluir del cálculo capturas de almacén cuyo nombre pertenece a otro producto y evitar que nuevas capturas mezclen IDs internos de insumos/productos. Mantener el histórico y sus fuentes para auditoría.

Compartir el lector validado de reventa con finanzas operativas. No crear tablas, migraciones, porcentajes estimados ni capturas paralelas. Los históricos contaminados conservados no se certifican: el lector busca otra fuente válida anterior al cierre o mantiene el pendiente.

La consulta real al proveedor IA devolvió HTTP 429 `credit_balance_exhausted`. El manejo de errores debe preservar el diagnóstico anterior y no marcar una consulta fallida como análisis completado.

Procedimiento reproducible: `manage.py inventario_fuentes_datos --term costo`; consultas acotadas con transacción de solo lectura; captura externa sin persistencia. Evidencia local de la tarea: `Documents/Codex/2026-10-30/margen-bruto-fuentes` (auditoría, compras, CFDI, historial, respuesta IA y pruebas). No contiene cambios de datos operativos.

## Validación y pendientes

246 pruebas de rentabilidad, confianza de ventas, finanzas operativas, gastos, personal e importación de costos aprobadas en PostgreSQL 16. Se cubren colisión de identificadores, complemento sin duplicar base, merma con cobertura insuficiente, histórico contaminado y error del proveedor sin sobrescribir diagnóstico.

Navegador local: producto sintético con venta $10, costo $6 y merma $3 muestra utilidad bruta por producto $4 y resultado parcial $1, sin errores de consola.

Pendientes: validación visible tras despliegue, costos aún no documentados e identificación operativa de conceptos genéricos. La cuenta del proveedor requiere créditos para completar un diagnóstico real. Este cambio no convierte un resultado incompleto en utilidad neta ni modifica costos maestros.
