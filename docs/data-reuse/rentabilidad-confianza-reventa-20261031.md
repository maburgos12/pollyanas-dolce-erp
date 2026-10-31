# Ficha de fuentes — confianza de costos de reventa

Fecha y ambiente consultado: 2026-10-31; producción en solo lectura para diagnóstico, PostgreSQL 16 aislado para pruebas. Mauricio autorizó corregir el criterio compartido y registrar tres costos comprobados.

## Necesidad y unidad de análisis

La confianza no debe exigir una receta a un producto de reventa con costo documentado. Cada grupo conserva mes, identidad del producto, fuente y cantidades de las ventas seleccionadas; no se reconstruyen ni modifican FactVentaDiaria ni ventas originales.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta seleccionada | FactVentaDiaria | analytics_service desde fuentes Point | día/sucursal/producto/fuente | Septiembre: $3,310,119.13; $110,355.93 marcado sin costo | core, reportes, rentabilidad |
| Identidad | PointProduct | integración Point | FK explícita o external_id + nombre normalizado exacto y único | Pirotecnia G: clave externa 169, ERP 84; ERP 169 es otro artículo | ventas, inventario, reportes |
| Adquisición | ProductoReventaCosto | compras/almacén/historial Point | producto + fecha + source_hash | Pirotecnia Ch $10.28, Coca-Cola $15.63, Espagueti $29.84 | costeo histórico, finanzas, rentabilidad |
| Costo de receta | RecetaCostoHistoricoMensual / RecetaCostoVersion | servicios de costeo existentes | receta + mes o versión previa al corte | complementos y empaques ya recuperados por lector común; cobertura completa requerida | mismos consumidores |
| Costo mensual | ProductoReventaCostoHistoricoMensual | servicio histórico | producto/mes + source_rows | existen históricos contaminados por colisión de IDs, ya rechazados por lector común | mismos consumidores |

## Alias y equivalencias

| Identificadores | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Fact.point_product_id | Confirmada | FK existente, no se modifica | Reutilización |
| Clave autoritativa y external_id de PointProduct | Confirmada solo con nombre exacto normalizado y candidato único | No tratar clave externa como PK; no igualar SKU, quitar ceros, buscar por nombre solo ni elegir entre duplicados | Incluida en corrección autorizada; cálculo en memoria |
| Costos de otro artículo | Distintos | metadata.point_name incompatible | Se rechazan; registros conservados |

## Decisión de diseño

Extender build_sales_confidence con evidencia de reventa y recetas para grupos que carecen del respaldo mensual reconocido. Reutilizar el lector validado de rentabilidad; conservar también el mes de origen para mostrar arrastres. Mantener la precedencia por sucursal/día, exclusión de ventas espejo, controles de identidad, fecha máxima y faltantes de productos regalados. Cantidad cero no puede respaldar una venta positiva. Para recetas sin respaldo en la materialización se reutilizan el histórico mensual completo y las versiones previas al corte, descontando mano de obra e indirectos; complementos y empaques usan únicamente las identidades ya validadas por el lector compartido. No se crean equivalencias nuevas.

Consumidores revisados: core/views.py, rentabilidad/views_rentabilidad.py, reportes/views.py, reportes/executive_panels.py. La interfaz de _costos_reventa_para_periodo sigue devolviendo importes; el lector de evidencia agrega vigencia sin cambiar sus callers.

Consultas reproducibles: inventario_fuentes_datos --term costo; selected_sales_facts septiembre 2026 agrupado por clave/nombre y metadata.costing; fuentes ProductoReventaCosto/Historico y movimientos Point 1685911, 1683689 y 1685236. Evidencia local fuera de Git en Documents/Codex/2026-10-30/margen-bruto-fuentes y 2026-10-31/confianza-reventa.

## Captura autorizada y pendientes

Añadir Caja G $19.86 (30-sep), Tarjeta regalo $2 (28-sep) y Vela humo rosa $20 (29-sep), PZA; source_hash idempotente y movimiento íntegro en metadata. Conservar todos los registros previos. No recalcular históricos ni alterar identidades maestras.

Persisten faltantes de Extras, Estuche Escolar, Topping Zanahoria C y otros artículos detallados en la evidencia. La equivalencia paquete/pieza de servilletas requiere revisión separada. La cobertura monetaria no certifica utilidad neta ni ignora artículos de venta cero. El proveedor IA respondió falta de crédito; esta autorización no permite compras de crédito ni cambios de credenciales.

Verificación productiva: Té del Jardín tiene receta 125 con modo_costeo=REVENTA, PointProduct 283 (external_id 402), adquisición válida $14 y una versión antigua de receta en cero. Las ventas Point no llevan receta; seis filas históricas sí la conservan. El criterio usa la clasificación REVENTA ya existente para elegir adquisición y no trata esa receta como fabricación. No modifica modo_costeo, FK ni ventas.
