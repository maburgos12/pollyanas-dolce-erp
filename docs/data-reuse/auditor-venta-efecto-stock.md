# Ficha de fuentes — venta comercial y efecto de stock

Fecha y ambiente: 2026-10-03, lectura PostgreSQL de producción y pruebas aisladas.

## Necesidad y unidad de análisis

Evitar que una cantidad comercial se sustituya por un efecto de stock distinto únicamente porque el historial alcanza el cierre. Unidad: producto, sucursal y mes; conservar ambas evidencias sin declarar equivalentes sus unidades.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta comercial | PointDailySale / pos_bridge_daily_sales | Reporte oficial Point | Fecha + sucursal + producto | Filas originales y cantidades conservadas; no modificadas | Trazo agregado y Producido vs Vendido |
| Movimiento de stock | PointProductHistoryImport y PointProductHistoryRow | Captura canónica existente | Sucursal/producto + FK_Movimiento | Hay historiales completos con cantidad de ventas distinta al reporte comercial | Servicio histórico y materializador |
| Proyección auditada | ProductInventoryAuditCase | InventoryAuditMaterializer | Mes + sucursal + producto | source_trace guarda fuente comercial y comparación con historial | Auditor, pantalla y exportaciones |
| Categoría/uso | PointProductCategory y ProductBusinessRule | Flujos existentes de catálogo | SKU o nombre normalizado confirmado | Una regla de reventa no prueba que todo complemento tenga igual efecto de stock | Separación de consumo y productos vendidos |

## Alias y equivalencias

Venta comercial y salida/retorno de stock son **distintos** mientras falte relación documental, fecha de contabilización o configuración de control de inventario. No crear equivalencias por nombre ni cambiar categoría por suposición.

## Decisión

Reutilizar las fuentes y source_trace existentes. Si un historial completo, sin movimientos desconocidos, alcanza el cierre pero sus ventas difieren de la fuente comercial, conservar la cantidad comercial y señalar SOURCE_INCOMPLETE con cantidades, producto, sucursal y referencias comerciales concretas. Guardar el historial como evidencia no aplicada, no como permiso para normalizar ventas o cerrar el mes.

En la lectura del reporte, recuperar la cantidad comercial ya conservada en aggregate_comparison para proyecciones anteriores; no escribir datos durante GET. Pantalla y exportaciones comparten esa lectura. Recalcular la auditoría con el materializador oficial, sin HTTP ni cambios en fuentes, para actualizar clasificaciones anteriores.

Procedimiento: inventario de fuentes con términos venta/historial/inventario; comparación acotada de filas canónicas, fuente comercial y metadata existente. Pruebas de materializador y reporte antes del cambio, seguido del flujo oficial PR/deploy y pantalla autenticada.

Pendientes: una diferencia puede deberse a fecha de contabilización, cancelación o relación con un producto base. Esta protección no decide cuál; exige comprobarla. No afecta inventario, mermas, RRHH, categorías ni ajustes operativos.
