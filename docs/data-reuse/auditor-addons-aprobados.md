# Ficha de fuentes — separación de add-ons aprobados en auditoría

Fecha y ambiente consultado: 3 de octubre de 2026; PostgreSQL del VPS, consultas acotadas de lectura. Pruebas en PostgreSQL 16 aislado.

## Necesidad y unidad de análisis

Separar complementos comerciales aprobados del balance de producto vendido, sin borrar sus cantidades comerciales ni sus expedientes. Una fila de auditoría representa producto/sucursal/mes; una agrupación add-on representa una relación comercial aprobada entre receta base y código Point del complemento, no un movimiento físico ni una equivalencia de cantidades.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Relación comercial aprobada | recetas.RecetaAgrupacionAddon / recetas_recetaagrupacionaddon | Flujo existente de agrupaciones y aprobación comercial | base_receta FK + addon_codigo_point; activo/status | 29 reglas activas APPROVED, creadas abril/mayo antes de septiembre. Regla 1: receta 110 / código 0003 y add-on receta 119 / 03SPFREB | Composición comercial y costeo semanal; filtro compartido de consumo |
| Configuración histórica Point | pos_bridge.PointRecipeNode | Sincronización existente de fichas | receta FK, captura y raw_detail | Receta 119, nodos septiembre 5716/5919/6115/6316/6519/6722/6926/7130: Reducir_nventa=False. Configuraciones guardadas de los otros sabores aprobados también acreditan ese valor | Investigación de efecto comercial vs stock |
| Ventas comerciales | pos_bridge.PointDailySale | Importación canónica de ventas | producto/código, sucursal y fecha | Caso 2565 conserva cantidad comercial 14; el historial registra efecto neto -1. No son cantidades intercambiables | Producido vs Vendido y consumo/costeo |
| Expediente mensual | reportes.ProductInventoryAuditCase | InventoryAuditMaterializer | producto FK + sucursal FK + mes | 13 códigos aprobados sin nombre TOPPING tienen 130 casos septiembre; 66 con diferencia no cero antes de separar el ámbito | Agente, reporte, exportaciones |
| Clasificación previa | pos_bridge.PointProductCategory | Catálogo existente | codigo_point único | TOPPING ya queda separado; no crear ni cambiar categorías para extender este contrato | inventory_consumption_filter |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| 03SPFREB / receta 119 como complemento de receta 110 | Confirmada | Regla 1 ACTIVE/APPROVED, fuente POINT_ZERO_REVENUE_ADDON y notas de curación comercial DG | No se crea una equivalencia nueva |
| Otros códigos de reglas activas APPROVED | Confirmada por cada regla | Identidad exacta addon_codigo_point; no inferencia por nombre Sabor, precio cero o categoría de receta | DETECTED/REJECTED/inactivas no habilitan separación nueva |
| Cantidad comercial del complemento / piezas del pastel base | No resuelta por agregados | Falta relación individual de ticket/movimiento; una regla comercial no acredita consumo físico 1:1 | No atribuir movimientos ni descontar piezas de la base |

## Decisión de diseño

Extender únicamente inventory_consumption_filter con los códigos de reglas activas APPROVED no vacíos. Reutilizarlo en sold_products, BranchInventoryTraceabilityService y MonthlyPointProductBalanceService. No crear tabla, importación, categoría, ajuste ni otra captura. La composición comercial y el costeo de insumos existentes permanecen intactos.

La separación es de ámbito del reporte de producto vendido: no elimina casos, no modifica ventas originales ni resuelve automáticamente diferencias de insumos/complementos. No se clasifica todo Sabor por nombre. No se modifica la vigencia ni se presume evidencia física a partir de aprobación comercial.

Consultas reproducibles: inventario_fuentes_datos --term addon --term sabor --term transferencia; verificar RecetaAgrupacionAddon activo=True/status=APPROVED por código exacto y PointRecipeNode de septiembre para esas recetas; contar casos septiembre por product__sku antes/después de aplicar sold_products. El inventario de fuentes sólo produce candidatos léxicos, no prueba identidad.

Riesgos y pendientes: el filtro compartido afecta proyección de productos y recetas de consumo. Conservar bases, reventa y sabores no aprobados, validar reportes y agente sin HTTP ni avisos duplicados, y confirmar pantalla autenticada tras despliegue. Este cambio no acredita cierre documental ni conteo físico del mes.
