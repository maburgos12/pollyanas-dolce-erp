# Ficha de fuentes — conciliación de producto vendido

Fecha y ambiente consultado: 2026-10-02; PostgreSQL local aislado y VPS producción, consultas acotadas de solo lectura.

## Necesidad y unidad de análisis

Conciliar septiembre primero: una ubicación ERP canónica, producto Point y mes. Apertura del 31/08, movimientos reales de septiembre y cierre del 30/09. Empaques y toppings son consumo interno, no producto vendido; sus movimientos se conservan para la auditoría de insumos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Producción | PointProductionLine / pos_bridge_production_lines | Sincronización Point producción | source_hash único, sucursal, fecha; sync_job mutable | Septiembre: 1571 filas, 1327 del job mensual 78172 y 244 actualizadas por 79083 | Auditor, balance mensual, producción |
| Merma | PointWasteLine / pos_bridge_waste_lines | Sincronización Point mermas | source_hash único, ubicación, instante; sync_job mutable | Septiembre: 267 filas, 234 de 78023 y 33 actualizadas por 79066 | Auditor, balance mensual, mermas |
| Ventas oficiales | PointDailySale / pos_bridge_daily_sales | Backfill oficial Point | Producto, sucursal, día; manifiesto PointExtractionLog | Septiembre: 10454 filas canónicas y cobertura de 270 sucursal-días | Ventas, auditor, balance |
| Materialización de ventas | VentaHistorica, VentaAutoritativaPoint | Materializador de ventas | Receta, sucursal ERP, día | Histórico: 7369 filas; excluye artículos no elegibles para receta, mientras Point registra sus movimientos | Pronósticos, reportes |
| Cierres | PointHistoricalInventoryClosingLine | Captura de cierre Point verificada | Cierre, sucursal Point, producto Point | 31/08: 1520 filas, 10 sucursales; 30/09: 1413 filas, 9 sucursales; Bamoa ausente del segundo | Auditor, cierres |
| Consumo interno | PointProduct, PointProductCategory, Receta | Catálogo Point existente | SKU; categoría TOPPING; nombre inequívoco empaque/topping | 17 recetas con dichos conceptos; faltantes de espejo restantes: TE DEL JARDIN (27), CAJA G PARA VENTA (2) | Matching, auditor de producto e insumos |
| Traslados y conversiones | PointTransferLine, PointConversionLine y Logística | Point y Logística existentes | source_hash, folio, ubicación de origen/destino | Evidencia existente conservada; 17 casos con origen de conversión pendiente | Auditor, rutas |
| Investigación persistida | ProductInventoryAuditCase/Event/Run | Materializador y agente auditor | Mes, sucursal canónica, producto; eventos humanos | Septiembre: 1959 casos; 1254 conciliados, 225 por explicar, 480 con fuentes incompletas antes del cambio | Auditor, Producido vs Vendido |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Sucursales Point duplicadas | Confirmada cuando ya existe erp_branch compartida | Reutilizar canonical_point_branch_identity; no crear equivalencias nuevas | Ambigüedad permanece pendiente |
| Empaque / empaques, topping / toppings | Distinta de producto vendido | Autorización explícita de Mauricio; categoría TOPPING existente | No extender exclusión a todo accesorio o reventa |
| Job mensual y actualización incremental | Misma fuente, distinta versión | Sync modifica sync_job sin duplicar source_hash; validar fechas, counters y antigüedad del registro | No aceptar fallos, filtros o filas nuevas del mes como simple actualización |
| Histórico de recetas y venta completa Point | Distinta cobertura | is_non_recipe_sale_row determina qué se materializa como receta | No borrar bebidas/accesorios del inventario vendido por ausencia del espejo |

## Decisión de diseño

Reutilizar y corregir validaciones compartidas. No crear tablas, capturas ni descargar otra vez Point. Separar consumo interno del ámbito de producto vendido sin eliminar casos ni eventos históricos. Mantener intactos ajustes operativos, aprobaciones y cierres protegidos. Las diferencias y falta de cierres siguen pendientes cuando no exista evidencia suficiente.

Consultas reproducibles: `inventario_fuentes_datos --term produccion --term merma --term topping --term empaque` con PostgreSQL configurado; ORM acotado a septiembre y jobs 78172/79083/78023/79066; comparación por receta/sucursal/día de fuentes canónicas; conteos de cierres 6 y 7.

Riesgos y pendientes: cierres de Bamoa, Almacén y Devoluciones sin cobertura completa; snapshots cercanos no prueban por sí mismos el último saldo del mes. Los 710 historiales Point guardados se consultaron antes del cierre de septiembre: se conservan, pero no acreditan su mes completo. La cobertura exige `fetched_at` con zona horaria posterior al límite del mes en Mazatlán, además de acreditar su comienzo. Septiembre es piloto antes de reprocesar otros meses. No sustituir evidencia de conversión, merma o traslado por una diferencia aritmética cero.

Investigación dirigida posterior: no existían historiales de producto 183 (0109) para Colosio, Payán y Nío. Se consultaron una vez esos tres ámbitos y se conservaron en la fuente existente (imports 711–713, 500 movimientos cada uno, comienzo anterior a septiembre y consulta del 02/10). En Nío el movimiento Point 1676768 `CANCELACION VENTA` devuelve una pieza: existencia 1→2, cantidad 1. No es otra salida por venta ni merma; debe reducir la venta neta solo cuando el aumento de existencia acredita la cantidad. Cancelaciones sin efecto probado o de otro tipo permanecen desconocidas. Los movimientos originales y las ventas oficiales no se modifican.

Cobertura Bamoa autorizada el 02/10: identidad confirmada Point `2` (PointBranch 5) → ERP 17, código `BAMOA`, activa y apertura 14/07/2026. Crucero ERP 4 está inactiva y es distinta. El cierre 6 del 31/08 contiene 152 productos de Bamoa; no existe cierre de Bamoa del 30/09. La omisión procede de `POINT_NETWORK_BRANCH_CODES`: se agrega `BAMOA` sin alterar códigos, nombres ni alias de Crucero. Consumidores: cierre diario, correo/exportación y manifiesto de captura histórica. La ampliación histórica reutiliza las líneas verificadas del mismo día y manifiesto de productos, consulta solo sucursales faltantes y conserva el cierre previo si no completa la ampliación o el mes está bloqueado. No genera otra copia de las líneas existentes ni cambia sus existencias; registra el fingerprint anterior como evidencia.

El historial requerido por el cierre reutiliza `AuditStockHistoryService` y sus tablas `PointProductHistoryImport`/`PointProductHistoryRow`: una consulta conserva el payload completo para cierre y auditoría. El cache debe acreditar el mes completo; un historial vacío contradictorio con existencia actual no cero se vuelve a verificar, nunca se convierte automáticamente en cierre cero.

La cobertura y frontera usan el manifiesto de movimientos de la última consulta (`fetched_movement_ids`), no la unión con filas antiguas retenidas. Sin manifiesto, solo se reutiliza una captura cuyo número de filas coincida con las recibidas; si difieren debe verificarse de nuevo. Esto evita certificar un cierre con filas antiguas cuando la última respuesta truncada no alcanza la fecha.

Investigación de evidencias: reutilizar las relaciones explícitas `source_trace.transfers` → `PointTransferLine` → `RutaCargaChecklistLinea.point_transfer_line`, incluyendo folio, origen/destino, recepción y finalización. El saldo aritmético y la trazabilidad documental se muestran por separado; una transferencia pendiente no demuestra piezas faltantes. Preservar los mensajes de `TraceSourceIssue` dentro de la proyección existente y señalar apertura/cierre faltantes por fecha, producto y ubicación. Consultar los historiales guardados en lote también para expedientes con diferencia cero y trazabilidad pendiente; cobertura parcial nunca acredita el mes completo. No se crean tablas, equivalencias ni descargas adicionales. La firma de actualización incluye la versión de estas reglas para reproyectar automáticamente los meses abiertos existentes.
