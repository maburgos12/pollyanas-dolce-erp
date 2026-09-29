# Ficha de fuentes — conciliación conservadora de conversiones de inventario

Fecha y ambiente consultado: 29/09/2026; metadatos PostgreSQL local aislado y evidencia productiva de agosto 2026 consultada en modo solo lectura.

## Necesidad y unidad de análisis

La auditoría mensual necesita conciliar, por producto y sucursal ERP, el inventario inicial, producción, ventas, mermas, transferencias, conversiones y cierre Point. Una conversión confirma la entrada del producto destino; la salida del producto origen sólo se considera confirmada cuando Point informa un código o nombre de origen inequívoco.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Conversiones Point | `pos_bridge.PointConversionLine` / `pos_bridge_conversion_lines` | Sincronización canónica del reporte de conversiones Point | `source_hash`; movimiento, fecha y sucursal | Agosto 2026 contiene 414 entradas de 3 Pecados Rebanada sin campos de producto origen | Producido vs Vendido, cierre de producto, auditoría por sucursal |
| Relación de presentaciones | `recetas.RecetaEquivalencia` y `recetas.RecetaPresentacionDerivada` | Catálogo de recetas ERP | receta derivada, receta padre y factor vigente | Existe relación Rebanada → Mediano con factor 10; no prueba el origen de cada movimiento Point | Balance mensual y trazabilidad |
| Transferencias | `pos_bridge.PointTransferLine` y snapshots abiertos | Sincronización Point/Logística | `source_hash`, origen, destino, cantidades enviada y recibida | Agosto conserva transferencias; existen diferencias enviada/recibida que deben mostrarse sin crear otra descarga | Logística y auditoría por sucursal |
| Mermas | `pos_bridge.PointWasteLine`; fuentes ERP de control y mermas | Point y flujos operativos existentes | movimiento, producto, sucursal y fecha | 3 Pecados Rebanada tiene 29 unidades Point; Chico y Mediano no tienen merma directa registrada | Auditoría y reportes de merma |
| Cierres Point | `PointHistoricalInventoryClosing` y líneas | Snapshot histórico verificado | fecha operativa, sucursal Point y producto | Cierres 31/07/2026 y 31/08/2026 disponibles | Balance y auditoría mensual |
| Identidad de sucursal | `pos_bridge.PointBranch.erp_branch` → `core.Sucursal` | Homologación Point/ERP existente | `erp_branch_id` | Hay alias numéricos y nominales enlazados a la misma sucursal ERP | Ventas, movimientos, cierres y auditoría |

## Alias y equivalencias

| Términos o identificadores | Estado: confirmada / candidata / distinta / no resuelta | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Dos `PointBranch` con el mismo `erp_branch_id` | confirmada | La FK explícita identifica la misma sucursal ERP; se conserva una sucursal Point representativa para persistencia | Aprobada por Mauricio el 29/09/2026 |
| 414 rebanadas → 41.4 medianos | no resuelta | Point confirma el destino y cantidad, pero deja vacíos `source_item_code` y `source_item_name`; dividir entre 10 es una inferencia, no evidencia | No asignar a Chico ni Mediano hasta que Point informe el origen |
| Diferencia de inventario → merma | distinta | Una diferencia puede provenir de transferencia, conversión, venta o cierre; sólo `PointWasteLine`/fuentes de merma confirman merma | Mantener como pendiente de conciliación |

## Decisión de diseño

Reutilizar las filas ya persistidas de Point y Logística. La entrada de conversión se aplica al producto destino desde la propia fila Point, aun cuando no exista una relación derivada; la relación y el factor sólo habilitan la salida del origen informado inequívocamente por Point. Las sucursales Point enlazadas a la misma `core.Sucursal` se agregan antes del cálculo y usan el mismo selector canónico de vigencia que los indicadores de ventas. Los casos históricos se trasladan a esa identidad cuando no existe conflicto y el detalle reúne los eventos de todos los alias, sin borrar evidencia. No se crea tabla, importador, descarga ni captura nueva.

Consultas o procedimiento reproducible: `python3 manage.py inventario_fuentes_datos --term conversion`, `--term transferencia` y `--term merma`; consultas productivas acotadas al periodo 2026-08 y a los códigos 0108, 0109 y 0110.

Riesgos y pendientes: Point no conserva el origen en las filas observadas de agosto. La auditoría debe mostrar “Origen de conversión por identificar” y no cerrar el faltante como merma ni atribuirlo automáticamente a una presentación.
