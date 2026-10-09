# Ficha de fuentes — cobertura Point en Producido vs Vendido

Fecha y ambiente consultado: 2026-10-07; esquema PostgreSQL 16 local aislado y lectura acotada de producción.

## Necesidad y unidad de análisis

Mostrar los saldos inicial y final de Point ya acreditados sin ocultarlos por un faltante en otra sucursal. La unidad original es mes × sucursal Point × producto Point. La fila del reporte agrupa esas unidades por producto; un saldo parcial no es el total del producto.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Saldo y trazabilidad acreditados | `reportes.ProductInventoryAuditCase` / `reportes_productinventoryauditcase` | `InventoryAuditMaterializer` desde las fuentes Point locales verificadas | `(month, branch_id, product_id)` | Septiembre: 1,684 casos; apertura documental en 1,209 y cierre en 1,259. Entre 50 casos con producción positiva, 47 tienen apertura. | Auditoría de producto y sucursal; Producido vs Vendido |
| Estado de publicación | `reportes.ProductInventoryAuditRun` | Reconstrucción oficial y refresco automático | `month` | El 2026-10-07 a las 21:41 UTC el refresco dejó `partial_published=False` pese a casos actualizados y anteriormente publicados; el último éxito mensual seguía en 2026-10-04. | Reporte, auditoría, actualización automática |
| Snapshot original Point | `pos_bridge.PointInventorySnapshot` / `pos_bridge_inventory_snapshots` | Jobs Point protegidos | `(branch_id, product_id, sync_job_id, captured_at)` más `raw_payload` | Candidato fuente original; el reporte no lo lee ni lo captura directamente. | Lector documental existente |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Sucursal Point y sucursal ERP | Confirmada únicamente por el mapeo canónico existente | `read_audit_report` deduplica por identidad canónica; no equipara por nombre | No crear equivalencias nuevas |
| Producto Point y receta | Confirmada solo por código/alias curado unívoco | Coincidencias ambiguas quedan sin receta; no se usa nombre como prueba | Revisar catálogo aparte |
| Saldo faltante y cero | Distintos | `SOURCE_INCOMPLETE` con traza de apertura/cierre ausente devuelve `None`; un cero documentado conserva `Decimal(0)` | No sustituir faltantes por cero |

## Decisión de diseño

Reutilizar los casos persistidos: exponer la suma comprobada junto con `sucursales con saldo / sucursales del producto` solo como cifra parcial; mantener `None` en el total contractual y en la diferencia que no pueda calcularse. Usar el contrato de reconstrucción parcial ya existente únicamente para meses que previamente la publicaron; no abrir publicaciones automáticas en otros meses. No hay nueva tabla, captura Point, equivalencia ni cambio del servicio de cierre.

Consultas reproducibles: `inventario_fuentes_datos --term ProductInventoryAuditCase --term PointInventorySnapshot` sobre PostgreSQL 16 y agregados de solo lectura filtrados por `month=2026-09-01`; no se consultó Point por HTTP.

Riesgos y pendientes: 475 aperturas y 425 cierres carecen de prueba en el universo vendido mostrado; la cifra parcial no puede usarse como saldo total ni como conciliación cerrada. Validar que HTML y exportaciones etiqueten cobertura, que el refresco preserve la publicación parcial previa y que la vista autenticada sirva el cambio tras despliegue.
