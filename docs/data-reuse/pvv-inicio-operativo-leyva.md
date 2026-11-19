# Ficha de fuentes — inicio operativo observado de Leyva

Fecha y ambiente: 18/11/2026, producción consultada en solo lectura; pruebas PG16 sintéticas aisladas.

## Necesidad y unidad de análisis
Mostrar la cantidad observada por producto y sucursal al inicio operativo autorizado del 03/10. La observación de las 10:48 no acredita el saldo al 30/09 ni el cierre mensual.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Caso mensual | ProductInventoryAuditCase | Conciliación mensual existente | Caso + producto + sucursal + mes | Octubre, apertura desconocida, sucursal Point28/ERP18 | Producido vs Vendido |
| Stock observado | PointInventorySnapshot / pos_bridge_inventory_snapshots | Extractor Point de inventario | Job81086 + captura03/10/2026 17:48:28.494613UTC + productoFK + sucursal28 externa14 | 44 productos del reporte; raw original y stock se cotejan | Detalle HTML suplementario |
| Procedencia de sucursal | PointExtractionLog / pos_bridge_extraction_logs | Mismo extractor | Log302317, job81086, branch_id28, branch_external_id14 | «Sucursal procesada 14.» | Verificación de la captura |

## Alias y equivalencias

| Identificadores | Estado | Evidencia | Revisión requerida |
| --- | --- | --- | --- |
| Point28 / externa14 / ERP18 | Identidad ya existente | FK actual y selección archivada del job | No se crea equivalencia |
| Captura03/10 / saldo30/09 | Distintos | Fecha y hora originales; autorización operativa del usuario | Saldo mensual sigue pendiente |

## Decisión de diseño
Reutilizar el snapshot existente exclusivamente como evidencia visible después de resolver la salida HTML. Cantidad exacta y unidad original, fecha/hora Mazatlán y referencias snapshot/job/log. No recalcular ni escribir casos, no modificar cantidades mensuales, confianza, API, JSON ni exportaciones. No promover el stock observado a apertura a medianoche. La venta disponible del03/10 tiene granularidad diaria y todavía impide conciliar un intervalo operativo completo sin evidencia horaria.

Procedimiento: `inventario_fuentes_datos --term saldo --term inventario --term snapshot` con PG16 configurado; fuentes léxicas no equivalen a identidad. Consulta acotada de producción por job81086/sucursal28 y log302317; no llamadas nuevas a Point para renderizar.

Pendientes: primera nota y conversiones/ajustes previos a la captura; 30/09 sigue sin soporte. La vista conserva todo pendiente mensual que corresponda.
