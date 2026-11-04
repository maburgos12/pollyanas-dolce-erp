# Ficha de fuentes — cierre mensual automático de producto

Fecha: 2026-11-04. Ambiente de descubrimiento: PostgreSQL local aislado, estructura de `origin/main`; diagnóstico operativo de producción realizado con consultas de solo lectura.

## Necesidad

Cerrar automáticamente cada mes de producto cuando estén completas las fuentes Point y la evidencia física enviada por las sucursales. Los reintentos no deben duplicar movimientos, un faltante no debe convertirse en cero y una reserva ERP no debe contabilizarse como venta.

## Fuentes existentes

| Concepto | Fuente | Identidad y unidad | Escritura | Consumidores | Decisión |
|---|---|---|---|---|---|
| Cierre mensual | `recetas.ProductoMonthClosure` / `ProductoMonthClosureLine` | mes único; receta padre; piezas equivalentes | `ProductMonthClosureService` | cierre de producto, auditoría y reportes | Reutilizar como cierre canónico e inmutable. |
| Inventario Point | `pos_bridge.PointInventorySnapshot` y `PointHistoricalInventoryClosing` | sucursal Point + producto Point + fecha operativa; unidad Point homologada | sincronizadores Point e historial acreditado | balance mensual y auditoría | Reutilizar. No relabelar una captura actual como histórica. |
| Producción | `pos_bridge.PointProductionLine` | `source_hash` único; producto/receta; piezas | sincronización de producción Point | balance mensual | Reutilizar idempotencia existente. |
| Ventas | `pos_bridge.PointDailySale` | fecha + sucursal + producto únicos; piezas | reporte oficial Point | balance mensual | Reutilizar únicamente filas oficiales acreditadas. |
| Merma | `pos_bridge.PointWasteLine` | `source_hash` único; producto/receta; piezas | sincronización de mermas Point | balance mensual | Reutilizar. |
| Conversiones | `pos_bridge.PointConversionLine` | `source_hash` único; origen/destino; piezas | sincronización de conversiones Point | balance mensual | Reutilizar las equivalencias verificadas del catálogo. |
| Conteo físico | `inventario.ConteoSucursal`, `LineaConteoSucursal`, `LecturaConteoSucursal`, `EventoConteoSucursal` | sucursal + fecha operativa + producto + ronda; cantidad y hora declaradas | App Operativa y conciliación nocturna | revisión de conteos | Extender el flujo: aceptación automática sólo con productos calculados y diferencia cero; reconteo sólo de diferencias. Nunca ajustar stock. |
| Reserva pickup | `crm.PickupReservation` | token único + receta + sucursal; piezas | servicio pickup ERP | disponibilidad para promesa | Reutilizar como fuente independiente. No restarla como venta ni duplicarla dentro del cierre. |
| Auditoría visible | `reportes.ProductInventoryAuditRun` / `ProductInventoryAuditCase` | mes único + casos por producto/sucursal | `InventoryAuditMaterializer` | Producido vs Vendido | Refrescar antes de bloquear el cierre y exigir estado `READY`. |

## Evidencia de registros y cobertura

- En producción, octubre 2026 tenía `ProductInventoryAuditRun` en `SOURCE_INCOMPLETE` y no tenía `ProductoMonthClosure` materializado.
- El 31/10/2026 existían conteos enviados de seis de diez sucursales de venta; cuatro sucursales no tenían conteo para esa fecha y había líneas pendientes.
- Las fuentes Point del mes tenían filas, pero el inventario final histórico y varias identidades seguían sin autoridad suficiente para bloquear.
- La tarea `pos_bridge.monthly_product_closure` estaba deshabilitada, con `lock_after_build=false` y sin ejecución registrada.

## Búsqueda reproducible

Se ejecutó `inventario_fuentes_datos` sobre PostgreSQL para `ProductoMonthClosure`, `PointInventorySnapshot`, `PointProductionLine`, `PointDailySale`, `PointWasteLine`, `PointConversionLine`, `ConteoSucursal`, `PickupReservation` y `ProductInventoryAuditRun`. Las coincidencias confirman candidatos estructurales; la equivalencia semántica se validó contra sus servicios, constraints y consumidores.

## Decisión

Extender el orquestador existente. Antes del lock: conciliar conteos, exigir cobertura de las sucursales operativas y diferencias físicas cero en las líneas de producto, materializar la auditoría y comprobar `READY`. Conservar las reservas como disponibilidad independiente. No crear tablas maestras, no modificar Point ni aplicar ajustes automáticos.
