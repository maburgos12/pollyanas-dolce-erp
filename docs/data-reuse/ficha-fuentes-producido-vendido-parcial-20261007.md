# Ficha de fuentes — Producido vs Vendido, publicación parcial

Fecha y ambiente consultado: 7 de octubre de 2026; PostgreSQL 16 local aislado y producción ERP en lectura.

## Necesidad y unidad de análisis

Actualizar la proyección guardada por mes, producto Point y sucursal Point sin convertir una frontera faltante en saldo cero ni certificar el mes. Cada fila de `ProductInventoryAuditCase` representa un producto/sucursal/mes; la pantalla agrupa esas filas por producto.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Saldos y movimientos del mes | `BranchInventoryTraceabilityService` sobre cierres Point, ventas, producción, merma, transferencias y conversiones ya guardadas | Importaciones protegidas de Point y lectores de dominio existentes | Fecha operativa, producto Point, sucursal Point y FK de documento | `build(2026-09, allow_partial=True)` conserva incidencias por producto; `build()` falla cerrado ante dos fronteras de producto 206/CEDIS | `InventoryAuditMaterializer`, cierre mensual |
| Auditoría materializada | `ProductInventoryAuditRun`, `ProductInventoryAuditCase` | `InventoryAuditMaterializer.rebuild` | Mes y par producto/sucursal; huella de cálculo | Producción: último resultado completo 2026-10-04 04:17 UTC; intento 2026-10-07 13:59 UTC `SOURCE_INCOMPLETE`, dos incidencias producto 206; 1684 casos existentes | Producido vs Vendido, auditor por producto, agente |
| Historial canónico | `AuditStockHistoryService` e importaciones de historia existentes | Captura protegida Point ya realizada | FK movimiento, producto, sucursal y período | Solo conciliación de fuentes ya guardadas; este cambio no hace HTTP ni nueva captura | Materializador, auditor documental |
| Reporte | `read_audit_report`, `ProducidoVsVendidoMermaView` | Lectura de casos persistidos | Mes, producto y filtro sucursal/categoría | En producción 194 productos, uno sin vendido/producido/merma (`Galleta Oreo`), lo que dejaba todas las tarjetas generales en `Sin dato` | HTML, JSON, CSV, XLSX, PDF |

El inventario léxico `inventario_fuentes_datos --term 'auditoria inventario producto'` y `--term 'producido vendido'` devolvió cero candidatos de modelos; la identidad se confirmó trazando servicios, modelos y registros, no por ese resultado.

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Producto Point, receta ERP | Solo equivalencia confirmada por mapa de receta existente | SKU/nombre aislado no establece identidad | Ninguna equivalencia nueva aplicada |
| Sucursal Point, sucursal ERP | Solo alias canónico existente | Bamoa/Crucero histórico no se fusionan por nombre | Ninguna equivalencia nueva aplicada |
| Dato faltante, cero | Distinta | `CASE_MISSING_FROM_REBUILD` se muestra sin cantidad; no es cero | No procede convertirlo |

## Decisión de diseño

Extender el materializador y el reporte existentes, sin segunda captura ni tabla maestra. La publicación parcial es explícita; solo procede con líneas identificadas e incidencias por par. Las incidencias globales conocidas de cobertura (`SOURCE_INCOMPLETE`) o destino de conversión no homologado (`MISSING_CONVERSION_DESTINATION`) permanecen en el run y no bloquean las filas identificadas; cualquier código global distinto falla cerrado. En producción se observaron dos cierres Point incompletos y las conversiones 189/195 sin destino homologado. El estado mensual sigue `SOURCE_INCOMPLETE`, conserva la última fecha completa y añade una marca separada de publicación parcial. Los totales estrictos del detalle/exportación permanecen estrictos; las tarjetas muestran suma conocida con cobertura visible y etiqueta parcial aun si todas sus filas tienen valor.

Procedimiento reproducible: PostgreSQL 16 local; `python3 manage.py inventario_fuentes_datos --term ...`; lectura de `ProductInventoryAuditRun` y `read_audit_report(2026-09)` en producción sin escritura; regresiones Django en base aislada.

Riesgos y pendientes: faltan las fronteras documentales de producto 206/CEDIS y otros pares; la publicación parcial no habilita `ProductMonthClosureService.lock`, no sustituye conteo físico y no altera fuentes Point.
