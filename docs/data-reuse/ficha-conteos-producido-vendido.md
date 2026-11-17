# Ficha de fuentes — Lecturas físicas en Producido vs Vendido

Fecha y ambiente: 17/11/2026; ERP de producción `1640a365` (solo lectura) y PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Mostrar en «Ver detalle» evidencia por folio, sucursal ERP, producto de catálogo y día de corte. No sustituye el saldo mensual de Point ni cambia un estado de auditoría.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Folio y lectura física | ConteoSucursal, LineaConteoSucursal, LecturaConteoSucursal, EventoConteoSucursal | App operativa y revisión; importador histórico #1657 | FK sucursal/producto, fecha, folio, línea y ronda | 30/09 sin folios; 31/10 diez folios, 378 lecturas de ronda 1, cero aceptados | Revisión de conteos y nuevo detalle de solo lectura |
| Saldo y cálculo Point | ProductInventoryAuditCase | Conciliación documental | Caso, mes, sucursal Point y producto | Pantalla actual muestra únicamente saldos y diferencia Point | Reporte y exportaciones, sin cambios |
| Excel original de ventas | Libro GENERAL, SHA-256 68ea680f… | Captura mensual externa | Archivo, pestaña, fecha declarada y celda | 387 y 586 lecturas directas, más fórmulas agregadas excluidas | Cotejo externo; no fuente instalada en el ERP |

## Alias y equivalencias

- Confirmada: FK de producto de la línea de conteo = producto del caso; FK de la sucursal del conteo = sucursal ERP mapeada en el caso. No enlazar por nombre visible.
- Distinta: el Excel GENERAL CORREGIDO (SHA 22693029…) originó #224–227 y conserva procedencia en eventos `importacion_historica`. El libro GENERAL no debe tratarse como la misma versión ni volver a cargarse como otro folio.
- Candidata: la comparación provisional de Operación usa nombre normalizado. En octubre hay 100 candidatos, 93 con cantidad ERP, 54 iguales y 39 distintos; en septiembre hay 70 candidatos. No acredita identidad, unidad ni equivalencia temporal.

## Decisión de diseño

Reutilizar folios, evidencia enviada y procedencia histórica existentes, con los permisos de revisión de conteos. Mostrar cantidades y hora de observación, estado actual y ronda enviada, sin sustituir un reconteo vacío por cero. El día de folio no equivale a validación del saldo a medianoche. Sin cambios de esquema, Point, saldos o exportaciones para este bloque.

El histórico externo restante necesita una fuente independiente, ya que #1657 rechaza una fecha con folio existente y crea folios para el flujo operativo. Los adjuntos de conteo requieren folio y los eventos de cierre/aprobación modifican el significado del expediente; no usarlos como almacén de datos. Si se aprueba persistencia independiente, el mínimo es una evidencia inmutable por SHA, pestaña y celda, con cantidad original, fecha declarada y solo relaciones producto/sucursal/unidad verificadas. Las filas no vinculadas o en conflicto se conservan en cotejo, nunca pasan a saldo.
