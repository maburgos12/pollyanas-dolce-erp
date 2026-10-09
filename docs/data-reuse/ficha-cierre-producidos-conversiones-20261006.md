# Ficha de fuentes — cierre de productos producidos, septiembre 2026

Fecha y ambientes: 6 octubre 2026; código `main`/worktree aislado PostgreSQL 16 y expedientes originales de producción conservados. `inventario_fuentes_datos --term conversion` y `--term venta` localizados; sus resultados son candidatos léxicos, no identidad.

## Necesidad y unidad de análisis

Separar en el balance mensual de productos producidos: (a) artículo comercial comprado o cargo adicional, sin quitarlo de ventas/Point/inventario; (b) entrada y salida independientes de conversión según el producto y sucursal del movimiento original. Una fila agregada `AGG` no es una ejecución ni lleva pareja transaccional.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente creadora | Identidad y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta comercial | `PointDailySale`, `PointProduct` | reporte oficial Point 3, `OfficialSalesBackfillService` | fecha, sucursal, SKU, nombre completo y categoría originales en `raw_payload`; FK producto ERP derivada | 7 Cake Topper/7 PZA/630 MXN, categoría original `Cake Topper`, variantes `010204` y `010207` | balance, ventas y reportes comerciales |
| Cargo adicional | `PointConversionLine`, `PointRecipeNode` | job 77629 `/Report/crea_Reporte_Largo` y catálogo original | fila 189/195, sucursal, código `0227`, nombre `Extra 10`, nodo PRODUCT Point 267 | 4 y 12 PZA; SKU también usado por productos distintos | balance, trazabilidad |
| Destino conversión | `PointConversionLine` | job 77629 | sucursal, producto, mes; no fecha de ejecución ni FK origen | 23 renglones de producto cotejados 23/23 contra entradas Stock tipo 21 | balance y trazabilidad |
| Movimiento de salida | `PointProductHistoryImport/Row` | Stock/GetHistorial original bajo `AuditStockHistoryService` | sucursal Point, producto Point, FK_Movimiento, instante Stock UTC, tipo 22 | lectura VPS read-only 6oct: 168 salidas/367 PZA, 32 pares; 29 historias COMPLETE, 3 INCOMPLETE; secuencia Zanahoria 1677659–1677689 | inventario histórico, balance y expediente |
| Configuración | `RecetaEquivalencia`, `RecetaPresentacionDerivada`, `ProductBusinessRule` | configuración ERP | receta/nombre, factor, estado | no demuestra ejecución ni relación 1:1 entre entrada/salida | recetas, clasificación |

## Alias y equivalencias

| Términos | Estado | Evidencia y conflicto | Revisión |
| --- | --- | --- | --- |
| `0227` = Extra10 | distinta por sí sola | también Extra Fresa Chico y Love You Rojo; exigir nombre y nodo Point 267 | ninguna equivalencia nueva |
| `010204`/`010207` = una variante CakeTopper | no resuelta por SKU | PLATA/NEGRO o ROSA/PLATA comparten código; raw nombre+categoría delimitan etiqueta comercial, no PK ticket | no reescribir FK |
| `AGG` destino ↔ salida Stock | no hay FK transaccional | movimientos tipo 21/22 independientes, factores varían y puede haber reverso | no inventar pareja |
| Sucursal `AGG` ↔ sucursal Stock | identidad ERP compartida, no PK Point | `CEDIS` PK20 ↔ Point `8` PK3 = ERP10; `Las Glorias` PK21 ↔ Point `3` PK8 = ERP7; `Matriz` PK24 ↔ Point `1` PK10 = ERP1; `Guamuchil` PK25 ↔ Point `13` PK23 = ERP6. Comprobar además nombre normalizado y FK ERP de cada fila. | no enlazar movimientos individuales |

## Decisión de diseño

Extender sólo el lector del balance producido, reutilizando fuentes actuales. Conservar ventas, cantidades, raw, importaciones, costo y categorías de otros consumidores. La exclusión comercial requiere original y corroboración exactos; no se declara ausencia de inventario físico. Salidas se registran desde movimientos originales verificados sin dividir el agregado por factor ni crear relación artificial. El agregado y Stock usan filas `PointBranch` diferentes para la misma sucursal: comparar el FK ERP corroborado y el nombre, no sus PK Point. Si los FK/nombres discrepan, conservar origen sin resolver. Fuente parcial/contradictoria falla cerrada.

Procedimiento: `inventario_fuentes_datos` en PostgreSQL16 aislado; revisar expedientes `decision-caketopper-regla-curada-20261004.md`, `auditoria-conversiones-fk-exacto-20261004.md` y `conciliacion-conversiones-origen-destino-20261006.md`. No ejecutar HTTP Point ni reimportar.

Riesgos: la fuente agregada no identifica ejecución; Stock incompleto no prueba todas las salidas. La coincidencia global 367 PZA con reporte Point salida por conversión 1089 es corroboración de suma, no prueba de cada pareja; no se convierte en factor ni documento ejecutado. El caso Point CEDIS producto 0065/salto 23→22 queda independiente y asignado a Mauricio/Point. Cierre oficial sólo con controles reales.
