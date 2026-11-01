# Ficha de fuentes — Rentabilidad y reparto histórico

Fecha: 2026-11-01. Consultas productivas de solo lectura y pruebas PostgreSQL 16 aisladas.

## Necesidad y unidad de análisis
Sucursal/mes de enero–agosto 2026; reutilizar el reparto autorizado y mantener distinta la contribución por productos vendidos.

## Fuentes candidatas
| Concepto | Modelo / tabla | Fuente | Identificador / ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta por producto | PointDailySale / pos_bridge_pointdailysale | Point | fecha/producto/PointBranch | 76,161 filas ene–ago, bruto 28,186,782.95 | Rentabilidad, Estado de resultados |
| Fondos reales | LineaPresupuestoMensual | consolidación ERP | período/rubro/versión ORIGINAL | mismo conjunto conciliado del PR 1599 | reparto y P&L |
| Sucursal | core.Sucursal y PointBranch | ERP y Point | código/external_id/vigencia | externo 2 reutilizado; Crucero hasta junio, Bamoa desde 14/07 | lectura mensual y reparto |
| Venta fiscal | FactVentaDiaria, selected_sales_facts | integración existente | sucursal/día y prioridad fuente | no sustituye costos ni fondos del P&L | conciliación del dashboard |

## Alias y equivalencias
Point externo 2 enero–junio 2026 = Crucero, confirmado por Mauricio en este hilo. Bamoa comienza 14/07; el reparto rechaza ventas en el intervalo anterior. No se cambia el catálogo ni registros históricos. Guamúchil conserva etapas ya aprobadas.

## Decisión
Reutilizar el servicio de reparto, su tabla y CSV. Extraer sin cambiar el algoritmo la lectura de fondos del P&L. Compartir la identidad histórica de Point en la lectura mensual que consumen detalle, exportaciones y tarea; no ejecutar recálculos ni consultas IA. Reagrupar las identidades fiscales al conciliar, sin escribir en FactVentaDiaria. Incluir sucursales inactivas en el filtro histórico del reporte mensual.

## Verificación
`inventario_fuentes_datos --term rentabilidad --term sucursal --term ventas` en PostgreSQL aislado. Comparación de ambas superficies y CSV; pruebas de identidad junio/julio, permisos, discrepancia y ausencia de escrituras. Producción: comparar los 77 registros/539 importes contra el reporte conciliado, además de conteos y sumas fuente antes/después.

## Límites
El reparto con impuestos de la fuente P&L y la contribución fiscal por productos vendidos son bases distintas: se presentan por separado. Septiembre en adelante no forma parte del reparto autorizado. No se reclasifican inversiones ni se infieren costos evitables por cierre.
