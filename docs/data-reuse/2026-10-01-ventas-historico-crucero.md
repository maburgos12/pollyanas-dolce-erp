# Ficha de fuentes — ventas históricas de sucursales cerradas

Fecha y ambiente: 1-oct-2026; PostgreSQL 16 de producción leído sin modificaciones, entorno local aislado `erp_ventas_historico_crucero`, puerto 55672.

## Necesidad y unidad de análisis

Comparar ventas de la red al mismo periodo entre años, conservando sucursales que operaron en cada periodo aunque estén cerradas hoy. Una venta es sucursal/fecha/producto/fuente; el estado operativo actual del catálogo no redefine esa transacción histórica.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Sucursal histórica | core.Sucursal / core_sucursal | Catálogo y separación Crucero–Bamoa | PK 4 Crucero, PK 17 Bamoa | Crucero inactiva; Bamoa activa | Lectores de ventas, filtros operativos |
| Venta histórica Point | ventas.VentaAutoritativaPoint / ventas_autoritativas_point | Importación category_report y sync VPS | sucursal, fecha, código | 2025: 97,312 filas, $44,418,445.59; ene–sep $32,901,734.21 | Lector canónico y materialización analítica |
| Venta diaria oficial | pos_bridge.PointDailySale / pos_bridge_daily_sales | Reporte Point idreporte=3 | sucursal Point, fecha, producto | Ene–sep 2026 $31,282,401.00 | Totales canónicos, cierre, finanzas |
| Copia analítica | reportes.FactVentaDiaria / reportes_factventadiaria | analytics_service | sucursal, fecha, producto, source_kind | Los seis totales 2025 de la captura excluyen exactamente Crucero | Dashboard, BI, forecast |
| Total mensual oficial | pos_bridge.PointMonthlySalesOfficial | backfill mensual Point | mes único | Abr-2026 $3,526,386.86; detalle diario $3,526,386.76 | Comparativo mensual |
| Tickets diarios | pos_bridge.PointDailyBranchIndicator | Sincronización Point | fecha y sucursal Point | Persistencia existente con FK a sucursal ERP | Ticket promedio y forecast |

## Alias y equivalencias

| Identificadores | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Crucero ERP 4 y Bamoa ERP 17 | Distintas | Tienda cerrada y tienda nueva; el histórico no se traslada | Confirmado por Mauricio: preservar Crucero como histórico |
| Bollo Chocolate 0116 y 116, Matriz 10-mar-2026 | Candidato duplicado | Misma cantidad/importe y archivo; dos procedencias | No se fusionan ni borran; se usa el lector canónico existente |
| Dos representaciones de marzo 2026 | Superposición confirmada del total | category_report y PointSalesDailyProductFact suman cada una $3,326,094.19 | El comparativo consulta la fuente canónica; depuración física separada |

## Decisión de diseño

Reutilizar `canonical_point_sales_range_total` para los rangos de ambos años históricos, incluyendo cortes parciales y segundo año previo. Mantener el fallback analítico existente para periodos sin persistencia Point. Quitar el filtro de actividad actual de los mapas históricos de ventas y tickets. Invalidar el caché del comparativo mediante `CLOSED_SALES_VERSION`.

No crear tablas, importar ventas, reactivar Crucero, reasignar registros a Bamoa ni borrar duplicados. El arreglo evita sumar la copia analítica duplicada de marzo en el comparativo cuando existe evidencia canónica; no depura físicamente la tabla autoritativa ni cambia lectores ajenos a este flujo.

## Procedimiento y validación

- `manage.py inventario_fuentes_datos --term ventas --term histórico` sobre PostgreSQL local: candidatos léxicos, no identidad semántica.
- Lectura de producción con `BEGIN READ ONLY`: agrupaciones por mes, source_kind, source_sheet y actividad de sucursal. Marzo 2026: $6,652,188.38 autoritativo frente a $3,326,094.19 en detalle Point; el comparativo debe usar este último.
- `git blame`: filtro de actividad en `6d6126683` (16-abr-2026); separación de catálogo en `766d3142` (30-sep-2026).
- Pruebas de regresión: histórico inactivo, ambos años anteriores, corte parcial/completo y copia analítica duplicada. Conteos y huellas antes/después acreditan conservación de ventas de Crucero.

Riesgos: los códigos de las cargas duplicadas aún necesitan conciliación antes de una depuración física. No se aplican equivalencias nuevas. Los selectores de operación activa permanecen fuera del alcance; no deben habilitar capturas nuevas para Crucero.
