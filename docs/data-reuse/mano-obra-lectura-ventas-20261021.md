# Ficha de fuentes — lectura de ventas del análisis de mano de obra

Fecha y ambiente consultado: 2026-10-21; PostgreSQL 16 local aislado y producción en solo lectura.

## Necesidad y unidad de análisis

Comparar el costo de un cierre mensual aprobado con el precio promedio vendido por receta.
Una fila del resultado agrega cantidad e importe total por `receta_id`, entre el primer y último día del mes, ambos incluidos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta diaria Point | `PointDailySale` / `pos_bridge_daily_sales` | Integración y materialización Point existentes | Fecha, sucursal y producto; FK `receta_id` | Septiembre: 10,454 filas, 77 recetas vinculadas, consulta de solo lectura | Servicio central de ventas y análisis financiero |
| Cierre de mano de obra | `CierreManoObraMensual` / `reportes_cierremanoobramensual` | Aprobación explícita del módulo | Periodo y revisión | 0 cierres en producción al consultar | `analisis_aprobado`, panel financiero |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Receta de venta y producto del cierre | Confirmada por identificador existente | `PointDailySale.receta_id` y `productos[].id`; ventas sin receta quedan fuera como antes | Ninguna nueva equivalencia; no se modifica el vínculo |
| Importe de comparación | Distinto de importe bruto | Se conserva `total_amount`, no `gross_amount`; pruebas usan valores diferentes | No cambia la definición vigente |

## Decisión de diseño

Extender `ventas/services/sales_read_service.py` con `get_point_sales_recipe_totals` y reutilizarlo desde `analisis_aprobado`. Se traslada exactamente la consulta vigente: mismas fechas, recetas no nulas, agrupación y sumas. No se agregan filtros de sucursal, fuente o cantidad positiva; las devoluciones siguen participando. La decisión de mostrar precio y margen solo con cantidad positiva permanece en el consumidor.

No hay modelos, migraciones, capturas, recalculados ni modificaciones de cierres. No se amplía la allowlist ni se desactiva el guard.

Consultas reproducibles:

- `python manage.py inventario_fuentes_datos --term ventas --term PointDailySale` en PostgreSQL local; candidatos léxicos, no evidencia de equivalencia semántica.
- Producción, dentro de `BEGIN READ ONLY`: `SELECT count(*), count(DISTINCT receta_id) FROM pos_bridge_daily_sales WHERE sale_date BETWEEN '2026-09-01' AND '2026-09-30';`
- Conteo y huella de cierres antes/después para demostrar que no se alteran.

Validación: prueba de frontera arquitectónica reproduce las dos violaciones originales; pruebas PostgreSQL verifican ambos extremos del mes, exclusión de fechas externas y receta nula, agregación entre sucursales, devolución fraccionaria, uso de total y no bruto, ausencia de precio con cantidad cero/negativa y conservación del cierre.

Límite: esta corrección mantiene la semántica existente; no audita ni cambia la cobertura de la materialización de ventas. Al no haber cierres reales, la comparación de márgenes con cierre aprobado se comprueba con datos sintéticos, sin crear cierres operativos.
