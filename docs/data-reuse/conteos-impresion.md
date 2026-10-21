# Ficha de fuentes — impresión de conteos por sucursal

Fecha y ambiente: 2026-10-21, PostgreSQL 16 local aislado; datos sintéticos.

## Necesidad y unidad de análisis

Imprimir las comparativas nocturnas ya visibles en el detalle. Una fila representa un artículo de un conteo por sucursal. No se capturan ni recalculan cantidades.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Conteo y resultado al cierre | inventario.ConteoSucursal / inventario_conteosucursal | Servicios de captura y conciliación nocturna | PK, sucursal, fecha, huella de lecturas | Fixture NightlyCountTests: un conteo enviado, un artículo y resultado calculado | Detalle, Excel, impresión |
| Cantidad y unidad | LineaConteoSucursal y LecturaConteoSucursal | Captura existente y catálogo Point | Línea, ronda | Fixture: cantidad 3; movimientos comprobados producen físico 4, Point 4, diferencia 0 | Detalle, Excel, impresión |

## Alias y equivalencias

Comparativa manual y conciliación al cierre son conceptos distintos. El impreso usa exclusivamente `rows[].conciliacion` y `conciliacion` del contexto existente, igual que la sección nocturna del detalle. No se introducen equivalencias ni tablas.

## Decisión de diseño

Reutilizar `inventario.views_conteos._context`. Conserva la validación de huella, el alcance por sucursal y la exclusión de comparativas durante captura ciega. La opción de impresión exige revisión y contexto no editable. Una conciliación ausente o desactualizada imprime pendientes, nunca ceros inventados. Los permisos no se amplían.

Procedimiento reproducible: `manage.py inventario_fuentes_datos --term conteo --term conciliacion`; `manage.py test inventario.tests_conteos_nocturna`. La búsqueda léxica incluye conciliación bancaria, descartada por dominio. Los tests verifican igualdad con el resultado existente y ausencia de mutaciones al consultar la impresión.

Riesgos: paginación y anchos dependen del navegador y tamaño del papel; validar PDF de varias páginas. No requiere migraciones ni cambios a jobs o integración Point.
