# Ficha de fuentes — pantalla Cierre producto, septiembre 2026

Fecha y ambiente: 2026-10-06, ERP producción (lecturas acotadas y vista autenticada) y PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Mostrar el cierre documental Point vigente por receta padre sin convertir una frontera faltante de otro producto en «Sin dato» para todos. Una línea de pantalla representa una receta padre proyectada; el permiso de bloquear el mes es global y separado.

## Fuentes candidatas

| Concepto | Modelo / tabla | Creador y actualización | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Cierre persistido | `recetas.ProductoMonthClosure` y `ProductoMonthClosureLine` | `ProductMonthClosureService.build`; `lock` separado | mes único; mes + receta padre | septiembre: cierre ID 12, construido el 1-oct, 95 líneas; la pantalla servía metadatos anteriores a las correcciones | vista, CSV/XLSX, API interna |
| Fronteras documentales | `PointHistoricalInventoryClosing` y líneas | capturas originales Point verificadas | fecha operativa + sucursal + producto Point | apertura y cierre presentes; una línea CEDIS SKU 0065 no probada en cada extremo; la autoridad mensual es falsa | `MonthlyPointProductBalanceService`, cierre, auditor |
| Balance vigente | `MonthlyPointProductBalanceService.build` | lectura determinista de originales guardados | mes + receta exacta | preview read-only actual: 95 proyectadas, 155 recetas raw; sólo cinco recetas raw sin apertura, pero la línea proyectada replicaba ese issue a las 95 | `ProductMonthClosureService.preview` |
| Proyección visible | `project_product_closure_line` | lectura de líneas del cierre persistido | cierre + receta padre | `opening_source_authoritative=False` mensual hacía nulo el saldo inicial de todas las líneas | vista, exportaciones, API |

`inventario_fuentes_datos --term cierre` confirmó los modelos y unicidades en PostgreSQL local; el inventario es léxico y no valida equivalencias. Las lecturas de producción no llamaron Point ni cambiaron fuentes.

## Alias y equivalencias

| Identidad | Estado | Evidencia / límite | Revisión |
| --- | --- | --- | --- |
| Línea histórica Point → receta | Confirmada sólo por el `matcher` compartido con código y nombre del producto | Una línea no probada conserva su receta identificada; si no se puede ubicar de manera única, falla cerrado para el valor mensual | No se crea alias ni maestro nuevo |
| Receta exacta → receta padre | Confirmada únicamente por relación activa del proyector | No se infiere equivalencia por SKU, nombre u horario | Relaciones actuales intactas |

## Decisión de diseño

Reutilizar los modelos, servicios y vista existentes. La lectura histórica adjunta el conjunto de recetas con frontera no probada; la proyección distingue autoridad de línea frente a autoridad del mes. Conserva `lock_ready=False`, los originales y los problemas mensuales, y permite mostrar sólo saldos de líneas con fuente localizada. Reconstruir el cierre provisional con `build(rebuild=True, lock_after_build=False)` después de publicar y verificar la implementación; no bloquear el mes ni crear capturas nuevas.

Procedimiento: `inventario_fuentes_datos --term cierre` con PostgreSQL 16; comparar GET autenticado de `/reportes/cierre-producto/?month=2026-09` con preview `refresh_official_sales=False` y HTTP prohibido; verificar cambios de la única fila de cierre y huellas de fuentes antes/después.

Riesgos: nunca mostrar la suma parcial del SKU con extremo no probado como cierre autoritativo; una línea sin identidad de receta permanece «Sin dato». Los totales generales permanecen nulos si cualquier línea requerida sigue sin autoridad. Conteo físico, aprobación y bloqueo mensual no se deducen de esta visualización.
