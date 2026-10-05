# Ficha de fuentes — Inicio y BI: venta completa con IVA

Fecha y ambiente consultado: 2026-10-04; producción, consultas de solo lectura; PostgreSQL16 local aislado para validaciones.

## Necesidad y unidad de análisis

Venta registrada con IVA, después de descuentos, contado y crédito, por sucursal histórica/día/producto. Comparativos mensuales y anuales de toda la red, composición por categorías y cantidad/precio medio por producto. No sustituir el total por sucursales actualmente activas ni por una muestra comparable.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Histórico de venta | ventas.VentaAutoritativaPoint | Importación histórica Point | sucursal ERP histórica, fecha, código original | 97,312 filas en 2025; doce meses; total_amount $44,418,445.59 | ventas, reportes |
| Hechos analíticos | reportes.FactVentaDiaria | analytics_service.rebuild_sales_facts | fecha/sucursal/producto_clave/source_kind | AUTHORITATIVE de 2025 coincide por mes e importe con el original; septiembre 2025 $3,297,844.00 | Inicio, BI, confianza, materializaciones |
| Venta reciente | PointSalesDailyProductFact y PointDailySale | Sincronización Point | sucursal Point, fecha, producto; vínculo ERP vigente | septiembre 2026 incluye nueve sucursales, mientras 2025 conserva Crucero | ventas, analítica |
| Control mensual | PointMonthlySalesOfficial | Reporte mensual oficial Point | mes empresa | ninguna fila 2025: no reemplazar historia con cero | dataset ejecutivo |
| Tickets / contado-crédito | PointDailyBranchIndicator | Indicadores Point | sucursal Point/día | septiembre 2025 sin filas; septiembre 2026 269 filas; no usar tickets por producto | cierre diario |
| Costos | FactVentaDiaria.metadata.costing | Costeo operativo mensual | receta/mes/fuente documentada | cobertura revisada por servicio sales_confidence existente | rentabilidad, confianza |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Crucero ERP4 / Bamoa ERP17 | Distintas | Histórico 2025 en Crucero; Bamoa apertura julio2026; Point externo2 tiene relación vigente distinta | Ninguna reasignación histórica |
| venta_total / venta_neta / venta_bruta | Distintas | total incluye IVA después de descuento; neta excluye impuesto; bruta antes de descuento | No cambiar fórmulas financieras existentes |
| SKU sin receta y códigos con ceros | No resuelta | Nombres similares no prueban identidad | No se crean equivalencias; conservados por fuente/clave/categoría |
| Vínculos receta existentes | Confirmada como relación persistida, no nueva equivalencia | Reutilizar FK existente; preservar categoría/presentación | No alterar catálogo |

## Decisión de diseño

Extender Inicio y BI con el mismo servicio de lectura y el mismo componente. Reutilizar FactVentaDiaria y precedencia actual por sucursal/día AUTHORITATIVE > V2_FACT > LEGACY. No captura, tabla, migración ni importador nuevos. Conservar sucursales inactivas y todos los registros, incluso asignación pendiente. N/D no es cero. Precio medio realizado no es tarifa; efectos dentro de cada sucursal/producto: q1(p1-p0) y p0(q1-q0); ajustes y productos sin comparación separados. La composición es participación en venta, no un residuo denominado mezcla. Cantidades se muestran por producto, sin sumar unidades incompatibles.

Consultas reproducibles: inventario_fuentes_datos --term venta --term ingreso --term ticket con PostgreSQL válido; agregaciones mensuales acotadas al año2025 de venta_autoritativa_point.total_amount y reportes_factventadiaria.venta_total; agrupar fuente por año para comprobar superposición. PGOPTIONS default_transaction_read_only=on, statement_timeout=45000 en producción.

Riesgos y pendientes: hechos materializados pueden ir detrás de la captura, registros no prueban cada día operativo. No inferir tickets de líneas de producto. Costos permanecen provisionales cuando falta evidencia; desplegables existentes conservados. Validar sumas de sucursales, categorías, productos y componentes contra total del mismo panel y luego pantalla autenticada.

Evidencia adicional: marzo2026 contenía dos importaciones del mismo export (source_sheet category_report y PointSalesDailyProductFact). Cada conjunto suma $3,326,094.19; juntos $6,652,188.38. PointDailySale oficial suma $3,326,094.19. La lectura omite solamente la copia derivada cuando existe el reporte original con la misma sucursal/fecha/source_file no vacío; no elimina ni fusiona datos. Otros archivos y derivados sin original permanecen. Los hechos históricos de 2025 no presentan ese doble origen.
