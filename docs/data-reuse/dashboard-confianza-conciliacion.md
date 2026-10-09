# Ficha de fuentes — confianza y conciliación de dashboards

Fecha y ambiente consultado: 2026-10-04; producción, consultas acotadas de solo lectura; pruebas en PostgreSQL 16 aislado.

## Necesidad y unidad de análisis

Explicar cobertura y vigencia del costo de ventas observadas, y conciliar venta bruta del periodo por sucursal contra cálculos guardados de Rentabilidad. Una fila de venta analítica conserva fecha, sucursal histórica, clave original y fuente. Un cálculo de rentabilidad representa sucursal/mes, no una transacción.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta analítica | reportes.FactVentaDiaria | rebuild_sales_facts, jobs de ventanas | fecha, sucursal, producto_clave, source_kind | Septiembre 2026: neta 3,310,119.13; sin costo respaldado 3,198 filas / 110,845.93 | Dashboard, BI, reportes |
| Venta histórica | ventas.VentaAutoritativaPoint | integración Point autoritativa | sucursal ERP histórica, fecha, product_code | Septiembre 2025: 759 filas de Crucero; v2 identifica esas ventas con mapeo actual Bamoa | Ventas, hechos analíticos |
| Venta Point | pos_bridge.PointDailySale | sincronización Point | PointBranch/producto/fecha | Septiembre 2026: bruta 3,357,735.00 | Rentabilidad, reportes |
| Venta Point v2 | pos_bridge.PointSalesDailyProductFact | integración v2 | PointBranch/producto/fecha | claves receta:65 frente a 1/0001 en autoridad; identidad no intercambiable por nombre | Hechos analíticos |
| Costo aplicado | metadata.costing de FactVentaDiaria / ProductoCostoOperativoMensual | reconstrucción analítica / costeo mensual | receta/mes, fuente y mes aplicado | Septiembre usa julio/agosto; no hay metadatos de costeo en hechos septiembre 2025 | Confianza de ventas |
| Costo histórico de receta | RecetaCostoHistoricoMensual | costeo histórico | receta/mes y coverage_pct | Septiembre 2026: 123 recetas con cobertura de líneas completa | Productos de Rentabilidad |
| Costo de reventa | ProductoReventaCostoHistoricoMensual / ProductoReventaCosto | costeo/importación | producto Point/mes o vigencia | 384 históricos enero-septiembre 2026 | Productos de Rentabilidad |
| Cálculo guardado | rentabilidad.SucursalRentabilidad | recálculo mensual | sucursal/periodo | Septiembre: bruta 3,585,204.00; diferencia con Point 227,469.00; Crucero sin fuente, El Túnel discrepante | Rentabilidad |
| Panel guardado | DashboardSnapshot / mv_dashboard_full | materialización | ventana y generated_at | Ventas se actualizan independientemente de otros paneles | Inicio ejecutivo |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Crucero histórico / Bamoa actual / Point externo 2 | Distinta para historia | Bamoa abrió 2026-07-14; autoridad septiembre 2025 conserva sucursal ERP 4 | Revisar equivalencias antes de cualquier corrección; esta entrega no reasigna |
| producto 1 / 0001 / receta:65 | Candidata | Mismo nombre/receta en algunos registros; eso no prueba identidad universal | No unir por nombre ni normalizar claves en esta entrega |
| Sin costo / costo cero | Distinta | Cero almacenado sin evidencia genera margen aparente de 100% | Mostrar N/D y excluir del ranking de utilidad |
| Venta neta / total / bruta | Distinta | Neta = total menos impuestos; bruta = total más descuento | Comparar bases iguales y etiquetarlas explícitamente |

## Decisión de diseño

Reutilizar fuentes y cálculos existentes. Extender el contrato de lectura con evidencia de costo, meses aplicados, días/sucursales observados, actualización e identidad pendiente. Seleccionar fuente preferida por sucursal/día sin fusionar claves. Conciliar por sucursal con precisión de centavos; errores que se compensan no certifican el total. Ausencia de fuente/cálculo conserva N/D, incluso para importe cero. El aviso no certifica cobertura operativa ni conciliación bancaria.

Los productos sin costo positivo, con costo mensual parcial o clasificados como anticipo no compiten en utilidad. Los paneles semanales describen su costo como estimación de la semana disponible al corte; no se usa una semana futura. La hidratación del dashboard renueva ese panel junto al corte de ventas para no reutilizar un margen guardado obsoleto.

No se crean tablas, migraciones, capturas, importaciones ni equivalencias nuevas. No se borran snapshots históricos ni se modifican ventas, costos, sucursales, nómina o configuración de producción.

Consultas/procedimiento: inventario_fuentes_datos --term venta --term ticket --term costo --term produccion --term sucursal --presence --limit 100; transacciones BEGIN READ ONLY con statement_timeout; agregaciones acotadas a septiembre 2025/2026 y catálogos de cobertura. EXPLAIN de agrupación mensual usa índice de fecha; no equivale a benchmark.

## Riesgos y pendientes

La cobertura financiera sigue provisional si falta costo, se arrastra costo anterior, hay identidad pendiente, gastos incompletos o discrepancia de venta. Días observados no prueban todos los envíos esperados. La identidad pendiente detecta sucursal ausente o venta anterior a apertura conocida; fechas de apertura nulas y otros conflictos semánticos requieren revisión. FactVentaDiaria no identifica tickets/transacciones y no autoriza inferir actividad cero a partir de tickets=0. El ranking de productos revisa los 80 de mayor venta, no todo el catálogo. La conciliación compara Point legacy con cálculos guardados, y no convierte el mapeo actual PointBranch en evidencia de identidad histórica.
