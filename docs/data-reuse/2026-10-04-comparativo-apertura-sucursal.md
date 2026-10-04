# Ficha de fuentes — comparativo de ventas y apertura de sucursal

Fecha y ambiente consultado: 2026-10-04; PostgreSQL 16 de producción, consultas acotadas de solo lectura; inventario de metadatos en PostgreSQL local aislado con main migrado.

## Necesidad y unidad de análisis

Validar cobertura por sucursal/día del acumulado mensual hasta el último cierre completo. Una sucursal no debe tener ventas exigidas antes de su apertura registrada. La comparación conserva el mismo intervalo de fechas en ambos años y la red histórica de cada año.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Apertura operativa | core.Sucursal / core_sucursal | Catálogo existente de sucursales | id; codigo único; fecha_apertura | ERP 18, SINALOA_LEYVA, activa, apertura 2026-10-03 | core.branch_catalog, reportes, ventas, Dashboard |
| Correspondencia Point | pos_bridge.PointBranch / pos_bridge_branch | Integración existente Point–ERP | external_id y erp_branch_id | Point 28, external_id 14, ERP 18, ACTIVE | Integración y lector de ventas |
| Venta diaria canónica | reportes.FactVentaDiaria y fuentes seleccionadas por get_daily_sales_bulk | Importación existente Point y materialización analítica | fecha, sucursal_id, producto y fuente | 2026-10-01: 102209.00; día 2: 117260.68; día 3: 145019.01 | Dashboard, Ventas, BI |
| Cierre de venta | pos_bridge.PointDailyBranchIndicator / pos_bridge_daily_branch_indicator | Sincronización Point posterior al cierre | indicator_date, branch_id, sync_job | ERP 18 solo tiene indicador de 2026-10-03: 8257.00; job 82118 SUCCESS el 2026-10-04 | latest_closed_sales_date y cobertura mensual |
| Histórico equivalente | Venta canónica histórica y caché oficial mensual/rangos | Integración y lector existentes | red histórica, 2025-10-01 a 2025-10-03 | 119666.00 + 105714.00 + 111563.00 = 336943.00 | build_closed_yoy_panel |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Sinaloa de Leyva / SINALOA_LEYVA / ERP 18 / Point external_id 14 | Confirmada | Relación explícita PointBranch.erp_branch_id; Mauricio confirma sucursal nueva con apertura día 3 | Confirmación recibida el 2026-10-04 |
| Sucursal Leyva / LEYVA / ERP 8 | Distinta | Registro diferente; pertenece a la red histórica 2025 | No fusionar con ERP 18 |
| Apertura de inventario / apertura de sucursal | Distinta | StockMensualSucursal.stock_apertura es saldo de inventario, no fecha operativa | No reutilizar como fecha |
| Rentabilidad.SucursalRentabilidad.fecha_apertura | Candidata secundaria | Snapshot por sucursal/periodo; la elegibilidad operativa ya utiliza core.Sucursal | Reutilizar core, sin nueva equivalencia |

## Decisión de diseño

Reutilizar core.Sucursal.fecha_apertura al validar cada sucursal/día. Consultar las fechas una vez para las sucursales ya seleccionadas por required_sales_branches y omitir solo las obligaciones anteriores a la apertura. Exigir los datos desde el propio día de apertura. No crear registros de venta cero, cambiar aperturas, reasignar ventas ni añadir tablas. Conservar las reglas actuales de cierre, fuente canónica, ceros acreditados y red histórica completa.

Cambiar la versión del caché del panel y actualizar el shell PWA junto con la etiqueta visible del intervalo en el Dashboard. Los consumidores del lector compartido son Dashboard, Ventas y BI.

Consultas o procedimiento reproducible: `inventario_fuentes_datos --term sucursal --term apertura`; `Sucursal.objects.filter(id__in=[17,18]).values()`; indicadores de ERP 18 entre 2026-10-01 y 2026-10-03; `get_daily_sales_bulk` para los tres días de octubre de 2025 y 2026; `closed_month_comparison` al corte 2026-10-03. Consultas productivas dentro de `SET TRANSACTION READ ONLY`.

Riesgos y pendientes: una fecha nula conserva la obligación existente desde el inicio del intervalo; un faltante real después de apertura debe seguir pendiente. No modificar fuentes, identidades ni configuración operativa. Verificar ambos totales, fechas y variación en las tres pantallas después del despliegue.
