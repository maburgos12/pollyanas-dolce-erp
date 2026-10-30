# Ficha de fuentes — auditoría de rentabilidad mensual

Fecha y ambiente: 30 octubre 2026, producción PostgreSQL; lectura, sin reconstruir snapshots. Julio–septiembre de 2026. Commit 9f470cf9.

## Necesidad y unidad de análisis

Una fila sucursal-mes debe explicar ingresos netos, costos de producto, merma, gastos propios y costos centrales. Distinguir transacción de origen, histórico de costo, snapshot mensual y cálculo mostrado en memoria. La unidad de costeo de receta debe conciliarse con la unidad vendida.

## Fuentes candidatas

| Concepto | Modelo / tabla | Crea/actualiza | Identificador y ámbito | Evidencia | Consumidores |
|---|---|---|---|---|---|
| Sucursal e identidad Point | core.Sucursal, pos_bridge.Branch | Catálogo ERP y enlace Point | ERP id y branch.erp_branch_id; activos y vigencia | 9 sucursales con ventas en cada mes, 12 snapshots septiembre | Tarea, dashboard, nómina |
| Ventas | PointDailySale / pos_bridge_daily_sales; ventas_autoritativas_point; FactVentaDiaria | Sincronización/importación Point y proyección | fecha, sucursal, producto; bruto, impuesto, descuento, neto | Totales independientes julio 3128168.88, agosto 3409584.02, septiembre 3310119.13 | Rentabilidad, confianza de ventas, BI |
| Costos ingredientes y preparación | RecetaCostoHistoricoMensual; RecetaCostoVersion; RecetaCostoSemanal | Costeo histórico y semanal | receta-periodo o receta-versión/fecha; unidad/rendimiento | 114/119/123 filas históricas; 0 cobertura incompleta; 65/63/66 combinaciones sucursal-receta usan fallback de versión | Tarea, productos, OperatingFinance |
| Costo de producto fabricado | ProductoCostoOperativoMensual, ProductoSucursalContribucionMensual | OperatingFinanceSnapshotService.build_snapshot | receta-mes; receta-sucursal-mes | MO cero en los tres meses; indirectos septiembre 441 | Costeo operativo, Consejo IA |
| Reventa | ProductoReventaCostoHistoricoMensual / ProductoReventaCosto | Costo de compra/Point e histórico | producto Point-mes o fecha de vigencia | Guamúchil 3021.732158; Extra 10 1753.646778 | Tarea, productos, costeo operativo |
| Merma | control.MermaMensualSucursal | Materialización de PointWasteLine | sucursal-receta-mes y source_ids | Guamúchil septiembre 21 filas, costo registrado 2292.23, algunas filas cero | No consumida en tarea Rentabilidad |
| Gasto económico | GastoOperativoMensual; ObligacionGasto; GastoRecurrenteVersion | Captura/importación y generación recurrente | gasto/obligación, centro, cobertura, regla | sources de los tres meses; sin claves de fila duplicadas | Lector gastos, presupuesto, costeo operativo |
| Nómina | rrhh.NominaLinea y NominaConceptoLinea | Nómina cerrada/pagada | línea-periodo; departamento/sucursal snapshot | Producción 36/36 líneas concilian despensa; Matriz importe antiguo excede fuente 5166.66 | Lector personal, reglas presupuestales |
| Cargas | ExpedienteCedulaIMSS, detalles y LineaPresupuestoMensual | Aplicación cédula/SIPARE; manual identificado | expediente-tipo-periodo-registro patronal, línea-rubro-versión | Julio mensual y agosto mensual/bimestral aplicados; septiembre sin expediente aplicado en consulta | Nómina costo patronal, presupuesto |
| Gastos producción/centrales | LineaPresupuestoMensual, RubroPresupuesto, ReglaFuenteRubro | Servicios de consolidación de fuentes existentes | rubro-periodo-version ORIGINAL y origen | Producción septiembre no CONSUMO_MP 273077.15; logística 47817; administración 92814.16 | Presupuesto; no integrados completamente en Rentabilidad |
| Snapshot de tienda | rentabilidad.SucursalRentabilidad | Celery/recalcular_rentabilidad_mensual | único sucursal-periodo | 32 filas julio–septiembre, ventas/costos almacenados y gastos actuales difieren | UI, reporte mensual, ROI/payback, agente, Consejo IA, OperatingFinance |

## Alias y equivalencias

| Candidatos | Estado | Evidencia y caso contrario | Revisión |
|---|---|---|---|
| Costo materia prima de snapshot = costo completo fabricación | Distinta | MO/indirectos usados cero, costo mensual fábrica en otra fuente | Separar capas |
| Costo total receta = costo por unidad de rendimiento | No universal | 6 filas históricas por mes tienen valores distintos; no inferir igual por nombre | Resolver unidad en recetas efectivamente vendidas |
| Crucero = Bamoa para sumar meses | No resuelta en esta auditoría | Crucero inactivo sin ventas fuente y Bamoa sí tiene fuente; no se reasigna historia | Identidad/vigencia antes de migrar |
| Gasto, obligación, pago y espejo presupuestal | Misma operación cuando hay vínculo explícito | Lector evita sumar obligación enlazada y espejo automático; no asumir equivalencia por importe | Preservar IDs y desglose |
| Extra 10 = mercancía con costo de adquisición correcto | No resuelta | 39 unidades, venta 390, costo histórico 1753.646778; unidad/identidad requiere contraste | No retirar costo sin evidencia |
| Exceso Matriz = nómina de Sinaloa de Leyva | Candidata | Diferencia 5166.66 y mismo importe en nuevo destino; falta traza individual | No modificar RRHH |
| Despensa producción ya en total oficial | Refutada para las 36 líneas revisadas | Desglose − oficial = despensa en 36/36 | Mantener separada una vez |
| Cobertura 100% = cierre completo | Distinta | Siete familias presentes, soporte parcial y componentes adicionales pendientes | Cambiar contrato de completitud |

## Decisión

Reutilizar las fuentes y servicios existentes. Extender un contrato común de resultado mensual con componentes, periodos, origen, versión y completitud. No crear captura paralela. La asignación gerencial de fábrica por tienda es una decisión material pendiente, no una equivalencia autorizada. Antes de modificar: aprobar alcance de contratos y recálculo; conciliación por componente sin tocar importes oficiales ni datos de nómina.

## Consultas reproducibles

`extraer.py`: PostgreSQL validado, transacción READ ONLY, inventario con términos rentabilidad/produccion/nomina/costo; lectores de costos y snapshots sin guardar. `conciliar.sql`, `puente.sql`, `versiones.sql`, `componentes_fuente.sql`, `despensa.sql`, `empaques.sql`: todos SELECT ejecutados mediante erp_read.py con READ ONLY y timeout. JSONL adjuntos. `generar_informe.py`: fórmulas con Decimal y asserts, CSV y verificacion.json.

## Riesgos y pendientes

Falta cierre documental íntegro de cargas septiembre, fuente completa de merma/costos faltantes, conciliación de versiones de receta y clasificación de Extra 10. Las líneas de presupuesto no deben sumarse a los gastos económicos originales. No hay regla autorizada nueva de asignación de fábrica. El resultado integral por tienda permanece no determinado; no se sustituyó con un prorrateo ni se trasladó al usuario una lista genérica de búsquedas.


## Decisión de implementación

Lectura común de ventas netas Point, costos de materiales por unidad, adquisición de reventa, merma y gastos trazables. No se crea tabla maestra ni se cambia asignación de personal. Se conserva el snapshot previo cuando falta fuente y se excluye ese importe no confirmado de la proyección de lectura. Nómina parcial actual sustituye al total anterior sin aplicar max.

Los costos centrales se leen una sola vez desde el real ORIGINAL de rubros activos de producción, logística y administración. Se excluyen CAPEX, consumo MP y rubros de materias primas. Se muestran como registrados pendientes de conciliación, nunca como costo integral certificado. No se distribuyen a sucursales. No se usa el presupuesto como real.

Los consumidores usan el mismo resultado: tarea, tablero, detalle, historia, reporte mensual, JSON/CSV, agente de rentabilidad, Consejo IA y totales de operating finance. El alias de exportación utilidad_neta se conserva por compatibilidad, acompañado de alcance RESULTADO_PARCIAL_SUCURSAL y utilidad_neta_confirmada=null; los nuevos campos central_registrado y resultado_empresa_parcial identifican la lectura de empresa. Horas extra son informativas y no se vuelven a sumar.

Sin migraciones ni modificación de fuentes operativas. No hay una regla aprobada de reparto integral: ROI, recuperación y payback permanecen no calculables. El costo histórico con cobertura incompleta y los productos sin costo se reportan pendientes; no se inventa 30% ni se ocultan costos superiores a venta.
