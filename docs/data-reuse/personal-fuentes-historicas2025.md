# Ficha de fuentes — Planeación de personal histórica

Fecha: 2026-11-05. PostgreSQL 16 local aislado y producción en solo lectura.

## Necesidad y unidad de análisis

Comparar personal real con ventas del mismo periodo, conservando el contraste fiscal y las exclusiones documentadas. Venta por sucursal y día; ajuste de nómina por UUID e importación. No capturar nuevamente ventas, nómina, pagos ni personas.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Venta oficial | PointDailySale | Sincronización del reporte oficial Point | Licencia, sucursal ERP y día | Sin filas 2025 en el corte anterior; pruebas conservan lectura 2026 | Planeación, ventas |
| Venta histórica | FactVentaDiaria | Export autoritativo materializado; selección existente selected_sales_facts | Sucursal ERP, día, producto y procedencia | 97,312 filas AUTHORITATIVE 2025; venta con impuestos 44,418,445.59 antes de selección de linaje | Rentabilidad, analítica |
| Percepciones reales | NominaLinea y NominaConceptoLinea | CONTPAQi y recibos conciliados | Periodo, empleado y snapshot histórico | 1,764 líneas, 75 periodos, 8,018,073.26; auditorías 42539 y 42578 | Personal, rentabilidad |
| Contraste y ajustes | CfdiDescargado y NominaImportacion.resumen | SAT e importación conciliada | UUID vigente, periodo e importación IMPORTADA | Nueve ajustes con estados de conciliación explícitos, suma -1,476.44 | Contraste fiscal; nunca otro gasto |
| ISN y otros CFDI estatales | ExpedienteISN y CfdiDescargado | Expediente aplicado y XML original | Periodo fiscal y código de concepto | 43 alertas eran peajes, trámites vehiculares y documentos en cero; 13 fuera de 2025 | Cargas documentales |

## Alias y equivalencias

No se crean equivalencias. Se usan sucursal_id y la relación erp_branch existente. Los códigos y nombres Point no identifican empleados. Las funciones históricas provienen de CONTPAQi y prenóminas corroboradas; las sucursales de personal sin prueba permanecen sin asignar.

## Decisión de diseño

Reutilizar selected_sales_facts y su exclusión de exportaciones reflejadas. Incorporar únicamente AUTHORITATIVE del ejercicio auditado 2025 donde no exista reporte oficial Point de la misma sucursal y día. No sumar V2/LEGACY como sustituto automático. Conservar IDs históricos y sucursales cerradas. Las ventas históricas sin sucursal permanecen excluidas con aviso explícito.

Conservar la diferencia fiscal original y explicar por separado los ajustes conciliados de la importación. Solo cierra aritméticamente si los UUID corresponden a los CFDI leídos, los importes son válidos, los estados están conciliados y no se repiten. Un ajuste inválido sigue como pendiente; no cambia percepciones ni XML.

Clasificar como ajenos al ISN solamente los conceptos vehiculares inequívocos y documentos exactamente sin movimientos con importe cero. Mantener visibles las exclusiones del corte. Un concepto fiscal desconocido continúa pendiente. Las reservas futuras no se incluyen en los componentes de costo real.

Procedimiento: inventario_fuentes_datos --term ventas; consulta acotada FactVentaDiaria por source_kind en 2025; lectura de los XML de las alertas y metadatos de importación. No se modifican esas fuentes.

Límites: estas correcciones no prueban asignación histórica de personal por sucursal, gastos sin comprobantes ni costo histórico de fabricación. No certifican utilidad neta ni autorizan contrataciones.

Comprobación de compatibilidad: aplicar el mismo fallback sin límite de ejercicio incorporaría otras referencias de enero a mayo 2026 (763/762/775/464/132 filas). Esas identidades no forman parte de esta comprobación 2025; se conserva la lectura actual de 2026. No se crean equivalencias por nombre ni se usan esas diferencias para modificar ventas.
