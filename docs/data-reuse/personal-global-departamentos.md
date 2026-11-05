# Ficha de fuentes — global de personal y gastos

Fecha y ambiente: 2026-11-04, lectura acotada de producción (octubre 2026) y PostgreSQL16 local aislado.

## Necesidad y unidad de análisis

Costo registrado mensual por función de personal, gastos por área presupuestal y peso sobre ventas totales; conservar pendientes sin inventar costo integral ni distribuirlos por ubicación.

## Fuentes candidatas

| Concepto | Modelo / fuente oficial | Identificador y ámbito | Evidencia y consumidores |
| --- | --- | --- | --- |
| Nómina y vales | RRHH NominaLinea / NominaConceptoLinea; leer_personal_registrado | Línea/periodo/persona; listas cerradas o pagadas | Octubre: periodos22/24; subtotal703270.89, incluye vales adicionales21770. Planeación HTML/JSON/CSV. |
| Función y personas | RRHH Empleado + clasificación existente | ID de persona y destino; sucursal histórica por línea | Agrupar por función, unión de IDs entre sucursales. No duplicar traslado. Sin modificar clasificación. |
| Ventas | official_point_sales_rows_for_range | Mes de venta oficial Point con impuestos | Octubre3691537.84; mismos filtros del informe actual. |
| Cargas y servicio vales | build_personnel_plan: expedientes IMSS/ISN aplicados y CFDI vigente | Mes de obligación, registro patronal, UUID | Se reutilizan campos imss/rcv/isn/fees. No sumar CFDI de nómina ni diferencia fiscal a las listas. |
| Gastos operativos | leer_gastos_mensuales(usar_reparto_consolidado=True) | Clave de origen y asignación; cobertura mensual | Consulta octubre:14 filas sumables;115 referencias pendientes. Fuentes GastoOperativoMensual/ObligacionGasto/LineaPresupuestoMensual y reglas existentes; no recálculo ni escritura. |

## Alias y equivalencias

Las funciones de personal y las áreas presupuestales son dimensiones distintas: se muestran en tablas separadas, sin inventar reparto entre limpieza, almacén y administración. Matriz es ubicación, no toda la función de tienda. CEDIS no agrega otra persona ni gasto. No se crean equivalencias ni catálogos.

## Decisión de diseño

Reutilizar lectores existentes. Extender el resultado de personal con agregado por función (IDs únicos), añadir resumen del corte seleccionado, mantener claves anteriores. Gastos parciales suman sólo su importe mensual reconocido y exhiben estado; pendientes no suman. No usar presupuesto, contratos sin recibo ni total fiscal como sustituto de nómina. El total es un subtotal registrado: costo integral permanece pendiente.

Consultas reproducibles: inventario_fuentes_datos --term nomina --term gasto --term departamento; leer_gastos_mensuales(date(2026,10,1), usar_reparto_consolidado=True) dentro de transacción READ ONLY. Evidencia local fuera del repositorio en Documents/Codex/2026-11-04/personal-global-departamentos.

Riesgos: diferencia fiscal, provisiones, cargas faltantes, gastos sin cobertura/reparto y archivos ausentes. El lector de gastos excluye materia prima, empaque de fabricación e inversiones; el informe declara ese perímetro y no presenta utilidad ni costo total de fabricar. No ampliar a esos conceptos sin sus fuentes y conciliaciones. No se modifican datos, modelos, permisos ni migraciones.
