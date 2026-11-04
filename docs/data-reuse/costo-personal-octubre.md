# Ficha de fuentes — costo de personal mensual

Fecha y ambiente: 2026-11-04, consultas acotadas de sólo lectura en producción; pruebas PostgreSQL 16 aisladas.

## Necesidad y unidad de análisis

Mostrar el mes completo de personal contra ventas, sin duplicar prestaciones, distinguiendo función operativa y costo fiscal corporativo. Una fila es mes/sucursal histórica/destino operativo actual; no se presupone una función histórica que RRHH no conserva.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Nómina | rrhh.NominaPeriodo/Linea/ConceptoLinea | Importación de listas de raya | periodo, empleado y concepto | Octubre 2026: periodos 22 y 24 pagados, 135 líneas, 681500.89 de percepciones | RRHH, rentabilidad, planeación |
| Despensa | NominaConceptoLinea | Lista de raya | percepción código 32 | 21770.00 excluidos de total_percepciones en ambas quincenas | Presupuesto, planeación |
| Puesto y destino | Empleado, CatalogoFuncionOperativa | RRHH | ID estable, puesto_operativo, nivel | Cinco fichas verificadas: Call center y encargado corregidos con bitácora; Paula capturó CAJAS en Alondra/Carlos y AUXILIAR_CAJAS en Leonor, confirmado por DG y releído en producción | Organigrama, costeo |
| Percepciones fiscales | sat_client.CfdiDescargado | SAT | UUID vigente, RFC emisor, FechaFinalPago | Octubre: 138 CFDI, 729484.92, incluyendo ordinarias y extraordinarias | Planeación existente |
| Cargas patronales | ExpedienteCedulaIMSS, ExpedienteISN | Conciliación aplicada | periodo, registro patronal, tipo | Sin expediente aplicado de octubre en consulta; septiembre IMSS id13 | Planeación existente |
| Servicio vales | CfdiDescargado | SAT | UUID de servicio, distinto de carga de saldo | Octubre 2789.02: incluye 1914.00 de TARJETAS TITULAR antes omitidas | Planeación existente |
| Venta | official_point_sales_rows_for_range | Sincronización oficial Point | licencia, producto, día | Octubre 3691537.84 con impuestos | Planeación existente |

## Alias y equivalencias

| Términos | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Nómina del departamento VENTAS / personal directo de tienda | Distintos | VENTAS incluye reparto, call center y jefatura | Se separa por función, no por nombre libre |
| Percepciones fiscales / lista de raya | Distintos, conciliables | Diferencia octubre 26214.03; no sumar ambas fuentes | Conservar diferencia visible |
| Puesto actual / puesto en octubre | No equivalentes automáticamente | No hay snapshot de función | Etiquetar función actual |
| Despensa / bono | No equivalentes automáticamente | Bonos operativos en borrador pueden solaparse | No añadir borradores |

## Decisión

Extender Planeación de personal existente con lectura de listas registradas y comparación fiscal, sin nuevas tablas, capturas, migraciones ni recálculos de nómina. Reutilizar destino_de para jefaturas y funciones de producción. Mantener los cierres históricos y sus contratos; aclarar alcance y enlazar desde el reporte mensual. JSON y CSV añaden datos, conservan columnas anteriores. No se cambian permisos.

Consulta reproducible: inventario_fuentes_datos --term nomina; auditoría ORM de NominaPeriodo.fecha_fin de octubre, conceptos relacionados, expedientes por periodo y build_personnel_plan(date(2026,10,1)). Evidencia privada fuera del repositorio: Documents/Codex/2026-11-04/nomina-ventas/implementacion.

Riesgos: la suma documentada sigue sin cerrar cargas y provisiones. No usar la diferencia frente al presupuesto como plazas disponibles. No prorratear call center ni usar datos recuperados de 2025 como costo de octubre 2026. Puestos pendientes se muestran sin clasificar; no inferirlos de grupos de acceso.
