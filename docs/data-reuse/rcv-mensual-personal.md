# Ficha de fuentes — RCV mensual del costo de personal

Fecha y ambiente consultado: 2026-11-05; producción en solo lectura y PostgreSQL16 local aislado.

## Necesidad y unidad de análisis

Mostrar el RCV/Infonavit de septiembre2026 que falta en el consolidado mensual. Una obligación pertenece a una persona y a un mes, aunque las percepciones tengan dos quincenas. No transformar el costo registrado en prueba de pago ni crear un expediente SUA ficticio.

## Fuentes candidatas

|Concepto|Modelo / tabla|Fuente que crea y actualiza|Identificador y ámbito|Evidencia|Consumidores|
|---|---|---|---|---|---|
|Personal y nómina|rrhh.Empleado, NominaLinea, NominaPeriodo|Capital Humano / importador de listas|Empleado existente + periodos20/21|71personas, dos quincenas PAGADA|Planeación, nómina y rentabilidad|
|Obligaciones mensuales|rrhh.NominaConceptoLinea|Lista mensual CONTPAQi; revisión documentada|tipo OBLIGACION; M202609:AV/AX/BB, una vez por persona|213componentes, suma90088.00; no existen antes de la integración|leer_personal_registrado, cargas_mensuales_personal, tabla histórica, resumen y CSV|
|Documento original|NAS/finanzas/contabilidad/SEPTIEMBRE 2026/NOMINA R H/LISTA DE RAYA TABULAR DE SEPTIEMBRE.xlsx|Contabilidad/CONTPAQi|GEF211230KR2, E5240157100, 01–30septiembre2026|SHA2568956bc3b50e985b703fcdee93ff782ec257c7c9ac7b7f56ba0d896c0a62f3116; 71filas y total impreso de columna|Trazabilidad de importación y AuditLog|
|Control patronal|reportes.ExpedienteCedulaIMSS|SUA/SIPARE|Registro patronal, tipo y periodo|No hay expediente BIMESTRAL septiembre/octubre aplicado; último aplicado agosto|Presupuesto, rentabilidad y lector de cargas pagadas|
|Importación y auditoría|rrhh.NominaImportacion, core.AuditLog|Importador y correcciones autorizadas|Importación10 y marca idempotente|Ajuste previo de−630.08 conservado; no otro gasto|Conciliación fiscal contra lista y pago|

## Alias y equivalencias

|Identificadores|Estado|Evidencia y caso contrario|Revisión requerida|
|---|---|---|---|
|Código002 y código2, etc.|Confirmada como formato del mismo identificador|Mismo código numérico y nombre exacto de las71personas. No se modifica ni fusiona ningún maestro.|Ninguna equivalencia nueva de personas|
|ColumnaAT septiembre → concepto canónicoAV; AV → AX; AZ → BB|Confirmada por encabezado exacto|Cesantía y vejez45064.70; retiro12863.49; Infonavit32159.81. Los códigos existentes provienen de la disposición física de octubre, no indican la columna de cualquier archivo.|Documentar columna original y nombre en importación y auditoría|
|Obligación mensual y cédula/pago|Distintas|El tabular acredita el costo mensual registrado; no acredita pago bancario ni permite crear una cédula oficial.|Conservar los lectores de pagos y sus controles|

## Decisión de diseño

Reutilizar NominaConceptoLinea y NominaImportacion, sin nuevas tablas, capturas o maestros. Leer la familia RCV por separado cuando estén sus tres conceptos completos, una sola vez para cada persona del mes. Mantener la regla actual de las nueve obligaciones para cerrar y repartir el costo integral de personal. El IMSS oficial existente tiene prioridad; no se sustituye con la cifra divergente del tabular. El RCV se usa únicamente si falta el control del mes, con etiqueta «registrado en nómina». Los lectores de cargas pagadas no reciben una afirmación de pago desde este documento.

Consultas reproducibles: inventario_fuentes_datos --term nomina --term obligaciones --term Infonavit (artefacto externo inventario-fuentes.txt); NominaPeriodo20/21; NominaImportacion9/10; NominaConceptoLinea filtrado por periodos y tipo; hash y encabezados del XLSX; suma de filas por columna contra el total impreso. Extracción limitada a71empleados de septiembre.

## Riesgos y validación

Una familia incompleta, duplicada o un mes incompleto no debe producir un total válido. La existencia del RCV no cierra el costo total por persona/departamento. Antes de aplicar, cotejar nombres y códigos, conservar huellas de líneas y conceptos previos, preservar el ajuste630.08 y validar transacción en simulación. Verificar tabla, resumen, CSV y ausencia de cambios en octubre después del despliegue.
