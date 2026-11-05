# Ficha de fuentes — Cargas reales históricas 2025

Fecha: 2026-11-05. Inspección de producción de solo lectura y PostgreSQL 16 local aislado.

## Necesidad y unidad de análisis

Una cédula por registro patronal, periodo fiscal y tipo. El pago bancario puede liquidar una cédula mensual y otra bimestral; no representa dos movimientos bancarios ni equivale íntegramente al gasto de empresa.

## Fuentes candidatas

| Concepto | Modelo | Fuente y clave | Evidencia | Consumidores |
| --- | --- | --- | --- | --- |
| Control patronal | ExpedienteCedulaIMSS | SUA; registro, periodo, tipo, revisión | Cinco expedientes válidos 2025, trece pendientes de materializar | Planeación, rentabilidad, costeo |
| Original | DocumentoCedulaIMSS | SHA256 único, expediente | Existen XLS y trece controles recuperados en PDF | Detalle y descarga protegida |
| Persona y cuota | DetalleCedulaIMSS | Documento y NSS únicos | 1,311 filas en 18 cédulas; 126 códigos CONTPAQi | Cruces y distribución |
| Identidad histórica | Empleado, NominaLinea | Código y NSS CONTPAQi; snapshot de nómina | 1,764 recibos materializados, auditoría 42539 | No usar sucursal o función actual |
| Pago y separación contable | metadata del expediente | Estado de cuenta, póliza y retenciones CFDI, huellas de originales | 12 obligaciones concilian 2,022,668.83; gasto corregido 1,954,688.81 | Control corporativo y costeo pagado |

## Alias y equivalencias

NSS y código CONTPAQi confirmados por el historial restaurado. No se autorizan ni aplican alias Point por similitud de nombre. La asignación histórica por sucursal continúa sin prueba suficiente. No crear un segundo padrón.

## Decisión

Extender las clases documentales con SUA PDF y conservar un único documento SUA principal por expediente, sea XLS o PDF. Los componentes de retiro, cesantía y vivienda ya integran cuota_patronal; no se suman dos veces.

Conservar total_patronal como control documental. En metadata.pago_conciliado, versión 1, registrar fecha real, importe de la cédula liquidado, gasto de empresa, retenciones, cuentas por cobrar y huellas del pago, conciliación y SUA. El importe del comprobante compartido permanece separado. Validar que pago = gasto + retenciones + cuentas por cobrar; no sustituir datos inválidos por cero ni por un total sin conciliar.

Planeación conserva la mensualización existente del bimestre. Costeo pagado usa fecha de pago documentada. La cuota absorbida sin distribución individual permanece sin asignar; no se reparte usando porcentajes supuestos. El cruce histórico no se vuelve a calcular contra RRHH actual.

Procedimiento: inventario_fuentes_datos --term cedula con PostgreSQL local; consulta acotada de los cinco expedientes 2025 en producción; matrices privadas con SHA256, control bancario, detalle y código CONTPAQi. Las fuentes privadas no se versionan en Git.

Límites: no se emiten pagos ni se modifica el auxiliar contable original. La corrección de retención de agosto se conserva como ajuste documentado de 2.77. No certificar rentabilidad por sucursal hasta resolver la asignación histórica y restantes fuentes de gasto y producción.
