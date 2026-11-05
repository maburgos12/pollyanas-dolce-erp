# Costo de personal: cargas mensuales registradas

## Necesidad y unidad

Costo mensual por persona canónica, departamento y empresa; las percepciones, vales, cargas patronales e impuesto devengado se conservan como conceptos distintos. Los pagos no generan un segundo gasto.

## Fuentes y consumidores

| Concepto | Fuente existente / identidad | Escritura | Lectura |
|---|---|---|---|
| Persona y adscripción | rrhh.Empleado; NominaLinea con snapshots históricos | Capital Humano e importaciones de nómina | Lector de personal registrado |
| Percepciones y despensa | NominaConceptoLinea PERCEPCION y totales de NominaLinea | Importaciones conciliadas | Planeación, rentabilidad, exportación |
| Cargas mensuales | NominaConceptoLinea OBLIGACION, código MAAAAMM:columna | Conciliación autorizada del tabular mensual CONTPAQi | Consolidado por departamento y resumen final |
| ISN | bases_gravadas_empleados, calcular_isn_sinaloa; ExpedienteISN aplicado cuando existe | Servicios actuales; el informe no presenta declaraciones ni crea pagos | Consolidado y reparto por base gravada |
| Gasto y soporte | GastoOperativoMensual, ObligacionGasto, CFDI vigente y reparto existente | Flujo de compromisos y pagos | Lector compartido de gastos, consolidado y descarga |

## Contrato de las cargas

Se conservan las columnas originales del tabular: AU Invalidez y Vida, AV Cesantía y Vejez, AW Enfermedad y Maternidad patronal, AX Retiro SAR, AY Impuesto estatal del tabular, AZ Riesgo de trabajo, BB Infonavit, BC Guardería y BD Otras obligaciones. BA es un resumen de contraste y BE es el control total: ninguno se vuelve a sumar. Cada persona tiene exactamente las nueve columnas una vez en el mes; se guardan también los ceros para distinguir cobertura de ausencia.

El documento mensual se asocia a la última línea de nómina existente de cada persona del mes. La cobertura es mensual, no una carga adicional de cada quincena. Los importes, columnas, códigos de empleado y hash del documento fueron contrastados contra el original del NAS antes de aplicar; el AuditLog conserva cobertura, totales, hash y condición de que percepciones, deducciones y netos no cambian. Los registros se verificaron en producción para el periodo solicitado.

## Decisiones y límites

Reutilizar NominaConceptoLinea y AuditLog; no crear otro maestro ni una segunda nómina mensual. La identidad se enlaza por el código numérico CONTPAQi único, comprobando además el nombre; no se fusionan personas por semejanza de texto. La función proviene del snapshot cuando existe y conserva la lectura legada en su ausencia.

IMSS suma AU, AW, AZ y BC; RCV/Infonavit suma AV, AX y BB. El impuesto AY del tabular se conserva como fuente original pero no sustituye la determinación del ISN: el consolidado usa el expediente aplicado o el cálculo existente del ERP, y reparte por base gravada con conservación de centavos. Una diferencia entre el control patronal aplicado y las cargas de nómina impide presentar un reparto conciliado. El servicio de vales permanece global, sin asignación inventada por departamento.

El costo de departamentos incluye nómina, vales y cargas verificadas, antes del servicio de vales y de provisiones contables de prestaciones. La cobertura incompleta o duplicada no se transforma en costo cero. Este cambio no certifica un cierre contable integral ni altera XML fiscales, sueldos, bonos o pagos.

## Verificación

Pruebas con traslado entre sucursales, cobertura completa e incompleta de cargas, impuesto estatal del tabular deliberadamente distinto, cálculo de ISN reutilizado, control patronal discordante y conservación de los totales anteriores. Pantalla y CSV consumen el mismo consolidado. El detalle de auditoría y respaldos operativos permanece fuera del repositorio.
