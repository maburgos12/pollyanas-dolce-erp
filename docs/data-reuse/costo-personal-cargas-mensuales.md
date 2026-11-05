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

## Reconocimiento de prestaciones confirmado por Dirección

Dirección confirmó que vacaciones, prima y aguinaldo se reconocen al pagarse, y que los apartados bancarios son ahorro interno sin provisión contable mensual documentada. Se conserva esta confirmación, para el mes analizado, en `NominaImportacion.resumen.criterio_prestaciones`, con fuente CONFIRMACION_DG, método AL_PAGO y provisión contable adicional 0.00; AuditLog conserva la actualización. No se modifican el archivo original, los importes de nómina ni los pagos.

El lector distingue esa confirmación de la ausencia de información: solo una confirmación válida permite mostrar cero de provisión adicional. Los conceptos pagados 19/20/21/22/24 ya están en percepciones; su desglose informativo no se suma de nuevo. Cero de provisión contable adicional no significa que no existan derechos laborales acumulados ni que los ahorros bancarios sean un gasto.

Los comprobantes estatales de peaje o calcomanía vehicular se excluyen de la lectura de ISN mediante identificación positiva de sus conceptos; no se presentan como documentos de nómina sin lectura. Los comprobantes de ISN y los casos desconocidos conservan su validación estricta.

El reparto anual verifica la historia del mes consultado. Si otro mes de la cobertura carece de historia, el lector conserva ese mes sin certificar, pero permite el importe mensual del mes que sí tiene reglas acreditadas; conserva el residuo de centavos en el último mes.

La tabla por sucursal y su CSV conservan nómina y vales separados y añaden las cargas mensuales e ISN del mismo grupo histórico de nómina; el porcentaje de costo usa las ventas de esa sucursal. Los cargos no se copian a otros grupos cuando una persona aparece en más de una quincena. El servicio de vales permanece en el total de empresa, sin asignación por sucursal inventada.

Para contratos con versiones superpuestas se elige la versión vigente más reciente, igual que la generación canónica de obligaciones. Una versión anterior sustituida no genera una segunda exigencia de gasto del mismo contrato y mes. Las obligaciones y gastos históricos existentes permanecen intactos.

Las facturas globales se conservan como un único gasto con su CFDI completo. Un contrato mensual sólo queda acreditado por sus partes repartidas cuando la regla es explícita, todas las partes tienen soporte disponible y la suma mensual coincide exactamente con la versión vigente inequívoca del mismo rubro, categoría y sucursal. No se satisfacen contratos distintos con una obligación ya enlazada. Dirección confirmó la décima licencia Point y su reinstalación en Sinaloa de Leyva; el detalle operativo y su auditoría permanecen en el ERP.

El consolidado de personal y gastos incluye los gastos indirectos de planta mediante una opción explícita del lector. El costeo por etapas conserva su comportamiento anterior. La nómina de producción, materia prima, empaque y otras partidas de fabricación permanecen excluidas de esta suma de gastos para evitar duplicidades.

La factura compartida de fumigación conserva un CFDI completo y su reparto explícito por rubro. Las categorías genéricas INDIRECTO_SUC e INDIRECTO_PROD distinguen el destino comercial o de planta; el mismo rubro exacto y el importe íntegro de la tarifa vigente permiten acreditar el contrato sin copiar el CFDI. No se extiende esta correspondencia a otras categorías.

El gasto de combustible se lee de CargaCombustibleUnidad mediante el conciliador existente: los anticipos, pagos y CFDI no se vuelven a sumar al consumo. Se admite el cruce documental o un reconocimiento del costo en la auditoría de la misma carga, vinculado a su ID, mes e importe. La lectura manual de un ticket conserva su hash y exige el archivo; el gasto sin ticket requiere confirmación explícita de Dirección y nunca cambia el resultado de la auditoría OCR. Un cambio de importe deja sin efecto el reconocimiento anterior. No se crea otra captura de combustible.

La auditoría automática mezcla sus campos nuevos con el JSON vigente en una actualización PostgreSQL atómica, conservando los reconocimientos manuales incluso si se agregaron mientras se leía el ticket. El filtro existente contra cambios concurrentes de importe, litros o foto se conserva.
