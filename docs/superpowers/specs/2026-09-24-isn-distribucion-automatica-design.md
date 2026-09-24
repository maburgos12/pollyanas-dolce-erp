# ISN automatico y distribucion de costo laboral

## Objetivo

Incorporar el Impuesto Sobre Nominas (ISN) de Sinaloa como costo real del periodo fiscal al que corresponde, aunque se pague en el mes siguiente. El importe corporativo debe aparecer una sola vez en Presupuesto vs Real y su desglose debe asignarse por empleado, area y sucursal sin duplicar el estado de resultados.

## Fuente y periodo

- La fuente canonica del importe pagado sera el CFDI recibido del Gobierno del Estado de Sinaloa (`RFC GES8101015I7`).
- El periodo se obtendra del identificador del concepto en formato `AAAAMM`, no de la fecha de expedicion o pago.
- El UUID del CFDI sera la llave de idempotencia. Un mismo comprobante no podra contabilizarse dos veces.
- Los CFDI cancelados o no vigentes no se consideraran.
- Si todavia no existe el XML descargado del SAT, el sistema mostrara el componente pendiente; el PDF no sustituira silenciosamente la fuente fiscal estructurada.

## Calculo fiscal e interno

La base gravada mensual se construira por empleado a partir de los conceptos de nomina importados. Se consideran gravadas las percepciones salvo los conceptos exentos identificados por la Ley de Hacienda del Estado de Sinaloa y configurados de forma explicita.

Para agosto de 2026, la primera configuracion excluira:

- prima vacacional;
- indemnizacion por terminacion o riesgo de trabajo;
- aguinaldo hasta el limite legal aplicable;
- despensa o prevision social solamente en la proporcion fiscalmente exenta;
- otros conceptos que posteriormente se clasifiquen expresamente como exentos.

La tarifa progresiva se calculara sobre la suma mensual empresarial, no empleado por empleado. Para distribuir el costo real pagado se usara:

```text
ISN empleado = base gravada empleado / base gravada total * ISN pagado
```

El redondeo se hara a centavos y el residuo se asignara de forma determinista para que la suma del desglose sea exactamente igual al CFDI. La distribucion no reemplaza la declaracion fiscal; es una asignacion interna de costo.

## Persistencia y auditoria

Se agregara un expediente mensual de ISN con:

- periodo fiscal;
- UUID y referencia al CFDI fuente;
- importe pagado;
- base gravada calculada;
- base declarada opcional cuando se disponga del preelaborado;
- diferencia entre base calculada y declarada;
- estado de conciliacion y marcas de tiempo.

Cada expediente conservara una linea por empleado con la base gravada, el ISN asignado y fotografias del area y sucursal usadas al momento del calculo. Esto preserva el costo historico aunque cambie posteriormente la ficha del empleado.

Las escrituras seran transaccionales e idempotentes. Una rematerializacion reemplazara solamente las lineas automaticas del mismo expediente; nunca modificara capturas manuales de otros conceptos ni datos de nomina fuente.

## Integracion con reportes

### Planeacion de personal

El componente ISN mensual se tomara del expediente aplicado. Mientras no exista, podra leerse directamente del CFDI vigente como control corporativo, pero no se presentara como conciliado hasta materializar el desglose.

### Presupuesto vs Real

- El renglon corporativo `Impuesto sobre nomina` conservara su presupuesto original.
- `monto_real` recibira el total del expediente aplicado mediante una fuente automatica identificable.
- El desglose por empleado, area y sucursal sera informativo y no creara renglones adicionales que sumen otra vez al estado de resultados.
- Si existe una captura manual, el consolidador no la pisara y reportara el conflicto para revision.

## Flujo operativo

1. La descarga SAT incorpora el CFDI recibido.
2. El servicio identifica emisor, concepto y periodo fiscal.
3. El servicio obtiene las dos nominas quincenales del mes y clasifica sus conceptos.
4. Se valida que todos los empleados tengan area y sucursal historicamente asignables.
5. Se calcula y persiste la distribucion en modo simulacion o aplicacion.
6. Se comprueba que la suma de lineas sea exactamente igual al ISN pagado.
7. El consolidador publica un unico monto real corporativo en Presupuesto vs Real.

## Manejo de errores

- Sin CFDI vigente: no se escribe un real automatico.
- Periodo ambiguo o ausente: el comprobante queda pendiente de revision.
- Nomina incompleta: no se aplica la distribucion.
- Empleado sin area o sucursal: no se aplica y se reporta el faltante.
- Diferencia material entre base declarada y calculada: se conserva el expediente como pendiente, sin presentarlo como conciliado.
- UUID repetido: la operacion es idempotente.

## Pruebas y criterios de aceptacion

- La tarifa progresiva produce el impuesto esperado en sus cuatro rangos.
- Los conceptos exentos no integran la base; los gravados si.
- El periodo proviene de `AAAAMM` y no de la fecha de pago.
- El prorrateo por empleado suma exactamente el importe pagado, incluso con redondeos.
- El detalle conserva area y sucursal del periodo.
- Reejecutar el proceso no duplica expedientes ni lineas.
- Una captura manual en Presupuesto vs Real queda protegida.
- Agosto de 2026 muestra presupuesto `$14,890.00`, real `$16,168.00` y variacion `$1,278.00`.
- Planeacion de personal y Presupuesto vs Real muestran el mismo total de ISN para agosto.
- La validacion final incluye pruebas automatizadas, `migrate --check`, `manage.py check` y verificacion autenticada en produccion.

## Fuera de alcance

- Modificar el presupuesto autorizado.
- Presentar o corregir declaraciones ante SATES.
- Inferir una base declarada como si fuera evidencia fiscal cuando no se tenga el preelaborado.
- Alterar importes fuente de nomina, salarios, bajas o movimientos de empleados.
