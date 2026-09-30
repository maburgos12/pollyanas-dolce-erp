# Ficha de fuentes - Reembolso con cargos de marketplace

Fecha y ambiente consultado: 2026-09-29; PostgreSQL local para metadatos y producción en modo de solo lectura para evidencia acotada.

## Necesidad y unidad de análisis

Compras departamentales necesita registrar un reembolso asociado a un intento de compra pagado cuando el proveedor o marketplace devuelve, además del producto, cargos posteriores documentados como envío o ajustes del pedido. La unidad de análisis es un intento de compra; cada solicitud y cada reembolso recibido pertenecen a un solo intento y continúan siendo eventos separados e inmutables.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Importe realmente pagado por el artículo | `compras.CompraRealizadaDepartamental` / `compras_comprarealizadadepartamental` | Flujo de registro/corrección de compra | `intento_id` único | Intento 5: `importe_final=199.56`; intento 13: `importe_final=119.00` | Compromiso presupuestal, detalle y corrección de compra, cancelación/reembolso |
| Solicitud de reembolso | `compras.IntentoCompraDepartamental` / `compras_intentocompradepartamental` | Cancelación de intento pagado | `item_id + numero`; un intento vigente por artículo | 15 intentos; ninguno con solicitud de reembolso antes de este caso | Bandeja, resumen, línea de tiempo, registro de reembolso recibido |
| Reembolso recibido | `compras.ReembolsoCompraDepartamental` / `compras_reembolsocompradepartamental` | Registro posterior de abonos recibidos | `id`; relación `intento_id` | Sin registros antes de este caso | Saldo pendiente, resumen, línea de tiempo |
| Envío cotizado | `compras.CotizacionCompraDepartamental.envio` | Captura de cotización previa a la compra | `cotizacion_id` | Una cotización con envío positivo; el caso intento 5 tiene `envio=0.00` | Total estimado de adquisición y selección de cotización |
| Reembolso documentado por marketplace | Comprobante externo asociado al intento/reembolso | Mercado Pago y operador de Compras | Número de comprobante externo, intento y archivo | 40281718195: total 318.56, solicitud y procesamiento 20/09/2026. 40280662463: total 119.00, solicitud 17/09/2026; American Express lo muestra como transacción del 17/09 y procesada el 18/09 | Cancelación auditada y registro de cada abono recibido |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| `envio` de cotización = cargo adicional reembolsado | Distinta | La cotización es previa y estimada; en el caso real vale 0 aunque la devolución excede el producto en 119.00 | No reutilizar como monto real |
| `importe_final` = precio del producto pagado | Confirmada para este flujo | El formulario lo define como importe realmente pagado y el caso conserva 199.56 | Mantener como fuente de la parte del producto |
| Diferencia documentada = envío/ajuste del marketplace | Confirmada como cargo adicional; desglose interno no resuelto | 318.56 - 199.56 = 119.00; el comprobante no desglosa por concepto | Etiquetar de forma amplia como cargo adicional documentado, no afirmar que todo es envío |
| Solicitud confirmada por plataforma = abono bancario observado | Confirmada para este caso | El comprobante de Mercado Pago y el estado de cuenta coinciden en monto; el estado de cuenta muestra procesamiento el 20/09/2026 | Registrar recepción el 20/09/2026 sin crear una conciliación bancaria automática |
| Diferencia de 119.00 del primer reembolso = segundo reembolso de 119.00 | Distinta | Los comprobantes tienen números, pedidos, fechas de pago y fechas de devolución diferentes; American Express presenta dos abonos separados | Nunca relacionarlos ni compensarlos automáticamente aunque coincida el monto |
| Fecha visible 17/09 = fecha procesada 18/09 en el segundo abono | Dos hitos del mismo abono | American Express muestra ambos datos en la misma operación | Usar 18/09/2026 como fecha recibida recomendada y conservar 17/09/2026 en la referencia operativa |

## Decisión de diseño

Extender `IntentoCompraDepartamental` con un monto de cargos adicionales documentados. No crear otra entidad ni reutilizar el envío cotizado. `reembolso_solicitado` seguirá siendo el total; la parte del producto será `total - cargos_adicionales` y no podrá superar `CompraRealizadaDepartamental.importe_final`. Si los cargos adicionales son mayores que cero, el comprobante será obligatorio. Los eventos y resúmenes conservarán el total solicitado/recibido y mostrarán el desglose para auditoría. Los intentos 5 y 13 se capturarán como operaciones independientes: el segundo no requiere cargos adicionales.

Consultas o procedimiento reproducible:

- `python3 manage.py inventario_fuentes_datos --term reembolso`
- `python3 manage.py inventario_fuentes_datos --term envio`
- `python3 manage.py inventario_fuentes_datos --term "cargos adicionales"`
- Conteos acotados de los modelos de Compras en producción y lectura de los intentos 5 y 13.

Riesgos y pendientes:

- El comprobante prueba el total devuelto, pero no identifica si los 119.00 corresponden íntegramente a envío; la etiqueta debe permanecer neutral.
- El segundo comprobante también es por 119.00, pero corresponde a otro producto y pedido; la coincidencia numérica no prueba relación contable con la diferencia del primer caso.
- No se debe sumar automáticamente el envío cotizado ni modificar el importe de la compra.
- Registrar un reembolso recibido no equivale a conciliación bancaria; se conservará la referencia documental.
