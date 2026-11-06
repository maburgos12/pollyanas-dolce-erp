# Pago de convenio por fecha bancaria

Necesidad: reconocer el neto realmente entregado por convenio en su mes autorizado, sin volver a sumar la nómina fiscal de otro mes. Unidad: una línea de nómina conciliada contra un pago acreditado.

## Fuentes verificadas

- `rrhh.NominaLinea` conserva empleado, adscripción, periodo, bruto y neto fiscales. No se mueve ni recalcula.
- `rrhh.NominaImportacion.resumen` ya conserva criterios y conciliaciones. Se extiende con `conciliaciones_pagos_reales`: lista de referencias a línea y soporte, estado APLICADO, criterio PAGO_REAL_CONFIRMADO_DG, autorización y cifras fiscales esperadas para detectar cambios. No es otra nómina ni una captura del pago.
- `core.AuditLog` ya contiene la observación documental y las rutas, páginas y SHA-256 de los originales cotejados. Se reutilizan `importe_bancario` y `fecha_bancaria` del soporte existente. No se crea un registro bancario, otra importación o gasto para ejecutar esta imputación.
- `syncfy_client.MovimientoBancario` e `ImportacionBancaria` fueron candidatos; no se necesitan para volver a acreditar el documento ya verificado y no se inventa una conciliación fiscal bancaria.
- `LineaPresupuestoMensual` es la fuente consumida por Estado de resultados y Rentabilidad 2026. Septiembre tenía una conciliación manual protegida; se conserva su base y se agrega una sola vez el componente autorizado. Las consolidaciones automáticas futuras excluyen el bruto del mes fiscal y agregan el neto del mes bancario. El área de control `nomina` conserva el bruto fiscal.

Evidencia de producción, consulta de solo lectura del 6 de noviembre de 2026: línea 1518, empleado 48/código 307; importación 60/periodo 24; AuditLog 43111 con tres originales; LP 12725 septiembre 115183.59; LP 12726 octubre 159669.92 tras reconsolidación; control fiscal 13218 octubre 707714.92. Ningún control de pago real previo. Inventario léxico ejecutado en PostgreSQL; los 34 candidatos documentados en la investigación anterior y los registros exactos permiten reutilizar estas fuentes.

## Decisión y autorización

Mauricio autorizó: “Procede a aplicar los $21,000 en septiembre sin duplicarlos en octubre y termina el reporte completo en PDF.” Identidad del destinatario corroborada con la misma cuenta en dos dispersiones originales y el cargo del estado BBVA, sin crear alias ni cambiar RFC/NSS. El pago es distinto del fondeo entre cuentas propias del 11 de septiembre. No se acredita otro pago de 9249.33 ni un entero de ISR por esta operación.

Contrato: el lector compartido valida unicidad de línea, pertenencia a importación, estatus de nómina, identidad del soporte y bruto/neto sin cambios. Produce +neto en el mes bancario y -bruto en el mes fiscal; ambos deltas se combinan si coinciden los meses. No convierte neto en bruto, no mueve cargas/ISN y conserva la diferencia fiscal como contraste no sumable. Los controles inválidos bloquean la consolidación de su rubro y se muestran como observación en el reporte.

Consumidores: consolidación financiera NOMINA, listas de raya (sin alterar sus totales fiscales), sucursales, departamentos, resumen global, pantalla, CSV y PDF generado con esas fuentes. Estado de resultados/Rentabilidad siguen leyendo las mismas LP. Sin modelos, migraciones, segundas capturas ni nuevos permisos.

## Validación

Pruebas: neto en septiembre una sola vez tras repetir consolidación; exclusión del bruto en octubre; control fiscal intacto; protección MANUAL; otros campos monetarios intactos; contraste fiscal separado; mismo mes; fuente inválida y duplicado rechazados; pantalla/CSV sin cuentas bancarias. Verificación posterior en producción y revisión visual de PDF obligatorias.
