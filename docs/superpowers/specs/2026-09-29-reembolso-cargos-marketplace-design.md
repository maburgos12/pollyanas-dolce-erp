# Diseño: reembolsos separados con cargos adicionales de marketplace

## Problema y evidencia

La cancelación de un intento pagado limita actualmente el reembolso al importe del artículo. El caso SCD-2608-0002 demuestra que un marketplace puede devolver también cargos o ajustes asociados al pedido: el artículo está registrado en 199.56 y Mercado Pago documenta una devolución total de 318.56. La diferencia es 119.00. El comprobante no desglosa esa diferencia, por lo que el ERP no debe etiquetarla exclusivamente como envío.

Existe además otro reembolso, de 119.00, correspondiente a SCD-2609-0010 (Espátula Cuadrada). Su comprobante 40280662463 pertenece a otro pedido, con pago del 14/09/2026, solicitud de devolución del 17/09/2026 y un abono separado en American Express. La coincidencia entre esos 119.00 y la diferencia del primer caso no los convierte en la misma operación: ambos reembolsos deben registrarse por separado y nunca sumarse, compensarse ni relacionarse automáticamente.

## Alternativas consideradas

1. **Recomendada: cargo adicional documentado separado.** Mantener el importe real del producto, capturar el cargo adicional y conservar el reembolso total. Es auditable, evita alterar presupuesto y permite validar cada componente.
2. **Cambiar la compra a 318.56.** Es más simple, pero mezcla producto y cargos del marketplace, altera el compromiso y pierde el desglose. Se descarta.
3. **Permitir cualquier reembolso con archivo.** Evita una migración, pero elimina un límite financiero verificable y depende solo de revisión manual. Se descarta.

## Modelo y reglas

- Añadir a `IntentoCompraDepartamental` un decimal no negativo `reembolso_cargos_adicionales`, con valor predeterminado 0.00.
- `reembolso_solicitado` conserva el total solicitado o confirmado por el proveedor.
- La parte atribuible al producto se calcula como `reembolso_solicitado - reembolso_cargos_adicionales`.
- Reglas obligatorias:
  - total solicitado mayor que cero;
  - cargos adicionales entre 0 y el total solicitado;
  - parte del producto no mayor que `compra.importe_final`;
  - comprobante obligatorio cuando los cargos adicionales sean mayores que cero;
  - máximo dos decimales y fechas no futuras;
  - la validación vive en formulario y servicio; el servicio es la defensa autoritativa.
- `ReembolsoCompraDepartamental` permanece sin cambios: sus abonos no pueden superar el saldo total solicitado.
- La corrección posterior de una compra no podrá dejar el importe del producto por debajo de la parte del producto ya solicitada.

## Experiencia de captura

En la cancelación de una compra pagada se mostrarán:

- `Importe total solicitado o confirmado`;
- `Cargos adicionales documentados`, con ayuda: envío, ajuste u otro cargo que el proveedor incluyó en la devolución;
- el importe pagado del producto como referencia;
- un resumen legible del máximo: producto pagado + cargos documentados;
- comprobante obligatorio únicamente cuando se capturen cargos adicionales.

Los errores se mostrarán junto al campo correspondiente. El flujo mantiene el botón y la respuesta asíncrona existentes, conserva la posición y no agrega un modal. La línea de tiempo mostrará el total y el desglose cuando exista.

## Flujos operativos posteriores al despliegue

Después del despliegue y la validación visual:

### Caso 1: Brach's Candy Corn Mix

1. Cancelar el intento 5, folio SCD-2608-0002, por proveedor canceló.
2. Capturar fecha de solicitud 20/09/2026.
3. Capturar total 318.56 y cargos adicionales documentados 119.00.
4. Adjuntar `refund_voucher_40281718195.pdf` como evidencia.
5. Registrar el reembolso recibido el 20/09/2026 por 318.56 con el mismo comprobante y la referencia visible en el estado de cuenta.
6. Verificar que el intento quede reembolsado, con saldo cero y la compra histórica visible.

### Caso 2: Espátula Cuadrada

1. Cancelar el intento 13, folio SCD-2609-0010, por proveedor canceló.
2. Capturar fecha de solicitud 17/09/2026.
3. Capturar total 119.00 y cargos adicionales 0.00, porque el reembolso coincide con el importe pagado de este producto.
4. Adjuntar `refund_voucher_40280662463.pdf` como evidencia.
5. Registrar un reembolso separado por 119.00. La fecha recomendada en el ERP es 18/09/2026, cuando American Express indica que se procesó; la referencia conservará que la transacción se muestra con fecha 17/09/2026.
6. Verificar que este segundo intento también quede reembolsado, con saldo cero y sin alterar el primero.

Las referencias bancarias completas se almacenan únicamente en los registros operativos, no en la documentación del repositorio. Registrar estos abonos no crea por sí solo una conciliación bancaria.

## Consumidores afectados

- Formulario y vista de cancelación de intento.
- Servicio transaccional de cancelación.
- Corrección trazable de compra realizada.
- Línea de tiempo y resumen de compras departamentales.
- Migración de Compras y pruebas del módulo.

No cambian API pública, presupuesto histórico, cotización, conciliación bancaria ni modelos de proveedores.

## Pruebas y aceptación

- El caso 199.56 + 119.00 = 318.56 es válido con comprobante.
- El caso 119.00 + 0.00 = 119.00 conserva el flujo normal y queda asociado exclusivamente al intento 13.
- Los dos comprobantes generan dos solicitudes y dos reembolsos recibidos independientes; ningún monto se agrega o reutiliza entre intentos.
- El mismo caso sin comprobante es rechazado sin mutar datos.
- La parte del producto mayor que 199.56 es rechazada aunque existan cargos.
- Cargos mayores que el total, negativos o con fracciones de centavo son rechazados.
- Los flujos sin cargos adicionales conservan el límite anterior y no exigen comprobante.
- Los servicios rechazan una llamada manipulada aunque se omita el formulario.
- Los resúmenes no duplican el cargo ni cambian el total recibido.
- Migraciones, checks, suite de Compras, CI y validación autenticada en producción quedan en verde.
- La lectura final de producción confirma, por separado: intento 5 con total 318.56, cargos 119.00 y saldo cero; intento 13 con total 119.00, cargos 0.00 y saldo cero; cada uno con su propia evidencia.

## Despliegue y reversibilidad

La migración es aditiva y usa 0.00 como valor seguro para registros existentes. El despliegue seguirá el script oficial. No se eliminarán archivos ni registros. Si la validación de producción falla, no se registrará el caso operativo hasta corregir el código; el intento permanecerá vigente.
