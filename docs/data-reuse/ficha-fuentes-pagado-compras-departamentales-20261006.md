# Ficha de fuentes — pagado en compras departamentales

Fecha y ambiente consultado: 2026-10-06; PostgreSQL local aislado y producción (solo lectura).

## Necesidad y unidad de análisis

Mostrar el importe pagado registrado en el detalle de una solicitud departamental. La unidad es una compra realizada por intento de un artículo. El total es la suma de `importe_final` de todas las compras de sus artículos, incluidas las históricas; los reembolsos recibidos se muestran por separado. Esto no equivale al gasto contable reconocido.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Pago de compra | `compras.CompraRealizadaDepartamental` / `compras_comprarealizadadepartamental` | Registro y corrección de compra departamental | `id`; `intento_id` único; `item_id` | Producción: 17 compras; solicitud 21: 2 compras por $707.50 | Detalle de solicitud, avisos, seguimiento de intentos |
| Reembolso recibido | `compras.ReembolsoCompraDepartamental` / `compras_reembolsocompradepartamental` | Registro de reembolso recibido | `id`, `intento_id` | Producción: 2 reembolsos | Historial del intento y bandeja |
| Compromiso | `compras.CompromisoCompraDepartamental` / `compras_compromisocompradepartamental` | Selección de cotización, orden y compra | `id`, `intento_id` | La solicitud 21 muestra $707.50 comprometidos | Presupuesto y detalle de solicitud |
| Gasto contable | `ItemCompraDepartamental.monto_gastado` / `compras_itemcompradepartamental` | Fuente financiera aún no integrada a este campo | `item_id` | Producción: 0 artículos con valor distinto de cero | Tarjeta anterior “Gastado” del detalle |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Compra realizada / pagado registrado | Confirmada para este indicador | `CompraRealizadaDepartamental.importe_final` guarda el importe final y admite corrección trazable; una cotización sin compra no entra | No |
| Comprometido / pagado | Distinta | Una orden puede existir sin pago y una compra registra el importe final | No |
| Pagado / gasto contable | Distinta | La solicitud 21 tiene $707.50 pagados y `monto_gastado=0`; los bienes pueden requerir otra clasificación contable | No |
| Reembolso solicitado / reembolso recibido | Distinta | Solo `ReembolsoCompraDepartamental.importe` confirma devolución recibida | No |

## Decisión de diseño

Reutilizar compras realizadas ya precargadas en `departamental_detalle`; sumar su importe final sin crear captura ni columna nueva. Sustituir la tarjeta engañosa “Gastado” por “Pagado registrado” e indicar que los reembolsos se consultan aparte y el gasto contable sigue pendiente de integración. No modificar presupuesto, compromiso, órdenes ni contabilidad.

Consultas reproducibles: `python3 manage.py inventario_fuentes_datos --term compra --term pago --term reembolso` con PostgreSQL configurado; en producción, conteos acotados de las tres tablas y suma de compras de la solicitud 21.

Riesgos y pendientes: los pagos históricos cancelados siguen dentro del pagado bruto; los reembolsos recibidos permanecen separados. La integración contable requiere su propio contrato de fuente y reconciliación.
