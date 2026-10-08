# Ficha de fuentes — pedidos especiales Point y WEB

Fecha y ambiente: 2026-10-08; consultas de solo lectura en producción ERP y Point.

## Necesidad y unidad de análisis
Un PedidoCliente y una SolicitudDomicilio por compra WEB. Una relación explícita con el documento Point que respalda la compra: nota inmediata o pedido especial programado. La nota de venta final se incorpora al mismo pedido.

## Fuentes candidatas
| Concepto | Modelo / tabla | Fuente oficial | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Compra WEB | orders, OmnichannelSyncAttempt | e-commerce | order_number / external_source + external_id | dos excepciones pagadas; snapshots rechazados por precisión GPS | operaciones, cliente, ERP |
| Pedido canónico y domicilio | crm.PedidoCliente, logistica.SolicitudDomicilio | ERP | external_source + external_id, pedido FK | contratos existentes e idempotencia | reparto, seguimiento |
| Pedido programado | Point /Pedidos/getPedidos y /Pedidos/GetById | Point | PK_Pedido global; LPK_Pedido + sucursal es folio visible | 17114 / 00426 y 17118 / 00428; liquidados, fechas y totales verificados | producción, entrega, venta |
| Nota final | Point /Report/NotasByPlaza y detalle nota | Point | PK_NOTA; LPK_Nota del encabezado especial + sucursal | pedidos vendidos devuelven LPK_Nota; detalle especial no lo devuelve | facturación y cliente |

## Alias y equivalencias
| Identificadores | Estado | Evidencia | Revisión |
| --- | --- | --- | --- |
| PK_Pedido / LPK_Pedido / PK_NOTA | distintos | claves internas y folios visibles de documentos diferentes | conservar namespaces |
| WEB PD-261005-5GO80M / especial 17114 | confirmada por usuario y documentos | folio 00426, Matriz, entrega 8 octubre, total 254 | instrucción del usuario: hagamos esos ajustes |
| WEB PD-261006-GGKMD2 / especial 17118 | confirmada por usuario y documentos | folio 00428, Matriz, entrega 9 octubre, total 540 | instrucción del usuario: hagamos esos ajustes |
| Teléfono WEB y teléfono Point | distintos cuando difieran | discrepancia observada en primer pedido | no sustituir teléfono WEB |

## Decisión de diseño
Reutilizar PedidoCliente, SolicitudDomicilio, correo del servidor y backfill de procedencia de nota. Añadir una relación documental única, sin crear otra compra ni dirección. Vinculación explícita por operador; validar total y fecha, nunca unir por nombre/importe únicamente. Buscar pedidos especiales por fecha de entrega, permitiendo fechas futuras. Refrescar nota final antes de crear domicilios automáticos para impedir duplicados.

Procedimiento: inventario_fuentes_datos --term pedido --term domicilio --term point; consultas Point acotadas por fecha de entrega; GetById para claves confirmadas; consultar dos especiales vendidos para comprobar LPK_Nota. No ejecutar cambiarStatus ni recapturar en Point.

Riesgos: un folio especial aún no es una nota de venta; el correo del folio final depende de que Point publique esa nota. Conflictos con notas ya vinculadas requieren revisión, nunca fusión automática.
