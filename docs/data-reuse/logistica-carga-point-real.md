# Ficha de fuentes — actualización de carga Point

Fecha y ambientes consultados 23 de noviembre de 2026, producción en lectura y PostgreSQL 16 aislado.

## Necesidad y unidad de análisis

Actualizar el estado de cada detalle de transferencia Point y las líneas aún capturables del tramo vigente. Detectar solicitudes que fallan o dejan la carga bloqueada y alertar mediante el monitor existente.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Transferencia y recepción | PointTransferLine | Point, transfer_extractor y movement_sync_service | Folio, detalle y source_hash, sucursal y fecha | Folio 41769 recibido en Point real y todavía abierto en el espejo al auditar | Logística, inventario, trazabilidad |
| Captura física | RutaCargaChecklistLinea | App del repartidor | Ruta, parada y línea | Ruta 294 con 52 líneas en paradas posteriores al regreso a CEDIS | Carga de ruta y entrega |
| Tramo operativo | ParadaRuta y EventoRuta | Planeación y recarga CEDIS | Ruta, orden y parada con evento de recarga | Ruta 294 separa Leyva del siguiente tramo | ERP, PWA |
| Resultado de actualización | PointSyncJob | API y worker logistica_point | Job, actor, ruta y recarga | 17 resultados SIN_CARGA en ruta 294, registrados SUCCESS | App, monitor de carga |
| Entrega de alerta | delivery.json del monitor | carga_point_alerts.py y Telegram | Ruta, tramo, fecha y estado | Timer activo cada minuto, inicialmente reportaba cero solicitudes vencidas | Supervisión operativa |

## Alias y equivalencias

| Términos | Estado | Evidencia | Revisión requerida |
| --- | --- | --- | --- |
| Folio Point y transferencia ERP | Confirmada | Mismo identificador externo 41769 en consulta directa y espejo | Se conserva el vínculo existente |
| Actualización técnica y carga disponible | Distintas | SUCCESS puede contener SIN_CARGA o PENDIENTE_ENVIO | Clasificar el resultado funcional para alertar |
| Recepción Point y entrega física capturada | Distintas | Fuentes y eventos diferentes | La actualización sólo materializa el espejo y la carga capturable |

## Decisión de diseño

Reutilizar PointTransferLine, PointSyncJob, los límites de tramo y la protección de líneas capturadas. Consultar también las recibidas dentro del rango de fechas y mantener el manifiesto de abiertas con su contenido propio. La actualización no aplica movimientos de inventario. Ampliar la selección de fallos del monitor existente y deduplicar por ruta, recarga, día y estado.

El inventario local se ejecutó con los términos carga, transferencia y notificacion. Reportó 16 modelos candidatos. Las consultas productivas se limitaron a la ruta 294, sus paradas, los jobs recientes y el folio 41769. Las coincidencias de nombres se conservan como candidatas, sin introducir equivalencias nuevas.

## Riesgos y validación

Probar borradores editados durante la consulta, preservación de capturas y entregas, exclusión de otros días y rutas, transición de abierta a recibida y reintentos de alertas. Verificar el código y caché servidos, el resultado real de Point y recibos de alertas operativas tras publicar.
