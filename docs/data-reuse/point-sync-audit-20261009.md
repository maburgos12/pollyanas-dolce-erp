# Ficha de fuentes — auditoría sincronización domicilios Point

Fecha y ambiente: 2026-10-09; VPS producción, consultas acotadas de solo lectura; pruebas PostgreSQL16 aisladas.

## Necesidad y unidad de análisis
Una nota Point con servicio a domicilio, su pedido ERP y su única solicitud logística. Conservar identidad y recuperar únicamente capturas manuales auditadas ante errores de la API externa.

## Fuentes candidatas
| Concepto | Modelo / tabla | Fuente creadora y actualizadora | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Nota de venta | PointNote / Point API | Point | PK_Nota; folio, sucursal, fecha | Bandeja 7 días: 14 notas, 13 ya vinculadas; nota912802 folio16462 tiene detalle válido y error en cliente de domicilio | ERP CRM, logística, centro operativo |
| Pedido | crm.PedidoCliente | link_point_note, captura pendiente existente | point_note_id único; external_source/external_id | 144 pedidos POINT y 2 WEB; no duplicados de nota ni solicitudes pendientes con nota final | CRM, API omnicanal |
| Captura auditada pendiente | PedidoCliente.payload_snapshot.point_pending | Operador y servicio existente | folio+sucursal+fecha, fingerprint; propietario API | Sin registros POINT_PENDING en consulta de producción | Reconciliación automática y manual |
| Domicilio | logistica.SolicitudDomicilio | Flujo canónico ERP | pedido_cliente único | Nota912802 aún no tiene pedido ni solicitud | Preparación y reparto |
| Intento y diagnóstico | pos_bridge.PointSyncJob, PointExtractionLog | Job automático | job.id, nota, sucursal, código seguro | PARTIAL repetido: seen14/existing13/failed1; anteriormente sin logs por nota | Salud Point y auditoría ERP |

## Alias y equivalencias
PK_Nota con PedidoCliente.point_note_id: confirmada por identificador explícito. Captura POINT_PENDING con nota: regla existente folio+sucursal+fecha y fingerprint, sin nueva equivalencia. Cliente de domicilio con FK_Cliente de cabecera: confirmar igualdad si ambos existen; rechazar diferencias. Dirección general del catálogo y dirección de entrega de una nota son distintas; no usar catálogo como fallback.

## Decisión de diseño
Reutilizar pedidos, capturas auditadas y logs existentes. Sin nuevas tablas, migraciones o cambios del contrato público. Reconciliar estados por la transición compartida para generar una sola revisión y evento de auditoría. Guardar folio/nota/sucursal/código en PointExtractionLog, sin nombres, dirección, teléfono, credenciales o excepción cruda. Cuando Point falla en cliente de domicilio pero la nota está íntegramente validada, permitir reconciliar exclusivamente una captura manual existente mediante el mismo servicio, locks, fingerprint y restricciones. Sin esa captura, mantener fallo y no fabricar un pedido incompleto.

Procedimiento: inventario_fuentes_datos --term domicilio --term point --term solicitud; consultas de PointSyncJob recientes, agrupación de fuentes de pedidos, duplicados de point_note_id, pendientes con nota; consulta Point bandeja, detalle y cliente para nota912802 bajo candado de cuenta. Cobertura operativa automática actual: últimos7 días,11 sucursales canónicas activas; no se afirma auditoría de todo el histórico de ventas.

Riesgos: Point devuelve hasError=true para domicilio912802; falta respuesta válida de entrega o captura manual autorizada con dirección real. No eliminar aviso ni usar una dirección supuesta. El cliente frontend sigue mostrando el conteo de errores, mientras el job ERP conserva el diagnóstico individual.
