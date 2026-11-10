# Ficha de fuentes — actualización de carga Point por el repartidor

Fecha y ambientes consultados: 7 de noviembre de 2026. PostgreSQL local aislado para metadatos y casos sintéticos. Producción en solo lectura para tiempos de sincronización.

## Necesidad y unidad de análisis

El titular de una ruta planeada solicita al ERP consultar Point y actualizar la lista que debe confirmar físicamente. La asignación se decide por ruta; el dato de envío se identifica por detalle de transferencia y la captura física por línea de la lista de carga.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Asignación | `RutaEntrega` / `logistica_rutaentrega` | Planeación de Logística | ID de ruta, repartidor titular, fecha y paradas | La prueba local cubre titular, acompañante y reasignación | App de repartidor y control de rutas |
| Transferencia enviada | `PointTransferLine` / `pos_bridge_transfer_lines` | Point mediante el sincronizador de transferencias abiertas | ID de transferencia y detalle de Point, fecha y sucursal destino | La prueba local verifica líneas enviadas y pendientes | Lista de carga, recepción y trazabilidad de inventario |
| Lista y confirmación física | `RutaCargaChecklist` y `RutaCargaChecklistLinea` | Servicio existente de carga y captura por sucursal | Ruta, parada y detalle de transferencia | La prueba local confirma que actualizar la lista deja la cantidad física sin capturar y la ruta planeada | PWA, validación de salida y revisión de discrepancias |
| Solicitud y resultado | `PointSyncJob` / `pos_bridge_sync_jobs` | El ERP registra la solicitud y el trabajador ejecuta la consulta | Modo `logistica_carga_repartidor`, ruta y usuario solicitante | La prueba local comprueba una sola solicitud al pulsar dos veces. En producción, 167 consultas exitosas de transferencias abiertas en 14 días, de 23.5 s de mediana y 46.4 s de máximo por consulta | App, seguimiento de sincronización y diagnóstico operativo |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Enviado en Point y cargado físicamente | Distinta | El envío proviene de Point. La carga se confirma por sucursal en la app | Ninguna |
| Acompañante y repartidor titular | Distinta | Son asignaciones separadas de la misma ruta | Ninguna |
| Trabajo de actualización y evidencia de cierre de transferencias | Distinta | La solicitud usa un modo propio. Las consultas a Point conservan su modo, persistencia y evidencia habituales | Ninguna |

## Decisión de diseño

Reutilizar la consulta de transferencias abiertas y la construcción de la lista. El ERP verifica la asignación al solicitar y al aplicar el resultado. Point conserva la autoridad sobre el envío y la app conserva la captura física por separado. El registro de solicitud permite consultar el avance sin mantener abierta la conexión del celular.

Procedimiento reproducible: `manage.py inventario_fuentes_datos --term carga --term transfer --term sync --limit 28` en PostgreSQL local. La revisión de tiempos en producción filtró `PointSyncJob` por `mode=open_transfers`, estado exitoso y los 14 días anteriores.

La sincronización usa la cuenta Point compartida. Si está ocupada, la app informa que debe reintentar. La lista que se reconstruye corresponde a la ruta autorizada.

## Recarga en ruta

El caso RUT-202611-0009 del 9 de noviembre confirmó que la primera recarga proyectó cinco líneas de envío y que una sincronización posterior agregó 88 líneas en tres sucursales. La acción del titular identifica el tramo mediante la parada de recarga registrada en `EventoRuta`. Sólo reconstruye la lista de paradas abiertas de ese tramo. La sincronización de Point conserva su espejo compartido. Las líneas con captura o evidencia y las paradas cerradas permanecen protegidas, incluso si se genera una nueva versión del mismo detalle en Point.
