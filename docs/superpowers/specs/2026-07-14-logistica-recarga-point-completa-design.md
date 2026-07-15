# Sincronización completa de Point al regresar a CEDIS

## Objetivo

Evitar que un repartidor capture la recarga de un tramo con una fotografía obsoleta de Point. Al regresar a CEDIS, el ERP debe distinguir la presencia geográfica de la recarga operativa, consultar el estado completo de las transferencias, reconciliar el checklist y sólo entonces habilitar la carga del siguiente tramo.

## Incidente comprobado

En `RUT-202607-0025`, la geocerca registró la llegada a CEDIS y después marcó la parada como `VISITADA`, pero no se creó un evento `RECARGA_CEDIS` ni se llamó al endpoint de recarga. La PWA ocultó el botón porque trató la visita como una recarga resuelta.

El sincronizador de carga consultó únicamente transferencias abiertas. El folio de El Túnel dejó de aparecer en ese origen al pasar a Enviado/Recibido, por lo que el ERP conservó 53 solicitadas y cero enviadas. El comprobante de Point muestra el resultado verdadero: 53 solicitadas, 41 enviadas y 41 recibidas. Los renglones con Enviado igual a cero son resultados finales válidos y deben seguir visibles.

## Alternativas consideradas

1. Refrescar sólo la pantalla de carga. Se descarta porque seguiría consumiendo datos obsoletos del ERP.
2. Mantener un botón manual de recarga. Se conserva como recuperación, pero no como camino principal porque depende de que el repartidor detecte la falla.
3. Orquestar la recarga desde el backend al confirmar la permanencia en CEDIS. Es la opción elegida porque centraliza el contrato, funciona para cualquier cliente y permite auditar éxito, espera y falla.

## Contrato de estados

- `LLEGADA_GEOFENCE`: presencia confiable en CEDIS; no equivale a recarga.
- `VISITADA`: permanencia física comprobada; tampoco equivale a recarga.
- `RECARGA_CEDIS`: la sincronización completa terminó, el checklist fue reconciliado y el siguiente tramo puede capturarse.
- `PENDIENTE_ENVIADO`: Point todavía no confirma el cambio Solicitado a Enviado; se conserva la espera y se notifica al jefe.
- `ERROR_POINT`: Point no respondió o la extracción falló; se conserva la espera, se notifica al jefe y se ofrece reintento.

La fuente para la carga física es `sent_at` junto con `sent_quantity`. Un `sent_quantity` de cero con `sent_at` presente es un resultado final válido. `received_at`, `received_quantity` e `is_received` constituyen la conciliación posterior de recepción, pero no sustituyen por sí solos la confirmación explícita de entrega del repartidor.

## Flujo propuesto

1. El tracking registra una llegada confiable a CEDIS.
2. Cuando se cumple la permanencia y la parada pasa a `VISITADA`, se agenda una sola reconciliación automática para esa ruta y esa parada.
3. La reconciliación ejecuta la extracción completa de transferencias para las fechas operativas de la ruta. No usa exclusivamente el listado de transferencias abiertas.
4. Se actualizan las `PointTransferLine` existentes y después se reconstruye el checklist con `ejecutar_sync=False`, ya sobre la caché actualizada.
5. Si todas las sucursales del siguiente tramo tienen un hecho explícito de Enviado, se registra `RECARGA_CEDIS` de forma idempotente.
6. Si faltan hechos Enviado o Point falla, se registra una alerta revisable y se notifica a la jefatura. El siguiente tramo no se considera abierto.
7. La PWA consulta el estado y muestra `Sincronizando Point`, `Esperando Enviado` o `Error de Point`. El botón `Reintentar sincronización` permanece visible hasta existir `RECARGA_CEDIS`.

El endpoint manual reutiliza exactamente la misma orquestación. Debe aceptar una parada CEDIS ya `VISITADA`, pues la visita geográfica ocurre antes de la recarga. No debe duplicar eventos ni sincronizaciones exitosas.

## Consistencia del tramo

El tramo siguiente comienza después del último CEDIS con evento `RECARGA_CEDIS`, no después de una geocerca o de `VISITADA`. Así, una llegada física nunca habilita captura contra una fotografía vieja.

El checklist consolidará por producto únicamente las líneas activas del tramo. El desglose por sucursal seguirá siendo informativo. Las líneas `SUPERADA` permanecerán auditables y fuera de los totales activos.

## Recuperación del incidente actual

Después del despliegue se ejecutará una sincronización completa y una reconciliación controlada de `RUT-202607-0025`. Antes y después se leerán los folios y el checklist. Para El Túnel se debe observar 53 solicitadas, 41 enviadas y 41 recibidas; los productos enviados en cero deben conservarse visibles y no requerir captura. No se marcará la entrega física por inferencia: `Confirmar entrega` seguirá disponible mientras `entrega_estado=PENDIENTE`.

## Errores y observabilidad

- Cada intento automático tendrá una clave idempotente por ruta y parada CEDIS.
- Los fallos externos generarán un evento con el trabajo de Point, el estado y el detalle saneado.
- Una tarea repetida no duplicará `RECARGA_CEDIS`, alertas ni líneas del checklist.
- La PWA no enviará la recarga a la cola offline; mostrará que requiere conexión.
- La jefatura podrá reintentar o autorizar un snapshot únicamente mediante el flujo auditado existente.

## Pruebas TDD

1. Una llegada o una parada `VISITADA` no abre el siguiente tramo sin `RECARGA_CEDIS`.
2. La permanencia en CEDIS agenda una sola reconciliación automática.
3. La reconciliación completa actualiza una transferencia que desapareció del listado de abiertas al pasar a Enviado/Recibido.
4. Un folio con 53 solicitadas, 41 enviadas y 41 recibidas produce carga esperada 41 y conserva los Enviado cero.
5. El endpoint manual acepta CEDIS `VISITADA` y es idempotente.
6. Un fallo de Point deja la recarga pendiente, genera alerta y permite reintentar.
7. `Confirmar entrega` permanece visible para una sucursal visitada con entrega pendiente.
8. Las pruebas de rutas sin CEDIS intermedio, múltiples recargas, cola offline y cierre de ruta continúan pasando.

## Archivos previstos

- `logistica/services_carga_ruta.py`: contrato de tramo y sincronización completa.
- `logistica/services_rutas_control.py`: disparo idempotente tras permanencia en CEDIS.
- `logistica/tasks.py` o tarea equivalente: reconciliación automática fuera de la transacción de tracking.
- `api/logistica_views.py` y serializadores: estado de recarga y recuperación manual.
- `logistica/templates/logistica/pwa.html`: estados y botón persistente.
- `logistica/static/logistica/pwa/sw.js` y `logistica/checks.py`: versión de caché coordinada.
- `logistica/tests_invariantes_ruta.py`: regresiones del contrato completo.

## Despliegue y validación

El cambio seguirá rama limpia, pruebas focalizadas y suite de logística, `manage.py check`, `migrate --check`, PR, merge y `scripts/deploy_web_safe.sh` sin `git pull` manual previo. Al tocar la PWA se incrementará la versión del service worker y se ejecutará `collectstatic`.

La validación final se realizará en producción con la ruta afectada y en navegador real: respuesta de tracking, estado de recarga, carga del tramo, consola y solicitudes de red. La tarea no se considerará terminada hasta verificar los datos Point, el checklist y la pantalla usada por el repartidor.
