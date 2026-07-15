# Estabilización integral del flujo de rutas

## Objetivo

Evitar que una ruta dependa de correcciones manuales durante la jornada. El ERP debe conservar la verdad de Point, la bitácora, la geocerca y las autorizaciones, y debe impedir estados contradictorios entre esos cuatro orígenes.

## Alcance

La estabilización cubre cuatro contratos del backend y sus consumidores:

1. Solicitud Point y cantidad enviada que alimenta la carga del tramo.
2. Regreso a CEDIS y apertura de la carga del tramo siguiente.
3. Confirmación de entrega con o sin geocerca y continuidad operativa.
4. Liberación de ruta contra el turno y la unidad realmente activos.

No incluye cambios de datos históricos, migraciones, ajustes de `.env`, rediseños generales ni cambios a otros módulos.

## Decisión principal: solicitado no equivale a enviado en cero

La fecha `PointTransferLine.sent_at` o la bandera fuente `raw_payload.transfer.isEnviado=true` son la evidencia de que Point realizó la transición de **Solicitado** a **Enviado**. `sent_quantity` por sí solo no prueba la transición porque su valor por defecto también es cero.

- Sin `sent_at` ni `isEnviado=true`: la solicitud sigue pendiente de Enviado. La línea permanece pendiente y no puede convertirse en `ZERO_EXPECTED`.
- Con transición confirmada y `sent_quantity = 0`: Point confirmó Enviado en cero. La línea se conserva, queda visible y se resuelve como `ZERO_EXPECTED` sin pedir captura.
- Con transición confirmada y `sent_quantity > 0`: esa cantidad alimenta la carga física.
- Una cantidad enviada puede ser menor, igual o mayor que la solicitada.
- Un folio o detalle `SUPERADA` permanece auditable, pero queda fuera de la carga y sus totales.
- Folios distintos del mismo producto continúan siendo transferencias distintas.
- Una actualización construida sólo desde caché no modifica `sincronizado_en`; esa marca representa exclusivamente una consulta externa exitosa a Point.

## Recarga CEDIS

Al registrar el regreso a CEDIS, el backend debe ejecutar la sincronización de transferencias Point antes de marcar la parada CEDIS como visitada y antes de abrir el siguiente tramo.

El orden obligatorio es:

1. Identificar las sucursales comprendidas entre la parada CEDIS actual y la siguiente parada CEDIS, o el final de la ruta.
2. Ejecutar la sincronización Point.
3. Reconstruir o actualizar el checklist conservando capturas previas y la línea canónica de cada detalle Point.
4. Verificar que las solicitudes activas del siguiente tramo tengan transición a Enviado confirmada.
5. Si todas tienen transición a Enviado, marcar CEDIS visitada y mostrar la carga consolidada del tramo.
6. Si Point falla o quedan solicitudes sin transición, no usar el cero por defecto ni marcar la recarga como completada. Crear un evento de alerta y notificaciones para responsables con permiso de gestión logística.

La alerta debe indicar ruta, parada CEDIS, sucursales del tramo, causa, hora y actor. Repetir la misma petición no debe duplicar eventos ni notificaciones.

Un responsable de logística puede autorizar continuar con el último snapshot únicamente mediante una acción explícita y motivada. La autorización queda en `EventoRuta.metadata` con actor, fecha, motivo y referencias del snapshot. Nunca convierte solicitudes sin `sent_at` en Enviado cero.

## Entrega excepcional y continuidad

La presencia física y la resolución operativa son conceptos separados:

- `ParadaRuta.estado = VISITADA` sólo representa una visita sustentada por el flujo de ubicación existente.
- Una entrega confirmada fuera de geocerca conserva `entrega_estado`, evidencia, causa y revisión pendiente; no se inventan coordenadas ni una geocerca exitosa.
- El backend expone si la parada está `operativamente_resuelta`: entrega confirmada como entregada, con diferencia o no entregada, aunque la revisión administrativa siga pendiente.
- La PWA utiliza ese contrato del backend para la marca visual, la selección de la siguiente parada y la continuidad. No reinterpreta estados por su cuenta.
- La revisión administrativa permanece visible y auditable, pero no obliga al director a reparar la ruta durante la jornada.

## Turno, unidad y ruta

Una ruta sólo puede pasar a `EN_RUTA` cuando el turno abierto del repartidor corresponde a la misma unidad asignada a la ruta.

La validación será una función de dominio compartida por:

- liberación desde la bitácora/PWA;
- cambio administrativo de estatus;
- cualquier endpoint futuro que libere una ruta.

La liberación exitosa liga `ruta.bitacora_salida` al turno activo. Si no existe turno activo o la unidad no coincide, se rechaza sin modificar la ruta y se devuelve una causa estructurada.

## Idempotencia y auditoría

- Reintentar una recarga CEDIS ya completada devuelve el mismo evento.
- Reintentar una alerta Point para la misma parada y el mismo snapshot no duplica el evento ni las notificaciones.
- Una autorización sólo aplica a la parada CEDIS y snapshot para los que fue emitida.
- Cambios posteriores en Point invalidan cualquier autorización previa de cantidades.
- Las líneas canónicas se identifican por el detalle Point existente; un resync actualiza la línea, no crea otra.
- Una línea fusionada con un placeholder CEDIS queda reservada globalmente por `point_transfer_line_id`; no puede reaparecer en otra ruta aunque conserve un `source_hash` CEDIS.
- Una línea anterior sólo pasa a `SUPERADA` cuando pertenece al mismo folio, sucursal y producto, no fue validada, está sin transición o fue enviada explícitamente en cero, y aparece un detalle posterior con transición confirmada. Dos detalles positivos se conservan y suman; una sustitución ambigua genera incidencia en vez de borrar carga operativa.
- Toda excepción registra actor, fecha, causa, ruta, parada y datos comparados.

## Contratos de respuesta

La API de recarga CEDIS devuelve uno de estos resultados:

- `200`, `estado_sync=ACTUALIZADO`: recarga completada y tramo listo.
- `409`, `estado_sync=PENDIENTE_ENVIADO`: Point respondió, pero existen solicitudes sin transición a Enviado; incluye resumen y señala que se notificó al jefe.
- `503`, `estado_sync=ERROR_POINT`: Point no pudo sincronizar; conserva la parada sin completar e informa que se notificó al jefe.
- `200`, `estado_sync=AUTORIZADO`: un responsable autorizó continuar con el snapshot identificado.

La serialización de parada añade `operativamente_resuelta` sin cambiar el significado histórico de `estado` ni de la geocerca.

## Pruebas de aceptación

1. Solicitud 7, `sent_at=NULL`, `sent_quantity=0`: permanece pendiente de Enviado y bloquea la carga definitiva.
2. Solicitud 7, `sent_at` presente, `sent_quantity=0`: aparece en cero, no solicita captura y no desaparece de auditoría.
3. Solicitud 7, Enviado 5: la carga esperada es 5, no 7.
4. Enviado cambia de 5 a 8 después de una nueva sincronización: se actualiza la misma línea y se recalcula su diferencia sin duplicarla.
5. Un detalle nuevo del mismo folio y producto marca el anterior `SUPERADA`; folios diferentes no se fusionan.
6. Ruta `CEDIS -> Nío -> Payán -> CEDIS -> Las Glorias`: la primera carga consolida Nío y Payán; tras el regreso sólo abre Las Glorias.
7. Si Point falla en la segunda CEDIS, ésta no se completa, el siguiente tramo no usa datos viejos y se crea una única alerta.
8. Si Point responde sin que alguna solicitud haya pasado a Enviado, se solicita revisión al jefe sin convertirla en Enviado cero.
9. Entrega sin geocerca: conserva revisión pendiente y ausencia de geocerca, pero la parada queda operativamente resuelta y la ruta puede continuar.
10. Cambio administrativo a `EN_RUTA` con turno en otra unidad: se rechaza y no crea evento de salida.
11. Liberación correcta: enlaza la bitácora activa y crea una sola salida.
12. Recorrido integral con productos, pasteles y pays: cada tramo muestra una sola sumatoria por producto y ofrece el desglose por sucursal sin duplicar capturas.
13. Actualizar el checklist desde caché no cambia la fecha de la última sincronización externa.
14. Dos detalles positivos del mismo producto y folio permanecen independientes; una corrección cero→positivo deja sólo el detalle nuevo en operación y conserva el anterior en auditoría.
15. Después de una entrega excepcional, las posiciones confiables de la siguiente sucursal se asignan a esa siguiente parada y no quedan atrapadas en la parada ya resuelta.
16. Web, API y PWA aplican el mismo contrato al liberar y cerrar; ninguna superficie puede evitar la bitácora o la unidad activa.

## Validación y despliegue

La implementación requiere pruebas unitarias y de API, `manage.py check`, `migrate --check`, pruebas completas de `logistica` y validación en navegador real de la PWA. Después de revisión de Claude: commit quirúrgico, PR, deploy mediante `scripts/deploy_web_safe.sh`, validación en producción y actualización del service worker si se modifica la PWA.
