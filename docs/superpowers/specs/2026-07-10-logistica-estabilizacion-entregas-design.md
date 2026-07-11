# Estabilización de entregas y geocercas — Diseño

## Objetivo

Evitar que una sincronización, un ajuste administrativo o una falla de GPS fabrique visitas físicas o convierta silenciosamente una parada en una entrega normal. El repartidor debe poder continuar durante una falla técnica, pero toda entrega sin geocerca confiable debe quedar visible, auditada y pendiente de revisión por un jefe.

## Principios no negociables

1. La presencia física solo la acredita el seguimiento GPS/geocerca.
2. La entrega solo la acredita una acción explícita del repartidor o un ajuste administrativo identificado.
3. Point aporta cantidades y recepción administrativa; nunca visita, entrega física, horas ni confirmador.
4. Una falla de geolocalización no bloquea al repartidor: registra la entrega como excepción y genera revisión.
5. Ninguna excepción inventa `VISITADA`, `hora_llegada_real` ni un evento de geocerca.
6. Ninguna automatización autoriza, rechaza o reescribe hechos históricos; detecta y alerta de forma idempotente.
7. Toda mutación crítica conserva origen, actor, motivo, identificador idempotente y fecha.

## Modelo de estado

`ParadaRuta.estado` continúa representando únicamente el estado físico: `PENDIENTE` o `VISITADA`.

`ParadaRuta.entrega_estado` continúa representando el hecho capturado: `PENDIENTE`, `ENTREGADA`, `CON_DIFERENCIA` o `NO_ENTREGADA`.

Se agrega una dimensión independiente de revisión:

- `NO_REQUERIDA`: la confirmación tuvo geocerca válida.
- `PENDIENTE`: la entrega pudo registrarse, pero la ubicación no acredita presencia.
- `AUTORIZADA`: un jefe revisó y aceptó la excepción.
- `RECHAZADA`: un jefe revisó y rechazó la excepción; la evidencia original se conserva y se genera una acción correctiva.

La revisión guarda causa, motivo del repartidor, coordenadas, precisión y distancia disponibles, usuario revisor, motivo de resolución y fecha de resolución. La entrega y su revisión no se mezclan en un solo estado.

## Servicio único de dominio

Todos los escritores productivos deben pasar por un servicio transaccional con bloqueo de fila e idempotencia:

- `registrar_llegada_geocerca`: única operación que acredita llegada/visita de sucursal.
- `confirmar_entrega`: confirma normalmente cuando existe llegada confiable.
- `registrar_entrega_excepcional`: conserva la visita física intacta, registra entrega y abre revisión.
- `revisar_entrega_excepcional`: autoriza o rechaza con permisos y motivo obligatorio.
- `registrar_recarga_cedis`: excepción explícita del dominio CEDIS, sin reutilizar semántica de sucursal.

La API PWA, el ajuste ERP, el replay offline y las automatizaciones usarán estas operaciones. Los campos críticos quedarán de solo lectura en Django Admin. Point seguirá creando evidencia administrativa sin llamar operaciones físicas.

## Flujo del repartidor

### Geocerca válida

La ubicación confiable dentro del radio crea la llegada; al cumplirse la permanencia marca la visita. El botón registra la entrega normal con revisión `NO_REQUERIDA`.

### Sin geocerca válida

El botón no se bloquea. La app solicita motivo y muestra antes de confirmar:

> Estás fuera de la sucursal o no pudimos validar tu ubicación. La entrega se registrará, pero será revisada por tu jefe.

La respuesta vuelve con la entrega registrada y un aviso visible. La parada permanece físicamente `PENDIENTE`, sin hora de llegada fabricada, y la revisión queda `PENDIENTE`. Se guarda la causa concreta: fuera de radio, GPS denegado, sin señal, precisión insuficiente, ubicación tardía, salto imposible o sucursal sin coordenadas.

El recorrido puede continuar y finalizar operativamente aunque existan revisiones abiertas.

## Flujo del jefe

Un panel muestra las revisiones pendientes con ruta, sucursal, repartidor, hora, causa, motivo, coordenadas, precisión, distancia y evidencias. Solo usuarios con permiso explícito pueden resolverlas.

- Autorizar conserva la entrega, registra revisor, fecha y motivo, y no modifica la visita física.
- Rechazar conserva el hecho y sus evidencias, registra la resolución y crea una acción correctiva; no borra ni reabre silenciosamente.

El cierre operativo de la ruta no bloquea al repartidor. El cierre administrativo/auditado permanece pendiente mientras existan revisiones sin resolver o rechazadas sin corrección.

## Eventos, evidencia y alertas

Se crean tipos distintos para llegada por geocerca, entrega normal, entrega excepcional, autorización, rechazo e inconsistencia detectada. Un evento de llegada será válido únicamente si contiene la evidencia GPS requerida y fue producido por el servicio de geocerca.

La evidencia de Point tendrá procedencia inequívoca y ningún consumidor podrá interpretarla como confirmación física solo por su tipo genérico.

La alerta excepcional es única por parada y confirmación. Un replay idéntico devuelve el resultado original; el mismo identificador con payload distinto responde conflicto y no sobrescribe.

## Automatización de vigilancia

Un comando idempotente, programable desde Celery Beat, inspecciona como mínimo:

- entrega sin geocerca ni revisión;
- visita sin evento GPS válido;
- entrega atribuida a sincronización o usuario no autorizado;
- horas físicas derivadas de Point o ajuste administrativo;
- eventos de geocerca sin coordenadas/evidencia;
- confirmaciones duplicadas o incompatibles;
- distancia o salto imposibles;
- revisión pendiente sin alerta.

La automatización crea o actualiza alertas y métricas, pero nunca corrige registros históricos automáticamente. Debe poder ejecutarse en modo diagnóstico para revisar producción antes de habilitar la periodicidad.

## Cantidades y Point

- `solicitado` y `enviado` pueden aumentar, disminuir o llegar a cero.
- Una línea con `enviado = 0` permanece visible como valor final y no solicita captura positiva.
- `enviado = 0` no bloquea carga, salida, entrega ni cierre.
- Si cambia después de una carga previa, se conserva historial y se alerta si la ruta ya salió.
- Point recibido, corregido o revertido actualiza evidencia administrativa y conciliación, nunca estados físicos ni de revisión.

## Fecha operativa y CEDIS

La misma regla que selecciona una ruta nocturna del día anterior debe gobernar su seguimiento GPS. Una ruta reconocida como la ruta operativa actual no puede aparecer en la app y después ser rechazada por fecha. Las rutas realmente vencidas requieren reapertura autorizada.

CEDIS mantiene su flujo de recarga y cambio de tramo, pero usa una operación y eventos propios. El botón de entrega de sucursal no aplica a CEDIS.

## Idempotencia y modo offline

Toda confirmación requiere `client_event_id`, actor, ruta, parada, fecha del cliente y versión esperada. Se cubren:

- reintento idéntico;
- payload diferente con el mismo identificador;
- dos dispositivos simultáneos;
- replay después de autorización o rechazo;
- replay de otra ruta, otro usuario o evento vencido;
- orden de la cola y conflictos de estado.

El servidor es la autoridad: un replay nunca reabre, autoriza, rechaza ni sobrescribe una resolución posterior.

## Matriz de aceptación

| Escenario | Visita física | Entrega | Revisión | Continuidad |
|---|---|---|---|---|
| GPS confiable y permanencia cumplida | `VISITADA` con evidencia real | Pendiente hasta botón | No requerida | Sí |
| Botón después de geocerca | Conserva visita | Estado solicitado | `NO_REQUERIDA` | Sí |
| Fuera del radio | No cambia ni crea hora | Estado solicitado | `PENDIENTE` con distancia | Sí |
| GPS denegado/sin señal | No cambia | Estado solicitado | `PENDIENTE` con causa | Sí |
| Precisión baja/timestamp tardío/salto imposible | No acredita visita | Estado solicitado | `PENDIENTE` o alerta crítica | Sí |
| Sucursal sin coordenadas | No acredita visita | Estado solicitado | `PENDIENTE` | Sí |
| Retry idéntico/offline | No cambia | No duplica | No duplica alerta | Sí |
| Retry con payload diferente | No cambia | No sobrescribe | Conflicto auditado | Sí |
| Replay tras autorización/rechazo | No cambia | Conserva resolución | No reabre | Sí |
| Otro repartidor | No cambia | Rechazado | Intento auditado | No para ese usuario |
| Jefe autoriza | No cambia | Conserva hecho | `AUTORIZADA` | Sí |
| Jefe rechaza | No cambia ni borra evidencia | Conserva hecho capturado | `RECHAZADA` + corrección | Sí |
| Point antes/después/corrección/reversa | No cambia | No cambia | No cambia | Sí |
| Solicitado mayor que cero y enviado cero | No aplica | Línea visible y resuelta en cero | No requerida | Sí |
| Ruta nocturna aceptada como operativa | GPS permitido por la misma regla | Flujo normal | Según geocerca | Sí |
| Ruta realmente vencida | No recibe transiciones | Requiere reapertura | Alerta | No |
| CEDIS | Evento/operación CEDIS | No crea entrega de sucursal | No aplica | Sí |
| Finalización con excepciones | Conserva hechos | Conserva entregas | Revisiones abiertas visibles | Sí |
| Auditoría repetida | No autocorrige | No autocorrige | Una sola alerta | Sí |

Las pruebas detalladas expandirán esta tabla con diferencias, no entregada, cambios de enviado, secuencia CEDIS, concurrencia y permisos.

## Estrategia de pruebas y validación

1. Pruebas de dominio parametrizadas por origen, estado y acción; no preparar estados válidos mediante `save()` directo.
2. Pruebas API de permisos, mensajes, códigos, idempotencia y conflictos.
3. Pruebas funcionales reales de cola offline, no búsquedas de strings en HTML.
4. Pruebas de Point que demuestren que nunca modifica visita, entrega, revisión, horas o confirmador.
5. Pruebas del auditor en dos ejecuciones consecutivas para demostrar idempotencia.
6. Revisión de datos productivos en modo diagnóstico antes de habilitar el job.
7. Validación en navegador real de geocerca válida, excepción, aviso al repartidor y resolución del jefe.
8. Bump coordinado del service worker y `collectstatic` si cambia la PWA.

## Despliegue seguro

El cambio se dividirá en migración/modelo, servicio de dominio, consumidores, UI de revisión, auditor y endurecimiento de pruebas. Antes del deploy se ejecutarán checks, migraciones y pruebas completas de Logística. En producción se aplicarán migraciones, se desplegará, se ejecutará primero el auditor en modo diagnóstico y se validará el flujo real. La periodicidad se habilitará solo después de revisar el diagnóstico, evitando convertir datos históricos automáticamente.

## Fuera de alcance

No se borran entregas históricas, no se inventan visitas faltantes, no se corrigen automáticamente registros antiguos y no se cambia la propiedad operativa de Point. Cualquier reparación histórica surgida del diagnóstico será una operación separada, explícita y auditable.
