# Acuerdos equivalentes en ERP y app: diseño y plan de ejecución

Fecha: 6 de noviembre de 2026. Estado: diseño aprobado por Mauricio el 6 de noviembre de 2026; implementación coordinada ERP/app en curso.

## Resultado esperado

Gestionar una minuta/acuerdo desde ERP o app debe conservar el mismo acuerdo, responsable, puntos, documentos y reglas de aprobación, y generar las mismas comunicaciones para la misma acción y selección de canales. La confirmación debe explicar si se guardó, se sincronizó y qué pasó con cada notificación. La entrega incluye visibilidad con cuenta del colaborador, no solo registros en base de datos.

La solicitud cubre paridad funcional del ciclo de acuerdos, no una copia estética de la app. No se incorporan proyectos, reuniones o compromisos recurrentes como nuevos módulos dentro de esta entrega. Las opciones existentes de vincular un compromiso o invitación se incluyen en la matriz de compatibilidad sin cambiar su dominio.

## Decisión de arquitectura propuesta

Recomendación: conservar Agente DG como origen de las minutas existentes y centralizar allí la ejecución y registro de las comunicaciones. El ERP será otro consumidor del mismo contrato. Ambos frontends deben invocar el mismo comportamiento de servidor. Reutilizar proveedores y plantillas de la app, remitente institucional y relaciones por ID. El espejo ERP conserva sus contratos locales de revisión y aprobación ya existentes; no se eliminan ni sobrescriben al sincronizar.

Alternativas evaluadas:

1. Añadir únicamente una llamada a send-selected en ERP: corrige la omisión inmediata, pero mantiene la pérdida de errores parciales, los reenvíos de correo y la desigualdad del ciclo de gestión. Insuficiente para el alcance pedido.
2. Servicio compartido en el backend de la app, consumido por ambas interfaces: recomendado. Permite unificar mensajes, eventos, reintentos y trazabilidad; necesita cambios coordinados en dos repositorios.
3. Duplicar proveedores, plantillas y envíos en Django: descartado; dos implementaciones podrían notificar dos veces y divergir de nuevo.

## Comportamiento acordable por acción

| Acción | Comportamiento común propuesto |
|---|---|
| Guardar | Conserva el acuerdo sin comunicación externa; lo expresa claramente y no lo presenta como enviado. |
| Guardar y notificar | Guarda una vez y solicita los canales seleccionados. El formulario ofrece correo institucional y WhatsApp con iguales reglas y valores iniciales en ambos sistemas. |
| Editar | Conserva puntos y avances no editados. Presenta opción explícita de avisar cambios; el mensaje muestra la versión vigente, sin inventar obligaciones. |
| Marcar un punto / entregar | Aplica el permiso y flujo de revisión correspondiente, conserva evidencia y comunica según la misma matriz en ambas entradas. No envía un nuevo aviso de asignación por cada clic. |
| Aprobar / devolver / completar / cancelar | Registra actor, transición y motivo cuando el flujo lo exige. Genera la comunicación del evento, sin confundir guardado con entrega ni cierre automático con aprobación del DG. |
| Solicitar / resolver prórroga | Conserva fecha anterior, nueva, motivo y decisión; misma regla de comunicación y zona horaria. |
| Adjuntar y comentar | Mismos límites de acceso, tipo y tamaño; documentos disponibles al destinatario y sincronización verificable. No adjuntar automáticamente evidencias privadas a terceros. |
| Reintentar canal fallido | Usa el mismo acuerdo y evento; no reenvía canales ya aceptados. Ante resultado incierto, consulta evidencia antes de repetir. |
| Reenvío deliberado | Acción distinguible del reintento, con actor y motivo; confirmación clara de destinatarios y canales. |
| Recordatorio | Un único responsable de emitirlo por evento y periodo; evitar que scheduler de app y Celery ERP notifiquen lo mismo dos veces. Preservar recordatorios de otros tipos de seguimiento. |

No es política de avisar todo por todos los canales. La equivalencia se define por acción, destinatario, selección y plantilla; el mismo evento produce la misma comunicación desde ambas interfaces. No se añadirán puntos ni entregables por interpretación del asistente. Caso obligatorio de regresión: el acuerdo #104 conserva exactamente un punto «Solicitar los tickets de las cargas de gasolina.».

## Contrato y datos

- Identidad: `minute_agreements.id` enlazado a `SeguimientoItem.metadata(source=agente_dg, source_table=minute_agreements, source_id=ID)`. No inferir equivalencias nuevas por nombre.
- Un evento identifica acción, acuerdo, revisión de contenido, actor, destinatario y canal. Los reintentos conservan esa identidad; un reenvío deliberado crea otra intención auditable.
- Adición compatible de resultados por acuerdo y canal a las respuestas existentes. Conservar las claves actuales de los consumidores.
- Estados de comunicación: no solicitado, pendiente, en proceso, aceptado, entregado, leído, fallido y resultado incierto, según evidencia disponible. El correo no adquiere estado leído por aceptación SMTP.
- Persistir la intención antes de llamar al proveedor y reclamarla de forma atómica. Verificar qué registros existentes pueden extenderse; cualquier migración debe venir con diff y reversión inspeccionados. No usar un booleano global ni metadata sin exclusión atómica como sustituto de idempotencia.
- Correlacionar respuesta del proveedor y callback. Los webhooks duplicados o atrasados no deben retroceder estados ni generar avisos nuevos.
- Un fallo de comunicación no deshace el acuerdo guardado ni vuelve a crearlo. La interfaz ofrece recuperar el canal concreto y conserva el vínculo al registro original.
- El envío inicial, el aviso de modificación y el recordatorio son eventos distintos. `sent_to_collaborator` no describe todos ellos: el #104 tiene WhatsApp leído por aviso posterior aunque ese indicador siga falso.
- ERP importa resultados verificables del origen; no vuelve a enviar al recibir webhook. Ampliar de manera compatible `build_minute_payload` y el importador para estados y versión.
- Captura y edición deben soportar texto sin truncarlo silenciosamente, fechas en America/Mazatlan y ausencia de datos de contacto como bloqueo visible del canal, nunca como éxito.

## Matriz y archivos de trabajo

La matriz de paridad se completa por cada acción antes de modificarla: campos, opciones, validaciones, actor permitido, destinatarios, plantilla, adjuntos, persistencia, sincronización y prueba en ambas interfaces. Debe incluir creación individual, sesión consolidada de la app, edición, puntos, evidencias, comentarios, entrega, aprobación/devolución, cierre, prórroga, envío, reintento y recordatorio.

Fuentes verificadas que se deben revisar/modificar según el contrato:

| Repositorio | Archivos/áreas | Responsabilidad |
|---|---|---|
| ERP | seguimiento/agente_dg_client.py | Cliente de operaciones y resultados comunes. |
| ERP | seguimiento/views.py, urls.py | Acciones autorizadas y mensajes veraces al usuario. |
| ERP | seguimiento/services.py, webhooks.py | Importación compatible, orden de eventos y revisión local. |
| ERP | seguimiento/models.py, tests.py | Persistencia existente y regresiones; cambiar modelo solo si la ficha lo justifica. |
| ERP | seguimiento/templates/seguimiento/panel_dg.html, mi_seguimiento.html, _detalle_contenido.html | Opciones equivalentes, visibilidad personal y resultado por canal. |
| ERP | core/notificaciones.py, seguimiento/tasks.py | Avisos internos y coordinación de recordatorios; evitar envíos paralelos. |
| App | backend/routers/minutas.py, schemas/minute_agreement.py | Contrato existente de creación, actualización y envío. |
| App | backend/templates/minutas.html | Consumir el contrato común; corregir mensajes de éxito parcial. |
| App | backend/services/erp_webhook.py | Publicar contenido, revisión y comunicación al espejo ERP. |
| App | backend/models/minute_agreement.py, minute_whatsapp_event.py, servicios de entrega y recordatorios existentes | Reutilización de historial y callbacks; extender solo lo necesario. |

No copiar el archivo completo de vistas ni introducir un segundo motor de acuerdos. Si se necesita separar el despacho, extraer solo ese servicio conservando validación y manejo de errores.

## Ejecución con software factory

### 1. Preparación y aceptación

- [ ] Revisar registros de tareas antes de crear recursos. Coordinar las tareas activas de ambos repositorios sin tocar sus worktrees ni desplegar concurrentemente.
- [ ] ERP: iniciar rama/worktree propio desde main actualizado con `scripts/task_workspace_start.sh` y ejecutar preflight. Inscribir la entrega como production en `scripts/task_factory.py`, con autorización, matriz y aceptación.
- [ ] App: identificar main actual y correspondencia entre `/opt/agente-final-dg-release` y el código realmente servido en `/opt/agente-final-dg/backend`. La carpeta operativa no tiene .git. No desplegar una revisión sin cotejarla con los archivos vigentes y proteger cambios no integrados.
- [ ] Crear worktree propio de la app desde su base actualizada y aplicar su protocolo de operación. Mantener un registro coordinador con ambos PR, SHA, orden de promoción y evidencia; no presentar los gates ERP como prueba de CI/deploy de la app.
- [ ] Registrar antes de crearlos Compose, puertos, volúmenes y dueño; PostgreSQL 16 aislado para Django. No usar datos operativos ni stg compartido sin verificar exclusividad. Sin SQLite.

### 2. Contrato común y pruebas de regresión

- [ ] Escribir primero regresiones de omisión de envío, resultado parcial y doble petición sobre los endpoints actuales; usar proveedores simulados claramente identificados.
- [ ] Completar servicio y resultado por canal en app; pruebas de concurrencia y timeout con aceptación incierta, además de éxito/falla normal.
- [ ] Actualizar ambos consumidores de la app —formulario individual y consolidado— y mantener compatibilidad del contrato previo.
- [ ] Ampliar webhook y validar firma, campos, actor e identificadores sin modificar permisos existentes.

### 3. Paridad ERP y comunicación

- [ ] Integrar las operaciones comunes en el cliente ERP y las pantallas; conservar un acuerdo al fallar el espejo o el envío.
- [ ] Completar opciones y campos faltantes detectados por la matriz, incluidos documentos y resultado por canal. No enviar instrucciones o fechas distintas según la pantalla de origen.
- [ ] Probar ida y vuelta: crear en ERP y editar en app; crear en app y completar/revisar en ERP. Verificar revisión/aprobación y evidencia, sin sobrescritura por eventos atrasados.
- [ ] Diagnosticar y resolver la visibilidad reportada de Johana usando su ámbito real: cuenta, sistema, filtros y listado. No dar por visible #843 solo porque existe en PostgreSQL. Comprobar que no aparece a usuarios ajenos.
- [ ] Coordinar los recordatorios de minutas para que un evento solo tenga un emisor, conservando los de compromisos, proyectos y calendario fuera del dominio modificado.

### 4. Validación, PR e integración

- [ ] Ejecutar pruebas del dominio con PostgreSQL; `python manage.py check`, `migrate --check`, revisión de migraciones y consumidores. Registrar revisión exacta de cada prueba.
- [ ] Revisión completa de ambos diffs: contratos, manejo de fallos, permisos, archivos, logs sin credenciales ni datos personales innecesarios. No introducir dependencias para pequeños atajos.
- [ ] Navegador real en escritorio y móvil: guardar, notificar, editar, adjuntar, responder, entregar, aprobar/devolver, reintentar y ver estados. Revisar consola y XHR; comprobar caché y SW si corresponde.
- [ ] PR separados por repositorio, mismo objetivo y referencias cruzadas; borradores hasta terminar la revisión. CI obligatorio del HEAD actual de cada PR. Integración solo con gates satisfechos.

### 5. Despliegue, aceptación real y cierre

- [ ] Desplegar primero el contrato compatible de app por `deploy_prod_safe.sh`; después ERP por `deploy_web_safe.sh`, sin git pull manual previo. Serializar despliegue y verificaciones con los mecanismos propios de cada repositorio.
- [ ] Verificar versiones realmente servidas, health y smoke de ambas aplicaciones. El SHA del disco solo no prueba el proceso en ejecución.
- [ ] Ejecutar aceptación con destinatarios de prueba expresamente acordados antes de enviar a personas reales; no usar los acuerdos de colaboradores como pruebas. La autorización de código no autoriza avisos históricos masivos ni gasto adicional a proveedores.
- [ ] Confirmar correo institucional aceptado y evidencia WhatsApp; distinguir entrega, lectura y ausencia de callback. Registrar limitación si el proveedor no permite comprobar entrega.
- [ ] Retirar exclusivamente recursos propios tras respaldo y verificación; helper obligatorio para Docker local cuando aplique. Cerrar factory y registros, conservar evidencias y retirar ramas/worktrees ya integrados y verificados.

## Casos de aceptación obligatorios

| Caso | Resultado exigido |
|---|---|
| Crear y notificar desde cada interfaz | Mismo acuerdo y reglas, dos canales solicitados con resultados separados. |
| Guardar desde cada interfaz | Cero envío externo; confirmación de guardado sin afirmar notificación. |
| Correo exitoso y WhatsApp fallido, y caso inverso | Éxito parcial visible en ambas interfaces, con causa y reintento del canal faltante. |
| Doble clic, timeout y reintento | No crea dos acuerdos ni repite un mensaje ya aceptado; resultado incierto visible. |
| Webhook duplicado o fuera de orden | No restaura contenido antiguo, no borra avances, no genera otro envío. |
| Edición cruzada y prórroga | Mantiene IDs, motivo, zona horaria, checklist y responsabilidades. |
| Aviso posterior de #104 | No deduce ausencia de envío únicamente por sent_to_collaborator=false. |
| Visibilidad de Johana | Aparece en su lista y detalle correctos con su ámbito; usuarios ajenos no acceden. |
| Adjuntos y evidencia | Acceso autorizado desde ambas pantallas, sin enlaces rotos ni exposición cruzada. |
| Recordatorio en ambos schedulers | Un solo aviso por evento y canal; no afecta recordatorios de otros módulos. |
| Semántica | Conserva exactamente el punto aprobado por Mauricio; no agrega reportes, conciliaciones ni entregables. |

## Reversión y recuperación histórica

Cambios de API compatibles antes del cliente. Si falla la promoción ERP, la app anterior debe seguir funcionando. Conservar acuerdos e historial; revertir código por el procedimiento de cada repositorio. No borrar eventos aceptados ni revertir migraciones mediante pérdida de datos.

Los acuerdos anteriores se revisan individualmente después de reparar el flujo. Excluir cerrados y textos reemplazados; no reenviar #104 por asumir que su booleano de correo implica ausencia de todos los avisos. Las 137 aclaraciones del conciliador son otro origen y no se convierten masivamente en minutas ni se notifican en esta entrega sin revisar su política.

## Autorización y límites actuales

Mauricio aprobó el diseño y la implementación completa el 6 de noviembre de 2026 en el chat 01a1b267-d0e8-7192-bb16-2ecf9096b645. La autorización incluye los cambios coordinados y las migraciones descritas, con pruebas, revisión, integración, despliegue y verificación. Los recursos temporales tienen registro por tarea en software factory. Las pruebas de proveedores, si implican gasto nuevo o mensajes a colaboradores, conservan su autorización específica.

Las habilidades elegidas son herramientas de ejecución, no permisos: erp-data-reuse, api-and-interface-design, systematic-debugging, writing-plans; observability-and-instrumentation para estados y correlación; impeccable para UX del ERP; shipping-and-launch y verification-before-completion para publicación y evidencia.
