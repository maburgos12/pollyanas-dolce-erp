# F3 — Agent Core READ/SHADOW

Estado del corte: implementación backend con proveedor simulado. No activa un piloto ni modifica configuración de producción. El despacho en `execute_chat_turn` exige `AI_AGENT_READ_ENABLED is True`; la ausencia del setting conserva el camino legado. El Gateway requiere por separado `AI_GATEWAY_ASSETS_ENABLED is True` y acceso vigente. Un rechazo del subset nunca cae al catálogo legado.

## Contrato

El servicio usa Responses del SDK instalado y exclusivamente las tools `erp.search_assets`, `erp.get_asset_context` y `erp.get_pending_maintenance` del Gateway. READ es una decisión del servidor; los schemas se reutilizan con `strict=False` explícito. Los serializers existentes validan tipos, campos extra y alcance. JSON inválido nunca se convierte en argumentos vacíos. No hay aprobaciones, sincronizaciones, jobs, nuevas tablas ni escrituras operativas.

Antes de llamar al proveedor y de cada tool se refrescan identidad, conversación activa, permisos, ACL, sucursal y acceso financiero. Los mensajes deben pertenecer a la conversación y al turno, con usuario autor y roles correctos. Un fingerprint de permisos y la revalidación de activos materializados detienen el turno ante revocaciones antes de retransmitir contexto previo.

Responses recibe sólo el mensaje actual, un prompt operativo acotado y referencias rehidratadas. No recibe prompts DG, historial narrativo, pins ni otros namespaces de estado. `store=False`, `max_retries=0` y timeout igual al presupuesto temporal restante. Los reasoning items necesarios para continuar se conservan únicamente en memoria durante ese turno; no se persisten.

| Presupuesto por turno | Máximo |
| --- | ---: |
| Respuestas de modelo | 6 |
| Llamadas de tools | 10 |
| Tiempo medido con reloj monótono | 60 segundos |
| Texto del mensaje actual | 6000 caracteres |
| JSON de argumentos | 6000 caracteres |
| Salida individual de tool | 30000 caracteres |
| Contexto serializado acumulado | 100000 caracteres |

Estos límites no son un SLA, conteo de tokens ni cuota monetaria. Una salida grande se sustituye por un error JSON completo con metadata de fuente disponible; no se corta JSON. El timeout de SDK acota la espera del proveedor; un handler o consulta de base ya iniciados no se cancelan activamente al vencer el reloj. Se verifica el presupuesto al recuperar control, antes de la siguiente llamada y antes del cierre.

## Referencias, auditoría y presentación

`ChatConversationState.context_window_json.agent_read` conserva sólo `option_ids` (máximo 50, orden original) y `last_asset_id`. Cada nuevo turno los resuelve con `activos_autorizados` fresco; las opciones revocadas mantienen su posición y figuran como indisponibles sin nombre ni ID. La actualización bloquea brevemente la fila y conserva namespaces ajenos y metadata técnica de otros procesos. No son tareas transaccionales F4.

Cada intento ejecutado tiene `ChatToolCall`/`ChatToolResult` terminal vinculado al turno. Los inputs inválidos guardan argumentos vacíos; nombres desconocidos guardan `unregistered`. Se reutiliza la auditoría Gateway incluso en rechazos previos al handler. Un fallo de auditoría o de permisos interrumpe el turno. Un fallo o respuesta incompleta del proveedor después de una tool conserva su evidencia en un cierre ERROR del servidor sólo si una revalidación fresca confirma que aún es accesible; una revocación simultánea oculta esa evidencia. Una salida que excede el límite muestra fuente, fecha, zona y unidad disponibles, sin su payload grande. No se almacenan excepciones, argumentos inválidos ni nombres arbitrarios del proveedor. Los timestamps abarcan el intento real.

`ChatMessage.metadata_json.agent_read` guarda runtime, modelo, rondas, llamadas, fingerprint, IDs materializados y usage observado cuando el proveedor lo entrega. No se calcula costo ni se reemplazan tokens por caracteres. Un replay completo no vuelve a llamar al proveedor ni a tools: devuelve el cierre previo sólo si los permisos y activos siguen accesibles. Ante cambio de alcance produce un error seguro sin contenido anterior y conserva el fingerprint, IDs, rondas y usage del intento original; el rechazo del replay se registra por separado. Los cierres parciales ERROR también conservan los IDs materializados para esta comprobación.

El cierre F3 es estructurado y construido por el servidor a partir de payloads del Gateway: incluye datos útiles de ficha y planes, fuente, fecha, ausencia de datos, ambigüedad y límites. La prosa final del proveedor no se publica. Esto evita que una afirmación del modelo se presente como una acción ejecutada; no pretende ser una evaluación de lenguaje natural. Siempre se conserva el aviso de historial parcial y de que la ausencia de plan no prueba ausencia de servicio.

Las proyecciones de historial, herramientas y previews de F3 revalidan esa prueba original mediante la política del runtime antes de mostrar contenido, headers, resumen o payload. El acceso independiente a la página de chat no concede acceso a costos o equipos. Gates apagados, propietario inactivo, conversación archivada, prueba inválida, revocación de permisos o traslado de un equipo ocultan la proyección READ sin borrar sus registros técnicos/auditoría. La preparación del historial enviado al cliente legado usa el mismo guard: con el gate F3 apagado nunca reinyecta mensajes F3 antiguos, aunque sigan vigentes los permisos de lectura. El historial legado puro conserva su comportamiento.

La fila assistant pasa de pending a streaming bajo un bloqueo breve: el mismo mensaje no se ejecuta simultáneamente. La serialización de turnos distintos de una conversación, recuperación tras interrupción de proceso y ejecución durable quedan fuera de F3 y requieren F4.

## Evidencia local y puerta del piloto

Las pruebas usan fixtures sintéticos de PostgreSQL 16 y un doble de OpenAI; el Gateway, serializers, permisos, queryset y auditoría se ejecutan realmente. Cubren encadenamiento en tres respuestas, referencias entre turnos, ordinales conservados, gates exactos, ownership, replay, inputs inválidos, tools desconocidas, límites, revocación financiera/sucursal, errores de proveedor/handler/auditoría, ausencia de datos e historial parcial. No demuestran interpretación real de OpenAI, su costo/latencia ni uso autenticado de un agente en producción.

Entorno de esta tarea: Compose `erp_ai_agent_core_read_20261007`, PostgreSQL local puerto 55507, propietario `codex-erp-ai-root`. Los artefactos RED/GREEN quedan fuera de Git en el directorio privado de la tarea. Integración, CI, despliegue con ambos gates apagados y retiro exacto del entorno corresponden al cierre del propietario; este documento no los declara completados.

Un piloto real necesita una autorización separada con modelo, presupuesto, usuarios/datos nominales y evaluación real. ChatKit, Telegram, streaming de proveedor y acciones transaccionales siguen fuera del corte. No se han leído ni expuesto credenciales; las pruebas usan un valor ficticio sólo dentro de settings de test y el cliente está siempre reemplazado.
