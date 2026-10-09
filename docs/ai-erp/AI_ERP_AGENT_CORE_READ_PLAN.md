# F3 — Agent Core READ/SHADOW

> Para ejecución: subagent-driven-development, implementación única seguida de revisión de especificación y calidad. Mauricio autorizó este corte y reutilizar la clave existente; no se harán llamadas reales al proveedor.

**Objetivo:** consultas encadenadas de activos y mantenimiento con referencias conversacionales revalidadas, sin escrituras operativas ni activación del piloto.

**Arquitectura:** reutilizar el SDK OpenAI instalado (Responses), las tres tools del Gateway y las tablas de conversación existentes. Un servicio acotado se selecciona desde `execute_chat_turn` solo cuando `AI_AGENT_READ_ENABLED is True`; el gate existente `AI_GATEWAY_ASSETS_ENABLED` también debe estar habilitado y autorizar al usuario. Ambos permanecen apagados en producción; no modificar settings, env, roles, endpoints ni modelos.

**Stack:** Django, PostgreSQL16, DRF, SDK OpenAI ya instalado. Sin dependencias nuevas, ChatKit o Telegram en este diff.

## Archivos y propiedad

- Crear `orquestacion/services/agent_read_runtime.py`: único runtime del subset, prompt operativo sin documentos DG, Responses loop, persistencia técnica y referencias.
- Modificar `orquestacion/services/chat_service.py`: dispatch mínimo detrás del gate exacto y protección específica de mensajes, tools y previews F3 al serializar el historial. Conservar firmas/shape y comportamiento legado; revalidar acceso antes de exponer resultados F3 persistidos, incluso tras apagar el gate o denegar un replay.
- Crear `orquestacion/tests_agent_read_runtime.py`: fixtures sintéticos y dobles del proveedor; el Gateway y sus permisos se ejecutan realmente.
- Crear `docs/ai-erp/AI_ERP_AGENT_CORE_READ.md`: contrato, límites, estado real y puerta del piloto.
- Crear `docs/data-reuse/ai-erp-agent-core-read.md`: reutilizar la ficha F2; agregar fuente de conversación y evidencia del inventario local, sin reinterpretar conteos históricos como actuales.

## Contrato del corte

1. Validar en servidor usuario activo fresco, propiedad de conversación y pertenencia/roles de los dos mensajes. Estado no reutilizable o conversación archivada se rechazan antes del proveedor. Gate off no invoca el nuevo runtime y conserva el comportamiento legado. Gate on con subset denegado no hace fallback al catálogo legado.
2. Catálogo exclusivamente `list_read_shadow_tools`, ejecución exclusivamente `invoke_read_shadow_tool`. Modelo y READ/SHADOW son decisiones del servidor; argumentos del modelo nunca conceden usuario, scope, aprobación o escritura. El catálogo legado, borradores, sincronizaciones y aprobaciones quedan fuera.
3. Model calling via SDK Responses existente: `store=False`, retries automáticos desactivados y timeout limitado por presupuesto restante. No usar Conversations hospedadas, herramientas web/SQL/computer-use ni cargar prompts/memorias DG.
4. Máximos iniciales: 6 respuestas de modelo, 10 llamadas de herramientas y 60 segundos por turno; texto actual <=6000 caracteres, argumentos JSON <=6000, cada salida tool <=30000 y contexto acumulado <=100000 caracteres. Rechazar o sustituir resultados demasiado grandes por un error JSON explícito, nunca cortar JSON a mitad. Son límites de esta versión, no SLA ni una cuota monetaria global.
5. Preservar en Responses la salida necesaria para encadenamiento, incluidos reasoning items cuando existan. No almacenar razonamiento privado. Las tools nuevas conservan schemas del Gateway; si se usa strict, convertir opcionales a nullable requerido y omitir nulls solo en esas propiedades opcionales al volver al serializer. No duplicar reglas de tipos/alcance. No ignorar campos extra o convertir JSON inválido en `{}`. Los límites se verifican antes de ejecutar cada llamada.
6. Antes de cada request y tool, revalidar identidad y permisos. Si el alcance/permiso financiero cambió durante el turno, detenerlo antes de retransmitir resultados materializados con el alcance anterior. No devolver excepciones, headers, argumentos inválidos o nombres arbitrarios del proveedor en mensajes/logs persistidos. Tool falsa o inputs inválidos producen errores estructurados sin handler. PermissionDenied o fallo de auditoría detienen de forma segura; no se ocultan como éxito.
7. Conservar únicamente IDs acotados y orden de opciones/último equipo consultado en una clave propia de ChatConversationState. No reinyectar narrativas, resultados financieros, pins ni historial legado en el modelo. Al recuperar opciones, resolverlas por el queryset autorizado fresco y presentar posiciones originales; una opción que dejó de ser accesible se vuelve indisponible, sin revelar su nombre ni renumerar silenciosamente. Actualizar estado propio sin borrar metadata de otros procesos. Las referencias son contexto de consulta; no constituyen tareas transaccionales reanudables F4.
8. Persistir ChatToolCall/Result vinculados al turno con estado final y argumentos válidos únicamente. Registrar éxito, rechazo e inputs inválidos sin secretos; resultados acotados y evidencia Gateway conservan fuente/fecha/límites. La auditoría Gateway existente es obligatoria. Guardar en metadata técnica modelo/runtime, rondas, llamadas y usage observado sin inventar costo ni afirmar que el conteo de caracteres es tokens. Turno fallido marca assistant ERROR con explicación segura; no deja tools RUNNING. Replay de un mensaje completado no reejecuta herramientas/proveedor ni divulga texto con permisos cambiados. Conservar la huella, IDs y usage de la ejecución original; el rechazo posterior no convierte su evidencia en autorizada bajo permisos nuevos. La lectura de historial, tools y preview F3 exige acceso fresco y falla cerrada; mantener auditoría interna sin publicar resultados revocados.
9. Separar preparación de contexto y presupuesto del loop; no crear framework, factory o registro nuevo. El texto final no puede afirmar acciones ejecutadas en READ/SHADOW. Si el modelo no devuelve cierre útil o alcanza límites, devolver un cierre del servidor que conserve evidencia y faltantes/limitación. No inferir fecha de servicio desde ausencia de plan.

## Pasos de ejecución y comprobación

- [x] Tarea registrada y preflight limpio; PostgreSQL16 exclusivo puerto55507, propietario `ai-erp-agent-core-read-20261007`.
- [x] Aplicar migraciones existentes de main; `migrate --check` y `check` sin errores antes de escribir código.
- [x] Prueba roja: con gate True, dos respuestas consecutivas proponen buscar y consultar ficha; una tercera cierra. Usar las tools reales y un proveedor doble. Verificar que el cliente Chat Completions legado nunca se usa.
- [x] Pruebas rojas de ownership/gates, JSON inválido, tool falsa, límites, permisos/scope revocados, replay multi-turn y GET de historial/previews tras revocación. Comando: `bash /Users/mauricioburgos/.codex/task-artifacts/ai-erp-agent-core-read-20261007/run-django.sh test orquestacion.tests_agent_read_runtime --keepdb`. Registrar el fallo por comportamiento faltante.
- [x] Implementar mínimo runtime y dispatch; hacer pasar esos casos.
- [x] Verificar ambigüedad/nodata/historial parcial, intentos prohibidos e injection en datos, aislamiento entre roles, timeouts y errores seguros; ninguna escritura a tablas operativas o AgentSuggestion/jobs.
- [x] Ejecutar nuevas pruebas y suites `orquestacion.tests_chat_service`, `api.tests_ai_gateway_assets`, Gateway legado y consumidores de chat; check/migrate/makemigrations sin cambios.
- [x] Revisión independiente de especificación y, luego, calidad; resolver hallazgos y revisar diff completo.
- [ ] Commit/PR, CI completo, merge y deploy seguro oficial; verificar código servido con los gates apagados, no probar OpenAI en producción ni alterar usuarios.
- [ ] Retirar exactamente el entorno temporal propio mediante el helper de cierre con evidencia recuperable, cerrar worktree y ramas y documentar espacio observado.

## Aceptación y límites explícitos

El cierre inicial se construye con datos y evidencia del Gateway; la prosa del proveedor no se publica en F3. Es una decisión conservadora de este corte apagado, no la interfaz ni el comportamiento conversacional final.

Las pruebas deben demostrar encadenamiento, recuperación de opciones en otro turno y cumplimiento del backend con datos sintéticos. No demuestran interpretación real de OpenAI, costo/latencia del modelo ni un agente disponible para usuarios. El piloto real requiere modelo/presupuesto/datos/usuarios definidos y una evaluación real separada. ChatKit/Telegram, streaming en vivo, tareas durables y acciones transaccionales conservan sus cortes F4–F9. Este diff no los simula como completados.
