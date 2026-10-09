# Ficha de fuentes — procesos pendientes READ del agente

Fecha: 2026-10-07. Base inspeccionada: `29286b52cf843ba2ed33a3d14bd879fd3cb13259`. Evidencia de producción tomada a las 21:47 UTC durante el diseño; comprobación local adicional sobre PostgreSQL 16 aislado, puerto 55527. Los conteos siguientes son históricos de ese corte, no métricas permanentes.

## Necesidad y unidad de análisis

Una intención de consultar un equipo y su mantenimiento que pertenece a un usuario ERP y puede continuar desde otra conversación. El estado técnico conserva datos faltantes y referencias; no es una segunda ficha de equipo, orden, incidente, solicitud o compromiso empresarial.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Conversación | ChatConversation / orquestacion_chatconversation | chat_service | UUID, propietario, estado | 3 activas | Chat y runtime |
| Referencias del chat | ChatConversationState / orquestacion_chatconversationstate | chat_service y runtime READ | Una fila por chat | 3 | Chat legado y F3 |
| Mensajes | ChatMessage / orquestacion_chatmessage | Servicio de turnos | UUID, secuencia, autor | 16: 14 completos, 2 con error | Historial |
| Ejecución / resultado | ChatToolCall, ChatToolResult / orquestacion_chattoolcall, orquestacion_chattoolresult | Runtime y Gateway | UUID y vínculos al turno | 5 llamadas completas, 5 resultados | Historial y auditoría |
| Tarea de orquestación | AgentTask / orquestacion_agenttask | Loops, reglas y agent_runtime | Run/agente obligatorios | 3020: 2956 pendientes, 64 resueltas | Dashboard y reglas existentes |
| Sugerencia / aprobación anterior | AgentSuggestion / orquestacion_agentsuggestion | Reglas y aprobaciones de orquestación | Tarea/decisión | 2956 pendientes | Orquestación existente |
| Compromiso operativo | SeguimientoItem / seguimiento_seguimientoitem | Módulo seguimiento | Responsable, participantes, tipo | 507: 422 compromisos, 83 minutas, 2 proyectos | Seguimiento |
| Equipo / mantenimiento | Activo, PlanMantenimiento y fuentes del pasaporte | Módulos operativos existentes | PK y ámbito autorizado | [Ficha READ existente](ai-erp-gateway-read-shadow.md), fixtures sintéticos | Pasaporte, mantenimiento y Gateway |
| Auditoría | AuditLog / core_auditlog | Gateway y servicios existentes | Usuario, acción, objeto | Modelo existente; transiciones verificadas en pruebas | Auditoría común |

## Alias y equivalencias

| Términos | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| Conversación / chat / thread | Confirmada en el contrato existente | UUID de ChatConversation; Telegram aún no implementado | Ninguna equivalencia nueva |
| Estado del chat / pendiente durable | Distinta | OneToOne por chat no identifica varios procesos independientes entre chats | Nueva entidad técnica aprobada por Mauricio |
| AgentTask / pendiente conversacional | Distinta | FK run/agente y consumidores de reglas; no es una bandeja personal de continuidad | Conservar tareas existentes |
| AgentSuggestion / confirmación nueva | Distinta | La decisión anterior no confirma argumentos ni versión de un proceso nuevo | No convertir aprobaciones |
| SeguimientoItem / intención incompleta | Distinta | Minuta/proyecto/compromiso tiene semántica operativa propia | No crear compromisos automáticamente |
| Posición de opción / PK de equipo | Distinta | La posición permanece aunque se revoque el recurso | Rehidratar con alcance vigente |

## Decisión de diseño

Reutilizar conversaciones, mensajes, resultados, Gateway, AuditLog y la política canónica `activos.services_pasaporte.activos_autorizados`. Añadir una sola entidad técnica `AgentWorkflow` para propietario, versión y continuidad entre chats. Mauricio aprobó F4.1/F4.2 con migración aditiva y gates apagados. No hay backfill, fusiones, reclasificación de tareas existentes ni otra captura maestra.

El workflow conserva búsqueda acotada e IDs; nombres y mantenimiento se consultan de nuevo. Sus opciones son propias del proceso y no sustituyen el namespace F3 de ChatConversationState. La evidencia de lectura se conserva en ChatToolResult, sin copiar historias/costos al estado del workflow.

## Procedimiento y límites

La revisión de diseño confirmó PostgreSQL 16 y ejecutó en sesión READ ONLY con timeout de 5 segundos:

```text
inventario_fuentes_datos --term conversacion --term chat --term tarea --term pendiente --term estado --term workflow --details --presence --limit 25
```

Se obtuvieron 63 candidatos léxicos y 25 mostrados; se verificaron conteos de las ocho tablas anteriores y grupos de estados/tipos limitados a 12, sin leer títulos, nombres, mensajes o payloads. La igualdad de conteos de tareas/sugerencias no demuestra duplicación.

La comprobación local previa a implementar ejecutó `inventario_fuentes_datos --term conversacion --term workflow --term proceso --term tarea --presence --details --limit 15`: 9 modelos candidatos y una tabla sin modelo. Son pistas léxicas, no equivalencias semánticas. La base sólo contiene migraciones y posteriores fixtures ficticios; no se importó producción. Artefactos originales privados: `ai-erp-pending-design-20261007/source-evidence.json` y `ai-erp-workflows-read-20261007/source-inventory-local.log` bajo `~/.codex/task-artifacts/`.

Riesgos: revocación o traslado de equipos exige autorización fresca; un proceso sin propietario no se reasigna. AuditLog no garantiza inmutabilidad física frente a administradores de BD. No se purgan pendientes ni se fija una política definitiva de retención. RH, facturas y otras transacciones requieren su propia revisión de fuentes antes de incorporar herramientas.
