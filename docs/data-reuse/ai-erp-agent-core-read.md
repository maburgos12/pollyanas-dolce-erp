# Ficha de fuentes — Agent Core READ/SHADOW

Fecha y ambiente: 2026-10-07, worktree `codex/ia-erp-agent-core-read`, PostgreSQL local aislado 16.11, puerto 55507. No se consultó producción para este corte.

## Necesidad y unidad de análisis

Encadenar lecturas autorizadas de equipos y mantenimiento y recuperar el orden de opciones de un turno anterior. Un equipo sigue siendo la entidad canónica `Activo`; un plan, orden y falla conservan unidades distintas. La conversación y sus mensajes son evidencia técnica del intercambio, no una nueva captura de datos operativos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Equipo | activos.Activo / activos_activo | Catálogo existente | PK/código/QR; sucursal/vigencia | Ficha F2 histórica y fixtures sintéticos de F3 | Pasaporte, Operación, Mantenimiento, Gateway |
| Plan / trabajo / incidente | activos.PlanMantenimiento, activos.OrdenMantenimiento, fallas.ReporteFalla | Flujos existentes | PK; activo_ref o activo_relacionado; ámbito histórico | Ficha F2 histórica; serializers/DTO reales en tests | Gateway y pasaporte existentes |
| Alcance / costos | auth.User, core.UserProfile, core.UserModuleAccess | Administración de usuarios y permisos | Usuario activo, ACL y sucursal | Fixtures sintéticos y revocaciones verificadas en tests | Gateway, Mantenimiento, Activos, runtime |
| Conversación | orquestacion.ChatConversation / orquestacion_chatconversation | Servicio de chat existente | PK/public_id, owner y estado activo | Tabla local presente; COUNT=0 antes de fixtures | UI/API de chat y runtime |
| Contexto acotado | orquestacion.ChatConversationState / orquestacion_chatconversationstate | Servicio de chat y runtime | OneToOne conversation; namespace agent_read | Tabla local presente; COUNT=0 antes de fixtures | Chat legado; F3 conserva namespaces ajenos |
| Mensaje | orquestacion.ChatMessage / orquestacion_chatmessage | create_user_turn y ejecución de turno | PK/public_id; sequence única por conversación; roles y autor | Tabla local presente; COUNT=0 antes de fixtures | Historial/UI de chat y runtime |
| Intento / resultado técnico | orquestacion.ChatToolCall / orquestacion_chattoolcall; orquestacion.ChatToolResult / orquestacion_chattoolresult | Runtime y servicio de chat existente | Public_id; vínculos al turno; resultado OneToOne | Tablas locales presentes; COUNT=0 antes de fixtures | Historial/UI de tools y auditoría técnica |
| Auditoría de acceso | core.AuditLog / core_auditlog | Gateway existente | Evento AI_GATEWAY_TOOL_INVOKE, actor/scope/tool | Audit real verificado por fixtures | Auditoría existente |

## Alias y equivalencias

| Términos | Estado | Evidencia y caso contrario | Revisión |
| --- | --- | --- | --- |
| Equipo / activo | Confirmada sólo por PK/código/QR | Un nombre puede devolver varios equipos | Usuario selecciona opción; no fusionar |
| Function name / tool key | Equivalencia de contrato | Catalog: `erp_search_assets` corresponde a `erp.search_assets` | Runtime usa mapa del catálogo existente |
| Posición conversacional / PK | Distinta | La posición se conserva aun cuando otro equipo deja de ser accesible | Rehidratar con queryset fresco, no renumerar |
| Plan / orden / falla | Distinta | Tablas y unidades de análisis independientes | No inferir calendario de órdenes/fallas |
| Referencia de conversación / tarea operativa durable | Distinta | Sólo IDs de consulta; no aprobación ni transacción | F4 queda pendiente |

## Evidencia y procedimiento reproducible

Se reutiliza [la ficha F2](ai-erp-gateway-read-shadow.md), fechada el 2026-10-06 en America/Mazatlan. Sus conteos de producción son históricos; este documento no los refresca ni los presenta como estado actual.

El inventario local ejecutado por el propietario: `inventario_fuentes_datos --term conversacion --term activo --term equipo --term mantenimiento --presence --details --limit 12`. El artefacto privado `source-inventory-local.log` informa 62 modelos candidatos y 12 mostrados; son coincidencias léxicas, no equivalencias ni conteos actuales de operación. No extrae valores de registros.

Verificación adicional de las cinco tablas de chat: conexión directa al PostgreSQL exclusivo, transacción READ ONLY, `statement_timeout=15000`, `SELECT version(), current_database(), current_setting('transaction_read_only')` y cinco `SELECT COUNT(*)` con nombres de tabla exactos. Versión observada: 16.11; database `pastelerias_erp`; read_only=on; las cinco tablas estaban vacías. Esto prueba esquema local y ausencia de datos operativos copiados, no disponibilidad de esas tablas en producción.

## Decisión de diseño

Reutilizar fuentes y tablas existentes. El catálogo/executor READ, serializers, `activos_autorizados`, DTO y política de costos siguen siendo autoridades. El runtime sólo agrega contexto por IDs en `context_window_json.agent_read` y métricas/prueba original de alcance en metadata del mensaje. Historial y previews F3 reutilizan esa prueba y la misma política fresca antes de proyectar contenido o resultados; un replay rechazado conserva IDs y usage originales. La proyección oculta datos fuera de alcance sin borrar evidencia técnica/auditoría ni cambiar el historial legado. No crea modelos, tablas maestras, equivalencias, importadores ni una segunda captura.

Riesgos y pendientes: referencias conversacionales no constituyen tareas durables; respuestas de proveedor son simuladas; la prosa de proveedor no se publica en F3; validación real del lenguaje/piloto requiere autorización posterior. Conteos históricos F2 y fixtures F3 son evidencias distintas. No se alteran datos operativos ni configuración de producción.
