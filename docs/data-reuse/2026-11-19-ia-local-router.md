# Ficha de fuentes — asignación local de solicitudes

Fecha: 19/11/2026 (Mazatlán); PG16 local exclusivo `erp_ia_local_router_20261119`, puerto55487, base sin datos operativos. Base Git045b51100a539b076c4adac97bcb873149907b4a. Inventario ejecutado con `inventario_fuentes_datos --term AgentTask --term OrchestrationRun`; ambas tablas existen. Conteos iniciales acotados mediante ORM: AgentTask0, OrchestrationRun0, OrchestrationRule0. No se extrajeron personas ni importes.

## Necesidad y unidad de análisis

Una solicitud técnica acotada procedente de un agente activo; no es una aprobación de negocio ni una nueva entidad de inventario. Necesita contrato, evidencia mínima, identidad idempotente, equipo/modelo asignado, reserva temporal y resultado validado.

| Concepto | Fuente oficial / identificador | Consumidores y decisión |
| --- | --- | --- |
| Solicitud y resultado | AgentTask, PK id, task_type local_ai_request | Reutilizar input_payload/output_payload y estados existentes; no otra tabla o catálogo |
| Ejecución idempotente | OrchestrationRun, run_key único local_ai_request:UUID | Reutilizar; conflicto UUID con contenido distinto se rechaza |
| Agente solicitante | AgentDefinition, code único y status active | Reutilizar registro vigente; acceso privado administrativo existente |
| Activación, reservas y contacto | OrchestrationRule code local_ai_router; bloqueo transaccional | Extender configuración técnica de regla; llaves fuera de JSON/DB/Git |
| Evidencia de resúmenes periódicos | local_ai._sources y referencias AgentTask/run/entity exactas | Consumidor existente de cinco tipos, máximo2000/7d, sin notas libres; revalidar datos y referencias al terminar |
| Operación de negocio | Tareas originales y registros del dominio | Se conservan; resolver cómputo técnico no cambia su estado ni autoriza acciones |

## Alias y equivalencias

`mac_ia` identifica Debian8GB y `mac_ia_02` la Mac Intel16GB: distintos, comprobados por SSH/hostname/runtime. Qwen4B, Qwen8B y Nemotron son perfiles de ejecución, no agentes de negocio ni servidores nuevos equivalentes. No se crean equivalencias entre entidades Point/ERP ni se fusionan catálogos. La relación solicitud→fuentes usa identificadores explícitos verificados; no inferencia léxica.

## Decisión

Extender las tablas/cola y fragmento autenticado existentes. La entrada privada `local_ai_route` y cliente SSH no abren endpoints públicos ni requieren copiar credenciales de ERP a nodos. El ejecutor usa comandos forzados y modelos fijos. Las solicitudes no compatibles se registran como escaladas, sin fallback pagado. El chat nativo READ conserva sus permisos/herramientas y no se redirige por palabras clave: sus contratos transaccionales y documentos requieren su propio control.

## Límites

PG local acredita esquema y pruebas, no cobertura de producción. El piloto publicado ya acredita598tareas→34casos; su estado fresco y solicitudes de aceptación se verifican durante despliegue. Una extracción de citas literales no prueba correspondencia semántica perfecta; resúmenes requieren revisión humana. No se atribuye ahorro económico o de cuota sin medición comparable.
