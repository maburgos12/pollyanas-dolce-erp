# Capacidad Efectiva y Runtime Minimo de Agente ERP

Fecha de corte: 2026-04-09

## Objetivo

Evitar que el repositorio siga prometiendo mas de lo que ejecuta y definir la estructura minima para que un agente sea operable con trazabilidad.

## Evidencia base

Este documento usa como base verificable:

- [AGENTS_RUNTIME_SNAPSHOT.json](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/AGENTS_RUNTIME_SNAPSHOT.json)
- [MATRIZ_CATALOGO_RUNTIME_AGENTES_ERP.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/MATRIZ_CATALOGO_RUNTIME_AGENTES_ERP.md)
- [orquestacion/catalog.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/catalog.py)
- [orquestacion/services/rule_runners.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/rule_runners.py)
- [api/ai_gateway_services.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/api/ai_gateway_services.py)
- [pos_bridge/management/commands/setup_celery_schedules.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/management/commands/setup_celery_schedules.py)

## Hallazgo clave

El snapshot actual confirma tres cosas a la vez:

1. ningun `AgentDefinition.allowed_tools_json` hace match exacto literal con las keys reales del AI Gateway
2. ya existe un binding runtime minimo para goals mediante `supported_goal_types_json`, `context_files_json`, `memory.md` y `tool_hints` en [agent_runtime.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/agent_runtime.py)
3. ya existe una primera capa formal de alias `catalogo -> gateway` en [tool_binding.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/tool_binding.py), y el snapshot ya reporta `resolved_gateway_tool_matches`

La conclusion correcta es que el binding formal con gateway ya comenzo, pero sigue incompleto; el runtime minimo ya no es solo metadata.

## Definicion propuesta de capacidad efectiva

Una capacidad solo debe llamarse efectiva cuando cumple todas las condiciones aplicables:

1. esta declarada en catalogo
2. tiene un camino ejecutable real
3. tiene evidencia observable
4. respeta RBAC y aprobaciones
5. su estado puede distinguirse entre manual, activo o programado

## Niveles de readiness

### 1. `catalog_only`

Existe en catalogo o modelo, pero no hay evidencia de runner, scheduler o binding real.

### 2. `runtime_partial`

Existe ejecucion real, pero no esta completamente conectada con memoria, gateway, scheduler o contrato fuerte por agente.

### 3. `runtime_operable`

Existe camino ejecutable, evidencia, observabilidad y gobierno suficiente para operar de forma controlada.

### 4. `runtime_programado`

Ademas de ser operable, corre por schedule o trigger real y auditable.

## Estado actual por agente

| Agente | Estado derivado | Motivo |
|---|---|---|
| `director_operativo` | `runtime_partial` | coordina `operational_chain_review`, pero no tiene runner primario de reglas ni scheduler propio del runtime |
| `agente_demanda_ventas` | `runtime_partial` | tiene 1 regla primaria con runner y schedule |
| `agente_produccion` | `runtime_partial` | tiene 1 regla primaria con runner y schedule y 1 goal handler (`production_readiness_guard`) |
| `agente_compras` | `runtime_partial` | tiene 1 regla primaria con runner y schedule y 1 goal handler (`purchase_review_guard`) |
| `agente_conciliacion` | `runtime_partial` | tiene 2 reglas primarias con runner y 1 goal handler (`reconciliation_guard`) |
| `agente_publicacion_eventos_ventas` | `runtime_partial` | no tiene reglas, pero si un goal handler real (`sales_event_publication_guard`) con checkpoints, memoria y publish path |

Ningun agente llega aun a `runtime_operable` pleno porque faltan:

- escritura automatica/gobernada de memoria persistente
- binding formal a tools efectivas
- contrato operativo uniforme entre catalogo, runtime y gateway

## Contrato minimo de runtime de agente

### Entrada

- `agent_code`
- `trigger_source`
- `objective`
- `context_snapshot`
- `memory_snapshot`
- `allowed_capabilities_effective`
- `approval_policy`

### Contexto

Debe combinar:

- contexto transaccional del caso
- datos fuente consultados
- restricciones RBAC y de sucursal
- gaps conocidos relevantes

No debe incluir transcript completo como si fuera memoria.

### Memoria

Debe venir de memoria estable validada, no de logs sin depurar.

### Resolucion de tools

Debe existir una capa explicita que resuelva:

- tools declaradas en catalogo
- tools reales del gateway
- operaciones permitidas por rol y por agente

Hoy ya existe una version parcial para el runtime minimo:

- `supported_goal_types_json` limita que goals puede ejecutar un agente
- `context_files_json` y `GOAL_CONTEXT_FILES` resuelven skills/contexto
- `load_agent_memory()` carga `memory.md`
- `resolve_tool_registry()` combina runtime base, skills documentales, `allowed_tools_json`, alias de gateway y `tool_hints`
- `resolve_gateway_tool_alias()` resuelve aliases historicos `api.*` a keys reales `erp.*` solo cuando la equivalencia es clara

Mientras no exista cobertura total ni enforcement contra RBAC/runtime externo, `allowed_tools_json` no debe tratarse todavia como enforcement fuerte sobre integraciones externas.

### Loop minimo

1. cargar input y contexto
2. cargar memoria estable aplicable
3. resolver capacidades efectivas
4. validar RBAC, sucursal y aprobacion
5. ejecutar runner o tool permitida
6. persistir `OrchestrationRun`, `AgentTask`, `AgentSuggestion` o `AgentExecutionLink` segun aplique
7. devolver salida estructurada con evidencia

### Salida estructurada

- `status`
- `agent_code`
- `action_taken`
- `evidence_refs`
- `approval_state`
- `next_step`
- `gaps_detected`

## Componentes ya listos

- persistencia transaccional: corridas, tareas, sugerencias, ejecuciones y gaps
- runners reales para 5 reglas
- runtime minimo con 5 goal handlers y delegacion persistida
- gateway con manifest, invoke, approvals y execute
- dashboard y admin del bounded context

## Componentes faltantes

- binding formal `AgentDefinition -> tools efectivas`
- versionado de prompt realmente usado en ejecucion
- resolucion uniforme de capacidades efectivas
- estado de readiness visible desde codigo o dashboard
- escritura gobernada de memoria de largo plazo

## Checklist

- [x] definida la nocion de capacidad efectiva
- [x] documentados niveles de readiness
- [x] definido contrato minimo de runtime
- [x] explicitado el gap de binding con tools del gateway

## Riesgos

- seguir usando metadata declarativa como si fuera enforcement real
- mezclar memoria, transcript y estado transaccional
- prometer agentes autonomos sin loop uniforme de ejecucion

## Rollback

- mantener este contrato como documento rector hasta que exista implementacion directa en codigo
- no activar enforcement automatico hasta que se valide contra los flujos actuales
