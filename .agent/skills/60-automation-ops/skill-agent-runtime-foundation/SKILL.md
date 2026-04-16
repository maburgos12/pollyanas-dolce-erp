# skill-agent-runtime-foundation

## Objective
Definir y reforzar el contrato mínimo de un agente real dentro del ERP: objetivo, loop, contexto, memoria y herramientas, diferenciando claramente entre capa documental y runtime ejecutable.

## Contract

### 1. Goal
El agente recibe un objetivo explícito, no una conversación libre.

Campos mínimos:
- `goal_type`
- `objective`
- `entity_type`
- `entity_id`
- `requested_action`
- `metadata`

### 2. Autonomy / Agent Loop
El agente debe entrar en un loop ejecutable y auditable:
1. cargar objetivo
2. cargar contexto
3. cargar memoria
4. resolver herramientas
5. observar estado vivo
6. decidir siguiente acción
7. ejecutar
8. verificar
9. cerrar o bloquear

Sin loop ejecutable no existe agente real; solo existe documentación o tooling parcial.

### 3. Context
El contexto vive en Markdown y se carga en orden determinista.

Orden base:
1. `AGENTS.md`
2. `.agent/skills/README.md`
3. `.agent/skills/00-core/skill-erp-context/SKILL.md`
4. `.agent/skills/00-core/skill-director-general-mode/SKILL.md`
5. skill específico del objetivo

Regla:
- si falta un archivo, el runtime debe dejar evidencia
- el contexto no sustituye observación viva del ERP

### 4. Memory
La memoria persistente vive en `memory.md`.

Uso correcto:
- recordar hechos estables
- recordar errores recurrentes
- recordar gaps estructurales

Uso incorrecto:
- usar memoria como evidencia transaccional
- usar memoria como log completo
- ignorar divergencias con el estado vivo

### 5. Tools
Las herramientas del agente se resuelven en runtime.

Tipos mínimos:
- `skill` documental
- `orm` / `python_callable` ejecutable
- `shell` / scripts
- `mcp` si existe binding real

Regla:
- no presentar una tool como operativa si solo está declarada en Markdown

## Runtime boundary

### Documentación
Debe definir:
- intención
- guardrails
- orden de lectura
- bloqueos
- criterios de salida

### Código
Debe ejecutar:
- `Goal`
- `ContextAssembler`
- `MemoryLoader`
- `ToolRegistry`
- `AgentLoopRunner`
- `Checkpoint/Audit`

## First production use case
El primer caso operativo del runtime es:
- `sales_event_publication_guard`

Debe poder:
- revisar evento comercial
- validar taxonomía
- validar workflow
- validar artifacts y financieros
- bloquear publicación si hay hallazgos
- publicar solo si el estado es liberable
- dejar checkpoints y auditoría

## Non-negotiable honesty rule
No llamar “agente real completo” a algo que:
- no cargue memoria en runtime
- no tenga loop ejecutable
- no tenga tools binding ejecutable
- no deje estado persistente y checkpoints
