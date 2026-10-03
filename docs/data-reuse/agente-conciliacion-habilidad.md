# Ficha de fuentes — habilidad ejecutable de conciliación

3 de octubre de 2026. Diseño aprobado por Mauricio en este hilo: habilidad dentro del agente ERP, revisar expedientes, reutilizar evidencia, decidir siguiente paso y registrar decisiones; sin sincronizaciones automáticas ni mutaciones de fuentes operativas.

## Necesidad y unidad de análisis

Una revisión explícita de un ProductInventoryAuditCase. El resultado es diagnóstico y siguiente paso; completar la revisión no significa cerrar el expediente ni el mes. Separar contexto versionado, evidencia transaccional vigente y registros de ejecución.

## Fuentes candidatas

| Concepto | Modelo / fuente | Crea/actualiza | Clave/ámbito | Evidencia | Consumidor |
| --- | --- | --- | --- | --- | --- |
| Agente | orquestacion.AgentDefinition | catálogo/flujo existente | code agente_conciliacion único | VPS id5 activo, supported_goal_types=[reconciliation_guard], solo skill runtime foundation | agent_runtime |
| Habilidad | .agent/skills | Git, revisión y deploy oficial | ruta/version de skill | runtime build_agent_context lee archivos declarados; no sigue enlaces | contexto persistido en OrchestrationRun |
| Expediente | ProductInventoryAuditCase | materializador/auditor existentes | mes/sucursal/producto únicos | 3893,3911,3963,3237 documentados en seguimiento | InventoryAuditAgent.investigate_case |
| Cobertura mensual | ProductInventoryAuditRun | materializador | mes único; status/source_issues | septiembre conserva bloqueo WASTE_SYNC_COUNT_MISMATCH/JOB_MIXED | decisión de revisión |
| Historial canónico | PointProductHistoryImport/Row | captura oficial | identidad sucursal/producto; FK_Movimiento | servicio reconcile/reconcile_many solo lee | revisión actual de cobertura |
| Evidencia documental | PointTransferLine/raw/carga FK/eventos del caso | importación y captura humana existentes | folio/detalle/FK; fecha | documentos de cuatro investigaciones, ver referencias de skill | auditor existente y contexto versionado |
| Registro de revisión | OrchestrationRun,AgentTask,AgentLoopCheckpoint,AuditLog | runtime existente | run/task IDs | no nuevo modelo/tabla | dashboard y auditoría |

Consulta: inventario_fuentes_datos --term AgentDefinition --term habilidad --term auditor --limit 8, PostgreSQL VPS, 11 candidatos (8 mostrados). Consulta acotada AgentDefinition por code y _goal_handlers(): agente declarado, handlers={} en main087cbec2. Grafo indexado de tarea anterior localizó InventoryAuditAgent pero no tenía source actual del runtime; se inspeccionaron archivos main de solo lectura. No se alteró catálogo operativo en diagnóstico.

## Alias y equivalencias

Codex skill local y habilidad de runtime ERP son distintas. Agente de orquestación agente_conciliacion y servicio InventoryAuditAgent son componentes distintos; la relación propuesta es invocar investigación read-only, no ejecutar run_month. Producto/insumo/SKU/eventos permanecen separados. No equivalencias nuevas de datos.

## Decisión y diseño aprobado

Reutilizar goal existente reconciliation_guard con entidad ProductInventoryAuditCase. Registrar observer sin executor y contexto obligatorio del goal, incluso para definiciones existentes. Adaptar catálogo solo del agente para nuevas instalaciones; no ejecutar seed global en producción que sobreescriba otros agentes. Validar petición review, agente activo correcto, expediente existente y contexto antes de registrar corrida. La habilidad documental y decisiones conservadoras quedan versionadas en el ERP, no dependientes de rutas del Mac.

Leer historial canónico y llamar investigate_case; no capture/run_month/materialize/persist/HTTP. Devuelve balance, missing documental conservador, estado físico, identidad, fuente mensual y próximo paso. Notas fechadas por mes+producto/sucursal externa reutilizan las investigaciones exactas; no sustituyen estados vivos ni autorizan cierre.

Runtime conserva load/observe/think/verify y cierre de revisión, con mensaje visible en dashboard existente. Cada invocación explícita tiene traza propia; segunda revisión debe dejar intactos casos, imports, filas, avisos y sugerencias. No habilitar scheduler, decisiones autónomas de escritura, nuevas herramientas de Point ni aprobación ficticia.

## Pruebas y publicación

TDD: goal registrado/contexto obligatorio; rechazo de publish y entidades/agentes erróneos; carga de conocimiento por identidad; COMPLETE sin HTTP; ventas distintas conservadas; fuente global bloquea captura aunque caso viejo BALANCED; missing vence etiqueta COMPLETE; desconocidos, cierre/apertura y negativos; segunda revisión sin mutaciones operativas. Regresión de orquestación y auditor. PostgreSQL16/Redis aislados5467/6467. CI completo, merge/deploy oficial, fresh VPS y dashboard autenticado. Sin migraciones ni alteración de permisos/maestros/mermas.

Riesgos: documentación cargada no es autonomía general; el observer solo recomienda y registro SUCCESS se refiere a revisión. SourceAuthority sigue bloqueada por merma1683114; reparar importador/restaurar permanece fuera de esta autorización. No codificar BALANCED físico ni suponer que el runtime interpreta Markdown para cambiar cálculos.
