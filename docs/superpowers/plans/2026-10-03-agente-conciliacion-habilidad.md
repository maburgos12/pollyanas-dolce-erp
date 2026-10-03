# Integración de habilidad en agente — plan de implementación

**Objetivo:** ejecutar la revisión read-only aprobada de un expediente usando contexto versionado y servicios existentes; registrar decisión sin alterar fuentes operativas.

**Arquitectura:** goal reconciliation_guard de agente_conciliacion; observer read-only con contexto obligatorio. InventoryAuditAgent y AuditStockHistoryService son lectores reutilizados; OrchestrationRun/Task/Checkpoint/AuditLog registran cada revisión. Sin executor ni scheduler.

**Stack:** Django5/PostgreSQL16, skill Markdown/JSON dentro de .agent; pruebas con unittest/Django.

## Secuencia y aceptación

- [ ] Crear pruebas en orquestacion/tests_inventory_reconciliation_runtime.py: run_agent_goal(Goal(goal_type='reconciliation_guard',entity_id=case.pk,objective='Revisar')) debe terminar revisión, cargar skill y no mutar case; actualmente falla goal no soportado.
- [ ] Ver rojo en PostgreSQL aislado con manage.py test orquestacion.tests_inventory_reconciliation_runtime --keepdb --noinput.
- [ ] Añadir orquestacion/services/inventory_reconciliation.py con validación review y observer; registrar en agent_runtime._goal_handlers y GOAL_CONTEXT_FILES. Registrar habilidad/runtime_hint no ejecutores comerciales. Mantener decisiones separadas de cierre.
- [ ] Incorporar .agent/skills/42-domain-inventory/skill-point-inventory-reconciliation: SKILL/procedimiento/continuidad/evidencias y notas estructuradas fechadas; cargar todos los archivos requeridos explícitamente, no confiar en enlaces Markdown del loader.
- [ ] Catálogo del agente declara goal/contexto; runtime funciona con definición VPS existente sin seed global. Revisión rechaza agente inactivo, entidad incorrecta o publish antes de crear corrida.
- [ ] Ver verde y ampliar negativos: fuente mensual incompleta, ventas comerciales diferentes, historia COMPLETE/unknown/coverage, missing documental con saldo0, notas equivocadas de dominio/sucursal, contexto ausente. No requests ni callbacks capture/persist.
- [ ] Regresión orquestacion/reportes auditor/historial; checks/migratecheck/makemigrationscheck sin cambios de modelo. Diff quirúrgico y commit.
- [ ] PR y CI completo SHA actual; merge/deploy_web_safe oficial; fresh VPS HEAD/check/migrate; dos revisiones HTTP prohibido con casos reales y conteos operativos intactos; mensajes en dashboard autenticado.
- [ ] Cierre oficial de task exacta y Docker down solo proyecto local, sin borrar volúmenes; audit y prune dry-run.

Ejecución en este hilo, sin delegación. El siguiente paso se observa en observation.next_step y en mensaje de corrida; nunca se aplica automáticamente. Documentar resultado y limitaciones reales, no llamar cierre mensual a revisión SUCCESS.

Avance local: primeras seis etapas implementadas con ciclos rojo/verde. Regresión 85 pruebas PASS antes de la revisión final; check0, migratecheck0 y makemigrations sin cambios. Se reutilizó también CLI run_agent_goal, extendido con --entity-type explícito. Identidades externas de cuatro expedientes comprobadas read-only en VPS; Matriz3237 es external1, no PK10. Faltan CI/publicación/verificación real y cierre de tarea; esta nota no afirma deploy.
