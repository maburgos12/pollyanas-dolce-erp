# Herramientas de operación

Leer [AGENTS raíz](../AGENTS.md) y [ciclo de trabajo](../docs/agents/workflow.md).
Para despliegue/migraciones, leer además [procedimiento de producción](../docs/agents/deployment.md).
Usar `task_workspace_start.sh`, preflight y el cierre registrado; no sustituirlos
por creación/borrado manual ni eludir bloqueos de propiedad, revisión o respaldos.
No cambiar scripts, configuración operativa, contratos o protecciones sin el
alcance y autorización correspondientes. El runner de CI se selecciona en CI;
no modificar el runner de producción ni omitir pruebas para acelerar una tarea.
