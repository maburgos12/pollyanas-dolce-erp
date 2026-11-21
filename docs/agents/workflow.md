# Ciclo de trabajo y validación

Este procedimiento amplía [AGENTS raíz](../../AGENTS.md). No amplía autorización.
Una consulta/diagnóstico es de sólo lectura. Un prototipo debe quedar aislado del
ERP real; implementación requiere los gates de aplicación cuando correspondan.

## Inicio y propiedad

1. Identificar resultado esperado, aceptación observable, carpetas permitidas,
   consumidores, contratos, riesgos, pruebas y si requiere producción.
2. Ejecutar `bash scripts/task_workspace_audit.sh --repo RUTA_CHECKOUT_BASE`.
   Revisar rama, limpieza, SHAs y registro; no limpiar recursos ajenos.
3. Crear exclusivamente con `scripts/task_workspace_start.sh`. El script hace
   fetch/fast-forward seguro del checkout raíz bajo bloqueo, fija SHA y registra
   tarea, dueño, rama, worktree y alcance. No usar `git worktree add` como alternativa.
4. Desde el worktree, ejecutar `bash scripts/git_workspace_preflight.sh --write`
   antes de editar. Detenerse si falla. Para continuar un worktree heredado válido,
   usar `task_workspace_adopt.sh`; no adoptar árboles sucios ni detached HEAD.

```bash
bash scripts/task_workspace_start.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 \
  --root /Users/mauricioburgos/Downloads/codex_worktrees --task TAREA \
  --branch codex/modulo-descripcion --owner PROPIETARIO --scope ALCANCE
cd /Users/mauricioburgos/Downloads/codex_worktrees/TAREA
bash scripts/git_workspace_preflight.sh --write
```

El checkout base permanece en `main`, limpio y sólo como plano de control.
No escribir allí, hacer cambios directos a main, cambiar ramas de otros agentes,
mezclar tareas, usar reset/stash sobre cambios ajenos ni omitir preflight. Ante
suciedad o divergencia, conservar evidencia y aislar el trabajo sin pisar archivos.

## Entorno y comprobaciones proporcionales

**Sólo documentación, instrucciones o configuración Codex:** revisar contenido,
enlaces/rutas, symlinks, sintaxis TOML, comportamiento de carga cuando corresponda
y diff. No levantar bases ni ejecutar Django. Mantener el CI remoto requerido.

**Código o configuración de aplicación:** preparar PostgreSQL 16 aislado antes de
ejecutar cualquier `manage.py`. Registrar tarea, proyecto Compose único, puerto
libre y volumen exclusivo; configurar `DATABASE_URL` o `DB_HOST` y `DB_*`, confirmar
conectividad. No alterar puertos de Compose ni `.env` productivo sin autorización.
Nunca usar SQLite como alternativa. Con PostgreSQL disponible:

```bash
python manage.py migrate
python manage.py migrate --check
python manage.py check
```

Aplicar migraciones de main antes de escribir código; no avanzar con pendientes.
Antes de commit: `check` sin errores y pruebas del módulo/consumidores afectados.
Si se tocan modelos, verificar `migrate --check` y `showmigrations` de las apps
afectadas; bonos/RRHH incluyen `bonos_produccion bonos_ventas rrhh` cuando aplique.
No borrar migraciones ni modificar las ya aplicadas. Para errores de migración,
seguir [diagnóstico y autorización](deployment.md), nunca inferir permiso de fake.

## Revisión, commit y CI

Antes de cada commit/PR revisar `git status --short --branch`, historial reciente,
worktrees y diff completo frente al SHA base. Confirmar sólo archivos de la tarea;
no incluir credenciales, capturas temporales, logs, outputs, `.DS_Store`,
`.playwright-mcp` ni `.claude`. No presentar pruebas de un SHA anterior como actuales.
Antes de subir, asegurar upstream y rama coherentes con el objetivo.

PR de una sola tarea, como borrador salvo instrucción contraria; incluir resultado
funcional, archivos principales, pruebas/comandos/SHA, validación real y recursos
pendientes. Revisar aquí el diff antes de aprobar; si Mauricio pidió verlo antes
del merge, entregar revisión y esperar su autorización.

El workflow y runner vigentes son la fuente del reparto de pruebas. Exigir check
`test` exitoso del HEAD actual y PR antes del merge, también para administradores.
No hacer bypass, quitar protecciones, omitir pruebas ni cancelar otros PR/main.
Sólo cancelar ejecuciones reemplazadas del mismo PR cuando corresponda. Esperar
CI de manera acotada; distinguir cola de ejecución y no prometer tiempos fijos.
Documentación también conserva los checks remotos obligatorios.

## Entrega y cierre

Implementación autorizada: validación local → revisión/PR → CI vigente aprobado →
merge → [despliegue oficial](deployment.md) si cambia la aplicación → verificación
en el consumidor real → cierre registrado. No declarar completo por compilar,
HTTP 200, PR mergeado o filesystem actualizado. Para UI usar [validación UI](ui.md).
Para datos verificar registros/conteos y el reporte/pantalla donde se consumen.
No abrir una segunda implementación dejando pendiente el deploy de la anterior.

Toda tarea termina registrada como cerrada, entregada, bloqueada o descartada.
Usar `task_workspace_close.sh --state merged` sólo tras integración y validación
cuando apliquen: comprueba limpieza y pertenencia a main antes de retirar sus
ramas/worktree exactos. No ejecutar `git branch -D`, borrar ramas remotas ni retirar
worktrees manualmente fuera del ciclo de vida registrado y respaldado.
Para descarte, el cierre oficial crea antes un bundle recuperable. Para entrega
usar `task_workspace_handoff.sh` con responsable, motivo y fecha de revisión.
Auditar de nuevo con `task_workspace_audit.sh`; nunca limpiar por edad o cantidad.

Cumplir el cierre global de Docker local: verificar propietario, uso, ausencia de
datos únicos y respaldo recuperable cuando corresponda. Usar el helper global
`cleanup_closed_docker_tasks.py` para tareas terminales con propiedad suficiente;
no ejecutar prunes masivos ni limpiar Docker/volúmenes de servidores. Conservar
entornos principales/activos y registrar antes/después y espacio real recuperado.
No declarar cierre con recursos propios pendientes sin entrega documentada.

Para tareas explícitamente inscritas en el piloto, seguir
[software factory](../software-factory.md). Registrar aceptación, autorización,
modo según consumidor, evidencia por SHA y gates; no inscribir tareas ajenas
ni editar/borrar registros para eludirlos. `status`/`resume` no ejecutan agentes.

Resolver problemas autorizados hasta cumplir aceptación; si hay bloqueo real,
agotar alternativas seguras, completar trabajo independiente y explicar acceso o
decisión exactos faltantes. Mantener avances breves y entrega con evidencia,
limitaciones y recursos pendientes. No inventar pruebas ni salidas de producción.
