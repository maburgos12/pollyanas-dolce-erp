# Operacion Git para agentes IA

Este documento es obligatorio para Codex, Claude y cualquier otra IA que trabaje
en este repo. El objetivo es evitar ramas contaminadas, commits con artefactos y
deploys con cambios ajenos al pedido.

## Arranque obligatorio

Antes de cambiar cualquier archivo:

```bash
git status --short --branch
git log --oneline --decorate -5
```

La IA debe reportar:
- rama actual
- si esta ahead o behind de origin
- archivos modificados rastreados
- archivos nuevos sin rastrear
- si los cambios pendientes pertenecen o no a la tarea solicitada

## Rama correcta

Regla base:
- tarea nueva: iniciar desde `main` actualizado
- correccion sobre PR/rama existente: usar esa rama solo si Mauricio lo indica
- produccion urgente: confirmar primero si se trabajara en `main`, hotfix o rama
  temporal

Flujo recomendado para tarea nueva:

```bash
git checkout main
git pull origin main
git checkout -b fix/descripcion-corta
```

No iniciar una tarea nueva si `git status --short` muestra cambios pendientes.
Primero clasificar y pedir confirmacion.

## Clasificacion de archivos pendientes

1. Codigo de producto: Python, templates, URLs, tests, comandos, servicios.
2. Configuracion versionable: `.gitignore`, workflows, docs operativos.
3. Artefactos temporales: capturas `.png`, logs `.txt`, dumps `.html`,
   salidas `.yml` de Playwright/MCP, archivos de depuracion y carpetas
   `.playwright-mcp/`.
4. Runtime local: logs bajo `storage/`, caches, backups, salidas de jobs.

Solo los grupos 1 y 2 pueden entrar a commit. Los grupos 3 y 4 deben moverse a
`_archive/` o ignorarse.

## Antes de commitear

Obligatorio:

```bash
python manage.py check
python manage.py migrate --check
git status --short
git diff --stat
```

Si hay cambios fuera del alcance, no commitear. Usar staging quirurgico por
archivo:

```bash
git add ruta/archivo1 ruta/archivo2
git diff --cached --stat
git commit -m "Mensaje descriptivo"
```

## Reglas de limpieza

- No usar `git reset --hard` ni `git checkout --` para limpiar cambios sin
  confirmacion explicita de Mauricio.
- No borrar archivos sin confirmar; mover artefactos a `_archive/` si se deben
  preservar.
- No subir capturas ni dumps salvo que Mauricio lo pida expresamente.
- Si una rama tiene commits locales no publicados, no cambiar de rama sin
  reportarlo.

## Cierre de tarea

Al terminar, reportar:
- rama final
- commits creados
- pruebas ejecutadas
- archivos que quedaron pendientes y por que
- si se hizo push/deploy, URL o comando de verificacion usado
