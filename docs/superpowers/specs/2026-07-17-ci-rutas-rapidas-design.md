# Diseño: rutas de CI rápidas y sin duplicación

## Objetivo

Evitar que cambios exclusivamente documentales o de higiene del repositorio
levanten PostgreSQL, instalen Django y ejecuten toda la suite. Mantener la
cobertura actual cuando un cambio sí afecta la aplicación.

## Eventos

El workflow principal `ci.yml` se ejecutará:

- en `pull_request` cuando el cambio no sea exclusivamente documental;
- en `push` únicamente sobre `main`, también omitiendo cambios exclusivamente
  documentales.

Esto elimina la ejecución duplicada causada por cada push de una rama que ya
tiene un pull request abierto.

Un workflow nuevo y corto se ejecutará cuando cambien únicamente superficies de
higiene conocidas:

- archivos Markdown;
- `.gitignore`;
- `scripts/git_workspace_preflight.sh`.

Los cambios a `.github/workflows/` no quedan excluidos del CI principal: deben
seguir pasando la validación completa porque modifican la barrera de integración.

## Validaciones

La ruta corta hará checkout y ejecutará:

- `git diff --check`;
- `bash -n scripts/git_workspace_preflight.sh` cuando el script exista.

La ruta completa conserva PostgreSQL, dependencias, `manage.py check`, revisión
de migraciones, pruebas enfocadas y la suite histórica existente.

## Seguridad y casos mixtos

Si un pull request mezcla documentación con código Python, templates,
migraciones, configuración o cualquier archivo no excluido, el CI completo se
ejecuta. La optimización solo omite la suite cuando todos los archivos cambiados
pertenecen al conjunto documental/operativo explícito.

## Criterios de aceptación

1. Un push a una rama con PR no crea un segundo CI completo.
2. Un PR solo de Markdown, `.gitignore` o preflight usa la ruta corta.
3. Un PR con cualquier cambio de aplicación conserva el CI completo.
4. Un push mergeado a `main` conserva el CI completo salvo que el commit sea
   exclusivamente documental/operativo.
5. Los workflows son YAML válido y sus filtros no se solapan de forma que un
   cambio de aplicación pueda quedarse sin pruebas.
