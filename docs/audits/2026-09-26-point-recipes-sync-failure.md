# Auditoría read-only: fallas de sincronización de recetas Point

Fecha de revisión: 2026-09-26
Alcance: diagnóstico solamente; no se reintentó ningún job y no se modificaron datos, recetas, Point ni producción.

## Evidencia operativa

La lectura directa de `PointSyncJob` en producción confirmó:

| Job | Estado | Inicio UTC | Fin UTC | Intento | Error registrado |
| --- | --- | --- | --- | --- | --- |
| 70603 | FAILED | 2026-09-26 19:12:49 | 2026-09-26 19:16:21 | 1 | `An error occurred in the current transaction...` |
| 70607 | FAILED | 2026-09-26 19:19:11 | 2026-09-26 19:22:50 | 1 | `An error occurred in the current transaction...` |

El último job de recetas exitoso fue el `62393`, iniciado el 2026-09-21 11:00:00 UTC y finalizado el 2026-09-21 11:00:14 UTC.

Los dos fallos son reproducibles en los logs del worker de recetas con la misma causa raíz:

```text
UniqueViolation: duplicate key value violates unique constraint
"uniq_active_insumo_codigo_point"
Key (codigo_point)=(50181900) already exists.
```

El mensaje guardado en el job no conserva esa primera excepción; registra solamente el error transaccional posterior.

## Ruta exacta del fallo

El stack de producción ubica la secuencia así:

1. `task_catalog_recipe_sync` llama `PointProductRecipeSyncService.sync`.
2. `sync` entra en `_extract_product_node`, protegido por `@transaction.atomic`.
3. `_materialize_node_lines` resuelve un componente mediante `_resolve_component`.
4. `_resolve_component` entra recursivamente en `_extract_insumo_node`, también protegido por `@transaction.atomic`.
5. Al guardar una receta se ejecuta `recetas.signals.sync_derivados_on_receta_save`.
6. `sync_receta_derivados` llama `sync_preparacion_insumo`.
7. `sync_preparacion_insumo` intenta guardar un `Insumo` activo con `codigo_point=50181900`, pero ya existe otro registro activo con ese código.
8. La señal captura/oculta la `IntegrityError` original dentro del bloque atómico.
9. La siguiente consulta, `sync_recipe_point_identity -> RecetaCodigoPointAlias.get_or_create`, encuentra la transacción marcada como rota y genera `TransactionManagementError`.

Por eso el job muestra el error transaccional genérico en lugar del conflicto real de identidad del insumo.

## Conclusión

Esta falla no pertenece a Pronósticos/Proyecciones ni a su selector de productos. Es un conflicto de identidad en la generación automática de insumos derivados de recetas. Corregirlo dentro de esta rama ampliaría el alcance y modificaría el contrato compartido de sincronización de recetas.

No se recomienda reintentar el job en producción mientras el conflicto del código `50181900` siga sin resolverse: ambos intentos fallaron de la misma forma.

## Siguiente corrección recomendada, en tarea separada

1. Crear una prueba local que reproduzca dos insumos activos compitiendo por el mismo `codigo_point` durante `sync_preparacion_insumo`.
2. Hacer que la generación del insumo derivado reconcilie primero la identidad existente o libere de forma explícita el código obsoleto, respetando el constraint `uniq_active_insumo_codigo_point`.
3. No capturar una `IntegrityError` dentro de un bloque `atomic` para luego continuar consultando; debe propagarse o aislarse en un savepoint interno que termine antes de continuar.
4. Persistir en el job la excepción raíz y el contexto mínimo sanitizado —receta, código Point y etapa— para evitar que `TransactionManagementError` oculte la causa real.
5. Verificar localmente y después realizar un único reintento controlado, con lectura antes/después del insumo `50181900` y del job generado.
