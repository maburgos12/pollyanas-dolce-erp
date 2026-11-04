# Plan de implementación: cierre mensual automático de producto

**Objetivo:** conectar los componentes existentes para que el cierre mensual se concilie, materialice, bloquee y reintente automáticamente.

**Arquitectura:** `inventario.conteos_nocturna` resuelve evidencia física; `pos_bridge.tasks.run_monthly_product_closure` coordina fuentes, candados y lock; `reportes.services_inventory_audit_refresh` materializa la vista consumida por Producido vs Vendido. No hay modelos nuevos.

## Tarea 1: automatizar el resultado del conteo

- Modificar `inventario/conteos_nocturna.py` para aceptar conteos puros de producto con diferencia cero y abrir reconteo sólo para líneas distintas.
- Ampliar `inventario/tests_conteos_nocturna.py` con ambos casos y comprobación de evidencia inmutable.

## Tarea 2: agregar candados físicos y del auditor

- Modificar `pos_bridge/tasks/run_monthly_product_closure.py` para conciliar conteos, evaluar las sucursales operativas, materializar la auditoría y persistir los bloqueos en metadata.
- Ampliar `pos_bridge/tests/test_sales_sync_tasks.py` con cobertura faltante, auditoría incompleta y cierre listo.

## Tarea 3: habilitar la programación

- Modificar `pos_bridge/management/commands/setup_celery_schedules.py` para ejecución diaria 05:00, habilitada y con `lock_after_build=true`.
- Actualizar `pos_bridge/tests/test_celery_schedule_setup.py` y `docs/RUNBOOK_CIERRE_MENSUAL_PRODUCTO_POINT.md`.

## Tarea 4: validar y publicar

- Ejecutar pruebas focalizadas, `migrate --check` y `check` en PostgreSQL aislado.
- Revisar diff, commit, PR y merge.
- Desplegar con `scripts/deploy_web_safe.sh`, ejecutar el setup de schedules si el deploy no lo hace y validar el schedule productivo.
- Ejecutar octubre 2026; verificar cierre, auditoría y pantalla autenticada. Si las fuentes o sucursales siguen incompletas, conservar `BUILT` y reportar las excepciones exactas, sin forzar el lock.
- Retirar el Compose local de esta tarea conforme al registro de cierre.
