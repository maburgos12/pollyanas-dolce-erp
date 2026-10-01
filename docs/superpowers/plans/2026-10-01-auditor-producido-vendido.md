# Auditor y Producido vs Vendido — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mostrar la conciliación persistida del auditor en Producido vs Vendido y actualizarla por eventos y respaldo diario sin recapturas, descargas Point ni ajustes operativos.

**Architecture:** ProductInventoryAuditCase es la proyección compartida por producto, sucursal y mes. InventoryAuditMaterializer y su investigación después del commit siguen siendo el único escritor de esa proyección. Los flujos de fuentes encolan revisión de los meses afectados; las pantallas sólo leen resultados y vigencia.

**Tech Stack:** Django 5, PostgreSQL 16, Celery/django-celery-beat existentes, templates Django, CSS y JavaScript nativos.

---

## Contexto y límites obligatorios

Diseño aprobado: `docs/superpowers/specs/2026-10-01-auditor-producido-vendido-design.md`. Ficha: `docs/data-reuse/2026-10-01-auditor-producido-vendido.md`.

Worktree: `/Users/mauricioburgos/Downloads/codex_worktrees/auditor-reporte-unificado`; rama `codex/reportes-auditor-unificado`. No escribir en el checkout raíz. PostgreSQL aislado: contenedor `erp_auditor_unificado_db`, puerto 55650, volumen `erp_auditor_unificado_pg`. Compose no pudo reservar otra red por agotamiento de pools: el contenedor usa la red bridge existente, sin modificar otras redes ni servicios.

Ejecutar los comandos Django con este prefijo, sin modificar settings ni .env:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55650/pastelerias_erp /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python manage.py
```

No activar `pos_bridge.monthly_product_closure`: refresca fuentes Point y no sustituye la conciliación. No cambiar modelos, migraciones, permisos, maestros, inventarios ni responsables. No cerrar septiembre si faltan fuentes. No usar tolerancia para convertir diferencias pequeñas en conciliación.

## Mapa de archivos y responsabilidades

- Crear `reportes/services_inventory_audit_report.py`: lectura mensual por producto/sucursal, estados y cobertura; sin cálculos alternativos de saldo.
- Crear `reportes/services_inventory_audit_refresh.py`: selección de meses, vigencia, exclusión de bloqueados y coordinación de reconstrucción existente.
- Extender `reportes/tasks.py`: tareas de revisión mensual y respaldo diario.
- Extender `reportes/signals.py`: eventos de fuentes después del commit, reutilizando registro de señales existente.
- Extender los escritores bulk que no emiten señales, únicamente en su punto de finalización: `pos_bridge/services/movement_sync_service.py`, `pos_bridge/services/audit_stock_history_service.py` y escritores de ventas/snapshots que se identifiquen por grafo. Avisar antes de ampliar la lista; no envolver cada renglón en un job.
- Extender `pos_bridge/management/commands/setup_celery_schedules.py`: registro diario idempotente a las 04:15.
- Extender `reportes/views_produccion.py` y `reportes/templates/reportes/producido_vs_vendido.html`: leer proyección compartida, filtros, estados, detalle, exportaciones y actualización visible.
- Crear `reportes/tests_inventory_audit_refresh.py` y `reportes/tests_inventory_audit_report.py`; ampliar `reportes/tests_producido_vs_vendido.py` y pruebas de schedules/sources pertinentes.
- No tocar navegación: se conserva la ruta. Si se modifica un estático servido por un service worker, identificar su scope y hacer el bump correspondiente en el mismo commit.

## Task 1: Lectura compartida y estados por sucursal

**Files:** crear `reportes/services_inventory_audit_report.py`, `reportes/tests_inventory_audit_report.py`.

- [ ] Escribir primero esta prueba para impedir compensación silenciosa entre sucursales:

```python
from django.test import SimpleTestCase
from reportes.services_inventory_audit_report import audit_status

class AuditStatusTests(SimpleTestCase):
    def test_opposite_branch_differences_do_not_mean_balanced(self):
        self.assertEqual(audit_status(["NEEDS_EXPLANATION", "NEEDS_EXPLANATION"]),
                         "Pendiente de conciliar")
    def test_missing_source_wins_over_a_balanced_branch(self):
        self.assertEqual(audit_status(["BALANCED", "SOURCE_INCOMPLETE"]),
                         "Falta información")
    def test_no_cases_is_not_a_zero_balance(self):
        self.assertEqual(audit_status([]), "Aún no auditado")
```

- [ ] Ejecutar `manage.py test reportes.tests_inventory_audit_report --keepdb`; confirmar fallo por ausencia de la función antes de escribirla.
- [ ] Implementar la regla única:

```python
def audit_status(statuses):
    statuses = set(statuses)
    if not statuses:
        return "Aún no auditado"
    if "SOURCE_INCOMPLETE" in statuses:
        return "Falta información"
    if statuses <= {"BALANCED", "RESOLVED"}:
        return "Conciliado"
    return "Pendiente de conciliar"
```

- [ ] Añadir pruebas ORM con dos PointBranch vinculadas a sucursales ERP, un PointProduct y expedientes persistidos con diferencias +2 y −2. Crear el run usando los defaults del modelo, no simular la consulta con mocks. Verificar filtro por sucursal y que el estado global siga pendiente aunque la diferencia sumada sea cero.
- [ ] Leer `ProductInventoryAuditRun.objects.filter(month=month).first()` y expedientes con `select_related("branch__erp_branch", "product", "run")`, filtrados por mes. Reutilizar `canonical_point_branch_identity()` para elegir la identidad canónica y no sumar alias duplicados.
- [ ] Agrupar por PointProduct, conservar case IDs y enlaces `inventory_audit_case`. Tomar inicial, movimientos, saldo esperado, final y diferencia del expediente, nunca recalcular desde otro servicio. Para componentes sin cobertura, devolver `None`; no ocultar el resto de componentes cuya fuente sí está acreditada. Si el último run fue incompleto tras un éxito, conservar números del éxito pero indicar fecha anterior y pendiente de actualización.
- [ ] Resolver sólo enlaces producto/receta comprobados por código/alias vigente. PointProduct no tiene FK directa a Receta. No llamar un resolvedor que elija el primero de varios candidatos; una colisión mantiene producto visible sin costo atribuido. No crear equivalencias ni fusionar productos.
- [ ] Probar producto sin receta, conversión sin origen, fuente incompleta, saldo final disponible sin inicial, RESOLVED con evidencia humana y diferencia no nula. Las cantidades persistidas deben permanecer idénticas después de leer.
- [ ] Ejecutar pruebas, check y commit quirúrgico del servicio y pruebas. No conectar la vista hasta que estos contratos estén verdes.

## Task 2: Coordinación de actualización sin Point

**Files:** crear `reportes/services_inventory_audit_refresh.py`, `reportes/tests_inventory_audit_refresh.py`; ampliar `reportes/tasks.py`.

- [ ] Escribir pruebas de exclusión de mes bloqueado, llamadas duplicadas, sincronización activa y ejecución sin fuentes nuevas. Verificar que el coordinador no inicia sesión Point ni llama un refresco HTTP.
- [ ] Reutilizar `months_in_range`, `month_start`, `snapshot_affected_months` y `lock_product_month_sources` de `pos_bridge/services/product_month_source_mutex.py`. No crear otro namespace/orden de locks. El materializador bloquea mes anterior y actual en orden determinista.
- [ ] Definir `refresh_inventory_audit_month(month, force=False)` con entrada ISO normalizada. Dentro de una transacción y locks existentes, comprobar `ProductoMonthClosure.objects.filter(month_start=month, is_locked=True).exists()`. Devolver `{"status": "locked", "month": month.isoformat()}` sin escribir si está bloqueado.
- [ ] Comprobar vigencia con fingerprints de fuentes locales: jobs finalizados y registros relevantes del intervalo, incluidos conteo y última actualización; snapshots inicial/final y sus ventanas canónicas. No usar solamente hora del job, porque carga/recepción e historial pueden cambiar después. Reutilizar caché existente para comparar revisiones; una entrada ausente fuerza revisión, nunca prueba vigencia. Un fallo de caché no autoriza saltarse reconstrucción necesaria.
- [ ] Si hay escritor/sync activo para el intervalo, devolver `deferred` y mantener el último resultado. No convertir la condición transitoria en una pérdida de datos o un cierre confirmado. Reutilizar el guard de fuentes del materializador para la comprobación final bajo lock.
- [ ] Ejecutar exclusivamente `InventoryAuditMaterializer().rebuild(month)` y su investigación existente después del commit. Guardar la revisión de fuentes sólo si terminó el intento; nunca antes de reconstruir ni después de un rollback. No llamar a la investigación otra vez si el rebuild ya la ejecuta.
- [ ] Definir dos tareas en `reportes/tasks.py`, con imports diferidos:

```python
@shared_task(name="reportes.refresh_inventory_audit_month")
def refresh_inventory_audit_month_task(month):
    from reportes.services_inventory_audit_refresh import refresh_inventory_audit_month
    return refresh_inventory_audit_month(month)

@shared_task(name="reportes.refresh_inventory_audit_daily")
def refresh_inventory_audit_daily():
    from reportes.services_inventory_audit_refresh import inventory_audit_review_months
    return [refresh_inventory_audit_month_task(month.isoformat())
            for month in inventory_audit_review_months()]
```

- [ ] Selección diaria: mes local actual, anterior y meses de expedientes no BALANCED/RESOLVED ya existentes. Excluir bloqueados, ordenar y deduplicar. No recorrer todas las ventas históricas para descubrir meses. Las revisiones pendientes sin nueva evidencia no deben repetir notificaciones ni escrituras de cantidades.
- [ ] Probar reejecución idempotente, fallo/rollback, caché ausente, evento de mes histórico y protección de resoluciones humanas sin cambio de evidencia. Correr `manage.py test reportes.tests_inventory_audit_refresh reportes.tests_inventory_traceability_materializer reportes.tests_inventory_audit_agent --keepdb` y commit.

## Task 3: Disparadores completos, sin un job por fila

**Files:** extender `reportes/signals.py`, escritores bulk señalados en el mapa y pruebas de actualización.

- [ ] Trazar escritores mediante grafo y confirmar código vigente. Registrar una matriz modelo → escritor → fecha operativa → finalización → disparador. Cubrir ventas, producción, merma, conversiones, transferencias, cierres, historial y evidencia de Logística. Distinguir los imports bulk de guardados individuales.
- [ ] Añadir prueba con `captureOnCommitCallbacks(execute=True)` que espera sólo los meses afectados y prueba TransactionTestCase que hace rollback y no encola. No usar callbacks con valores mutables capturados por referencia.
- [ ] Centralizar en `enqueue_inventory_audit_months(months)`, que normaliza/deduplica y registra el envío después del commit. Usar una clave por mes en el caché compartido existente para coalescer lotes; no deduplicar con estado global del proceso. Si el broker falla, registrar el fallo y dejar el respaldo diario recuperar la revisión.
- [ ] Para PointSyncJob usar `parameters.start_date/end_date` comprobadas y estados finales relevantes. Evitar encolar por cambios posteriores únicamente en `result_summary`, que `run_production_sync` y `run_waste_sync` guardan después del éxito. Validar fechas ausentes/inválidas y conservar la investigación para el respaldo, no usar el día actual como reemplazo de una fecha histórica desconocida.
- [ ] Transferencias: fechas `sent_at`, `received_at`, `registered_at`, no sólo el rango solicitado; las actualizaciones pueden afectar meses históricos. Aprovechar los meses que el escritor ya calcula para adquirir locks.
- [ ] Snapshots: usar `snapshot_affected_months(captured_at)`; un cierre del 31 de agosto afecta agosto y septiembre. Historial: encolar sólo al terminar de guardar filas/cobertura, no con cada PointProductHistoryRow. Conversiones no deben quedar excluidas porque las señales actuales no las registren.
- [ ] Logística: carga, recepción y discrepancia deben usar su transferencia relacionada y fecha de ruta; no alterar Point ni su inventario al disparar el auditor. Reutilizar los puntos de finalización existentes, incluidos guardados bulk que no emiten señales.
- [ ] Probar un lote de varios productos que encola una sola revisión por mes, un rango que cruza mes/año, un retorno histórico, nueva conversión y recepción posterior. Verificar que los imports conservan sus cantidades y que no aparecen PointSyncJob adicionales. Check y commit de este alcance.

## Task 4: Schedule diario idempotente

**Files:** extender `pos_bridge/management/commands/setup_celery_schedules.py` y pruebas de schedules existentes.

- [ ] Escribir prueba que ejecuta el setup dos veces y cuenta una sola tarea del auditor; comprobar hora, timezone y que el cierre mensual Point permanezca desactivado.
- [ ] Añadir dentro de `handle`, usando los imports existentes:

```python
audit_cron, _ = CrontabSchedule.objects.get_or_create(
    minute="15", hour="4", day_of_week="*", day_of_month="*",
    month_of_year="*", timezone=timezone_name,
)
PeriodicTask.objects.update_or_create(
    name="reportes: auditor inventario diario",
    defaults={
        "task": "reportes.refresh_inventory_audit_daily",
        "crontab": audit_cron, "interval": None, "kwargs": "{}",
        "enabled": preserve_enabled("reportes: auditor inventario diario"),
    },
)
```

- [ ] Probar preservación de una desactivación humana. No crear una automatización de escritorio paralela ni habilitar tareas ajenas. Check y commit.

## Task 5: Consumir la conciliación en reporte y exportaciones

**Files:** extender `reportes/views_produccion.py`, `reportes/tests_producido_vs_vendido.py`.

- [ ] Escribir una prueba que persiste auditoría y verifica que `_build_context` devuelve esos saldos, no los de MonthlyPointProductBalanceService. Proteger la petición con mocks que lanzan si intenta construir/reconstruir el mes o llamar Point.
- [ ] Sustituir la fuente del contexto por el servicio de lectura de Task 1. Conservar `_group_rows`, formatos/exportaciones y costos actuales sólo cuando el enlace de receta esté confirmado. No sumar auditoría con balance legacy. Retirar métodos de diagnóstico que queden sin consumidores sólo tras comprobar callers; no hacer refactor del resto del archivo.
- [ ] Añadir filtro `branch` por sucursal ERP real, validado contra el catálogo vigente. Para un ID inválido, rechazar la petición; no volver silenciosamente a «Todas». Conservar `periodo`, `period`, `categoria` y `familia` existentes. Catálogo de meses mediante runs y fuentes existentes, sin leer filas históricas completas.
- [ ] JSON debe preservar `periodo`, `fuentes`, `rows`, `totals`; añadir `branch`, `updated_at`, `audit_status` y conteos. HTML y exportaciones leen el mismo contexto. Campos de cantidades sin evidencia se serializan como null, no como "0".
- [ ] Encabezados de CSV/XLSX/PDF incluyen sucursal y fecha de revisión; las filas incluyen transferencias/ajustes cuando sea necesario para explicar el saldo persistido. Costos sin enlace confirmado quedan sin dato. Etiquetar conciliación, no saldo cero, como criterio de estado.
- [ ] Adaptar pruebas antiguas que simulan el balance legacy para usar expedientes reales del auditor. Conservar sus regresiones: final conocido aunque falte inicial, conversiones no inferidas, totales incompletos y costos cero de merma acreditada. No borrar pruebas para conseguir verde.
- [ ] Ejecutar `manage.py test reportes.tests_producido_vs_vendido reportes.tests_inventory_audit_report --keepdb`; comprobar CSV/XLSX/PDF y JSON con los mismos filtros. Check y commit.

## Task 6: Pantalla limpia, vigente y accesible

**Files:** extender `reportes/templates/reportes/producido_vs_vendido.html`; crear estático específico sólo si separarlo reduce realmente el template y su prueba lo justifica.

- [ ] Añadir pruebas de template para filtro sucursal, última actualización, estado pendiente/no auditado y enlace a caso. Debe fallar antes de cambiar HTML. No afirmar «Conciliación Point completa» si cualquier sucursal del alcance sigue pendiente.
- [ ] Sustituir badges técnicos y párrafos explicativos repetidos por un resumen de estado, fechas inicial/final y última actualización. Concentrar diagnóstico y movimientos en el enlace de producto/sucursal, reutilizando el detalle del auditor. Mantener fuentes consultables en details, no eliminarlas de la evidencia.
- [ ] Añadir select nativo de sucursal junto al mes/categoría. Mantener el filtro en enlaces de exportación y auditoría. Usar `urlencode` y `reverse`, no insertar strings de producto sin escape.
- [ ] Mantener una sola tabla y colgroup compartido con encabezado sticky. Verificar ancestros con overflow; no duplicar una tabla para simular el header. Para numéricos usar alineación derecha y tabular-nums. El detalle debe ser accesible por teclado; estado no depende sólo del color.
- [ ] Actualización abierta: consultar el endpoint JSON existente cada 60 segundos sólo con página visible, sin peticiones superpuestas. Comparar `updated_at`; no renovar si no cambió. Ante cambio, actualizar las regiones del reporte conservando filtros, focus y scroll; un error mantiene el último resultado y avisa de actualización pendiente. No reload continuo, no HTML pesado/Point en cada comprobación y no sustituir el contenido mientras el usuario cambie filtros.
- [ ] Probar estado vacío y actual: ausencia de auditoría se muestra «Aún no auditado»; futuro cierre de un mes activo no aparece como saldo cero. Mantener la distinción entre movimiento registrado y cierre confirmado.
- [ ] Validar en navegador local a 1280px y 390px: headers alineados y fijos al scroll, cambio de mes/sucursal, exportaciones, detalle, red/console y renovación con fecha nueva. Si el scope de SW intercepta esta página, bump y prueba de cliente con cache anterior. Check y commit.

## Task 7: Revisión, producción y cierre responsable

- [ ] Revisar diff completo contra diseño/ficha; confirmar que no hay modelos/migraciones, otro importador ni cambios operativos. Ejecutar `manage.py check`, `manage.py migrate --check` y los módulos de pruebas afectados. No omitir CI completo obligatorio.
- [ ] Tomar baseline de producción sólo lectura: número/estado de expedientes de septiembre, fingerprints de cantidades, notificaciones, resoluciones humanas y jobs Point. Guardar temporalmente fuera de Git. No usar constantes del documento como comprobación circular.
- [ ] Crear PR borrador con resumen, archivos, pruebas y faltantes; adjuntarlo al chat. Esperar gate completo para el SHA actual, revisar y mergear. No push directo a main.
- [ ] Desplegar con `bash scripts/deploy_web_safe.sh` en `/opt/pastelerias-erp`, sin git pull previo. Registrar exclusivamente el schedule autorizado del auditor; si el setup general alteraría schedules ajenos, usar el registro exacto de Task 4 desde Django, no ejecutar el setup completo como atajo.
- [ ] Verificar tareas registradas en worker/beat y cron 04:15 America/Mazatlan. Ejecutar septiembre una vez usando exclusivamente fuentes guardadas y repetir. Comparar baseline: cambios de conciliación sólo cuando nueva evidencia los respalde, ninguna modificación en movimientos Point/Logística y ningún job de descarga adicional. No forzar conciliación de los 76/51 casos descritos en el diseño.
- [ ] Navegador autenticado: comparar auditor y Producido vs Vendido para septiembre/sucursal, verificar estado global sin compensaciones y próxima actualización. Revisar consola y Network/XHR; HTTP 200 no basta. Confirmar exportaciones reales y estado de pantalla abierta tras una revisión.
- [ ] Si siguen faltando fuentes, entregar lista por sucursal con evidencia y siguiente paso, sin afirmar mes cerrado. No asignar responsables inexistentes.
- [ ] Cerrar sólo después de merge/deploy/validación mediante `task_workspace_close.sh --state merged`; detener únicamente el PostgreSQL propio. Conservar tareas ajenas. Si aún falta ejecución, usar handoff con commit, entorno y paso preciso.

## Revisión del plan

Cobertura: fuentes y no duplicación (Tasks 1–3), automatización (2–4), pantalla/exportaciones (5–6), evidencia y validación productiva (7). No hay autorizaciones automáticas de pérdidas/cierres. No hay nuevas equivalencias de identidad. La ejecución continúa en el worktree registrado; no requiere otra rama ni volver a aprobar el mismo diseño.
