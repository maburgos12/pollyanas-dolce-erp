# Logística: recarga Point completa en CEDIS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sincronizar y reconciliar automáticamente el estado completo de Point al regresar a CEDIS, impidiendo que el repartidor cargue un tramo con datos viejos.

**Architecture:** Separar presencia (`LLEGADA_GEOFENCE`/`VISITADA`) de recarga operativa (`RECARGA_CEDIS`). La permanencia confiable agenda una tarea Celery idempotente; esa tarea ejecuta la extracción completa de transferencias, actualiza el checklist y sólo entonces registra la recarga. El endpoint y la PWA reutilizan el mismo servicio como recuperación manual.

**Tech Stack:** Django 5, DRF, PostgreSQL, Celery, JavaScript PWA, service worker, unittest/Django TestCase.

---

## Estructura de archivos

- `logistica/services_carga_ruta.py`: sincronización completa para recarga, selección del tramo y aceptación de CEDIS visitado.
- `logistica/services_rutas_control.py`: agenda idempotente al confirmar permanencia en CEDIS.
- `logistica/tasks.py`: tarea Celery que ejecuta la recarga automática fuera de la transacción GPS.
- `api/logistica_serializers.py`: expone si la recarga CEDIS quedó resuelta.
- `logistica/templates/logistica/pwa.html`: conserva recuperación manual y muestra estados de sincronización.
- `logistica/static/logistica/pwa/sw.js`: invalida caché de la PWA.
- `logistica/checks.py`: mantiene coordinadas las versiones PWA.
- `logistica/tests_invariantes_ruta.py`: pruebas del contrato central y del incidente 53/41/41.

### Task 1: Reproducir la pérdida de Enviado/Recibido

**Files:**
- Modify: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/services_carga_ruta.py`

- [ ] **Step 1: Write the failing test for full Point refresh**

Agregar a la clase de invariantes de carga una prueba que cree un folio abierto con `requested_quantity=53`, `sent_at=None`, lo adjunte al checklist y simule que `PointMovementSyncService.run_transfer_sync()` lo actualiza a `sent_quantity=41`, `received_quantity=41`, `sent_at` y `received_at` presentes. La llamada deseada es:

```python
resumen = sincronizar_checklist_recarga_desde_point(
    ruta=self.ruta,
    user=self.user,
)
self.assertEqual(resumen.checklist.point_sync_job, successful_job)
linea.refresh_from_db()
self.assertTrue(linea.point_transfer_line.sent_at)
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
python3 manage.py test logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests.test_recarga_usa_sync_completo_y_actualiza_transferencia_cerrada --keepdb
```

Expected: FAIL because `sincronizar_checklist_recarga_desde_point` does not exist.

- [ ] **Step 3: Implement the narrow full-sync service**

Crear en `logistica/services_carga_ruta.py`:

```python
def sincronizar_checklist_recarga_desde_point(*, ruta, user=None):
    sync_job = None
    service = PointMovementSyncService()
    for fecha in [ruta.fecha_ruta - timedelta(days=1), ruta.fecha_ruta]:
        sync_job = service.run_transfer_sync(
            start_date=fecha,
            end_date=fecha,
            triggered_by=user,
        )
        if sync_job.status != sync_job.STATUS_SUCCESS:
            raise PointSyncUnavailableError(
                "No se pudo sincronizar Point para generar la recarga.",
                sync_job=sync_job,
            )
    return _actualizar_checklist_carga_desde_point(
        ruta=ruta,
        user=user,
        sync_job=sync_job,
    )
```

No cambiar la sincronización rápida de transferencias abiertas usada antes de la salida inicial.

- [ ] **Step 4: Verify GREEN and the 53/41/41 case**

Agregar un segundo test que represente 21 detalles: total solicitado 53, total enviado 41, total recibido 41, incluyendo detalles con Enviado cero y `sent_at` presente. Verificar que la suma activa esperada sea 41 y que los ceros queden `ZERO_EXPECTED`.

Run:

```bash
python3 manage.py test logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests --keepdb
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add logistica/services_carga_ruta.py logistica/tests_invariantes_ruta.py
git commit -m "fix(logistica): sincronizar transferencias completas en recarga"
```

### Task 2: Separar visita CEDIS de recarga resuelta

**Files:**
- Modify: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/services_carga_ruta.py`

- [ ] **Step 1: Write failing segment and visited-stop tests**

Crear pruebas con la secuencia `CEDIS -> sucursal -> CEDIS -> sucursal`:

```python
self.cedis_intermedio.estado = ParadaRuta.ESTADO_VISITADA
self.cedis_intermedio.save(update_fields=["estado", "actualizado_en"])
self.assertNotIn(
    self.sucursal_segundo_tramo.orden,
    _ordenes_tramo_carga_actual(self.ruta),
)
EventoRuta.objects.create(
    ruta=self.ruta,
    parada=self.cedis_intermedio,
    tipo=EventoRuta.TIPO_RECARGA_CEDIS,
    descripcion="Recarga reconciliada",
)
self.assertIn(
    self.sucursal_segundo_tramo.orden,
    _ordenes_tramo_carga_actual(self.ruta),
)
```

Agregar prueba de `registrar_recarga_cedis` con CEDIS ya `VISITADA`; debe sincronizar y crear un único evento.

- [ ] **Step 2: Run and verify RED**

Run:

```bash
python3 manage.py test \
  logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests.test_visita_cedis_no_abre_tramo_sin_recarga \
  logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests.test_recarga_acepta_cedis_visitada --keepdb
```

Expected: both FAIL against the current coupling.

- [ ] **Step 3: Change the domain predicates**

En `_ordenes_tramo_carga_actual`, sustituir el avance por `VISITADA`/geocerca con existencia de `EventoRuta.TIPO_RECARGA_CEDIS` para el CEDIS intermedio. Mantener el CEDIS inicial como inicio de ruta.

En `_validar_parada_recarga_pre_sync`, seleccionar el siguiente CEDIS sin evento de recarga, permitiendo estados `PENDIENTE`, `OMITIDA` o `VISITADA`. Rechazar únicamente CEDIS de otro tramo o con recarga ya resuelta, salvo el retorno idempotente existente.

En `_orquestar_recarga_cedis`, llamar `sincronizar_checklist_recarga_desde_point` para rutas `EN_RUTA`.

- [ ] **Step 4: Verify GREEN and adjacent route rules**

Run:

```bash
python3 manage.py test \
  logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests \
  logistica.tests.LogisticaControlRutasTests.test_tramo_carga_avanza_con_llegada_a_cedis \
  logistica.tests.LogisticaControlRutasTests.test_registrar_recarga_cedis_en_ruta_no_cierra_ni_duplica_ruta --keepdb
```

Actualizar el test legado para que espere avance con `RECARGA_CEDIS`, no con geocerca sola. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add logistica/services_carga_ruta.py logistica/tests.py logistica/tests_invariantes_ruta.py
git commit -m "fix(logistica): separar visita CEDIS de recarga reconciliada"
```

### Task 3: Automatizar la reconciliación tras permanencia

**Files:**
- Modify: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/services_rutas_control.py`
- Modify: `logistica/tasks.py`

- [ ] **Step 1: Write failing task and scheduling tests**

Probar que una transición real a `VISITADA` en un punto CEDIS agenda exactamente una tarea mediante `transaction.on_commit`, y que una señal posterior no la agenda de nuevo:

```python
with self.captureOnCommitCallbacks(execute=True):
    registrar_ubicacion_ruta(
        user=self.user,
        ruta=self.ruta,
        payload=self.payload_cedis_permanencia(),
    )
delay.assert_called_once_with(
    ruta_id=self.ruta.id,
    parada_id=self.cedis.id,
    user_id=self.user.id,
)
```

Probar la tarea directamente con el servicio de recarga simulado: éxito devuelve `estado_sync=ACTUALIZADO`; `RecargaCedisPendienteEnviado` devuelve estado revisable sin crear un evento falso de recarga.

- [ ] **Step 2: Run and verify RED**

Run:

```bash
python3 manage.py test \
  logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests.test_permanencia_cedis_agenda_recarga_una_vez \
  logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests.test_task_recarga_no_inventa_exito_si_point_pendiente --keepdb
```

Expected: FAIL because the task and hook do not exist.

- [ ] **Step 3: Implement the Celery task**

Agregar en `logistica/tasks.py` una tarea con reintento sólo para errores transitorios de base de datos:

```python
@shared_task(
    name="logistica.tasks.procesar_recarga_cedis_automatica",
    autoretry_for=(OperationalError,),
    retry_backoff=True,
    max_retries=3,
)
def procesar_recarga_cedis_automatica(*, ruta_id, parada_id, user_id=None):
    ruta = RutaEntrega.objects.get(pk=ruta_id)
    parada = ruta.paradas.get(pk=parada_id)
    user = get_user_model().objects.filter(pk=user_id).first()
    try:
        evento = registrar_recarga_cedis(
            ruta=ruta,
            parada=parada,
            user=user,
            notas="Recarga CEDIS reconciliada automáticamente al confirmar permanencia.",
        )
    except (RecargaCedisPendienteEnviado, RecargaCedisSinLineasPoint, RecargaCedisPointError) as exc:
        return {"estado_sync": exc.estado_sync, "ruta_id": ruta_id, "parada_id": parada_id}
    return {"estado_sync": evento.metadata.get("estado_sync", "ACTUALIZADO"), "evento_id": evento.id}
```

Importar el servicio y las excepciones de forma explícita.

- [ ] **Step 4: Schedule after the transaction commits**

Cuando `_marcar_visitada_por_permanencia` devuelva `True` y `parada.punto.tipo == PuntoLogistico.TIPO_CEDIS`, usar import local para evitar ciclos:

```python
from .tasks import procesar_recarga_cedis_automatica

transaction.on_commit(
    lambda: procesar_recarga_cedis_automatica.delay(
        ruta_id=ruta.id,
        parada_id=parada.id,
        user_id=getattr(user, "id", None),
    )
)
```

Pasar `user` a la función interna o devolver la transición para que el agendamiento ocurra en `registrar_ubicacion_ruta`.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
python3 manage.py test logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests --keepdb
```

Expected: PASS and no duplicate scheduling.

- [ ] **Step 6: Commit**

```bash
git add logistica/tasks.py logistica/services_rutas_control.py logistica/tests_invariantes_ruta.py
git commit -m "feat(logistica): reconciliar Point al permanecer en CEDIS"
```

### Task 4: Mantener recuperación visible en la PWA

**Files:**
- Modify: `api/logistica_serializers.py`
- Modify: `logistica/templates/logistica/pwa.html`
- Modify: `logistica/static/logistica/pwa/sw.js`
- Modify: `logistica/checks.py`
- Modify: `logistica/tests_invariantes_ruta.py`

- [ ] **Step 1: Write failing API and PWA contract tests**

Agregar `recarga_cedis_resuelta` al serializador. Debe ser `False` para CEDIS `VISITADA` sin evento y `True` cuando existe `RECARGA_CEDIS`.

Agregar prueba de plantilla que exija:

```javascript
const puedeRegistrarRecarga = !requiereEntrega && rutaEnSeguimiento && parada.recarga_cedis_resuelta !== true;
```

y que el texto del botón cambie a `Reintentar sincronización Point` cuando la parada está visitada pero no resuelta.

- [ ] **Step 2: Run and verify RED**

Run:

```bash
python3 manage.py test \
  logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests.test_serializer_separa_visita_de_recarga \
  logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests.test_pwa_conserva_reintento_hasta_recarga --keepdb
```

Expected: FAIL because the field and condition do not exist.

- [ ] **Step 3: Implement serializer and UI**

Agregar un `SerializerMethodField` que consulte una colección precargada en contexto y use una consulta puntual sólo como fallback. En `LogisticaRutaActivaView`, precalcular los IDs de CEDIS con `RECARGA_CEDIS` y pasarlos al contexto del serializador.

En la PWA, no usar `operativamente_resuelta` para ocultar la recuperación CEDIS. Mostrar un estado `Sincronización de recarga pendiente` y mantener el POST manual existente con `skipOfflineQueue: true`.

- [ ] **Step 4: Bump coordinated PWA cache version**

Incrementar el número actual de `CACHE_NAME` en `sw.js` y el marcador correspondiente en `pwa.html`/`checks.py`. No reutilizar una versión previa.

- [ ] **Step 5: Verify GREEN and cache checks**

Run:

```bash
python3 manage.py test logistica.tests_invariantes_ruta.LogisticaRecargaCedisInvariantesTests --keepdb
python3 manage.py check
```

Expected: PASS and `System check identified no issues`.

- [ ] **Step 6: Commit**

```bash
git add api/logistica_serializers.py api/logistica_views.py logistica/templates/logistica/pwa.html logistica/static/logistica/pwa/sw.js logistica/checks.py logistica/tests_invariantes_ruta.py
git commit -m "fix(logistica/pwa): conservar recarga pendiente hasta sincronizar"
```

### Task 5: Verificación integral, PR y producción

**Files:**
- Verify all modified files
- Update: `docs/superpowers/plans/2026-07-14-logistica-recarga-point-completa.md` checkboxes only if useful

- [ ] **Step 1: Run focused and full tests**

```bash
python3 manage.py test logistica.tests_invariantes_ruta --keepdb
python3 manage.py test logistica --keepdb
python3 manage.py migrate --check
python3 manage.py check
```

Expected: zero failures, zero pending migrations, zero check errors.

- [ ] **Step 2: Inspect the final branch**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
git log --oneline --decorate -8
git worktree list
```

Expected: only logistics files and the approved spec/plan; clean worktree.

- [ ] **Step 3: Push and create a draft PR**

The PR must include the incident evidence, files changed, tests executed, PWA cache bump and production recovery steps. Mark ready only after review.

- [ ] **Step 4: Merge and deploy safely**

On VPS, do not run `git pull` manually before the deployment script:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
docker compose -f docker-compose.yml exec -T web python manage.py migrate --check
docker compose -f docker-compose.yml exec -T web python manage.py check
docker compose -f docker-compose.yml exec -T web python manage.py collectstatic --noinput
```

Confirm web, worker and beat loaded the merged commit. If the repository had already been updated outside the script, restart `web worker beat` explicitly.

- [ ] **Step 5: Repair and verify `RUT-202607-0025`**

Use a guarded production command that performs: read current folio/checklist, full Point sync for the route dates, checklist reconciliation, then fresh read. Expected El Túnel totals:

```text
Solicitado: 53
Enviado: 41
Recibido: 41
```

Verify zero-sent lines remain visible, active load total is 41, `Confirmar entrega` remains available while `entrega_estado=PENDIENTE`, and no physical delivery is created by inference.

- [ ] **Step 6: Validate in a real browser**

Open production as the affected flow, inspect Console and Network, and verify:

1. CEDIS visit alone does not resolve recarga.
2. Automatic task produces a full Point sync and `RECARGA_CEDIS`.
3. Manual retry remains visible on failure.
4. The next segment shows the reconciled product totals without duplicate lines.
5. Delivery confirmation remains separate from geofence and Point receipt.

- [ ] **Step 7: Clean the branch after production proof**

After merge, deploy and validation:

```bash
git branch -D codex/logistica-recarga-point-completa
git push origin --delete codex/logistica-recarga-point-completa
git fetch --prune origin
```
