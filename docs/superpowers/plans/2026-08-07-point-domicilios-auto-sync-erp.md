# Point Delivery Auto-Sync ERP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Work test-first and do not push, merge or deploy from an implementation subagent.

**Goal:** Importar automáticamente cada nota que Point clasifica como servicio a domicilio, crear/reutilizar la orden canónica del ERP sin duplicados y exponer salud y cierre de captura a Centro Operativo.

**Architecture:** Una tarea Celery periódica abre una sesión Point, consulta la bandeja oficial con ventana solapada, normaliza nota/cliente/dirección y llama al servicio existente `link_point_note`. `PK_Nota` continúa siendo la identidad. El canal inicial `POR_CONFIRMAR` bloquea reparto hasta que el endpoint de intake lo confirme.

**Tech Stack:** Django 4, PostgreSQL advisory locks, Celery/Beat, requests, unittest de Django.

---

## Task 1: Contrato de bandeja de domicilios Point

**Files:**
- Create: `pos_bridge/services/point_delivery_note_service.py`
- Create: `pos_bridge/tests/test_point_delivery_note_service.py`
- Modify: `pos_bridge/services/point_note_detail_service.py`
- Modify: `pos_bridge/tests/test_point_note_detail_service.py`

1. Escribir pruebas fallidas para parsing estricto de `/Ventas/getNotasByDateServicioDomicilio`, `getDataClienteById`, búsqueda exacta por `Pk_cliente`, teléfono alterno y reutilización de una sesión.
2. Ejecutar:

```bash
python3 manage.py test pos_bridge.tests.test_point_delivery_note_service pos_bridge.tests.test_point_note_detail_service --keepdb --noinput
```

Esperado inicial: `FAIL` por módulo/método inexistente.
3. Extraer `fetch_with_session(session, pk_nota)` en el servicio de detalle sin cambiar `fetch(pk_nota)`.
4. Implementar dataclasses inmutables y validación de campos. El listado solo acepta notas devueltas por el endpoint de domicilios.
5. Repetir el comando hasta `OK` y salida 0.

## Task 2: Canal pendiente y protección de asignación

**Files:**
- Modify: `crm/models.py`
- Create: `crm/migrations/0009_pedidocliente_canal_por_confirmar.py`
- Modify: `crm/services/point_order_link.py`
- Modify: `crm/tests_point_order_link.py`
- Modify: `logistica/services_domicilio_assignment.py`
- Create: `logistica/migrations/0051_alter_solicituddomicilio_canal_origen.py`
- Modify: `logistica/tests_domicilios_omnicanal.py`

1. Probar que el dominio acepta `POR_CONFIRMAR` solo desde el servicio automático, que la API de captura manual lo rechaza y que asignar ese pedido sin canal o GPS produce un error controlado.
2. Crear la elección `CANAL_POR_CONFIRMAR`, el vínculo único nullable con `PK_Cliente` y las migraciones derivadas.
3. Agregar una entrada interna explícita al servicio de enlace; buscar primero cliente por `PK_Cliente`, impedir fusión de Point IDs distintos y no debilitar las validaciones públicas.
4. Ejecutar:

```bash
python3 manage.py test crm.tests_point_order_link logistica.tests_domicilios_omnicanal --keepdb --noinput
```

Esperado: `OK`, salida 0.

## Task 3: Orquestador idempotente y job Celery

**Files:**
- Create: `crm/services/point_delivery_auto_sync.py`
- Create: `crm/tests_point_delivery_auto_sync.py`
- Modify: `pos_bridge/models/sync_job.py`
- Create: `pos_bridge/migrations/0019_alter_pointsyncjob_job_type.py`
- Modify: `pos_bridge/tasks/celery_tasks.py`
- Modify: `pos_bridge/management/commands/setup_celery_schedules.py`
- Modify: `pos_bridge/tests/test_celery_schedule_setup.py`

1. Probar antes de implementar: ventana rodante de siete días, recorrido por sucursales/workspaces configurados, job de tipo `deliveries`, selección explícita/fallback seguro de cliente API, link canónico, repetición idempotente, conciliación `POINT_PENDING` preservando captura, lock concurrente, éxito parcial y sanitización de errores.
2. Implementar un advisory lock de sesión con llave constante y liberación en `finally`.
3. Resolver el `PublicApiClient` por setting de prefijo/id y validar capability `OMNICHANNEL` y `created_by` activo. Sin setting solo se acepta exactamente uno; cualquier ambigüedad falla cerrado.
4. Registrar `PointSyncJob` sin PII y devolver conteos `seen/created/existing/failed`.
5. Agregar `pos_bridge.delivery_note_sync` al catálogo de tipos y una tarea periódica de 60 segundos detrás de `POINT_DELIVERY_SYNC_ENABLED`, apagado por defecto. Ajustar la expectativa total sin modificar la contención de inventarios. El lookback será configurable con valor seguro predeterminado de siete días.
6. Ejecutar:

```bash
python3 manage.py test crm.tests_point_delivery_auto_sync pos_bridge.tests.test_celery_schedule_setup --keepdb --noinput
```

Esperado: `OK`, salida 0.

## Task 4: Intake y salud API

**Files:**
- Modify: `api/omnichannel_serializers.py`
- Modify: `api/omnichannel_views.py`
- Modify: `api/urls.py`
- Modify: `api/tests_omnichannel.py`

1. Probar autorización por `PublicApiClient`, aislamiento, estados health `NEVER_RUN/RUNNING/SUCCESS/PARTIAL/FAILED`, actualización atómica de canal, referencia social, completar solo vacíos, GPS como par, replay igual, conflicto al sobrescribir y carrera PostgreSQL intake/asignación.
2. Implementar:

```text
GET   /api/public/v1/omnichannel/point-delivery-sync/health/
PATCH /api/public/v1/omnichannel/deliveries/<id>/intake/
```

3. Reutilizar `_serialize_delivery_detail` como respuesta del PATCH y registrar evento de auditoría sin PII sensible.
4. Ejecutar:

```bash
python3 manage.py test api.tests_omnichannel --keepdb --noinput
```

Esperado: `OK`, salida 0.

## Task 5: Regresión, migraciones y verificación ERP

1. Ejecutar:

```bash
python3 manage.py check
python3 manage.py makemigrations --check --dry-run
python3 manage.py migrate --check
python3 manage.py test \
  pos_bridge.tests.test_point_delivery_note_service \
  pos_bridge.tests.test_point_note_detail_service \
  pos_bridge.tests.test_celery_schedule_setup \
  crm.tests_point_order_link \
  crm.tests_point_delivery_auto_sync \
  logistica.tests_domicilios_omnicanal \
  api.tests_omnichannel \
  --keepdb --noinput
```

2. Verificar con una base PostgreSQL aislada que dos invocaciones producen una sola orden/domicilio.
3. Revisar diff, migraciones y ausencia de PII/secretos.
4. Commit sugerido:

```bash
git add pos_bridge crm logistica api docs/superpowers
git commit -m "feat(domicilios): sincroniza notas Point automaticamente"
```
