# Estabilización de Entregas Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Centralizar las transiciones de visita y entrega, permitir excepciones sin geocerca con revisión del jefe y automatizar la detección de estados imposibles sin reescribir datos históricos.

**Architecture:** `ParadaRuta` conserva estados físico y de entrega y gana una dimensión de revisión. Un nuevo servicio de dominio transaccional será la única puerta para confirmar entregas y resolver excepciones; GPS y CEDIS conservan operaciones explícitas. APIs, ERP y replay offline delegan al servicio. Un auditor idempotente crea alertas, nunca autocorrige.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL, Celery, JavaScript PWA, Django TestCase.

---

### Task 1: Modelo de revisión y servicio único de entregas

**Files:**
- Modify: `logistica/models.py`
- Create: `logistica/services_entregas.py`
- Create: `logistica/migrations/00xx_paradaruta_revision_entrega.py`
- Modify: `logistica/tests.py`

- [ ] **Step 1: Write failing domain tests**

Agregar pruebas que expresen la API deseada:

```python
resultado = confirmar_entrega_parada(
    ruta=self.ruta,
    parada=self.parada,
    actor=self.user,
    entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
    motivo="GPS sin señal",
    client_event_id="entrega-excepcional-1",
    ubicacion={"causa": "GPS_SIN_SENAL"},
)
self.parada.refresh_from_db()
self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
self.assertIsNone(self.parada.hora_llegada_real)
self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)
self.assertTrue(resultado.requiere_revision)
```

Cubrir también geocerca válida, idempotencia exacta, conflicto con payload diferente, Point sin autoridad, autorización, rechazo y permisos.

- [ ] **Step 2: Verify RED**

Run:

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaEntregaDomainTests --keepdb -v 2
```

Expected: FAIL porque `services_entregas` y los campos de revisión aún no existen.

- [ ] **Step 3: Add review fields and event types**

Añadir a `ParadaRuta` constantes y campos equivalentes a:

```python
REVISION_NO_REQUERIDA = "NO_REQUERIDA"
REVISION_PENDIENTE = "PENDIENTE"
REVISION_AUTORIZADA = "AUTORIZADA"
REVISION_RECHAZADA = "RECHAZADA"

revision_entrega_estado = models.CharField(max_length=20, choices=REVISION_CHOICES, default=REVISION_NO_REQUERIDA)
revision_entrega_causa = models.CharField(max_length=40, blank=True)
revision_entrega_datos = models.JSONField(default=dict, blank=True)
revision_entrega_revisada_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name="entregas_logistica_revisadas")
revision_entrega_revisada_en = models.DateTimeField(null=True, blank=True)
revision_entrega_resolucion = models.TextField(blank=True)
```

Crear tipos de `EventoRuta` para entrega, entrega excepcional, autorización, rechazo e inconsistencia. Generar migración Django, sin escribirla manualmente.

- [ ] **Step 4: Implement the transactional domain service**

Crear interfaces:

```python
def confirmar_entrega_parada(*, ruta, parada, actor, entrega_estado, motivo, client_event_id, evidencias=(), ubicacion=None): ...
def revisar_entrega_excepcional(*, parada, actor, decision, motivo): ...
```

El servicio usa `transaction.atomic()` y `select_for_update()`, valida actor/ruta/CEDIS/secuencia, detecta un evento real de geocerca, no modifica estado físico cuando falta, crea evidencia/evento/alerta una sola vez y rechaza colisiones de idempotencia.

- [ ] **Step 5: Verify GREEN and migrations**

Run:

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaEntregaDomainTests --keepdb -v 2
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py makemigrations --check
```

Expected: PASS y “No changes detected”.

- [ ] **Step 6: Commit**

```bash
git add logistica/models.py logistica/services_entregas.py logistica/migrations logistica/tests.py
git commit -m "feat(logistica): centralizar entregas y revision de excepciones"
```

### Task 2: Integrar PWA, ERP, offline y Admin con el servicio

**Files:**
- Modify: `api/logistica_views.py`
- Modify: `api/logistica_serializers.py`
- Modify: `logistica/views.py`
- Modify: `logistica/admin.py`
- Modify: `logistica/templates/logistica/app.html`
- Modify: `logistica/tests.py`

- [ ] **Step 1: Write failing API tests**

Probar que una entrega sin geocerca responde `200`, incluye `requiere_revision: true` y `warning`, conserva estado físico, crea una sola alerta y exige motivo. Probar que el ajuste ERP no fabrica visita, que Admin no permite editar campos críticos y que replay con payload divergente responde `409`.

- [ ] **Step 2: Verify RED**

Run:

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaEntregaApiStabilizationTests --keepdb -v 2
```

Expected: FAIL con el bloqueo HTTP 400 actual o visita fabricada.

- [ ] **Step 3: Replace direct writers**

`LogisticaRutaParadaEntregaView.post` y `ruta_detail/ajustar_entrega_manual` llaman `confirmar_entrega_parada`. Eliminar asignaciones directas de `estado`, horas y entrega. No crear `TIPO_LLEGADA_GEOFENCE` desde el botón. Serializar revisión y aviso.

- [ ] **Step 4: Harden Admin and offline contract**

Añadir todos los campos físicos, entrega y revisión a `readonly_fields`. Hacer `client_event_id`, motivo y contexto del cliente obligatorios para excepciones; conservar respuesta original en retry exacto y devolver conflicto para reutilización incompatible.

- [ ] **Step 5: Update the PWA message and verify GREEN**

Antes de confirmar fuera de geocerca mostrar el aviso aprobado y pedir motivo. Tras confirmar, mostrar que se registró y será revisada, sin bloquear la siguiente parada.

Run:

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaEntregaApiStabilizationTests --keepdb -v 2
```

- [ ] **Step 6: Commit**

```bash
git add api/logistica_views.py api/logistica_serializers.py logistica/views.py logistica/admin.py logistica/templates/logistica/app.html logistica/tests.py
git commit -m "fix(logistica): registrar excepciones sin fabricar visitas"
```

### Task 3: Revisión del jefe y cierre administrativo

**Files:**
- Modify: `logistica/views.py`
- Modify: `logistica/urls.py`
- Modify: `logistica/templates/logistica/ruta_detail.html`
- Modify: `logistica/tests.py`

- [ ] **Step 1: Write failing permission and workflow tests**

Probar lista pendiente, autorización/rechazo con motivo, rechazo por repartidor, conservación de evidencia, cierre operativo permitido y cierre administrativo señalado como pendiente.

- [ ] **Step 2: Verify RED**

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaRevisionEntregaTests --keepdb -v 2
```

- [ ] **Step 3: Implement review actions and visible queue**

Agregar acciones POST que llamen `revisar_entrega_excepcional`, protegidas por el permiso de administración de Logística. Mostrar causa, distancia, ubicación, repartidor, motivo, evidencia y botones Autorizar/Rechazar. Las rutas completadas pueden conservar revisiones abiertas y deben mostrar el contador.

- [ ] **Step 4: Verify GREEN and commit**

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaRevisionEntregaTests --keepdb -v 2
git add logistica/views.py logistica/urls.py logistica/templates/logistica/ruta_detail.html logistica/tests.py
git commit -m "feat(logistica): revisar entregas excepcionales"
```

### Task 4: Reglas adyacentes — enviado cero, fecha operativa y CEDIS

**Files:**
- Modify: `logistica/services_carga_ruta.py`
- Modify: `logistica/services_rutas_control.py`
- Modify: `api/logistica_views.py`
- Modify: `logistica/tests.py`

- [ ] **Step 1: Write failing matrix tests**

Probar `solicitado > 0/enviado = 0` visible y resuelto sin captura; ruta nocturna elegida por API acepta GPS con la misma regla; CEDIS usa evento propio y no crea entrega/geocerca de sucursal.

- [ ] **Step 2: Verify RED**

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaReglasAdyacentesStabilizationTests --keepdb -v 2
```

- [ ] **Step 3: Implement canonical rules**

Mantener líneas enviadas en cero con estado final `ZERO_EXPECTED`; compartir una función de fecha operativa entre selección y tracking; enrutar recarga CEDIS por la operación explícita sin `LLEGADA_GEOFENCE` falsa.

- [ ] **Step 4: Verify GREEN and commit**

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaReglasAdyacentesStabilizationTests --keepdb -v 2
git add logistica/services_carga_ruta.py logistica/services_rutas_control.py api/logistica_views.py logistica/tests.py
git commit -m "fix(logistica): unificar reglas de carga fecha y CEDIS"
```

### Task 5: Auditor automático e integración completa

**Files:**
- Create: `logistica/services_auditoria_entregas.py`
- Create: `logistica/management/commands/auditar_entregas_ruta.py`
- Modify: `logistica/tasks.py`
- Modify: `config/settings.py` or existing Celery Beat registration file
- Modify: `logistica/tests.py`
- Modify: `logistica/static/logistica/sw.js`
- Modify: `logistica/checks.py`

- [ ] **Step 1: Write failing auditor tests**

Sembrar estados corruptos mediante `QuerySet.update()` exclusivamente para auditoría. Ejecutar dos veces y comprobar una sola alerta para: entrega sin geocerca/revisión, visita sin GPS, evento geocerca inválido, usuario de sync y revisión sin alerta. Confirmar que ningún campo histórico cambia.

- [ ] **Step 2: Verify RED**

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica.tests.LogisticaAuditoriaEntregaTests --keepdb -v 2
```

- [ ] **Step 3: Implement diagnostic auditor and scheduled task**

Crear:

```python
def auditar_entregas(*, queryset=None, dry_run=False) -> AuditoriaEntregaResumen: ...
```

La clave idempotente debe identificar regla+ruta+parada+hecho. El comando soporta `--dry-run` y salida de conteos. La tarea periódica ejecuta solo creación de alertas, nunca reparación.

- [ ] **Step 4: Update service-worker version and checks**

Si la PWA cambió, incrementar coordinadamente `CACHE_NAME`, el query de registro y los checks existentes.

- [ ] **Step 5: Run full verification**

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py test logistica --keepdb -v 2
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py check
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp python3 manage.py migrate --check
git diff --check
```

- [ ] **Step 6: Commit**

```bash
git add logistica config
git commit -m "feat(logistica): auditar automaticamente entregas inconsistentes"
```

### Task 6: Validación independiente y flujo real

**Files:**
- Review all changed files

- [ ] **Step 1: Run spec-compliance review**

Un agente independiente compara cada principio y fila de la matriz del diseño contra código y pruebas, incluyendo los consumidores Point, offline, fecha y cierre.

- [ ] **Step 2: Run code-quality and security review**

Otro agente revisa transacciones, concurrencia, permisos, idempotencia, datos históricos y posibles bypasses.

- [ ] **Step 3: Run browser validation**

Validar geocerca normal, excepción con aviso, continuación de ruta, panel del jefe y autorización/rechazo. Revisar consola y XHR.

- [ ] **Step 4: Production diagnostic gate**

Tras PR/merge/deploy, ejecutar `auditar_entregas_ruta --dry-run`, revisar conteos y solo entonces habilitar la periodicidad. No reparar datos históricos como parte del deploy.
