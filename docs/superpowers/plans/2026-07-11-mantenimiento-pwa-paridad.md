# Paridad Operativa de Mantenimiento PWA Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dar a la PWA conteos reales, detalle completo con evidencias protegidas y un historial unificado de reportes, órdenes, reparaciones, servicios de unidades y trabajos sin reporte previo.

**Architecture:** Crear contratos v2 aditivos y normalizadores separados del archivo grande `mantenimiento/views.py`. Un servicio de autorización común limitará listas, detalles y archivos; la PWA consumirá listas paginadas y pedirá el detalle bajo demanda. Los endpoints legacy conservarán su shape durante la transición.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL, templates/JavaScript PWA, service worker, Django TestCase.

---

## Estructura de archivos

- Crear `mantenimiento/services_access.py`: alcance autorizado reutilizable por usuario y sucursal.
- Crear `mantenimiento/services_history.py`: periodos, estados canónicos, normalización, conteos y paginación estable.
- Crear `mantenimiento/api_v2.py`: endpoints HTTP v2 y entrega protegida de evidencias.
- Crear `mantenimiento/api_v2_urls.py`: rutas v2 aisladas.
- Modificar `config/urls.py`: montar `/api/mantenimiento/v2/`.
- Modificar `mantenimiento/serializers.py`: completar servicios de unidad y helpers de identidad/evidencia cuando sean reutilizables.
- Modificar `mantenimiento/views.py`: únicamente para reutilizar validadores de carga y mantener compatibilidad; no añadir más lógica histórica grande.
- Modificar `templates/mantenimiento/pwa.html`: conteos, filtros, detalle, galería e historial.
- Modificar `static/mantenimiento/sw.js`: bump y exclusión explícita de API/evidencias.
- Modificar `mantenimiento/tests.py`: contratos legacy y pruebas integradas de PWA.
- Crear `mantenimiento/tests_v2.py`: tests unitarios/HTTP específicos de v2.

### Task 1: Periodos y estados canónicos

**Files:**
- Create: `mantenimiento/services_history.py`
- Create: `mantenimiento/tests_v2.py`

- [ ] **Step 1: Escribir pruebas fallidas de periodos y estados**

```python
from datetime import datetime
from zoneinfo import ZoneInfo

from django.test import SimpleTestCase

from mantenimiento.services_history import canonical_status, period_bounds


class MaintenanceHistoryDomainTests(SimpleTestCase):
    def test_30d_uses_mazatlan_inclusive_start_exclusive_end(self):
        now = datetime(2026, 7, 11, 15, 0, tzinfo=ZoneInfo("America/Mazatlan"))
        start, end = period_bounds("30d", now=now)
        self.assertEqual(start.isoformat(), "2026-06-12T00:00:00-07:00")
        self.assertEqual(end.isoformat(), "2026-07-12T00:00:00-07:00")

    def test_source_statuses_map_without_losing_programmed(self):
        self.assertEqual(canonical_status("orden", "EN_PROCESO"), "en_proceso")
        self.assertEqual(canonical_status("orden", "CERRADA"), "cerrado")
        self.assertEqual(canonical_status("reporte_unidad", "PROGRAMADO"), "programado")
```

- [ ] **Step 2: Ejecutar RED**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceHistoryDomainTests`

Expected: FAIL con `ModuleNotFoundError: mantenimiento.services_history`.

- [ ] **Step 3: Implementar funciones puras mínimas**

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from django.utils import timezone

MAZATLAN = ZoneInfo("America/Mazatlan")

STATUS_MAP = {
    "orden": {
        "PENDIENTE": "abierto",
        "EN_PROCESO": "en_proceso",
        "CERRADA": "cerrado",
        "CANCELADA": "cancelado",
    },
    "reporte_unidad": {
        "ABIERTO": "abierto",
        "EN_PROCESO": "en_proceso",
        "PROGRAMADO": "programado",
        "CERRADO": "cerrado",
        "CANCELADO": "cancelado",
    },
}


def canonical_status(source, value):
    return STATUS_MAP[source][value]


def period_bounds(period, *, now=None):
    local_now = (now or timezone.now()).astimezone(MAZATLAN)
    today = local_now.date()
    end = datetime.combine(today + timedelta(days=1), datetime.min.time(), MAZATLAN)
    if period == "30d":
        start = end - timedelta(days=30)
    elif period == "90d":
        start = end - timedelta(days=90)
    elif period == "semana":
        start = datetime.combine(today - timedelta(days=today.weekday()), datetime.min.time(), MAZATLAN)
    elif period == "mes":
        start = datetime(today.year, today.month, 1, tzinfo=MAZATLAN)
    elif period == "todo":
        return None, end
    else:
        raise ValueError("Periodo no soportado")
    return start, end
```

- [ ] **Step 4: Ejecutar GREEN y ampliar bordes**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceHistoryDomainTests`

Expected: PASS. El mismo archivo debe contener `test_week_starts_on_monday`, `test_month_uses_first_day` y `test_invalid_period_raises_value_error`, cada uno con valores ISO exactos.

- [ ] **Step 5: Commit**

```bash
git add mantenimiento/services_history.py mantenimiento/tests_v2.py
git commit -m "feat(mantenimiento): normalizar periodos y estados v2"
```

### Task 2: Política de acceso por objeto

**Files:**
- Create: `mantenimiento/services_access.py`
- Modify: `mantenimiento/tests_v2.py`

- [ ] **Step 1: Escribir tests de alcance**

Crear usuarios global, limitado a sucursal y solo lectura. Probar que `authorized_fallas(user)` incluye únicamente sucursales permitidas y que un objeto ajeno no puede resolverse.

```python
class MaintenanceAccessTests(TestCase):
    def test_limited_user_cannot_resolve_other_branch_report(self):
        qs = authorized_fallas(self.limited_user)
        self.assertTrue(qs.filter(pk=self.own_report.pk).exists())
        self.assertFalse(qs.filter(pk=self.other_report.pk).exists())
```

- [ ] **Step 2: Ejecutar RED**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceAccessTests`

Expected: FAIL porque `authorized_fallas` no existe.

- [ ] **Step 3: Implementar querysets autorizados**

```python
def authorized_branch_ids(user):
    if is_admin_or_dg(user) or can_manage_module(user, "mantenimiento"):
        return None
    profile = getattr(user, "profile", None)
    branch_id = getattr(profile, "sucursal_id", None)
    return [branch_id] if branch_id else []


def authorized_fallas(user):
    qs = ReporteFalla.objects.all()
    branch_ids = authorized_branch_ids(user)
    return qs if branch_ids is None else qs.filter(sucursal_id__in=branch_ids)
```

Implementar `authorized_orders`, `authorized_unit_reports`, `authorized_repairs` y `authorized_unit_services` con el mismo patrón. Órdenes filtran `activo_ref__sucursal_id`; fuentes de unidad filtran `unidad__sucursal_id`. Ninguna función resuelve sucursal por texto.

- [ ] **Step 4: Probar lectura, gestión y costos**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceAccessTests`

Expected: PASS para global/limitado/ajeno y para el helper `can_view_costs(user)`.

- [ ] **Step 5: Commit**

```bash
git add mantenimiento/services_access.py mantenimiento/tests_v2.py
git commit -m "feat(mantenimiento): centralizar alcance autorizado"
```

### Task 3: Conteos y bandeja v2

**Files:**
- Modify: `mantenimiento/services_history.py`
- Create: `mantenimiento/api_v2.py`
- Create: `mantenimiento/api_v2_urls.py`
- Modify: `config/urls.py`
- Modify: `mantenimiento/tests_v2.py`

- [ ] **Step 1: Escribir tests del contrato**

```python
def test_closed_count_is_independent_from_page_size(self):
    response = self.client.get("/api/mantenimiento/v2/bandeja/", {
        "estado": "cerrados", "periodo": "30d", "page_size": 1,
    })
    self.assertEqual(response.status_code, 200)
    payload = response.json()
    self.assertEqual(payload["schema_version"], 2)
    self.assertEqual(payload["counts"]["cerrados"], 3)
    self.assertEqual(len(payload["results"]), 1)
    self.assertEqual(payload["pagination"]["total"], 3)
```

Incluir cerrados de falla, orden y unidad; 29/31 días; cancelado excluido; usuario de otra sucursal.

- [ ] **Step 2: Ejecutar RED**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceInboxV2Tests`

Expected: 404 para la ruta v2.

- [ ] **Step 3: Implementar rutas y respuesta paginada**

```python
urlpatterns = [
    path("bandeja/", api_v2.bandeja_v2, name="mantenimiento-v2-bandeja"),
]
```

El endpoint validará enums, limitará `page_size` a 100, calculará conteos antes de paginar y devolverá orden estable `fecha_evento DESC, uid DESC`.

- [ ] **Step 4: Verificar contrato y consultas**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceInboxV2Tests`

Expected: PASS. Añadir comparación de consultas para 1 y 20 registros; el crecimiento permitido debe ser cero por tipo fuente.

- [ ] **Step 5: Commit**

```bash
git add mantenimiento/services_history.py mantenimiento/api_v2.py mantenimiento/api_v2_urls.py config/urls.py mantenimiento/tests_v2.py
git commit -m "feat(mantenimiento): agregar bandeja y conteos v2"
```

### Task 4: Detalle completo y evidencia autenticada

**Files:**
- Modify: `mantenimiento/api_v2.py`
- Modify: `mantenimiento/api_v2_urls.py`
- Modify: `mantenimiento/services_history.py`
- Modify: `mantenimiento/tests_v2.py`

- [ ] **Step 1: Escribir tests de detalle y archivo**

Probar foto inicial, bitácora ordenada, evidencia vinculada, nulos explícitos y usuario visible. Probar anónimo/sin permiso/otra sucursal, HEAD, archivo ausente y nombre malicioso.

```python
def test_falla_detail_contains_initial_photo_and_timeline_evidence(self):
    response = self.client.get(f"/api/mantenimiento/v2/items/falla/{self.report.id}/")
    self.assertEqual(response.status_code, 200)
    data = response.json()
    self.assertEqual(data["reporte_inicial"]["reportado_por"]["nombre"], "Reportante QA")
    self.assertEqual(data["seguimiento"][0]["evidencias"][0]["nombre"], "avance.jpg")
    self.assertTrue(data["seguimiento"][0]["evidencias"][0]["url"].startswith("/api/mantenimiento/v2/evidencias/"))
```

- [ ] **Step 2: Ejecutar RED**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceDetailV2Tests mantenimiento.tests_v2.MaintenanceEvidenceV2Tests`

Expected: 404 para detalle y evidencia.

- [ ] **Step 3: Implementar normalizador y FileResponse**

```python
@api_view(["GET", "HEAD"])
@authentication_classes(AUTH)
@permission_classes([EsMantenimiento])
def evidencia_v2(request, tipo, pk):
    evidence = resolve_authorized_evidence(request.user, tipo, pk)
    if not evidence.file or not evidence.file.storage.exists(evidence.file.name):
        raise Http404
    response = FileResponse(evidence.file.open("rb"), content_type=evidence.mime_type)
    response["Content-Disposition"] = content_disposition(evidence.safe_name, inline=evidence.inline)
    response["Cache-Control"] = "private, no-store"
    return response
```

No serializar `archivo.url` ni rutas físicas.

- [ ] **Step 4: Ejecutar tests y presupuesto de consultas**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceDetailV2Tests mantenimiento.tests_v2.MaintenanceEvidenceV2Tests`

Expected: PASS con 0 y múltiples seguimientos/evidencias sin crecimiento N+1.

- [ ] **Step 5: Commit**

```bash
git add mantenimiento/api_v2.py mantenimiento/api_v2_urls.py mantenimiento/services_history.py mantenimiento/tests_v2.py
git commit -m "feat(mantenimiento): proteger detalle y evidencias"
```

### Task 5: Validación compartida de nuevas evidencias

**Files:**
- Create: `mantenimiento/evidence_validation.py`
- Modify: `mantenimiento/views.py`
- Modify: `mantenimiento/tests_v2.py`

- [ ] **Step 1: Escribir tests del validador**

Casos: JPEG/PNG válido; SVG, HTML, ejecutable y extensión discordante rechazados; imagen mayor a 10 MB; PDF mayor a 30 MB; más de cinco archivos.

- [ ] **Step 2: Ejecutar RED**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceEvidenceUploadTests`

Expected: FAIL porque el validador no existe y el endpoint acepta archivos inseguros.

- [ ] **Step 3: Implementar allowlist y verificación**

```python
IMAGE_TYPES = {"image/jpeg": {".jpg", ".jpeg"}, "image/png": {".png"}, "image/webp": {".webp"}}
PDF_TYPES = {"application/pdf": {".pdf"}}

def validate_evidence_files(files):
    if len(files) > 5:
        raise ValidationError("Máximo 5 evidencias por avance.")
    for upload in files:
        validate_extension_and_declared_mime(upload, IMAGE_TYPES | PDF_TYPES)
        validate_size(upload, image_limit=10 * MB, pdf_limit=30 * MB)
        verify_binary_signature(upload)
```

- [ ] **Step 4: Integrar en foto inicial y seguimiento**

Invocar `validate_evidence_files(request.FILES.getlist("evidencias"))` antes de `_guardar_evidencias_falla`. En error devolver `Response({"evidencias": [str(exc)]}, status=400)` en API y `messages.error(request, str(exc))` en formulario, antes de crear cualquier modelo o archivo.

- [ ] **Step 5: Ejecutar tests y commit**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceEvidenceUploadTests mantenimiento.tests.MantenimientoUnifiedInboxTests`

Expected: PASS.

```bash
git add mantenimiento/evidence_validation.py mantenimiento/views.py mantenimiento/tests_v2.py
git commit -m "fix(mantenimiento): validar archivos de evidencia"
```

### Task 6: Historial unificado y paridad de flota

**Files:**
- Modify: `mantenimiento/services_history.py`
- Modify: `mantenimiento/api_v2.py`
- Modify: `mantenimiento/api_v2_urls.py`
- Modify: `mantenimiento/serializers.py`
- Modify: `mantenimiento/tests_v2.py`

- [ ] **Step 1: Escribir tests de clasificación y deduplicación**

Crear un reporte, orden normal, orden emergencia sin reporte, reparación vinculada y servicio directo. Afirmar UIDs únicos, `parent_uid`, autor/sucursal/factura y filtros.

```python
def test_history_classifies_direct_work_without_duplication(self):
    data = self.client.get("/api/mantenimiento/v2/historial/", {"tipo": "todo"}).json()
    by_uid = {item["uid"]: item for item in data["results"]}
    self.assertEqual(by_uid[f"orden:{self.direct_order.id}"]["tipo"], "sin_reporte")
    self.assertEqual(by_uid[f"servicio_unidad:{self.service.id}"]["tipo"], "servicio_unidad")
    self.assertTrue(by_uid[f"servicio_unidad:{self.service.id}"]["captura_directa"])
    self.assertEqual(len(by_uid), len(data["results"]))
```

- [ ] **Step 2: Ejecutar RED**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceHistoryV2Tests`

Expected: 404 o ausencia de tipos normalizados.

- [ ] **Step 3: Implementar adaptadores por fuente**

Crear `history_from_fallas`, `history_from_orders`, `history_from_repairs` y `history_from_unit_services`. Todas devuelven diccionarios con `uid`, `tipo`, `fecha_evento`, `estado`, `sucursal`, `sujeto`, `actor`, `origen`, `parent_uid` y `captura_directa`. Si falta autor persistido, `actor` será `null` y `actor_label` será `Sin usuario registrado`; nunca inferir por fecha o sesión.

- [ ] **Step 4: Implementar merge/paginación estable**

Ordenar por `fecha_evento` y `uid`; aplicar filtros autorizados antes de materializar. Si el volumen impide unión eficiente, usar consultas por fuente acotadas al periodo y merge de ventanas, documentando el presupuesto.

- [ ] **Step 5: Ejecutar tests y commit**

Run: `python manage.py test mantenimiento.tests_v2.MaintenanceHistoryV2Tests`

Expected: PASS para origen, estado, periodo, sucursal, búsqueda, paginación y deduplicación.

```bash
git add mantenimiento/services_history.py mantenimiento/api_v2.py mantenimiento/api_v2_urls.py mantenimiento/serializers.py mantenimiento/tests_v2.py
git commit -m "feat(mantenimiento): unificar historial operativo"
```

### Task 7: PWA — conteos, detalle, galería e historial

**Files:**
- Modify: `templates/mantenimiento/pwa.html`
- Modify: `mantenimiento/tests.py`

- [ ] **Step 1: Escribir pruebas estáticas y de contrato UI**

```python
def test_pwa_uses_v2_counts_detail_and_history(self):
    self.client.force_login(self.mantenimiento)
    response = self.client.get(reverse("mantenimiento:app"))
    self.assertContains(response, 'apiFetchV2("/bandeja/')
    self.assertContains(response, 'showMaintenanceDetail(item.uid)')
    self.assertContains(response, 'id="maintenance-evidence-viewer"')
    self.assertContains(response, 'data-period="30d"')
```

- [ ] **Step 2: Ejecutar RED**

Run: `python manage.py test mantenimiento.tests.MantenimientoUnifiedAccessTests.test_pwa_uses_v2_counts_detail_and_history`

Expected: FAIL porque la PWA todavía usa bandeja legacy.

- [ ] **Step 3: Migrar carga inicial y filtros**

Agregar estado explícito:

```javascript
history: {period: "30d", type: "todo", status: "cerrado", page: 1},
counts: {abiertos: 0, en_proceso: 0, criticos: 0, cerrados: 0},
detailCache: new Map(),
requestGeneration: 0,
```

Incrementar `requestGeneration` por cambio de filtro e ignorar respuestas con generación anterior.

- [ ] **Step 4: Implementar pantalla de detalle y visor**

Renderizar secciones semánticas, miniaturas `loading="lazy"`, alt descriptivo, botón cerrar de 44px, Escape y retorno del foco. Cargar el detalle solo al abrirlo. Mostrar archivo faltante/error sin ocultar texto ni cronología.

- [ ] **Step 5: Implementar historial paginado**

Reemplazar el recorte fijo de órdenes/reparaciones/servicios por `/v2/historial/`. Mostrar origen explícito y filtros de periodo/tipo/estado/sucursal. Mantener una acción `Cargar más` o paginación simple, no scroll infinito.

- [ ] **Step 6: Ejecutar tests y commit**

Run: `python manage.py test mantenimiento.tests.MantenimientoUnifiedAccessTests mantenimiento.tests.MantenimientoUnifiedInboxTests`

Expected: PASS, incluidos cerrados, detalle, estados vacíos y permisos.

```bash
git add templates/mantenimiento/pwa.html mantenimiento/tests.py
git commit -m "feat(mantenimiento): mostrar detalle e historial en PWA"
```

### Task 8: Service worker y protección post-logout

**Files:**
- Modify: `static/mantenimiento/sw.js`
- Modify: `templates/mantenimiento/pwa.html`
- Modify: `mantenimiento/tests.py`

- [ ] **Step 1: Escribir test de política de caché**

```python
def test_maintenance_worker_never_caches_api_or_evidence(self):
    worker = Path("static/mantenimiento/sw.js").read_text()
    self.assertIn('url.pathname.startsWith("/api/")', worker)
    self.assertIn('return fetch(event.request)', worker)
    self.assertNotIn('cache.put(event.request', protected_branch(worker))
```

- [ ] **Step 2: Ejecutar RED**

Run: `python manage.py test mantenimiento.tests.MantenimientoUnifiedAccessTests.test_maintenance_worker_never_caches_api_or_evidence`

Expected: FAIL con el worker actual.

- [ ] **Step 3: Implementar network-only para protegido y bump**

```javascript
if (url.pathname.startsWith("/api/") || url.pathname.startsWith("/media/")) {
  event.respondWith(fetch(event.request));
  return;
}
```

Actualizar `CACHE_NAME` y el querystring de registro del SW en la misma edición.

- [ ] **Step 4: Ejecutar tests y commit**

Run: `python manage.py test mantenimiento.tests.MantenimientoUnifiedAccessTests`

Expected: PASS para alcance del worker, versión y no-caché.

```bash
git add static/mantenimiento/sw.js templates/mantenimiento/pwa.html mantenimiento/tests.py
git commit -m "fix(mantenimiento): excluir evidencias del cache PWA"
```

### Task 9: Verificación integrada y producción

**Files:**
- Modify only if a failing verification proves a defect in files already in scope.

- [ ] **Step 1: Sincronizar y auditar rama**

Run:

```bash
git fetch origin main
git status --short --branch
git diff origin/main..HEAD --stat
git rev-list --left-right --count origin/main...HEAD
```

Expected: rama limpia, un solo objetivo y base actualizada.

- [ ] **Step 2: Ejecutar checks y pruebas**

```bash
python manage.py migrate --check
python manage.py check
python manage.py makemigrations --check --dry-run
python manage.py test mantenimiento fallas activos logistica
```

Expected: cero migraciones pendientes, cero errores y pruebas en verde. Los fallos preexistentes deben reproducirse en `origin/main` antes de clasificarlos como ajenos.

- [ ] **Step 3: Validar navegador local autenticado**

En viewport móvil real, verificar:

- conteo cerrado con 30d y cambio de periodo;
- detalle con foto inicial y evidencia;
- Escape/foco del visor;
- historial de servicio de unidad y trabajo sin reporte;
- consola sin errores;
- Network sin URLs públicas de media ni respuestas cacheadas.

- [ ] **Step 4: Revisión cruzada por agentes**

Asignar revisores separados para: seguridad/autorización, contrato API/rendimiento y UX móvil/accesibilidad. Corregir únicamente hallazgos demostrables y volver a ejecutar la verificación completa.

- [ ] **Step 5: PR y deploy**

Crear PR de una sola tarea. Tras merge:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

Confirmar migraciones, `collectstatic`, versión del SW y commit desplegado.

- [ ] **Step 6: Validar producción sin fabricar datos**

Comparar conteos ORM/SQL antes/después y abrir IDs reales existentes de: reporte con foto/evidencia, cerrado, servicio de unidad y trabajo sin reporte. Confirmar usuario autorizado y usuario sin permiso. Documentar cualquier histórico sin autor como `Sin usuario registrado`.
