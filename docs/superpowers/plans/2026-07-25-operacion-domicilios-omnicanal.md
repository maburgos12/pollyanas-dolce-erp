# Operación de Domicilios Omnicanal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Concentrar en una sola operación móvil los pedidos de tienda en línea, teléfono, WhatsApp y redes sociales, reutilizando clientes, direcciones GPS y repartidores canónicos del ERP.

**Architecture:** El ERP es la fuente canónica de clientes, direcciones, pedidos omnicanal y repartidores. La tienda conserva checkout, pagos y su pedido web, pero `/operacion` funciona como fachada operativa y escribe en el ERP mediante una API autenticada e idempotente. La app de reparto recibe asignaciones derivadas del repartidor ERP y nunca mantiene un padrón independiente.

**Tech Stack:** Django, Django REST Framework, PostgreSQL, FastAPI, SQLAlchemy, React/TypeScript, PWA, pytest/Django TestCase, Vitest y Playwright.

---

## Límites de fuente de verdad

- `crm.Cliente`: cliente canónico para cualquier canal.
- `crm.DireccionCliente`: libreta GPS canónica.
- `crm.PedidoCliente`: encabezado omnicanal y canal de origen.
- `logistica.SolicitudDomicilio`: ejecución y asignación del domicilio.
- `logistica.Repartidor`: identidad operativa del repartidor.
- E-commerce `Order`: pago, checkout y detalle comercial de la tienda.
- E-commerce `Driver`: únicamente enlace operativo; debe conservar `erp_id`.
- Toda creación externa debe llevar `external_source` y `external_id` únicos.

## Worktrees y aislamiento

- ERP: `/Users/mauricioburgos/Downloads/codex_worktrees/crm-direcciones-omnicanal`
- Rama ERP: `codex/crm-direcciones-omnicanal`
- E-commerce, cuando el contrato ERP quede verde:
  `/Users/mauricioburgos/Downloads/codex_worktrees/pollyanas-operacion-omnicanal`
- Rama e-commerce: `codex/operacion-omnicanal`
- No trabajar en los checkouts raíz.
- Cada entorno debe usar `COMPOSE_PROJECT_NAME`, puerto PostgreSQL y nombre de base de pruebas exclusivos.

### Task 1: Libreta GPS canónica

**Files:**
- Create: `crm/migrations/0003_direccioncliente.py`
- Modify: `crm/models.py`
- Modify: `api/crm_serializers.py`
- Modify: `api/crm_views.py`
- Modify: `api/urls.py`
- Test: `api/tests_crm.py`

- [x] **Step 1: Escribir pruebas de creación, deduplicación, GPS y permisos**
- [x] **Step 2: Ejecutar las pruebas y observar `NoReverseMatch`**
- [x] **Step 3: Crear `DireccionCliente` y la API `/api/crm/clientes/<id>/direcciones/`**
- [x] **Step 4: Aplicar migración en PostgreSQL aislado**
- [x] **Step 5: Ejecutar 34 pruebas CRM/API pública**
- [x] **Step 6: Commit `bb83f7b5 feat(crm): agregar direcciones GPS de clientes`**

### Task 2: Vincular pedido, dirección y solicitud de domicilio

**Files:**
- Modify: `crm/models.py`
- Modify: `logistica/models.py`
- Create: `crm/migrations/0004_pedido_direccion_origen_externo.py`
- Create: `logistica/migrations/0043_solicitud_domicilio_cliente_direccion.py`
- Modify: `api/crm_serializers.py`
- Test: `api/tests_crm.py`
- Test: `logistica/tests.py`

- [x] **Step 1: Escribir prueba de contrato de pedido omnicanal**

```python
def test_pedido_omnicanal_conserva_direccion_y_origen_externo(self):
    pedido = PedidoCliente.objects.create(
        cliente=self.cliente,
        direccion_entrega=self.direccion,
        descripcion="Pastel de chocolate",
        canal=PedidoCliente.CANAL_WHATSAPP,
        external_source="CALL_CENTER",
        external_id="wa-6670000000-001",
    )
    self.assertEqual(pedido.direccion_entrega_id, self.direccion.id)
    self.assertEqual(pedido.external_source, "CALL_CENTER")
```

- [x] **Step 2: Ejecutar y confirmar fallo por campos inexistentes**

Run:

```bash
APP_ENV=test DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55447/pastelerias_erp \
TEST_DB_NAME=test_omnicanal_task2 \
./scripts/run_tests_local.sh api.tests_crm
```

Expected: `TypeError` por `direccion_entrega` o `external_source`.

- [x] **Step 3: Agregar campos canónicos**

```python
direccion_entrega = models.ForeignKey(
    "crm.DireccionCliente",
    null=True,
    blank=True,
    on_delete=models.PROTECT,
    related_name="pedidos",
)
external_source = models.CharField(max_length=40, blank=True, default="")
external_id = models.CharField(max_length=120, blank=True, default="")
```

Agregar una restricción única condicional para `(external_source, external_id)` cuando ambos tengan valor. En `SolicitudDomicilio`, agregar relaciones protegidas hacia `Cliente`, `DireccionCliente` y `PedidoCliente` sin retirar los campos de texto históricos.

- [x] **Step 4: Crear y revisar migraciones**

Run:

```bash
./.venv/bin/python manage.py makemigrations crm logistica
./.venv/bin/python manage.py makemigrations --check --dry-run
```

Expected: migraciones `0004` y `0043`; después `No changes detected`.

- [x] **Step 5: Probar fallback histórico**

Crear una prueba donde `SolicitudDomicilio` no tenga relaciones nuevas y verificar que su dirección y teléfono históricos sigan serializándose. Esto protege registros existentes.

- [x] **Step 6: Ejecutar regresión y commit**

Run:

```bash
./scripts/run_tests_local.sh api.tests_crm logistica.tests
./.venv/bin/python manage.py migrate --check
./.venv/bin/python manage.py check
git diff --check
```

Commit:

```bash
git commit -m "feat(domicilios): vincular pedidos con clientes y direcciones"
```

Resultado de regresión al ejecutar la fase:

- Contrato afectado: 12/12 pruebas en verde.
- Suite amplia CRM + logística: 382/384.
- Los dos fallos restantes también fallan aislados y corresponden a contratos
  previos de entrega/PWA en `logistica/tests.py`; esta fase no modifica esos
  archivos ni los comportamientos señalados.

### Task 3: API idempotente para call center y tienda

**Files:**
- Create: `api/omnichannel_serializers.py`
- Create: `api/omnichannel_views.py`
- Modify: `api/urls.py`
- Test: `api/tests_omnichannel.py`
- Modify: `integraciones/models.py` only if the existing API client cannot express the required scope.

- [ ] **Step 1: Probar alta única por canal y referencia**

```python
def test_reintento_no_duplica_cliente_direccion_pedido_o_domicilio(self):
    first = self.client.post(self.url, self.payload, format="json", **self.auth)
    second = self.client.post(self.url, self.payload, format="json", **self.auth)
    self.assertEqual(first.status_code, 201)
    self.assertEqual(second.status_code, 200)
    self.assertEqual(first.data["pedido_id"], second.data["pedido_id"])
    self.assertEqual(Cliente.objects.count(), 1)
    self.assertEqual(DireccionCliente.objects.count(), 1)
    self.assertEqual(SolicitudDomicilio.objects.count(), 1)
```

- [ ] **Step 2: Implementar `POST /api/public/v1/omnichannel-orders/`**

Payload obligatorio:

```json
{
  "external_source": "ECOMMERCE",
  "external_id": "order_123",
  "canal": "WEB",
  "cliente": {
    "nombre": "Ana Pérez",
    "telefono": "6671234567",
    "email": "ana@example.com"
  },
  "direccion": {
    "direccion": "Av. Obregón 123",
    "referencias": "Portón blanco",
    "latitud": "24.809064",
    "longitud": "-107.394011",
    "place_id": "ChIJ..."
  },
  "pedido": {
    "descripcion": "Pastel chocolate",
    "fecha_compromiso": "2026-07-26",
    "monto_estimado": "850.00"
  }
}
```

La respuesta debe incluir `cliente_id`, `direccion_id`, `pedido_id`, `solicitud_domicilio_id`, `created` y los enlaces de seguimiento.

- [ ] **Step 3: Implementar búsqueda rápida**

Crear `GET /api/public/v1/omnichannel-customers/?q=<telefono|nombre|email>` con máximo 20 resultados y direcciones activas. No devolver notas internas ni datos fuera del alcance operativo.

- [ ] **Step 4: Probar autorización, rate limiting y rollback atómico**

Casos obligatorios: API key ausente, API key inválida, payload incompleto, coordenadas inválidas, canal inválido, reintento idéntico y excepción después de crear cliente. En la excepción no debe persistir ninguna fila parcial.

- [ ] **Step 5: Regresión y commit**

```bash
./scripts/run_tests_local.sh api.tests_omnichannel api.tests_public_api api.tests_crm
git commit -m "feat(api): agregar contrato omnicanal idempotente"
```

### Task 4: Cliente ERP del backend e-commerce

**Files:**
- Create: `backend/app/services/erp_omnichannel_service.py`
- Modify: `backend/app/core/config.py`
- Modify: `backend/app/api/v1/endpoints/operations.py`
- Test: `backend/tests/test_erp_omnichannel_service.py`
- Test: `backend/tests/test_operations_api.py`

- [ ] **Step 1: Probar éxito, timeout y respuesta inválida**

```python
async def test_create_order_timeout_returns_retryable_result(httpx_mock):
    httpx_mock.add_exception(httpx.TimeoutException("timeout"))
    result = await client.create_order(payload)
    assert result.status == "retryable"
    assert result.created is False
```

- [ ] **Step 2: Implementar cliente con timeouts explícitos**

Usar `httpx.AsyncClient`, encabezado `X-API-Key`, `external_id` estable y máximo dos intentos para errores de red. No reintentar respuestas 400. Nunca generar un nuevo identificador durante el retry.

- [ ] **Step 3: Implementar fallback visible**

Si el ERP no responde, guardar el intento en la cola existente de sincronización o en una tabla `OmnichannelSyncAttempt` con payload cifrado o minimizado, estado `PENDING`, contador de intentos y error sanitizado. La API al frontend debe responder `202` y `sync_status: "pending"`, no simular éxito definitivo.

- [ ] **Step 4: Ejecutar backend**

```bash
cd backend
pytest tests/test_erp_omnichannel_service.py tests/test_operations_api.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(operations): conectar pedidos omnicanal con ERP"
```

### Task 5: Centro operativo móvil en `/operacion`

**Files:**
- Modify: `frontend/src/components/operacion/OperacionDomicilios.tsx`
- Create: `frontend/src/components/operacion/ClienteLookup.tsx`
- Create: `frontend/src/components/operacion/DireccionEditor.tsx`
- Create: `frontend/src/components/operacion/PedidoOmnicanalForm.tsx`
- Modify: `frontend/src/services/api.ts`
- Test: `frontend/src/components/operacion/OperacionDomicilios.test.tsx`
- Test: `frontend/e2e/operacion-omnicanal.spec.ts`

- [ ] **Step 1: Probar flujo telefónico completo**

La prueba debe buscar por teléfono, seleccionar cliente, reutilizar dirección, registrar pedido `TELEFONO` y mostrar un único folio confirmado.

- [ ] **Step 2: Crear formulario móvil**

Orden de captura:

1. Canal.
2. Teléfono o búsqueda del cliente.
3. Cliente existente o alta.
4. Dirección existente o nueva con mapa.
5. Productos/descripción y fecha.
6. Confirmación.

Botones táctiles de mínimo 44 px, etiquetas visibles, foco claro y ninguna acción primaria dependiente solo del color.

- [ ] **Step 3: Estados y fallback**

Mostrar explícitamente:

- `Guardado en ERP`.
- `Pendiente de sincronizar`.
- `Requiere corrección`.
- `Ya existía; se recuperó el folio`.

Desactivar el botón mientras la petición está en curso y reutilizar la misma llave idempotente si el operador reintenta.

- [ ] **Step 4: Pruebas frontend**

```bash
cd frontend
npm test -- OperacionDomicilios
npx playwright test e2e/operacion-omnicanal.spec.ts
npm run build
```

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(operacion): concentrar captura omnicanal"
```

### Task 6: Asignación de repartidor ERP

**Files:**
- Modify: `api/logistica_serializers.py`
- Modify: `api/logistica_views.py`
- Modify: `api/urls.py`
- Test: `api/tests_logistica.py`
- Modify: `backend/app/services/erp_omnichannel_service.py`
- Modify: `frontend/src/components/operacion/OperacionDomicilios.tsx`

- [ ] **Step 1: Probar catálogo activo y asignación**

El API debe devolver únicamente repartidores activos autorizados por RRHH/logística. La asignación debe persistir el `Repartidor` ERP, no un nombre libre.

- [ ] **Step 2: Probar idempotencia de asignación**

Repetir la misma asignación debe devolver 200 sin duplicar entregas ni notificaciones. Cambiar de repartidor debe conservar auditoría de anterior, nuevo, usuario y fecha.

- [ ] **Step 3: Implementar endpoints**

- `GET /api/logistica/repartidores-disponibles/`
- `POST /api/logistica/domicilios/<id>/asignar/`

- [ ] **Step 4: Enlazar e-commerce `Driver.erp_id`**

No crear un driver nuevo si ya existe un enlace por `erp_id`. Si el enlace falta, mostrar `Repartidor sin acceso a app` y no fingir que recibió el pedido.

- [ ] **Step 5: Regresión y commit**

```bash
./scripts/run_tests_local.sh api.tests_logistica logistica.tests
cd backend && pytest tests/test_erp_omnichannel_service.py -q
git commit -m "feat(logistica): asignar domicilios a repartidores ERP"
```

### Task 7: Entrega en app del repartidor y GPS

**Files:**
- Modify: `backend/app/api/v1/endpoints/driver.py`
- Modify: `backend/app/models/tracking.py`
- Test: `backend/tests/test_driver_assignments.py`
- Modify sibling app only in its own clean branch:
  `/Users/mauricioburgos/Downloads/Pollyana's Dolce e-commerce/pollyanas-repartidor-app/src`

- [ ] **Step 1: Probar que el repartidor solo vea sus pedidos**
- [ ] **Step 2: Incluir dirección, referencias, GPS, teléfono y enlace de navegación**
- [ ] **Step 3: Probar pedido reasignado y revocación del anterior**
- [ ] **Step 4: Implementar fallback sin GPS con dirección textual**
- [ ] **Step 5: Probar cola offline sin duplicar cambio de estado**
- [ ] **Step 6: Ejecutar build y pruebas de la PWA**

```bash
npm test
npm run build
```

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(repartidor): recibir domicilios omnicanal"
```

### Task 8: Evidencia visual, Figma y no regresión

**Files:**
- Create screenshots under the task artifact directory.
- Update Figma design file:
  `https://www.figma.com/design/j0ubgnRfzwHvMcvwcVCxb6`
- Update FigJam:
  `https://www.figma.com/board/5eGWNtzpKBTQPpKtzNBzpE`

- [ ] **Step 1: Capturar desktop y móvil**

Capturas obligatorias:

- Bandeja unificada.
- Búsqueda de cliente.
- Dirección guardada con GPS.
- Alta telefónica.
- Pedido pendiente de sincronizar.
- Selector de repartidor.
- Vista del repartidor con mapa.

- [ ] **Step 2: Validar screenshots**

Confirmar que cada PNG existe, abre correctamente, tiene resolución esperada y no contiene datos personales reales.

- [ ] **Step 3: Anotar Figma**

Cada pantalla debe incluir notas de fuente de verdad, fallback, permisos, estado de sincronización y comportamiento móvil.

- [ ] **Step 4: Regresión cruzada**

ERP:

```bash
./scripts/run_tests_local.sh api.tests_crm api.tests_omnichannel api.tests_logistica logistica.tests
./.venv/bin/python manage.py migrate --check
./.venv/bin/python manage.py check
```

E-commerce:

```bash
cd backend && pytest -q
cd ../frontend && npm test && npm run build
```

PWA repartidor:

```bash
npm test
npm run build
```

- [ ] **Step 5: Prueba manual**

Ejecutar una orden de cada canal, repetir el envío, asignar y reasignar repartidor, abrir GPS, cambiar estados y verificar que exista un solo cliente, una sola dirección, un solo pedido y una sola solicitud de domicilio por `external_id`.

### Task 9: PRs y despliegue controlado

- [ ] **Step 1: Actualizar cada rama desde `origin/main` y repetir pruebas**
- [ ] **Step 2: Abrir primero PR del ERP**
- [ ] **Step 3: Desplegar ERP y aplicar migraciones con respaldo**
- [ ] **Step 4: Validar API ERP en producción con datos de prueba controlados**
- [ ] **Step 5: Abrir y desplegar PR e-commerce**
- [ ] **Step 6: Reparar/verificar dominio de la PWA de repartidor**
- [ ] **Step 7: Ejecutar smoke test real desde celular**
- [ ] **Step 8: Registrar rollback, evidencias y pendientes**

No desplegar e-commerce antes de que el contrato ERP correspondiente esté disponible. No activar la selección de repartidor hasta que la app confirme que el usuario enlazado por `erp_id` puede recibir el pedido.

## Criterios de terminación

- Un operador captura pedidos de cualquier canal desde un celular.
- El teléfono recupera al cliente y sus direcciones.
- Un reintento no duplica cliente, dirección, pedido ni domicilio.
- Las coordenadas llegan al repartidor y abren navegación.
- El repartidor procede del ERP.
- La asignación aparece en la app operativa.
- Una falla de ERP queda visible como pendiente, nunca como éxito falso.
- Backend, frontend, PWA, migraciones y regresiones quedan verdes.
- Existen screenshots verificadas y notas en Figma.
