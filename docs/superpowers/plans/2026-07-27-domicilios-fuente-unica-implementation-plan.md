# Domicilios con fuente única Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir un flujo único de domicilios donde Point sea la fuente comercial, el ERP sea la fuente operativa y Centro Operativo sea una interfaz responsive del mismo registro.

**Architecture:** El ERP consultará y normalizará notas de Point, conservará un snapshot inmutable identificado por `PK_NOTA` y será dueño de cliente, dirección, canal, domicilio, asignación y estado. El backend del e-commerce actuará como BFF autenticado y Centro Operativo consumirá esas API sin mantener una segunda base de pedidos.

**Tech Stack:** Django 4/DRF/PostgreSQL/Celery en ERP; FastAPI/SQLAlchemy/httpx en el BFF; Next.js/React/TypeScript/Tailwind/Vitest/Playwright en Centro Operativo; Point HTTP autenticado; Docker Compose en producción.

---

## Límites y orden de ejecución

Este programa contiene cuatro entregas. Son secuenciales:

1. contrato de detalle de nota Point;
2. dominio y API canónicos del ERP;
3. Centro Operativo responsive;
4. consolidación, migración y despliegue.

No ejecutar tareas de entregas distintas sobre el mismo worktree. Cada entrega usa una rama y un worktree limpios creados desde el `origin/main` vigente de su repositorio. La entrega 3 no inicia hasta que la entrega 2 esté desplegada en staging o exponga un stub contractual versionado.

El trabajo ya fusionado en ERP por PR `#1138` (`crm-direcciones-omnicanal`) es la base. No se recrean `Cliente`, `DireccionCliente`, `PedidoCliente`, `SolicitudDomicilio`, idempotencia omnicanal ni asignación M2M.

## Mapa de archivos

### ERP

- Crear `pos_bridge/services/point_note_detail_service.py`: consulta y normalización de notas individuales.
- Crear `pos_bridge/tests/test_point_note_detail_service.py`: contrato Point con fixtures sanitizados.
- Crear `pos_bridge/management/commands/probe_point_note_detail.py`: diagnóstico de solo lectura.
- Crear `pos_bridge/tests/fixtures/point_note_detail.json`: respuesta Point sanitizada.
- Modificar `crm/models.py`: canales Facebook/Instagram y metadatos de nota.
- Crear `crm/migrations/0007_pedidocliente_point_note_and_channels.py`: cambios canónicos.
- Modificar `logistica/models.py`: estados operativos y ventana de entrega.
- Crear `logistica/migrations/0045_solicituddomicilio_operacion_canonica.py`: cambios de domicilio.
- Crear `crm/services/point_order_link.py`: transacción idempotente nota → pedido.
- Crear `crm/tests_point_order_link.py`: concurrencia, snapshot y dedupe.
- Modificar `api/omnichannel_serializers.py`: contratos de búsqueda, creación y detalle.
- Modificar `api/omnichannel_views.py`: búsqueda de nota, ficha, bandeja y transiciones.
- Modificar `api/urls.py`: rutas canónicas.
- Modificar `api/tests_omnichannel.py`: seguridad, API y regresión.
- Crear `crm/templates/crm/pedido_domicilio_detail.html`: ficha ERP del mismo pedido.
- Modificar `crm/urls.py` y `crm/views.py`: acceso exacto desde “Gestionar en ERP”.

### E-commerce/BFF

- Modificar `backend/app/services/erp_omnichannel_service.py`: nuevos endpoints ERP.
- Modificar `backend/app/schemas/omnichannel.py`: tipos de nota, ficha y filtros.
- Modificar `backend/app/routers/admin.py`: BFF de notas y pedidos.
- Modificar `backend/tests/test_erp_omnichannel_service.py`: fallos y respuestas.

### Centro Operativo

- Crear `frontend/src/components/operacion/domicilios/types.ts`: tipos compartidos.
- Crear `frontend/src/components/operacion/domicilios/PointNoteSearch.tsx`: pasos 1 y 2.
- Crear `frontend/src/components/operacion/domicilios/CustomerStep.tsx`: cliente y direcciones.
- Crear `frontend/src/components/operacion/domicilios/DeliveryStep.tsx`: canal, GPS y ventana.
- Crear `frontend/src/components/operacion/domicilios/OrderSummary.tsx`: resumen inmutable Point.
- Crear `frontend/src/components/operacion/domicilios/DeliveryInbox.tsx`: bandeja única.
- Crear `frontend/src/components/operacion/domicilios/DeliveryDetail.tsx`: ficha compartida.
- Crear `frontend/src/components/operacion/domicilios/DeliveryStatusTimeline.tsx`: estados y auditoría.
- Modificar `frontend/src/components/operacion/PedidoOmnicanalForm.tsx`: convertirlo en orquestador por pasos.
- Modificar `frontend/src/components/operacion/OperacionDomicilios.tsx`: bandeja y enlace exacto.
- Modificar `frontend/src/lib/api.ts`: cliente BFF.
- Crear pruebas Vitest por componente y `frontend/tests/operacion-domicilios-responsive.spec.ts`.
- Modificar `frontend/public/sw.js`: versión de caché únicamente en la entrega de frontend.

## Entrega 1: contrato de detalle de nota Point

### Task 1: Capturar el endpoint real de detalle

**Files:**
- Create: `pos_bridge/management/commands/probe_point_note_detail.py`
- Create: `pos_bridge/tests/test_probe_point_note_detail.py`
- Create: `docs/point/point-note-detail-contract.md`

- [ ] **Step 1: escribir la prueba fallida del comando seguro**

```python
def test_probe_requires_explicit_note_id():
    with pytest.raises(CommandError, match="--pk-nota"):
        call_command("probe_point_note_detail")
```

- [ ] **Step 2: comprobar el fallo**

Run:

```bash
docker compose run --rm web pytest pos_bridge/tests/test_probe_point_note_detail.py -q
```

Expected: `FAIL` porque el comando no existe.

- [ ] **Step 3: crear el comando de diagnóstico**

El comando debe aceptar `--pk-nota`, `--folio`, `--sucursal` y `--output`. Debe usar `PointHttpSessionService`, rechazar ejecución sin `--pk-nota`, ocultar cookies/tokens y escribir únicamente JSON sanitizado.

```python
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--pk-nota", required=True)
        parser.add_argument("--folio", default="")
        parser.add_argument("--sucursal", default="")
        parser.add_argument("--output", required=True)

    def handle(self, *args, **options):
        result = discover_note_detail(
            pk_nota=options["pk_nota"],
            folio=options["folio"],
            sucursal=options["sucursal"],
        )
        Path(options["output"]).write_text(
            json.dumps(sanitize_note_payload(result), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
```

- [ ] **Step 4: observar Point en solo lectura**

En una nota de prueba autorizada, abrir el reporte de notas con la sesión Point existente, seleccionar la nota y capturar la solicitud de red que carga sus líneas. Registrar método, path, parámetros, campos y códigos de error en `docs/point/point-note-detail-contract.md`. No registrar datos personales ni credenciales.

- [ ] **Step 5: ejecutar el comando contra esa nota**

Primero seleccionar una nota real autorizada desde `NotasByPlaza` y exportar sus tres identificadores sin escribirlos en el repositorio:

```bash
export POINT_NOTE_TEST_ID='900001'
export POINT_NOTE_TEST_FOLIO='18452'
export POINT_NOTE_TEST_BRANCH='Matriz'
```

Los valores del ejemplo pertenecen al fixture sanitizado. En producción deben sustituirse en la sesión de terminal por los valores de la nota autorizada.

Run:

```bash
docker compose exec -T web python manage.py probe_point_note_detail \
  --pk-nota "$POINT_NOTE_TEST_ID" \
  --folio "$POINT_NOTE_TEST_FOLIO" \
  --sucursal "$POINT_NOTE_TEST_BRANCH" \
  --output /tmp/point-note-detail.json
```

Expected: JSON con cabecera, líneas y totales; cero secretos.

- [ ] **Step 6: documentar la decisión**

El contrato debe declarar uno de dos resultados:

```text
DIRECT_API: endpoint estable con líneas por PK_NOTA.
OFFICIAL_REPORT: reporte oficial descargable que contiene PK_NOTA y líneas.
```

Si ninguno entrega productos por nota, detener el programa. No continuar con reconstrucción por totales.

- [ ] **Step 7: comprobar si Point expone clientes reutilizables**

En la misma sesión autenticada, revisar si la nota o un endpoint oficial de clientes devuelve un identificador estable, nombre, teléfono y correo. Documentar exactamente uno de estos resultados:

```text
POINT_CUSTOMERS_AVAILABLE: importar por identificador estable y teléfono.
POINT_CUSTOMERS_UNAVAILABLE: construir CRM progresivamente por teléfono desde domicilios.
```

No deducir clientes desde ventas agregadas ni desde nombres de cajero.

- [ ] **Step 8: ejecutar pruebas y commit**

```bash
docker compose run --rm web pytest pos_bridge/tests/test_probe_point_note_detail.py -q
git add pos_bridge/management/commands/probe_point_note_detail.py \
  pos_bridge/tests/test_probe_point_note_detail.py \
  docs/point/point-note-detail-contract.md
git commit -m "docs(point): confirma contrato de detalle de nota"
```

### Task 2: Implementar normalizador de nota Point

**Files:**
- Create: `pos_bridge/services/point_note_detail_service.py`
- Create: `pos_bridge/tests/test_point_note_detail_service.py`
- Create: `pos_bridge/tests/fixtures/point_note_detail.json`

- [ ] **Step 1: crear fixture sanitizado**

```json
{
  "PK_NOTA": "900001",
  "FOLIO": "18452",
  "SUCURSAL": "Matriz",
  "DIA": "2026-07-27",
  "HORA": "12:40",
  "MONTO": "565.00",
  "FACTURADO": "NO",
  "CANAL_VENTA": "Mostrador",
  "TIPO": "CONTADO",
  "DETALLE": [
    {"CODIGO": "P001", "PRODUCTO": "Pastel tres leches", "CANTIDAD": "1", "PRECIO": "525.00", "DESCUENTO": "0"},
    {"CODIGO": "257", "PRODUCTO": "Servicio Domicilio 2", "CANTIDAD": "1", "PRECIO": "15.00", "DESCUENTO": "0"},
    {"CODIGO": "V001", "PRODUCTO": "Velas", "CANTIDAD": "1", "PRECIO": "25.00", "DESCUENTO": "0"}
  ]
}
```

- [ ] **Step 2: escribir pruebas fallidas**

```python
def test_fetch_returns_canonical_note(point_note_payload):
    note = service.fetch(pk_nota="900001")
    assert note.pk_nota == "900001"
    assert note.folio == "18452"
    assert note.total == Decimal("565.00")
    assert note.lines[1].point_code == "257"

def test_fetch_rejects_total_mismatch(point_note_payload):
    point_note_payload["MONTO"] = "999.00"
    with pytest.raises(PointNoteIntegrityError, match="total"):
        service.fetch(pk_nota="900001")
```

- [ ] **Step 3: comprobar fallos**

```bash
docker compose run --rm web pytest pos_bridge/tests/test_point_note_detail_service.py -q
```

Expected: `FAIL` porque el servicio no existe.

- [ ] **Step 4: implementar tipos y normalización**

```python
@dataclass(frozen=True)
class PointNoteLine:
    point_code: str
    description: str
    quantity: Decimal
    unit_price: Decimal
    discount: Decimal
    line_total: Decimal

@dataclass(frozen=True)
class PointNote:
    pk_nota: str
    folio: str
    branch_name: str
    sold_at: datetime
    total: Decimal
    invoiced: bool
    payment_type: str
    point_channel: str
    lines: tuple[PointNoteLine, ...]
    source_endpoint: str
```

`fetch()` debe usar únicamente el contrato confirmado en Task 1, validar campos obligatorios, decimales no negativos y reconciliar total según la semántica real de Point documentada.

- [ ] **Step 5: ejecutar pruebas**

```bash
docker compose run --rm web pytest \
  pos_bridge/tests/test_point_note_detail_service.py \
  pos_bridge/tests/test_probe_point_note_detail.py -q
```

Expected: `PASS`.

- [ ] **Step 6: commit**

```bash
git add pos_bridge/services/point_note_detail_service.py \
  pos_bridge/tests/test_point_note_detail_service.py \
  pos_bridge/tests/fixtures/point_note_detail.json
git commit -m "feat(point): normaliza detalle de nota"
```

## Entrega 2: dominio y API canónicos del ERP

### Task 3: Extender canales y vínculo Point

**Files:**
- Modify: `crm/models.py`
- Create: `crm/migrations/0007_pedidocliente_point_note_and_channels.py`
- Modify: `crm/tests.py`

- [ ] **Step 1: escribir pruebas de modelo fallidas**

```python
def test_pedido_accepts_social_channels(self):
    self.assertIn(("FACEBOOK", "Facebook"), PedidoCliente.CANAL_CHOICES)
    self.assertIn(("INSTAGRAM", "Instagram"), PedidoCliente.CANAL_CHOICES)

def test_point_note_identity_is_unique(self):
    PedidoCliente.objects.create(
        cliente=self.cliente,
        descripcion="Uno",
        point_note_id="900001",
        point_note_snapshot={"pk_nota": "900001"},
    )
    with self.assertRaises(IntegrityError):
        PedidoCliente.objects.create(
            cliente=self.otro_cliente,
            descripcion="Dos",
            point_note_id="900001",
            point_note_snapshot={"pk_nota": "900001"},
        )
```

- [ ] **Step 2: ejecutar pruebas y confirmar fallo**

```bash
docker compose run --rm web python manage.py test \
  crm.tests.CRMModelsTests.test_pedido_accepts_social_channels \
  crm.tests.CRMModelsTests.test_point_note_identity_is_unique
```

- [ ] **Step 3: implementar campos**

Añadir a `PedidoCliente`:

```python
CANAL_FACEBOOK = "FACEBOOK"
CANAL_INSTAGRAM = "INSTAGRAM"

point_note_id = models.CharField(max_length=120, blank=True, default="", db_index=True)
point_note_folio = models.CharField(max_length=80, blank=True, default="", db_index=True)
point_note_snapshot = models.JSONField(default=dict, blank=True, editable=False)
point_note_fetched_at = models.DateTimeField(null=True, blank=True, editable=False)
social_reference = models.CharField(max_length=180, blank=True, default="")
```

Crear restricción única condicional para `point_note_id != ""`. Aplicar la misma inmutabilidad de `payload_snapshot` a `point_note_snapshot`.

- [ ] **Step 4: generar y revisar migración**

```bash
docker compose run --rm web python manage.py makemigrations crm
docker compose run --rm web python manage.py migrate --plan
```

Expected: creación de cinco campos, ampliación de choices y una restricción única; sin operaciones destructivas.

- [ ] **Step 5: pruebas y commit**

```bash
docker compose run --rm web python manage.py test crm.tests
git add crm/models.py crm/migrations/0007_pedidocliente_point_note_and_channels.py crm/tests.py
git commit -m "feat(crm): vincula pedidos con notas Point"
```

### Task 4: Consolidar estados y datos de entrega

**Files:**
- Modify: `logistica/models.py`
- Create: `logistica/migrations/0045_solicituddomicilio_operacion_canonica.py`
- Modify: `logistica/tests_domicilios_omnicanal.py`

- [ ] **Step 1: escribir pruebas fallidas**

```python
def test_domicilio_ready_requires_point_note_and_gps(self):
    solicitud = self.make_solicitud(estatus="LISTO")
    with self.assertRaises(ValidationError):
        solicitud.full_clean()

def test_en_ruta_requires_assignment(self):
    solicitud = self.make_solicitud(estatus="EN_RUTA")
    solicitud.pedido_cliente.point_note_id = "900001"
    solicitud.direccion_cliente.latitud = Decimal("25.790001")
    solicitud.direccion_cliente.longitud = Decimal("-108.990001")
    with self.assertRaises(ValidationError):
        solicitud.full_clean()
```

- [ ] **Step 2: implementar estados y campos**

`SolicitudDomicilio` conservará historial pero usará:

```python
ESTATUS_PENDIENTE_POINT = "PENDIENTE_POINT"
ESTATUS_CONFIRMADO = "CONFIRMADO"
ESTATUS_PREPARANDO = "PREPARANDO"
ESTATUS_LISTO = "LISTO"
ESTATUS_EN_RUTA = "EN_RUTA"
ESTATUS_ENTREGADO = "ENTREGADO"
ESTATUS_CANCELADO = "CANCELADO"

ventana_inicio = models.DateTimeField(null=True, blank=True)
ventana_fin = models.DateTimeField(null=True, blank=True)
instrucciones_entrega = models.CharField(max_length=500, blank=True, default="")
cancelacion_motivo = models.CharField(max_length=300, blank=True, default="")
legacy_without_point = models.BooleanField(default=False, editable=False)
```

Implementar `clean()` para exigir Point + GPS antes de `LISTO`, asignación antes de `EN_RUTA`, evidencia antes de `ENTREGADO` y motivo en `CANCELADO`.

- [ ] **Step 3: migración de estados existentes**

La migración de datos mapeará registros que ya tengan `point_note_id`:

```python
{
    "PENDIENTE": "CONFIRMADO",
    "ASIGNADO": "LISTO",
    "EN_RUTA": "EN_RUTA",
    "ENTREGADO": "ENTREGADO",
    "CANCELADO": "CANCELADO",
}
```

Los registros activos sin `point_note_id` pasarán a `PENDIENTE_POINT`. Los históricos `ENTREGADO` y `CANCELADO` conservarán su estado con `legacy_without_point=True`; no entrarán en la bandeja activa ni serán revalidados por `clean()` hasta una conciliación explícita.

- [ ] **Step 4: pruebas y commit**

```bash
docker compose run --rm web python manage.py test logistica.tests_domicilios_omnicanal
docker compose run --rm web python manage.py migrate --plan
git add logistica/models.py \
  logistica/migrations/0045_solicituddomicilio_operacion_canonica.py \
  logistica/tests_domicilios_omnicanal.py
git commit -m "feat(logistica): unifica estados de domicilios"
```

### Task 5: Crear servicio transaccional nota → pedido

**Files:**
- Create: `crm/services/point_order_link.py`
- Create: `crm/tests_point_order_link.py`

- [ ] **Step 1: escribir pruebas fallidas**

```python
def test_same_point_note_returns_same_order(self):
    first = link_point_note(command=self.command, actor=self.user)
    second = link_point_note(command=self.command, actor=self.user)
    self.assertEqual(first.order.pk, second.order.pk)
    self.assertEqual(PedidoCliente.objects.count(), 1)
    self.assertEqual(SolicitudDomicilio.objects.count(), 1)

def test_customer_reused_by_normalized_phone(self):
    existing = Cliente.objects.create(nombre="María", telefono="667 123 4567")
    result = link_point_note(command=self.command_with_phone("6671234567"), actor=self.user)
    self.assertEqual(result.order.cliente_id, existing.id)
```

- [ ] **Step 2: implementar comando y resultado**

```python
@dataclass(frozen=True)
class LinkPointOrderCommand:
    pk_nota: str
    channel: str
    customer_name: str
    customer_phone: str
    customer_email: str
    address: str
    references: str
    latitude: Decimal | None
    longitude: Decimal | None
    place_id: str
    social_reference: str
    delivery_window_start: datetime | None
    delivery_window_end: datetime | None
    instructions: str

@transaction.atomic
def link_point_note(*, command: LinkPointOrderCommand, actor) -> LinkPointOrderResult:
    note = PointNoteDetailService().fetch(pk_nota=command.pk_nota)
    existing = PedidoCliente.objects.select_for_update().filter(
        point_note_id=note.pk_nota,
    ).first()
    if existing:
        return LinkPointOrderResult.from_existing(existing)
    # Crear o reutilizar cliente/dirección y después pedido/domicilio.
```

El snapshot se obtiene dentro del servicio, no desde el navegador. Usar `select_for_update` más la restricción única; ante carrera, recuperar el pedido ganador.

- [ ] **Step 3: pruebas de concurrencia**

```bash
docker compose run --rm web python manage.py test crm.tests_point_order_link --keepdb
```

Expected: una nota, un pedido y un domicilio aun con dos llamadas simultáneas.

- [ ] **Step 4: commit**

```bash
git add crm/services/point_order_link.py crm/tests_point_order_link.py
git commit -m "feat(crm): crea pedido idempotente desde Point"
```

### Task 6: Exponer API canónica

**Files:**
- Modify: `api/omnichannel_serializers.py`
- Modify: `api/omnichannel_views.py`
- Modify: `api/urls.py`
- Modify: `api/tests_omnichannel.py`

- [ ] **Step 1: escribir pruebas fallidas de rutas**

```python
def test_note_search_returns_candidates(self):
    response = self.client.get(
        "/api/public/v1/omnichannel/point-notes/",
        {"folio": "18452", "sucursal": "Matriz"},
        **self.auth_headers,
    )
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.json()["results"][0]["pk_nota"], "900001")

def test_create_from_point_is_idempotent(self):
    payload = self.point_order_payload()
    first = self.client.post(self.orders_url, payload, format="json", **self.auth_headers)
    second = self.client.post(self.orders_url, payload, format="json", **self.auth_headers)
    self.assertEqual(first.json()["pedido_id"], second.json()["pedido_id"])
```

- [ ] **Step 2: definir contratos**

Crear serializadores:

```python
class PointNoteSearchSerializer(serializers.Serializer):
    folio = serializers.CharField(max_length=80)
    sucursal = serializers.CharField(max_length=120)
    fecha = serializers.DateField(required=False)

class PointLinkedOrderSerializer(serializers.Serializer):
    pk_nota = serializers.CharField(max_length=120)
    canal = serializers.ChoiceField(choices=PedidoCliente.CANAL_CHOICES)
    social_reference = serializers.CharField(max_length=180, required=False, allow_blank=True)
    cliente = OmnichannelCustomerInputSerializer()
    direccion = OmnichannelAddressInputSerializer()
    ventana_inicio = serializers.DateTimeField(required=False, allow_null=True)
    ventana_fin = serializers.DateTimeField(required=False, allow_null=True)
    instrucciones_entrega = serializers.CharField(max_length=500, required=False, allow_blank=True)
```

- [ ] **Step 3: añadir endpoints**

```text
GET  /api/public/v1/omnichannel/point-notes/
GET  /api/public/v1/omnichannel/point-notes/<pk_nota>/
POST /api/public/v1/omnichannel/point-orders/
GET  /api/public/v1/omnichannel/deliveries/
GET  /api/public/v1/omnichannel/deliveries/<id>/
PATCH /api/public/v1/omnichannel/deliveries/<id>/status/
```

Todas las rutas requieren API key con capability omnicanal, aplican límites de consulta y registran acceso sin PII en logs.

- [ ] **Step 4: ejecutar suite**

```bash
docker compose run --rm web python manage.py test \
  api.tests_omnichannel \
  crm.tests_point_order_link \
  logistica.tests_domicilios_omnicanal
docker compose run --rm web python manage.py check
```

- [ ] **Step 5: commit**

```bash
git add api/omnichannel_serializers.py api/omnichannel_views.py api/urls.py api/tests_omnichannel.py
git commit -m "feat(api): expone domicilios canónicos"
```

### Task 7: Crear ficha ERP exacta y retirar doble alta

**Files:**
- Modify: `crm/views.py`
- Modify: `crm/urls.py`
- Create: `crm/templates/crm/pedido_domicilio_detail.html`
- Modify: `crm/tests.py`
- Modify: `logistica/views.py`
- Modify: `logistica/templates/logistica/domicilios_ecommerce.html`

- [ ] **Step 1: escribir pruebas de navegación**

```python
def test_domicilio_detail_uses_canonical_order(self):
    response = self.client.get(reverse("crm:pedido_domicilio_detail", args=[self.pedido.id]))
    self.assertContains(response, self.pedido.point_note_folio)
    self.assertContains(response, self.solicitud.get_estatus_display())

def test_legacy_ecommerce_page_does_not_create_second_delivery(self):
    response = self.client.get(reverse("logistica:domicilios_ecommerce"))
    self.assertRedirects(response, reverse("crm:pedidos_domicilios"))
```

- [ ] **Step 2: implementar ficha de solo un registro**

La vista debe cargar `PedidoCliente` con cliente, dirección, `SolicitudDomicilio`, repartidor, unidad y operaciones de estado. El template mostrará snapshot Point como solo lectura y acciones ERP según permisos.

- [ ] **Step 3: redirigir la vista legacy**

`domicilios_ecommerce` dejará de crear `EntregaEcommerce`. Durante una versión responderá con redirección y mensaje a la bandeja canónica. Conservar modelos e historial sin borrarlos.

- [ ] **Step 4: pruebas y commit**

```bash
docker compose run --rm web python manage.py test crm.tests logistica.tests_domicilios_omnicanal
git add crm/views.py crm/urls.py crm/templates/crm/pedido_domicilio_detail.html \
  crm/tests.py logistica/views.py logistica/templates/logistica/domicilios_ecommerce.html
git commit -m "feat(crm): abre ficha única de domicilio"
```

## Entrega 3: BFF y Centro Operativo responsive

### Task 8: Extender el BFF del e-commerce

**Files:**
- Modify: `backend/app/schemas/omnichannel.py`
- Modify: `backend/app/services/erp_omnichannel_service.py`
- Modify: `backend/app/routers/admin.py`
- Modify: `backend/tests/test_erp_omnichannel_service.py`

- [ ] **Step 1: escribir pruebas fallidas**

```python
async def test_search_point_notes_proxies_validated_results():
    result = await client.search_point_notes(folio="18452", sucursal="Matriz")
    assert result["results"][0]["pk_nota"] == "900001"

async def test_point_timeout_returns_pending_not_fake_order():
    with pytest.raises(OmnichannelRequestError) as exc:
        await client.get_point_note("900001")
    assert exc.value.retryable is True
```

- [ ] **Step 2: implementar métodos**

```python
async def search_point_notes(self, *, folio: str, sucursal: str, fecha: str | None = None):
    return await self._request(
        "GET",
        "/api/public/v1/omnichannel/point-notes/",
        params={"folio": folio, "sucursal": sucursal, "fecha": fecha},
    )

async def create_point_order(self, payload: dict[str, Any]):
    return await self._request(
        "POST",
        "/api/public/v1/omnichannel/point-orders/",
        json=payload,
    )
```

Validar que las URLs ERP devueltas pertenecen al host configurado, como ya hace `create_order`.

- [ ] **Step 3: exponer rutas admin**

```text
GET  /api/admin/omnichannel/point-notes
GET  /api/admin/omnichannel/point-notes/{pk_nota}
POST /api/admin/omnichannel/point-orders
GET  /api/admin/omnichannel/deliveries
GET  /api/admin/omnichannel/deliveries/{id}
```

- [ ] **Step 4: pruebas y commit**

```bash
cd backend
pytest tests/test_erp_omnichannel_service.py -q
git add app/schemas/omnichannel.py app/services/erp_omnichannel_service.py \
  app/routers/admin.py tests/test_erp_omnichannel_service.py
git commit -m "feat(operacion): conecta notas Point del ERP"
```

### Task 9: Construir captura por pasos con TDD

**Files:**
- Create: `frontend/src/components/operacion/domicilios/types.ts`
- Create: `frontend/src/components/operacion/domicilios/PointNoteSearch.tsx`
- Create: `frontend/src/components/operacion/domicilios/OrderSummary.tsx`
- Create: `frontend/src/components/operacion/domicilios/CustomerStep.tsx`
- Create: `frontend/src/components/operacion/domicilios/DeliveryStep.tsx`
- Modify: `frontend/src/components/operacion/PedidoOmnicanalForm.tsx`
- Modify: `frontend/src/lib/api.ts`
- Test: `frontend/src/components/operacion/PedidoOmnicanalForm.test.tsx`

- [ ] **Step 1: escribir prueba fallida del flujo**

```tsx
it("bloquea productos y total después de confirmar la nota Point", async () => {
  render(<PedidoOmnicanalForm token="token" onSuccess={vi.fn()} onSaved={vi.fn()} />);
  await user.type(screen.getByLabelText("Folio de Point"), "18452");
  await user.selectOptions(screen.getByLabelText("Sucursal"), "Matriz");
  await user.click(screen.getByRole("button", { name: "Buscar en Point" }));
  await user.click(await screen.findByRole("button", { name: "Usar esta nota" }));
  expect(screen.getByText("Pastel tres leches")).toBeVisible();
  expect(screen.getByText("$565.00")).toBeVisible();
  expect(screen.queryByLabelText("Total manual")).not.toBeInTheDocument();
});
```

- [ ] **Step 2: comprobar fallo**

```bash
cd frontend
npx vitest run src/components/operacion/PedidoOmnicanalForm.test.tsx
```

- [ ] **Step 3: crear componentes enfocados**

`PedidoOmnicanalForm` solo mantendrá el estado del wizard:

```ts
type CaptureStep = "point" | "customer" | "delivery" | "review";
type CaptureState = {
  step: CaptureStep;
  note: PointNoteDetail | null;
  customer: CustomerDraft;
  address: AddressDraft;
  channel: DeliveryChannel;
  delivery: DeliveryDraft;
};
```

Cada paso tendrá encabezado, instrucciones, error cercano, acción primaria y acción secundaria. El snapshot del borrador incluirá `pk_nota`, no productos editables.

- [ ] **Step 4: aplicar UI/UX Pro Max**

Validar en código:

```text
labels visibles; foco visible; tab order = orden visual; cuerpo móvil >= 16 px;
targets >= 44 px; sin iconos emoji; loading deshabilita botones;
errores junto al campo; prefers-reduced-motion; contraste >= 4.5:1.
```

Mantener variables de marca actuales de Pollyana's Dolce. No introducir la paleta azul/naranja genérica sugerida por el buscador.

- [ ] **Step 5: ejecutar pruebas**

```bash
cd frontend
npx vitest run src/components/operacion/PedidoOmnicanalForm.test.tsx
npx tsc --noEmit
npm run lint
```

- [ ] **Step 6: commit**

```bash
git add frontend/src/components/operacion/domicilios \
  frontend/src/components/operacion/PedidoOmnicanalForm.tsx \
  frontend/src/components/operacion/PedidoOmnicanalForm.test.tsx \
  frontend/src/lib/api.ts
git commit -m "feat(operacion): captura domicilios desde Point"
```

### Task 10: Construir bandeja y ficha responsive

**Files:**
- Create: `frontend/src/components/operacion/domicilios/DeliveryInbox.tsx`
- Create: `frontend/src/components/operacion/domicilios/DeliveryDetail.tsx`
- Create: `frontend/src/components/operacion/domicilios/DeliveryStatusTimeline.tsx`
- Modify: `frontend/src/components/operacion/OperacionDomicilios.tsx`
- Test: `frontend/src/components/operacion/OperacionDomicilios.test.tsx`
- Create: `frontend/tests/operacion-domicilios-responsive.spec.ts`
- Modify: `frontend/package.json`
- Modify: `frontend/package-lock.json`

Esta tarea debe entregar y comprobar las tres familias de presentación: desktop, tablet y móvil.

- [ ] **Step 1: escribir pruebas de componente**

```tsx
it("abre la ficha ERP exacta y no una bandeja legacy", async () => {
  render(<OperacionDomicilios token="token" onError={vi.fn()} onSuccess={vi.fn()} />);
  const link = await screen.findByRole("link", { name: "Abrir en ERP" });
  expect(link).toHaveAttribute("href", expect.stringMatching(/\/crm\/pedidos\/\\d+\/domicilio\/$/));
});

it("muestra Facebook e Instagram como filtros separados", async () => {
  render(<OperacionDomicilios token="token" onError={vi.fn()} onSuccess={vi.fn()} />);
  expect(await screen.findByRole("option", { name: "Facebook" })).toBeVisible();
  expect(screen.getByRole("option", { name: "Instagram" })).toBeVisible();
});
```

- [ ] **Step 2: implementar layout adaptable**

```text
320–767: una columna, tarjetas, filtros en disclosure, acción primaria de ancho completo.
768–1279: dos columnas, filtros plegables, detalle en panel.
1280+: tabla/bandeja densa con panel lateral; mismo orden semántico.
```

No comprimir tablas en móvil. `DeliveryInbox` debe renderizar tarjetas móviles y encabezado tabular solo en `lg`.

- [ ] **Step 3: escribir pruebas Playwright responsive**

Instalar el runner de navegador en el worktree del frontend y conservar el lockfile:

```bash
cd frontend
npm install --save-dev @playwright/test@1.55.1
npx playwright install chromium
```

```ts
for (const viewport of [
  { width: 320, height: 700 },
  { width: 375, height: 812 },
  { width: 768, height: 1024 },
  { width: 1024, height: 768 },
  { width: 1280, height: 800 },
  { width: 1440, height: 900 },
]) {
  test(`domicilios sin overflow ${viewport.width}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.goto("/operacion/domicilios");
    await expect(page.locator("body")).toHaveJSProperty(
      "scrollWidth",
      await page.locator("body").evaluate((node) => node.clientWidth),
    );
  });
}
```

Añadir capturas de bandeja, formulario y ficha para los seis viewports.

- [ ] **Step 4: ejecutar revisión UI/UX Pro Max**

Ejecutar el diseño de sistema y guía Next.js:

```bash
python3 /Users/mauricioburgos/.agents/skills/ui-ux-pro-max/scripts/search.py \
  "responsive omnichannel operations dashboard bakery delivery accessible" \
  --design-system -p "Pollyana's Domicilios" -f markdown
python3 /Users/mauricioburgos/.agents/skills/ui-ux-pro-max/scripts/search.py \
  "responsive dashboard forms tables accessibility touch" --stack nextjs
```

Registrar en la PR qué recomendaciones se aplicaron y cuáles se descartaron por marca o arquitectura.

- [ ] **Step 5: pruebas y commit**

```bash
cd frontend
npx vitest run src/components/operacion/OperacionDomicilios.test.tsx
npx playwright test tests/operacion-domicilios-responsive.spec.ts
npx tsc --noEmit
npm run lint
npm run build
git add src/components/operacion/domicilios \
  src/components/operacion/OperacionDomicilios.tsx \
  src/components/operacion/OperacionDomicilios.test.tsx \
  tests/operacion-domicilios-responsive.spec.ts \
  package.json package-lock.json
git commit -m "feat(operacion): unifica bandeja responsive de domicilios"
```

### Task 11: Fallback, caché y regresión

**Files:**
- Modify: `frontend/public/sw.js`
- Modify: `frontend/tests/pwa-cache-version.test.mjs`
- Modify: `frontend/src/components/operacion/PedidoOmnicanalForm.test.tsx`
- Modify: `backend/tests/test_erp_omnichannel_service.py`

- [ ] **Step 1: probar Point caído, ERP caído y doble envío**

```tsx
it("conserva borrador pendiente sin inventar total cuando Point falla", async () => {
  api.adminSearchPointNotes.mockRejectedValue(new ApiError(503, "Point no disponible"));
  // buscar
  expect(await screen.findByText("Pendiente de vincular con Point")).toBeVisible();
  expect(screen.queryByLabelText("Total manual")).not.toBeInTheDocument();
});
```

```python
async def test_double_submit_uses_same_idempotency_key():
    first, second = await asyncio.gather(
        service.create_point_order(payload),
        service.create_point_order(payload),
    )
    assert first["pedido_id"] == second["pedido_id"]
```

- [ ] **Step 2: versionar caché**

Incrementar `CACHE_NAME` una sola vez después de terminar el frontend. No cachear `/api/`, `/backend-api/` ni respuestas autenticadas de domicilios.

- [ ] **Step 3: ejecutar regresión completa**

```bash
cd backend
pytest -q
cd ../frontend
npm test
npx tsc --noEmit
npm run lint
npm run build
```

Expected: todas las suites aprobadas; ningún cambio en checkout, pedidos web, despacho, tracking o driver.

- [ ] **Step 4: commit**

```bash
git add frontend/public/sw.js frontend/tests/pwa-cache-version.test.mjs \
  frontend/src/components/operacion/PedidoOmnicanalForm.test.tsx \
  backend/tests/test_erp_omnichannel_service.py
git commit -m "test(operacion): protege fallbacks de domicilios"
```

## Entrega 4: consolidación, despliegue y validación

### Task 12: Inventariar y redirigir superficies duplicadas

**Files:**
- Create: `docs/operacion/domicilios-route-inventory.md`
- Modify: rutas clasificadas en el inventario solo después de validación.

- [ ] **Step 1: documentar cada superficie**

La tabla debe contener:

```markdown
| Sistema | Ruta | Navegable hoy | Modelo escrito | Acción |
| ERP | /logistica/domicilios/ecommerce/ | Sí | EntregaEcommerce | Redirigir |
| ERP | /crm/pedidos/<id>/domicilio/ | Nueva | PedidoCliente/SolicitudDomicilio | Conservar |
| Centro | /operacion/domicilios | Sí | ERP vía BFF | Conservar |
```

- [ ] **Step 2: comprobar enlaces**

Ejecutar búsqueda de rutas y botones; cada “Gestionar en ERP” debe incluir el ID real. No aceptar enlaces a una raíz genérica.

- [ ] **Step 3: probar que rutas legacy no escriben**

Añadir una prueba por ruta retirada que confirme `301/302` o modo solo lectura y conteos de modelos sin cambios.

- [ ] **Step 4: commit**

```bash
git add docs/operacion/domicilios-route-inventory.md
git commit -m "docs(operacion): inventaría rutas de domicilios"
```

### Task 13: Desplegar ERP con respaldo y gate

**Files:** no crear archivos de aplicación en este paso.

- [ ] **Step 1: respaldo**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && ./scripts/backup_db.sh'
```

Registrar ruta y tamaño del respaldo.

- [ ] **Step 2: preflight**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && git status --short --branch && \
   docker compose exec -T web python manage.py migrate --check && \
   docker compose exec -T web python manage.py check'
```

Detener si el checkout está sucio o las migraciones no coinciden.

- [ ] **Step 3: desplegar**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

- [ ] **Step 4: smoke ERP**

Validar:

```text
health; búsqueda de nota autorizada; creación idempotente en datos de prueba;
ficha exacta; asignación; permisos; audit log; vista legacy redirigida.
```

No crear pedidos ficticios en producción sin una nota autorizada.

### Task 14: Desplegar Centro Operativo y validar dispositivos

**Files:** no crear archivos de aplicación en este paso.

- [ ] **Step 1: respaldo y preflight del e-commerce**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pollyanas-ecommerce && ./scripts/ops/preflight_production.sh && ./scripts/ops/db_backup.sh'
```

- [ ] **Step 2: desplegar frontend/BFF**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pollyanas-ecommerce && ./scripts/ops/deploy_public.sh'
```

- [ ] **Step 3: smoke de servicios**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pollyanas-ecommerce && ./scripts/ops/monitor_snapshot.sh'
```

Expected: backend healthy, ERP configurado y frontend listo.

- [ ] **Step 4: validación visual real**

En sesión autenticada, capturar:

```text
320, 375: formulario, búsqueda, ficha y bandeja en una columna.
768, 1024: dos columnas y filtros plegables.
1280, 1440: bandeja densa y panel de detalle.
```

Comprobar teléfono, mapa/GPS, foco, teclado, errores, carga, Point caído y reintento.

- [ ] **Step 5: validación funcional cruzada**

Con una nota autorizada:

1. buscar en Centro Operativo;
2. ligar cliente y dirección;
3. confirmar que ERP muestra el mismo `pedido_id`;
4. asignar repartidor ERP;
5. confirmar que Centro Operativo refleja asignación;
6. abrir acceso del repartidor y validar GPS/detalle mínimo;
7. repetir la creación y confirmar que no cambia el conteo.

- [ ] **Step 6: cierre**

Documentar commits, PR, respaldos, rutas, resultados de pruebas, capturas y cualquier limitación de Point. Solo marcar completo si no quedan pantallas activas que creen registros paralelos.

## Verificación final del programa

Ejecutar antes de declarar terminado:

```bash
# ERP
docker compose run --rm web python manage.py test \
  pos_bridge.tests.test_point_note_detail_service \
  crm.tests_point_order_link \
  api.tests_omnichannel \
  logistica.tests_domicilios_omnicanal
docker compose run --rm web python manage.py check

# E-commerce/BFF
cd backend && pytest -q

# Centro Operativo
cd ../frontend
npm test
npx tsc --noEmit
npm run lint
npm run build
npx playwright test tests/operacion-domicilios-responsive.spec.ts
```

La evidencia mínima es: una sola nota → un solo pedido; productos y total de Point; cliente/dirección reutilizados; canal separado; mismo ID en ERP y Centro; repartidor ERP visible; cero duplicados por reintento; y capturas correctas en los seis viewports.
