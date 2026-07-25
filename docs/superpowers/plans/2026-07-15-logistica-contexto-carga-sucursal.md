# Logistics Canonical Context and Branch Load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a mobile-first branch load workflow whose route, driver, unit, segment, checklist version, products, and permitted actions are validated as one canonical backend context, with auditable load and receipt discrepancies.

**Architecture:** Add a signed, short-lived operational context assembled only by the backend and revalidated under database locks for every load or receipt mutation. Keep the existing route/checklist models as operational truth, add focused idempotency and discrepancy records, expose one atomic branch endpoint, and adapt the existing PWA plus supervisor review/Planning gate without creating a parallel logistics module.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL transactions and row locks, Django signing, vanilla JavaScript PWA, service worker/WhiteNoise, Django TestCase/TransactionTestCase, browser mobile validation.

---

## File map

- Create `logistica/services_contexto_operativo.py`: assemble, sign, decode, and transactionally revalidate the canonical context.
- Create `logistica/services_carga_sucursal.py`: atomic branch save and idempotent replay.
- Create `logistica/services_discrepancias.py`: create, assign, resolve, and query overdue load/receipt discrepancies.
- Modify `logistica/models.py`: persist branch save events and auditable discrepancy cases.
- Create `logistica/migrations/0039_contexto_carga_sucursal.py`: database schema and constraints.
- Modify `api/logistica_serializers.py`: canonical context and atomic branch payload validation.
- Modify `api/logistica_views.py`: return context and expose the branch save endpoint; reuse it during receipt.
- Modify `api/urls.py`: register the branch save route.
- Modify `logistica/templates/logistica/pwa.html`: mobile branch selector, alphabetical search, compact editable rows, single discrepancy modal, and context-aware offline payloads.
- Modify `logistica/static/logistica/pwa/sw.js`: bump cache version and preserve mutation queue context.
- Modify `logistica/views.py`: supervisor review actions and overdue Planning gate.
- Modify `logistica/templates/logistica/revisiones_entrega.html`: unified `Enviado → Cargado → Recibido` review.
- Modify `logistica/templates/logistica/rutas.html`: mandatory review gate instead of the route creation form when debt exists.
- Create `logistica/tests_contexto_operativo.py`, `logistica/tests_carga_sucursal.py`, `logistica/tests_discrepancias.py`: focused unit, transactional, authorization, and gate tests.
- Modify `logistica/tests_invariantes_ruta.py`: PWA/HTML/service-worker contract tests.
- Modify `docs/ux/action-context-coverage.md`: register new async save and supervisor actions.

## Task 1: Establish a clean implementation baseline

**Files:**
- Verify: repository and PostgreSQL test environment

- [ ] **Step 1: Fetch and create the implementation branch from current main**

```bash
git fetch origin main
git worktree add /Users/mauricioburgos/Downloads/codex_worktrees/logistica-contexto-carga-sucursal -b codex/logistica-contexto-carga-sucursal origin/main
```

Expected: a clean worktree on `codex/logistica-contexto-carga-sucursal` with no unrelated files.

- [ ] **Step 2: Audit the branch before editing**

```bash
git status --short --branch
git diff --stat
git branch -vv
git rev-list --left-right --count origin/main...HEAD
```

Expected: clean status and `0 0` divergence.

- [ ] **Step 3: Verify migrations and Django using the configured PostgreSQL container**

```bash
docker compose run --rm -v /Users/mauricioburgos/Downloads/codex_worktrees/logistica-contexto-carga-sucursal:/app web python manage.py migrate --check
docker compose run --rm -v /Users/mauricioburgos/Downloads/codex_worktrees/logistica-contexto-carga-sucursal:/app web python manage.py check
```

Expected: no pending migrations and no errors. Existing unrelated warnings must be recorded verbatim.

- [ ] **Step 4: Run the eight route invariants already proven on the deployed fix**

```bash
docker compose run --rm -v /Users/mauricioburgos/Downloads/codex_worktrees/logistica-contexto-carga-sucursal:/app web python manage.py test \
  logistica.tests.LogisticaControlRutasTests.test_acompanante_no_puede_consultar_ni_confirmar_carga_de_la_ruta \
  logistica.tests.LogisticaControlRutasTests.test_api_checklist_repartidor_usa_tramo_actual \
  logistica.tests.LogisticaControlRutasTests.test_api_reubica_captura_si_la_sincronizacion_reemplazo_la_linea \
  logistica.tests.LogisticaControlRutasTests.test_api_confirma_entrega_de_parada_con_evidencia_idempotente \
  logistica.tests.LogisticaControlRutasTests.test_api_confirmar_entrega_rechaza_linea_carga_id_superada \
  logistica.tests.LogisticaControlRutasTests.test_api_no_confirma_entrega_despues_de_cedis_pendiente \
  logistica.tests.LogisticaPwaApiTests.test_bitacora_salida_bloquea_unidad_distinta_si_hay_ruta_activa \
  logistica.tests.LogisticaControlRutasTests.test_db_bloquea_dos_rutas_en_ruta_mismo_repartidor \
  --settings=config.settings_test --keepdb
```

Expected: `Ran 8 tests` and `OK`.

## Task 2: Add the canonical operational context

**Files:**
- Create: `logistica/services_contexto_operativo.py`
- Create: `logistica/tests_contexto_operativo.py`

- [ ] **Step 1: Write failing context tests**

Extend `LogisticaInvariantFixtures` from `logistica.tests_invariantes_ruta` with a CEDIS stop, companion user, and direct boss, then test the exact public contract:

```python
class ContextoOperativoTests(LogisticaInvariantFixtures):
    def test_contexto_usa_chofer_unidad_y_tramo_de_la_ruta(self):
        contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user_chofer)
        self.assertEqual(contexto.ruta_id, self.ruta.id)
        self.assertEqual(contexto.chofer_autorizado_id, self.repartidor.id)
        self.assertEqual(contexto.unidad_id, self.ruta.unidad_operativa_id)
        self.assertEqual(contexto.parada_cedis_origen_id, self.cedis_inicial.id)
        self.assertEqual(contexto.sucursales_permitidas, (self.sucursal.id,))

    def test_acompanante_no_recibe_contexto(self):
        with self.assertRaises(PermissionDenied):
            construir_contexto_operativo(ruta=self.ruta, actor=self.user_acompanante)

    def test_cambio_de_point_invalida_firma_anterior(self):
        firmado = construir_contexto_operativo(ruta=self.ruta, actor=self.user_chofer).token
        self.linea.cantidad_enviada_esperada += Decimal("1")
        self.linea.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])
        with self.assertRaises(ContextoOperativoObsoleto) as error:
            validar_contexto_operativo(token=firmado, ruta=self.ruta, actor=self.user_chofer)
        self.assertEqual(error.exception.codigo, "checklist_actualizado")
```

- [ ] **Step 2: Run the tests and verify RED**

```bash
docker compose run --rm web python manage.py test logistica.tests_contexto_operativo --settings=config.settings_test --keepdb
```

Expected: import failure for `services_contexto_operativo`.

- [ ] **Step 3: Implement the focused service**

Use immutable values and Django signing; the token must contain identifiers and a deterministic SHA-256 checklist version, never authorization supplied by the client:

```python
@dataclass(frozen=True)
class ContextoOperativo:
    ruta_id: int
    chofer_autorizado_id: int
    unidad_id: int
    tramo_id: str
    parada_cedis_origen_id: int
    version_checklist: str
    sucursales_permitidas: tuple[int, ...]
    productos_permitidos: tuple[int, ...]
    acciones_permitidas: tuple[str, ...]
    token: str

def version_checklist(lineas) -> str:
    facts = [
        (linea.id, linea.parada_id, linea.source_hash, str(linea.cantidad_enviada_esperada), linea.estatus)
        for linea in lineas
    ]
    payload = json.dumps(facts, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def validar_contexto_operativo(*, token, ruta, actor, bloquear=False):
    payload = signing.loads(token, salt=CONTEXT_SALT, max_age=CONTEXT_MAX_AGE_SECONDS)
    ruta_qs = RutaEntrega.objects.select_related("repartidor", "unidad_operativa")
    if bloquear:
        ruta_qs = ruta_qs.select_for_update()
    ruta_actual = ruta_qs.get(pk=ruta.pk)
    actual = _construir_contexto(ruta=ruta_actual, actor=actor, firmar=False, bloquear=bloquear)
    _comparar_payload(payload, actual)
    return actual
```

`_comparar_payload` must emit one of `contexto_obsoleto`, `tramo_cambiado`, or `checklist_actualizado`, including affected line IDs for the last code.

- [ ] **Step 4: Run context tests and existing authorization tests**

```bash
docker compose run --rm web python manage.py test logistica.tests_contexto_operativo logistica.tests.LogisticaControlRutasTests.test_acompanante_no_puede_consultar_ni_confirmar_carga_de_la_ruta --settings=config.settings_test --keepdb
```

Expected: all tests pass.

- [ ] **Step 5: Commit the context service**

```bash
git add logistica/services_contexto_operativo.py logistica/tests_contexto_operativo.py
git commit -m "feat(logistica): centralizar contexto operativo de ruta"
```

## Task 3: Persist atomic branch events and discrepancy cases

**Files:**
- Modify: `logistica/models.py`
- Create: `logistica/migrations/0039_contexto_carga_sucursal.py`
- Create: `logistica/tests_carga_sucursal.py`

- [ ] **Step 1: Write model constraint tests**

```python
class PersistenciaCargaSucursalTests(LogisticaInvariantFixtures):
    def test_evento_cliente_es_unico_por_ruta(self):
        RutaCargaSucursalEvento.objects.create(
            ruta=self.ruta, parada=self.parada, client_event_id="evt-1",
            payload_hash="a" * 64, contexto_version="b" * 64, respuesta={"ok": True},
        )
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                RutaCargaSucursalEvento.objects.create(
                    ruta=self.ruta, parada=self.parada, client_event_id="evt-1",
                    payload_hash="c" * 64, contexto_version="b" * 64, respuesta={},
                )

    def test_discrepancia_conserva_origen_y_tres_cantidades(self):
        caso = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta, parada=self.parada, linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_RECEPCION,
            cantidad_enviada=Decimal("10"), cantidad_cargada=Decimal("8"),
            cantidad_recibida=Decimal("7"), motivo="faltante_fisico",
            asignado_a=self.jefe,
        )
        self.assertEqual(caso.estado, DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE)
```

- [ ] **Step 2: Run model tests and verify RED**

Expected: missing model classes.

- [ ] **Step 3: Add models and constraints**

```python
class RutaCargaSucursalEvento(models.Model):
    ruta = models.ForeignKey(RutaEntrega, on_delete=models.PROTECT, related_name="eventos_carga_sucursal")
    parada = models.ForeignKey(ParadaRuta, on_delete=models.PROTECT, related_name="eventos_carga_sucursal")
    client_event_id = models.CharField(max_length=80)
    payload_hash = models.CharField(max_length=64)
    contexto_version = models.CharField(max_length=64)
    respuesta = models.JSONField(default=dict)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["ruta", "client_event_id"], name="carga_sucursal_evento_unico")]

class DiscrepanciaLogistica(models.Model):
    ORIGEN_CARGA = "CARGA_CEDIS"
    ORIGEN_RECEPCION = "RECEPCION_SUCURSAL"
    ESTADO_PENDIENTE_JEFE = "PENDIENTE_JEFE"
    ESTADO_VALIDADA_REAL = "VALIDADA_REAL"
    ESTADO_MARCADA_INCORRECTA = "MARCADA_INCORRECTA"
    ESTADO_ACLARACION_SOLICITADA = "ACLARACION_SOLICITADA"
    ruta = models.ForeignKey(RutaEntrega, on_delete=models.PROTECT, related_name="discrepancias")
    parada = models.ForeignKey(ParadaRuta, on_delete=models.PROTECT, related_name="discrepancias")
    linea_carga = models.ForeignKey(RutaCargaChecklistLinea, on_delete=models.PROTECT, related_name="discrepancias")
    origen = models.CharField(max_length=30, choices=[(ORIGEN_CARGA, "Carga CEDIS"), (ORIGEN_RECEPCION, "Recepción sucursal")])
    cantidad_enviada = models.DecimalField(max_digits=18, decimal_places=3)
    cantidad_cargada = models.DecimalField(max_digits=18, decimal_places=3, null=True)
    cantidad_recibida = models.DecimalField(max_digits=18, decimal_places=3, null=True)
    motivo = models.CharField(max_length=40)
    notas = models.TextField(blank=True, default="")
    estado = models.CharField(max_length=30, default=ESTADO_PENDIENTE_JEFE)
    asignado_a = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL, related_name="discrepancias_logistica_asignadas")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="discrepancias_logistica_creadas")
    creado_en = models.DateTimeField(auto_now_add=True)
    revisado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL, related_name="discrepancias_logistica_revisadas")
    revisado_en = models.DateTimeField(null=True)
    resolucion = models.TextField(blank=True, default="")
```

Add a conditional uniqueness constraint for one open discrepancy per `linea_carga` and `origen`.

- [ ] **Step 4: Generate, inspect, and test the migration**

```bash
docker compose run --rm web python manage.py makemigrations logistica
docker compose run --rm web python manage.py migrate --check
docker compose run --rm web python manage.py test logistica.tests_carga_sucursal.PersistenciaCargaSucursalTests --settings=config.settings_test --keepdb
```

Expected: migration contains only these two models/constraints; tests pass.

- [ ] **Step 5: Commit schema changes**

```bash
git add logistica/models.py logistica/migrations/ logistica/tests_carga_sucursal.py
git commit -m "feat(logistica): auditar carga por sucursal y discrepancias"
```

## Task 4: Implement atomic branch save with TDD

**Files:**
- Create: `logistica/services_carga_sucursal.py`
- Create: `logistica/services_discrepancias.py`
- Modify: `logistica/tests_carga_sucursal.py`

- [ ] **Step 1: Write failing service tests**

Add named tests `test_guarda_sucursal_completa`, `test_diferencia_exige_motivo`, `test_linea_invalida_revierte_sucursal_completa`, `test_contexto_obsoleto_no_escribe`, `test_reintento_identico_devuelve_respuesta_previa`, and `test_reintento_distinto_rechaza_conflicto`. Add the following concurrency assertion:

```python
class CargaSucursalConcurrenciaTests(TransactionTestCase):
    reset_sequences = True

    def test_dos_guardados_con_misma_version_solo_permiten_un_ganador(self):
        resultados = ejecutar_dos_transacciones_concurrentes(
            lambda event_id: guardar_carga_sucursal(
                actor=self.user, ruta=self.ruta, contexto_token=self.token,
                parada_id=self.parada.id, client_event_id=event_id,
                lineas=self.payload,
            )
        )
        self.assertEqual(sum(resultado.ok for resultado in resultados), 1)
        self.assertEqual(sum(resultado.codigo == "contexto_obsoleto" for resultado in resultados), 1)
        self.assertEqual(RutaCargaSucursalEvento.objects.count(), 1)
```

- [ ] **Step 2: Run the service tests and verify RED**

Expected: missing `guardar_carga_sucursal`.

- [ ] **Step 3: Implement the atomic service**

```python
@transaction.atomic
def guardar_carga_sucursal(*, actor, ruta, contexto_token, parada_id, client_event_id, lineas):
    contexto = validar_contexto_operativo(
        token=contexto_token, ruta=ruta, actor=actor, bloquear=True,
    )
    evento = RutaCargaSucursalEvento.objects.select_for_update().filter(
        ruta=ruta, client_event_id=client_event_id,
    ).first()
    payload_hash = calcular_payload_hash(parada_id=parada_id, lineas=lineas)
    if evento:
        if evento.payload_hash != payload_hash:
            raise ConflictoIdempotencia("client_event_id ya fue usado con otro contenido")
        return evento.respuesta
    bloqueadas = list(
        RutaCargaChecklistLinea.objects.select_for_update()
        .filter(checklist__ruta=ruta, parada_id=parada_id)
        .exclude(estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        .order_by("id")
    )
    validar_cobertura_exacta(contexto=contexto, esperadas=bloqueadas, recibidas=lineas)
    for linea, captura in emparejar_lineas(bloqueadas, lineas):
        aplicar_cantidad_y_motivo(linea=linea, captura=captura, actor=actor)
    respuesta = serializar_resultado_sucursal(ruta=ruta, parada_id=parada_id)
    RutaCargaSucursalEvento.objects.create(
        ruta=ruta, parada_id=parada_id, client_event_id=client_event_id,
        payload_hash=payload_hash, contexto_version=contexto.version_checklist,
        respuesta=respuesta, creado_por=actor,
    )
    return respuesta
```

`aplicar_cantidad_y_motivo` creates/updates a load discrepancy through `services_discrepancias.py` when `cantidad_cargada != cantidad_enviada_esperada`; it never sends notifications until `transaction.on_commit`.

- [ ] **Step 4: Run branch, context, and existing checklist tests**

Expected: all pass with PostgreSQL; no partial rows after forced exceptions.

- [ ] **Step 5: Commit the domain services**

```bash
git add logistica/services_carga_sucursal.py logistica/services_discrepancias.py logistica/tests_carga_sucursal.py
git commit -m "feat(logistica): guardar carga de sucursal de forma atomica"
```

## Task 5: Expose context and branch save through DRF

**Files:**
- Modify: `api/logistica_serializers.py`
- Modify: `api/logistica_views.py`
- Modify: `api/urls.py`
- Modify: `logistica/tests_carga_sucursal.py`

- [ ] **Step 1: Write failing API tests**

Use `reverse("api_logistica_ruta_carga_sucursal", kwargs={"ruta_id": ruta.id, "parada_id": parada.id})`; assert 200 for the driver, 403 for the companion, 409 for stale/idempotency conflicts, 400 for invalid lines, and no database changes for every rejection.

- [ ] **Step 2: Add explicit serializers**

```python
class RutaCargaSucursalLineaSerializer(serializers.Serializer):
    linea_id = serializers.IntegerField(min_value=1)
    source_hash = serializers.CharField(max_length=64)
    cantidad_cargada = serializers.DecimalField(max_digits=18, decimal_places=3, min_value=Decimal("0"))
    motivo_diferencia = serializers.ChoiceField(choices=RutaCargaChecklistLinea.MOTIVO_CHOICES, allow_blank=True, required=False)
    notas = serializers.CharField(allow_blank=True, required=False, default="")

class RutaCargaSucursalGuardarSerializer(serializers.Serializer):
    contexto_token = serializers.CharField()
    version_checklist = serializers.CharField(max_length=64)
    client_event_id = serializers.CharField(max_length=80)
    lineas = RutaCargaSucursalLineaSerializer(many=True, allow_empty=False)
```

- [ ] **Step 3: Add the endpoint and structured errors**

The GET checklist response adds `contexto_operativo`; POST delegates only to `guardar_carga_sucursal`. Map stale context/idempotency to HTTP 409 and validation to HTTP 400 with `{error, mensaje, productos_afectados, contexto_operativo}`.

- [ ] **Step 4: Register the URL**

```python
path(
    "logistica/rutas/<int:ruta_id>/carga-checklist/sucursales/<int:parada_id>/guardar/",
    LogisticaRutaCargaSucursalGuardarView.as_view(),
    name="api_logistica_ruta_carga_sucursal",
)
```

- [ ] **Step 5: Run API tests and commit**

```bash
docker compose run --rm web python manage.py test logistica.tests_carga_sucursal --settings=config.settings_test --keepdb
git add api/logistica_serializers.py api/logistica_views.py api/urls.py logistica/tests_carga_sucursal.py
git commit -m "feat(api): exponer contexto y carga masiva por sucursal"
```

## Task 6: Replace per-product PWA capture with the mobile branch workflow

**Files:**
- Modify: `logistica/templates/logistica/pwa.html`
- Modify: `logistica/tests_invariantes_ruta.py`

- [ ] **Step 1: Write failing PWA contract tests**

Assert the template contains stable functions `renderResumenSucursalesCarga`, `renderCapturaSucursal`, `abrirModalDiferencias`, and `guardarCargaSucursal`; assert the old UI no longer calls the per-product endpoint; assert search uses `localeCompare` and the bulk payload contains `contexto_token`, `version_checklist`, `client_event_id`, and every line.

- [ ] **Step 2: Implement state keyed by canonical IDs**

```javascript
state.cargaSucursal = {
  paradaId: null,
  busqueda: "",
  drafts: {},
  guardando: false,
  modalAbierto: false,
};

function cargaLineaKey(contexto, linea) {
  return `${contexto.ruta_id}:${contexto.tramo_id}:${contexto.version_checklist}:${linea.id}`;
}
```

Never key by array index. Reset the state whenever route, segment, or checklist version changes.

- [ ] **Step 3: Implement the compact mobile flow**

Render a segment summary with one button per branch; inside a branch, sort by `item_name.localeCompare(..., "es")`, filter by name/code, prefill `cantidad_enviada_esperada`, and keep one sticky `Guardar sucursal` button respecting safe-area insets. Use existing wine/gold variables and existing button/input classes.

- [ ] **Step 4: Implement one accessible discrepancy modal**

The modal lists only changed lines, requires one reason per line, preserves drafts on cancel/Escape, traps focus, returns focus to `Guardar sucursal`, disables only the pressed confirm button, and uses the global toast/error surface.

- [ ] **Step 5: Send one atomic request and handle stale context**

On 409, retain user-entered quantities in memory, replace the server context, show affected products, and require explicit review before resubmission. Offline queue entries include route, segment, branch, checklist version, token, event ID, and payload hash.

- [ ] **Step 6: Run PWA contract and backend tests, then commit**

```bash
docker compose run --rm web python manage.py test logistica.tests_invariantes_ruta logistica.tests_carga_sucursal --settings=config.settings_test --keepdb
git add logistica/templates/logistica/pwa.html logistica/tests_invariantes_ruta.py
git commit -m "feat(pwa): simplificar carga completa por sucursal"
```

## Task 7: Validate receipt against the canonical loaded quantity

**Files:**
- Modify: `logistica/services_entregas.py`
- Modify: `api/logistica_serializers.py`
- Modify: `api/logistica_views.py`
- Create: `logistica/tests_discrepancias.py`

- [ ] **Step 1: Write failing receipt discrepancy tests**

Test `Enviado 10 → Cargado 8 → Recibido 7`, require a receipt reason, keep the prior load case, create a separate receipt case, allow delivery completion, reject `SUPERADA`/wrong-segment lines, and replay identical events once.

- [ ] **Step 2: Extend the receipt payload**

Each evidence row carries `motivo_diferencia`; the root carries `contexto_token` and `version_checklist`. The serializer requires a reason only when `cantidad_entregada != linea.cantidad_cargada`.

- [ ] **Step 3: Revalidate and create receipt cases inside `confirmar_entrega_parada`**

Lock route, stop, and referenced checklist lines; validate context before changing `ParadaRuta`; create receipt discrepancies through `services_discrepancias.py`; schedule notifications with `transaction.on_commit`; preserve the existing geofence/idempotency behavior.

- [ ] **Step 4: Run receipt plus legacy delivery tests and commit**

```bash
docker compose run --rm web python manage.py test logistica.tests_discrepancias logistica.tests.LogisticaControlRutasTests.test_api_confirma_entrega_de_parada_con_evidencia_idempotente --settings=config.settings_test --keepdb
git add logistica/services_entregas.py api/logistica_serializers.py api/logistica_views.py logistica/tests_discrepancias.py
git commit -m "feat(logistica): trazar diferencia entre carga y recepcion"
```

## Task 8: Add supervisor resolution and next-day Planning gate

**Files:**
- Modify: `logistica/services_discrepancias.py`
- Modify: `logistica/views.py`
- Modify: `logistica/templates/logistica/revisiones_entrega.html`
- Modify: `logistica/templates/logistica/rutas.html`
- Modify: `logistica/tests_discrepancias.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Write failing assignment and gate tests**

Cover RRHH direct boss, Sales department fallback, Logistics/DG fallback, all three decisions, audit metadata, clarification counting as attended, GET Planning showing the mandatory inbox, and POST route creation returning 403/redirect without creating a route while overdue cases remain.

- [ ] **Step 2: Implement assignment and resolution services**

```python
def resolver_discrepancia(*, caso, actor, accion, comentario):
    if actor != caso.asignado_a and not can_manage_submodule(actor, "logistica", "rutas"):
        raise PermissionDenied
    estados = {
        "validar_real": DiscrepanciaLogistica.ESTADO_VALIDADA_REAL,
        "marcar_incorrecta": DiscrepanciaLogistica.ESTADO_MARCADA_INCORRECTA,
        "solicitar_aclaracion": DiscrepanciaLogistica.ESTADO_ACLARACION_SOLICITADA,
    }
    caso.estado = estados[accion]
    caso.revisado_por = actor
    caso.revisado_en = timezone.now()
    caso.resolucion = comentario.strip()
    caso.save(update_fields=["estado", "revisado_por", "revisado_en", "resolucion"])
```

`pendientes_vencidos_para_planeacion(user, fecha)` returns only prior-day `PENDIENTE_JEFE` cases assigned to that boss; clarification is attended and excluded.

- [ ] **Step 3: Gate Planning before both display and mutation**

At the start of `rutas`, compute overdue debt. On GET, render the mandatory discrepancy inbox and hide/disable creation. On POST, reject before parsing or creating any route. This server-side POST gate is mandatory; UI hiding alone is insufficient.

- [ ] **Step 4: Render the unified trace and async actions**

Show `Enviado → Cargado → Recibido`, origin, route/branch/product, reason, notes, and three actions using `data-async-action`; preserve scroll/focus and use the global toast.

- [ ] **Step 5: Run review/gate tests and commit**

```bash
docker compose run --rm web python manage.py test logistica.tests_discrepancias --settings=config.settings_test --keepdb
git add logistica/services_discrepancias.py logistica/views.py logistica/templates/logistica/revisiones_entrega.html logistica/templates/logistica/rutas.html logistica/tests_discrepancias.py docs/ux/action-context-coverage.md
git commit -m "feat(logistica): revisar discrepancias antes de planear"
```

## Task 9: Version the PWA and run the complete regression gate

**Files:**
- Modify: `logistica/static/logistica/pwa/sw.js`
- Modify: `logistica/tests_invariantes_ruta.py`

- [ ] **Step 1: Write the failing service-worker version assertion**

Require a cache name newer than v67 and containing `carga-sucursal-contexto`.

- [ ] **Step 2: Bump the cache and clear obsolete segment mutations**

```javascript
const CACHE_NAME = "pollyanas-logistica-pwa-v68-carga-sucursal-contexto";
```

Queue replay must discard entries whose route/segment/version no longer match the freshly loaded context and show an actionable warning instead of silently applying them.

- [ ] **Step 3: Run checks and the full focused suite**

```bash
docker compose run --rm web python manage.py migrate --check
docker compose run --rm web python manage.py check
docker compose run --rm web python manage.py test \
  logistica.tests_contexto_operativo \
  logistica.tests_carga_sucursal \
  logistica.tests_discrepancias \
  logistica.tests_invariantes_ruta \
  --settings=config.settings_test --keepdb
```

Expected: zero failures/errors. Record unrelated warnings separately.

- [ ] **Step 4: Run static and diff hygiene checks**

```bash
git diff --check
git status --short --branch
git diff origin/main..HEAD --stat
```

Expected: only planned Logistics/API/docs files.

- [ ] **Step 5: Commit the cache/version gate**

```bash
git add logistica/static/logistica/pwa/sw.js logistica/tests_invariantes_ruta.py
git commit -m "chore(pwa): versionar flujo canonico de carga"
```

## Task 10: Browser validation, PR, deploy, and production proof

**Files:**
- No new source files unless a verified defect is found

- [ ] **Step 1: Validate locally in a real mobile viewport**

Authenticate as a driver and verify: branch selector, alphabetical search, prefilled editable values, one modal, cancel preservation, atomic save, branch reopen, disabled/enabled exit, receipt discrepancy, console with no errors, relevant Network payloads, and offline replay rejection after a segment change.

- [ ] **Step 2: Validate authorization using a companion account**

The companion must receive no active route context and every copied GET/POST route URL must return 403 with zero writes.

- [ ] **Step 3: Review branch and open one draft PR**

```bash
git status --short --branch
git log --oneline --decorate -10
git worktree list
git diff origin/main..HEAD --stat
git push -u origin codex/logistica-contexto-carga-sucursal
gh pr create --draft --title "Logística: contexto canónico y carga por sucursal" --body-file /tmp/pr-logistica-contexto.md
```

The PR body must list functional behavior, main files, PostgreSQL tests, browser evidence, migration, service-worker bump, and known warnings.

- [ ] **Step 4: Require green CI and review every failure**

Do not merge while any required check is pending or failing. Fix defects with targeted tests and rerun the affected suite plus the complete focused suite.

- [ ] **Step 5: Merge and deploy through the safe script**

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web python manage.py migrate --check
```

Verify that migrations applied, `collectstatic` served v68, and the web process actually reloaded; restart only if the safe script evidence shows the preload process stayed stale.

- [ ] **Step 6: Validate production with a controlled real route**

Use one driver, assigned unit, CEDIS segment, two branches, one intentional load discrepancy, one receipt discrepancy, and supervisor review. Re-read database counts and confirm the same facts in PWA and supervisor UI. Do not alter unrelated operational records.

- [ ] **Step 7: Repeat until there are no known reproducible failures**

For every defect: capture exact state, add a failing regression test, implement the smallest fix, rerun affected and focused suites, redeploy, and repeat the real flow. Completion requires zero known reproducible failures in the approved flow; passing compilation/API alone is insufficient.

- [ ] **Step 8: Clean merged branches only after production validation**

```bash
git branch -D codex/logistica-contexto-carga-sucursal
git push origin --delete codex/logistica-contexto-carga-sucursal
git fetch --prune origin
```
