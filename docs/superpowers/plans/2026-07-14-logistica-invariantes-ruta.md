# Estabilización integral del flujo de rutas Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hacer que carga, recarga CEDIS, entregas excepcionales y liberación de rutas compartan invariantes de backend y no requieran correcciones manuales durante la jornada.

**Architecture:** Incorporar predicados de dominio para la transición Point y la resolución operativa, y servicios únicos para recarga CEDIS y liberación de ruta. Web, API y PWA serán consumidores; la pantalla no decidirá por su cuenta qué está enviado, resuelto o autorizado.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL, servicios Point existentes, JavaScript PWA y unittest de Django.

---

## Estructura de archivos

- Crear `logistica/domain_ruta.py`: predicados puros de transición Point y resolución de parada.
- Crear `logistica/tests_invariantes_ruta.py`: batería TDD aislada.
- Modificar `logistica/services_carga_ruta.py`: sincronización canónica, recarga, alertas y autorización.
- Modificar `logistica/services_rutas_control.py`: siguiente parada y liberación con bitácora.
- Modificar `api/logistica_views.py`, `api/logistica_serializers.py` y `logistica/views.py`: consumidores del dominio.
- Modificar `logistica/templates/logistica/pwa.html` y `logistica/static/logistica/pwa/sw.js`: consumo del contrato y bump de caché.

### Task 1: Distinguir Solicitado de Enviado explícito

**Files:**
- Create: `logistica/domain_ruta.py`
- Create: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/services_carga_ruta.py:56-83,181-376`

- [ ] **Step 1: Escribir pruebas fallidas para pendiente, cero confirmado y positivo**

```python
class PointEnviadoInvariantTests(LogisticaInvariantFixtures):
    def test_point_sin_transicion_enviado_permanece_pendiente_y_bloquea(self):
        line = self.point_line(requested="7", sent="0", sent_at=None, is_enviado=False)
        row = self.sync_line(line)
        self.assertEqual(row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIn("aún no registra Enviado", row.notas)
        self.assertIsNotNone(checklist_bloquea_salida(self.ruta))

    def test_point_enviado_confirmado_cero_genera_zero_expected_visible(self):
        line = self.point_line(requested="7", sent="0", sent_at=timezone.now(), is_enviado=True)
        row = self.sync_line(line)
        self.assertEqual(row.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertEqual(row.cantidad_enviada_esperada, Decimal("0"))
        self.assertEqual(row.cantidad_cargada, Decimal("0"))

    def test_point_enviado_distinto_de_solicitado_usa_enviado(self):
        line = self.point_line(requested="7", sent="5", sent_at=timezone.now(), is_enviado=True)
        row = self.sync_line(line)
        self.assertEqual(row.cantidad_solicitada, Decimal("7"))
        self.assertEqual(row.cantidad_enviada_esperada, Decimal("5"))
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar el rojo correcto**

Run: `python manage.py test logistica.tests_invariantes_ruta.PointEnviadoInvariantTests -v 2`

Expected: la solicitud sin transición falla porque hoy termina como `ZERO_EXPECTED`.

- [ ] **Step 3: Crear los predicados de dominio**

```python
def point_transfer_enviada(line: PointTransferLine) -> bool:
    transfer = (line.raw_payload or {}).get("transfer") or {}
    return bool(line.sent_at) or transfer.get("isEnviado") is True


def parada_resuelta_operativamente(parada: ParadaRuta) -> bool:
    if parada.estado in {ParadaRuta.ESTADO_VISITADA, ParadaRuta.ESTADO_OMITIDA}:
        return True
    if parada.punto.tipo == PuntoLogistico.TIPO_CEDIS:
        return False
    return parada.entrega_estado != ParadaRuta.ENTREGA_PENDIENTE
```

- [ ] **Step 4: Aplicar transición en todas las ramas de sincronización**

```python
enviada = point_transfer_enviada(line)
if not enviada:
    cantidad_cargada = None
    estatus = RutaCargaChecklistLinea.ESTATUS_PENDIENTE
    notas = "Point aún no registra Enviado para esta solicitud."
elif cantidad_esperada <= 0:
    cantidad_cargada = Decimal("0")
    estatus = RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED
    notas = "Point confirmó enviado final en cero; no requiere captura."
else:
    cantidad_cargada = None
    estatus = RutaCargaChecklistLinea.ESTATUS_PENDIENTE
    notas = ""
```

Actualizar `_linea_pendiente_envio_point` para usar `point_transfer_enviada()`, no `cantidad <= 0`.

- [ ] **Step 5: Ejecutar pruebas focalizadas y regresiones de cero**

Run:

```bash
python manage.py test \
  logistica.tests_invariantes_ruta.PointEnviadoInvariantTests \
  logistica.tests.LogisticaControlRutasTests.test_checklist_carga_point_en_cero_genera_linea_visible_resuelta \
  logistica.tests.LogisticaControlRutasTests.test_checklist_carga_point_cero_resuelve_solicitud_cedis_visible -v 2
```

Expected: PASS. Los fixtures de Enviado cero deben incluir evidencia de transición.

- [ ] **Step 6: Preparar checkpoint sin commit**

Run: `git diff --check && git status --short`

Expected: sólo archivos de logística; Claude conserva la decisión de commit.

### Task 2: Hacer canónica la línea Point y honesta la frescura

**Files:**
- Modify: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/services_carga_ruta.py:181-640`

- [ ] **Step 1: Escribir pruebas de caché, idempotencia y SUPERADA**

```python
def test_sync_cache_no_actualiza_sincronizado_en_como_sync_externo(self):
    anterior = timezone.now() - timedelta(hours=2)
    self.checklist.sincronizado_en = anterior
    self.checklist.save(update_fields=["sincronizado_en"])
    sincronizar_checklist_carga_desde_point(ruta=self.ruta, ejecutar_sync=False)
    self.checklist.refresh_from_db()
    self.assertEqual(self.checklist.sincronizado_en, anterior)

def test_mismo_folio_producto_dos_detalles_enviados_no_se_superan(self):
    self.sync_all(
        self.point_line(detail="10", sent="2", sent_at=timezone.now()),
        self.point_line(detail="11", sent="3", sent_at=timezone.now()),
    )
    self.assertEqual(self.active_lines().count(), 2)
    self.assertEqual(sum(r.cantidad_enviada_esperada for r in self.active_lines()), Decimal("5"))

def test_detalle_cero_es_superado_por_reemplazo_positivo_confirmado(self):
    old = self.point_line(detail="10", sent="0", sent_at=timezone.now())
    self.sync_all(old)
    new = self.point_line(detail="11", sent="3", sent_at=timezone.now())
    self.sync_all(old, new)
    self.assertEqual(
        RutaCargaChecklistLinea.objects.get(point_transfer_line=old).estatus,
        RutaCargaChecklistLinea.ESTATUS_SUPERADA,
    )

def test_linea_fusionada_cedis_no_puede_reutilizarse_en_otra_ruta(self):
    point_line = self.fuse_cedis_placeholder_with_point()
    other = self.create_route_for_same_branch()
    sincronizar_checklist_carga_desde_point(ruta=other, ejecutar_sync=False)
    self.assertFalse(other.checklist_carga.lineas.filter(point_transfer_line=point_line).exists())
```

- [ ] **Step 2: Ejecutar rojo**

Run: `python manage.py test logistica.tests_invariantes_ruta.PointCanonicalLineTests -v 2`

Expected: FAIL por frescura falsa, heurística amplia y reserva global incompleta.

- [ ] **Step 3: Corregir frescura y unicidad**

```python
checklist.point_sync_job = sync_job or checklist.point_sync_job
sync_fields = ["point_sync_job", "estatus", "actualizado_en"]
if sync_job is not None:
    checklist.sincronizado_en = timezone.now()
    sync_fields.append("sincronizado_en")
checklist.save(update_fields=sync_fields)
```

Excluir globalmente por `source_hash` **o** `point_transfer_line_id`. Sólo superar una línea del mismo folio/sucursal/producto cuando la nueva está enviada, la anterior no fue validada y está sin transición o enviada explícitamente en cero. Dos detalles positivos se conservan.

- [ ] **Step 4: Ejecutar regresiones**

Run: `python manage.py test logistica.tests_invariantes_ruta.PointCanonicalLineTests logistica.tests.LogisticaReglasAdyacentesStabilizationTests logistica.tests.LogisticaControlRutasTests -v 2`

Expected: PASS sin duplicar ni perder auditoría.

- [ ] **Step 5: Preparar checkpoint**

Run: `git diff --check && git status --short`

### Task 3: Sincronizar Point antes de abrir el siguiente tramo CEDIS

**Files:**
- Modify: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/services_carga_ruta.py:552-975`
- Modify: `api/logistica_views.py:1572-1607`
- Modify: `logistica/views.py:2288-2310`

- [ ] **Step 1: Escribir pruebas de orden, error, alerta y autorización**

```python
@patch("logistica.services_carga_ruta.sincronizar_checklist_carga_desde_point")
def test_recarga_sincroniza_antes_de_marcar_visita(self, sync):
    sync.side_effect = lambda **kwargs: self.assertEqual(
        ParadaRuta.objects.get(pk=self.cedis.pk).estado,
        ParadaRuta.ESTADO_PENDIENTE,
    ) or self.resumen_sync()
    self.assertEqual(self.post_recarga().status_code, 200)

@patch("logistica.services_carga_ruta.sincronizar_checklist_carga_desde_point")
def test_recarga_fallida_no_desbloquea_y_notifica_una_vez(self, sync):
    sync.side_effect = ValidationError("Point no respondió")
    first, second = self.post_recarga(), self.post_recarga()
    self.assertEqual((first.status_code, second.status_code), (503, 503))
    self.cedis.refresh_from_db()
    self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)
    self.assertEqual(self.alertas_sync().count(), 1)

def test_solicitud_sin_enviado_solicita_autorizacion(self):
    self.point_line_for_next_segment(sent="0", sent_at=None)
    response = self.post_recarga()
    self.assertEqual(response.status_code, 409)
    self.assertEqual(response.json()["estado_sync"], "PENDIENTE_ENVIADO")

def test_jefe_autoriza_snapshot_con_motivo_sin_convertir_cero(self):
    self.point_line_for_next_segment(sent="0", sent_at=None)
    response = self.post_recarga(
        user=self.manager,
        autorizar=True,
        motivo="Point caído; conteo físico revisado",
    )
    self.assertEqual(response.json()["estado_sync"], "AUTORIZADO")
    self.assertEqual(self.pending_point_row().estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
```

- [ ] **Step 2: Ejecutar rojo**

Run: `python manage.py test logistica.tests_invariantes_ruta.RecargaCedisInvariantTests -v 2`

Expected: FAIL porque hoy CEDIS se marca sin consultar Point.

- [ ] **Step 3: Implementar resultados estructurados**

```python
class RecargaCedisPointError(ValidationError):
    estado_sync = "ERROR_POINT"
    http_status = 503

class RecargaCedisPendienteEnviado(ValidationError):
    estado_sync = "PENDIENTE_ENVIADO"
    http_status = 409
```

Crear `_lineas_siguiente_tramo()` con límites de orden explícitos y un snapshot con ids de detalle, evidencia Enviado, cantidades y fecha de sync. Crear una alerta idempotente por `ruta + parada + snapshot_hash`; notificar sólo cuando el evento sea nuevo.

- [ ] **Step 4: Orquestar sync antes de mutar CEDIS**

```python
try:
    sincronizar_checklist_carga_desde_point(ruta=ruta, user=user, ejecutar_sync=True)
except ValidationError as exc:
    registrar_alerta_sync_point(ruta, parada, "ERROR_POINT", str(exc))
    if not autorizacion_valida(user, autorizar_sin_sync, motivo_autorizacion):
        raise RecargaCedisPointError("Point no pudo sincronizar; el jefe fue notificado.")

pendientes = lineas_siguiente_tramo_sin_enviado(ruta, parada)
if pendientes and not autorizacion_valida(user, autorizar_sin_sync, motivo_autorizacion):
    registrar_alerta_sync_point(ruta, parada, "PENDIENTE_ENVIADO", pendientes)
    raise RecargaCedisPendienteEnviado(
        "Hay solicitudes que aún no pasan a Enviado; el jefe fue notificado."
    )

return _confirmar_recarga_cedis_atomica(...)
```

La autorización exige permiso de gestión y motivo; registra actor/snapshot pero no altera datos Point.

- [ ] **Step 5: Mapear API y web**

```python
return Response(
    {"detail": str(exc), "estado_sync": exc.estado_sync, "jefe_notificado": True},
    status=exc.http_status,
)
```

La acción web usa el mismo servicio y sólo permite autorización al jefe.

- [ ] **Step 6: Ejecutar regresiones**

Run: `python manage.py test logistica.tests_invariantes_ruta.RecargaCedisInvariantTests logistica.tests.LogisticaReglasAdyacentesStabilizationTests logistica.tests.LogisticaControlRutasTests -v 2`

Expected: PASS, sin desactivar la nueva sincronización en fixtures.

- [ ] **Step 7: Preparar checkpoint**

Run: `git diff --check && git status --short`

### Task 4: Separar visita física de resolución operativa

**Files:**
- Modify: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/services_rutas_control.py:150-240`
- Modify: `logistica/services_carga_ruta.py:980-1060`
- Modify: `api/logistica_serializers.py`
- Modify: `api/logistica_views.py`
- Modify: `logistica/views.py`

- [ ] **Step 1: Escribir pruebas de excepción y geocerca siguiente**

```python
def test_entrega_excepcional_resuelve_sin_fabricar_visita(self):
    result = self.confirmar_sin_geocerca(self.first_stop)
    self.first_stop.refresh_from_db()
    self.assertTrue(result.requiere_revision)
    self.assertEqual(self.first_stop.estado, ParadaRuta.ESTADO_PENDIENTE)
    self.assertIsNone(self.first_stop.hora_llegada_real)
    self.assertTrue(parada_resuelta_operativamente(self.first_stop))

def test_geocerca_siguiente_no_queda_atrapada(self):
    self.confirmar_sin_geocerca(self.first_stop)
    self.registrar_dos_posiciones_confiables(self.second_stop)
    self.second_stop.refresh_from_db()
    self.assertEqual(self.second_stop.estado, ParadaRuta.ESTADO_VISITADA)

def test_cierres_comparten_resolucion_operativa(self):
    self.confirmar_sin_geocerca(self.first_stop)
    self.assertFalse(ruta_tiene_paradas_entregables_pendientes(self.ruta))
    self.assertEqual(self.close_by_api().status_code, 200)
```

- [ ] **Step 2: Ejecutar rojo**

Run: `python manage.py test logistica.tests_invariantes_ruta.EntregaOperativaInvariantTests -v 2`

Expected: FAIL en siguiente parada y cierre divergente.

- [ ] **Step 3: Usar predicado compartido**

En `_marcar_visitada_por_permanencia`, elegir la primera parada pendiente que no esté resuelta operativamente. En todos los cierres usar el mismo predicado. CEDIS pendiente siempre bloquea.

Agregar al serializer:

```python
operativamente_resuelta = serializers.SerializerMethodField()

def get_operativamente_resuelta(self, obj):
    return parada_resuelta_operativamente(obj)
```

- [ ] **Step 4: Ejecutar dominio de entregas**

Run: `python manage.py test logistica.tests_invariantes_ruta.EntregaOperativaInvariantTests logistica.tests.LogisticaEntregaDomainTests logistica.tests.LogisticaEntregaContratoFinalTests logistica.tests.LogisticaRevisionEntregaTests -v 2`

Expected: PASS sin fabricar GPS ni `VISITADA`.

- [ ] **Step 5: Preparar checkpoint**

Run: `git diff --check && git status --short`

### Task 5: Unificar liberación con turno y unidad activos

**Files:**
- Modify: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/services_rutas_control.py`
- Modify: `api/logistica_views.py:730-790,1219-1314`
- Modify: `logistica/views.py:2580-2660`

- [ ] **Step 1: Escribir pruebas de rechazo y enlace**

```python
def test_liberacion_administrativa_sin_turno_no_modifica_ruta(self):
    response = self.set_status_api(RutaEntrega.ESTATUS_EN_RUTA)
    self.assertEqual(response.status_code, 400)
    self.ruta.refresh_from_db()
    self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
    self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).exists())

def test_liberacion_administrativa_rechaza_unidad_distinta(self):
    self.open_shift(unit=self.other_unit)
    response = self.set_status_api(RutaEntrega.ESTATUS_EN_RUTA)
    self.assertEqual(response.status_code, 400)
    self.assertIn("unidad", response.json()["detail"].lower())

def test_liberacion_compatible_liga_bitacora_y_una_salida(self):
    shift = self.open_shift(unit=self.ruta.unidad_operativa)
    self.assertEqual(self.set_status_api(RutaEntrega.ESTATUS_EN_RUTA).status_code, 200)
    self.assertEqual(self.set_status_api(RutaEntrega.ESTATUS_EN_RUTA).status_code, 200)
    self.ruta.refresh_from_db()
    self.assertEqual(self.ruta.bitacora_salida_id, shift.id)
    self.assertEqual(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).count(), 1)
```

- [ ] **Step 2: Ejecutar rojo**

Run: `python manage.py test logistica.tests_invariantes_ruta.LiberacionRutaInvariantTests -v 2`

Expected: FAIL por bypass administrativo.

- [ ] **Step 3: Implementar servicio único**

```python
@transaction.atomic
def liberar_ruta_con_turno(*, ruta, actor, bitacora=None):
    ruta = RutaEntrega.objects.select_for_update().get(pk=ruta.pk)
    bitacora = bitacora or (
        BitacoraSalidaLlegada.objects.select_for_update()
        .filter(repartidor_id=ruta.repartidor_id, cerrada=False)
        .order_by("-hora_salida", "-id")
        .first()
    )
    if bitacora is None:
        raise ValidationError("El repartidor no tiene un turno activo.")
    if bitacora.repartidor_id != ruta.repartidor_id:
        raise ValidationError("El turno activo pertenece a otro repartidor.")
    if bitacora.unidad_id != ruta.unidad_operativa_id:
        raise ValidationError("El turno activo no corresponde a la unidad asignada a la ruta.")
    blocker = checklist_bloquea_salida(ruta)
    if blocker:
        raise ValidationError(blocker)
    ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
    ruta.bitacora_salida = bitacora
    ruta.hora_inicio_real = ruta.hora_inicio_real or bitacora.hora_salida
    ruta.save(update_fields=["estatus", "bitacora_salida", "hora_inicio_real", "updated_at"])
    EventoRuta.objects.get_or_create(
        ruta=ruta,
        tipo=EventoRuta.TIPO_SALIDA,
        defaults={
            "severidad": EventoRuta.SEVERIDAD_INFO,
            "descripcion": "Ruta liberada con turno activo validado.",
            "creado_por": actor,
        },
    )
    return ruta
```

Conservar bloqueos de otra ruta activa y traducir `IntegrityError` a conflicto controlado.

- [ ] **Step 4: Delegar las tres superficies**

`_liberar_ruta_desde_bitacora_salida`, `LogisticaRutaStatusView.post` y `ruta_status` web llaman `liberar_ruta_con_turno`. Ninguna escribe el estado directamente.

- [ ] **Step 5: Ejecutar pruebas**

Run: `python manage.py test logistica.tests_invariantes_ruta.LiberacionRutaInvariantTests logistica.tests.LogisticaPwaApiTests logistica.tests.LogisticaControlRutasTests -v 2`

Expected: PASS con un solo contrato.

- [ ] **Step 6: Preparar checkpoint**

Run: `git diff --check && git status --short`

### Task 6: Integrar PWA y comprobar el recorrido completo

**Files:**
- Modify: `logistica/tests_invariantes_ruta.py`
- Modify: `logistica/templates/logistica/pwa.html`
- Modify: `logistica/static/logistica/pwa/sw.js`
- Modify: `logistica/checks.py` sólo si el system check lo exige

- [ ] **Step 1: Escribir prueba integral de dos tramos**

```python
def test_recorrido_dos_tramos_con_cero_pasteles_pays_y_excepcion(self):
    route = self.route("CEDIS", "Nío", "Payán", "CEDIS", "Las Glorias")
    self.point_sent("Nío", "Bollo Vainilla", requested=2, sent=2)
    self.point_sent("Payán", "Bollo Vainilla", requested=5, sent=5)
    self.point_sent("Payán", "Pay de Limón", requested=2, sent=0)
    first = self.active_load(route)
    self.assertConsolidated(
        first,
        "Bollo Vainilla",
        expected=7,
        branches={"Nío": 2, "Payán": 5},
    )
    self.assertZeroExpected(first, "Pay de Limón")
    self.complete_first_segment_with_exception(route)
    self.return_to_cedis_and_sync(route)
    second = self.active_load(route)
    self.assertEqual(set(second.branch_names()), {"Las Glorias"})
    self.assertNoDuplicatePointDetails(route)
```

- [ ] **Step 2: Ejecutar recorrido**

Run: `python manage.py test logistica.tests_invariantes_ruta.RutaJourneyInvariantTests -v 2`

Expected: PASS sólo con los contratos conectados.

- [ ] **Step 3: Consumir backend en PWA**

En `renderParadasRuta` y `proximaParadaId`, usar `operativamente_resuelta`. Mantener separada la revisión. Ante 409/503 mostrar el mensaje y no avanzar; ante `AUTORIZADO`, recargar ruta/checklist.

- [ ] **Step 4: Bump del service worker**

```javascript
const CACHE_NAME = "pollyanas-logistica-pwa-v61-route-invariants";
```

No modificar otros caches.

- [ ] **Step 5: Ejecutar suite y checks**

Run:

```bash
python manage.py test logistica.tests_invariantes_ruta -v 2
python manage.py test logistica --parallel
python manage.py migrate --check
python manage.py check
git diff --check
```

Expected: tests en verde, cero migraciones y cero errores.

- [ ] **Step 6: Validar navegador real**

Validar con repartidor: carga consolidada, Enviado cero sin captura, solicitud no enviada con alerta, excepción sin geocerca que continúa, siguiente geocerca correcta, recarga CEDIS sincronizada y segundo tramo aislado. Revisar consola y XHR.

- [ ] **Step 7: Preparar entrega para Claude**

Run:

```bash
git status --short --branch
git diff --stat
git diff --check
git log --oneline --decorate -5
git worktree list
```

Entregar especificación, plan, diff, pruebas y evidencia. Claude decide commit, PR y deploy conforme a `CLAUDE.md`.

