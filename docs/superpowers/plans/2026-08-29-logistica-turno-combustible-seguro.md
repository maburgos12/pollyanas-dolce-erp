# Turno y combustible seguros para repartidores - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evitar que GPS o botones inertes bloqueen gasolina/cierre y serializar aperturas concurrentes para que un repartidor conserve un solo turno abierto.

**Architecture:** La PWA conservará la evidencia principal y tratará coordenadas como metadato opcional, igual que el modelo actual. El endpoint de apertura bloqueará la fila del repartidor y repetirá la comprobación del turno dentro de la transacción antes de crear. No habrá cambios de modelos, migraciones, permisos ni datos históricos.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL 16, JavaScript embebido en template PWA, Service Worker, `TestCase`/`TransactionTestCase`.

---

## Mapa de archivos

- `logistica/test_pwa_ticket_photo.py`: contratos estáticos y visibles del formulario de combustible/cierre.
- `logistica/tests.py`: regresión conductual del endpoint de apertura de turno.
- `logistica/templates/logistica/pwa.html`: validaciones accionables y GPS opcional.
- `api/logistica_views.py`: sección crítica transaccional para abrir un turno.
- `logistica/static/logistica/pwa/sw.js`: versión de caché que entrega la PWA corregida.

### Task 1: PWA accionable sin GPS obligatorio

**Files:**
- Modify: `logistica/test_pwa_ticket_photo.py`
- Modify: `logistica/templates/logistica/pwa.html:4892-5115,5250-5388`

- [ ] **Step 1: Escribir pruebas fallidas del contrato visible**

Agregar pruebas que lean el template y exijan estas propiedades:

```python
def test_combustible_no_bloquea_guardado_por_falta_de_gps(self):
    source = self._source()
    self.assertNotIn('draft.carga_geoStatus === "ready" &&', source)
    self.assertIn("Se guardará sin ubicación", source)
    self.assertNotIn('id="guardar_carga_combustible_btn" class="primary-btn" type="button" onclick="guardarCargaCombustible()" ${puedeGuardarCargaCombustible() ? "" : "disabled"}', source)

def test_acciones_criticas_permanecen_habilitadas_y_validan_al_pulsar(self):
    source = self._source()
    self.assertIn('function faltantesCargaCombustible(draft)', source)
    self.assertIn('faltantes.join(", ")', source)
    self.assertIn('<button class="primary-btn" type="submit">Cerrar turno</button>', source)
```

Extraer `_source()` únicamente para evitar repetir la lectura del archivo.

- [ ] **Step 2: Ejecutar RED**

Run:

```bash
python manage.py test logistica.test_pwa_ticket_photo --verbosity=2
```

Expected: FAIL porque el GPS todavía forma parte de `puedeGuardarCargaCombustible()` y ambos botones se renderizan con `disabled`.

- [ ] **Step 3: Implementar el cambio mínimo en la PWA**

Introducir una función de faltantes basada solo en evidencia obligatoria:

```javascript
function faltantesCargaCombustible(draft) {
  const faltantes = [];
  if (!draft.carga_litros) faltantes.push("litros");
  if (!draft.carga_importe_total) faltantes.push("importe total");
  if (!draft.carga_foto_ticket) faltantes.push("foto del ticket");
  return faltantes;
}

function puedeGuardarCargaCombustible() {
  const draft = ensureBitacoraSalidaDraft();
  return faltantesCargaCombustible(draft).length === 0 && !draft.carga_guardando;
}
```

Cuando falle geolocalización, usar el texto `Se guardará sin ubicación. El ticket, importe y turno quedarán registrados.`. Mantener los botones habilitados salvo el estado `carga_guardando`; `guardarCargaCombustible()` y `guardarLlegada()` deben enumerar faltantes al pulsar. Solo agregar `latitud`/`longitud` al `FormData` cuando existan.

- [ ] **Step 4: Ejecutar GREEN y regresiones PWA**

Run:

```bash
python manage.py test logistica.test_pwa_ticket_photo logistica.tests_copy_pwa --verbosity=2
```

Expected: PASS; ticket, litros e importe continúan exigidos y el cierre conserva KM, nivel y foto.

- [ ] **Step 5: Commit**

```bash
git add logistica/test_pwa_ticket_photo.py logistica/templates/logistica/pwa.html
git commit -m "fix(logistica): evitar bloqueo de combustible por GPS"
```

### Task 2: Apertura de turno serializada

**Files:**
- Modify: `logistica/tests.py:3478-3939`
- Modify: `api/logistica_views.py:713-785`

- [ ] **Step 1: Escribir una prueba fallida de carrera concurrente**

Crear un `TransactionTestCase` enfocado que:

1. Construya usuario, grupo, sucursal, unidad, repartidor y licencia vigentes.
2. Lance dos clientes autenticados en hilos distintos contra `api_logistica_bitacora_salida`.
3. Sincronice ambos intentos antes de validar usando un wrapper temporal de `LogisticaBitacoraSalidaCreateSerializer.is_valid` con `threading.Barrier(2)` y timeout corto.
4. Compruebe respuestas `[201, 400]`, error `turno_abierto` para la segunda y exactamente una `BitacoraSalidaLlegada(cerrada=False)`.

La aserción central será:

```python
self.assertEqual(sorted(statuses), [201, 400])
self.assertEqual(
    BitacoraSalidaLlegada.objects.filter(repartidor=self.repartidor, cerrada=False).count(),
    1,
)
```

- [ ] **Step 2: Ejecutar RED**

Run:

```bash
python manage.py test logistica.tests.LogisticaBitacoraSalidaConcurrencyTests --verbosity=2
```

Expected: FAIL con dos respuestas 201 o dos turnos abiertos.

- [ ] **Step 3: Implementar la sección crítica mínima**

En `LogisticaBitacoraSalidaView.post`, después de permisos/licencia, entrar a `transaction.atomic()` y bloquear al repartidor real:

```python
with transaction.atomic():
    repartidor = Repartidor.objects.select_for_update().get(pk=repartidor.pk)
    abierta = (
        BitacoraSalidaLlegada.objects.select_related("unidad")
        .filter(repartidor=repartidor, cerrada=False)
        .first()
    )
    if abierta:
        return self._turno_abierto_response(abierta, ruta_activa)
    # validar unidad/ruta y serializer dentro de la misma transacción
    bitacora = serializer.save(ip_registro=request.META.get("REMOTE_ADDR"))
```

Extraer únicamente el constructor de la respuesta duplicada si evita duplicar el contrato actual. No cambiar permisos, elección de ruta ni mensajes existentes.

- [ ] **Step 4: Ejecutar GREEN y regresiones del endpoint**

Run:

```bash
python manage.py test logistica.tests.LogisticaBitacoraSalidaConcurrencyTests logistica.tests.LogisticaPwaApiTests --verbosity=2
```

Expected: PASS; una creación, un `turno_abierto`, y flujos existentes sin regresión.

- [ ] **Step 5: Commit**

```bash
git add logistica/tests.py api/logistica_views.py
git commit -m "fix(logistica): serializar apertura de turnos"
```

### Task 3: Caché y verificación integral local

**Files:**
- Modify: `logistica/static/logistica/pwa/sw.js:1`
- Modify: `logistica/templates/logistica/pwa.html:boot()`

- [ ] **Step 1: Escribir/actualizar la prueba de versión PWA**

Agregar una aserción que exija la nueva versión tanto en `CACHE_NAME` como en la URL de registro, por ejemplo `v89-turno-combustible-seguro`.

- [ ] **Step 2: Ejecutar RED**

Run:

```bash
python manage.py test logistica.test_pwa_ticket_photo --verbosity=2
```

Expected: FAIL porque producción sigue declarando v88.

- [ ] **Step 3: Incrementar la versión de caché en ambos consumidores**

```javascript
const CACHE_NAME = "pollyanas-logistica-pwa-v89-turno-combustible-seguro";
```

y registrar `pwa_sw` con `?v=route-control-v89-turno-combustible-seguro`.

- [ ] **Step 4: Ejecutar verificación completa local**

Run:

```bash
python manage.py test logistica.test_pwa_ticket_photo logistica.tests_copy_pwa logistica.tests.LogisticaBitacoraSalidaConcurrencyTests logistica.tests.LogisticaPwaApiTests --verbosity=2
python manage.py migrate --check
python manage.py check
git diff --check
```

Expected: todos los tests PASS, cero migraciones pendientes, `check` sin errores y diff limpio.

- [ ] **Step 5: Commit**

```bash
git add logistica/static/logistica/pwa/sw.js logistica/templates/logistica/pwa.html logistica/test_pwa_ticket_photo.py
git commit -m "chore(logistica): renovar cache de PWA"
```

### Task 4: Revisión, PR, despliegue y consumidor real

**Files:**
- Review only: todo el diff contra `origin/main`

- [ ] **Step 1: Revisar alcance y estado**

```bash
git status --short --branch
git log --oneline --decorate -8
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
git worktree list
```

Expected: solo especificación, plan, tests, PWA, vista API y Service Worker.

- [ ] **Step 2: Ejecutar nuevamente las pruebas frescas antes del PR**

Repetir los cuatro comandos de verificación del Task 3 y conservar sus salidas.

- [ ] **Step 3: Push y PR**

```bash
git push -u origin codex/logistica-turno-combustible-seguro
gh pr create --base main --head codex/logistica-turno-combustible-seguro --title "Logística: evitar bloqueos de gasolina y turnos duplicados" --body "## Resumen
- permite registrar gasolina con evidencia aunque el GPS falle
- mantiene obligatorio el cierre con KM, nivel y foto
- serializa aperturas para impedir turnos duplicados

## Verificación
- pruebas focalizadas de PWA y API
- python manage.py migrate --check
- python manage.py check"
```

El PR debe documentar diagnóstico productivo, garantías conservadas y comandos ejecutados.

- [ ] **Step 4: Esperar CI y revisar antes de mergear**

```bash
task_pr_number="$(gh pr view --json number --jq .number)"
gh pr checks "$task_pr_number" --watch
gh pr view "$task_pr_number" --json state,mergeStateStatus,statusCheckRollup,url
```

Expected: CI verde y diff sin hallazgos sustantivos.

- [ ] **Step 5: Mergear y desplegar por el único flujo autorizado**

```bash
task_pr_number="$(gh pr view --json number --jq .number)"
gh pr merge "$task_pr_number" --squash --delete-branch
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

No ejecutar `git pull` manual en el VPS.

- [ ] **Step 6: Validar producción**

Confirmar por lectura fresca:

- SHA desplegado y servicios saludables.
- `CACHE_NAME` y URL de registro v89 servidos.
- Endpoint activo conserva el turno real sin modificarlo.
- Navegador real: GPS permitido y denegado, mensajes de faltantes, Network sin duplicados, consola limpia.
- Consulta posterior: ninguna apertura nueva duplicada.

- [ ] **Step 7: Cerrar el worktree registrado**

```bash
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
bash scripts/task_workspace_close.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 --task logistica_turno_combustible_seguro --state merged
```

Expected: tarea cerrada, branch/worktree exactos eliminados y referencias remotas podadas.
