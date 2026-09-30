# Horas Extra por Corte Quincenal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Asignar cada hora extra autorizada a un solo corte de prenómina, arrastrando al siguiente corte las que seguían pendientes al ejecutar el anterior y sin reactivar autorizaciones históricas ambiguas.

**Architecture:** `HoraExtra` conserva la solicitud y una bandera de participación en el control nuevo. `PrenominaMovimiento` sigue siendo la única asignación al corte; una restricción condicional global evita que la misma hora extra aparezca en dos cortes. El instante de corte es `PrenominaCorte.creado_en` y la bandeja muestra el estado usando una consulta agrupada, no consultas por fila.

**Tech Stack:** Django 5, PostgreSQL 16, templates Django, CSS existente, `django.test.TestCase`.

---

### Task 1: Proteger el histórico y la unicidad de asignación

**Files:**
- Modify: `rrhh/models.py:1437-1506`
- Modify: `rrhh/models.py:2470-2542`
- Create: `rrhh/migrations/0053_horaextra_seguimiento_prenomina.py`
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Write the failing model tests**

Agregar a `PrenominaModelTests`:

```python
def test_hora_extra_nueva_participa_en_prenomina(self):
    extra = HoraExtra.objects.create(
        empleado=self.empleado,
        fecha=date(2026, 9, 30),
        horas=Decimal("1.00"),
    )
    self.assertTrue(extra.requiere_aplicacion_prenomina)

def test_hora_extra_no_puede_asignarse_a_dos_cortes(self):
    extra = HoraExtra.objects.create(
        empleado=self.empleado,
        fecha=date(2026, 9, 15),
        horas=Decimal("1.00"),
    )
    corte_1 = PrenominaCorte.objects.create(
        fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 15),
        fecha_corte=date(2026, 9, 15), creado_por=self.user,
    )
    corte_2 = PrenominaCorte.objects.create(
        fecha_inicio=date(2026, 9, 16), fecha_fin=date(2026, 9, 30),
        fecha_corte=date(2026, 9, 30), creado_por=self.user,
    )
    datos = {
        "empleado": self.empleado,
        "fecha": extra.fecha,
        "tipo_movimiento_erp": PrenominaMovimiento.TIPO_HORA_EXTRA,
        "fuente_modelo": "rrhh.HoraExtra",
        "fuente_id": str(extra.pk),
        "horas": extra.horas,
    }
    PrenominaMovimiento.objects.create(corte=corte_1, **datos)
    with self.assertRaises(IntegrityError), transaction.atomic():
        PrenominaMovimiento.objects.create(corte=corte_2, **datos)

def test_migracion_protege_resueltas_y_conserva_pendientes(self):
    from importlib import import_module
    from django.apps import apps

    pendiente = HoraExtra.objects.create(
        empleado=self.empleado, fecha=date(2026, 9, 29), horas=Decimal("1.00"),
    )
    historica = HoraExtra.objects.create(
        empleado=self.otro_empleado, fecha=date(2026, 9, 15),
        horas=Decimal("1.00"), estado=HoraExtra.ESTADO_AUTORIZADO,
    )
    migracion = import_module("rrhh.migrations.0053_horaextra_seguimiento_prenomina")
    migracion.proteger_historico_horas_extra(apps, None)
    pendiente.refresh_from_db()
    historica.refresh_from_db()
    self.assertTrue(pendiente.requiere_aplicacion_prenomina)
    self.assertFalse(historica.requiere_aplicacion_prenomina)
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 manage.py test \
  rrhh.tests_prenomina.PrenominaModelTests.test_hora_extra_nueva_participa_en_prenomina \
  rrhh.tests_prenomina.PrenominaModelTests.test_hora_extra_no_puede_asignarse_a_dos_cortes \
  rrhh.tests_prenomina.PrenominaModelTests.test_migracion_protege_resueltas_y_conserva_pendientes \
  --keepdb
```

Expected: first test errors because the field does not exist; second test fails because the current constraint permits one movement per cut.

- [ ] **Step 3: Add the minimum schema**

Add to `HoraExtra`:

```python
requiere_aplicacion_prenomina = models.BooleanField(
    default=True,
    db_index=True,
    help_text="Incluye esta solicitud en el control de cortes de prenómina.",
)
```

Add to `PrenominaMovimiento.Meta.constraints`:

```python
models.UniqueConstraint(
    fields=["fuente_modelo", "fuente_id", "tipo_movimiento_erp"],
    condition=Q(
        fuente_modelo="rrhh.HoraExtra",
        fuente_id__gt="",
        tipo_movimiento_erp="HORA_EXTRA",
    ),
    name="rrhh_prenomina_hora_extra_fuente_unica",
),
```

Create migration `0053` with the field, the conditional constraint and this forward data operation:

```python
def proteger_historico_horas_extra(apps, schema_editor):
    HoraExtra = apps.get_model("rrhh", "HoraExtra")
    HoraExtra.objects.exclude(estado="pendiente").update(
        requiere_aplicacion_prenomina=False,
    )
```

The field is added with default `True`; therefore existing pending rows—including 281–284 and the current weekly backlog—remain pending and tracked, while every already-resolved historical row becomes `False`.

- [ ] **Step 4: Run tests and verify GREEN**

Run the two tests from Step 2. Expected: `OK`.

- [ ] **Step 5: Verify migration state with PostgreSQL**

Run the three tests from Step 2, which execute the data migration against PostgreSQL, and then run:

```bash
python3 manage.py migrate --check
```

Expected: no pending migrations.

- [ ] **Step 6: Commit**

```bash
git add rrhh/models.py rrhh/migrations/0053_horaextra_seguimiento_prenomina.py rrhh/tests_prenomina.py
git commit -m "feat(rrhh): proteger asignación de horas extra a prenómina"
```

### Task 2: Arrastrar horas autorizadas después del corte

**Files:**
- Modify: `rrhh/services_prenomina.py:178-187`
- Test: `rrhh/tests_prenomina.py:410-441`

- [ ] **Step 1: Write failing service tests**

Agregar a `PrenominaServiceTests` pruebas que creen una equivalencia `HORA_EXTRA` y comprueben:

```python
def test_pendiente_al_corte_se_arrastra_al_siguiente(self):
    extra = HoraExtra.objects.create(
        empleado=self.empleado, fecha=date(2026, 9, 15),
        horas=Decimal("2.00"), estado=HoraExtra.ESTADO_PENDIENTE,
    )
    corte_1 = crear_corte_prenomina(
        fecha_inicio=date(2026, 9, 1), fecha_fin=date(2026, 9, 15),
        fecha_corte=date(2026, 9, 15), creado_por=self.user,
    )
    extra.estado = HoraExtra.ESTADO_AUTORIZADO
    extra.fecha_autorizacion_jefe = corte_1.creado_en + timedelta(seconds=1)
    extra.save(update_fields=["estado", "fecha_autorizacion_jefe"])
    recalcular_corte_prenomina(corte_1)
    self.assertFalse(corte_1.movimientos.filter(fuente_id=str(extra.pk)).exists())

    corte_2 = PrenominaCorte.objects.create(
        fecha_inicio=date(2026, 9, 16), fecha_fin=date(2026, 9, 30),
        fecha_corte=date(2026, 9, 30), creado_por=self.user,
    )
    PrenominaCorte.objects.filter(pk=corte_2.pk).update(
        creado_en=extra.fecha_autorizacion_jefe + timedelta(seconds=1),
    )
    corte_2.refresh_from_db()
    recalcular_corte_prenomina(corte_2)
    self.assertTrue(corte_2.movimientos.filter(fuente_id=str(extra.pk)).exists())
```

Agregar además:

```python
def test_historica_sin_seguimiento_no_se_arrastra(self):
    extra = HoraExtra.objects.create(
        empleado=self.empleado, fecha=date(2026, 9, 15),
        horas=Decimal("1.00"), estado=HoraExtra.ESTADO_AUTORIZADO,
        fecha_autorizacion_jefe=timezone.now() - timedelta(days=5),
        requiere_aplicacion_prenomina=False,
    )
    corte = crear_corte_prenomina(
        fecha_inicio=date(2026, 9, 16), fecha_fin=date(2026, 9, 30),
        fecha_corte=date(2026, 9, 30), creado_por=self.user,
    )
    self.assertFalse(corte.movimientos.filter(fuente_id=str(extra.pk)).exists())
```

Actualizar la prueba idempotente existente para proporcionar `fecha_autorizacion_jefe=timezone.now() - timedelta(minutes=1)`.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 manage.py test \
  rrhh.tests_prenomina.PrenominaServiceTests.test_pendiente_al_corte_se_arrastra_al_siguiente \
  rrhh.tests_prenomina.PrenominaServiceTests.test_historica_sin_seguimiento_no_se_arrastra \
  rrhh.tests_prenomina.PrenominaServiceTests.test_hora_extra_autorizada_genera_movimiento_y_recalculo_idempotente \
  --keepdb
```

Expected: the carryover test fails because the current query requires the worked date inside the second cut; the protection test fails because the flag is ignored.

- [ ] **Step 3: Implement the minimum eligibility query**

Replace `_horas_extra_por_empleado` with:

```python
def _horas_extra_por_empleado(corte: PrenominaCorte, empleado_ids: list[int]):
    grouped = defaultdict(list)
    asignadas = PrenominaMovimiento.objects.filter(
        fuente_modelo="rrhh.HoraExtra",
        tipo_movimiento_erp=PrenominaMovimiento.TIPO_HORA_EXTRA,
    ).exclude(corte=corte).values_list("fuente_id", flat=True)
    asignadas_ids = [int(pk) for pk in asignadas if pk.isdigit()]
    horas_extra = HoraExtra.objects.filter(
        empleado_id__in=empleado_ids,
        fecha__lte=corte.fecha_fin,
        estado=HoraExtra.ESTADO_AUTORIZADO,
        fecha_autorizacion_jefe__lte=corte.creado_en,
        requiere_aplicacion_prenomina=True,
    ).exclude(pk__in=asignadas_ids).order_by("empleado_id", "fecha", "id")
    for hora_extra in horas_extra:
        grouped[hora_extra.empleado_id].append(hora_extra)
    return grouped
```

- [ ] **Step 4: Run tests and verify GREEN**

Run the tests from Step 2. Expected: `OK`.

- [ ] **Step 5: Run the complete prenómina suite**

```bash
python3 manage.py test rrhh.tests_prenomina --keepdb
```

Expected: `OK`.

- [ ] **Step 6: Commit**

```bash
git add rrhh/services_prenomina.py rrhh/tests_prenomina.py
git commit -m "feat(rrhh): arrastrar horas extra al siguiente corte"
```

### Task 3: Mostrar el corte sin agrandar la vista móvil

**Files:**
- Modify: `rrhh/views.py:3046-3094`
- Modify: `rrhh/templates/rrhh/horas_extra_list.html`
- Modify: `static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css`
- Test: `rrhh/tests_prenomina.py`

- [ ] **Step 1: Write the failing view test**

Crear `HorasExtraCorteViewTests` en `rrhh/tests_prenomina.py` con un superusuario autenticado, una solicitud pendiente rastreada, una autorizada rastreada y una asignada. Verificar:

```python
response = self.client.get(reverse("rrhh:rrhh_he_list"))
self.assertContains(response, "Pendiente de autorizar · siguiente corte")
self.assertContains(response, "Pendiente del próximo corte")
self.assertContains(response, f"Pago: {self.corte.folio}")
self.assertContains(response, "01/09/2026–15/09/2026")
```

- [ ] **Step 2: Run test and verify RED**

```bash
python3 manage.py test rrhh.tests_prenomina.HorasExtraCorteViewTests --keepdb
```

Expected: FAIL because the bandeja does not load or render assignment state.

- [ ] **Step 3: Load assignments in one query**

In `horas_extra_list`, after materializing `horas_extra`, query `PrenominaMovimiento` once:

```python
movimientos_prenomina = PrenominaMovimiento.objects.filter(
    fuente_modelo="rrhh.HoraExtra",
    tipo_movimiento_erp=PrenominaMovimiento.TIPO_HORA_EXTRA,
    fuente_id__in=[str(he.pk) for he in horas_extra],
).select_related("corte")
movimiento_por_hora = {
    int(movimiento.fuente_id): movimiento
    for movimiento in movimientos_prenomina
    if movimiento.fuente_id.isdigit()
}
```

Dentro del ciclo existente:

```python
he.movimiento_prenomina = movimiento_por_hora.get(he.pk)
```

Importar `PrenominaMovimiento` desde `rrhh.models` en el bloque existente.

- [ ] **Step 4: Render one compact status line**

En la primera columna de cada fila, después de las notas:

```django
{% if he.movimiento_prenomina %}
  <p class="ch-payroll-status">Pago: {{ he.movimiento_prenomina.corte.folio }} · {{ he.movimiento_prenomina.corte.fecha_inicio|date:"d/m/Y" }}–{{ he.movimiento_prenomina.corte.fecha_fin|date:"d/m/Y" }}</p>
{% elif he.requiere_aplicacion_prenomina and key == "pendiente" %}
  <p class="ch-payroll-status">Pendiente de autorizar · siguiente corte</p>
{% elif he.requiere_aplicacion_prenomina and key == "autorizado" %}
  <p class="ch-payroll-status">Pendiente del próximo corte</p>
{% endif %}
```

Agregar CSS sin crear tarjeta nueva:

```css
.ch-row-item .ch-payroll-status {
  color: #6f5415;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.35;
  margin-top: 6px;
}
```

Actualizar el query string de la hoja CSS en el template para invalidar caché de WhiteNoise. RRHH no tiene service worker propio, por lo que no requiere `CACHE_NAME`.

- [ ] **Step 5: Run test and verify GREEN**

Run the test from Step 2. Expected: `OK`.

- [ ] **Step 6: Verify query count and RRHH tests**

```bash
python3 manage.py test \
  rrhh.tests_prenomina.HorasExtraCorteViewTests \
  rrhh.tests.RRHHViewsTests.test_horas_extra_contexto_no_agrega_consultas_por_registro \
  rrhh.tests.RRHHViewsTests.test_jefe_asignado_ve_y_autoriza_horas_extra_en_su_bandeja \
  --keepdb
```

Expected: `OK`; the new assignment lookup remains one query regardless of row count.

- [ ] **Step 7: Commit**

```bash
git add rrhh/views.py rrhh/templates/rrhh/horas_extra_list.html static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css rrhh/tests_prenomina.py
git commit -m "feat(rrhh): mostrar destino de pago de horas extra"
```

### Task 4: Integración, navegador y entrega

**Files:**
- Verify all modified files

- [ ] **Step 1: Run integrity checks**

```bash
python3 manage.py makemigrations --check --dry-run
python3 manage.py migrate --check
python3 manage.py check
python3 manage.py test rrhh.tests_prenomina rrhh.tests_extra_conciliacion rrhh.tests_extra_jefatura --keepdb
```

Expected: no pending migration, 0 system-check errors and all tests `OK`.

- [ ] **Step 2: Validate the local authenticated page**

Create only local test data, run the development server and inspect `/rrhh/horas-extra/` on desktop and mobile widths. Confirm the three status labels, no console errors, no failed XHR/fetch and no extra vertical card.

- [ ] **Step 3: Review the final diff**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
git log --oneline --decorate -5
```

Expected: only RRHH source, migration, tests and the two approved documents.

- [ ] **Step 4: Run branch completion workflow**

Use `finishing-a-development-branch`: push the branch, create a draft PR, review it, merge to `main`, run `scripts/deploy_web_safe.sh` on the VPS without a manual `git pull`, apply migrations and validate production.

- [ ] **Step 5: Validate production safely**

Confirm after deploy:

```text
281–284: remain PENDIENTE, same dates and hours, tracked for future cut
other weekly backlog: remains PENDIENTE
resolved historical requests: seguimiento disabled
authenticated /rrhh/horas-extra/: compact status labels render
```

No authorization, rejection, payment or time edit is part of this deployment.

- [ ] **Step 6: Close the task**

Run `scripts/task_workspace_close.sh --state merged`, audit worktrees and prune only the verified task branch/worktree through the official lifecycle script.
