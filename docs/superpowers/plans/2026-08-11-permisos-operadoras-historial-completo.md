# Historial de permisos para operadoras Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Agregar a la PWA limitada de Produccion los alcances `Este mes`, `Mes anterior` y `Todo el historial` para los permisos personales y capturados por cada operadora.

**Architecture:** Reutilizar el endpoint existente, que ya filtra por capturista/empleada y hace opcionales `mes` y `anio`. El frontend calculara el mes anterior y omitira ambos parametros para el historial completo; el backend seguira siendo la autoridad de seguridad. No se agregaran modelos, migraciones ni endpoints.

**Tech Stack:** Django 5, Django REST Framework, React 18 UMD embebido en template, PostgreSQL 16, Django TestCase y Playwright CLI.

---

### Task 1: Probar el contrato historico de minimo privilegio

**Files:**
- Modify: `bonos_produccion/tests_solicitudes_operadoras.py`
- Test: `bonos_produccion/tests_solicitudes_operadoras.py`

- [ ] **Step 1: Write the failing backend test**

Agregar a `OperadoraCatalogoPermisosTests` una prueba que cree dos permisos de meses distintos con `creado_por=self.operadora`, un permiso personal legacy y un permiso capturado por otra operadora. Consultar sin `mes` ni `anio` y exigir que solo regresen los tres permitidos, en orden descendente:

```python
def test_lista_operadora_sin_periodo_devuelve_todo_su_historial(self):
    otra_operadora = get_user_model().objects.create_user(username="otra.operadora")
    propios = [
        PermisoSalida.objects.create(
            empleado=self.produccion_empleado,
            creado_por=self.operadora,
            tipo=PermisoSalida.TIPO_PERMISO_HORA,
            fecha_inicio=fecha_inicio,
            motivo=motivo,
            origen_solicitud=PermisoSalida.ORIGEN_BONOS_PRODUCCION,
        )
        for fecha_inicio, motivo in (
            ("2026-07-10T12:00:00Z", "Mes anterior"),
            ("2026-06-10T12:00:00Z", "Mes historico"),
        )
    ]
    personal = PermisoSalida.objects.create(
        empleado=self.operadora_empleado,
        tipo=PermisoSalida.TIPO_PERMISO_HORA,
        fecha_inicio="2026-05-10T12:00:00Z",
        motivo="Personal legacy",
        origen_solicitud=PermisoSalida.ORIGEN_BONOS_PRODUCCION,
    )
    PermisoSalida.objects.create(
        empleado=self.produccion_empleado,
        creado_por=otra_operadora,
        tipo=PermisoSalida.TIPO_PERMISO_HORA,
        fecha_inicio="2026-08-10T12:00:00Z",
        motivo="Captura ajena",
        origen_solicitud=PermisoSalida.ORIGEN_BONOS_PRODUCCION,
    )

    response = self.client.get("/api/bonos-produccion/permisos/")

    self.assertEqual(response.status_code, 200)
    self.assertEqual(
        [row["id"] for row in response.json()["permisos"]],
        [propios[0].id, propios[1].id, personal.id],
    )
```

- [ ] **Step 2: Run the backend test and inspect the result**

Run:

```bash
python manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraCatalogoPermisosTests.test_lista_operadora_sin_periodo_devuelve_todo_su_historial -v 2
```

Expected: PASS if the existing optional-period contract is intact. This is a characterization test; it does not authorize skipping the frontend RED test.

- [ ] **Step 3: Write the failing PWA contract test**

Extender `OperadoraPwaTests.test_pwa_operadora_contiene_solo_las_cuatro_secciones_aprobadas` con aserciones para las etiquetas, el estado predeterminado y la funcion de mes anterior:

```python
self.assertContains(response, "['actual','Este mes']")
self.assertContains(response, "['anterior','Mes anterior']")
self.assertContains(response, "['historial','Todo el historial']")
self.assertContains(response, "useState('actual')")
self.assertContains(response, "function periodoAnterior(mes,anio)")
```

- [ ] **Step 4: Run the PWA test and verify RED**

Run:

```bash
python manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraPwaTests.test_pwa_operadora_contiene_solo_las_cuatro_secciones_aprobadas -v 2
```

Expected: FAIL because the three history controls and `periodoAnterior` do not exist yet.

### Task 2: Implementar los tres alcances en `PermisosTab`

**Files:**
- Modify: `bonos_produccion/templates/bonos_produccion/index.html`
- Test: `bonos_produccion/tests_solicitudes_operadoras.py`

- [ ] **Step 1: Add the previous-period helper**

Agregar junto a los helpers de fecha:

```javascript
function periodoAnterior(mes,anio){
  const mesNumero=Number(mes);
  const anioNumero=Number(anio);
  return mesNumero===1?{mes:12,anio:anioNumero-1}:{mes:mesNumero-1,anio:anioNumero};
}
```

- [ ] **Step 2: Add local scope state and scope-aware loading**

En `PermisosTab` agregar:

```javascript
const [alcancePermisos,setAlcancePermisos]=React.useState('actual');
```

Cambiar el efecto y la carga para que acepten un alcance explicito:

```javascript
React.useEffect(()=>{cargarPermisos(alcancePermisos);},[mes,anio,area,alcancePermisos]);
function cargarPermisos(alcance=alcancePermisos){
  setLoading(true);
  const qs=new URLSearchParams();
  if(alcance==='actual'){
    qs.set('mes',String(mes));
    qs.set('anio',String(anio));
  }else if(alcance==='anterior'){
    const anterior=periodoAnterior(mes,anio);
    qs.set('mes',String(anterior.mes));
    qs.set('anio',String(anterior.anio));
  }
  if(area!==AREA_TODAS)qs.set('area',area);
  const sufijo=qs.toString()?`?${qs.toString()}`:'';
  api(`/api/bonos-produccion/permisos/${sufijo}`)
    .then(d=>{setEmpleados(d.empleados||[]);setPermisos(d.permisos||[]);setLoading(false);})
    .catch(err=>{showStatus(err.message,'err');setLoading(false);});
}
```

- [ ] **Step 3: Add visible controls and active context**

Calcular el titulo y renderizar el control solo para `operadorSolicitudes`:

```javascript
const anterior=periodoAnterior(mes,anio);
const alcanceLabel=alcancePermisos==='historial'
  ?'Todo el historial'
  :alcancePermisos==='anterior'
    ?`${MESES[anterior.mes]} ${anterior.anio}`
    :`${MESES[Number(mes)]} ${anio}`;
```

```javascript
operadorSolicitudes&&h('div',{className:'segmented permission-history-filter'},
  [['actual','Este mes'],['anterior','Mes anterior'],['historial','Todo el historial']]
    .map(([valor,etiqueta])=>h('button',{
      key:valor,
      type:'button',
      className:`segment ${alcancePermisos===valor?'on':''}`,
      onClick:()=>setAlcancePermisos(valor),
    },etiqueta))
)
```

El encabezado limitado debe mostrar `Permisos registrados · ${alcanceLabel}`; la vista administrativa conserva `Permisos de mi equipo`.

- [ ] **Step 4: Return to current month after creating a permission**

En el `then` de `submitPermiso`, ejecutar:

```javascript
setAlcancePermisos('actual');
cargarPermisos('actual');
```

Conservar el toast, la vista carta y la limpieza del motivo existentes.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run:

```bash
python manage.py test \
  bonos_produccion.tests_solicitudes_operadoras.OperadoraCatalogoPermisosTests.test_lista_operadora_sin_periodo_devuelve_todo_su_historial \
  bonos_produccion.tests_solicitudes_operadoras.OperadoraPwaTests.test_pwa_operadora_contiene_solo_las_cuatro_secciones_aprobadas -v 2
```

Expected: 2 tests, OK.

### Task 3: Renovar la caché PWA y cubrir la regresion

**Files:**
- Modify: `bonos_produccion/static/bonos_produccion/sw.js`
- Modify: `bonos_produccion/tests.py`
- Test: `bonos_produccion/tests.py`

- [ ] **Step 1: Update the expected cache version first**

Cambiar la expectativa a:

```python
self.assertIn("pollyanas-bonos-produccion-pwa-v25-historial-permisos", sw_content)
```

- [ ] **Step 2: Run the service-worker test and verify RED**

Run:

```bash
python manage.py test bonos_produccion.tests.BonosProduccionTests.test_pwa_manifest_y_service_worker -v 2
```

Expected: FAIL porque el service worker todavia sirve v24.

- [ ] **Step 3: Bump the production cache**

Modificar `bonos_produccion/static/bonos_produccion/sw.js`:

```javascript
const CACHE_NAME = "pollyanas-bonos-produccion-pwa-v25-historial-permisos";
```

- [ ] **Step 4: Run the service-worker test and verify GREEN**

Run the same test. Expected: 1 test, OK.

- [ ] **Step 5: Commit the implementation**

```bash
git add bonos_produccion/templates/bonos_produccion/index.html \
  bonos_produccion/tests_solicitudes_operadoras.py \
  bonos_produccion/static/bonos_produccion/sw.js \
  bonos_produccion/tests.py
git commit -m "feat: agrega historial de permisos para operadoras"
```

### Task 4: Validar regresion y experiencia local

**Files:**
- Verify: `bonos_produccion/templates/bonos_produccion/index.html`
- Verify: `bonos_produccion/tests_solicitudes_operadoras.py`
- Verify: `bonos_produccion/static/bonos_produccion/sw.js`
- Verify: `bonos_produccion/tests.py`

- [ ] **Step 1: Run automated verification**

```bash
python manage.py test bonos_produccion.tests_solicitudes_operadoras -v 1
python manage.py test bonos_produccion bonos_ventas rrhh --keepdb --parallel -v 1
python manage.py makemigrations --check --dry-run
python manage.py migrate --check
python manage.py check
git diff --check
```

Expected: all tests OK, no model changes, no pending migrations, no Django issues and no whitespace errors.

- [ ] **Step 2: Seed isolated browser fixtures**

Crear en la base PostgreSQL aislada una operadora ficticia y cuatro permisos: mes actual, mes anterior, historico y captura ajena. No usar datos ni credenciales productivas.

- [ ] **Step 3: Validate with Playwright**

Abrir la PWA local como la operadora ficticia y comprobar:

- `Este mes` abre seleccionado y muestra solo el registro actual.
- `Mes anterior` muestra solo el registro anterior.
- `Todo el historial` muestra los tres registros permitidos y excluye la captura ajena.
- No existen botones `Autorizar`, `Rechazar`, `Editar` o `Eliminar`.
- Consola sin errores y las tres consultas GET responden 200.

### Task 5: Integrar, desplegar y verificar produccion

**Files:**
- Verify: all files changed since `origin/main`

- [ ] **Step 1: Review branch hygiene and push**

```bash
git fetch origin main
git status --short --branch
git log --oneline --decorate -5
git worktree list
git diff origin/main..HEAD --stat
git push -u origin codex/permisos-operadoras-historial-completo
```

- [ ] **Step 2: Create a draft PR and wait for all checks**

El PR debe explicar los tres alcances, el contrato sin cambios de API/modelos, las pruebas y la validacion en navegador. Marcarlo listo y mergear solo con CI e higiene verdes.

- [ ] **Step 3: Create and validate a fresh production backup**

Ejecutar `/opt/pastelerias-erp/scripts/backup_db.sh`, identificar el archivo nuevo y comprobarlo con `gzip -t`. No desplegar si el respaldo sigue creciendo o falla integridad.

- [ ] **Step 4: Deploy only through the safe script**

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

No ejecutar `git pull` manual antes.

- [ ] **Step 5: Validate production read-only**

Comprobar commit, servicios, checks y caché v25. Con una sesion efimera de Rosa, verificar en navegador los conteos del mes actual, mes anterior e historial completo, ausencia de acciones administrativas y consola limpia. Eliminar la sesion temporal y no crear solicitudes reales.

- [ ] **Step 6: Close the registered task**

Eliminar solo el contenedor y volumen PostgreSQL de esta tarea. Cerrar `permisos-operadoras-historial-completo` mediante `scripts/task_workspace_close.sh --state merged` y auditar el registro antes y despues.
