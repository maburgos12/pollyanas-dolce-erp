# Operadoras de solicitudes de Produccion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dar a Julissa Angulo y Rosa Icela Cervantes una superficie acotada para capturar permisos y prestamos propios o del personal elegible de Produccion, manteniendo las autorizaciones en Carolina y Direccion General.

**Architecture:** El grupo existente `bonos_produccion_captura` sera la unica senal del perfil operativo. Un catalogo de empleados compartido aplicara el alcance en servidor; los permisos reutilizaran el flujo vigente y los prestamos usaran un endpoint nuevo de Bonos que delega la creacion a un servicio transaccional de RRHH. La interfaz solo presentara captura y confirmacion, nunca acciones de autorizacion.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL 16, React 18 UMD dentro del template de Bonos de Produccion, Service Worker y `django.test`.

---

## Mapa de archivos

- Crear `bonos_produccion/solicitudes.py`: catalogo elegible y endpoint acotado de prestamos.
- Crear `bonos_produccion/tests_solicitudes_operadoras.py`: contrato de acceso, privacidad, permisos y prestamos.
- Crear `rrhh/tests_prestamos_service.py`: pruebas unitarias del servicio transaccional de alta.
- Modificar `bonos_produccion/views.py`: hacer que permisos use el catalogo acotado y niegue gestion a operadoras.
- Modificar `bonos_produccion/urls.py`: registrar `prestamos` bajo `/api/bonos-produccion/`.
- Modificar `rrhh/services_prestamos.py`: centralizar alta, deuda vigente, calculo, jefatura y notificacion.
- Modificar `rrhh/api_views.py`: reutilizar el servicio sin cambiar permisos ni respuestas del API de RRHH.
- Modificar `bonos_produccion/templates/bonos_produccion/index.html`: agregar pestana de prestamos y limitar pestanas del perfil operativo.
- Modificar `bonos_produccion/static/bonos_produccion/sw.js`: incrementar `CACHE_NAME` por el cambio visible.
- Modificar `docs/ux/action-context-coverage.md`: registrar la nueva accion asincrona.

### Task 1: Catalogo elegible y permisos sin facultades de gestion

**Files:**
- Create: `bonos_produccion/solicitudes.py`
- Create: `bonos_produccion/tests_solicitudes_operadoras.py`
- Modify: `bonos_produccion/views.py`

- [ ] **Step 1: Escribir pruebas rojas del alcance de empleados**

Crear una clase `OperadoraProduccionFixtures(TestCase)` que construya: grupo `bonos_produccion_captura`, operadora vinculada, Carolina con usuario activo, un empleado activo de Produccion con Carolina, uno inactivo, uno de Ventas y Carolina sin jefa. Agregar:

```python
def test_catalogo_solo_incluye_produccion_activa_con_jefa_erp(self):
    ids = set(empleados_operables_solicitudes_produccion().values_list("id", flat=True))
    self.assertEqual(ids, {self.operadora_empleado.id, self.produccion_empleado.id})

def test_operadora_puede_solicitar_pero_nunca_gestionar(self):
    self.client.force_authenticate(self.operadora)
    response = self.client.get(reverse("bonoproduccion-permiso-list"))
    self.assertEqual(response.status_code, 200)
    self.assertTrue(all(item["puede_solicitar"] for item in response.data["empleados"]))
    self.assertTrue(all(not item["puede_gestionar"] for item in response.data["empleados"]))
```

- [ ] **Step 2: Ejecutar las pruebas y confirmar el fallo esperado**

Run:

```bash
$PY manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraCatalogoPermisosTests --verbosity 2
```

Expected: `ImportError` para `empleados_operables_solicitudes_produccion` o asercion fallida porque el queryset actual depende de bonos/equipo.

- [ ] **Step 3: Implementar el catalogo cerrado**

En `bonos_produccion/solicitudes.py`:

```python
def empleados_operables_solicitudes_produccion():
    return Empleado.objects.filter(
        Q(departamento=AREA_PRODUCCION) | Q(departamento_origen=AREA_PRODUCCION),
        activo=True,
        jefe_directo__activo=True,
        jefe_directo__usuario_erp__is_active=True,
    ).select_related("jefe_directo__usuario_erp", "sucursal_ref").distinct()
```

En `PermisosProduccionEquipoViewSet`, si `is_bonos_produccion_capture_only(request.user)` es verdadero, usar exactamente ese catalogo. Hacer que `can_gestionar_empleado()` devuelva `False` antes de evaluar jefatura o permisos administrativos y que `filter_permisos()` limite a `empleado__usuario_erp=request.user` para no revelar historiales ajenos.

- [ ] **Step 4: Probar captura propia, ajena y denegaciones**

Agregar pruebas POST que confirmen `201` para operadora y otro empleado elegible, `400/403` para Ventas o inactivo, y `403` sin cambios para `editar`, `eliminar`, `preautorizar` y `rechazar`. Confirmar `origen_solicitud == "bonos_produccion"` y `estado == solicitado`.

- [ ] **Step 5: Ejecutar pruebas focalizadas**

Run:

```bash
$PY manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraCatalogoPermisosTests --verbosity 2
```

Expected: todas las pruebas pasan y no se modifica ningun permiso preexistente.

- [ ] **Step 6: Commit quirurgico**

```bash
git add bonos_produccion/solicitudes.py bonos_produccion/views.py bonos_produccion/tests_solicitudes_operadoras.py
git commit -m "fix(bonos): acota permisos de operadoras de produccion"
```

### Task 2: Servicio transaccional unico para crear prestamos

**Files:**
- Create: `rrhh/tests_prestamos_service.py`
- Modify: `rrhh/services_prestamos.py`
- Modify: `rrhh/api_views.py`

- [ ] **Step 1: Escribir pruebas rojas del servicio**

Cubrir creacion con estos valores y aserciones:

```python
prestamo = crear_solicitud_prestamo(
    empleado=self.empleado,
    actor=self.operadora,
    concepto="Emergencia familiar",
    metodo_pago=Prestamo.METODO_TRANSFERENCIA,
    importe=Decimal("3000.00"),
    num_quincenas=6,
    fecha_deposito=date(2026, 8, 15),
)
self.assertEqual(prestamo.estado, Prestamo.ESTADO_SOLICITADO)
self.assertEqual(prestamo.descuento_quincenal, Decimal("500.00"))
self.assertEqual(prestamo.saldo_actual, Decimal("3000.00"))
self.assertEqual(prestamo.jefe_directo, self.carolina_user)
self.assertEqual(prestamo.creado_por, self.operadora)
```

Agregar casos de deuda vigente, jefa ausente/inactiva, importe cero y quincenas cero; todos deben rechazar sin crear fila.

- [ ] **Step 2: Ejecutar y confirmar fallo esperado**

Run:

```bash
$PY manage.py test rrhh.tests_prestamos_service.CrearSolicitudPrestamoTests --verbosity 2
```

Expected: `ImportError` porque el servicio aun no existe.

- [ ] **Step 3: Implementar servicio minimo atomico**

En `rrhh/services_prestamos.py`, agregar `@transaction.atomic` y bloquear al empleado con `Empleado.objects.select_for_update().get(pk=empleado.pk)`. Validar importe y plazo positivos, resolver `usuario_jefe_directo_de_empleado`, buscar saldo vigente en estados solicitado/autorizado/aprobado/activo, calcular `importe / num_quincenas` a dos decimales, crear el prestamo en solicitado y llamar una sola vez a `notificar_prestamo_solicitado`.

El servicio debe aceptar solamente:

```python
def crear_solicitud_prestamo(
    *, empleado, actor, concepto, metodo_pago, importe, num_quincenas,
    fecha_deposito=None,
) -> Prestamo:
```

y nunca aceptar `estado`, firmas, saldo, autorizadores o fechas de autorizacion.

- [ ] **Step 4: Refactorizar el API generico sin ampliar acceso**

Reemplazar el cuerpo de `PrestamoViewSet.perform_create` por la misma comprobacion existente de empleado propio/RRHH y una llamada al servicio. Asignar la instancia retornada a `serializer.instance` para conservar la respuesta DRF.

- [ ] **Step 5: Ejecutar pruebas del servicio y regresion del ViewSet**

Run:

```bash
$PY manage.py test rrhh.tests_prestamos_service rrhh.tests --verbosity 1
```

Expected: servicio y suite RRHH pasan; un usuario comun sigue sin poder solicitar para otra persona.

- [ ] **Step 6: Commit quirurgico**

```bash
git add rrhh/services_prestamos.py rrhh/api_views.py rrhh/tests_prestamos_service.py
git commit -m "refactor(rrhh): centraliza alta segura de prestamos"
```

### Task 3: Endpoint acotado de prestamos de Produccion

**Files:**
- Modify: `bonos_produccion/solicitudes.py`
- Modify: `bonos_produccion/urls.py`
- Modify: `bonos_produccion/tests_solicitudes_operadoras.py`

- [ ] **Step 1: Escribir pruebas rojas de API y privacidad**

Agregar pruebas para `reverse("bonoproduccion-prestamo-list")`: GET solo devuelve prestamos `creado_por=operadora` o del empleado propio; POST propio y ajeno elegible crea en solicitado; empleado fuera de alcance da `400/403`; deuda vigente da `400`; enviar `estado`, `jefe_directo`, `firma_jefe` o `autorizado_dg` no cambia los valores de servidor; PATCH/DELETE y rutas de autorizacion dan `405/404`.

- [ ] **Step 2: Ejecutar y confirmar 404 esperado**

Run:

```bash
$PY manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraPrestamosApiTests --verbosity 2
```

Expected: `NoReverseMatch` o `404` porque la ruta aun no existe.

- [ ] **Step 3: Implementar serializer de entrada/salida y ViewSet**

En `bonos_produccion/solicitudes.py`, crear un `PrestamoProduccionSerializer` con campos escribibles `empleado`, `concepto`, `metodo_pago`, `importe`, `num_quincenas`, `fecha_deposito`; declarar folio, estado, descuento, saldo, jefa y creado_por como solo lectura. Crear `PrestamosProduccionViewSet` con `http_method_names = ["get", "post", "head", "options"]`, `IsAuthenticated` y `CanAccessBonosProduccion`.

En `initial()` o `check_permissions()`, exigir `is_bonos_produccion_capture_only(request.user)` (permitiendo superusuario solo para soporte). En `get_queryset()` aplicar:

```python
Q(creado_por=request.user) | Q(empleado__usuario_erp=request.user)
```

En `perform_create()`, resolver `empleado` exclusivamente desde `empleados_operables_solicitudes_produccion()` y llamar `crear_solicitud_prestamo`.

- [ ] **Step 4: Registrar ruta y ejecutar pruebas**

Agregar:

```python
router.register("prestamos", PrestamosProduccionViewSet, basename="bonoproduccion-prestamo")
```

Run:

```bash
$PY manage.py test bonos_produccion.tests_solicitudes_operadoras --verbosity 2
```

Expected: todas las pruebas de operadoras pasan, incluyendo privacidad y metodos denegados.

- [ ] **Step 5: Commit quirurgico**

```bash
git add bonos_produccion/solicitudes.py bonos_produccion/urls.py bonos_produccion/tests_solicitudes_operadoras.py
git commit -m "feat(bonos): agrega captura acotada de prestamos"
```

### Task 4: PWA operativa de permisos y prestamos

**Files:**
- Modify: `bonos_produccion/templates/bonos_produccion/index.html`
- Modify: `bonos_produccion/static/bonos_produccion/sw.js`
- Modify: `bonos_produccion/tests_solicitudes_operadoras.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Leer reglas visuales obligatorias antes de editar**

Run:

```bash
sed -n '1,240p' PRODUCT.md
sed -n '1,260p' DESIGN_STACK.md
```

Usar solo el skill que `DESIGN_STACK.md` enrute para una extension funcional de interfaz existente; conservar los patrones visuales actuales.

- [ ] **Step 2: Escribir prueba roja de superficie operativa**

Agregar una prueba de render con operadora que busque identificadores estables `data-operadora-solicitudes="true"`, `PrestamosTab` y `/api/bonos-produccion/prestamos/`, y otra con usuario administrador que confirme que no se marca como operadora acotada.

- [ ] **Step 3: Ejecutar y confirmar fallo esperado**

Run:

```bash
$PY manage.py test bonos_produccion.tests_solicitudes_operadoras.OperadoraPwaTests --verbosity 2
```

Expected: falta el indicador y el formulario de prestamos.

- [ ] **Step 4: Implementar navegacion y formulario**

Exponer en el contexto un booleano `operador_solicitudes`. Para ese perfil, limitar las pestanas visibles a `permisos` y `prestamos`, usar `permisos` como inicial y no cargar periodos/resumen de bonos. Agregar `PrestamosTab` con selector buscable de empleado, concepto, metodo, importe, quincenas y fecha; POST al endpoint nuevo; bloquear solo el boton `Enviar solicitud`; conservar inputs en error; limpiar tras `201`; mostrar folio y estado con el status accesible existente. No renderizar botones de autorizar, rechazar, editar o eliminar.

- [ ] **Step 5: Incrementar cache y registrar cobertura UX**

Cambiar `CACHE_NAME` de `v21-cancelacion-visible` a `v22-operadoras-solicitudes` en el mismo commit. Agregar a `docs/ux/action-context-coverage.md` la accion “Enviar solicitud de prestamo de Produccion”, indicando async, bloqueo del boton, preservacion en error y confirmacion por folio.

- [ ] **Step 6: Ejecutar pruebas de PWA y Bonos**

Run:

```bash
$PY manage.py test bonos_produccion --verbosity 1
```

Expected: suite verde; el template solo carga endpoints administrativos para usuarios con sus permisos vigentes.

- [ ] **Step 7: Commit quirurgico**

```bash
git add bonos_produccion/templates/bonos_produccion/index.html bonos_produccion/static/bonos_produccion/sw.js bonos_produccion/tests_solicitudes_operadoras.py docs/ux/action-context-coverage.md
git commit -m "feat(bonos): incorpora solicitudes operativas de prestamos"
```

### Task 5: Regresion integral y verificacion local real

**Files:**
- Test only; no production data mutations.

- [ ] **Step 1: Ejecutar suites afectadas y regresion de Ventas**

Run:

```bash
$PY manage.py test bonos_produccion rrhh bonos_ventas --parallel --verbosity 1
```

Expected: `OK`, sin cambios en calculo de bonos, ajustes, permisos historicos ni flujo de autorizacion.

- [ ] **Step 2: Verificar configuracion y migraciones**

Run:

```bash
$PY manage.py check
$PY manage.py migrate --check
$PY manage.py makemigrations --check --dry-run
```

Expected: cero errores, cero migraciones pendientes y `No changes detected`.

- [ ] **Step 3: Validar navegador local con cuatro roles**

Levantar Django contra PostgreSQL aislado y probar en navegador real: operadora ve solo Permisos/Prestamos; captura propia y ajena; manipulacion de Ventas falla; acciones de resolucion no existen y endpoint da `403/405`; Carolina conserva autorizacion; DG conserva aprobacion final. Revisar consola sin errores, XHR correctos y Service Worker con `v22-operadoras-solicitudes`.

- [ ] **Step 4: Revisar diff, estado y contratos compartidos**

Run:

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD -- core rrhh bonos_produccion bonos_ventas docs/ux/action-context-coverage.md
git worktree list
git worktree prune --dry-run
```

Expected: solo archivos del alcance; sin migraciones, datos, secretos ni artefactos temporales.

### Task 6: Entrega, despliegue y alta productiva reversible

**Files:**
- No code changes expected; production mutation only after reviewed merge/deploy.

- [ ] **Step 1: Publicar rama y PR de una sola tarea**

Antes de publicar, verificar estado limpio, historial quirurgico y diff contra `origin/main`. Crear PR en borrador con resumen funcional, archivos, pruebas y validacion local. No mezclar otra tarea.

- [ ] **Step 2: Mergear y desplegar por el canal seguro**

Despues de revision y CI verde, mergear. En VPS ejecutar exclusivamente:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

No ejecutar `git pull` manual. Confirmar migraciones, `collectstatic`, recarga de procesos y version servida del Service Worker.

- [ ] **Step 3: Tomar respaldo logico y vista previa de identidades**

En una transaccion de solo lectura, confirmar por ID/codigo/email: Julissa vinculada solo a `julissa.angulo`; Rosa codigo `348` sin otro usuario; grupos actuales; `UserModuleAccess`; conteos y estados de permisos/prestamos. Mostrar la delta exacta antes de aplicar.

- [ ] **Step 4: Aplicar acceso atomico sin tocar historicos**

Dentro de `transaction.atomic()`: crear `rosa.cervantes` con password temporal generado fuera de codigo/logs, vincularla solo a empleado 348, agregar Rosa y Julissa a `bonos_produccion_captura`, retirar a Julissa de `PRODUCCION`, eliminar solo accesos explicitos que contradigan el perfil si la vista previa los detecta, y registrar cada delta en `AuditLog`. No actualizar ningun campo laboral, bono, permiso o prestamo.

- [ ] **Step 5: Validar produccion y rollback inmediato si falla Julissa**

Autenticar como Julissa primero. Confirmar rutas permitidas, rutas denegadas, catalogo, captura de prueba propia controlada y ausencia de autorizaciones. Si falla, restaurar su grupo `PRODUCCION` desde el respaldo y detener el alta de Rosa. Si pasa, validar Rosa, Carolina y DG; no aprobar solicitudes reales de terceros.

- [ ] **Step 6: Comparar antes/despues y cerrar worktree**

Confirmar mismos conteos/estados/responsables historicos y que solo cambiaron las dos identidades/grupos previstos. Registrar evidencia de UI/API/Service Worker, cerrar la tarea con `scripts/task_workspace_close.sh --state merged` y podar la rama local/remota ya integrada.
