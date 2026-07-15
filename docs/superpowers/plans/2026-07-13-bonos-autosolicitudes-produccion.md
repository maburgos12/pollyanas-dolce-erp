# Autosolicitudes en Bonos de Producción Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Permitir que el empleado activo vinculado a la sesión aparezca como “Yo” y cree permisos y horas extra propios desde Bonos de Producción, manteniendo la autorización en su jefa directa y sin cambiar Bonos de Ventas ni los flujos de gestión existentes.

**Architecture:** Se agregan hooks compatibles a los ViewSets compartidos para separar solicitar de gestionar y para ejecutar efectos posteriores a la creación. Producción compone al empleado propio dentro de sus querysets, lo marca en el payload y niega acciones de gestión sobre registros propios; la UI consume banderas explícitas y el service worker cambia de versión.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL, React 18 embebido en template Django, PWA Service Worker, Django TestCase.

---

## Mapa de archivos

- Modify: `rrhh/bonos_permisos.py` — hook compatible para autorizar la creación sobre un empleado ya incluido en el alcance.
- Modify: `rrhh/bonos_horas_extra.py` — separación entre solicitar y gestionar, y hook posterior a creación.
- Modify: `bonos_produccion/views.py` — identidad propia, composición y orden del selector, marcas de payload, bloqueo de gestión propia y notificación de horas extra.
- Modify: `bonos_produccion/templates/bonos_produccion/index.html` — etiqueta visible “Yo” basada en `es_usuario_actual`.
- Modify: `bonos_produccion/static/bonos_produccion/sw.js` — bump obligatorio de caché PWA.
- Modify: `bonos_produccion/tests.py` — contratos TDD de autosolicitud, seguridad, jerarquía y UI.
- Modify: `bonos_ventas/tests.py` — prueba focalizada de compatibilidad del hook compartido.

No se crearán migraciones ni se modificarán modelos, roles, nómina, vacaciones o préstamos.

## Task 1: Fijar por pruebas el contrato de autosolicitud de Producción

**Files:**
- Modify: `bonos_produccion/tests.py`

- [ ] **Step 1: Agregar un fixture focalizado de usuario propio, jefa y empleado ajeno**

Dentro de `BonosProduccionTests`, agregar un helper que cree identidad y jerarquía sin depender del roster de bonos:

```python
def crear_contexto_autosolicitud(self):
    grupo, _ = Group.objects.get_or_create(name=ROLE_PRODUCCION)
    user = get_user_model().objects.create_user(username="julissa.angulo", password="test12345")
    user.groups.add(grupo)
    carolina_user = get_user_model().objects.create_user(username="carolina.cayetano", password="test12345")
    carolina = Empleado.objects.create(
        nombre="CAYETANO VALENZUELA CAROLINA",
        activo=True,
        area="PRODUCCION",
        departamento="PRODUCCION",
        puesto="Jefa de Producción",
        nivel_organizacional=Empleado.NIVEL_JEFATURA,
        usuario_erp=carolina_user,
    )
    julissa = Empleado.objects.create(
        nombre="ANGULO PARRA JULISSA",
        activo=True,
        area="PRODUCCION",
        departamento="PRODUCCION",
        departamento_origen="PRODUCCION",
        puesto="Encargada de Producción",
        nivel_organizacional=Empleado.NIVEL_SUPERVISION,
        participa_bonos_produccion=False,
        usuario_erp=user,
        jefe_directo=carolina,
    )
    ajeno = Empleado.objects.create(
        nombre="EMPLEADO FUERA DE ALCANCE",
        activo=True,
        area="ADMINISTRACION",
        departamento="ADMINISTRACION",
    )
    return user, carolina_user, julissa, ajeno
```

Agregar a los imports existentes:

```python
from core.access import ROLE_PRODUCCION
```

- [ ] **Step 2: Escribir la prueba fallida del selector propio**

```python
def test_autosolicitud_incluye_usuario_actual_sin_participar_en_bonos(self):
    user, _, julissa, _ = self.crear_contexto_autosolicitud()
    self.client.force_login(user)

    permisos = self.client.get("/api/bonos-produccion/permisos/?area=PRODUCCION")
    horas = self.client.get("/api/bonos-produccion/horas-extra/?area=PRODUCCION")

    self.assertEqual(permisos.status_code, 200)
    self.assertEqual(horas.status_code, 200)
    for response in (permisos, horas):
        propios = [row for row in response.json()["empleados"] if row["id"] == julissa.id]
        self.assertEqual(len(propios), 1)
        self.assertTrue(propios[0]["es_usuario_actual"])
        self.assertTrue(propios[0]["puede_solicitar"])
        self.assertFalse(propios[0]["puede_gestionar"])
        self.assertEqual(response.json()["empleados"][0]["id"], julissa.id)
```

- [ ] **Step 3: Escribir las pruebas fallidas de creación y asignación jerárquica**

```python
@patch("rrhh.bonos_permisos.notificar_permiso_solicitado")
def test_usuario_crea_permiso_propio_y_notifica_a_su_jefa(self, notificar_permiso):
    user, carolina_user, julissa, _ = self.crear_contexto_autosolicitud()
    self.client.force_login(user)

    response = self.client.post(
        "/api/bonos-produccion/permisos/",
        json.dumps(
            {
                "empleado": julissa.id,
                "area": "PRODUCCION",
                "tipo": PermisoSalida.TIPO_PERMISO_HORA,
                "fecha_inicio": "2026-07-14T12:00:00",
                "fecha_fin": "2026-07-14T13:00:00",
                "goce_sueldo": True,
                "motivo": "Cita médica",
            }
        ),
        content_type="application/json",
    )

    self.assertEqual(response.status_code, 201)
    permiso = PermisoSalida.objects.get(pk=response.json()["id"])
    self.assertEqual(permiso.empleado, julissa)
    self.assertEqual(permiso.estado_jefe, PermisoSalida.ESTADO_JEFE_PENDIENTE)
    self.assertFalse(permiso.requiere_direccion)
    notificar_permiso.assert_called_once_with(permiso, actor=user)

@patch("bonos_produccion.views.notificar_hora_extra_solicitada")
def test_usuario_crea_hora_extra_propia_asignada_y_notificada_a_su_jefa(self, notificar_hora):
    user, carolina_user, julissa, _ = self.crear_contexto_autosolicitud()
    self.client.force_login(user)

    response = self.client.post(
        "/api/bonos-produccion/horas-extra/",
        json.dumps(
            {
                "empleado": julissa.id,
                "area": "PRODUCCION",
                "fecha": "2026-07-14",
                "horas": "1.50",
                "notas": "Pedido especial",
            }
        ),
        content_type="application/json",
    )

    self.assertEqual(response.status_code, 201)
    hora_extra = HoraExtra.objects.get(pk=response.json()["id"])
    self.assertEqual(hora_extra.empleado, julissa)
    self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)
    self.assertEqual(hora_extra.jefe_directo, carolina_user)
    notificar_hora.assert_called_once_with(hora_extra, actor=user)
```

- [ ] **Step 4: Escribir las pruebas fallidas de segregación de funciones**

```python
def test_usuario_no_gestiona_ni_autoriza_su_hora_extra(self):
    user, carolina_user, julissa, _ = self.crear_contexto_autosolicitud()
    hora_extra = HoraExtra.objects.create(
        empleado=julissa,
        fecha="2026-07-14",
        horas="1.50",
        notas="Pedido especial",
        jefe_directo=carolina_user,
    )
    self.client.force_login(user)

    listado = self.client.get("/api/bonos-produccion/horas-extra/?area=PRODUCCION")
    payload = next(row for row in listado.json()["horas_extra"] if row["id"] == hora_extra.id)
    self.assertFalse(payload["puede_editar"])
    self.assertFalse(payload["puede_eliminar"])
    self.assertFalse(payload["puede_autorizar"])

    editar = self.client.post(
        f"/api/bonos-produccion/horas-extra/{hora_extra.id}/editar/",
        json.dumps(
            {
                "fecha": "2026-07-15",
                "horas": "2.00",
                "notas": "Intento propio",
                "motivo_cambio": "Intento propio",
            }
        ),
        content_type="application/json",
    )
    eliminar = self.client.post(
        f"/api/bonos-produccion/horas-extra/{hora_extra.id}/eliminar/",
        json.dumps({"motivo_cambio": "Intento propio"}),
        content_type="application/json",
    )
    autorizar = self.client.post(f"/api/bonos-produccion/horas-extra/{hora_extra.id}/autorizar/")

    self.assertEqual(editar.status_code, 403)
    self.assertEqual(eliminar.status_code, 403)
    self.assertEqual(autorizar.status_code, 403)
    hora_extra.refresh_from_db()
    self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)
```

- [ ] **Step 5: Escribir pruebas de identidad inválida y empleado fuera del conjunto permitido**

```python
def test_autosolicitud_no_incluye_empleado_sin_jefa_erp_valida(self):
    user, _, julissa, _ = self.crear_contexto_autosolicitud()
    julissa.jefe_directo.usuario_erp = None
    julissa.jefe_directo.save(update_fields=["usuario_erp"])
    self.client.force_login(user)

    listado = self.client.get("/api/bonos-produccion/horas-extra/?area=PRODUCCION")

    self.assertEqual(listado.status_code, 200)
    self.assertNotIn(julissa.id, [row["id"] for row in listado.json()["empleados"]])

def test_autosolicitud_rechaza_empleado_fuera_del_selector(self):
    user, _, _, ajeno = self.crear_contexto_autosolicitud()
    self.client.force_login(user)

    response = self.client.post(
        "/api/bonos-produccion/horas-extra/",
        json.dumps(
            {
                "empleado": ajeno.id,
                "area": "PRODUCCION",
                "fecha": "2026-07-14",
                "horas": "1.00",
                "notas": "Manipulación",
            }
        ),
        content_type="application/json",
    )

    self.assertEqual(response.status_code, 400)
    self.assertFalse(HoraExtra.objects.filter(empleado=ajeno).exists())
```

- [ ] **Step 6: Ejecutar las pruebas nuevas y confirmar que fallan por el contrato ausente**

Run:

```bash
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp \
python3 manage.py test \
  bonos_produccion.tests.BonosProduccionTests.test_autosolicitud_incluye_usuario_actual_sin_participar_en_bonos \
  bonos_produccion.tests.BonosProduccionTests.test_usuario_crea_permiso_propio_y_notifica_a_su_jefa \
  bonos_produccion.tests.BonosProduccionTests.test_usuario_crea_hora_extra_propia_asignada_y_notificada_a_su_jefa \
  bonos_produccion.tests.BonosProduccionTests.test_usuario_no_gestiona_ni_autoriza_su_hora_extra \
  bonos_produccion.tests.BonosProduccionTests.test_autosolicitud_no_incluye_empleado_sin_jefa_erp_valida \
  bonos_produccion.tests.BonosProduccionTests.test_autosolicitud_rechaza_empleado_fuera_del_selector -v 2
```

Expected: FAIL porque el empleado propio todavía no aparece y los hooks/marcas aún no existen. No aceptar errores de conexión, importación o migración como fallo TDD válido.

- [ ] **Step 7: Commit de las pruebas rojas**

```bash
git add bonos_produccion/tests.py
git commit -m "test(bonos): cubrir autosolicitudes de producción"
```

## Task 2: Separar solicitar de gestionar en los contratos compartidos

**Files:**
- Modify: `rrhh/bonos_permisos.py`
- Modify: `rrhh/bonos_horas_extra.py`
- Test: `bonos_ventas/tests.py`

- [ ] **Step 1: Agregar el hook compatible al ViewSet de permisos**

En `BasePermisosEquipoViewSet`, agregar:

```python
def can_solicitar_empleado(self, empleado: Empleado) -> bool:
    return True
```

Después de resolver `empleado` mediante `self._empleados().get(...)` en
`create`, agregar:

```python
if not self.can_solicitar_empleado(empleado):
    return Response(
        {"empleado": "No tienes permiso para solicitar para este empleado."},
        status=status.HTTP_403_FORBIDDEN,
    )
```

El valor predeterminado preserva el comportamiento actual: pertenecer al
queryset sigue siendo el filtro principal.

- [ ] **Step 2: Agregar hooks compatibles al ViewSet de horas extra**

En `BaseHorasExtraEquipoViewSet`, agregar:

```python
def can_solicitar_empleado(self, empleado: Empleado) -> bool:
    return self.can_gestionar_empleado(empleado)

def after_create(self, hora_extra: HoraExtra) -> None:
    return None
```

En `create`, reemplazar la validación de creación:

```python
if not self.can_solicitar_empleado(empleado):
    return Response(
        {"empleado": "No tienes permiso para registrar horas extra para este empleado."},
        status=status.HTTP_403_FORBIDDEN,
    )
```

Después de `HoraExtra.objects.create(...)`, ejecutar:

```python
self.after_create(hora_extra)
```

No poner notificaciones en el comportamiento predeterminado; así Bonos de
Ventas no cambia silenciosamente.

- [ ] **Step 3: Agregar prueba focalizada de compatibilidad de Ventas**

En `BonosVentasTests`, junto al test existente de horas extra, agregar:

```python
def test_horas_extra_ventas_conserva_permiso_de_creacion_compartido(self):
    user = get_user_model().objects.create_user(username="johana.compat", password="test")
    user.groups.add(Group.objects.get_or_create(name=ROLE_VENTAS)[0])
    self.client.force_login(user)
    jefe = Empleado.objects.create(nombre="Johana Compat", usuario_erp=user, area="VENTAS")
    empleado = Empleado.objects.create(
        nombre="Cajera Compat",
        area="VENTAS",
        participa_bonos_ventas=True,
        jefe_directo=jefe,
    )

    response = self.client.post(
        "/api/bonos-ventas/horas-extra/",
        json.dumps(
            {
                "empleado": empleado.id,
                "fecha": "2026-07-14",
                "horas": "1.00",
                "notas": "Cierre",
            }
        ),
        content_type="application/json",
    )

    self.assertEqual(response.status_code, 201)
    self.assertEqual(HoraExtra.objects.get(pk=response.json()["id"]).empleado, empleado)
```

Usar el nombre real de la constante de rol ya importada en el archivo; no crear
un rol paralelo.

- [ ] **Step 4: Ejecutar las regresiones compartidas**

Run:

```bash
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp \
python3 manage.py test bonos_ventas.tests.BonosVentasTests.test_horas_extra_ventas_conserva_permiso_de_creacion_compartido -v 2
```

Expected: PASS.

- [ ] **Step 5: Commit de los contratos compartidos**

```bash
git add rrhh/bonos_permisos.py rrhh/bonos_horas_extra.py bonos_ventas/tests.py
git commit -m "refactor(rrhh): separar solicitud y gestión en bonos"
```

## Task 3: Implementar la política propia en Producción

**Files:**
- Modify: `bonos_produccion/views.py`
- Test: `bonos_produccion/tests.py`

- [ ] **Step 1: Importar la notificación existente**

Agregar junto a los imports de `core`:

```python
from core.notificaciones import notificar_hora_extra_solicitada
```

- [ ] **Step 2: Resolver la identidad propia válida sin inferencias de nombre**

Dentro de `PermisosProduccionEquipoViewSet`, agregar:

```python
def _empleado_propio(self):
    empleado = _empleado_de_usuario(self.request.user)
    if not empleado or not empleado.activo:
        return None
    departamento = (empleado.departamento or empleado.departamento_origen or "").strip().upper()
    if departamento != AREA_PRODUCCION:
        return None
    jefe = empleado.jefe_directo
    if not jefe or not jefe.activo or not jefe.usuario_erp_id:
        return None
    return empleado

def _es_empleado_propio(self, empleado):
    propio = self._empleado_propio()
    return bool(propio and propio.id == empleado.id)
```

No agregar fallback por nombre, email o username: la relación `usuario_erp` es
el contrato.

- [ ] **Step 3: Agregar el empleado propio a todos los querysets de solicitud**

Agregar:

```python
def _con_empleado_propio(self, qs):
    propio = self._empleado_propio()
    if not propio:
        return qs
    return Empleado.objects.filter(Q(id__in=qs.values_list("id", flat=True)) | Q(id=propio.id), activo=True)
```

En cada retorno de `empleados_queryset`, envolver el queryset final con
`self._con_empleado_propio(...)`. Mantener intactos los filtros de periodo,
área, jerarquía y elegibilidad existentes.

- [ ] **Step 4: Marcar las capacidades en el payload sin cambiar modelos**

Agregar un método común:

```python
def _marcar_capacidades(self, payload, empleado):
    es_usuario_actual = self._es_empleado_propio(empleado)
    payload["es_usuario_actual"] = es_usuario_actual
    payload["puede_solicitar"] = self.can_solicitar_empleado(empleado)
    payload["puede_gestionar"] = False if es_usuario_actual else self.can_gestionar_empleado(empleado)
    return payload
```

Aplicarlo tanto en el `list` especial de permisos como en `empleado_payload` de
horas extra. En el `list` de permisos, insertar primero al empleado propio y
deduplicar por `empleado.id` antes de recorrer jefaturas, bonos y empleados del
queryset.

- [ ] **Step 5: Especializar solicitud y gestión propia**

En `PermisosProduccionEquipoViewSet`, agregar:

```python
def can_solicitar_empleado(self, empleado):
    if self._es_empleado_propio(empleado):
        return True
    return super().can_solicitar_empleado(empleado)
```

En `HorasExtraProduccionEquipoViewSet`, agregar:

```python
def can_solicitar_empleado(self, empleado):
    if self._es_empleado_propio(empleado):
        return True
    return super().can_solicitar_empleado(empleado)

def can_gestionar_empleado(self, empleado):
    if self._es_empleado_propio(empleado):
        return False
    return can_manage_submodule(self.request.user, "produccion", "bonos") or super().can_gestionar_empleado(empleado)

def after_create(self, hora_extra):
    notificar_hora_extra_solicitada(hora_extra, actor=self.request.user)
```

El orden es obligatorio: comprobar identidad propia antes del permiso amplio de
gestión evita que el rol `PRODUCCION` habilite editar o eliminar lo propio.

- [ ] **Step 6: Ejecutar las pruebas rojas y confirmar que pasan**

Run el mismo comando focalizado de Task 1.

Expected: PASS para las seis pruebas.

- [ ] **Step 7: Ejecutar regresión focalizada de flujos existentes**

Run:

```bash
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp \
python3 manage.py test \
  bonos_produccion.tests.BonosProduccionTests.test_permisos_equipo_produccion_crea_y_rechaza \
  bonos_produccion.tests.BonosProduccionTests.test_horas_extra_produccion_crea_y_autoriza_en_rrhh \
  bonos_produccion.tests.BonosProduccionTests.test_permisos_produccion_incluye_equipo_directo_sin_bono_periodo \
  bonos_ventas.tests.BonosVentasTests.test_horas_extra_ventas_crea_y_autoriza_en_rrhh -v 2
```

Expected: PASS sin cambios de assertions existentes.

- [ ] **Step 8: Commit de la política de Producción**

```bash
git add bonos_produccion/views.py bonos_produccion/tests.py
git commit -m "feat(bonos): permitir autosolicitudes de producción"
```

## Task 4: Hacer visible “Yo” y renovar el caché PWA

**Files:**
- Modify: `bonos_produccion/templates/bonos_produccion/index.html`
- Modify: `bonos_produccion/static/bonos_produccion/sw.js`
- Test: `bonos_produccion/tests.py`

- [ ] **Step 1: Escribir la prueba estática fallida de UI y caché**

```python
def test_app_marca_usuario_actual_y_service_worker_cambia_version(self):
    template = Path("bonos_produccion/templates/bonos_produccion/index.html").read_text(encoding="utf-8")
    service_worker = Path("bonos_produccion/static/bonos_produccion/sw.js").read_text(encoding="utf-8")

    self.assertIn("es_usuario_actual", template)
    self.assertIn("Yo —", template)
    self.assertIn('const CACHE_NAME = "pollyanas-bonos-produccion-pwa-v20-autosolicitudes";', service_worker)
```

Agregar `from pathlib import Path` si todavía no existe en el archivo.

- [ ] **Step 2: Ejecutar la prueba y confirmar fallo**

Run:

```bash
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp \
python3 manage.py test bonos_produccion.tests.BonosProduccionTests.test_app_marca_usuario_actual_y_service_worker_cambia_version -v 2
```

Expected: FAIL porque el template no muestra “Yo” y el caché sigue en v19.

- [ ] **Step 3: Renderizar el nombre propio desde la bandera del backend**

En `EmployeeSearchList`, calcular el nombre visible dentro del `map`:

```javascript
filtered.map(b=>{
  const displayName=b.es_usuario_actual?`Yo — ${b.empleado_nombre}`:b.empleado_nombre;
  return h('button',{key:b.id,type:'button',className:`employee-row ${selected?.id===b.id?'on':''}`,onClick:()=>onSelect(b)},
    h('div',{className:'av'},initials(b.empleado_nombre)),
    h('div',{className:'employee-row-main'},
      h('div',{className:'persona-name'},displayName),
      h('div',{className:'persona-sub'},metaText?metaText(b):(AREA_LABELS[b.area]||b.area||'Produccion'))
    ),
    rightText&&h('div',{className:'employee-row-right'},rightText(b))
  );
})
```

No comparar `empleado_nombre` con el usuario ni codificar “Julissa”.

- [ ] **Step 4: Cambiar la versión del service worker**

En `bonos_produccion/static/bonos_produccion/sw.js`:

```javascript
const CACHE_NAME = "pollyanas-bonos-produccion-pwa-v20-autosolicitudes";
```

- [ ] **Step 5: Ejecutar la prueba estática**

Run el comando de Step 2.

Expected: PASS.

- [ ] **Step 6: Commit de UI y caché**

```bash
git add bonos_produccion/templates/bonos_produccion/index.html bonos_produccion/static/bonos_produccion/sw.js bonos_produccion/tests.py
git commit -m "feat(bonos): mostrar autosolicitud propia en la app"
```

## Task 5: Ejecutar la matriz completa y validar en navegador local

**Files:**
- No new files

- [ ] **Step 1: Verificar migraciones y configuración**

```bash
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp \
python3 manage.py migrate --check

APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp \
python3 manage.py check
```

Expected: cero migraciones pendientes y cero errores.

- [ ] **Step 2: Ejecutar suites completas afectadas**

```bash
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp \
python3 manage.py test bonos_produccion.tests bonos_ventas.tests rrhh.tests -v 2
```

Expected: PASS completo. Si una prueba falla, diagnosticar la causa antes de
modificar código; no actualizar assertions para ocultar una regresión.

- [ ] **Step 3: Levantar el servidor desde el worktree limpio**

```bash
APP_ENV=development DEBUG=1 ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55434/pastelerias_erp \
CANONICAL_LOCAL_HOST=localhost:8021 \
python3 manage.py runserver 0.0.0.0:8021
```

Expected: servidor disponible en `http://localhost:8021` sin tocar el servidor
de otro hilo en 8011.

- [ ] **Step 4: Validar el flujo real en Chrome DevTools**

Con usuarios locales equivalentes a Julissa y Carolina:

1. Entrar como Julissa a `/bonos-produccion/app/?captura=1&tab=permisos`.
2. Confirmar “Yo — ANGULO PARRA JULISSA” una sola vez y en primer lugar.
3. Crear un permiso propio y verificar `201` en Network.
4. Repetir en `?captura=1&tab=horas_extra`.
5. Confirmar que no aparecen Editar, Eliminar, Autorizar ni Rechazar en lo propio.
6. Cerrar sesión y entrar como Carolina.
7. Confirmar que ambas solicitudes aparecen y pueden resolverse.
8. Revisar consola sin errores JavaScript.
9. Revisar que XHR no redirija a login ni devuelva HTML.
10. En Application > Service Workers, confirmar el nuevo `CACHE_NAME` después de recargar.

- [ ] **Step 5: Revisar diff final y estado**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
git log --oneline --decorate -8
git worktree list
```

Expected: únicamente archivos declarados, sin capturas, logs, bases locales ni
artefactos temporales.

## Task 6: PR, despliegue seguro y validación productiva

**Files:**
- No new files

- [ ] **Step 1: Confirmar que la rama sigue actualizada y aislada**

```bash
git fetch origin main
git rev-list --left-right --count origin/main...HEAD
git status --short --branch
git diff origin/main..HEAD --stat
```

Expected: worktree limpio y diff exclusivo. Si `origin/main` avanzó, rebasear
antes del PR y repetir todas las pruebas afectadas.

- [ ] **Step 2: Push y PR borrador**

```bash
git push -u origin codex/bonos-autosolicitudes-produccion
gh pr create --draft \
  --base main \
  --head codex/bonos-autosolicitudes-produccion \
  --title "Permitir autosolicitudes seguras en Bonos de Producción" \
  --body-file /tmp/bonos-autosolicitudes-pr.md
```

El cuerpo debe incluir resumen funcional, archivos, pruebas, validación en
navegador, ausencia de migraciones y el bump PWA. Crear `/tmp/bonos-autosolicitudes-pr.md`
fuera del repositorio y no confirmarlo.

- [ ] **Step 3: Revisar CI y convertir a listo solo con todo verde**

```bash
gh pr checks --watch
gh pr ready
```

Expected: todos los checks requeridos exitosos.

- [ ] **Step 4: Mergear sin mezclar tareas**

```bash
gh pr merge --squash --delete-branch
```

Expected: PR mergeado a `main` y rama remota eliminada por GitHub.

- [ ] **Step 5: Auditar el VPS antes de desplegar**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && git status --short --branch && git rev-parse HEAD'
```

Expected: checkout limpio. Si hay cambios o una rama distinta, detener el
deploy y reportar el estado.

- [ ] **Step 6: Desplegar por el canal oficial**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

Expected: actualización desde `main`, migraciones, estáticos y HUP/reinicio
según determine el script, sin `502` sostenido.

- [ ] **Step 7: Verificar checks y service worker en producción**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && docker compose exec -T web python manage.py migrate --check && docker compose exec -T web python manage.py check'

curl -fsS https://erp.pollyanasdolce.com/bonos-produccion/sw.js | \
  grep 'pollyanas-bonos-produccion-pwa-v20-autosolicitudes'
```

Expected: cero migraciones, cero errores y versión nueva del caché visible.

- [ ] **Step 8: Validar con Julissa y Carolina en producción**

Usar navegador real y las cuentas operativas autorizadas:

1. Julissa visualiza “Yo” en Permisos y Horas extra.
2. Crear un registro de prueba solamente si Mauricio confirma que puede
   conservarse o cancelarse como evidencia operativa.
3. Confirmar en base de datos empleado, estado y `jefe_directo`.
4. Entrar como Carolina y confirmar visibilidad y acciones.
5. Resolver o cancelar el registro conforme a la instrucción operativa.
6. Revisar consola, Network/XHR, notificación y caché PWA.

No declarar terminado si solo responde la API o si no pudo verificarse la
bandeja real de Carolina.

- [ ] **Step 9: Limpiar rama y worktree después de validación**

```bash
git -C /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 fetch --prune origin
git -C /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 worktree remove \
  /Users/mauricioburgos/Downloads/codex_worktrees/bonos-autosolicitudes-produccion
git -C /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 branch -D \
  codex/bonos-autosolicitudes-produccion
```

Expected: worktree eliminado, rama local eliminada y referencias remotas
podadas. Ejecutar únicamente después de merge, deploy y validación productiva.
