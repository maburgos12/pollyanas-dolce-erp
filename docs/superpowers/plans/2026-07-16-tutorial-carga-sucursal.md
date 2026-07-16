# Tutorial único de carga por sucursal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mostrar una sola vez por cuenta un tutorial móvil de cinco pasos sobre la carga por sucursal, sin interrumpir rutas activas.

**Architecture:** `Repartidor` conserva la confirmación durable y un servicio puro decide elegibilidad usando la fecha de lanzamiento, la fecha de alta del usuario y la existencia de una ruta `EN_RUTA`. El perfil PWA expone la bandera y un endpoint idempotente registra `Entendido, comenzar`; la plantilla presenta una hoja inferior accesible y reutiliza la cola offline existente.

**Tech Stack:** Django 5, Django REST Framework, PostgreSQL, JavaScript embebido en la PWA, CSS responsive, Service Worker.

---

### Task 1: Persistencia y regla de elegibilidad

**Files:**
- Modify: `logistica/models.py`
- Create: `logistica/services_tutorial_carga.py`
- Create: `logistica/migrations/0040_repartidor_tutorial_carga_sucursal_visto_en.py`
- Create: `logistica/tests_tutorial_carga.py`

- [ ] **Step 1: Escribir pruebas fallidas del servicio**

```python
class TutorialCargaElegibilidadTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="TUT", nombre="Tutorial", activa=True)
        self.user = User.objects.create_user(username="tutorial.chofer")
        User.objects.filter(pk=self.user.pk).update(
            date_joined=datetime(2026, 7, 15, 12, 0, tzinfo=ZoneInfo("America/Mazatlan"))
        )
        self.user.refresh_from_db()
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal)

    def test_existente_sin_confirmar_y_sin_ruta_activa_debe_verlo(self):
        self.assertTrue(debe_mostrar_tutorial_carga(self.repartidor))

    def test_ruta_en_curso_no_se_interrumpe(self):
        RutaEntrega.objects.create(
            nombre="Ruta activa",
            fecha_ruta=date(2026, 7, 16),
            repartidor=self.repartidor,
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        self.assertFalse(debe_mostrar_tutorial_carga(self.repartidor))

    def test_confirmado_no_lo_ve(self):
        self.repartidor.tutorial_carga_sucursal_visto_en = timezone.now()
        self.repartidor.save(update_fields=["tutorial_carga_sucursal_visto_en"])
        self.assertFalse(debe_mostrar_tutorial_carga(self.repartidor))

    def test_creado_despues_del_lanzamiento_no_lo_ve(self):
        User.objects.filter(pk=self.user.pk).update(
            date_joined=datetime(2026, 7, 17, 12, 0, tzinfo=ZoneInfo("America/Mazatlan"))
        )
        self.user.refresh_from_db()
        self.assertFalse(debe_mostrar_tutorial_carga(self.repartidor))
```

- [ ] **Step 2: Ejecutar la prueba y confirmar fallo**

Run: `python manage.py test logistica.tests_tutorial_carga.TutorialCargaElegibilidadTests -v 2`
Expected: FAIL porque el campo y el servicio no existen.

- [ ] **Step 3: Implementar el campo y el servicio mínimo**

```python
TUTORIAL_CARGA_LANZAMIENTO = datetime(2026, 7, 16, tzinfo=ZoneInfo("America/Mazatlan"))

def debe_mostrar_tutorial_carga(repartidor):
    return (
        repartidor.user.date_joined <= TUTORIAL_CARGA_LANZAMIENTO
        and repartidor.tutorial_carga_sucursal_visto_en is None
        and not RutaEntrega.objects.filter(repartidor=repartidor, estatus=RutaEntrega.ESTATUS_EN_RUTA).exists()
    )
```

- [ ] **Step 4: Crear y revisar la migración**

Run: `python manage.py makemigrations logistica --name repartidor_tutorial_carga_sucursal_visto_en`
Expected: una migración que agrega únicamente el `DateTimeField(null=True, blank=True)`.

- [ ] **Step 5: Ejecutar pruebas y checks**

Run: `python manage.py test logistica.tests_tutorial_carga.TutorialCargaElegibilidadTests -v 2 && python manage.py migrate --check`
Expected: PASS y cero migraciones sin registrar.

### Task 2: Contrato API idempotente

**Files:**
- Modify: `api/logistica_views.py`
- Modify: `api/urls.py`
- Modify: `logistica/tests_tutorial_carga.py`

- [ ] **Step 1: Escribir pruebas fallidas del perfil y confirmación**

```python
def test_perfil_expone_bandera(self):
    response = self.client.get(reverse("api_logistica_mi_perfil"))
    self.assertTrue(response.json()["mostrar_tutorial_carga_sucursal"])

def test_confirmar_es_idempotente(self):
    url = reverse("api_logistica_tutorial_carga_confirmar")
    self.assertEqual(self.client.post(url).status_code, 200)
    primera = Repartidor.objects.get(pk=self.repartidor.pk).tutorial_carga_sucursal_visto_en
    self.assertEqual(self.client.post(url).status_code, 200)
    self.assertEqual(Repartidor.objects.get(pk=self.repartidor.pk).tutorial_carga_sucursal_visto_en, primera)
```

- [ ] **Step 2: Ejecutar pruebas y confirmar 404/campo ausente**

Run: `python manage.py test logistica.tests_tutorial_carga.TutorialCargaApiTests -v 2`
Expected: FAIL.

- [ ] **Step 3: Implementar bandera y endpoint**

```python
class LogisticaTutorialCargaConfirmarView(_LogisticaBaseView):
    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "Perfil de repartidor requerido."}, status=403)
        if repartidor.tutorial_carga_sucursal_visto_en is None:
            repartidor.tutorial_carga_sucursal_visto_en = timezone.now()
            repartidor.save(update_fields=["tutorial_carga_sucursal_visto_en"])
        return Response({"confirmado": True, "visto_en": repartidor.tutorial_carga_sucursal_visto_en})
```

- [ ] **Step 4: Ejecutar pruebas API**

Run: `python manage.py test logistica.tests_tutorial_carga.TutorialCargaApiTests -v 2`
Expected: PASS.

### Task 3: Hoja inferior de cinco pasos

**Files:**
- Modify: `logistica/templates/logistica/pwa.html`
- Modify: `logistica/tests_tutorial_carga.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Agregar prueba de contrato HTML fallida**

```python
def test_pwa_declara_tutorial_accesible_y_confirmacion_unica(self):
    html = self.client.get(reverse("logistica:pwa_app")).content.decode()
    self.assertIn('aria-labelledby="tutorial-carga-title"', html)
    self.assertIn("Entendido, comenzar", html)
    self.assertIn("mostrar_tutorial_carga_sucursal", html)
    self.assertIn("prefers-reduced-motion", html)
```

- [ ] **Step 2: Ejecutar y confirmar fallo**

Run: `python manage.py test logistica.tests_tutorial_carga.TutorialCargaPwaTests -v 2`
Expected: FAIL.

- [ ] **Step 3: Implementar estado, render y navegación**

El estado debe incluir `tutorialCarga: { abierto, paso, guardando }`. `loadPerfil()` abre la hoja solo cuando la bandera es verdadera; `abrirPantallaInicial()` no la abre si la respuesta de ruta indica `EN_RUTA`. La hoja contiene cinco pasos, foco atrapado, Escape como `Ahora no`, gesto horizontal y botones Atrás/Siguiente.

- [ ] **Step 4: Implementar confirmación y reintento offline**

`confirmarTutorialCarga()` bloquea únicamente el botón final, hace POST a `/tutorial-carga-sucursal/confirmar/`, cierra de forma optimista y usa la cola de mutaciones existente cuando no hay red. Cerrar o `Ahora no` no hace POST.

- [ ] **Step 5: Registrar cobertura de acción**

Agregar `Tutorial carga por sucursal / Entendido, comenzar` a `docs/ux/action-context-coverage.md` con estado cubierto, bloqueo del botón y respuesta accesible.

- [ ] **Step 6: Ejecutar pruebas PWA**

Run: `python manage.py test logistica.tests_tutorial_carga.TutorialCargaPwaTests -v 2`
Expected: PASS.

### Task 4: Caché, regresión y validación móvil

**Files:**
- Modify: `logistica/static/logistica/pwa/sw.js`
- Modify: `logistica/templates/logistica/pwa.html`

- [ ] **Step 1: Incrementar la versión de caché**

Cambiar el identificador del Service Worker de `v68` a `v69-tutorial-carga-sucursal` tanto en `CACHE_NAME` como en el registro de la PWA.

- [ ] **Step 2: Ejecutar la suite enfocada**

Run: `python manage.py test logistica.tests_tutorial_carga logistica.tests_contexto_operativo logistica.tests_carga_sucursal -v 2`
Expected: PASS.

- [ ] **Step 3: Ejecutar checks globales**

Run: `python manage.py check && python manage.py migrate --check`
Expected: cero errores y cero migraciones pendientes.

- [ ] **Step 4: Validar en navegador real**

Probar 360, 390 y 430 px: aparición tras login, cinco pasos, gesto, foco, Escape, `Ahora no`, confirmación, segundo login sin tutorial, ruta activa sin interrupción, consola sin errores y XHR 200.

- [ ] **Step 5: Revisar el diff y confirmar únicamente archivos relacionados**

Run: `git status --short --branch && git diff --check && git diff origin/main..HEAD --stat`
Expected: solo modelo, migración, servicio, API, PWA, SW, pruebas y documentación de este tutorial.
