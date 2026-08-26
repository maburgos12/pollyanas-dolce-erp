# Cierre claro de acuerdos Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mostrar `Listo para cerrar` cuando un acuerdo abierto tiene el checklist completo, conservar el cierre explícito y explicar a Dirección qué acción falta.

**Architecture:** La clasificación seguirá siendo calculada en `seguimiento.views._aplicar_estado_visual_seguimiento`, sin persistir un estado nuevo. Las plantillas consumirán `estado_operativo_label` y `estado_operativo_tone`; la sección técnica seguirá usando el estado real de ERP y Agente DG.

**Tech Stack:** Django 5, Django templates, CSS existente del módulo `seguimiento`, `django.test.TestCase`, PostgreSQL 16.

---

## Estructura de archivos

- `seguimiento/views.py`: calcula la etiqueta y el tono operativos junto con el porcentaje existente.
- `seguimiento/templates/seguimiento/detalle_item.html`: usa el estado operativo en la ficha y explica el cierre pendiente en las vistas de colaborador y DG.
- `seguimiento/templates/seguimiento/panel_dg.html`: usa el estado operativo en ambas listas del Panel de acuerdos.
- `static/css/template_modules/seguimiento-templates-seguimiento-detalle-item.css`: estilo de la insignia `listo_cerrar`.
- `static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css`: estilo de la píldora `listo_cerrar`.
- `seguimiento/tests.py`: cubre clasificación, detalle, Panel DG y conservación del estado técnico.

### Task 1: Clasificación operativa test-first

**Files:**
- Modify: `seguimiento/tests.py:390-530`
- Modify: `seguimiento/views.py:203-229`

- [ ] **Step 1: Escribir pruebas fallidas para los estados operativo y técnico**

Agregar a `SeguimientoColaboradorTests`:

```python
    def test_checklist_completo_abierto_expone_estado_listo_para_cerrar(self):
        self.check.completado = True
        self.check.completado_por = self.user
        self.check.completado_at = timezone.now()
        self.check.save(update_fields=["completado", "completado_por", "completado_at", "updated_at"])
        self.item.metadata = {"source": "agente_dg", "source_status": "OPEN"}
        self.item.save(update_fields=["metadata", "updated_at"])

        response = self.client.get(f"/seguimiento/{self.item.pk}/")

        item = response.context["item"]
        self.assertEqual(getattr(item, "estado_operativo_label", None), "Listo para cerrar")
        self.assertEqual(getattr(item, "estado_operativo_tone", None), "listo_cerrar")
        self.assertEqual(item.get_estatus_display(), "Pendiente")

    def test_estado_operativo_conserva_pendiente_si_faltan_checks(self):
        response = self.client.get(f"/seguimiento/{self.item.pk}/")

        item = response.context["item"]
        self.assertEqual(getattr(item, "estado_operativo_label", None), "Pendiente")
        self.assertEqual(getattr(item, "estado_operativo_tone", None), "pendiente")

    def test_estado_operativo_conserva_completado_si_acuerdo_esta_cerrado(self):
        self.check.completado = True
        self.check.save(update_fields=["completado", "updated_at"])
        self.item.estatus = SeguimientoItem.ESTATUS_COMPLETADO
        self.item.save(update_fields=["estatus", "updated_at"])

        response = self.client.get(f"/seguimiento/{self.item.pk}/")

        item = response.context["item"]
        self.assertEqual(getattr(item, "estado_operativo_label", None), "Completado")
        self.assertEqual(getattr(item, "estado_operativo_tone", None), "completado")
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55476/pastelerias_erp /usr/local/bin/python3 manage.py test \
  seguimiento.tests.SeguimientoColaboradorTests.test_checklist_completo_abierto_expone_estado_listo_para_cerrar \
  seguimiento.tests.SeguimientoColaboradorTests.test_estado_operativo_conserva_pendiente_si_faltan_checks \
  seguimiento.tests.SeguimientoColaboradorTests.test_estado_operativo_conserva_completado_si_acuerdo_esta_cerrado \
  --keepdb
```

Expected: `FAIL` porque `SeguimientoItem` aún no expone `estado_operativo_label` ni `estado_operativo_tone`.

- [ ] **Step 3: Implementar la clasificación mínima**

En `_aplicar_estado_visual_seguimiento`, después de calcular `progreso_pct` y antes de `visual_bucket`, agregar:

```python
    checklist_completo = bool(item.checklist_total) and item.checklist_done == item.checklist_total
    if item.esta_cerrado or item.estatus == SeguimientoItem.ESTATUS_EN_REVISION:
        item.estado_operativo_label = item.get_estatus_display()
        item.estado_operativo_tone = item.estatus.lower()
    elif checklist_completo:
        item.estado_operativo_label = "Listo para cerrar"
        item.estado_operativo_tone = "listo_cerrar"
    else:
        item.estado_operativo_label = item.get_estatus_display()
        item.estado_operativo_tone = item.estatus.lower()
```

- [ ] **Step 4: Ejecutar las pruebas y comprobar GREEN**

Repetir el comando del Step 2.

Expected: `Ran 3 tests ... OK`.

- [ ] **Step 5: Confirmar el cambio de clasificación**

```bash
git add seguimiento/tests.py seguimiento/views.py
git commit -m "fix(seguimiento): distingue checklist listo de acuerdo pendiente"
```

### Task 2: Mensajes y estados visibles test-first

**Files:**
- Modify: `seguimiento/tests.py:390-530`
- Modify: `seguimiento/templates/seguimiento/detalle_item.html:20-330`
- Modify: `seguimiento/templates/seguimiento/panel_dg.html:70-105`
- Modify: `static/css/template_modules/seguimiento-templates-seguimiento-detalle-item.css:386-398`
- Modify: `static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css:248-253`

- [ ] **Step 1: Escribir pruebas fallidas de colaborador, DG y Panel de acuerdos**

Agregar a `SeguimientoColaboradorTests`:

```python
    def test_colaborador_con_checklist_completo_ve_cierre_como_siguiente_accion(self):
        self.check.completado = True
        self.check.save(update_fields=["completado", "updated_at"])

        response = self.client.get(f"/seguimiento/{self.item.pk}/")

        self.assertContains(response, 'class="seg-badge-status listo_cerrar">Listo para cerrar</span>')
        self.assertContains(response, "Todos los puntos están completos. Solo falta confirmar el cierre.")
        self.assertContains(response, ">Cerrar acuerdo</button>")
        self.assertContains(response, "<dt>Estado ERP</dt><dd>Pendiente</dd>", html=True)

    def test_dg_ve_que_colaborador_termino_y_falta_cierre(self):
        self.check.completado = True
        self.check.save(update_fields=["completado", "updated_at"])
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.cierre", password="test12345")
        dg_user.groups.add(dg_group)
        self.client.force_login(dg_user)

        response = self.client.get(f"/seguimiento/panel/{self.item.pk}/")

        self.assertContains(response, 'class="seg-badge-status listo_cerrar">Listo para cerrar</span>')
        self.assertContains(
            response,
            "El colaborador terminó todos los puntos. Falta que confirme el cierre del acuerdo.",
        )
        self.assertNotContains(response, ">Cerrar acuerdo</button>")

    def test_panel_dg_muestra_listo_para_cerrar_sin_ocultar_estado_tecnico(self):
        self.check.completado = True
        self.check.save(update_fields=["completado", "updated_at"])
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        dg_user = get_user_model().objects.create_user(username="mauricio.panel.cierre", password="test12345")
        dg_user.groups.add(dg_group)
        self.client.force_login(dg_user)

        response = self.client.get("/seguimiento/panel/?estado=activos")

        self.assertContains(response, 'class="bi-pill pill-listo_cerrar">Listo para cerrar</span>')
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55476/pastelerias_erp /usr/local/bin/python3 manage.py test \
  seguimiento.tests.SeguimientoColaboradorTests.test_colaborador_con_checklist_completo_ve_cierre_como_siguiente_accion \
  seguimiento.tests.SeguimientoColaboradorTests.test_dg_ve_que_colaborador_termino_y_falta_cierre \
  seguimiento.tests.SeguimientoColaboradorTests.test_panel_dg_muestra_listo_para_cerrar_sin_ocultar_estado_tecnico \
  --keepdb
```

Expected: `FAIL` porque las plantillas aún renderizan `Pendiente` y la vista DG muestra `Consulta del acuerdo`.

- [ ] **Step 3: Usar el estado operativo en el detalle**

Reemplazar las dos insignias operativas del detalle por:

```django
<span class="seg-badge-status {{ item.estado_operativo_tone }}">{{ item.estado_operativo_label }}</span>
```

En el encabezado de `Conversación y cierre`, usar:

```django
<strong>{% if item.esta_cerrado %}Cierre establecido{% elif item.estatus == estatus_en_revision %}En revisión{% elif checklist_completo %}Cierre pendiente del responsable{% else %}Seguimiento abierto{% endif %}</strong>
```

Después de la rama `item.estatus == estatus_en_revision` del panel derecho, agregar:

```django
{% elif checklist_completo and es_vista_dg %}
  <span class="seg-next-label">Estado actual</span>
  <h2 id="seg-next-title">Listo para cerrar</h2>
  <p>El colaborador terminó todos los puntos. Falta que confirme el cierre del acuerdo.</p>
```

Conservar sin cambios la rama del colaborador que muestra `Cerrar el acuerdo`, el formulario `seguimiento:completar` y toda la sección `Detalles técnicos`.

- [ ] **Step 4: Usar el estado operativo en ambas listas de DG**

Reemplazar las dos píldoras de `panel_dg.html` por:

```django
<span class="bi-pill pill-{{ item.estado_operativo_tone }}">{{ item.estado_operativo_label }}</span>
```

- [ ] **Step 5: Añadir tonos visuales y actualizar versiones estáticas**

Agregar en el CSS del detalle:

```css
.seg-badge-status.listo_cerrar { background: rgba(47,108,119,.14); color: #2f6c77; }
```

Agregar en el CSS del Panel DG:

```css
.pill-listo_cerrar { background: var(--pd-info-soft); color: var(--pd-info); }
```

Actualizar los query strings de ambos CSS a `20260825-cierre-claro-v1` para evitar servir la versión anterior desde caché HTTP.

- [ ] **Step 6: Ejecutar las pruebas y comprobar GREEN**

Repetir el comando del Step 2.

Expected: `Ran 3 tests ... OK`.

- [ ] **Step 7: Ejecutar la clase completa de seguimiento**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55476/pastelerias_erp /usr/local/bin/python3 manage.py test seguimiento.tests.SeguimientoColaboradorTests --keepdb
```

Expected: todas las pruebas de la clase pasan sin errores.

- [ ] **Step 8: Confirmar la UI y sus pruebas**

```bash
git add seguimiento/tests.py seguimiento/templates/seguimiento/detalle_item.html seguimiento/templates/seguimiento/panel_dg.html static/css/template_modules/seguimiento-templates-seguimiento-detalle-item.css static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css
git commit -m "fix(seguimiento): aclara cierre pendiente tras completar checklist"
```

### Task 3: Validación integral y navegador

**Files:**
- Verify: `seguimiento/views.py`
- Verify: `seguimiento/templates/seguimiento/detalle_item.html`
- Verify: `seguimiento/templates/seguimiento/panel_dg.html`
- Verify: `static/css/template_modules/seguimiento-templates-seguimiento-detalle-item.css`
- Verify: `static/css/template_modules/seguimiento-templates-seguimiento-panel-dg.css`
- Verify: `seguimiento/tests.py`

- [ ] **Step 1: Ejecutar validaciones Django y diff**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55476/pastelerias_erp /usr/local/bin/python3 manage.py migrate --check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55476/pastelerias_erp /usr/local/bin/python3 manage.py check
git diff --check origin/main...HEAD
git status --short --branch
```

Expected: sin migraciones pendientes, `System check identified no issues`, diff limpio y solo archivos del alcance.

- [ ] **Step 2: Ejecutar el servidor local con PostgreSQL aislado**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55476/pastelerias_erp /usr/local/bin/python3 manage.py runserver 127.0.0.1:8016
```

Expected: servidor accesible en `http://127.0.0.1:8016`.

- [ ] **Step 3: Validar en navegador los dos consumidores**

Crear datos locales equivalentes al caso productivo: acuerdo `PENDIENTE`, fuente `OPEN`, un check completo y responsable enlazado. Verificar:

- colaborador: insignia `Listo para cerrar`, explicación y botón `Cerrar acuerdo`;
- Dirección: insignia `Listo para cerrar`, explicación del cierre faltante y ausencia de controles de escritura;
- Panel de acuerdos: píldora `Listo para cerrar`;
- Detalles técnicos: `Estado app OPEN` y `Estado ERP Pendiente`;
- consola sin errores JavaScript y recursos CSS con versión `20260825-cierre-claro-v1`.

- [ ] **Step 4: Revisar el diff final y confirmar cualquier ajuste de validación**

```bash
git status --short --branch
git log --oneline --decorate -5
git worktree list
git diff origin/main..HEAD --stat
git diff --check origin/main...HEAD
```

Si la validación de navegador exige un ajuste, escribir primero una prueba fallida, aplicar el cambio mínimo y confirmar únicamente esos archivos.

### Task 4: PR, despliegue y comprobación productiva

**Files:**
- Deploy: `scripts/deploy_web_safe.sh` sin modificaciones.

- [ ] **Step 1: Subir la rama y crear PR draft**

```bash
git push -u origin codex/seguimiento-cierre-claro
gh pr create --draft --base main --head codex/seguimiento-cierre-claro \
  --title "fix(seguimiento): aclara cierre pendiente tras checklist" \
  --body "Resumen funcional: muestra Listo para cerrar al completar el checklist sin ocultar el estado técnico. Archivos principales: views, detalle, Panel DG, CSS y pruebas de seguimiento. Pruebas: clase SeguimientoColaboradorTests, check y migrate --check. Validación: navegador local en vistas de colaborador y Dirección."
```

Expected: rama remota creada y PR draft con un solo objetivo.

- [ ] **Step 2: Revisar CI y diff del PR**

```bash
gh pr checks --watch
gh pr diff
```

Expected: CI verde y ningún archivo fuera del alcance.

- [ ] **Step 3: Marcar listo y mergear solamente con revisión limpia**

```bash
gh pr ready
gh pr merge --squash --delete-branch
```

Expected: PR mergeado a `main` sin hallazgos sustantivos.

- [ ] **Step 4: Desplegar sin `git pull` manual**

```bash
ssh -i /Users/mauricioburgos/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

Expected: despliegue seguro completo, `collectstatic` aplicado y Gunicorn recargado/reiniciado según el diff detectado por el script.

- [ ] **Step 5: Verificar el caso real en producción**

Abrir la minuta 576 en el Panel de acuerdos y comprobar:

- ficha y Panel DG muestran `Listo para cerrar`;
- el avance sigue `1 de 1 · 100%`;
- el mensaje indica que Carolina debe confirmar el cierre;
- Detalles técnicos siguen mostrando `OPEN` y `Pendiente`;
- no hay errores en consola o Network.

No cerrar ni modificar la minuta productiva durante esta validación.

- [ ] **Step 6: Cerrar el ciclo de vida del worktree**

```bash
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
bash scripts/task_workspace_close.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 --task seguimiento_cierre_claro --state merged
```

Expected: worktree y ramas exactas eliminados después de corroborar que el commit está en `origin/main`, seguido de `fetch --prune`.
