# Reembolsos con cargos adicionales de marketplace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Permitir reembolsos mayores al precio del producto cuando existan cargos adicionales comprobados, sin mezclar intentos ni abonos.

**Architecture:** IntentoCompraDepartamental conservará reembolso_solicitado como total y añadirá reembolso_cargos_adicionales. La parte del producto será total menos cargos; formulario y servicio compartirán la regla y la línea de tiempo mostrará el desglose.

**Tech Stack:** Django 5.0.1, PostgreSQL 16, templates Django, django.test y Docker.

---

## Mapa de archivos

- compras/models.py y migración 0017: persistencia e invariantes.
- compras/forms_intentos_compra.py: validación inmediata.
- compras/services_intentos_compra.py: autoridad transaccional.
- compras/services_edicion_compra.py: protección al corregir el precio.
- compras/views_departamentales.py y templates de acción/detalle: desglose visible.
- compras/tests_intentos_compra.py y compras/tests_edicion_compra.py: regresión.

Compras no tiene service worker propio; el global usa red directa para estas rutas y no requiere cambio de caché.

### Task 1: Modelo y migración

**Files:**
- Modify: compras/models.py:493-609
- Create: compras/migrations/0017_intentocompradepartamental_reembolso_cargos_adicionales.py
- Test: compras/tests_intentos_compra.py

- [ ] **Step 1: Escribir la prueba fallida**

~~~python
def test_cargos_adicionales_se_validan_y_no_admiten_update_masivo(self):
    intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
    self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("0.00"))
    intento.reembolso_solicitado = Decimal("318.56")
    intento.reembolso_cargos_adicionales = Decimal("119.00")
    intento.save(update_fields=["reembolso_solicitado", "reembolso_cargos_adicionales"])
    intento.reembolso_cargos_adicionales = Decimal("318.57")
    with self.assertRaisesMessage(ValidationError, "no pueden superar el total"):
        intento.save(update_fields=["reembolso_cargos_adicionales"])
    with self.assertRaises(ValidationError):
        IntentoCompraDepartamental.objects.filter(pk=intento.pk).update(
            reembolso_cargos_adicionales=Decimal("1.00"),
        )
~~~

- [ ] **Step 2: Ejecutar y comprobar que falla porque el campo no existe**

~~~bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py test compras.tests_intentos_compra.IntentoCompraModelTests
~~~

- [ ] **Step 3: Implementar campo e invariantes**

Añadir el nombre a IntentoCompraQuerySet.CAMPOS_REEMBOLSO y el campo:

~~~python
reembolso_cargos_adicionales = models.DecimalField(
    max_digits=14, decimal_places=2, default=Decimal("0.00"),
)
~~~

Añadir restricciones: cargos no negativos; cargos en cero si total es nulo; cargos menores o iguales al total. En save(), resolver valores efectivos según update_fields y validar:

~~~python
if cargos < 0:
    raise ValidationError({"reembolso_cargos_adicionales": "Los cargos adicionales no pueden ser negativos."})
if solicitado is None and cargos:
    raise ValidationError({"reembolso_cargos_adicionales": "Los cargos adicionales requieren una solicitud de reembolso."})
if solicitado is not None and cargos > solicitado:
    raise ValidationError({"reembolso_cargos_adicionales": "Los cargos adicionales no pueden superar el total solicitado."})
~~~

- [ ] **Step 4: Generar, aplicar y probar la migración aditiva**

~~~bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py makemigrations compras --name intento_reembolso_cargos_adicionales
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py migrate compras
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py test compras.tests_intentos_compra.IntentoCompraModelTests
~~~

Expected: solo 0017, predeterminado 0.00, restricciones presentes y pruebas OK.

- [ ] **Step 5: Commit**

~~~bash
git add compras/models.py compras/migrations/0017_intentocompradepartamental_reembolso_cargos_adicionales.py compras/tests_intentos_compra.py
git commit -m "feat(compras): guardar cargos documentados de reembolso"
~~~

### Task 2: Formulario y servicio

**Files:**
- Modify: compras/forms_intentos_compra.py:13-61
- Modify: compras/services_intentos_compra.py:96-185
- Test: compras/tests_intentos_compra.py

- [ ] **Step 1: Escribir pruebas fallidas de los dos casos reales**

~~~python
def test_cancelacion_pagada_admite_cargos_documentados(self):
    intento = self._intento(pagado=True, importe=Decimal("199.56"))
    evidencia = SimpleUploadedFile("reembolso.pdf", b"%PDF-1.4", content_type="application/pdf")
    self._cancelar(
        intento,
        reembolso_solicitado_en=timezone.localdate(),
        reembolso_solicitado=Decimal("318.56"),
        reembolso_cargos_adicionales=Decimal("119.00"),
        evidencia_solicitud_reembolso=evidencia,
    )
    intento.refresh_from_db()
    self.assertEqual(intento.reembolso_solicitado, Decimal("318.56"))
    self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("119.00"))

def test_cargos_adicionales_exigen_evidencia_sin_mutar(self):
    intento = self._intento(pagado=True, importe=Decimal("199.56"))
    with self.assertRaisesMessage(ValidationError, "evidencia"):
        self._cancelar(
            intento,
            reembolso_solicitado_en=timezone.localdate(),
            reembolso_solicitado=Decimal("318.56"),
            reembolso_cargos_adicionales=Decimal("119.00"),
        )
    intento.refresh_from_db()
    self.assertEqual(intento.estado, IntentoCompraDepartamental.ESTADO_VIGENTE)
~~~

Añadir casos 119.00/0.00, cargos mayores al total, producto mayor a lo pagado y más de dos decimales en servicio y formulario.

- [ ] **Step 2: Ejecutar y confirmar el fallo anterior**

~~~bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py test compras.tests_intentos_compra.CancelacionIntentoCompraServiceTests compras.tests_intentos_compra.FormulariosIntentoCompraTests
~~~

- [ ] **Step 3: Implementar el formulario**

Añadir:

~~~python
self.fields["reembolso_cargos_adicionales"] = forms.DecimalField(
    label="Cargos adicionales documentados",
    min_value=Decimal("0.00"), initial=Decimal("0.00"),
    max_digits=14, decimal_places=2,
    help_text="Envío, ajuste u otro cargo incluido por el proveedor en esta devolución.",
)
~~~

Quitar max_value del total y validar en clean():

~~~python
total = cleaned.get("reembolso_solicitado")
cargos = cleaned.get("reembolso_cargos_adicionales")
evidencia = cleaned.get("evidencia_solicitud_reembolso")
if total is not None and cargos is not None:
    if cargos > total:
        self.add_error("reembolso_cargos_adicionales", "Los cargos adicionales no pueden superar el total solicitado.")
    elif total - cargos > self.compra.importe_final:
        self.add_error("reembolso_solicitado", "La parte del producto no puede superar la compra pagada.")
    if cargos > 0 and not evidencia:
        self.add_error("evidencia_solicitud_reembolso", "Adjunta evidencia cuando el reembolso incluya cargos adicionales.")
~~~

- [ ] **Step 4: Implementar la defensa autoritativa**

Crear _importe_no_negativo(), extender ambas firmas de cancelación y aplicar:

~~~python
cargos = _importe_no_negativo(reembolso_cargos_adicionales or Decimal("0"), nombre="reembolso_cargos_adicionales")
if cargos > importe:
    raise ValidationError({"reembolso_cargos_adicionales": "Los cargos adicionales no pueden superar el total solicitado."})
if importe - cargos > compra.importe_final:
    raise ValidationError({"reembolso_solicitado": "La parte del producto no puede superar la compra pagada."})
if cargos > 0 and not evidencia_solicitud_reembolso:
    raise ValidationError({"evidencia_solicitud_reembolso": "Adjunta evidencia cuando el reembolso incluya cargos adicionales."})
intento.reembolso_cargos_adicionales = cargos
~~~

Persistirlo, prohibirlo para intentos sin compra y detallar producto/cargos/total en el evento.

- [ ] **Step 5: Suite y commit**

~~~bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py test compras.tests_intentos_compra
git add compras/forms_intentos_compra.py compras/services_intentos_compra.py compras/tests_intentos_compra.py
git commit -m "feat(compras): validar desglose de reembolso"
~~~

### Task 3: UI y corrección posterior

**Files:**
- Modify: compras/views_departamentales.py:329-350
- Modify: compras/templates/compras/departamentales/partials/accion_intento_form.html
- Modify: compras/templates/compras/departamentales/detalle.html:65-74
- Modify: compras/services_edicion_compra.py:220-265
- Test: compras/tests_intentos_compra.py
- Test: compras/tests_edicion_compra.py

- [ ] **Step 1: Escribir pruebas fallidas**

Crear dos intentos y verificar textos de total 318.56, producto 199.56, cargos 119.00 y otro total 119.00. En CorreccionCompraRegistradaTests comprobar que 318.56 menos 119.00 permite precio 199.56 y rechaza 199.55.

~~~python
self.assertContains(response, "Total solicitado $318.56")
self.assertContains(response, "Producto $199.56")
self.assertContains(response, "Cargos adicionales $119.00")
self.assertContains(response, "Total solicitado $119.00")
~~~

- [ ] **Step 2: Ejecutar y observar fallos**

~~~bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py test compras.tests_intentos_compra.DetalleIntentosCompraTests compras.tests_edicion_compra.CorreccionCompraRegistradaTests
~~~

- [ ] **Step 3: Preparar el desglose sin consultas nuevas**

~~~python
historico.reembolso_producto_visible = max(
    (historico.reembolso_solicitado or Decimal("0")) - historico.reembolso_cargos_adicionales,
    Decimal("0"),
)
~~~

Mostrar en la pantalla de cancelación el importe pagado y en detalle.html total, producto, cargos, recibido y saldo. Añadir reembolso_producto_visible a la prueba assertNumQueries(0).

- [ ] **Step 4: Corregir la regla de edición**

~~~python
producto_solicitado = (
    (intento.reembolso_solicitado or Decimal("0"))
    - intento.reembolso_cargos_adicionales
)
if (
    intento.estado in (
        IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO,
        IntentoCompraDepartamental.ESTADO_REEMBOLSADO,
    )
    and intento.reembolso_solicitado is not None
    and compra.importe_final < producto_solicitado
):
    raise ValidationError("El importe final no puede ser menor que la parte del producto ya solicitada.")
~~~

- [ ] **Step 5: Probar y commit**

~~~bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py test compras.tests_intentos_compra.DetalleIntentosCompraTests compras.tests_intentos_compra.AccionesIntentoCompraViewTests compras.tests_edicion_compra.CorreccionCompraRegistradaTests
git add compras/views_departamentales.py compras/services_edicion_compra.py compras/templates/compras/departamentales/partials/accion_intento_form.html compras/templates/compras/departamentales/detalle.html compras/tests_intentos_compra.py compras/tests_edicion_compra.py
git commit -m "feat(compras): mostrar desglose auditable de reembolso"
~~~

### Task 4: Validación local integral

**Files:**
- Verify: todos los archivos anteriores.

- [ ] **Step 1: Ejecutar checks y suite**

~~~bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py makemigrations --check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py migrate --check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55692/pastelerias_erp python3 manage.py test compras
~~~

- [ ] **Step 2: Validar en navegador local**

Comprobar 318.56/119.00 con PDF; rechazo sin PDF sin mutación; 119.00/0.00; saldos separados; consola limpia y POST asíncrono correcto.

- [ ] **Step 3: Revisar diff**

~~~bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff origin/main..HEAD --check
git log --oneline --decorate -8
git worktree prune --dry-run
~~~

Expected: solo esta tarea; sin PDFs, capturas, referencias, logs o artefactos.

### Task 5: PR y despliegue

**Files:**
- No new code files.

- [ ] **Step 1: Abrir PR en borrador, revisar CI y fusionar**

~~~bash
git push -u origin codex/compras-reembolso-cargos-marketplace
gh pr create --draft --title "Permitir reembolsos con cargos documentados" --body "Separa producto y cargos adicionales. Conserva intentos y abonos independientes. Validado con migraciones, check, suite Compras y navegador local."
gh pr checks --watch
gh pr ready
gh pr merge --squash --delete-branch
~~~

- [ ] **Step 2: Desplegar sin git pull manual**

~~~bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
~~~

Expected: 0017 aplicada, collectstatic exitoso y servicio saludable.

- [ ] **Step 3: Validar producción antes de mutar**

Confirmar autenticado que intentos 5 y 13 siguen VIGENTE y sin abonos; revisar formulario, consola y Network sin enviar.

### Task 6: Registrar los dos reembolsos

**Files:**
- Evidence input only: /Users/mauricioburgos/Downloads/refund_voucher_40281718195.pdf
- Evidence input only: /Users/mauricioburgos/Downloads/refund_voucher_40280662463.pdf

- [ ] **Step 1: Registrar Brach's**

Intento 5, SCD-2608-0002: Proveedor canceló; solicitud 20/09/2026; total 318.56; cargos 119.00; primer PDF. Después, un abono 318.56 fechado 20/09/2026 con su referencia AmEx solo en producción.

Expected: REEMBOLSADO, saldo 0.00, producto 199.56, cargos 119.00.

- [ ] **Step 2: Registrar Espátula Cuadrada**

Intento 13, SCD-2609-0010: Proveedor canceló; solicitud 17/09/2026; total 119.00; cargos 0.00; segundo PDF. Después, un abono 119.00 con fecha procesada 18/09/2026; en la referencia operativa conservar que AmEx muestra transacción 17/09/2026.

Expected: REEMBOLSADO, saldo 0.00, producto 119.00, cargos 0.00 y sin tocar intento 5.

- [ ] **Step 3: Lectura fresca y cierre**

Confirmar dos solicitudes y un abono por intento; ningún total combinado 437.56; saldos cero; compromisos liberados; históricos visibles; referencias ausentes de Git/logs. Cerrar con task_workspace_close.sh --state merged y detener solo PostgreSQL/red Docker de esta tarea.

## Self-review

- Cubre modelo, formulario, servicio, corrección, UI, pruebas, despliegue y registro.
- Montos Decimal y nombre consistente reembolso_cargos_adicionales.
- Referencias completas solo en producción.
- Migración aditiva con valor 0.00.
