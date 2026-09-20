# Expediente documental IMSS por empleado Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guardar cédulas SUA y evidencia EMA/EBA como expedientes auditables con costo patronal por empleado, y materializar presupuesto y rentabilidad sin duplicar importes.

**Architecture:** `reportes` será dueño de tres modelos normalizados: expediente, documento y detalle por trabajador. El parser seguirá siendo puro y separado de la persistencia; un servicio transaccional validará totales, guardará evidencia y materializará las líneas `AUTO:SIPARE`. Los consumidores leerán el expediente como fuente canónica y mantendrán compatibilidad con líneas históricas durante la regularización.

**Tech Stack:** Django 5, PostgreSQL 16, `xlrd`, almacenamiento privado bajo `MEDIA_ROOT`, plantillas Django y pruebas `django.test.TestCase`.

---

## Mapa de archivos

- `reportes/models.py`: modelos `ExpedienteCedulaIMSS`, `DocumentoCedulaIMSS` y `DetalleCedulaIMSS`.
- `reportes/migrations/0049_expediente_cedula_imss.py`: esquema, índices y restricciones; sin escritura histórica.
- `reportes/services_cedula_imss.py`: parsing puro y DTOs enriquecidos; no hace persistencia de archivos.
- `reportes/services_cedula_expediente.py`: validación, persistencia idempotente y materialización presupuestal.
- `reportes/views_presupuesto_real.py`: previsualización, aplicación y consulta del expediente.
- `reportes/templates/reportes/cedula_imss_importar.html`: carga múltiple y resumen seguro.
- `reportes/templates/reportes/cedula_imss_detalle.html`: resumen del expediente sin NSS completo.
- `reportes/urls.py`: ruta de detalle.
- `core/private_operational_media.py`: autorización de archivos `reportes/cedulas-imss/`.
- `reportes/services_rentabilidad_personal.py`: lectura por sucursal desde materialización enlazada.
- `reportes/services_planeacion_personal.py`: control corporativo desde expediente.
- `reportes/management/commands/regularizar_expedientes_cedulas_imss.py`: backfill idempotente con modo seco predeterminado.
- `reportes/tests_cedula_expediente.py`: parser, persistencia, seguridad, UI y regularización.
- `reportes/tests_rentabilidad_personal.py` y `reportes/tests_planeacion_personal.py`: consumidores.
- `docs/ux/action-context-coverage.md`: cobertura de las acciones Previsualizar y Aplicar.

### Task 1: Preparar PostgreSQL aislado y fijar el baseline

**Files:** ninguno.

- [ ] **Step 1: Verificar preflight y elegir puerto libre**

```bash
bash scripts/git_workspace_preflight.sh --write
python - <<'PY'
import socket
s = socket.socket(); s.bind(('127.0.0.1', 0)); print(s.getsockname()[1]); s.close()
PY
```

Expected: preflight `OK` y un puerto numérico libre.

- [ ] **Step 2: Levantar PostgreSQL exclusivo**

```bash
export COMPOSE_PROJECT_NAME=erp_cedulas_expediente
export DB_HOST_PORT="$(python - <<'PY'
import socket
s = socket.socket(); s.bind(('127.0.0.1', 0)); print(s.getsockname()[1]); s.close()
PY
)"
docker compose up -d db
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:${DB_HOST_PORT}/pastelerias_erp"
docker compose exec -T db pg_isready -U postgres
python manage.py migrate
python manage.py migrate --check
python manage.py check
```

Expected: PostgreSQL acepta conexiones, cero migraciones pendientes y cero errores de Django.

- [ ] **Step 3: Ejecutar pruebas de referencia**

```bash
python manage.py test reportes.tests_presupuesto_real.CedulaImssTests reportes.tests_presupuesto_real.CedulaImssEndurecidaTests reportes.tests_presupuesto_real.PantallaCedulaImssTests reportes.tests_rentabilidad_personal reportes.tests_planeacion_personal
```

Expected: suite existente en verde antes de escribir código.

### Task 2: Crear el esquema del expediente

**Files:**
- Modify: `reportes/models.py`
- Create: `reportes/migrations/0049_expediente_cedula_imss.py`
- Create: `reportes/tests_cedula_expediente.py`

- [ ] **Step 1: Escribir la prueba de restricciones**

```python
class ExpedienteCedulaModelTests(TestCase):
    def test_sha_documento_es_unico_y_revision_aplicada_no_se_duplica(self):
        expediente = ExpedienteCedulaIMSS.objects.create(
            tipo="MENSUAL", periodo=date(2026, 8, 1),
            registro_patronal="E5240157100", razon_social="GRUPO EMPRESARIAL FONSMA SA DE CV",
            estado="APLICADO", total_patronal=Decimal("79931.51"),
        )
        DocumentoCedulaIMSS.objects.create(
            expediente=expediente, clase="SUA_XLS", nombre_original="agosto.xls",
            archivo=SimpleUploadedFile("agosto.xls", b"xls"), sha256="a" * 64,
            tamano=3, mime_type="application/vnd.ms-excel",
        )
        with self.assertRaises(IntegrityError):
            DocumentoCedulaIMSS.objects.create(
                expediente=expediente, clase="SUA_XLS", nombre_original="copia.xls",
                archivo=SimpleUploadedFile("copia.xls", b"xls"), sha256="a" * 64,
                tamano=3, mime_type="application/vnd.ms-excel",
            )
```

- [ ] **Step 2: Ejecutar la prueba y observar RED**

```bash
python manage.py test reportes.tests_cedula_expediente.ExpedienteCedulaModelTests
```

Expected: falla porque los modelos no existen.

- [ ] **Step 3: Implementar modelos mínimos**

```python
class ExpedienteCedulaIMSS(models.Model):
    tipo = models.CharField(max_length=12, choices=(("MENSUAL", "Mensual"), ("BIMESTRAL", "Bimestral")))
    periodo = models.DateField(db_index=True)
    registro_patronal = models.CharField(max_length=20, db_index=True)
    razon_social = models.CharField(max_length=200, blank=True, default="")
    revision = models.PositiveSmallIntegerField(default=1)
    estado = models.CharField(max_length=16, choices=(("VALIDO", "Válido"), ("APLICADO", "Aplicado"), ("DISCREPANCIA", "Discrepancia")))
    total_patronal = models.DecimalField(max_digits=14, decimal_places=2)
    trabajadores = models.PositiveIntegerField(default=0)
    cruzados = models.PositiveIntegerField(default=0)
    sin_cruce = models.PositiveIntegerField(default=0)
    aplicado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.PROTECT)
    creado_en = models.DateTimeField(default=timezone.now)
    aplicado_en = models.DateTimeField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["tipo", "periodo", "registro_patronal", "revision"], name="uniq_cedula_imss_revision")]
        indexes = [models.Index(fields=["registro_patronal", "periodo"], name="cedula_imss_reg_period_idx")]

class DocumentoCedulaIMSS(models.Model):
    expediente = models.ForeignKey(ExpedienteCedulaIMSS, on_delete=models.PROTECT, related_name="documentos")
    clase = models.CharField(max_length=12, choices=(("SUA_XLS", "SUA XLS"), ("EMA_PDF", "EMA PDF"), ("EBA_PDF", "EBA PDF")))
    nombre_original = models.CharField(max_length=255)
    archivo = models.FileField(upload_to="reportes/cedulas-imss/%Y/%m/")
    sha256 = models.CharField(max_length=64, unique=True)
    tamano = models.PositiveBigIntegerField()
    mime_type = models.CharField(max_length=100)
    total_visible = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

class DetalleCedulaIMSS(models.Model):
    expediente = models.ForeignKey(ExpedienteCedulaIMSS, on_delete=models.PROTECT, related_name="detalles")
    empleado = models.ForeignKey("rrhh.Empleado", null=True, blank=True, on_delete=models.PROTECT)
    nss = models.CharField(max_length=11, db_index=True)
    nombre_origen = models.CharField(max_length=200)
    dias = models.DecimalField(max_digits=6, decimal_places=2, default=0)
    sdi = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    retiro = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    cesantia_patronal = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    aportacion_vivienda = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    cuota_patronal = models.DecimalField(max_digits=14, decimal_places=2)
    area_codigo = models.CharField(max_length=50, blank=True, default="")
    sucursal = models.ForeignKey("core.Sucursal", null=True, blank=True, on_delete=models.PROTECT)
    cruce_estado = models.CharField(max_length=16, choices=(("CRUZADO", "Cruzado"), ("SIN_CRUCE", "Sin cruce")))

    class Meta:
        constraints = [models.UniqueConstraint(fields=["expediente", "nss"], name="uniq_cedula_imss_nss")]
        indexes = [models.Index(fields=["empleado", "expediente"], name="cedula_imss_emp_exp_idx")]
```

- [ ] **Step 4: Generar y revisar migración**

```bash
python manage.py makemigrations reportes --name expediente_cedula_imss
python manage.py sqlmigrate reportes 0049
python manage.py migrate
python manage.py migrate --check
```

Expected: solo crea tres tablas, índices y restricciones; no altera filas existentes.

- [ ] **Step 5: Ejecutar prueba GREEN y confirmar**

```bash
python manage.py test reportes.tests_cedula_expediente.ExpedienteCedulaModelTests
git add reportes/models.py reportes/migrations/0049_expediente_cedula_imss.py reportes/tests_cedula_expediente.py
git commit -m "feat(reportes): agrega expediente normalizado de cédulas IMSS"
```

Expected: prueba en verde y commit limitado al esquema.

### Task 3: Endurecer el parser y conservar componentes por empleado

**Files:**
- Modify: `reportes/services_cedula_imss.py`
- Modify: `reportes/tests_cedula_expediente.py`

- [ ] **Step 1: Agregar pruebas con los dos diseños observados**

```python
class ParserCedulaTests(SimpleTestCase):
    def test_registro_embebido_y_movimientos_se_suman_por_nss(self):
        filas = matriz_mensual_con_registro_embebido_y_dos_movimientos()
        cedula = parsear_cedula(filas)
        self.assertEqual(cedula.registro_patronal, "E52-40157-10-0")
        self.assertEqual(cedula.trabajadores[0].patronal, Decimal("1255.60"))

    def test_bimestral_separa_componentes_patronales(self):
        cedula = parsear_cedula(matriz_bimestral_realista())
        trabajador = cedula.trabajadores[0]
        self.assertEqual(trabajador.retiro, Decimal("410.99"))
        self.assertEqual(trabajador.cesantia_patronal, Decimal("1238.30"))
        self.assertEqual(trabajador.aportacion_vivienda, Decimal("1027.46"))
        self.assertEqual(trabajador.patronal, Decimal("2676.75"))
```

- [ ] **Step 2: Ejecutar RED**

```bash
python manage.py test reportes.tests_cedula_expediente.ParserCedulaTests
```

Expected: el registro embebido queda vacío o faltan componentes.

- [ ] **Step 3: Implementar DTO y extracción por bloques**

```python
@dataclass
class TrabajadorCedula:
    nss: str
    nombre: str
    patronal: Decimal
    dias: Decimal = Decimal("0")
    sdi: Decimal = Decimal("0")
    retiro: Decimal = Decimal("0")
    cesantia_patronal: Decimal = Decimal("0")
    aportacion_vivienda: Decimal = Decimal("0")

def _registro_patronal(fila, idx, texto):
    embebido = texto.split(":", 1)[1].strip() if ":" in texto else ""
    siguiente = str(fila[idx + 1] if idx + 1 < len(fila) else "").strip()
    return siguiente or embebido

def _bloques_trabajador(filas):
    actual = None
    for fila in filas:
        candidato = _nss_digits(fila[0]) if fila and NSS_RE.match(str(fila[0] or "").strip()) else ""
        if candidato:
            if actual:
                yield actual
            actual = {"nss": candidato, "cabecera": fila, "movimientos": []}
        elif actual:
            actual["movimientos"].append(fila)
    if actual:
        yield actual
```

En `parsear_cedula`, resolver los índices desde `_columnas_montos`, recorrer cada
bloque producido por `_bloques_trabajador`, acumular con `_decimal` las columnas
`retiro`, `cv_patronal` y `aportacion` para el bimestral, y usar `patronal` para
el mensual. Las columnas obreras, `Créd. Vivienda` y `Amortización` no entran en
`TrabajadorCedula.patronal`.

- [ ] **Step 4: Ejecutar GREEN y regresiones del parser**

```bash
python manage.py test reportes.tests_cedula_expediente.ParserCedulaTests reportes.tests_presupuesto_real.CedulaImssTests reportes.tests_presupuesto_real.CedulaImssEndurecidaTests
git add reportes/services_cedula_imss.py reportes/tests_cedula_expediente.py
git commit -m "fix(reportes): parsea registro y movimientos completos del SUA"
```

Expected: pruebas nuevas y existentes en verde.

### Task 4: Persistir expedientes de forma idempotente

**Files:**
- Create: `reportes/services_cedula_expediente.py`
- Modify: `reportes/tests_cedula_expediente.py`

- [ ] **Step 1: Escribir pruebas de conciliación, duplicado y PDF**

```python
class PersistenciaExpedienteTests(TestCase):
    def test_aplica_una_vez_y_pdf_no_incrementa_total(self):
        preview = preparar_expediente([self.sua, self.ema], usuario=self.user)
        expediente = aplicar_expediente(preview, usuario=self.user)
        self.assertEqual(expediente.total_patronal, Decimal("79931.51"))
        self.assertEqual(expediente.documentos.count(), 2)
        self.assertEqual(expediente.detalles.aggregate(t=Sum("cuota_patronal"))["t"], Decimal("79931.51"))
        repetido = aplicar_expediente(preparar_expediente([self.sua], usuario=self.user), usuario=self.user)
        self.assertEqual(repetido.pk, expediente.pk)

    def test_total_discordante_no_materializa(self):
        preview = preparar_expediente([self.sua_con_total_alterado], usuario=self.user)
        with self.assertRaises(CedulaDiscrepante):
            aplicar_expediente(preview, usuario=self.user)
        self.assertFalse(LineaPresupuestoMensual.objects.filter(fuente_real="AUTO:SIPARE").exists())
```

- [ ] **Step 2: Ejecutar RED**

```bash
python manage.py test reportes.tests_cedula_expediente.PersistenciaExpedienteTests
```

Expected: falla porque el servicio no existe.

- [ ] **Step 3: Implementar frontera transaccional**

```python
@transaction.atomic
def aplicar_expediente(preview: PreviewExpediente, *, usuario):
    existente = DocumentoCedulaIMSS.objects.filter(sha256=preview.sua.sha256).select_related("expediente").first()
    if existente:
        return existente.expediente
    if preview.total_detalle != preview.total_patronal:
        raise CedulaDiscrepante(f"Detalle {preview.total_detalle} != control {preview.total_patronal}")
    expediente = ExpedienteCedulaIMSS.objects.create(**preview.campos_expediente(usuario))
    DocumentoCedulaIMSS.objects.bulk_create(preview.documentos(expediente))
    DetalleCedulaIMSS.objects.bulk_create(preview.detalles(expediente))
    materializar_presupuesto(expediente)
    expediente.estado = "APLICADO"
    expediente.aplicado_en = timezone.now()
    expediente.save(update_fields=["estado", "aplicado_en"])
    log_event(usuario, "CEDULA_IMSS_APLICADA", "reportes.ExpedienteCedulaIMSS", expediente.pk, preview.audit_payload())
    return expediente
```

El servicio debe guardar archivos mediante `FileField.save()`, no con rutas manuales; si la transacción falla debe eliminar los blobs recién creados en el manejador de error.

- [ ] **Step 4: Ejecutar GREEN**

```bash
python manage.py test reportes.tests_cedula_expediente.PersistenciaExpedienteTests
git add reportes/services_cedula_expediente.py reportes/tests_cedula_expediente.py
git commit -m "feat(reportes): persiste cédulas IMSS idempotentes y auditables"
```

Expected: persistencia, rollback e idempotencia en verde.

### Task 5: Integrar pantalla, detalle y descarga privada

**Files:**
- Modify: `reportes/views_presupuesto_real.py`
- Modify: `reportes/urls.py`
- Modify: `reportes/templates/reportes/cedula_imss_importar.html`
- Create: `reportes/templates/reportes/cedula_imss_detalle.html`
- Modify: `core/private_operational_media.py`
- Modify: `reportes/tests_cedula_expediente.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Escribir pruebas de permisos y flujo visible**

```python
class PantallaExpedienteTests(TestCase):
    def test_preview_no_guarda_y_aplicar_redirige_al_expediente(self):
        self.client.force_login(self.user)
        preview = self.client.post(reverse("reportes:cedula_imss_importar"), self.archivos(previsualizar="1"))
        self.assertContains(preview, "Total conciliado")
        self.assertFalse(ExpedienteCedulaIMSS.objects.exists())
        applied = self.client.post(reverse("reportes:cedula_imss_importar"), self.archivos(aplicar="1"))
        expediente = ExpedienteCedulaIMSS.objects.get()
        self.assertRedirects(applied, reverse("reportes:cedula_imss_detalle", args=[expediente.pk]))

    def test_descarga_requiere_permiso_y_listado_enmascara_nss(self):
        response = self.client.get(self.documento.archivo.url)
        self.assertEqual(response.status_code, 404)
        self.client.force_login(self.user)
        detalle = self.client.get(reverse("reportes:cedula_imss_detalle", args=[self.expediente.pk]))
        self.assertContains(detalle, "•••• 5894")
        self.assertNotContains(detalle, "17190445894")
```

- [ ] **Step 2: Ejecutar RED**

```bash
python manage.py test reportes.tests_cedula_expediente.PantallaExpedienteTests
```

Expected: no existen ruta, plantilla ni autorización.

- [ ] **Step 3: Implementar POST único y rutas**

```python
if request.method == "POST":
    archivos = request.FILES.getlist("documentos")
    preview = preparar_expediente(archivos, usuario=request.user)
    if "aplicar" in request.POST:
        expediente = aplicar_expediente(preview, usuario=request.user)
        messages.success(request, "Expediente aplicado y conciliado.")
        return redirect("reportes:cedula_imss_detalle", pk=expediente.pk)
```

La aplicación usará `data-async-action`, bloqueará únicamente el botón pulsado, conservará el contexto y emitirá toast global. El detalle filtrará por `_puede_subir_cedulas`; la descarga privada aceptará `reportes/cedulas-imss/` solo cuando el archivo pertenezca a un expediente visible para el usuario.

- [ ] **Step 4: Ejecutar GREEN y validar HTML**

```bash
python manage.py test reportes.tests_cedula_expediente.PantallaExpedienteTests
python manage.py check
git add reportes/views_presupuesto_real.py reportes/urls.py reportes/templates/reportes/cedula_imss_importar.html reportes/templates/reportes/cedula_imss_detalle.html core/private_operational_media.py reportes/tests_cedula_expediente.py docs/ux/action-context-coverage.md
git commit -m "feat(reportes): incorpora expediente IMSS a la pantalla de carga"
```

Expected: permisos, preview, aplicación y enmascaramiento en verde.

### Task 6: Conectar rentabilidad y planeación al expediente

**Files:**
- Modify: `reportes/services_rentabilidad_personal.py`
- Modify: `reportes/services_planeacion_personal.py`
- Modify: `reportes/tests_rentabilidad_personal.py`
- Modify: `reportes/tests_planeacion_personal.py`

- [ ] **Step 1: Escribir pruebas de consumidores**

```python
def test_rentabilidad_lee_materializacion_enlazada_sin_doble_conteo(self):
    expediente = self.crear_expediente_aplicado()
    linea = self.crear_linea_sipare(expediente=expediente, area="gastos-venta", monto="1200.00")
    resultado = leer_personal_mensual(date(2026, 8, 1))
    cargas = [f for f in resultado["filas"] if f["familia"] == "cargas_patronales"]
    self.assertEqual(sum(f["monto"] for f in cargas), Decimal("1200.00"))

def test_planeacion_usa_expediente_aunque_metadata_legada_no_tenga_registro(self):
    self.crear_expediente_aplicado(registro_patronal="E5240157100", total="79931.51")
    agosto = next(r for r in build_personnel_plan(date(2026, 8, 31))["months"] if r["month"] == date(2026, 8, 1))
    self.assertEqual(agosto["imss"], Decimal("79931.51"))
```

- [ ] **Step 2: Ejecutar RED**

```bash
python manage.py test reportes.tests_rentabilidad_personal reportes.tests_planeacion_personal
```

Expected: consumidores todavía dependen únicamente de metadata legada.

- [ ] **Step 3: Implementar lectura canónica con compatibilidad temporal**

```python
expedientes = ExpedienteCedulaIMSS.objects.filter(
    estado="APLICADO", periodo__gte=start, periodo__lt=end,
    registro_patronal=REGISTRO,
).prefetch_related("documentos")
```

Planeación toma una sola vez el total corporativo por expediente. Rentabilidad conserva la distribución de `LineaPresupuestoMensual`, pero exige `metadata["expediente_cedula_imss_id"]` o, durante el backfill, la trazabilidad legada verificada.

- [ ] **Step 4: Ejecutar GREEN**

```bash
python manage.py test reportes.tests_rentabilidad_personal reportes.tests_planeacion_personal
git add reportes/services_rentabilidad_personal.py reportes/services_planeacion_personal.py reportes/tests_rentabilidad_personal.py reportes/tests_planeacion_personal.py
git commit -m "feat(reportes): usa expedientes IMSS en rentabilidad y planeación"
```

Expected: consumidores en verde sin doble conteo.

### Task 7: Crear regularización histórica idempotente

**Files:**
- Create: `reportes/management/commands/regularizar_expedientes_cedulas_imss.py`
- Modify: `reportes/tests_cedula_expediente.py`

- [ ] **Step 1: Escribir prueba de modo seco y backfill**

```python
class RegularizacionCedulasTests(TestCase):
    def test_dry_run_no_escribe_y_apply_enlaza_sin_cambiar_montos(self):
        linea = self.crear_linea_historica(monto="82049.89", archivo=self.xls_enero)
        call_command("regularizar_expedientes_cedulas_imss", "--root", self.tmpdir, stdout=StringIO())
        self.assertFalse(ExpedienteCedulaIMSS.objects.exists())
        call_command("regularizar_expedientes_cedulas_imss", "--root", self.tmpdir, "--apply", stdout=StringIO())
        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal("82049.89"))
        self.assertIsNotNone(linea.metadata["expediente_cedula_imss_id"])
```

- [ ] **Step 2: Ejecutar RED**

```bash
python manage.py test reportes.tests_cedula_expediente.RegularizacionCedulasTests
```

Expected: comando inexistente.

- [ ] **Step 3: Implementar comando seguro**

```python
parser.add_argument("--root", required=True)
parser.add_argument("--apply", action="store_true")
parser.add_argument("--registro", default="E52-40157-10-0")
```

El comando enumera archivos explícitamente bajo `--root`, verifica SHA y total, imprime por periodo `archivo`, `total_documento`, `monto_existente` y `accion`, y solo llama al servicio transaccional con `--apply`. Repetirlo produce cero expedientes nuevos.

- [ ] **Step 4: Ejecutar GREEN y prueba de idempotencia**

```bash
python manage.py test reportes.tests_cedula_expediente.RegularizacionCedulasTests
git add reportes/management/commands/regularizar_expedientes_cedulas_imss.py reportes/tests_cedula_expediente.py
git commit -m "feat(reportes): regulariza expedientes históricos sin alterar importes"
```

Expected: modo seco sin escrituras, aplicación consistente y segunda ejecución sin cambios.

### Task 8: Verificación local completa y preparación del PR

**Files:** todos los anteriores.

- [ ] **Step 1: Verificar migraciones y checks**

```bash
python manage.py makemigrations --check
python manage.py migrate --check
python manage.py showmigrations reportes
python manage.py check
```

Expected: sin migraciones generables o pendientes y cero errores.

- [ ] **Step 2: Ejecutar suites afectadas**

```bash
python manage.py test reportes.tests_cedula_expediente reportes.tests_presupuesto_real.CedulaImssTests reportes.tests_presupuesto_real.CedulaImssEndurecidaTests reportes.tests_presupuesto_real.PantallaCedulaImssTests reportes.tests_rentabilidad_personal reportes.tests_planeacion_personal
```

Expected: cero fallos.

- [ ] **Step 3: Previsualizar los cuatro archivos reales en local**

```bash
python manage.py runserver 127.0.0.1:8019
```

En navegador autenticado, subir los dos `.xls` y los PDF EMA/EBA. Confirmar consola sin errores, POST correcto, registro `E52-40157-10-0`, mensual agosto, bimestre julio-agosto, total patronal conciliado, documentos reconocidos y cero escrituras durante preview.

- [ ] **Step 4: Revisar el diff y commit final**

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git diff --check
git log --oneline --decorate -8
git worktree list
```

Expected: solo archivos del expediente IMSS, árbol limpio y commits quirúrgicos.

- [ ] **Step 5: Abrir PR borrador y esperar CI**

```bash
git push -u origin codex/reportes-cedulas-imss-expediente
gh pr create --draft --base main --head codex/reportes-cedulas-imss-expediente --title "Expediente auditable para cédulas IMSS" --body "$(printf '%s\n' '## Resumen' '- Conserva SUA/EMA/EBA como expediente auditable.' '- Guarda costo patronal por empleado y materializa presupuesto sin duplicados.' '## Pruebas' '- Django checks, migraciones, suites de cédulas, rentabilidad y planeación.' '## Validación' '- Flujo local validado con los cuatro documentos de agosto.' '## Producción' '- Backfill y aplicación productiva pendientes hasta después del merge.')"
gh pr checks --watch
```

El cuerpo incluirá resumen funcional, archivos principales, migración, pruebas, validación en navegador y dejará explícito que aún no se ha aplicado el backfill en producción.

### Task 9: Merge, despliegue, regularización y validación productiva

**Files:** no hay cambios de código adicionales salvo correcciones encontradas durante revisión.

- [ ] **Step 1: Mergear únicamente con CI verde**

```bash
gh pr ready "$(gh pr view --json number --jq .number)"
gh pr merge "$(gh pr view --json number --jq .number)" --squash --delete-branch
```

Expected: commit del PR presente en `origin/main`.

- [ ] **Step 2: Desplegar por el script oficial**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

Expected: migración aplicada, collectstatic correcto y servicios saludables. No ejecutar `git pull` manual antes.

- [ ] **Step 3: Ejecutar regularización en modo seco y capturar evidencia**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 'cd /opt/pastelerias-erp && docker compose exec -T web python manage.py regularizar_expedientes_cedulas_imss --root storage/uploads/cedulas_imss/IMSS-FONSMA-20260907-01'
```

Expected: enero-julio concilian contra sus importes actuales; cualquier diferencia detiene la aplicación y se reporta.

- [ ] **Step 4: Aplicar backfill y agosto después de la conciliación seca**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 'cd /opt/pastelerias-erp && docker compose exec -T web python manage.py regularizar_expedientes_cedulas_imss --root storage/uploads/cedulas_imss/IMSS-FONSMA-20260907-01 --apply'
```

Luego cargar los cuatro documentos de agosto desde la pantalla, previsualizar y aplicar. Registrar conteos e importes antes y después; confirmar que no cambian enero-julio y que julio-agosto reciben el bimestral una sola vez.

- [ ] **Step 5: Verificar consumidores y cerrar workspace**

En producción confirmar visualmente el expediente, Presupuesto vs Real, Rentabilidad y Planeación de personal. Verificar descarga autorizada, rechazo anónimo, consola y solicitudes de red. Después:

```bash
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
bash scripts/task_workspace_close.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 --task cedulas_imss_expediente --state merged
```

Expected: tarea cerrada, worktree retirado y ramas exactas limpiadas por el flujo oficial.
