# Jornadas semanales por empleado — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Incorporar jornadas semanales con vigencia e historial en el alta y edición de RRHH, y aplicar de forma auditada los perfiles administrativos aprobados desde el 1 de septiembre de 2026.

**Architecture:** Se agregará un catálogo semanal que reutiliza `Turno` por día y una asignación versionada por empleado. `horario_programado_para_fecha()` será el contrato canónico que distingue día laborable, descanso y ausencia de configuración; el helper existente seguirá como adaptador. La carga productiva será un comando idempotente preview-first que solo reconciliará propuestas automáticas pendientes.

**Tech Stack:** Django 5.0, PostgreSQL 16, templates Django, JavaScript progresivo compartido `data-async-action`, CSS del ERP, Docker Compose, unittest de Django.

**Entorno de todos los comandos Django:** ejecutar en este worktree con `APP_ENV=development`, `ALLOW_INSECURE_LOCAL_SECRET_KEY=1` y `DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55442/pastelerias_erp`; nunca usar SQLite.

---

## Mapa de archivos

- `rrhh/models.py`: catálogo semanal, detalle por día y asignación vigente.
- `rrhh/migrations/0051_jornadas_semanales.py`: esquema nuevo, sin alterar migraciones previas.
- `rrhh/services_turnos.py`: resolución canónica, compatibilidad y escritura versionada.
- `rrhh/services_jornadas_empleado.py`: validación del POST y cambio auditado desde RRHH.
- `rrhh/services_jornadas_administrativas_2026.py`: preview/aplicación acotada a las seis personas.
- `rrhh/management/commands/configurar_jornadas_administrativas_2026.py`: interfaz operativa preview/`--apply`.
- `rrhh/views.py`: integrar jornada en alta/edición y responder JSON/HTML con una sola lógica.
- `rrhh/templates/rrhh/empleados.html`: selector, resumen semanal, vigencia e historial.
- `static/css/template_modules/rrhh-templates-rrhh-empleados.css`: presentación compacta y responsive.
- `static/erp-sw.js`, `templates/base.html`, `core/templates/core/login.html`: versión del shell PWA.
- `docs/ux/action-context-coverage.md`: cobertura de la acción de jornada.
- `rrhh/tests_jornadas_semanales.py`: modelos, resolución, permisos y vista.
- `rrhh/tests_jornadas_administrativas_2026.py`: preview, idempotencia y límites de la carga inicial.
- `pos_bridge/tests/test_attendance_sync_service.py`, `rrhh/tests_hik_ingesta_v2.py`: regresión de consumidores.

### Task 1: Modelar jornadas semanales e historial

**Files:**
- Modify: `rrhh/models.py:1183-1235`
- Create: `rrhh/migrations/0051_jornadas_semanales.py`
- Create: `rrhh/tests_jornadas_semanales.py`

- [ ] **Step 1: Escribir pruebas fallidas de modelo**

```python
class JornadaSemanalModelTests(TestCase):
    def test_detalle_unico_por_dia(self):
        jornada = JornadaSemanal.objects.create(nombre="Administrativa 2026")
        turno = Turno.objects.create(nombre="Administrativa LV", hora_entrada=time(8), hora_salida=time(16, 30))
        JornadaSemanalDia.objects.create(jornada=jornada, dia_semana=0, turno=turno)
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                JornadaSemanalDia.objects.create(jornada=jornada, dia_semana=0, turno=turno)

    def test_asignaciones_de_jornada_no_se_traslapan(self):
        asignar_jornada_empleado(
            empleado=self.empleado,
            jornada=self.jornada,
            fecha_inicio=date(2026, 9, 1),
            fecha_fin=date(2026, 12, 31),
            motivo="Alta inicial",
            actor=self.user,
        )
        with self.assertRaisesMessage(ValidationError, "Ya existe una jornada asignada"):
            asignar_jornada_empleado(
                empleado=self.empleado,
                jornada=self.jornada,
                fecha_inicio=date(2026, 10, 1),
                fecha_fin=None,
                motivo="Traslape",
                actor=self.user,
            )
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55442/pastelerias_erp \
python3 manage.py test rrhh.tests_jornadas_semanales.JornadaSemanalModelTests --noinput
```

Expected: error de importación porque los modelos aún no existen.

- [ ] **Step 3: Agregar los modelos mínimos**

```python
class JornadaSemanal(models.Model):
    nombre = models.CharField(max_length=100, unique=True)
    descripcion = models.CharField(max_length=240, blank=True, default="")
    activo = models.BooleanField(default=True, db_index=True)
    vigencia_desde = models.DateField(null=True, blank=True)
    vigencia_hasta = models.DateField(null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)


class JornadaSemanalDia(models.Model):
    jornada = models.ForeignKey(JornadaSemanal, on_delete=models.CASCADE, related_name="dias")
    dia_semana = models.PositiveSmallIntegerField()
    turno = models.ForeignKey(Turno, on_delete=models.PROTECT, null=True, blank=True, related_name="jornadas_dia")

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["jornada", "dia_semana"], name="rrhh_jornada_dia_unico"),
            models.CheckConstraint(check=Q(dia_semana__gte=0, dia_semana__lte=6), name="rrhh_jornada_dia_valido"),
        ]


class AsignacionJornadaEmpleado(models.Model):
    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.CASCADE, related_name="jornadas_asignadas")
    jornada = models.ForeignKey(JornadaSemanal, on_delete=models.PROTECT, related_name="asignaciones")
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField(null=True, blank=True)
    motivo = models.CharField(max_length=200)
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    proteger_reingesta_historica = models.BooleanField(default=False)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-fecha_inicio", "-pk"]
        constraints = [
            models.UniqueConstraint(
                fields=["empleado", "fecha_inicio"],
                name="rrhh_jornada_empleado_inicio_unico",
            ),
            models.CheckConstraint(
                check=Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=F("fecha_inicio")),
                name="rrhh_jornada_empleado_rango_valido",
            ),
        ]

    def clean(self):
        super().clean()
        if not self.empleado_id or not self.fecha_inicio:
            return
        traslapes = type(self).objects.filter(empleado_id=self.empleado_id).exclude(pk=self.pk)
        traslapes = traslapes.filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=self.fecha_inicio))
        if self.fecha_fin:
            traslapes = traslapes.filter(fecha_inicio__lte=self.fecha_fin)
        if traslapes.exists():
            raise ValidationError("Ya existe una jornada asignada para parte de esa vigencia.")
```

- [ ] **Step 4: Generar e inspeccionar la migración**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55442/pastelerias_erp \
python3 manage.py makemigrations rrhh --name jornadas_semanales
python3 manage.py sqlmigrate rrhh 0051
```

Expected: solo tres tablas nuevas, llaves, checks e índices; ninguna operación sobre datos existentes.

- [ ] **Step 5: Implementar el servicio mínimo de asignación para satisfacer traslapes**

```python
@transaction.atomic
def asignar_jornada_empleado(*, empleado, jornada, fecha_inicio, fecha_fin, motivo, actor):
    list(
        AsignacionJornadaEmpleado.objects.select_for_update()
        .filter(empleado=empleado)
        .values_list("pk", flat=True)
    )
    asignacion = AsignacionJornadaEmpleado(
        empleado=empleado,
        jornada=jornada,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        motivo=motivo,
        creado_por=actor,
    )
    asignacion.full_clean()
    asignacion.save()
    return asignacion
```

- [ ] **Step 6: Ejecutar pruebas y confirmar GREEN**

Run: el mismo comando de Step 2.

Expected: `OK` con las dos pruebas aprobadas.

- [ ] **Step 7: Confirmar el cambio**

```bash
git add rrhh/models.py rrhh/migrations/0051_jornadas_semanales.py rrhh/services_turnos.py rrhh/tests_jornadas_semanales.py
git commit -m "feat(rrhh): modelar jornadas semanales por empleado"
```

### Task 2: Resolver horario por fecha sin romper consumidores existentes

**Files:**
- Modify: `rrhh/services_turnos.py`
- Modify: `rrhh/tests_jornadas_semanales.py`
- Modify: `rrhh/tests_hik_ingesta_v2.py`
- Modify: `pos_bridge/tests/test_attendance_sync_service.py`

- [ ] **Step 1: Escribir pruebas fallidas del contrato canónico**

```python
def test_resuelve_lunes_sabado_y_descanso(self):
    self.crear_jornada_administrativa()
    self.assertEqual(horario_programado_para_fecha(self.empleado, date(2026, 9, 7)).turno.hora_salida, time(16, 30))
    self.assertEqual(horario_programado_para_fecha(self.empleado, date(2026, 9, 12)).turno.hora_salida, time(13, 30))
    self.assertEqual(horario_programado_para_fecha(self.empleado, date(2026, 9, 13)).estado, ESTADO_DESCANSO)

def test_fallback_conserva_asignacion_turno_legacy(self):
    AsignacionTurnoEmpleado.objects.create(empleado=self.empleado, turno=self.turno_legacy, fecha_inicio=date(2026, 1, 1))
    resultado = horario_programado_para_fecha(self.empleado, date(2026, 9, 7))
    self.assertEqual(resultado.estado, ESTADO_LABORABLE)
    self.assertEqual(resultado.turno, self.turno_legacy)
```

- [ ] **Step 2: Ejecutar las pruebas y comprobar RED**

Expected: `horario_programado_para_fecha` no existe.

- [ ] **Step 3: Implementar el resultado estructurado y adaptador**

```python
ESTADO_LABORABLE = "laborable"
ESTADO_DESCANSO = "descanso"
ESTADO_SIN_ASIGNACION = "sin_asignacion"

@dataclass(frozen=True)
class HorarioProgramado:
    estado: str
    turno: Turno | None = None
    asignacion: AsignacionJornadaEmpleado | AsignacionTurnoEmpleado | None = None

def horario_programado_para_fecha(empleado, fecha) -> HorarioProgramado:
    asignaciones = list(
        AsignacionJornadaEmpleado.objects.filter(
            empleado=empleado,
            fecha_inicio__lte=fecha,
        )
        .filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=fecha))
        .select_related("jornada")[:2]
    )
    if len(asignaciones) > 1:
        raise ValidationError("Hay jornadas semanales traslapadas para la fecha consultada.")
    if asignaciones:
        detalle = asignaciones[0].jornada.dias.select_related("turno").get(dia_semana=fecha.weekday())
        estado = ESTADO_LABORABLE if detalle.turno_id else ESTADO_DESCANSO
        return HorarioProgramado(estado=estado, turno=detalle.turno, asignacion=asignaciones[0])
    turno_legacy = _turno_legacy_asignado_para_fecha(empleado, fecha)
    return HorarioProgramado(
        estado=ESTADO_LABORABLE if turno_legacy else ESTADO_SIN_ASIGNACION,
        turno=turno_legacy,
    )

def turno_asignado_para_fecha(empleado, fecha):
    resultado = horario_programado_para_fecha(empleado, fecha)
    return resultado.turno if resultado.estado == ESTADO_LABORABLE else None
```

Antes de agregar el adaptador, renombrar el cuerpo actual a `_turno_legacy_asignado_para_fecha()` sin cambiar su consulta. La implementación canónica debe cargar máximo dos asignaciones para detectar corrupción por traslape, lanzar `ValidationError` en lugar de escoger arbitrariamente y rechazar perfiles que no tengan exactamente un detalle para cada día 0..6.

- [ ] **Step 4: Cubrir prioridad semanal, fallback y protección histórica**

Agregar pruebas donde una jornada semanal vigente prevalece sobre una asignación legacy y donde `es_jornada_historica_antes_de_asignacion()` reconoce ambos modelos.

- [ ] **Step 5: Ejecutar regresiones Hikvision y Point**

```bash
python3 manage.py test \
  rrhh.tests_jornadas_semanales.JornadaSemanalResolverTests \
  rrhh.tests_hik_ingesta_v2 \
  pos_bridge.tests.test_attendance_sync_service \
  --settings=config.settings_test --noinput
```

Expected: todas aprobadas y los consumidores siguen recibiendo `Turno | None`.

- [ ] **Step 6: Confirmar el cambio**

```bash
git add rrhh/services_turnos.py rrhh/tests_jornadas_semanales.py rrhh/tests_hik_ingesta_v2.py pos_bridge/tests/test_attendance_sync_service.py
git commit -m "feat(rrhh): resolver jornadas semanales por fecha"
```

### Task 3: Crear y cambiar jornada desde la ficha del empleado

**Files:**
- Create: `rrhh/services_jornadas_empleado.py`
- Modify: `rrhh/views.py:1167-1231,1235-1716`
- Modify: `rrhh/tests_jornadas_semanales.py`

- [ ] **Step 1: Escribir pruebas fallidas del servicio**

```python
def test_alta_crea_asignacion_con_fecha_de_ingreso(self):
    response = self.client.post(reverse("rrhh:empleados"), self.payload(
        action="create", jornada_semanal=self.jornada.pk,
        jornada_fecha_inicio="2026-09-01", jornada_motivo="Jornada confirmada por Capital Humano",
    ))
    self.assertEqual(response.status_code, 302)
    empleado = Empleado.objects.get(codigo="ADM-001")
    self.assertTrue(AsignacionJornadaEmpleado.objects.filter(empleado=empleado, jornada=self.jornada).exists())

def test_cambio_cierra_vigencia_anterior_y_crea_historial(self):
    response = self.client.post(reverse("rrhh:empleados"), self.payload_update(
        jornada_semanal=self.otra_jornada.pk, jornada_fecha_inicio="2026-10-01",
        jornada_motivo="Cambio autorizado de horario",
    ), HTTP_ACCEPT="application/json")
    self.assertEqual(response.status_code, 200)
    self.assertEqual(self.asignacion.fecha_fin, date(2026, 9, 30))
```

Agregar pruebas de permiso, motivo obligatorio, fecha anterior al ingreso, perfil inactivo y error que no modifica al empleado.

- [ ] **Step 2: Ejecutar pruebas y comprobar RED**

Expected: el POST actual ignora la jornada y las aserciones fallan.

- [ ] **Step 3: Implementar un servicio único de captura**

```python
@dataclass(frozen=True)
class ResultadoJornada:
    cambio: bool
    mensaje: str
    asignacion: AsignacionJornadaEmpleado | None


@transaction.atomic
def aplicar_jornada_desde_post(*, empleado, post, actor, creacion=False):
    jornada_id = (post.get("jornada_semanal") or "").strip()
    fecha_inicio = parsear_fecha_jornada(post.get("jornada_fecha_inicio"))
    motivo = (post.get("jornada_motivo") or "").strip()
    if not can_manage_rrhh(actor):
        raise PermissionDenied("No tienes permiso para cambiar jornadas.")
    if not jornada_id:
        if creacion:
            return ResultadoJornada(cambio=False, mensaje="Empleado creado sin jornada asignada.", asignacion=None)
        return cerrar_jornada_vigente(empleado=empleado, fecha_fin=fecha_inicio - timedelta(days=1), motivo=motivo, actor=actor)
    if not motivo:
        raise ValidationError("Captura el motivo del cambio de jornada.")
    if fecha_inicio < empleado.fecha_ingreso:
        raise ValidationError("La jornada no puede iniciar antes del ingreso.")
    jornada = JornadaSemanal.objects.get(pk=jornada_id, activo=True)
    asignacion = asignar_jornada_empleado(
        empleado=empleado,
        jornada=jornada,
        fecha_inicio=fecha_inicio,
        fecha_fin=None,
        motivo=motivo,
        actor=actor,
    )
    return ResultadoJornada(cambio=True, mensaje="Jornada guardada correctamente.", asignacion=asignacion)
```

Devolver un objeto resultado con `cambio`, `mensaje` y `asignacion`; no duplicar reglas entre JSON y HTML.

- [ ] **Step 4: Integrar el servicio en alta y edición**

La creación del empleado y su asignación deben vivir en la misma transacción. En actualización, guardar empleado, sincronizar jefatura y aplicar jornada dentro del mismo `transaction.atomic()`.

Agregar helper de respuesta progresiva para `rrhh:empleados`:

```python
if _wants_progressive_response(request):
    return JsonResponse({
        "ok": True,
        "toast": {"type": "success", "message": mensaje},
        "redirect": f'{reverse("rrhh:empleados")}#empleado-{empleado.pk}',
        "reload": True,
    })
```

Los errores JSON deben regresar `400` con toast persistente; el POST tradicional debe conservar mensaje y fragmento estable.

- [ ] **Step 5: Ejecutar pruebas y confirmar GREEN**

Run: `python3 manage.py test rrhh.tests_jornadas_semanales.JornadaEmpleadoViewTests --settings=config.settings_test --noinput`

- [ ] **Step 6: Confirmar el cambio**

```bash
git add rrhh/services_jornadas_empleado.py rrhh/views.py rrhh/tests_jornadas_semanales.py
git commit -m "feat(rrhh): gestionar jornada desde la ficha del empleado"
```

### Task 4: Mostrar selector, semana e historial en alta y edición

**Files:**
- Modify: `rrhh/views.py:1590-1716`
- Modify: `rrhh/templates/rrhh/empleados.html:210-390,740-1040`
- Modify: `static/css/template_modules/rrhh-templates-rrhh-empleados.css`
- Modify: `rrhh/tests_jornadas_semanales.py`

- [ ] **Step 1: Escribir pruebas fallidas de render y accesibilidad**

```python
def test_formulario_muestra_jornada_y_resumen_semanal(self):
    response = self.client.get(reverse("rrhh:empleados"))
    self.assertContains(response, 'name="jornada_semanal"')
    self.assertContains(response, 'name="jornada_fecha_inicio"')
    self.assertContains(response, "48 h semanales")
    self.assertContains(response, "Sin jornada asignada")

def test_edicion_muestra_vigencia_e_historial(self):
    response = self.client.get(reverse("rrhh:empleados"))
    self.assertContains(response, "Vigente desde 01/09/2026")
    self.assertContains(response, "Cambiar jornada")
```

- [ ] **Step 2: Ejecutar pruebas y comprobar RED**

Expected: no existen los campos ni textos.

- [ ] **Step 3: Preparar contexto sin N+1**

Prefetch de jornadas/días/turnos y asignaciones por empleado. Para cada perfil exponer `dias_resumen` y `total_minutos`; para cada empleado exponer `jornada_vigente` e `historial_jornadas` ya ordenado.

- [ ] **Step 4: Añadir componentes al template**

El alta debe incluir:

```html
<section class="rrhh-schedule-panel" data-schedule-panel>
  <label for="jornada_semanal">Jornada semanal</label>
  <select id="jornada_semanal" name="jornada_semanal" data-schedule-select>
    <option value="">Sin jornada asignada</option>
    {% for jornada in jornadas_semanales %}
      <option value="{{ jornada.id }}">{{ jornada.nombre }}</option>
    {% endfor %}
  </select>
  <label for="jornada_fecha_inicio">Vigente desde</label>
  <input id="jornada_fecha_inicio" type="date" name="jornada_fecha_inicio" value="{{ fecha_ingreso_predeterminada|date:'Y-m-d' }}">
  <label for="jornada_motivo">Motivo</label>
  <textarea id="jornada_motivo" name="jornada_motivo" maxlength="200"></textarea>
  <div data-schedule-preview aria-live="polite"></div>
</section>
```

La edición usará IDs con el empleado, mostrará resumen e historial y llevará `data-async-action data-reset-on-success="false"` en el formulario.

- [ ] **Step 5: Implementar preview progresivo y CSS responsive**

Usar datos JSON seguros mediante `json_script`; al cambiar selector, renderizar días y total. En móvil, una sola columna; desde 720 px, días en cuadrícula. Mantener foco visible, `aria-live` y no depender de color.

- [ ] **Step 6: Ejecutar pruebas y confirmar GREEN**

Run: `python3 manage.py test rrhh.tests_jornadas_semanales.JornadaEmpleadoTemplateTests --settings=config.settings_test --noinput`

- [ ] **Step 7: Confirmar el cambio**

```bash
git add rrhh/views.py rrhh/templates/rrhh/empleados.html static/css/template_modules/rrhh-templates-rrhh-empleados.css rrhh/tests_jornadas_semanales.py
git commit -m "ui(rrhh): incorporar jornada semanal en empleados"
```

### Task 5: Hacer que asistencia distinga descanso de jornada faltante

**Files:**
- Modify: `rrhh/services_asistencia_reglas.py:659-731`
- Modify: `rrhh/tests_jornadas_semanales.py`
- Modify: `rrhh/tests_asistencia_reglas.py`

- [ ] **Step 1: Escribir prueba fallida de descanso configurado**

```python
def test_descanso_configurado_no_genera_falta(self):
    resultado = evaluar_dia_empleado(self.empleado, date(2026, 9, 13))
    self.assertEqual(resultado.creados, 0)
    self.assertFalse(IncidenciaAsistencia.objects.filter(empleado=self.empleado, fecha=date(2026, 9, 13)).exists())
```

Agregar otra prueba donde `sin_asignacion` conserva el comportamiento anterior para no esconder errores de configuración.

- [ ] **Step 2: Ejecutar pruebas y comprobar RED**

- [ ] **Step 3: Consultar el contrato canónico antes de generar falta**

En `evaluar_dia_empleado`, resolver `horario_programado_para_fecha()`. Si el estado es descanso y no hay asistencia, resolver incidencias stale y regresar sin generar falta. Si existe asistencia, conservar evaluación visible y no inventar un turno.

- [ ] **Step 4: Ejecutar pruebas y confirmar GREEN**

```bash
python3 manage.py test rrhh.tests_jornadas_semanales rrhh.tests_asistencia_reglas --settings=config.settings_test --noinput
```

- [ ] **Step 5: Confirmar el cambio**

```bash
git add rrhh/services_asistencia_reglas.py rrhh/tests_jornadas_semanales.py rrhh/tests_asistencia_reglas.py
git commit -m "fix(rrhh): respetar descansos de la jornada semanal"
```

### Task 6: Construir carga administrativa 2026 preview-first

**Files:**
- Create: `rrhh/services_jornadas_administrativas_2026.py`
- Create: `rrhh/management/commands/configurar_jornadas_administrativas_2026.py`
- Create: `rrhh/tests_jornadas_administrativas_2026.py`

- [ ] **Step 1: Escribir pruebas fallidas de alcance e idempotencia**

```python
def test_preview_no_escribe_y_reporta_seis_personas(self):
    resumen = configurar_jornadas_administrativas_2026(aplicar=False, hoy=date(2026, 9, 23))
    self.assertEqual(resumen["personas_objetivo"], 6)
    self.assertEqual(JornadaSemanal.objects.count(), 0)
    self.assertEqual(AsignacionJornadaEmpleado.objects.count(), 0)

def test_apply_es_idempotente_y_no_incluye_operativos(self):
    preview = configurar_jornadas_administrativas_2026(aplicar=False, hoy=date(2026, 9, 23))
    configurar_jornadas_administrativas_2026(
        aplicar=True, hoy=date(2026, 9, 23), actor=self.user,
        expected_fingerprint=preview["fingerprint"],
    )
    segundo_preview = configurar_jornadas_administrativas_2026(aplicar=False, hoy=date(2026, 9, 23))
    configurar_jornadas_administrativas_2026(
        aplicar=True, hoy=date(2026, 9, 23), actor=self.user,
        expected_fingerprint=segundo_preview["fingerprint"],
    )
    self.assertEqual(AsignacionJornadaEmpleado.objects.count(), 6)
    self.assertFalse(AsignacionJornadaEmpleado.objects.filter(empleado=self.limpieza).exists())
```

Agregar casos: nombre/ID no coincide, empleado inactivo, traslape, enero-agosto intacto, extras autorizadas/pagadas/rechazadas/canceladas intactas, propuesta pendiente reconciliada, umbral de 50 minutos y ajuste autorizado conservado en bloques de 30 minutos.

- [ ] **Step 2: Ejecutar pruebas y comprobar RED**

- [ ] **Step 3: Implementar manifiesto cerrado**

```python
PERSONAS_2026 = (
    (53, "EGUINO REYES LUIS OCTAVIO", "Administrativa 2026"),
    (4, "NORZAGARAY CONTRERAS JULIETA GUADALUPE", "Administrativa 2026"),
    (3, "SOTO INZUNZA YESENIA", "Administrativa 2026"),
    (99, "FIGUEROA SOTO JOHAN", "Administrativa 2026"),
    (33, "LUGO ESPINOZA PAULA ELIZABETH", "Administrativa 2026"),
    (8, "LOPEZ PALOS JOHANA ADELIN", "Johana 2026"),
)
```

Resolver por ID y validar nombre normalizado y `activo=True`; abortar si cualquier identidad difiere. Crear/reutilizar exactamente `08:00-16:30`, `09:00-17:30` y `08:00-13:30`, siempre con `deteccion_por_checada=False`.

- [ ] **Step 4: Implementar preview de impacto**

El resumen tipado debe incluir las claves `modo`, `fingerprint`, `personas_objetivo`, `personas`, `turnos`, `jornadas`, `asistencias_a_actualizar`, `pendientes_a_reconciliar`, `extras_resueltas_con_diferencia` y `conflictos`. El `fingerprint` será SHA-256 del plan canónico ordenado sin la clave `modo`. Cada persona debe informar ID, nombre esperado/observado, jornada, rango y conteos. No llamar `.save()`, `bulk_create()`, `update()`, `update_or_create()` ni evaluadores cuando `aplicar=False`.

- [ ] **Step 5: Implementar aplicación transaccional**

Dentro de `transaction.atomic()` y bloqueos `select_for_update()`:

1. Crear catálogos y asignaciones 2026-09-01..2026-12-31.
2. Actualizar `AsistenciaEmpleado.turno` solo desde septiembre y hasta `hoy`.
3. Reevaluar únicamente jornadas sin extras resueltas o con propuestas automáticas pendientes.
4. No modificar extras autorizadas, pagadas, rechazadas o canceladas.
5. Crear `AuditLog` con actor, antes/después, fecha y motivo.
6. Recalcular el preview ya bajo bloqueo y abortar si su SHA-256 difiere de `expected_fingerprint`.

- [ ] **Step 6: Exponer comando seguro**

```python
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true")
        parser.add_argument("--actor-username")
        parser.add_argument("--expected-fingerprint")

    def handle(self, *args, **options):
        actor = None
        if options["apply"]:
            username = options["actor_username"]
            expected_fingerprint = options["expected_fingerprint"]
            if not username or not expected_fingerprint:
                raise CommandError("--actor-username y --expected-fingerprint son obligatorios con --apply")
            actor = get_user_model().objects.get(username=username, is_active=True)
        resumen = configurar_jornadas_administrativas_2026(
            aplicar=options["apply"],
            actor=actor,
            expected_fingerprint=options["expected_fingerprint"],
        )
        self.stdout.write(json.dumps(resumen, ensure_ascii=False, default=str, indent=2))
```

- [ ] **Step 7: Ejecutar pruebas y confirmar GREEN**

Run: `python3 manage.py test rrhh.tests_jornadas_administrativas_2026 --settings=config.settings_test --noinput`

- [ ] **Step 8: Confirmar el cambio**

```bash
git add rrhh/services_jornadas_administrativas_2026.py rrhh/management/commands/configurar_jornadas_administrativas_2026.py rrhh/tests_jornadas_administrativas_2026.py
git commit -m "feat(rrhh): preparar jornadas administrativas 2026"
```

### Task 7: Registrar acción y renovar shell PWA

**Files:**
- Modify: `docs/ux/action-context-coverage.md`
- Modify: `static/erp-sw.js:1`
- Modify: `templates/base.html:1240`
- Modify: `core/templates/core/login.html:60`

- [ ] **Step 1: Agregar cobertura de contexto**

Registrar `RRHH / Empleados / Guardar o cambiar jornada` con toast, botón ocupado, reintento, fragmento `#empleado-<id>` y estado `Cubierto` solo después de las pruebas.

- [ ] **Step 2: Cambiar versión del shell en los tres lugares**

Usar esta versión única en el service worker:

```javascript
const CACHE_NAME = "pollyanas-erp-shell-20260923-jornadas-semanales";
```

y registro `/erp-sw.js?v=20260923-jornadas-semanales` en base y login.

- [ ] **Step 3: Verificar consistencia**

```bash
rg -n "pollyanas-erp-shell-20260923-jornadas-semanales|erp-sw.js\?v=20260923-jornadas-semanales" static/erp-sw.js templates/base.html core/templates/core/login.html
```

Expected: un `CACHE_NAME` y dos registros con la nueva versión.

- [ ] **Step 4: Confirmar el cambio**

```bash
git add docs/ux/action-context-coverage.md static/erp-sw.js templates/base.html core/templates/core/login.html
git commit -m "chore(rrhh): versionar interfaz de jornadas semanales"
```

### Task 8: Verificación local completa y navegador

**Files:**
- No code changes expected.

- [ ] **Step 1: Aplicar migración y verificar estado**

```bash
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55442/pastelerias_erp
python3 manage.py migrate
python3 manage.py migrate --check
python3 manage.py showmigrations rrhh
python3 manage.py check
```

Expected: `rrhh.0051` aplicada, cero migraciones pendientes y cero errores.

- [ ] **Step 2: Ejecutar suite enfocada**

```bash
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55442/pastelerias_erp
python3 manage.py test \
  rrhh.tests_jornadas_semanales \
  rrhh.tests_jornadas_administrativas_2026 \
  rrhh.tests_asistencia_reglas \
  rrhh.tests_extra_conciliacion \
  rrhh.tests_hik_ingesta_v2 \
  pos_bridge.tests.test_attendance_sync_service \
  --settings=config.settings_test --noinput
```

Expected: `OK`, cero fallos y cero errores.

- [ ] **Step 3: Ejecutar preview local**

```bash
export APP_ENV=development
export ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55442/pastelerias_erp
python3 manage.py configurar_jornadas_administrativas_2026
```

Expected: modo preview, seis objetivos y cero escrituras.

- [ ] **Step 4: Validar navegador en 390x844 y escritorio**

Comprobar alta, edición, historial, cambio retroactivo, toast, botón ocupado, errores que conservan inputs, consola sin errores y POST JSON correcto. Confirmar que el selector es legible en móvil y que no desplaza innecesariamente los datos principales.

- [ ] **Step 5: Revisar diff y estado**

```bash
git diff --check
git status --short --branch
git diff origin/main..HEAD --stat
git log --oneline --decorate -10
```

Expected: únicamente archivos declarados, árbol limpio y commits quirúrgicos.

### Task 9: PR, CI, despliegue y aplicación productiva

**Files:**
- No code changes expected unless CI reveals a defect scoped to this task.

- [ ] **Step 1: Publicar rama y abrir PR en borrador**

Incluir resumen funcional, modelos/migración, pruebas ejecutadas, validación de navegador y aclarar que la carga productiva todavía está en preview.

- [ ] **Step 2: Esperar CI y revisar diff final**

No mergear con checks pendientes o fallidos. Confirmar que `0051` es la única migración nueva y que no hay cambios de datos dentro de la migración.

- [ ] **Step 3: Mergear y desplegar por el flujo oficial**

```bash
ssh -i ~/.ssh/agente_dg_ops root@68.183.165.47 \
  'cd /opt/pastelerias-erp && bash scripts/deploy_web_safe.sh'
```

No ejecutar `git pull` manual antes.

- [ ] **Step 4: Verificar migración y preview productivo**

```bash
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web python manage.py migrate --check
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web python manage.py check
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web python manage.py configurar_jornadas_administrativas_2026
```

Guardar el JSON del preview como evidencia operacional; debe resolver seis identidades, cero traslapes y detallar extras resueltas sin modificarlas.

- [ ] **Step 5: Aplicar la carga autorizada**

```bash
PREVIEW_JSON="$(docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web \
  python manage.py configurar_jornadas_administrativas_2026)"
PREVIEW_FINGERPRINT="$(printf '%s' "$PREVIEW_JSON" | python3 -c \
  'import json, sys; print(json.load(sys.stdin)["fingerprint"])')"
printf '%s\n' "$PREVIEW_JSON"
docker compose -f /opt/pastelerias-erp/docker-compose.yml exec -T web \
  python manage.py configurar_jornadas_administrativas_2026 \
  --apply --actor-username admin --expected-fingerprint "$PREVIEW_FINGERPRINT"
```

Inmediatamente repetir el preview. Expected: seis asignaciones existentes, cero escrituras pendientes y estado idempotente.

- [ ] **Step 6: Verificar datos y UI reales**

Confirmar en lectura fresca:

- las seis asignaciones 2026-09-01..2026-12-31;
- lunes-viernes 08:00-16:30 para cinco personas;
- lunes-viernes 09:00-17:30 para Johana;
- sábado 08:00-13:30 para las seis;
- Yesenia 2026-09-17 ya no aparece `Sin turno asignado`;
- autorizadas/pagadas/rechazadas/canceladas conservan ID, estado y horas;
- pendientes reflejan el nuevo cálculo y umbral de 50 minutos;
- alta/edición servida, consola y Network sin errores;
- service worker y `CACHE_NAME` nuevos activos.

- [ ] **Step 7: Cerrar worktree por el ciclo de vida**

```bash
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
bash scripts/task_workspace_close.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 \
  --task rrhh-jornadas-semanales --state merged
git -C /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 fetch --prune origin
```

Expected: tarea cerrada, worktree y ramas exactas retiradas, sin tocar tareas ajenas.
