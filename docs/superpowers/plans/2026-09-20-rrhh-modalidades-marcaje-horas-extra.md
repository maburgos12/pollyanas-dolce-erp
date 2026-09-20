# RRHH Modalidades de Marcaje y Horas Extra Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evitar horas extra automáticas contradictorias al distinguir jornadas de cuatro marcas, dos marcas y ruta, y exigir un turno real antes de calcular una cantidad pagable.

**Architecture:** `Empleado.modalidad_marcaje` permite excepciones explícitas, mientras un resolver único infiere repartidores y registros Point cuando el valor es `AUTO`. `services_extra_conciliacion` devuelve un diagnóstico estructurado que usan generación, incidencias, reportes y autorización web; los templates solo presentan ese contexto y nunca reconstruyen reglas.

**Tech Stack:** Django 5, PostgreSQL 16, Django TestCase, templates Django, CSS existente del ERP.

---

## Mapa de archivos

- `rrhh/models.py`: opciones de modalidad e incidencias nuevas.
- `rrhh/migrations/0047_empleado_modalidad_marcaje.py`: migración aditiva de modalidad, sin backfill ni escrituras históricas.
- `rrhh/migrations/0048_incidencia_asistencia_tipos_marcaje.py`: ampliación aditiva de opciones de incidencia.
- `rrhh/admin.py`: exposición de modalidad como filtro y campo visible.
- `rrhh/services_extra_conciliacion.py`: resolver de modalidad, diagnóstico único y cálculo seguro.
- `rrhh/services/__init__.py`: generación idempotente condicionada a diagnóstico calculable.
- `rrhh/services_asistencia_reglas.py`: incidencias `marcaje_incompleto` y `hora_extra_no_calculable`.
- `rrhh/views.py`: validación de autorización y contexto de presentación por registro.
- `rrhh/templates/rrhh/horas_extra_list.html`: explicación, advertencia y ancla estable.
- `static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css`: estilos compactos y accesibles para el contexto.
- `rrhh/tests_extra_conciliacion.py`: modalidad, diagnóstico, cálculo e idempotencia.
- `rrhh/tests_asistencia_reglas.py`: incidencias y resolución automática.
- `rrhh/tests.py`: autorización web, preservación de registros manuales y presentación.
- `docs/ux/action-context-coverage.md`: cobertura de autorizar/rechazar horas extra.

### Task 1: Modelo y resolución de modalidad

**Files:**
- Modify: `rrhh/models.py:45-145`
- Modify: `rrhh/models.py:1917-1950`
- Modify: `rrhh/admin.py:66-110`
- Create: `rrhh/migrations/0047_empleado_modalidad_marcaje.py`
- Test: `rrhh/tests_extra_conciliacion.py`

- [ ] **Step 1: Escribir pruebas fallidas para la modalidad efectiva**

Agregar a `ExtraConciliacionTests`:

```python
from rrhh.services_extra_conciliacion import modalidad_marcaje_efectiva

def test_modalidad_auto_repartidor_es_ruta(self):
    self.empleado.puesto_operativo = "REPARTIDOR"
    self.empleado.save(update_fields=["puesto_operativo"])
    asistencia = self.asistencia(fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API)

    self.assertEqual(modalidad_marcaje_efectiva(asistencia), Empleado.MARCAJE_RUTA)

def test_modalidad_auto_point_es_dos_marcas(self):
    asistencia = self.asistencia(fuente=AsistenciaEmpleado.FUENTE_POINT)

    self.assertEqual(modalidad_marcaje_efectiva(asistencia), Empleado.MARCAJE_DOS_MARCAS)

def test_modalidad_explicita_prevalece_sobre_puesto_y_fuente(self):
    self.empleado.puesto_operativo = "REPARTIDOR"
    self.empleado.modalidad_marcaje = Empleado.MARCAJE_CUATRO_MARCAS
    self.empleado.save(update_fields=["puesto_operativo", "modalidad_marcaje"])
    asistencia = self.asistencia(fuente=AsistenciaEmpleado.FUENTE_POINT)

    self.assertEqual(modalidad_marcaje_efectiva(asistencia), Empleado.MARCAJE_CUATRO_MARCAS)
```

- [ ] **Step 2: Ejecutar las pruebas y confirmar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py test \
  rrhh.tests_extra_conciliacion.ExtraConciliacionTests.test_modalidad_auto_repartidor_es_ruta \
  rrhh.tests_extra_conciliacion.ExtraConciliacionTests.test_modalidad_auto_point_es_dos_marcas \
  rrhh.tests_extra_conciliacion.ExtraConciliacionTests.test_modalidad_explicita_prevalece_sobre_puesto_y_fuente
```

Expected: FAIL porque `modalidad_marcaje` y `modalidad_marcaje_efectiva` aún no existen.

- [ ] **Step 3: Agregar las opciones al modelo y el resolver mínimo**

En `Empleado`:

```python
MARCAJE_AUTO = "AUTO"
MARCAJE_CUATRO_MARCAS = "CUATRO_MARCAS"
MARCAJE_DOS_MARCAS = "DOS_MARCAS"
MARCAJE_RUTA = "RUTA"
MODALIDAD_MARCAJE_CHOICES = [
    (MARCAJE_AUTO, "Automática según puesto y fuente"),
    (MARCAJE_CUATRO_MARCAS, "Cuatro marcas"),
    (MARCAJE_DOS_MARCAS, "Dos marcas"),
    (MARCAJE_RUTA, "Trabajo en ruta"),
]

modalidad_marcaje = models.CharField(
    max_length=20,
    choices=MODALIDAD_MARCAJE_CHOICES,
    default=MARCAJE_AUTO,
    db_index=True,
    help_text="Define las marcas esperadas; Automática usa puesto y fuente de asistencia.",
)
```

En `services_extra_conciliacion.py`:

```python
from .models import AsistenciaEmpleado, Empleado, HoraExtra


def modalidad_marcaje_efectiva(asistencia):
    if not asistencia or not asistencia.empleado_id:
        return Empleado.MARCAJE_CUATRO_MARCAS
    empleado = asistencia.empleado
    if empleado.modalidad_marcaje != Empleado.MARCAJE_AUTO:
        return empleado.modalidad_marcaje
    if (empleado.puesto_operativo or "").strip().upper() == "REPARTIDOR":
        return Empleado.MARCAJE_RUTA
    if asistencia.fuente == AsistenciaEmpleado.FUENTE_POINT:
        return Empleado.MARCAJE_DOS_MARCAS
    return Empleado.MARCAJE_CUATRO_MARCAS
```

Agregar `modalidad_marcaje` a `EmpleadoAdmin.list_display`, `list_filter` y `search_fields` solo donde corresponda.

- [ ] **Step 4: Generar e inspeccionar la migración aditiva**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py makemigrations rrhh --name empleado_modalidad_marcaje
```

La migración 0047 debe contener exclusivamente `AddField(Empleado.modalidad_marcaje)`. No debe contener `RunPython`, actualizaciones masivas ni cambios destructivos.

- [ ] **Step 5: Ejecutar las pruebas y confirmar GREEN**

Run el comando del Step 2.

Expected: PASS, 3 pruebas.

- [ ] **Step 6: Commit quirúrgico**

```bash
git add rrhh/models.py rrhh/admin.py rrhh/migrations/0047_empleado_modalidad_marcaje.py rrhh/services_extra_conciliacion.py rrhh/tests_extra_conciliacion.py
git commit -m "feat(rrhh): definir modalidades de marcaje"
```

### Task 2: Diagnóstico único y bloqueo de cálculo sin turno

**Files:**
- Modify: `rrhh/services_extra_conciliacion.py:1-105`
- Modify: `rrhh/services/__init__.py:63-108`
- Test: `rrhh/tests_extra_conciliacion.py`

- [ ] **Step 1: Sustituir expectativas antiguas por pruebas fallidas del contrato nuevo**

Agregar el diagnóstico y cambiar `test_sin_turno_ocho_horas_incluyen_comida` para exigir `None`:

```python
from rrhh.services_extra_conciliacion import diagnosticar_horas_extra

def test_sin_turno_no_genera_cantidad_pagable(self):
    asistencia = self.asistencia()

    diagnostico = diagnosticar_horas_extra(asistencia)

    self.assertIsNone(diagnostico.minutos)
    self.assertEqual(diagnostico.codigo, "sin_turno")
    self.assertIn("turno", diagnostico.detalle.lower())
    self.assertIsNone(generar_horas_extra_automatico(asistencia))
    self.assertFalse(HoraExtra.objects.exists())

def test_repartidor_con_turno_calcula_contra_salida_programada(self):
    self.empleado.puesto_operativo = "REPARTIDOR"
    self.empleado.save(update_fields=["puesto_operativo"])
    turno = Turno.objects.create(
        nombre="Ruta 8 a 16",
        hora_entrada=time(8),
        hora_salida=time(16),
        tolerancia_minutos=10,
    )
    asistencia = self.asistencia(salida=time(16, 30), turno=turno)
    asistencia.salida_comida = None
    asistencia.regreso_comida = None
    asistencia.save(update_fields=["salida_comida", "regreso_comida"])

    diagnostico = diagnosticar_horas_extra(asistencia)

    self.assertEqual(diagnostico.minutos, 30)
    self.assertEqual(diagnostico.modalidad, Empleado.MARCAJE_RUTA)
    self.assertTrue(diagnostico.requiere_revision)
```

- [ ] **Step 2: Ejecutar pruebas y confirmar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py test \
  rrhh.tests_extra_conciliacion.ExtraConciliacionTests.test_sin_turno_no_genera_cantidad_pagable \
  rrhh.tests_extra_conciliacion.ExtraConciliacionTests.test_repartidor_con_turno_calcula_contra_salida_programada
```

Expected: FAIL por ausencia de `diagnosticar_horas_extra` y porque el cálculo anterior todavía usa ocho horas genéricas.

- [ ] **Step 3: Implementar el diagnóstico como fuente única**

En `services_extra_conciliacion.py`:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class DiagnosticoHoraExtra:
    minutos: int | None
    codigo: str
    detalle: str
    modalidad: str
    comida_observable: bool
    requiere_revision: bool
    duracion_minutos: int | None = None


def diagnosticar_horas_extra(asistencia):
    modalidad = modalidad_marcaje_efectiva(asistencia)
    if not asistencia or not asistencia.entrada or not asistencia.salida:
        return DiagnosticoHoraExtra(None, "marcaje_incompleto", "Falta entrada o salida final.", modalidad, False, True)
    if asistencia.salida <= asistencia.entrada:
        return DiagnosticoHoraExtra(None, "intervalo_invalido", "La salida no es posterior a la entrada.", modalidad, False, True)
    duracion_total = int((asistencia.salida - asistencia.entrada).total_seconds() // 60)
    if duracion_total > 24 * 60:
        return DiagnosticoHoraExtra(None, "intervalo_invalido", "El intervalo supera 24 horas.", modalidad, False, True, duracion_total)
    if not asistencia.turno_id:
        return DiagnosticoHoraExtra(None, "sin_turno", "Falta asignar el turno de esta jornada.", modalidad, False, True, duracion_total)

    tiene_salida_comida = bool(asistencia.salida_comida)
    tiene_regreso_comida = bool(asistencia.regreso_comida)
    if tiene_salida_comida != tiene_regreso_comida:
        return DiagnosticoHoraExtra(None, "marcaje_comida_incompleto", "Falta una marca de comida.", modalidad, False, True, duracion_total)

    turno = asistencia.turno
    inicio_turno = timezone.make_aware(datetime.combine(asistencia.fecha, turno.hora_entrada))
    fin_turno = timezone.make_aware(datetime.combine(asistencia.fecha, turno.hora_salida))
    if fin_turno <= inicio_turno:
        fin_turno += timedelta(days=1)
    inicio = max(asistencia.entrada, inicio_turno)
    duracion = int((asistencia.salida - inicio).total_seconds() // 60)
    jornada = int((fin_turno - inicio_turno).total_seconds() // 60)
    comida_observable = tiene_salida_comida and tiene_regreso_comida
    if comida_observable:
        if not asistencia.entrada <= asistencia.salida_comida < asistencia.regreso_comida <= asistencia.salida:
            return DiagnosticoHoraExtra(None, "marcaje_comida_invalido", "Las marcas de comida no pertenecen al intervalo trabajado.", modalidad, True, True, duracion_total)
        comida = int((asistencia.regreso_comida - asistencia.salida_comida).total_seconds() // 60)
        duracion -= max(comida - COMIDA_INCLUIDA_MINUTOS, 0)
    excedente = max(0, duracion - jornada)
    tolerancia = int(turno.tolerancia_minutos or 0)
    minutos = excedente if excedente > tolerancia else 0
    requiere_revision = not comida_observable
    detalle = "Comida registrada." if comida_observable else "La comida no es observable en las marcas."
    return DiagnosticoHoraExtra(minutos, "calculado", detalle, modalidad, comida_observable, requiere_revision, duracion_total)


def detectar_minutos_extra(asistencia):
    return diagnosticar_horas_extra(asistencia).minutos
```

En `generar_horas_extra_automatico`, obtener primero el diagnóstico y regresar el registro existente sin crear ni recalcular cuando `diagnostico.minutos is None`. Usar `diagnostico.minutos` para calcular el saldo y conservar la protección de estados no pendientes.

- [ ] **Step 4: Ejecutar pruebas dirigidas y luego todo el archivo**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py test rrhh.tests_extra_conciliacion
```

Expected: PASS. Actualizar exclusivamente las pruebas cuya expectativa era calcular sin turno; cada escenario calculable debe crear un `Turno` explícito.

- [ ] **Step 5: Commit quirúrgico**

```bash
git add rrhh/services_extra_conciliacion.py rrhh/services/__init__.py rrhh/tests_extra_conciliacion.py
git commit -m "fix(rrhh): exigir turno para calcular horas extra"
```

### Task 3: Incidencias de marcaje incompleto y extra no calculable

**Files:**
- Modify: `rrhh/models.py:1917-1950`
- Modify: `rrhh/services_asistencia_reglas.py:346-465`
- Create: `rrhh/migrations/0048_incidencia_asistencia_tipos_marcaje.py`
- Test: `rrhh/tests_asistencia_reglas.py`

- [ ] **Step 1: Escribir pruebas fallidas para las incidencias nuevas**

Agregar a `ReglasAsistenciaRRHHTests`:

```python
def test_intervalo_largo_sin_turno_genera_extra_no_calculable(self):
    fecha = date(2026, 6, 1)
    asistencia = self.crear_asistencia(fecha, time(8), salida=time(17), minutos=540)
    asistencia.turno = None
    asistencia.save(update_fields=["turno"])

    evaluar_dia_empleado(self.empleado, fecha)

    incidencia = IncidenciaAsistencia.objects.get(
        empleado=self.empleado,
        fecha=fecha,
        tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
    )
    self.assertEqual(incidencia.estado, IncidenciaAsistencia.ESTADO_PENDIENTE)
    self.assertEqual(incidencia.metadata["motivo"], "sin_turno")
    self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())

def test_una_marca_de_comida_genera_marcaje_incompleto(self):
    fecha = date(2026, 6, 1)
    asistencia = self.crear_asistencia(fecha, time(8), salida=time(17), minutos=540)
    asistencia.salida_comida = dt_local(fecha, time(12))
    asistencia.regreso_comida = None
    asistencia.save(update_fields=["salida_comida", "regreso_comida"])

    evaluar_dia_empleado(self.empleado, fecha)

    incidencia = IncidenciaAsistencia.objects.get(
        empleado=self.empleado,
        fecha=fecha,
        tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO,
    )
    self.assertIn("comida", incidencia.detalle.lower())
    self.assertFalse(HoraExtra.objects.filter(asistencia=asistencia).exists())

def test_point_sin_marcas_de_comida_no_genera_marcaje_incompleto(self):
    fecha = date(2026, 6, 1)
    self.crear_asistencia(
        fecha,
        time(8),
        salida=time(16),
        minutos=480,
        fuente=AsistenciaEmpleado.FUENTE_POINT,
    )

    evaluar_dia_empleado(self.empleado, fecha)

    self.assertFalse(IncidenciaAsistencia.objects.filter(
        empleado=self.empleado,
        fecha=fecha,
        tipo=IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO,
    ).exists())
```

- [ ] **Step 2: Ejecutar pruebas y confirmar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py test \
  rrhh.tests_asistencia_reglas.ReglasAsistenciaRRHHTests.test_intervalo_largo_sin_turno_genera_extra_no_calculable \
  rrhh.tests_asistencia_reglas.ReglasAsistenciaRRHHTests.test_una_marca_de_comida_genera_marcaje_incompleto \
  rrhh.tests_asistencia_reglas.ReglasAsistenciaRRHHTests.test_point_sin_marcas_de_comida_no_genera_marcaje_incompleto
```

Expected: FAIL porque los tipos y la evaluación todavía no existen.

- [ ] **Step 3: Agregar tipos y evaluación idempotente**

En `IncidenciaAsistencia`:

```python
TIPO_HORA_EXTRA_NO_CALCULABLE = "extra_no_calculable"
TIPO_MARCAJE_INCOMPLETO = "marcaje_incompleto"
```

Agregar ambas opciones a `TIPO_CHOICES`. Crear una migración separada e inspeccionarla:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py makemigrations rrhh --name incidencia_asistencia_tipos_marcaje
```

La migración 0048 debe contener exclusivamente `AlterField(IncidenciaAsistencia.tipo)`.

Importar `modalidad_marcaje_efectiva` y crear en `services_asistencia_reglas.py`:

```python
def _evaluar_integridad_marcaje(asistencia: AsistenciaEmpleado, touched: set[str]) -> tuple[int, int]:
    modalidad = modalidad_marcaje_efectiva(asistencia)
    falta_extremo = bool(asistencia.entrada) != bool(asistencia.salida)
    falta_comida = bool(asistencia.salida_comida) != bool(asistencia.regreso_comida)
    if not falta_extremo and not falta_comida:
        return 0, 0

    partes = []
    if falta_extremo:
        partes.append("Falta entrada o salida final")
    if falta_comida:
        partes.append("Falta una marca de comida")
    tipo = IncidenciaAsistencia.TIPO_MARCAJE_INCOMPLETO
    touched.add(tipo)
    _, creada, actualizada = _upsert_incidencia(
        empleado=asistencia.empleado,
        fecha=asistencia.fecha,
        tipo=tipo,
        estado=IncidenciaAsistencia.ESTADO_PENDIENTE,
        severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA,
        asistencia=asistencia,
        detalle=". ".join(partes) + ".",
        metadata={
            "modalidad": modalidad,
            "falta_entrada_o_salida": falta_extremo,
            "falta_marca_comida": falta_comida,
        },
    )
    return int(creada), int(actualizada)
```

La ausencia de ambas marcas de comida no es una incidencia para `DOS_MARCAS` ni `RUTA`; una sola marca sí es inconsistente en cualquier modalidad. Llamar `_evaluar_integridad_marcaje` antes de `_evaluar_jornada`.

En `_evaluar_hora_extra`, usar `diagnosticar_horas_extra`; cuando el código sea `sin_turno` y `duracion_minutos > JORNADA_DIARIA_MINUTOS + TOLERANCIA_EXTRA_MINUTOS`, hacer `_upsert_incidencia` con:

```python
tipo=IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE
estado=IncidenciaAsistencia.ESTADO_PENDIENTE
severidad=IncidenciaAsistencia.SEVERIDAD_MEDIA
detalle="No se calcularon horas extra: falta asignar el turno de esta jornada."
metadata={
    "motivo": "sin_turno",
    "duracion_minutos": diagnostico.duracion_minutos,
    "modalidad": diagnostico.modalidad,
}
```

Añadir a `touched` solamente los tipos que sí aplican; cuando dejan de aplicar, `_resolver_incidencias_stale` los cierra automáticamente. Si el diagnóstico no es calculable, `_evaluar_hora_extra` debe terminar después de crear la incidencia correspondiente y nunca llamar `generar_horas_extra_automatico`.

- [ ] **Step 4: Ejecutar archivo completo de reglas**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py test rrhh.tests_asistencia_reglas
```

Expected: PASS.

- [ ] **Step 5: Commit quirúrgico**

```bash
git add rrhh/models.py rrhh/migrations/0048_incidencia_asistencia_tipos_marcaje.py rrhh/services_asistencia_reglas.py rrhh/tests_asistencia_reglas.py
git commit -m "feat(rrhh): distinguir marcajes incompletos de horas extra"
```

### Task 4: Autorización segura y explicación visible

**Files:**
- Modify: `rrhh/services_extra_conciliacion.py`
- Modify: `rrhh/views.py:2649-2705`
- Modify: `rrhh/templates/rrhh/horas_extra_list.html`
- Modify: `static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css`
- Test: `rrhh/tests.py`

- [ ] **Step 1: Escribir pruebas fallidas de autorización y presentación**

Agregar al bloque de pruebas web de RRHH:

```python
def test_hora_automatica_sin_turno_no_se_puede_autorizar(self):
    fecha = date(2026, 9, 17)
    jefe_user = User.objects.create_user(username="jefe.extra.bloqueada", password="pass123")
    jefe = Empleado.objects.create(nombre="Jefe Extra Bloqueada", usuario_erp=jefe_user)
    empleado = Empleado.objects.create(nombre="Repartidor bloqueado", puesto_operativo="REPARTIDOR", jefe_directo=jefe)
    asistencia = AsistenciaEmpleado.objects.create(
        empleado=empleado,
        fecha=fecha,
        entrada=timezone.make_aware(datetime.combine(fecha, time(8))),
        salida=timezone.make_aware(datetime.combine(fecha, time(16, 30))),
    )
    hora_extra = HoraExtra.objects.create(
        empleado=empleado,
        asistencia=asistencia,
        jefe_directo=jefe_user,
        fecha=fecha,
        horas=Decimal("0.50"),
        notas="[Detección automática] Registro histórico",
    )
    self.client.force_login(jefe_user)

    response = self.client.post(
        reverse("rrhh:rrhh_he_list"),
        {"hora_extra_id": hora_extra.pk, "action": "autorizar"},
        follow=True,
    )

    hora_extra.refresh_from_db()
    self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_PENDIENTE)
    self.assertContains(response, "Asigna el turno")

def test_hora_manual_sin_turno_conserva_autorizacion(self):
    fecha = date(2026, 9, 17)
    jefe_user = User.objects.create_user(username="jefe.extra.manual", password="pass123")
    empleado = Empleado.objects.create(nombre="Hora manual", salario_diario=Decimal("400"))
    hora_extra = HoraExtra.objects.create(
        empleado=empleado,
        jefe_directo=jefe_user,
        fecha=fecha,
        horas=Decimal("1.00"),
        notas="Carga extraordinaria confirmada",
    )
    self.client.force_login(jefe_user)

    response = self.client.post(
        reverse("rrhh:rrhh_he_list"),
        {"hora_extra_id": hora_extra.pk, "action": "autorizar"},
        follow=True,
    )

    self.assertEqual(response.status_code, 200)
    hora_extra.refresh_from_db()
    self.assertEqual(hora_extra.estado, HoraExtra.ESTADO_AUTORIZADO)
```

- [ ] **Step 2: Ejecutar pruebas y confirmar RED**

Run ambas pruebas por nombre con `python3 manage.py test` y las variables PostgreSQL anteriores.

Expected: la automática se autoriza indebidamente; la manual continúa pasando.

- [ ] **Step 3: Implementar contexto y bloqueo centralizado**

Agregar en `services_extra_conciliacion.py`:

```python
def es_hora_extra_automatica(hora_extra):
    return bool(hora_extra.asistencia_id and (hora_extra.notas or "").startswith(NOTA_EXTRA_AUTOMATICA))


def contexto_hora_extra(hora_extra):
    if not es_hora_extra_automatica(hora_extra):
        return {
            "modalidad": "Manual",
            "comida": "Confirmada por captura manual",
            "turno": "No aplica al cálculo automático",
            "estado": "Captura manual",
            "puede_autorizar": True,
            "motivo_bloqueo": "",
            "requiere_revision": False,
        }
    diagnostico = diagnosticar_horas_extra(hora_extra.asistencia)
    labels = {
        Empleado.MARCAJE_CUATRO_MARCAS: "4 marcas",
        Empleado.MARCAJE_DOS_MARCAS: "2 marcas",
        Empleado.MARCAJE_RUTA: "Ruta",
    }
    puede_autorizar = diagnostico.minutos is not None
    return {
        "modalidad": labels.get(diagnostico.modalidad, diagnostico.modalidad),
        "comida": "Comida registrada" if diagnostico.comida_observable else "Comida no observable",
        "turno": hora_extra.asistencia.turno.nombre if hora_extra.asistencia.turno_id else "Sin turno asignado",
        "estado": "Calculado" if puede_autorizar and not diagnostico.requiere_revision else ("Requiere revisión" if puede_autorizar else "No calculable"),
        "puede_autorizar": puede_autorizar,
        "motivo_bloqueo": "" if puede_autorizar else diagnostico.detalle,
        "requiere_revision": diagnostico.requiere_revision,
    }
```

En `horas_extra_list`:

- cargar `asistencia__turno` y `empleado` con `select_related`;
- convertir el queryset en lista y asignar `he.contexto_calculo = contexto_hora_extra(he)`;
- antes de autorizar una automática, recalcular el contexto y rechazar la acción cuando `puede_autorizar` sea falso;
- conservar `rechazar`;
- redirigir siempre a `#hora-extra-<id>`.

- [ ] **Step 4: Actualizar template y CSS sin duplicar lógica**

En cada `article`:

```django
<article class="ch-row-item" id="hora-extra-{{ he.id }}">
  <div>
    <strong>{{ he.empleado.nombre }}</strong>
    <div class="ch-calculation-context" aria-label="Contexto del cálculo">
      <span>{{ he.contexto_calculo.modalidad }}</span>
      <span>{{ he.contexto_calculo.comida }}</span>
      <span>{{ he.contexto_calculo.turno }}</span>
      <strong>{{ he.contexto_calculo.estado }}</strong>
    </div>
    {% if he.contexto_calculo.motivo_bloqueo %}
      <p class="ch-calculation-warning" role="status">
        {{ he.contexto_calculo.motivo_bloqueo }} Asigna el turno y reevalúa la asistencia antes de autorizar.
      </p>
    {% endif %}
    {% if he.notas %}<p>{{ he.notas|truncatechars:140 }}</p>{% endif %}
  </div>
```

Mostrar `Autorizar` solo cuando `he.contexto_calculo.puede_autorizar`; mantener `Rechazar`. Añadir estilos con fondo semántico suave, texto con contraste AA, sin bordes laterales de acento ni sombras decorativas. Mantener una sola columna bajo 760 px.

Cambiar el query string del `<link>` CSS a `?v=20260920-contexto-extra-v1` para invalidar la caché HTTP del activo modificado. RRHH no tiene service worker propio en este flujo, por lo que no corresponde un bump de `CACHE_NAME`.

- [ ] **Step 5: Ejecutar pruebas web de RRHH**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py test \
  rrhh.tests.RRHHViewsTests.test_hora_automatica_sin_turno_no_se_puede_autorizar \
  rrhh.tests.RRHHViewsTests.test_hora_manual_sin_turno_conserva_autorizacion \
  rrhh.tests.RRHHViewsTests.test_jefe_asignado_ve_y_autoriza_horas_extra_en_su_bandeja \
  rrhh.tests.RRHHViewsTests.test_superuser_ve_y_autoriza_horas_extra_asignadas_a_otro_jefe
```

Expected: PASS; la automática sin turno permanece pendiente y la manual se autoriza.

- [ ] **Step 6: Commit quirúrgico**

```bash
git add rrhh/services_extra_conciliacion.py rrhh/views.py rrhh/templates/rrhh/horas_extra_list.html static/css/template_modules/rrhh-templates-rrhh-horas-extra-list.css rrhh/tests.py
git commit -m "fix(rrhh): explicar y bloquear extras no calculables"
```

### Task 5: Reportes, cobertura UX y regresiones consumidoras

**Files:**
- Modify: `rrhh/services_extra_conciliacion.py`
- Modify: `rrhh/tests_extra_conciliacion.py`
- Modify: `docs/ux/action-context-coverage.md`

- [ ] **Step 1: Escribir una prueba fallida para el reporte no calculable**

```python
def test_reporte_explica_extra_no_calculable_sin_turno(self):
    self.asistencia()

    reportes, _ = _build_reporte_asistencia(self.fecha, self.fecha, str(self.empleado.pk), "")

    extra = reportes[0]["filas"][0]["extra"]
    self.assertIsNone(extra["detectado_minutos"])
    self.assertEqual(extra["estado"], "No calculable: falta asignar turno")
    self.assertIn("modalidad", extra)
```

- [ ] **Step 2: Ejecutar la prueba y confirmar RED**

Run la prueba por nombre con el PostgreSQL aislado.

Expected: FAIL porque el estado actual solo menciona checadas o intervalo válido.

- [ ] **Step 3: Enriquecer conciliación sin mutaciones**

Hacer que `conciliar_extra_diario` use `diagnosticar_horas_extra` y devuelva, además de las claves actuales:

```python
{
    "modalidad": diagnostico.modalidad,
    "comida_observable": diagnostico.comida_observable,
    "requiere_revision": diagnostico.requiere_revision,
    "codigo": diagnostico.codigo,
}
```

Cuando `codigo == "sin_turno"`, usar exactamente `No calculable: falta asignar turno`. Conservar el carácter de solo lectura de `_build_reporte_asistencia`.

- [ ] **Step 4: Actualizar cobertura de acciones**

Agregar a la tabla de `docs/ux/action-context-coverage.md`:

```markdown
| RRHH / Horas extra | Autorizar y rechazar desde la bandeja | Toast global y bloqueo del botón por el contrato base; autorización revalida cálculo | Sí, `#hora-extra-<id>`; error conserva el registro visible | `rrhh.tests` + navegador local | Parcial: flujo tradicional con ancla; extras automáticas sin turno no se autorizan |
```

- [ ] **Step 5: Ejecutar regresiones dirigidas**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py test \
  rrhh.tests_extra_conciliacion \
  rrhh.tests_asistencia_reglas \
  rrhh.tests_prenomina \
  bonos_produccion \
  bonos_ventas
```

Expected: PASS. Confirmar en particular que `bono_extra`, `ajuste_positivo` y `ajuste_negativo` no son escritos por ninguna prueba o cambio.

- [ ] **Step 6: Commit quirúrgico**

```bash
git add rrhh/services_extra_conciliacion.py rrhh/tests_extra_conciliacion.py docs/ux/action-context-coverage.md
git commit -m "test(rrhh): cubrir conciliacion por modalidad de marcaje"
```

### Task 6: Verificación integral, navegador, PR y producción

**Files:**
- Verify only; no new production code unless una prueba demuestra un defecto.

- [ ] **Step 1: Aplicar migración y verificar que no hay drift**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py migrate

APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py makemigrations --check --dry-run

APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py migrate --check
```

Expected: `No changes detected` y cero migraciones pendientes.

- [ ] **Step 2: Ejecutar checks y suite RRHH completa**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py check

APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 \
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55529/pastelerias_erp \
python3 manage.py test rrhh --parallel
```

Expected: cero errores y todas las pruebas RRHH aprobadas.

- [ ] **Step 3: Revisar diff y estado antes de publicar**

```bash
git status --short --branch
git log --oneline --decorate -8
git worktree list
git diff origin/main..HEAD --stat
git diff --check origin/main..HEAD
git worktree prune --dry-run
```

Confirmar que solo aparecen RRHH, su CSS, migración, pruebas, especificación, plan y cobertura UX.

- [ ] **Step 4: Validar en navegador local**

Levantar el servidor local contra PostgreSQL y verificar con navegador real:

- registro manual pendiente: muestra contexto Manual y permite autorizar;
- registro automático con turno: muestra modalidad, comida y turno;
- registro automático histórico sin turno: muestra No calculable, oculta Autorizar y conserva Rechazar;
- consola sin errores;
- respuesta POST y redirección conservan `#hora-extra-<id>`;
- viewport de 390 px sin desplazamiento horizontal.

- [ ] **Step 5: Crear PR borrador y esperar CI**

Antes del PR, repetir `git status --short --branch`, revisar todos los commits y confirmar que no quedan archivos sin commit. Subir la rama, crear PR como borrador con resumen funcional, archivos principales, pruebas y validación en navegador. Esperar el check requerido y corregir cualquier fallo en la misma rama.

- [ ] **Step 6: Merge, despliegue y migración productiva**

Tras CI aprobado, sacar el PR de borrador, hacer merge a `main` y en el VPS ejecutar exclusivamente:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

No ejecutar `git pull` manual. Verificar que las migraciones 0047 y 0048 estén aplicadas y que los contenedores correspondientes recargaron el código nuevo.

- [ ] **Step 7: Validación productiva sin alterar históricos**

Realizar lecturas antes y después:

```sql
SELECT estado, COUNT(*), SUM(horas)
FROM rrhh_horaextra
GROUP BY estado
ORDER BY estado;

SELECT COUNT(*)
FROM rrhh_empleado
WHERE activo AND UPPER(TRIM(COALESCE(puesto_operativo, ''))) = 'REPARTIDOR';
```

Los conteos y sumas de horas extra deben permanecer iguales inmediatamente después del deploy. Abrir la bandeja productiva con un usuario autorizado, confirmar texto y bloqueo, revisar consola/Network y no pulsar `Autorizar` o `Rechazar` durante esta verificación.

- [ ] **Step 8: Cerrar el worktree oficial**

Después de merge, deploy y validación:

```bash
bash scripts/task_workspace_audit.sh --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1
bash scripts/task_workspace_close.sh \
  --repo /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 \
  --task rrhh_modalidad_marcaje_comida \
  --state merged
```

Expected: worktree y ramas de esta tarea eliminados de forma idempotente, con `origin/main` sincronizado y sin tocar tareas ajenas.
