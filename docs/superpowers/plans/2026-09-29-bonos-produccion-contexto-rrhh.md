# Bonos de producción con contexto RRHH Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Calcular faltas de bonos de producción desde el contexto laboral real de RRHH, preservar capturas manuales y habilitar los montos faltantes sin modificar datos productivos durante el deploy.

**Architecture:** RRHH expondrá un clasificador diario puro y cargable por lote. Bonos persistirá una proyección nullable por fecha para trazabilidad y compatibilidad histórica, y usará el conteo explícito de faltas penalizables sólo después de una sincronización completa.

**Tech Stack:** Django 5, PostgreSQL 16, Django TestCase, templates/React UMD existentes, Docker Compose, service worker PWA.

---

### Task 1: Entorno PostgreSQL y línea base

**Files:**
- Verify: `docker-compose.yml`
- Verify: `bonos_produccion/tests_sync_checador.py`
- Verify: `rrhh/tests_asistencia_reglas.py`

- [ ] **Step 1: Levantar PostgreSQL aislado**

Run:

```bash
COMPOSE_PROJECT_NAME=erp_bonos_rrhh DB_HOST_PORT=55440 docker compose up -d db
```

Expected: contenedor `db` healthy.

- [ ] **Step 2: Verificar conectividad y migraciones**

Run:

```bash
COMPOSE_PROJECT_NAME=erp_bonos_rrhh DB_HOST_PORT=55440 docker compose exec -T db pg_isready -U postgres
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py migrate
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py migrate --check
```

Expected: PostgreSQL acepta conexiones y no quedan migraciones pendientes.

- [ ] **Step 3: Ejecutar línea base del módulo**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py test bonos_produccion.tests_sync_checador bonos_produccion.tests_cancelacion_prorrateo rrhh.tests_asistencia_reglas --keepdb
```

Expected: suite base verde antes de modificar código.

### Task 2: Clasificador diario de RRHH

**Files:**
- Create: `rrhh/services_asistencia_contexto.py`
- Create: `rrhh/tests_asistencia_contexto.py`

- [ ] **Step 1: Escribir pruebas fallidas del contrato público**

Crear casos que construyan `Empleado`, `IncapacidadEmpleado`, `SuspensionEmpleado`, `SolicitudVacaciones`, `PermisoSalida`, jornada y asistencia. El contrato esperado:

```python
contexto = cargar_contexto_asistencia(
    empleados=[empleado],
    fecha_inicio=date(2026, 8, 28),
    fecha_fin=date(2026, 9, 26),
)
resultado = contexto.clasificar(empleado, date(2026, 9, 1))
self.assertEqual(resultado.codigo, CODIGO_INCAPACIDAD)
self.assertFalse(resultado.es_exigible)
self.assertFalse(resultado.falta_penalizable)
```

Agregar casos separados para festivo, descanso semanal, preingreso, postbaja, suspensión, vacaciones, permiso aprobado, asistencia, retardo pendiente y falta sin justificación.

- [ ] **Step 2: Ejecutar las pruebas y confirmar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py test rrhh.tests_asistencia_contexto --keepdb
```

Expected: FAIL por módulo/función inexistente.

- [ ] **Step 3: Implementar servicio puro y por lote**

Definir:

```python
@dataclass(frozen=True)
class ContextoDiaEmpleado:
    codigo: str
    es_exigible: bool
    falta_penalizable: bool
    motivo: str
    fuente_modelo: str = ""
    fuente_id: int | None = None

def cargar_contexto_asistencia(*, empleados, fecha_inicio, fecha_fin) -> ContextoAsistenciaLote:
    ...
```

El cargador hará consultas acotadas para asistencias, incidencias, incapacidades, suspensiones, vacaciones, permisos, bajas y jornadas. `clasificar` aplicará la precedencia del diseño y reutilizará `es_descanso_oficial` y `horario_programado_para_fecha`.

- [ ] **Step 4: Ejecutar pruebas y confirmar GREEN**

Run: comando del Step 2.

Expected: todos los casos pasan sin escrituras producidas por el clasificador.

- [ ] **Step 5: Commit**

```bash
git add rrhh/services_asistencia_contexto.py rrhh/tests_asistencia_contexto.py
git commit -m "feat(rrhh): exponer contexto laboral diario"
```

### Task 3: Proyección diaria compatible con históricos

**Files:**
- Modify: `bonos_produccion/models.py`
- Create: `bonos_produccion/migrations/0009_registro_contexto_rrhh.py`
- Modify: `bonos_produccion/serializers.py`
- Modify: `bonos_produccion/tests_sync_checador.py`

- [ ] **Step 1: Escribir pruebas fallidas de persistencia y API**

Esperar campos:

```python
registro = RegistroDiarioProduccion.objects.create(
    bono=bono,
    dia=1,
    fecha=date(2026, 9, 1),
    estado_rrhh="incapacidad",
    motivo_rrhh="Incapacidad vigente.",
    falta_penalizable=False,
)
self.assertEqual(serializer.data["estado_rrhh"], "incapacidad")
self.assertEqual(serializer.data["fecha"], "2026-09-01")
```

Verificar que filas históricas admiten `fecha=None` y `falta_penalizable=None`.

- [ ] **Step 2: Confirmar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py test bonos_produccion.tests_sync_checador --keepdb
```

Expected: FAIL por campos inexistentes.

- [ ] **Step 3: Añadir campos aditivos y restricción**

```python
fecha = models.DateField(null=True, blank=True)
estado_rrhh = models.CharField(max_length=32, blank=True, default="")
motivo_rrhh = models.CharField(max_length=200, blank=True, default="")
falta_penalizable = models.BooleanField(null=True, blank=True)
```

Agregar `faltas_rrhh = models.PositiveSmallIntegerField(null=True, blank=True)` a `BonoProduccionEmpleado`. Crear migración sin `RunPython`, sin backfill y sin alterar datos existentes.

- [ ] **Step 4: Exponer campos de solo lectura en serializers**

Mantener `capturado_por`, booleanos operativos y compatibilidad con `dia`. La escritura manual no podrá inventar `estado_rrhh` ni `falta_penalizable`.

- [ ] **Step 5: Migrar y confirmar GREEN**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py migrate
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py test bonos_produccion.tests_sync_checador --keepdb
```

Expected: migración aplicada y pruebas verdes.

- [ ] **Step 6: Commit**

```bash
git add bonos_produccion/models.py bonos_produccion/migrations/0010_registro_contexto_rrhh.py bonos_produccion/serializers.py bonos_produccion/tests_sync_checador.py
git commit -m "feat(bonos): guardar contexto RRHH por día"
```

### Task 4: Sincronización masiva, manual-safe e idempotente

**Files:**
- Modify: `bonos_produccion/services_checador.py`
- Modify: `bonos_produccion/services_recalculo.py`
- Modify: `bonos_produccion/models.py`
- Modify: `bonos_produccion/tests_sync_checador.py`
- Modify: `bonos_produccion/tests_cancelacion_prorrateo.py`

- [ ] **Step 1: Escribir reproducción fallida de Argelia**

Crear periodo 28-08 a 26-09, incapacidad hasta 01-09, 21 asistencias desde 02-09 y festivo 16-09. Esperar:

```python
sincronizar_asistencia_desde_checador(periodo)
bono.refresh_from_db()
self.assertEqual(bono.faltas_rrhh, 0)
self.assertFalse(bono.cancela_bono)
```

Verificar estados `incapacidad` el 01 y `festivo` el 16.

- [ ] **Step 2: Añadir pruebas fallidas de preservación e idempotencia**

- Registro con `capturado_por` y booleanos manuales queda idéntico.
- Ajustes monetarios y descripciones quedan idénticos.
- Segunda sincronización reporta cero creados/actualizados funcionales.
- Ausencia injustificada crea `falta_penalizable=True` y cancela según la regla.

- [ ] **Step 3: Confirmar RED**

Run: suite de `bonos_produccion.tests_sync_checador` y `tests_cancelacion_prorrateo`.

Expected: fallos específicos por el cálculo legado y la sobrescritura actual.

- [ ] **Step 4: Implementar carga y persistencia masivas**

Reemplazar `get_or_create` dentro del doble bucle por:

```python
def fecha_registro_legacy(periodo, dia):
    inicio, fin = _rango_periodo(periodo)
    fechas = [fecha for fecha in _fechas(inicio, fin) if fecha.day == dia]
    return fechas[0] if len(fechas) == 1 else None

existentes = {
    (registro.bono_id, registro.fecha or fecha_registro_legacy(registro.bono.periodo, registro.dia)): registro
    for registro in RegistroDiarioProduccion.objects.filter(bono__in=bonos)
}
contexto = cargar_contexto_asistencia(...)
# construir por_crear y por_actualizar
RegistroDiarioProduccion.objects.bulk_create(por_crear)
RegistroDiarioProduccion.objects.bulk_update(
    por_actualizar,
    ["fecha", "estado_rrhh", "motivo_rrhh", "falta_penalizable",
     "tiene_asistencia", "tiene_puntualidad"],
)
```

Si `capturado_por_id` existe, actualizar sólo fecha/contexto RRHH y conservar los booleanos.

- [ ] **Step 5: Calcular faltas explícitas con fallback legado**

`recalcular_desde_registros` asignará `faltas_rrhh` sólo cuando todos los registros sincronizables tengan clasificación no null. `BonoProduccionEmpleado.recalcular` usará:

```python
faltas = (
    int(self.faltas_rrhh)
    if self.faltas_rrhh is not None
    else max(dias_exigibles - int(self.dias_asistencia or 0), 0)
)
```

El resto de campos manuales no se modifica.

- [ ] **Step 6: Confirmar GREEN y ausencia de N+1**

Run: comando del Step 3 y una prueba `assertNumQueries` con al menos dos empleados.

Expected: pruebas verdes, segunda ejecución sin cambios y consultas acotadas.

- [ ] **Step 7: Commit**

```bash
git add bonos_produccion/services_checador.py bonos_produccion/services_recalculo.py bonos_produccion/models.py bonos_produccion/tests_sync_checador.py bonos_produccion/tests_cancelacion_prorrateo.py
git commit -m "fix(bonos): calcular faltas desde RRHH sin pisar capturas"
```

### Task 5: Montos faltantes y experiencia visible

**Files:**
- Modify: `bonos_produccion/views_html.py`
- Modify: `bonos_produccion/templates/bonos_produccion/dashboard.html`
- Modify: `bonos_produccion/templates/bonos_produccion/index.html`
- Modify: `bonos_produccion/static/bonos_produccion/sw.js`
- Modify: `bonos_produccion/tests.py`
- Modify: `bonos_produccion/tests_sync_checador.py`

- [ ] **Step 1: Escribir pruebas fallidas de los dos montos**

Comprobar que el HTML contiene inputs `monto_preparacion` y `monto_cuartos_frios`, usa valores del periodo y que POST guarda cada uno sin modificar ajustes manuales.

- [ ] **Step 2: Escribir prueba fallida de etiquetas y acción única**

Comprobar que el payload de captura expone `estado_rrhh`, `motivo_rrhh`, `falta_penalizable` y que el botón de sincronización tiene el contrato asíncrono/estado ocupado usado por el ERP.

- [ ] **Step 3: Confirmar RED**

Run:

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py test bonos_produccion.tests bonos_produccion.tests_sync_checador --keepdb
```

Expected: faltan inputs y etiquetas.

- [ ] **Step 4: Implementar inputs y visualización**

Añadir:

```html
<label>Preparación
  <input name="monto_preparacion" type="number" min="0" step="0.01" value="{{ periodo.monto_preparacion|default:defaults.monto_preparacion }}">
</label>
<label>Cuartos fríos
  <input name="monto_cuartos_frios" type="number" min="0" step="0.01" value="{{ periodo.monto_cuartos_frios|default:defaults.monto_cuartos_frios }}">
</label>
```

Agregar defaults de renderizado y badges por `estado_rrhh`. Deshabilitar sincronización mientras la solicitud está activa y mostrar el resumen devuelto.

- [ ] **Step 5: Aumentar caché PWA**

Cambiar `CACHE_NAME` a la siguiente versión disponible en el mismo commit.

- [ ] **Step 6: Confirmar GREEN**

Run: comando del Step 3.

Expected: pruebas verdes y ajustes manuales preservados.

- [ ] **Step 7: Commit**

```bash
git add bonos_produccion/views_html.py bonos_produccion/templates/bonos_produccion/dashboard.html bonos_produccion/templates/bonos_produccion/index.html bonos_produccion/static/bonos_produccion/sw.js bonos_produccion/tests.py bonos_produccion/tests_sync_checador.py
git commit -m "feat(bonos): explicar incidencias y editar montos por área"
```

### Task 6: Verificación, PR, deploy y preview sin escritura

**Files:**
- Verify: all changed files
- Create if needed: `bonos_produccion/management/commands/preview_sync_contexto_rrhh.py`
- Test: `bonos_produccion/tests_*.py`, `rrhh/tests_asistencia_contexto.py`

- [ ] **Step 1: Ejecutar pruebas completas afectadas**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py test bonos_produccion rrhh.tests_asistencia_contexto rrhh.tests_asistencia_reglas --keepdb
```

Expected: cero fallos.

- [ ] **Step 2: Verificar Django y migraciones**

```bash
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py migrate --check
APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1 DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55440/pastelerias_erp .venv/bin/python manage.py makemigrations --check --dry-run
```

Expected: cero errores y ninguna migración faltante.

- [ ] **Step 3: Revisar diff y crear PR**

Confirmar que sólo hay archivos del alcance, subir rama, abrir PR y adjuntarlo a la tarea.

- [ ] **Step 4: Mergear y desplegar por el flujo oficial**

En VPS ejecutar exclusivamente:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

No ejecutar `git pull` manual antes. No lanzar sincronización ni recálculo.

- [ ] **Step 5: Verificar UI y caché en producción**

Abrir dashboard y PWA autenticados. Confirmar inputs, etiquetas, consola, XHR y service worker actualizado.

- [ ] **Step 6: Generar preview productivo read-only**

Comparar por empleado:

- estado actual;
- faltas actuales;
- faltas RRHH propuestas;
- total actual y proyectado;
- hash de campos manuales antes/después de la simulación.

El comando debe usar rollback o cálculo puro y no guardar modelos.

- [ ] **Step 7: Pedir autorización de datos**

Presentar el preview, incluyendo Argelia y el cambio potencial de $850 a $300. No aplicar sincronización ni montos hasta recibir autorización explícita.
