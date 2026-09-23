# Identidad Hik segura después de una baja — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Impedir que una baja o la reutilización de un código Hik produzcan asistencias y horas extra para la persona equivocada, conservando íntegro el historial previo y reconciliando de forma auditable el incidente del código 355.

**Architecture:** Se comparte una regla pura de elegibilidad por baja entre las ingestas Hik antigua y v2; el ledger v2 conserva eventos rechazados como terminales. El modelo `Empleado` impide reasignaciones ordinarias de códigos con historia ajena. Un servicio y comando transaccionales, con `dry-run` por defecto y respaldo previo, corrigen exclusivamente un rango validado sin borrar recibos.

**Tech Stack:** Django 5, PostgreSQL 16, Django TestCase, management commands, JSON y `AuditLog`.

---

## Estructura de archivos

- Modificar `rrhh/services_hikvision.py`: exponer la regla compartida de bloqueo por baja sin alterar historial.
- Modificar `rrhh/services_hik_ingesta.py`: rechazo terminal e idempotente antes de proyectar asistencia.
- Modificar `rrhh/models.py`: validación de reserva histórica al cambiar `Empleado.codigo`.
- Crear `rrhh/services_hik_reconciliacion.py`: precondiciones, preview, respaldo y transacción de corrección.
- Crear `rrhh/management/commands/reconciliar_identidad_hik.py`: interfaz segura `dry-run`/`--apply`.
- Modificar `rrhh/tests_hik_ingesta_v2.py`: cobertura del bloqueo y la idempotencia.
- Crear `rrhh/tests_hik_identidad.py`: cobertura de reserva de código y reconciliación.

### Task 1: Rechazo terminal de marcas posteriores a la baja

**Files:**
- Modify: `rrhh/tests_hik_ingesta_v2.py`
- Modify: `rrhh/services_hik_ingesta.py`
- Modify: `rrhh/services_hikvision.py`

- [x] **Step 1: Write the failing tests**

Agregar pruebas que creen `EmpleadoBaja` y comprueben: el día de baja se acepta; el día posterior devuelve `outcome="rejected"`, `reason_code="employee_inactive_after_termination"`, no crea `AsistenciaEmpleado`, guarda el recibo rechazado con `projection_status="none"` y `effects_status="skipped"`; el mismo GUID vuelve a responder rechazado y no incrementa efectos.

- [x] **Step 2: Run tests to verify they fail**

Run: `python3 manage.py test rrhh.tests_hik_ingesta_v2.HikIngestaV2Tests.test_marca_posterior_a_baja_es_rechazo_terminal rrhh.tests_hik_ingesta_v2.HikIngestaV2Tests.test_reenvio_post_baja_conserva_rechazo_terminal`

Expected: FAIL porque v2 proyecta la asistencia o clasifica el duplicado como diferido.

- [x] **Step 3: Write minimal implementation**

Importar `_baja_bloquea_marca` en `services_hik_ingesta.py`. Después de bloquear al empleado y calcular la fecha local, si la regla bloquea, persistir:

```python
receipt.estado = EventoHikCloud.ESTADO_RECHAZADO
receipt.reason_code = "employee_inactive_after_termination"
receipt.retryable = False
receipt.projection_status = "none"
receipt.effects_status = "skipped"
receipt.procesado_en = timezone.now()
```

Hacer que `_duplicate_result` devuelva `rejected` no reintentable para recibos terminales rechazados y que `ingest_event` traduzca el recibo proyectado a su resultado real antes de ejecutar efectos.

- [x] **Step 4: Run focused and legacy tests**

Run: `python3 manage.py test rrhh.tests_hik_ingesta_v2 rrhh.tests_asistencia_checador_sucursal.BajaBloqueaChecadorTests`

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add rrhh/services_hik_ingesta.py rrhh/services_hikvision.py rrhh/tests_hik_ingesta_v2.py
git commit -m "fix(rrhh): bloquear marcas Hik posteriores a la baja"
```

### Task 2: Reserva histórica de códigos Hik

**Files:**
- Modify: `rrhh/models.py`
- Create: `rrhh/tests_hik_identidad.py`

- [x] **Step 1: Write the failing tests**

Crear casos que demuestren que cambiar el código de una persona a uno presente en `EventoHikCloud.empleado` de otra persona produce `ValidationError`; que guardar otros campos sin cambiar un código ya colisionado funciona; y que un código sin historia o con historia de la misma persona sí se permite.

- [x] **Step 2: Run tests to verify they fail**

Run: `python3 manage.py test rrhh.tests_hik_identidad.ReservaCodigoHikTests`

Expected: FAIL porque `Empleado.save()` todavía acepta la reasignación.

- [x] **Step 3: Write minimal implementation**

Agregar a `Empleado` un método privado que compare el código persistido y consulte `EventoHikCloud` solo cuando el código cambia. Si hay un recibo ligado a otro empleado, lanzar:

```python
raise ValidationError({
    "codigo": "Este código Hik conserva historial de otra persona y no puede reasignarse."
})
```

Ejecutarlo después de normalizar/generar el código y antes de `super().save()`.

- [x] **Step 4: Run focused tests**

Run: `python3 manage.py test rrhh.tests_hik_identidad.ReservaCodigoHikTests rrhh.tests_normalizacion_empleado rrhh.tests_empleado_sucursal_sync`

Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add rrhh/models.py rrhh/tests_hik_identidad.py
git commit -m "fix(rrhh): reservar códigos Hik con historial"
```

### Task 3: Reconciliador transaccional con preview y respaldo

**Files:**
- Create: `rrhh/services_hik_reconciliacion.py`
- Create: `rrhh/management/commands/reconciliar_identidad_hik.py`
- Modify: `rrhh/tests_hik_identidad.py`

- [x] **Step 1: Write the failing service and command tests**

Construir un escenario con persona origen inactiva y baja, destino activo, historia previa del código hacia destino, recibos/asistencias Hik posteriores a la baja y extra automática pendiente. Verificar que el comando sin `--apply` no cambia filas; que `--apply` restaura códigos, mueve recibos/asistencias/extra y crea `AuditLog`; y que conflicto de asistencia, extra no pendiente, ajuste humano o fuente no Hik abortan sin cambios.

- [x] **Step 2: Run tests to verify they fail**

Run: `python3 manage.py test rrhh.tests_hik_identidad.ReconciliarIdentidadHikTests`

Expected: ERROR por comando/servicio inexistente.

- [x] **Step 3: Implement preview and preconditions**

Definir una dataclass `PlanReconciliacionHik` y `preparar_reconciliacion(...)`. La consulta debe limitarse a código, IDs y rango dados; validar baja, actividad del destino, fuentes Hik, ausencia de asistencia destino, extras exclusivamente automáticas pendientes sin `ajuste_autorizacion`, e historia previa coherente. El resultado JSON debe incluir IDs y conteos exactos.

- [x] **Step 4: Implement backup and atomic apply**

`aplicar_reconciliacion(plan, backup_dir)` escribe primero un JSON con empleados, recibos, asistencias y extras. Luego, dentro de `transaction.atomic()`, toma advisory locks de ambas jornadas, bloquea filas en orden, repite precondiciones, usa un código temporal único para intercambiar códigos mediante `QuerySet.update()`, mueve recibos/asistencias/extras y registra un solo `AuditLog` con la huella SHA-256 del respaldo. No elimina registros.

- [x] **Step 5: Implement command interface**

Argumentos obligatorios:

```text
--codigo-afectado 355 --codigo-origen 356
--empleado-origen-id 100 --empleado-destino-id 99
--desde 2026-09-17 --hasta 2026-09-23
[--backup-dir /opt/backups/erp/rrhh-hik] [--apply]
```

Sin `--apply`, imprimir exclusivamente el preview JSON. Con `--apply`, exigir directorio escribible y devolver respaldo, huella, auditoría y conteos posteriores.

- [x] **Step 6: Run reconciliation tests**

Run: `python3 manage.py test rrhh.tests_hik_identidad.ReconciliarIdentidadHikTests`

Expected: PASS.

- [x] **Step 7: Commit**

```bash
git add rrhh/services_hik_reconciliacion.py rrhh/management/commands/reconciliar_identidad_hik.py rrhh/tests_hik_identidad.py
git commit -m "feat(rrhh): reconciliar identidad Hik con auditoría"
```

### Task 4: Regression, review and release

**Files:**
- Modify: `docs/superpowers/specs/2026-09-23-rrhh-hik-baja-identidad-design.md`
- Modify: `docs/superpowers/plans/2026-09-23-rrhh-hik-baja-identidad.md`

- [x] **Step 1: Run the full relevant suite**

Run: `python3 manage.py test rrhh.tests_hik_ingesta_v2 rrhh.tests_hik_identidad rrhh.tests_asistencia_checador_sucursal rrhh.tests_extra_conciliacion`

Expected: PASS.

- [x] **Step 2: Run project checks**

Run: `python3 manage.py migrate --check && python3 manage.py check && git diff --check`

Expected: no pending migrations, 0 issues, no whitespace errors.

- [x] **Step 3: Review diff and commit docs**

Marcar las casillas ejecutadas, registrar comandos/resultados reales y confirmar que no hay archivos fuera del alcance.

```bash
git add docs/superpowers/specs/2026-09-23-rrhh-hik-baja-identidad-design.md docs/superpowers/plans/2026-09-23-rrhh-hik-baja-identidad.md
git commit -m "docs(rrhh): registrar ejecución de protección Hik"
```

- [ ] **Step 4: PR, CI, merge and deploy**

Subir la rama, crear PR en borrador, revisar diff/CI, convertir y mergear. En producción ejecutar únicamente `bash scripts/deploy_web_safe.sh`, sin `git pull` manual, y corroborar el commit servido.

- [ ] **Step 5: Production dry-run before data write**

Ejecutar el comando con los IDs y rango verificados sin `--apply`; comparar conteos con una lectura fresca. Si cualquier precondición cambió, detenerse sin escribir.

- [ ] **Step 6: Apply the authorized scoped repair**

Ejecutar con `--apply` y un directorio de respaldo fuera del repositorio. Confirmar que Angélica conserva el historial hasta su baja y no tiene asistencia/extra posterior incorrecta; Johan recibe solo las jornadas del rango sustentadas por `355`; el ledger y `AuditLog` permanecen completos.

- [ ] **Step 7: Close workspace lifecycle**

Ejecutar `scripts/task_workspace_audit.sh`, comprobar árbol limpio, y cerrar con `scripts/task_workspace_close.sh --state merged` únicamente después de validación productiva.

## Registro de ejecución local

- Pruebas rojas confirmadas: v2 aceptaba marcas posteriores a la baja; el modelo
  permitía reutilizar códigos; el comando correctivo no existía.
- `rrhh.tests_hik_ingesta_v2` y `BajaBloqueaChecadorTests`: 28 pruebas OK.
- Reserva de código y regresiones de empleado: 21 pruebas OK.
- Reconciliador: 10 pruebas OK, incluido historial anterior intacto.
- Suite RRHH/Hik/extra afectada: 85 pruebas OK.
- `python3 manage.py migrate --check`: sin migraciones pendientes.
- `python3 manage.py check`: 0 errores.
- `git diff --check`: sin errores.
- Pendiente: PR, CI, merge, despliegue, `dry-run` productivo, aplicación autorizada
  y verificación visual/datos posterior.
