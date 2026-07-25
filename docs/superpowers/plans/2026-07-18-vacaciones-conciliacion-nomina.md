# Vacaciones Conciliación de Nómina Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Conciliar conceptos importados de vacaciones, prima vacacional y terminación contra las bolsas desplegadas, sin confundir pago con goce.

**Architecture:** Un catálogo explícito clasificará `NominaConceptoLinea`; `PagoVacaciones` preservará la evidencia y `AplicacionPagoVacaciones` la distribuirá FIFO entre bolsas. La importación creará eventos idempotentes y enviará ambigüedades a Capital Humano; ninguna operación económica modificará aplicaciones de goce.

**Tech Stack:** Django 5.0.1, PostgreSQL 16, Django ORM, importador de lista de raya, templates Django y Django TestCase.

---

## Precondiciones y archivos

La entrega de goce FIFO debe estar mergeada, desplegada y validada. Claude crea/revisa migraciones, commits, PR, merge y deploy.

- Modify: `rrhh/models.py`
- Create: `rrhh/migrations/0037_vacaciones_conciliacion_nomina.py` (ajustar al HEAD)
- Create: `rrhh/services_vacaciones_nomina.py`
- Modify: `rrhh/services/lista_raya.py:143-322`
- Modify: `rrhh/views.py`, `rrhh/urls.py`
- Create: `rrhh/templates/rrhh/vacaciones_conciliacion.html`
- Modify: `rrhh/admin.py`
- Create: `rrhh/tests_vacaciones_nomina.py`
- Modify: `docs/ux/action-context-coverage.md`

### Task 1: Worktree y línea base

- [ ] **Step 1: Crear worktree desde main ya validado**

```bash
git fetch origin main
git worktree add /Users/mauricioburgos/Downloads/codex_worktrees/rrhh-vacaciones-conciliacion-nomina \
  -b codex/rrhh-vacaciones-conciliacion-nomina origin/main
cd /Users/mauricioburgos/Downloads/codex_worktrees/rrhh-vacaciones-conciliacion-nomina
bash scripts/git_workspace_preflight.sh --write
```

- [ ] **Step 2: Verificar migraciones y bolsas**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py migrate
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py migrate --check
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py check
```

Expected: cero pendientes y modelos FIFO presentes.

### Task 2: Modelar equivalencias y evidencia económica

- [ ] **Step 1: Escribir pruebas fallidas**

```python
def test_pago_es_unico_por_concepto_nomina(self):
    PagoVacaciones.objects.create(
        concepto_nomina=self.concepto, empleado=self.empleado,
        tipo="vacaciones", importe=self.concepto.importe,
    )
    with self.assertRaises(IntegrityError):
        with transaction.atomic():
            PagoVacaciones.objects.create(
                concepto_nomina=self.concepto, empleado=self.empleado,
                tipo="vacaciones", importe=self.concepto.importe,
            )

def test_aplicacion_pago_requiere_dias_positivos(self):
    with self.assertRaises(IntegrityError):
        AplicacionPagoVacaciones.objects.create(
            pago=self.pago, periodo=self.periodo, dias=Decimal("0"),
        )
```

- [ ] **Step 2: Ejecutar; Expected: FAIL por modelos inexistentes**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test \
  rrhh.tests_vacaciones_nomina.ModelosVacacionesNominaTests --keepdb
```

- [ ] **Step 3: Implementar modelos mínimos**

```python
class EquivalenciaConceptoVacaciones(models.Model):
    codigo_concepto = models.CharField(max_length=20, blank=True, default="")
    nombre_normalizado = models.CharField(max_length=180)
    tipo = models.CharField(max_length=16)  # vacaciones, prima, terminacion
    valor_representa_dias = models.BooleanField(default=False)
    activo = models.BooleanField(default=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["codigo_concepto", "nombre_normalizado"],
                name="uq_equiv_concepto_vacaciones",
            )
        ]

class PagoVacaciones(models.Model):
    concepto_nomina = models.OneToOneField(
        NominaConceptoLinea, on_delete=models.PROTECT, related_name="pago_vacaciones",
    )
    empleado = models.ForeignKey(Empleado, on_delete=models.PROTECT, related_name="pagos_vacaciones")
    tipo = models.CharField(max_length=16)
    dias_informados = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    importe = models.DecimalField(max_digits=12, decimal_places=2)
    estado = models.CharField(max_length=16, default="pendiente", db_index=True)
    motivo_pendiente = models.CharField(max_length=220, blank=True, default="")
    creado_en = models.DateTimeField(auto_now_add=True)

class AplicacionPagoVacaciones(models.Model):
    pago = models.ForeignKey(PagoVacaciones, on_delete=models.PROTECT, related_name="aplicaciones")
    periodo = models.ForeignKey(PeriodoVacacional, on_delete=models.PROTECT, related_name="aplicaciones_pago")
    dias = models.DecimalField(max_digits=6, decimal_places=2)
    actor = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["pago", "periodo"], name="uq_aplicacion_pago_periodo"),
            models.CheckConstraint(condition=models.Q(dias__gt=0), name="ck_aplicacion_pago_dias_pos"),
        ]
```

- [ ] **Step 4: Claude crea, inspecciona y aplica migración**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py makemigrations rrhh --name vacaciones_conciliacion_nomina
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py sqlmigrate rrhh 0037
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py migrate
```

- [ ] **Step 5: Pruebas PASS y commit por Claude**

```bash
git add rrhh/models.py rrhh/migrations/0037_vacaciones_conciliacion_nomina.py rrhh/tests_vacaciones_nomina.py
git commit -m "feat(rrhh): modelar conciliacion economica de vacaciones"
```

### Task 3: Detectar conceptos de manera idempotente

- [ ] **Step 1: Probar clasificación explícita**

```python
def test_concepto_configurado_crea_pago_pendiente(self):
    pago = detectar_pago_vacaciones(self.concepto_vacaciones)
    self.assertEqual(pago.tipo, "vacaciones")
    self.assertEqual(pago.dias_informados, self.concepto_vacaciones.valor)
    self.assertEqual(pago.estado, "pendiente")

def test_concepto_no_configurado_no_inventa_pago(self):
    self.assertIsNone(detectar_pago_vacaciones(self.concepto_desconocido))
```

- [ ] **Step 2: Crear `rrhh/services_vacaciones_nomina.py`**

```python
def detectar_pago_vacaciones(concepto):
    equivalencia = equivalencia_para_concepto(concepto)
    if not equivalencia:
        return None
    dias = concepto.valor if equivalencia.valor_representa_dias else None
    pago, _ = PagoVacaciones.objects.get_or_create(
        concepto_nomina=concepto,
        defaults={
            "empleado": concepto.linea.empleado,
            "tipo": equivalencia.tipo,
            "dias_informados": dias,
            "importe": concepto.importe,
            "motivo_pendiente": "Cantidad de días no confiable" if dias is None else "",
        },
    )
    return pago
```

- [ ] **Step 3: Probar dos llamadas; Expected: un solo pago**

- [ ] **Step 4: Ejecutar módulo y commit por Claude**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test rrhh.tests_vacaciones_nomina --keepdb
git add rrhh/services_vacaciones_nomina.py rrhh/tests_vacaciones_nomina.py
git commit -m "feat(rrhh): detectar pagos vacacionales desde nomina"
```

### Task 4: Conciliar días pagados por FIFO económico

- [ ] **Step 1: Escribir prueba 7+5 e independencia de goce**

```python
def test_pago_doce_aplica_siete_2025_y_cinco_2026(self):
    aplicaciones = conciliar_pago_fifo(self.pago_12, actor=self.rrhh)
    self.assertEqual(
        [(a.periodo.aniversario.year, a.dias) for a in aplicaciones],
        [(2025, Decimal("7")), (2026, Decimal("5"))],
    )

def test_conciliar_pago_no_modifica_goce(self):
    antes = list(self.periodo_2025.aplicaciones_goce.values_list("id", "estado", "dias"))
    conciliar_pago_fifo(self.pago_12, actor=self.rrhh)
    self.assertEqual(
        antes,
        list(self.periodo_2025.aplicaciones_goce.values_list("id", "estado", "dias")),
    )
```

- [ ] **Step 2: Implementar saldo económico separado**

```python
def dias_pagados_periodo(periodo):
    return periodo.aplicaciones_pago.filter(
        pago__estado="conciliado"
    ).aggregate(total=Sum("dias"))["total"] or Decimal("0")
```

`conciliar_pago_fifo` usará `select_for_update`, ordenará por aniversario, aplicará contra `dias_generados - dias_pagados`, exigirá saldo suficiente y marcará el pago conciliado dentro de `transaction.atomic()`. El segundo intento devolverá aplicaciones existentes sin duplicar.

- [ ] **Step 3: Probar rollback por excedente y reintento idempotente**

- [ ] **Step 4: Ejecutar y commit por Claude**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test rrhh.tests_vacaciones_nomina --keepdb
git add rrhh/services_vacaciones_nomina.py rrhh/tests_vacaciones_nomina.py
git commit -m "feat(rrhh): conciliar pagos vacacionales por antiguedad"
```

### Task 5: Prima y terminación sin goce

- [ ] **Step 1: Probar prima independiente**

```python
def test_prima_no_altera_dias_pagados_ni_gozados(self):
    conciliar_prima(self.pago_prima, actor=self.rrhh)
    self.assertFalse(self.pago_prima.aplicaciones.exists())
    self.assertEqual(saldo_periodo_vacacional(self.periodo_2025).gozado, Decimal("0"))
```

- [ ] **Step 2: Probar terminación sin descanso futuro**

```python
def test_terminacion_no_crea_aplicaciones_goce(self):
    conciliar_pago_terminacion(self.pago_finiquito, actor=self.rrhh)
    self.assertFalse(
        AplicacionGoceVacaciones.objects.filter(periodo=self.periodo_2025).exists()
    )
```

- [ ] **Step 3: Implementar ramas explícitas**

`prima` conserva estado económico sin aplicaciones de días salvo contrato de base aprobado. `terminacion` usa únicamente aplicaciones económicas marcadas con origen de terminación y nunca llama servicios de goce.

- [ ] **Step 4: Ejecutar y commit por Claude**

```bash
git add rrhh/services_vacaciones_nomina.py rrhh/tests_vacaciones_nomina.py
git commit -m "feat(rrhh): separar prima y terminacion del goce"
```

### Task 6: Integrar importación y reemplazo

- [ ] **Step 1: Probar importación repetida y reemplazo**

Importar el mismo fixture dos veces debe conservar un pago vigente por concepto. En `replace=True`, el pago anterior queda `revertido`, deja de contar y el nuevo concepto genera evidencia distinta.

- [ ] **Step 2: Detectar después de persistir conceptos**

```python
conceptos = NominaConceptoLinea.objects.filter(
    linea__periodo=periodo
).select_related("linea__empleado")
for concepto in conceptos.iterator():
    detectar_pago_vacaciones(concepto)
```

La reversión se ejecutará antes de borrar líneas; no depender de `post_delete`.

- [ ] **Step 3: Ejecutar pruebas del importador**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test \
  rrhh.tests_vacaciones_nomina \
  rrhh.tests.RRHHViewsTests.test_nomina_importa_lista_raya_desde_web --keepdb
```

- [ ] **Step 4: Commit por Claude**

```bash
git add rrhh/services/lista_raya.py rrhh/services_vacaciones_nomina.py rrhh/tests_vacaciones_nomina.py
git commit -m "feat(rrhh): enlazar lista de raya con pagos vacacionales"
```

### Task 7: Bandeja, privacidad y cierre

- [ ] **Step 1: Probar permisos**

Empleado y jefe reciben 403; Capital Humano y superusuario reciben 200. La jefatura de vacaciones nunca recibe `importe`, salario o prima.

- [ ] **Step 2: Implementar bandeja restringida**

Mostrar empleado, quincena, concepto, valor, importe, motivo pendiente y propuesta FIFO. Permitir aceptar propuesta o distribuir manualmente con nota obligatoria.

- [ ] **Step 3: Aplicar contrato de acciones**

Usar `data-async-action`, misma lógica para JSON/HTML, toast global, botón `Procesando…`, prevención de doble envío, conservación de filtros/inputs y registro en `docs/ux/action-context-coverage.md`.

- [ ] **Step 4: Ejecutar suite y validar navegador**

```bash
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py check
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py migrate --check
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py showmigrations rrhh
APP_ENV=local ALLOW_INSECURE_LOCAL_SECRET_KEY=1 .venv/bin/python manage.py test \
  rrhh.tests_vacaciones_nomina rrhh.tests_vacaciones_saldos \
  rrhh.tests_asistencia_reglas rrhh.tests_prenomina --keepdb
git diff --check
git status --short --branch
```

Validar 403/200, consola, Network, reintento y ausencia de importes para jefatura.

- [ ] **Step 5: Claude revisa, despliega y valida una quincena real**

Tras merge, ejecutar `scripts/deploy_web_safe.sh` sin `git pull` previo. Confirmar idempotencia, permisos y que pagar no cambia el saldo de goce antes de limpiar rama/worktree.
