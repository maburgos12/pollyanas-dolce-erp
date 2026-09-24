# ISN Automatico Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Registrar el ISN real por su periodo fiscal, distribuirlo entre empleados segun remuneracion gravada y publicarlo una sola vez en Planeacion de personal y Presupuesto vs Real.

**Architecture:** Un servicio fiscal aislado extrae el periodo e importe desde el CFDI estatal, calcula la base gravada desde conceptos de nomina y persiste un expediente auditable con lineas por empleado. Los reportes consumen exclusivamente el expediente aplicado; Presupuesto vs Real incorpora una fuente automatica `ISN_CFDI` que protege capturas manuales y evita doble conteo.

**Tech Stack:** Django 5, PostgreSQL 16, `Decimal`, XML CFDI 4.0, pruebas `django.test`, comandos de gestion y despliegue Docker del ERP.

---

## File map

- Create `reportes/services_isn.py`: extraccion de CFDI, tarifa, clasificacion de base, prorrateo y aplicacion transaccional.
- Create `reportes/management/commands/materializar_isn.py`: simulacion/aplicacion operativa por periodo.
- Create `reportes/tests_isn.py`: pruebas unitarias e integrales del contrato ISN.
- Create `reportes/migrations/0052_expediente_isn.py`: expediente, distribucion e incorporacion del tipo de fuente.
- Modify `reportes/models.py`: modelos de expediente/distribucion y fuente `ISN_CFDI`.
- Modify `reportes/services_planeacion_personal.py`: consumir el expediente aplicado sin sumar otra vez el CFDI.
- Modify `reportes/services_presupuesto_real.py`: resolver `ISN_CFDI` como fuente corporativa.
- Modify `reportes/data/mapeo_rubros_fuentes.csv`: cambiar `Impuesto sobre Nomina` de manual a automatico.
- Modify `reportes/management/commands/seed_reglas_fuente_rubro.py`: aceptar y sincronizar la nueva fuente.
- Modify `reportes/admin.py`: consulta de expedientes y lineas, sin edicion destructiva.
- Modify `reportes/tests_planeacion_personal.py` y `reportes/tests_presupuesto_real.py`: cobertura de consumidores.

### Task 1: Fiscal math and deterministic allocation

**Files:**
- Create: `reportes/tests_isn.py`
- Create: `reportes/services_isn.py`

- [ ] **Step 1: Write failing tariff and allocation tests**

```python
from datetime import date
from decimal import Decimal as D
from django.test import SimpleTestCase

from reportes.services_isn import calcular_isn_sinaloa, prorratear_isn


class ISNMathTests(SimpleTestCase):
    def test_tarifa_progresiva_sinaloa(self):
        self.assertEqual(calcular_isn_sinaloa(D("500000.00")), D("12000.00"))
        self.assertEqual(calcular_isn_sinaloa(D("660307.70")), D("16168.00"))
        self.assertEqual(calcular_isn_sinaloa(D("707079.82")), D("17398.23"))
        self.assertEqual(calcular_isn_sinaloa(D("1000000.00")), D("25800.00"))

    def test_prorrateo_cierra_exactamente_a_centavos(self):
        result = prorratear_isn({3: D("1"), 1: D("1"), 2: D("1")}, D("10.00"))
        self.assertEqual(sum(result.values(), D("0")), D("10.00"))
        self.assertEqual(result, {1: D("3.34"), 2: D("3.33"), 3: D("3.33")})
```

- [ ] **Step 2: Run tests and verify RED**

Run: `python manage.py test reportes.tests_isn.ISNMathTests --keepdb`

Expected: import error because `reportes.services_isn` does not exist.

- [ ] **Step 3: Implement minimal pure functions**

```python
# reportes/services_isn.py
from decimal import Decimal, ROUND_HALF_UP

CENT = Decimal("0.01")
ZERO = Decimal("0")


def money(value: Decimal) -> Decimal:
    return Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def calcular_isn_sinaloa(base: Decimal) -> Decimal:
    base = max(ZERO, Decimal(base))
    if base <= Decimal("500000.00"):
        return money(base * Decimal("0.024"))
    if base <= Decimal("700000.00"):
        return money(Decimal("12000") + (base - Decimal("500000.01")) * Decimal("0.026"))
    if base <= Decimal("900000.00"):
        return money(Decimal("17200") + (base - Decimal("700000.01")) * Decimal("0.028"))
    return money(Decimal("22800") + (base - Decimal("900000.01")) * Decimal("0.03"))


def prorratear_isn(bases: dict[int, Decimal], total: Decimal) -> dict[int, Decimal]:
    bases = {key: max(ZERO, Decimal(value)) for key, value in bases.items()}
    denominator = sum(bases.values(), ZERO)
    if denominator <= ZERO:
        raise ValueError("La base gravada total debe ser positiva.")
    exact = {key: Decimal(total) * value / denominator for key, value in bases.items()}
    rounded = {key: value.quantize(CENT, rounding=ROUND_HALF_UP) for key, value in exact.items()}
    difference = money(Decimal(total) - sum(rounded.values(), ZERO))
    cents = int(abs(difference / CENT))
    direction = CENT if difference > ZERO else -CENT
    order = sorted(exact, key=lambda key: (-(exact[key] - rounded[key]) * (1 if direction > 0 else -1), key))
    for key in order[:cents]:
        rounded[key] += direction
    return rounded
```

- [ ] **Step 4: Run tests and verify GREEN**

Run: `python manage.py test reportes.tests_isn.ISNMathTests --keepdb`

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add reportes/services_isn.py reportes/tests_isn.py
git commit -m "feat(reportes): calcular y prorratear ISN Sinaloa"
```

### Task 2: Persist auditable ISN dossiers

**Files:**
- Modify: `reportes/models.py`
- Create: `reportes/migrations/0052_expediente_isn.py`
- Modify: `reportes/tests_isn.py`

- [ ] **Step 1: Write failing model-contract tests**

```python
from django.db import IntegrityError, transaction
from django.test import TestCase
from reportes.models import ExpedienteISN, DistribucionISNEmpleado


class ISNModelTests(TestCase):
    def test_solo_hay_un_expediente_aplicado_por_periodo(self):
        ExpedienteISN.objects.create(periodo=date(2026, 8, 1), revision=1,
            uuid="A", importe_pagado=D("100"), base_gravada_calculada=D("4000"), estado="APLICADO")
        with self.assertRaises(IntegrityError), transaction.atomic():
            ExpedienteISN.objects.create(periodo=date(2026, 8, 1), revision=2,
                uuid="B", importe_pagado=D("100"), base_gravada_calculada=D("4000"), estado="APLICADO")
```

- [ ] **Step 2: Run test and verify RED**

Run: `python manage.py test reportes.tests_isn.ISNModelTests --keepdb`

Expected: import error for missing models.

- [ ] **Step 3: Add models and source choice**

Add `ReglaFuenteRubro.FUENTE_ISN_CFDI = "ISN_CFDI"` to `FUENTE_CHOICES`, then add:

```python
class ExpedienteISN(models.Model):
    ESTADO_VALIDO = "VALIDO"
    ESTADO_APLICADO = "APLICADO"
    ESTADO_REEMPLAZADO = "REEMPLAZADO"
    ESTADO_DISCREPANCIA = "DISCREPANCIA"
    ESTADO_CHOICES = [(value, value.title()) for value in (
        ESTADO_VALIDO, ESTADO_APLICADO, ESTADO_REEMPLAZADO, ESTADO_DISCREPANCIA)]

    periodo = models.DateField(db_index=True)
    revision = models.PositiveSmallIntegerField(default=1)
    uuid = models.CharField(max_length=36, unique=True)
    cfdi = models.OneToOneField("sat_client.CfdiDescargado", on_delete=models.PROTECT,
        related_name="expediente_isn")
    importe_pagado = models.DecimalField(max_digits=14, decimal_places=2)
    base_gravada_calculada = models.DecimalField(max_digits=14, decimal_places=2)
    base_declarada = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    estado = models.CharField(max_length=16, choices=ESTADO_CHOICES)
    aplicado_por = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.PROTECT, related_name="expedientes_isn_aplicados")
    creado_en = models.DateTimeField(default=timezone.now)
    aplicado_en = models.DateTimeField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["periodo", "revision"], name="uniq_isn_periodo_revision"),
            models.UniqueConstraint(fields=["periodo"], condition=models.Q(estado="APLICADO"),
                                    name="uniq_isn_aplicado_periodo"),
        ]


class DistribucionISNEmpleado(models.Model):
    expediente = models.ForeignKey(ExpedienteISN, on_delete=models.PROTECT, related_name="distribuciones")
    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.PROTECT, related_name="distribuciones_isn")
    base_gravada = models.DecimalField(max_digits=14, decimal_places=2)
    monto_isn = models.DecimalField(max_digits=14, decimal_places=2)
    area_codigo = models.CharField(max_length=50)
    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.PROTECT, related_name="distribuciones_isn")

    class Meta:
        constraints = [models.UniqueConstraint(fields=["expediente", "empleado"], name="uniq_isn_expediente_empleado")]
```

- [ ] **Step 4: Generate and inspect migration**

Run: `python manage.py makemigrations reportes --name expediente_isn`

Expected: `0052_expediente_isn.py` with dependencies on `reportes.0051`, `rrhh`, `core`, `sat_client.0004` and the configured user model.

- [ ] **Step 5: Apply migration and verify GREEN**

Run: `python manage.py migrate && python manage.py test reportes.tests_isn.ISNModelTests --keepdb`

Expected: migration applies and model tests pass.

- [ ] **Step 6: Commit**

```bash
git add reportes/models.py reportes/migrations/0052_expediente_isn.py reportes/tests_isn.py
git commit -m "feat(reportes): persistir expedientes y distribucion de ISN"
```

### Task 3: Parse CFDI period and calculate taxable payroll

**Files:**
- Modify: `reportes/services_isn.py`
- Modify: `reportes/tests_isn.py`

- [ ] **Step 1: Write failing parsing and taxable-base tests**

Create payroll concepts for codes `1`, `20`, `22`, `24`, `26`, and `32`; assert that salary is taxable, vacation premium/indemnity/pantry are excluded, and code `24` excludes at most `30 * 117.31` per employee for 2026. Add a CFDI with issue date in September and `NoIdentificacion="202608 2-003"`; assert the extracted period is August and amount is the matching concept import.

- [ ] **Step 2: Run tests and verify RED**

Run: `python manage.py test reportes.tests_isn.ISNSourceTests --keepdb`

Expected: missing `extraer_isn_cfdi` and `bases_gravadas_empleados`.

- [ ] **Step 3: Implement strict source and base rules**

```python
RFC_SINALOA = "GES8101015I7"
RFC_EMPRESA = "GEF211230KR2"
CODIGOS_EXENTOS_COMPLETOS = {"20", "22", "26", "32"}
UMA_DIARIA = {2026: Decimal("117.31")}


def extraer_isn_cfdi(cfdi):
    if cfdi.rfc_emisor != RFC_SINALOA or cfdi.rfc_receptor != RFC_EMPRESA:
        raise ValueError("El CFDI no corresponde al ISN de la empresa.")
    if cfdi.estatus.lower() != "vigente" or cfdi.tipo_cfdi != "recibido" or cfdi.tipo_comprobante != "I":
        raise ValueError("El CFDI no esta vigente como ingreso recibido.")
    root = ET.fromstring((cfdi.xml_raw or "").lstrip("\ufeff"))
    matches = []
    for concept in root.findall(".//" + CFDI_NS + "Concepto"):
        if "nomina" not in concept.attrib.get("Descripcion", "").lower():
            continue
        period_match = re.match(r"(\d{4})(\d{2})\b", concept.attrib.get("NoIdentificacion", ""))
        if period_match:
            matches.append((date(int(period_match[1]), int(period_match[2]), 1), Decimal(concept.attrib["Importe"])))
    periods = {period for period, _ in matches}
    if len(periods) != 1 or not matches:
        raise ValueError("Periodo fiscal de ISN no inequivoco.")
    return periods.pop(), money(sum((amount for _, amount in matches), ZERO))
```

Implement `bases_gravadas_empleados(periodo)` using only closed/paid payroll periods ending in the requested month. Group `NominaConceptoLinea` by employee, subtract the explicit full exemptions, cap code `24` at `30 * UMA_DIARIA[year]`, reject missing UMA years, and reject any employee without `sucursal_ref` or `departamento`.

- [ ] **Step 4: Run source tests and verify GREEN**

Run: `python manage.py test reportes.tests_isn.ISNSourceTests --keepdb`

Expected: all source/base tests pass.

- [ ] **Step 5: Commit**

```bash
git add reportes/services_isn.py reportes/tests_isn.py
git commit -m "feat(reportes): obtener base gravada de ISN desde nomina"
```

### Task 4: Materialize dossier transactionally and idempotently

**Files:**
- Modify: `reportes/services_isn.py`
- Create: `reportes/management/commands/materializar_isn.py`
- Modify: `reportes/tests_isn.py`

- [ ] **Step 1: Write failing application tests**

Assert: dry-run writes nothing; apply creates one dossier and one line per employee; line amounts sum to the CFDI; rerunning keeps one dossier and the same lines; a changed/corrective CFDI creates a new revision and replaces the previous applied dossier; incomplete payroll raises before writes.

- [ ] **Step 2: Run tests and verify RED**

Run: `python manage.py test reportes.tests_isn.ISNApplicationTests --keepdb`

Expected: missing `preparar_expediente_isn` / `aplicar_expediente_isn`.

- [ ] **Step 3: Implement prepare/apply services**

Use a frozen preview dataclass containing CFDI, period, total, base total and immutable employee rows. `aplicar_expediente_isn` must use `transaction.atomic()` and `select_for_update()` on existing dossiers for the period. It must return the existing dossier for the same UUID, otherwise mark the previous applied revision as replaced, create the next revision, bulk-create lines and verify both sums before setting `APLICADO`.

- [ ] **Step 4: Add command contract**

```python
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--periodo", required=True, help="Mes fiscal YYYY-MM")
        parser.add_argument("--uuid", help="UUID exacto; si se omite debe existir un unico CFDI candidato")
        parser.add_argument("--base-declarada", type=Decimal)
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        preview = preparar_expediente_isn(parse_period(options["periodo"]), uuid=options["uuid"])
        self.stdout.write(preview.render())
        if options["apply"]:
            expediente = aplicar_expediente_isn(preview, base_declarada=options["base_declarada"])
            self.stdout.write(self.style.SUCCESS(f"APLICADO expediente={expediente.pk}"))
        else:
            self.stdout.write("DRY-RUN: sin cambios")
```

- [ ] **Step 5: Run application tests and verify GREEN**

Run: `python manage.py test reportes.tests_isn.ISNApplicationTests --keepdb`

Expected: all application tests pass.

- [ ] **Step 6: Commit**

```bash
git add reportes/services_isn.py reportes/management/commands/materializar_isn.py reportes/tests_isn.py
git commit -m "feat(reportes): materializar ISN con revision auditada"
```

### Task 5: Feed Planeacion and Presupuesto vs Real

**Files:**
- Modify: `reportes/services_planeacion_personal.py`
- Modify: `reportes/services_presupuesto_real.py`
- Modify: `reportes/data/mapeo_rubros_fuentes.csv`
- Modify: `reportes/management/commands/seed_reglas_fuente_rubro.py`
- Modify: `reportes/tests_planeacion_personal.py`
- Modify: `reportes/tests_presupuesto_real.py`

- [ ] **Step 1: Write failing consumer tests**

In Planeacion, create both the source CFDI and an applied dossier and assert August ISN is counted once with dossier provenance. In Presupuesto vs Real, create an `ISN_CFDI` rule and assert the corporate row becomes `AUTO:ISN_CFDI` with the applied total; create a `MANUAL:*` value and assert it remains protected.

- [ ] **Step 2: Run tests and verify RED**

Run: `python manage.py test reportes.tests_planeacion_personal reportes.tests_presupuesto_real --keepdb`

Expected: consumer assertions fail because dossiers/source type are not consumed.

- [ ] **Step 3: Prefer applied dossier in Planeacion**

Load applied `ExpedienteISN` rows for the report range before raw invoices. For periods with an applied dossier, set `row["isn"]` from `importe_pagado`, add one source record and ignore the matching raw CFDI. Keep raw CFDI fallback only when no dossier exists, with coverage but not reconciled status.

- [ ] **Step 4: Add budget consolidation source**

Extend `_monto_regla`:

```python
if regla.tipo_fuente == ReglaFuenteRubro.FUENTE_ISN_CFDI:
    if "isn" not in indices:
        indices["isn"] = {
            row["periodo"]: row["importe_pagado"]
            for row in ExpedienteISN.objects.filter(estado=ExpedienteISN.ESTADO_APLICADO)
                .values("periodo", "importe_pagado")
        }
    return (indices["isn"].get(periodo, Decimal("0")), periodo in indices["isn"])
```

Change the canonical CSV row to `administracion,Impuesto sobre Nomina,ISN_CFDI` and keep the rule corporate (no branch filters).

- [ ] **Step 5: Run consumer tests and verify GREEN**

Run: `python manage.py test reportes.tests_planeacion_personal reportes.tests_presupuesto_real --keepdb`

Expected: all tests pass and no double count occurs.

- [ ] **Step 6: Commit**

```bash
git add reportes/services_planeacion_personal.py reportes/services_presupuesto_real.py reportes/data/mapeo_rubros_fuentes.csv reportes/management/commands/seed_reglas_fuente_rubro.py reportes/tests_planeacion_personal.py reportes/tests_presupuesto_real.py
git commit -m "feat(reportes): publicar ISN real sin doble conteo"
```

### Task 6: Read-only administration and complete local verification

**Files:**
- Modify: `reportes/admin.py`
- Modify: `reportes/tests_isn.py`

- [ ] **Step 1: Add read-only admin registrations**

Register `ExpedienteISN` and `DistribucionISNEmpleado`; expose period, revision, state, paid amount, calculated/declared base, employee, branch and area. Mark fiscal amounts, UUID, source and distributions read-only; disable deletion in admin.

- [ ] **Step 2: Verify migration discipline**

Run: `python manage.py makemigrations --check --dry-run && python manage.py migrate --check`

Expected: no uncommitted model changes and no pending migrations.

- [ ] **Step 3: Run focused and module tests**

Run: `python manage.py test reportes.tests_isn reportes.tests_planeacion_personal reportes.tests_presupuesto_real --keepdb`

Expected: all tests pass, zero failures.

- [ ] **Step 4: Run Django check**

Run: `python manage.py check`

Expected: `System check identified no issues`.

- [ ] **Step 5: Commit**

```bash
git add reportes/admin.py reportes/tests_isn.py
git commit -m "feat(reportes): exponer auditoria de ISN en administracion"
```

### Task 7: PR, deployment and production backfill

**Files:**
- No new source files; operational execution only.

- [ ] **Step 1: Review branch purity**

Run: `git status --short --branch`, `git log --oneline --decorate -8`, `git diff origin/main..HEAD --stat`, `git worktree list`, and `git worktree prune --dry-run`.

Expected: clean branch with only ISN design, plan, source, tests and migration.

- [ ] **Step 2: Push and open draft PR**

Push `codex/reportes-isn-distribucion-automatica`, create a draft PR containing functional summary, main files, tests, migration and production validation plan, then wait for required CI.

- [ ] **Step 3: Merge and deploy through the official script**

After CI passes and diff review is clean, merge to `main`. On the VPS run only:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

Expected: deploy completes, migration `0052` is applied, `migrate --check` and `manage.py check` are clean.

- [ ] **Step 4: Ensure the CFDI exists from SAT**

Fresh-read UUID `22339E4C-AC86-47DF-A874-434F6B7CFC69`. If absent after the nightly sync, run the existing controlled received-CFDI download for September and re-read; do not create a synthetic CFDI from the PDF.

- [ ] **Step 5: Dry-run and apply August**

```bash
python manage.py materializar_isn --periodo 2026-08 --uuid 22339E4C-AC86-47DF-A874-434F6B7CFC69
python manage.py materializar_isn --periodo 2026-08 --uuid 22339E4C-AC86-47DF-A874-434F6B7CFC69 --apply
python manage.py consolidar_presupuesto_real --periodo 2026-08 --areas administracion
```

Expected: one applied dossier, distributions sum `$16,168.00`, no unmapped employees, one corporate budget line updated.

- [ ] **Step 6: Fresh production verification**

Verify by fresh database read and authenticated browser:

- Planeacion de personal August ISN = `$16,168.00`;
- Presupuesto vs Real August `Impuesto sobre nomina`: budget `$14,890.00`, real `$16,168.00`, variance `$1,278.00`;
- sum of employee/area/branch allocation = `$16,168.00`;
- UUID occurs once and the corporate P&L contains no duplicate ISN.

- [ ] **Step 7: Close the task workspace**

Run `scripts/task_workspace_audit.sh`, then `scripts/task_workspace_close.sh --state merged` for this registered task after production validation and remote branch cleanup.
