# Performance Guardrails Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Impedir que las páginas críticas del ERP regresen a patrones N+1 o consultas multiplicativas y detectar degradaciones reales de latencia en producción antes de que afecten la operación.

**Architecture:** La protección tendrá dos señales distintas. CI bloqueará regresiones deterministas de consultas mediante pruebas etiquetadas y presupuestos por vista; producción observará tráfico real con el middleware existente, muestreo configurable y logs estructurados, sin escribir tablas ni agregar dependencias. Los tiempos de CI serán informativos porque varían entre runners; los conteos de consultas y su crecimiento sí serán bloqueantes.

**Tech Stack:** Django 5, PostgreSQL, `CaptureQueriesContext`, Django test tags, middleware `execute_wrapper`, GitHub Actions, logging estructurado key-value.

---

## Scope and non-goals

- Primera página protegida: `logistica:rutas`.
- Segunda ola, en PR independientes: detalle de ruta, Producción, Inventario, Reportes y Bonos.
- No crear modelos, migraciones, dashboards nuevos ni tablas de telemetría.
- No instalar Sentry, Prometheus u OpenTelemetry en esta fase.
- No hacer fallar CI por milisegundos de un runner compartido.
- No registrar parámetros, cookies, cuerpos, nombres de usuarios ni SQL completo.
- La observabilidad de producción debe poder apagarse con una variable y no cambiar respuestas.

## File map

- Create: `core/performance.py` — tipos, normalización de rutas y decisión de muestreo; no depende de tests.
- Modify: `core/middleware.py` — recopila conteo SQL, tiempo SQL y consultas lentas; emite un solo evento resumen por request muestreado.
- Modify: `config/settings.py` — configuración tipada y defaults seguros.
- Create: `core/tests_performance.py` — pruebas unitarias del muestreo, privacidad y logging.
- Modify: `logistica/tests.py` — etiqueta y endurece los dos guardrails ya existentes para Rutas.
- Modify: `.github/workflows/ci.yml` — agrega un paso bloqueante y rápido para `--tag=performance` antes de la suite histórica tolerada.
- Create: `docs/operations/performance-guardrails.md` — SLO, operación, rollback y alta de nuevas páginas.

### Task 1: Central performance policy

**Files:**
- Create: `core/performance.py`
- Create: `core/tests_performance.py`

- [ ] **Step 1: Write failing tests for path normalization and sampling**

```python
from django.test import SimpleTestCase, override_settings

from core.performance import normalize_path, should_measure_request


class PerformancePolicyTests(SimpleTestCase):
    def test_normalize_path_replaces_numeric_ids(self):
        self.assertEqual(normalize_path("/logistica/rutas/123/"), "/logistica/rutas/:id/")

    @override_settings(ERP_PERF_CRITICAL_PATHS=("/logistica/rutas/",))
    def test_critical_path_is_always_measured(self):
        self.assertTrue(should_measure_request("/logistica/rutas/", random_value=0.99))

    @override_settings(ERP_PERF_SAMPLE_RATE=0.10, ERP_PERF_CRITICAL_PATHS=())
    def test_noncritical_path_uses_sample_rate(self):
        self.assertTrue(should_measure_request("/core/", random_value=0.09))
        self.assertFalse(should_measure_request("/core/", random_value=0.10))
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
python manage.py test core.tests_performance.PerformancePolicyTests
```

Expected: `ImportError` because `core.performance` does not exist.

- [ ] **Step 3: Implement the small, deterministic policy module**

```python
import re
from random import random

from django.conf import settings

_NUMERIC_SEGMENT = re.compile(r"(?<=/)\d+(?=/|$)")


def normalize_path(path: str) -> str:
    return _NUMERIC_SEGMENT.sub(":id", path)


def should_measure_request(path: str, *, random_value: float | None = None) -> bool:
    normalized = normalize_path(path)
    critical = set(getattr(settings, "ERP_PERF_CRITICAL_PATHS", ()))
    if normalized in critical:
        return True
    sample_rate = float(getattr(settings, "ERP_PERF_SAMPLE_RATE", 0.0))
    sample = random() if random_value is None else random_value
    return sample < sample_rate
```

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run: `python manage.py test core.tests_performance.PerformancePolicyTests`

Expected: all tests pass.

- [ ] **Step 5: Commit the policy unit**

```bash
git add core/performance.py core/tests_performance.py
git commit -m "feat(core): definir política de muestreo de rendimiento"
```

### Task 2: Safe production request metrics

**Files:**
- Modify: `core/middleware.py:23-52,245-292`
- Modify: `config/settings.py:466-468`
- Modify: `core/tests_performance.py`

- [ ] **Step 1: Write failing middleware tests**

Create tests that use `RequestFactory`, one real `SELECT 1`, `assertLogs("erp.performance")`, and `override_settings`. Assert exactly one `request_performance` summary containing normalized `path`, `status`, `total_ms`, `query_count`, `sql_ms`, and `slow_query_count`. Also assert the message does not contain the query string, query parameters, cookies, authorization headers, or username.

```python
@override_settings(
    ERP_PERF_LOGGING_ENABLED=True,
    ERP_PERF_SAMPLE_RATE=1.0,
    ERP_PERF_CRITICAL_PATHS=(),
    ERP_SLOW_ENDPOINT_MS=0,
    ERP_SLOW_QUERY_MS=0,
)
def test_middleware_logs_one_private_summary(self):
    request = RequestFactory().get("/logistica/rutas/42/?token=secret")

    def view(_request):
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        return HttpResponse("ok")

    with self.assertLogs("erp.performance", level="WARNING") as logs:
        response = PerformanceLoggingMiddleware(view)(request)

    self.assertEqual(response.status_code, 200)
    summary = "\n".join(logs.output)
    self.assertIn("event=request_performance", summary)
    self.assertIn("path=/logistica/rutas/:id/", summary)
    self.assertIn("query_count=1", summary)
    self.assertNotIn("secret", summary)
    self.assertNotIn("SELECT 1", summary)
```

- [ ] **Step 2: Run the middleware tests and verify RED**

Run: `python manage.py test core.tests_performance`

Expected: the current middleware emits separate events and does not expose all summary fields.

- [ ] **Step 3: Extend `_ConnectionTimingWrapper` to aggregate, not retain all SQL**

Use a request-local dataclass with `query_count`, `sql_ms`, `slow_query_count`, and at most three sanitized slow-query fingerprints. A fingerprint contains only operation and table names after truncation; never values or full SQL. Increment counters in `finally` so failed queries are counted. Preserve response and exception behavior.

- [ ] **Step 4: Emit one summary event only when sampled or over threshold**

The event contract is:

```text
event=request_performance path=/logistica/rutas/ method=GET status=200 total_ms=687.10 query_count=28 sql_ms=566.00 slow_query_count=1
```

Do not log query strings. If `get_response` raises, record `status=500` and re-raise the original exception unchanged.

- [ ] **Step 5: Add safe settings**

```python
ERP_PERF_LOGGING_ENABLED = env_bool("ERP_PERF_LOGGING_ENABLED", default=False)
ERP_PERF_SAMPLE_RATE = env_float("ERP_PERF_SAMPLE_RATE", 0.05)
ERP_PERF_CRITICAL_PATHS = ("/logistica/rutas/",)
ERP_SLOW_ENDPOINT_MS = env_int("ERP_SLOW_ENDPOINT_MS", 2000)
ERP_SLOW_QUERY_MS = env_int("ERP_SLOW_QUERY_MS", 500)
```

Validate `ERP_PERF_SAMPLE_RATE` is between `0.0` and `1.0` at startup. Do not enable the middleware in production in this commit; enabling requires Mauricio's explicit approval to modify the VPS environment.

- [ ] **Step 6: Run tests and verify GREEN**

Run:

```bash
python manage.py test core.tests_performance
python manage.py check
```

Expected: all tests pass and system check reports zero issues.

- [ ] **Step 7: Commit observability separately**

```bash
git add core/middleware.py core/performance.py core/tests_performance.py config/settings.py
git commit -m "feat(core): resumir métricas privadas de rendimiento por request"
```

### Task 3: Blocking CI query guardrail

**Files:**
- Modify: `logistica/tests.py:2912-2982`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Tag the existing Routes tests**

Import `tag` from `django.test` and decorate both regression tests:

```python
@tag("performance")
def test_rutas_view_no_crece_queries_por_fila(self):
    ...

@tag("performance")
def test_rutas_view_no_multiplica_paradas_por_lineas_point(self):
    ...
```

Add an absolute ceiling to the five-row request after measuring the stable baseline on PostgreSQL. Set the initial budget to baseline plus 20%, rounded up; do not copy the production count blindly because auth/test middleware differs.

- [ ] **Step 2: Verify the performance tag selects only the intended tests**

Run: `python manage.py test --tag=performance --verbosity=2`

Expected: exactly the registered performance tests run and pass.

- [ ] **Step 3: Add a required CI step before the tolerated legacy suite**

```yaml
      - name: Enforce critical page query budgets
        run: python manage.py test --tag=performance --verbosity=2

      - name: Run legacy full test suite
        continue-on-error: true
        run: python manage.py test
```

The new step must not use `continue-on-error`; a query regression must block merge even while the historical suite remains tolerated.

- [ ] **Step 4: Validate the workflow and focused suite locally**

Run:

```bash
python manage.py test --tag=performance
python manage.py test logistica.tests.LogisticaViewsTests
python manage.py check
python manage.py migrate --check
```

Expected: all commands succeed; no migrations are generated.

- [ ] **Step 5: Commit the CI gate**

```bash
git add logistica/tests.py .github/workflows/ci.yml
git commit -m "ci: bloquear regresiones de consultas en páginas críticas"
```

### Task 4: Operating contract and rollout

**Files:**
- Create: `docs/operations/performance-guardrails.md`

- [ ] **Step 1: Document SLO and ownership**

Record these initial contracts:

| Signal | CI | Production | Action |
|---|---:|---:|---|
| Query growth per added row | `<= 2` | n/a | Block PR |
| Absolute query count | Baseline + 20% | Observe | Block PR |
| Warm request p95 | n/a | `< 2 s` | Investigate after 3 breaches in 15 min |
| Individual SQL | n/a | `< 500 ms` | Capture fingerprint and `EXPLAIN ANALYZE` read-only |
| HTTP 5xx | Existing behavior | Any new cluster | Roll back if release-related |

Explain that CI time is not an SLO and cold Gunicorn startup is reported separately from warm traffic.

- [ ] **Step 2: Document how to enroll another page**

Require: owner, URL name, representative fixtures, one-vs-many growth test, absolute PostgreSQL query budget, visible-output invariant, production threshold, and rollback plan. Each new module gets its own PR.

- [ ] **Step 3: Document rollout and rollback**

Rollout:

1. Deploy code with `ERP_PERF_LOGGING_ENABLED=0`.
2. Verify health and response parity.
3. Obtain Mauricio's explicit approval for the environment change.
4. Enable with `ERP_PERF_SAMPLE_RATE=0.05` and restart only the required service.
5. Observe CPU, memory, endpoint p95, query counts and logs for 24 hours.
6. Increase sampling only if evidence requires it.

Rollback: set `ERP_PERF_LOGGING_ENABLED=0`; the CI query tests remain active because they add no production overhead.

- [ ] **Step 4: Commit documentation**

```bash
git add docs/operations/performance-guardrails.md
git commit -m "docs: definir operación de guardrails de rendimiento"
```

### Task 5: Final verification, PR and production proof

**Files:**
- Verify all files from Tasks 1-4.

- [ ] **Step 1: Review scope and repository state**

Run:

```bash
git status --short --branch
git diff origin/main..HEAD --stat
git log --oneline --decorate -5
git worktree list
```

Expected: only the seven planned files changed; no model, migration, template, static, PWA or data file.

- [ ] **Step 2: Run the complete focused verification**

```bash
python manage.py test core.tests_performance
python manage.py test --tag=performance
python manage.py test logistica.tests.LogisticaViewsTests
python manage.py check
python manage.py migrate --check
```

- [ ] **Step 3: Open one PR and require the new CI step**

PR description must include baseline query counts, growth result, middleware overhead measured with logging disabled/enabled, privacy assertions, exact files, and rollback switch.

- [ ] **Step 4: Merge and deploy through the safe script**

On VPS, do not run `git pull` manually:

```bash
cd /opt/pastelerias-erp
bash scripts/deploy_web_safe.sh
```

- [ ] **Step 5: Validate production before enabling telemetry**

Verify commit, containers, health, logs and `/logistica/rutas/` response parity. Measure one cold and two warm authenticated renders; record status, bytes, query count, SQL time and total time. Expected: no regression from the post-fix baseline of 28 warm queries and approximately 0.69 seconds, allowing normal data variance.

- [ ] **Step 6: Enable telemetry only after explicit environment approval**

If approved, back up the current environment file, change only `ERP_PERF_LOGGING_ENABLED` and `ERP_PERF_SAMPLE_RATE`, restart as required, and verify sampled events contain no sensitive values. If approval is absent, leave telemetry disabled and close the code/CI portion as complete with this limitation documented.

- [ ] **Step 7: Clean merged branch and worktree after production validation**

Run `git worktree prune --dry-run`, then remove only this task's worktree and local/remote branch, followed by `git fetch --prune origin`.

## Acceptance criteria

- A deliberate N+1 added to `logistica:rutas` makes the required CI step fail.
- Normal CI does not fail because a runner takes longer in milliseconds.
- Middleware disabled adds no SQL wrapper and emits no performance logs.
- Middleware enabled samples noncritical traffic, always measures whitelisted critical paths, and does not alter responses or exceptions.
- Logs never contain query parameters, cookies, auth headers, usernames, SQL values or full SQL.
- No migrations, data writes, template changes or service-worker bumps are needed.
- Production telemetry has an immediate configuration rollback.
