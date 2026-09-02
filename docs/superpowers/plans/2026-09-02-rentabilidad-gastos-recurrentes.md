# Rentabilidad: integración mensual de fuentes ERP — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for bounded service implementation and independent reviews; execute the approved contract below with test-driven-development. Do not commit, merge or deploy without the final responsible reviewer required by AGENTS.md.

**Goal:** Present auditable recurring expenses and personnel costs per branch, preserving Matriz's existing rent allocation and reporting incomplete sources instead of a false break-even.

**Architecture:** Add explicit service-month coverage to Reportes expenses, retain original obligation/payment amounts, and provide one read-only monthly result consumed by Rentabilidad recalculation and views. Keep expense-source resolution, personnel-source resolution, and consumer adaptation separately testable. Never write source data from GET.

**Tech Stack:** Existing Django 5, Decimal, PostgreSQL 16, Django templates. No new dependency or RRHH schema.

## Contract and verification environment

Approved specification: `docs/superpowers/specs/2026-09-02-rentabilidad-gastos-recurrentes-design.md`.
Worktree vigente `/Users/mauricioburgos/Downloads/codex_worktrees/rentabilidad_gastos_publicacion`, rama `codex/rentabilidad-gastos-publicacion`, base `1bb263b579566ce228cdca79e6a467df61b569b4`. Codex asume la revisión y eventual publicación conforme al protocolo actualizado por PR #1247; no se requiere revisión de Claude. Esta preparación no incluye commit, PR de Rentabilidad ni despliegue.

Antecedente conservado: la implementación se verificó en `/Users/mauricioburgos/Downloads/codex_worktrees/rentabilidad_gastos_recurrentes_actualizada`, rama `codex/rentabilidad-gastos-recurrentes-actualizada`, base `2199ba2f690f303c26e920f8394163d736a98485`, después del cierre Point. Ese origen permanece intacto como respaldo; no integrar ambas copias. En la nueva base se conserva el cambio concurrente de `reportes/tests_operating_finance.py`.

Test command prefix:
`docker compose -p erp_rentabilidad_publicacion -f /tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml run --rm check manage.py`
Before manage.py: `docker compose -p erp_rentabilidad_publicacion -f /tmp/erp-rentabilidad-publicacion-QEaa2A/compose.yml exec -T db pg_isready -U postgres -d pastelerias_erp`.

Monthly service interface: `leer_gastos_mensuales(periodo)` returns a dictionary with `filas` and `pendientes`. Each row identifies `origen`, `registro_id`, `clave`, `sucursal_id`, `area`, `familia`, `concepto`, `monto_original`, `monto_mensual`, `cobertura_inicio`, `cobertura_fin`, `regla_id`, `porcentaje`, `soporte`, `estado`, and `detalle`. Monetary fields are Decimal, dates are date, IDs are integers or None. Pending rows retain source IDs and a human-readable reason. No employee name or individual salary may reach templates.

Canonical families: renta, electricidad, telefono, sistemas, alarmas, mantenimiento, otros, nomina, cargas_patronales. Unknown economic classification is pending, not automatically other fixed expense. Existing category codes and rubro metadata are authoritative; never classify every arbitrary expense as fixed by exclusion. No hardcoded monetary production data.

## Task 1 — Explicit coverage and billing cycles (controller)

Files: `reportes/models.py`, a new additive Reportes migration, `reportes/services_gastos_compromisos.py`, existing expense form/view/template, `reportes/tests_cobertura_gastos.py`.

- [x] RED: create tests asserting paired first-of-month coverage, rejection of reversed/partial/nonmonthly dates, and backward compatibility of null coverage.
- [x] Run `test reportes.tests_cobertura_gastos --keepdb`; observe missing-field failure.
- [x] Add nullable coverage dates to GastoOperativoMensual and model validation/database constraints. Add periodicidad_meses choices 1/2 default1 to GastoRecurrenteVersion, validated in service and database. No historical data backfill.
- [x] RED: add recurring bimonthly tests: January invoice creates January–February coverage, February cannot create another obligation, March can, original amount/payment schedule unchanged, repeated generation idempotent.
- [x] Implement period anchoring and propagate coverage into the linked expense atomically. Preserve version history and existing monthly behavior. Expose optional service-month fields and periodicity through existing capture flows only.
- [x] Run new tests and `test reportes.tests_gastos_compromisos --keepdb`; require green.

## Task 2 — Non-personnel monthly source reader (bounded implementer)

Files: `reportes/services_rentabilidad_gastos.py`, `reportes/tests_rentabilidad_gastos.py` only. Schema handled by controller; tests can create coverage fields when available.

- [x] RED: expense66210.12 shared using explicit20%/80% fixture -> branch13242.02, production remainder52968.10; already allocated13242.00 -> exactly13242.00, never add wholecomplex. Production rows never included as branch fixed cost.
- [x] RED: Decimal100.01 January–February ->50.00/50.01; payment in March does not change coverage. Null coverage stays recorded month. Known bimonthly without coverage is pending.
- [x] RED: linked obligation/expense counts once, canceled obligation excludes linked expense; CONTROL and automatic consolidated mirrors do not add cost; no budget/estimated amounts count as complete real.
- [x] RED: distributions respect branch inheritance, explicit center/type, percentage, effective dates; inconsistent/ambiguous mapping is pending. Preserve exact cents with deterministic residual allocation.
- [x] Implement read-only normalization against existing GastoOperativoMensual, ObligacionGasto and ReglaFuenteRubro semantics; output rows for identifiable recurring categories and explicit pending reasons. Include direct/manual monthly lines only when traceable and unambiguously distinct from underlying sources; use ORIGINAL version matching existing consumer, never sum versions. No inferred bank/invoice assignment.
- [x] RED/GREEN: missing source for applicable active rules/recurrent contracts produces pending, an existing payroll row cannot hide it. Unknown centers/source classification remain pending with identifiers.
- [x] Run `test reportes.tests_rentabilidad_gastos --keepdb`; inspect query writes (must be none). Spec review, fixes, then quality review before consumer integration.

## Task 3 — ERP personnel and shared monthly result (controller)

Files: `reportes/services_rentabilidad_personal.py`, `reportes/services_rentabilidad_mensual.py`, `reportes/tests_rentabilidad_personal.py`.

- [x] Inspect RRHH import semantics before selecting total versus concepts. RED: closed/paid payroll only, month of fecha_fin, branch/area isolation, draft excluded, imported NOMINA expense excluded, total/components/bonuses counted once, employee deductions excluded.
- [x] Verify historical assignment evidence. If not recoverable, show explicit pending/provisional attribution, never certify current employee location as historical fact. No source mutations or parallel personnel catalog.
- [x] RED: existing monthly AUTO:SIPARE employer lines counted once and not divided again; manual values preserved; missing IMSS or RCV makes personnel incomplete.
- [x] Implement aggregate-only personnel rows and source references. Reconcile concepts with total; a disagreement is explicit pending, not silently additive.
- [x] Assemble one result per branch: rows, pending issues, family totals, completeness and snapshot field mapping. Preserve previously validated absent family values in recalculation with a clear stale warning; never mark complete from payroll alone.
- [x] Run `test reportes.tests_rentabilidad_personal --keepdb` and source-reader tests. No writes allowed in reader.

## Task 4 — Recalculation and visible breakdown (controller)

Files: `rentabilidad/tasks_rentabilidad.py`, `rentabilidad/models_rentabilidad.py` if needed for in-memory guards (no new schema assumed), `rentabilidad/views_rentabilidad.py`, Rentabilidad templates, `rentabilidad/tests.py` and new `rentabilidad/tests_gastos_recurrentes.py`.

- [x] RED: task and GET use same monthly values; task twice is idempotent; temporarily missing sources preserve prior values but state SIN_DATOS; missing family makes break-even no-calculable even when payrollpositive.
- [x] Replace category-name sums and fixed nine-way corporate split with normalized ERP sources. Preserve recipe/resale/investment calculations outside this scope.
- [x] On GET refresh expense fields in memory, never persist; apply same incomplete-source state to snapshots and history. Ensure downstream profitability and break-even presentation does not treat partial totals as final.
- [x] Read design-routing skills, then add existing-style breakdown: family, monthly amount, original charge/coverage, origin, complete/partial/pending. No individual salaries, new permissions, unrelated redesign or graph.
- [x] Update dashboard expense panel to use same monthly result and visible completeness explanations. Preserve endpoint permission checks.
- [x] Run `test rentabilidad --keepdb` and assert rendered response has no INSERT/UPDATE/DELETE triggered by GET.

## Task 5 — Cross-source regression, review, and accountable handoff

- [x] Run PostgreSQL `check`, `migrate --check`, `makemigrations --check --dry-run` and all targeted Rentabilidad, presupuesto-real, gastos-compromisos and cedula-imss tests.
- [x] Independently review spec coverage then code quality, fix important findings and rerun affected tests.
- [x] Validate local authenticated browser, including monthly details, pending-state warnings, permissions, console and network; distinguish local fixtures from real source data.
- [x] Produce read-only source-coverage preview: January–April REAL expense records by center; May onward absent; recurring contracts and obligations empty. Detailed invoice-to-branch historical reconciliation and any bulkload remain outside this implementation and require reviewed evidence.
- [x] Review exact diff/status and record remaining production-data gaps. Hand off commit/PR/deploy decision to required reviewer. Production success requires official VPS deployment plus authenticated UI validation; do not claim it from local tests.
- [x] Historical lifecycle handoff to `claude_revision_final`, status `implemented_local_review_required`; preserved uncommitted work, prior backup/stash and isolated DB state without touching other tasks. Following PR #1247, Codex assumes review and eventual publication in the new worktree; this historical handoff is not a current prerequisite. Full evidence and outstanding production work are in `2026-09-02-rentabilidad-gastos-recurrentes-verificacion.md`.
