# AGENTS

## Director General Mode (Permanent)

Primary objective:
- Maximize business value, operational control, reliability, and auditability (enterprise/SAP-style).

Mandatory startup sequence for every run:
1. Read `.agent/skills/README.md`.
2. Read `.agent/skills/00-core/skill-erp-context/SKILL.md`.
3. Read `.agent/skills/00-core/skill-director-general-mode/SKILL.md`.
4. Run `./scripts/diagnose_erp_runtime_context.sh --strict` before any operational analysis, forecast, reconciliation, agent runtime review, or executive reporting.

Blocking rule for startup:
- If `./scripts/diagnose_erp_runtime_context.sh --strict` fails, do not continue with ERP data analysis or state-changing work until the database context is corrected.
- Do not assume a local PostgreSQL name such as `pollyana_db` is the live ERP just because Django connected successfully.
- Do not use SQLite snapshots as a fallback source for operational answers while the strict runtime diagnosis is red.

Execution rules:
- Do not assume unknown data. If a decision depends on missing inputs, list the missing data explicitly.
- Propose safe incremental steps with checklists and rollback notes.
- Prioritize: inventory accuracy, production shelf-life rules, reconciliation, audit trail, RBAC, and reporting consistency.
- Ensure macOS compatibility for local scripts/processes.
- Never claim that an event forecast fix is "applied" or "closed" unless the new values were actually recalculated, persisted, and verified in the ERP snapshot/state.
- Use Pollyana's Dolce real branch governance: 9 active operating branches, where `MATRIZDBG` is not a real branch and must be excluded from operational completeness, same-store, and commercial forecasts; `GUAMUCHIL` is a recent opening in March 2026 and must be treated as new-branch expansion, not mature same-store.
- Do not treat missing product-level sales rows as an automatic branch-day data failure. If the branch-day exists in Point daily indicators, or a successful official backfill exists for that branch/date even with `0` imported rows, or it is explained by a validated operational closure/network incident, count it as a valid zero instead of an extraction gap.
- For recurring operational control, use `./.venv/bin/python manage.py audit_commercial_forecast_loop --days-back 30 --write-report` as the daily loop audit before declaring the commercial pipeline healthy.

## Permanent Operating Order For ERP Runtime And Deployment

This operating order is mandatory for every future thread in this repository.

### 1. Default working mode

- Default to `Railway` as the primary operating environment for validation, smoke checks, and publication readiness.
- Treat the remote Railway environment as the main truth for application availability once a feature is already deployed there.
- Do not assume that `localhost` must be running unless the task explicitly requires local development, local debugging, or local database inspection.

### 2. Local environment policy

- Local work is allowed only when it adds real value:
  - editing backend/frontend code
  - running focused tests
  - inspecting migrations
  - validating behavior before a clean remote deploy
- For normal local work, start only the minimum stack:
  - `db`
  - `redis`
  - `web`
- Do not start `worker`, `beat`, legacy external chat stacks, browser automation containers, or other heavy support services unless the task strictly requires them.
- If the machine is hot or constrained, prefer remote verification over raising more local services.

### 3. Local startup checklist

Before claiming the local ERP is broken, always verify in this order:
1. whether Docker Desktop is running
2. whether port `8011` is listening
3. whether `db`, `redis`, and `web` are up
4. whether the configured local PostgreSQL database actually exists
5. whether pending migrations are blocking startup

Mandatory rule:
- if `localhost:8011` fails, do not assume a code regression first; verify runtime state, database existence, and migrations before diagnosing application logic.

### 4. Publication policy

- For online publication of the ERP and native chat, use `Railway` as the default target.
- Do not use any retired external chat stack as the publication target or runtime base for the ERP chat.
- Keep PostgreSQL as the only supported conversational and transactional database.
- Do not reintroduce SQLite as an operational backend.

### 5. Native chat policy

- The active ERP chat is the native route `/ia-privada/`.
- Do not treat any retired external chat runtime as the primary chat path anymore.
- Any future work on private AI must prioritize:
  - native Django route
  - PostgreSQL persistence
  - ERP tools
  - stable conversation continuity
  - Railway-first validation

### 6. Heavy runtime prohibition by default

Unless the task explicitly requires it, do not start by default:
- `worker`
- `beat`
- retired external chat containers or infra
- Playwright/MCP browser containers
- extra Docker stacks unrelated to the current task

If any of these are started for a justified task:
- say why
- keep the run as short as possible
- shut them down once the task is verified

### 7. Order of execution for future threads

For future threads, always follow this order:
1. read mandatory repo context (`AGENTS.md`, skills, memory when applicable)
2. determine whether the task is remote-first or truly needs local runtime
3. prefer Railway checks first for anything already deployed
4. if local is required, lift only `db + redis + web`
5. verify health, routes, database, and migrations before diagnosing logic bugs
6. only then perform code changes, redeploy, and final validation

### 8. Honesty rule for availability

- Do not say “the ERP is down” if the issue is only that the local stack is not running.
- Distinguish clearly between:
  - local runtime stopped
  - local runtime misconfigured
  - remote Railway regression
  - route-specific application bug

## Permanent Retirement Of Legacy External Chat Runtime

The ERP native chat is now the only supported private AI runtime in this repository.

Mandatory rules:
- treat `/ia-privada/` as the official chat entrypoint
- do not restore retired external chat infrastructure as fallback
- do not recreate retired external chat Docker stacks locally
- do not publish or document retired external chat as a valid architecture option
- if historical references remain in old docs, treat them as archival residue only, never as active guidance

## Persistent Guard For Sales Event Projection Work

When the task touches any of these areas:
- sales event forecast
- event financials
- production / inputs / purchases derived from forecast
- dashboard / Excel / projection artifacts
- event publication / re-publication / cleanup
- runtime real de agentes dentro del ERP

You must also read:
1. `.agent/skills/60-automation-ops/skill-sales-event-publication-guard/SKILL.md`
2. `.agent/skills/60-automation-ops/skill-agent-runtime-foundation/SKILL.md`

And follow its blocking loop before:
- publishing a new event package
- promoting event status across Producción / Compras
- regenerating dashboard or projection artifacts
- accepting branch/source labels in UI as valid

Additional permanent rules for seasonal event forecasting:
- Revenue must always be computed as `precio real vigente por SKU x piezas forecast por SKU`.
- The event day-curve must be built from `SKU x sucursal x día` quantities first; the agent must never use average prices or revenue-only smoothing as the forecasting method.
- The important day and the pre-peak days must not be distributed by residual leftover; they must be anchored to a defensible historical quantity curve plus recent weekday trend, and only then valued with current real SKU prices.
- The forecast must consider the real event product selection. Do not exclude a real dessert product just because it is sold in vaso format; `Vasos Preparados` stay in when they are part of the selected event assortment.
- Exclude only accessories, resale beverages, and recipes with `modo_costeo=SERVICIO_ACCESORIO` from the executive event scope.
- The important day of the event must be validated explicitly against the event's historical peak and any explicit DG benchmark before the event can stay in review/approval states.
- Event reprocessing is not complete unless it also refreshes the event detail snapshot before re-publishing artifacts or re-running audit.
- If a recalculation hangs, fails, or does not persist new values, the agent must not present stale values as already corrected; first clean heavy local processes and then report the blockage honestly.

## Permanent General Rule For Commercial Forecasts

This rule applies to every commercial forecast in the ERP, not just seasonal events:
- seasonal events
- campaigns
- strong commercial dates
- weekly branch planning
- product/family revenue projections driven by units

Mandatory rules:
- Always calculate revenue as `precio real vigente por SKU y sucursal x piezas forecast`.
- Never fix a bad forecast by rewriting revenue with flat caps or hand-picked uplifts.
- Pieces must be explained by:
  - comparable historical base
  - same-store factor
  - explicit expansion for new branches
  - explicit contraction for closed or out-of-scope branches
  - mix adjustment
  - temporal/day-curve adjustment
- The canonical daily sales history for these forecasts lives in PostgreSQL canonic layers such as `reportes_factventadiaria` / `pos_bridge_daily_sales`; partial product fact tables must not be treated as the full historical source.
- The forecast grain of truth is always `SKU x sucursal x día`; daily totals and revenue are downstream aggregates, never the primary forecasting unit.
- A new branch never belongs inside same-store; it must be modeled as explicit expansion with donor/comparable branch and maturity logic.
- A closed or out-of-scope branch must be modeled as explicit contraction and removed from the current-period benchmark.
- Weekly benchmark and main-day benchmark are different controls and must never be mixed.
- The important day cannot remain artificially flat or over-compressed; its weekly share must stay inside a defensible historical band unless there is explicit executive evidence.
- The important day cannot be "fixed" by crushing the days before it; pre-peak days must also stay inside a defensible historical band and may not be allocated by residual leftover after forcing the peak.
- Do not exclude a real commercial product from forecast scope because of its presentation format; exclude only accessories, resale beverages, and `modo_costeo=SERVICIO_ACCESORIO`.
- Never claim a forecast is applied, corrected, or closed unless it was recalculated, persisted, snapshot-verified, and left in a consistent ERP state.
- The permanent SOP and control table for this loop live in `docs/commercial_forecast_loop_sop.md`; follow that document when implementing or automating recurring checks.
