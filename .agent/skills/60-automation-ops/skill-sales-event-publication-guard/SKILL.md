# skill-sales-event-publication-guard

## Objective
Recibir un objetivo operativo sobre forecast, dashboard, artifacts o workflow de eventos comerciales y perseguirlo dentro de un loop persistente, sin recaer en errores silenciosos de publicación, taxonomía, cobertura financiera o promoción indebida de estados.

## Agent Loop

### 1. Autonomy
El agente debe perseguir el objetivo hasta cerrarlo o bloquearlo con evidencia.

Principios:
- no responder solo con análisis si ya se puede ejecutar
- no publicar si existe un bloqueo de negocio o de datos
- no promover estados por conveniencia técnica
- si una validación crítica falla, detener publicación y dejar el sistema en el último estado defendible

El loop mínimo del agente es:
1. interpretar el objetivo
2. identificar el estado actual real
3. validar bloqueos
4. ejecutar cambios permitidos
5. verificar resultado
6. publicar o bloquear
7. dejar evidencia y rollback

### 2. Context
Antes de ejecutar, el agente debe cargar contexto estructurado en Markdown según la plataforma activa.

Contexto mínimo obligatorio:
- `AGENTS.md`
- `.agent/skills/README.md`
- `.agent/skills/00-core/skill-erp-context/SKILL.md`
- `.agent/skills/00-core/skill-director-general-mode/SKILL.md`

Contexto específico cuando aplique:
- bounded context de ventas
- dashboard / exports / BI
- production / requirements / compras
- pricing Point real
- screenshots o evidencia UI si el problema se detectó visualmente

Regla:
- si el objetivo toca forecast, publicación o workflow, el agente debe reconstruir el contexto real del evento antes de actuar

### 3. Memory
Después del contexto, el agente debe cargar `memory.md`.

Uso correcto de memoria:
- usar solo hechos confirmados, decisiones estables y errores recurrentes
- no usar memoria como sustituto de evidencia viva
- si memoria y estado real divergen, gana la evidencia viva y se actualiza la memoria después

La memoria debe servir para evitar recaídas en:
- taxonomías incompletas de `base_method`
- publicaciones duplicadas
- helpers que mueven estados por sí solos
- sucursales maduras mal etiquetadas como sin base
- artifacts activos compitiendo con una publicación nueva
- forecasts ejecutivos resueltos con caps planos o uplifts manuales en vez de same-store + expansión + contracción
- exclusiones erróneas del scope ejecutivo que sacan productos reales del evento; `Vasos Preparados` sí cuentan cuando son postre real seleccionado para el evento
- reportar un forecast como corregido solo porque cambió el código aunque el recálculo vivo no haya persistido nuevos valores
- mezclar same-store con expansión o contracción al explicar un forecast comercial
- mezclar benchmark semanal con benchmark del día principal como si fueran el mismo control

### 4. Tools
Después de cargar objetivo, contexto y memoria, el agente usa herramientas.

Orden de herramientas:
1. skills del repo
2. MCP / tools del runtime
3. shell / scripts / tests / consultas DB

Herramientas prioritarias para este skill:
- skills de ERP, reporting, quality y automation
- consultas ORM / Django shell
- pruebas focalizadas
- filesystem para artifacts
- tools MCP o conectores solo cuando aporten evidencia real

Regla:
- no usar herramientas sin trazabilidad cuando exista una herramienta del repo más determinista

## Use when
- Regenerating event forecast, financials, production, inputs, purchases, dashboard, or exports.
- Publishing or re-publishing event artifacts.
- Moving an event across `APROBADO`, `ENVIADO_A_PRODUCCION`, `VALIDADO_POR_PRODUCCION`, or `ENVIADO_A_COMPRAS`.
- Reviewing screenshots or UI labels from sales-event pages.
- Cleaning stale event artifacts or preparing a new publication.

## Guardrails
- Do not assume unknown data. Explicitly state missing inputs before proposing implementation details.
- Keep recommendations compatible with macOS local development and current Django/PostgreSQL stack.
- Use safe, incremental changes with rollback notes.
- Preserve auditability and deterministic behavior for imports/exports/jobs.
- Never publish a new event package while stale active artifacts still exist for the same event/version.
- Never auto-promote an event to Compras from a pure data-generation helper.
- Never accept branch/source labels as valid if they contradict historical reality of the branch.

## Business alignment
- Maximize business value, operational control, reliability, and auditability (enterprise/SAP-style).
- Align with Pollyana’s Dolce reality: 9 active operating branches, shelf life of 2 days, Sunday has no production/distribution but still has sales. `MATRIZDBG` is not a real operating branch and must be excluded from completeness/same-store/forecast controls. `GUAMUCHIL` is a recent March 2026 opening and must be handled as new-branch expansion, not mature same-store.
- Protect cross-area consistency for inventory, costing, production, purchasing, and reporting.
- Respect the real operating flow:
  1. Dirección / modelado
  2. Enviado a producción
  3. Validado por producción
  4. Compras / almacén revisan faltantes y stock

## Non-negotiable loop checks

### Step 1. Confirm source of truth
For the current event, identify:
- current event status
- current version
- active artifact rows in DB
- active artifact files on disk
- forecast coverage status
- financial coverage status
- current workflow owner (DG / Producción / Compras)

If any of these are unknown, stop and surface the gap.

### Step 2. Validate forecast method taxonomy
Before accepting branch or product labels in UI/dashboard:
- inspect `explanation_json.base_method`
- map each method to one and only one family:
  - `Directo`
  - `Sucursal comparable`
  - `Fallback categoría`
  - `Sin base suficiente`

Blocking rule:
- if a new `base_method` exists and is not classified, stop publication and treat it as a defect.

### Step 3. Validate branch plausibility
For mature branches such as `MATRIZ`, `LEYVA`, `LAS_GLORIAS`, `COLOSIO`, `PLAZA_NIO`, `PAYAN`:
- if UI says `Sin base suficiente`, verify the actual `base_method` counts first
- if dominant method is direct/YTD-anchor/intermittent direct logic, UI must not show `Sin base suficiente`

Blocking rule:
- if a historically mature branch is labeled `Sin base suficiente` without evidence, stop publication.

### Step 3.1 Validate executive event model
Before publishing a seasonal event package:
- inspect the event executive projection model
- confirm it explicitly reports:
  - `benchmark_source`
  - `same_store_factor`
  - `expansion_factor`
  - `contraction_factor`
  - `mix_adjustment_source`
  - `final_projection_reasoning`

Rules:
- revenue must remain `precio real vigente por SKU x piezas forecast por SKU`
- do not resolve the event with a flat cap or a hand-picked uplift when same-store / expansion / contraction can be derived
- a new branch must be handled as explicit expansion using a donor branch and maturity logic
- a closed or out-of-scope branch must be handled as explicit contraction, not silently left inside the benchmark
- same-store must use only branches present in both periods; new branches never belong inside same-store and closed branches never remain in the current benchmark
- the executive event product scope must follow the real selected assortment; exclude accessories, resale beverages and `SERVICIO_ACCESORIO`, but keep real dessert products even if they are sold in vaso format
- incomplete historical indicators must not override better comparable same-store signals
- if the executive model already computes a lower weekly `target_total_qty` / `branch_targets`, publication must not accept a higher persisted week just because the main day looks plausible; the weekly ceiling has to be applied first
- the main day must inherit a defensible historical event curve; publication must reject a flat day distribution when the homologue shows a materially stronger peak
- the main day weekly share must stay inside a defensible historical band unless explicit executive evidence supports a stronger compression
- the pre-peak days must also stay inside a defensible historical band; publication must reject a curve that makes `día -1 / día -2 / día -3` look dead or inflated just to make the peak or the weekly total fit
- the publication review must reason from `SKU x sucursal x día` quantities first and only then accept revenue derived from current real SKU prices; average-price shortcuts are not valid forecast logic
- weekly benchmark and main-day benchmark must be parsed, validated and explained separately
- the publication review should be able to explain `daily_curve_source` for the event peak
- if a recálculo posterior vuelve a disparar semana total o día fuerte, el evento no puede quedarse en `LISTO_PARA_REVISION` o `PENDIENTE_DIRECCION`; debe regresar a `EN_MODELADO`
- if a live recalculation stalls, fails, or leaves the persisted values unchanged, the agent must not present the event as corrected; first clean heavy local repo processes with `scripts/cleanup_erp_forecast_processes.sh` and then report the blockage with the last verified persisted values
- a reprocess loop is not complete until `detail_snapshot` is refreshed and version-aligned with the regenerated forecast/financials/artifacts
- branch/day completeness must be checked against the 9 real operating branches, not aliases such as `MATRIZDBG`
- missing product-level sales rows do not automatically mean an incomplete branch-day; if `PointDailyBranchIndicator` exists for the branch/date, or a successful official backfill exists for that branch/date even with `0` imported rows, or operations validate a closure/network incident, treat it as a valid zero and not as a data-hole blocker
- if Point official sync/backfill lacks `POINT_BASE_URL` or equivalent endpoint configuration, stop with an explicit configuration defect before treating the date as a data hole
- before declaring a seasonal event forecast closed, run `./.venv/bin/python manage.py audit_seasonal_event_forecasts --enforce-status --write-report` and use its 10 checks + daily 2026 vs 2025 comparison as the final go/no-go evidence
- for recurring loop control, run `./.venv/bin/python manage.py audit_commercial_forecast_loop --days-back 30 --write-report` so the agent verifies daily valid-zero completeness, forecast/snapshot/artifact alignment, and the seasonal 10/10 audit in one pass

Blocking rule:
- if the publication cannot explain same-store vs expansion vs contraction with traceable fields, stop publication.

### Generalization rule
This same logic must be reused for every commercial forecast in the ERP whenever the projection depends on units:
- seasonal events
- campaigns
- strong dates
- weekly branch planning
- product or family revenue projections tied to units

The agent must always be able to explain:
1. comparable base
2. same-store factor
3. expansion from new branches
4. contraction from closed or out-of-scope branches
5. mix adjustment
6. temporal/day-curve adjustment
7. final revenue as price x pieces

### Step 4. Validate workflow promotion
Status transitions must be explicit and role-correct.

Allowed principle:
- forecast / inputs / purchase generation may create data
- only workflow actions should move statuses

Blocking rule:
- if helper functions such as `build_purchase_requirements()` or publication helpers auto-promote to Compras, treat as defect.

### Step 5. Validate artifact hygiene
Before publishing:
- detect active artifact rows for the event/version
- detect duplicate directories or legacy publication variants
- archive or invalidate stale versions first
- ensure only one active publication path remains

Blocking rule:
- never leave two competing “actual/current” outputs for the same event.

### Step 6. Validate financial publication
Before dashboard/export publication:
- confirm pricing source is current Point-real logic
- confirm cost coverage and price coverage are still within the accepted rules
- confirm dashboard numbers equal current persisted event financials when expected

Blocking rule:
- if dashboard/export is generated from stale or mismatched financial data, stop publication.
- if the persisted week still exceeds the defendible executive weekly target, stop publication even if the main day already cleared its floor.
- if the main day remains materially below the defended homologue peak after executive alignment, or if an explicit DG main-day benchmark is not reflected in the final snapshot, stop publication.

### Step 7. Validate state before release
If the event is still pending production review:
- do not leave it in `ENVIADO_A_COMPRAS`
- artifacts may be archived/cleaned, but release-ready publication must wait for the correct state gate

### Step 8. Publish or block
Only after all checks are green:
- generate artifacts
- persist DB artifact rows
- verify physical files exist
- confirm one active publication path
- confirm correct event status

If any blocking rule fails:
- archive/cleanup as needed
- remove active artifact rows if publication is invalid
- leave the event in the last defensible workflow state

## Required outputs
- Current event state snapshot.
- Artifact hygiene summary: active, archived, deleted, pending regeneration.
- Method taxonomy summary for branch/product labels.
- Blocking findings, if any.
- Main-day peak explanation: source of daily curve, whether final peak floor was applied, and whether DG benchmark for the main day was used.
- Explicit go/no-go decision for publication.
- Rollback note.

## Acceptance criteria
- No stale active publication competes with the next one.
- No helper promotes workflow status incorrectly.
- No mature branch is mislabeled as lacking base without evidence.
- New forecast methods cannot appear without taxonomy mapping.
- One active event publication path only.
- DB artifact rows and disk files stay consistent.

## Rollback
- Restore archived artifact directories if publication must be reverted.
- Recreate DB artifact rows only for the chosen active version.
- Restore previous event status only if the workflow owner explicitly approves it.
