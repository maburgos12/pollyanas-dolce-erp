# skill-migrations

## Purpose
Plan safe schema migrations with forward strategy, rollback notes, and operational risk controls.

## Guardrails
- Do not assume unknown data. Explicitly state missing inputs before proposing implementation details.
- Keep recommendations compatible with macOS local development and current Django/PostgreSQL stack.
- Use safe, incremental changes with rollback notes.
- Preserve auditability and deterministic behavior for imports/exports/jobs.

## Business alignment
- Maximize business value, operational control, reliability, and auditability (enterprise/SAP-style).
- Align with Pollyana’s Dolce reality: 8 sucursales, shelf life of 2 days, Sunday has no production/distribution but still has sales.
- Protect cross-area consistency for inventory, costing, production, purchasing, and reporting.

## Inputs expected
- Real repository files, schemas, endpoints, screenshots, and sample files (Excel/PDF/CSV) relevant to this domain.
- Business rules and approval criteria from Dirección General or process owners.
- Current state + target state + constraints (time, risk, dependency, rollout window).

## Outputs expected
- Step-by-step plan with assumptions, risks, and rollback option.
- Domain checklist and acceptance criteria.
- Artifacts/templates required for implementation (queries, mappings, data contracts, runbook notes).

## Checklist
- Confirm source of truth and owner for each data element.
- Validate idempotency/retry behavior where applicable.
- Validate RBAC impact and audit trail impact.
- Define monitoring/verification (before/after metrics or reconciliation points).
- Document edge cases and failure handling.

## Enterprise notes
- Prioritize: ledger-based inventory movements, audit trail, RBAC, idempotent jobs, deterministic exports, KPI dictionary consistency.
- Reconcile system data vs Excel/internal controls where processes still coexist.
- Prefer stable naming standards and canonical catalogs across Point/ERP/sucursales.
