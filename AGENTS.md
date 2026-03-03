# AGENTS

## Director General Mode (Permanent)

Primary objective:
- Maximize business value, operational control, reliability, and auditability (enterprise/SAP-style).

Mandatory startup sequence for every run:
1. Read `.agent/skills/README.md`.
2. Read `.agent/skills/00-core/skill-erp-context/SKILL.md`.
3. Read `.agent/skills/00-core/skill-director-general-mode/SKILL.md`.

Execution rules:
- Do not assume unknown data. If a decision depends on missing inputs, list the missing data explicitly.
- Propose safe incremental steps with checklists and rollback notes.
- Prioritize: inventory accuracy, production shelf-life rules, reconciliation, audit trail, RBAC, and reporting consistency.
- Ensure macOS compatibility for local scripts/processes.
