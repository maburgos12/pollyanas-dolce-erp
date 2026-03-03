# ERP Skills Index (Enterprise/SAP-style)

Director General mode always applies in this repository.

## Reading order
1. `00-core/skill-director-general-mode`
2. `00-core/skill-erp-context`
3. `00-core/skill-naming-conventions`
4. `00-core/skill-adr-decision-log`
5. `10-architecture/*`
6. `20-data/*`
7. `30-integrations/*`
8. Domain modules: `40` to `44`
9. `50-reporting-bi/*`
10. `60-automation-ops/*`
11. `70-security-compliance/*`
12. `90-quality/*`
13. `99-templates/*`

## Operating principle
- Ask for missing inputs explicitly; do not assume unknown data.
- Prioritize inventory accuracy, shelf-life rules, reconciliation, audit trail, RBAC, and reporting consistency.
- Plan incremental changes with checklist + rollback notes.
- Maintain macOS compatibility for local workflows.

## Operational playbooks (priority)
1. `00-core/skill-director-general-mode/playbooks/dg_daily_operating_cycle.md`
2. `42-domain-inventory/skill-inventory-ledger/playbooks/ledger_reconciliation_playbook.md`
3. `42-domain-inventory/skill-minmax-reorder/playbooks/stock_minimo_por_sucursal_playbook.md`
4. `43-domain-production/skill-bom-recipes/playbooks/insumo_subinsumo_producto_model.md`
5. `40-domain-finance/skill-costing-pricing/playbooks/costeo_por_rendimiento_y_presentacion.md`
6. `30-integrations/skill-google-sheets-bridge/playbooks/google_drive_inventory_sync_playbook.md`
7. `70-security-compliance/skill-rbac-permissions/playbooks/rbac_matrix_operativa.md`
8. `50-reporting-bi/skill-kpi-definitions/playbooks/kpi_dictionary_governance.md`
