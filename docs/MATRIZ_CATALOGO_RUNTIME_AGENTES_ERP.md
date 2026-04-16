# Matriz Catalogo vs Runtime de Agentes ERP

Fecha de corte: 2026-04-09

## 1. Agentes

| Agente | Dominio | Declarado en catalogo | Capacidades catalogadas | Reglas asignadas | Tools expuestas por gateway ligadas formalmente al agente | Scheduler ligado formalmente al agente | Trabajo real hoy | Estado real | Evidencia |
|---|---|---|---:|---:|---|---|---|---|---|
| `director_operativo` | operaciones | si | 4 | 5 | no explicito | indirecto | coordina y recibe sugerencias; participa en cadena del plan y como secundario en escalamiento | parcial-real | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py`, `orquestacion/views.py` |
| `agente_demanda_ventas` | ventas | si | 5 | 4 | no explicito | indirecto | participa en la cadena demanda-produccion-compras; no tiene runner individual propio fuera de esa cadena | parcial-real | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py` |
| `agente_produccion` | produccion | si | 4 | 3 | no explicito | si, por regla | ejecuta runner de plan diario faltante y participa en cadena operativa del plan | real-parcial | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py`, `pos_bridge/management/commands/setup_celery_schedules.py` |
| `agente_compras` | compras | si | 6 | 3 | no explicito | si, por regla | ejecuta runner de excepciones DG y participa en cadena operativa del plan | real-parcial | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py`, `pos_bridge/management/commands/setup_celery_schedules.py` |
| `agente_conciliacion` | conciliacion | si | 5 | 5 | no explicito | si, por regla | ejecuta runner de guardia de ajustes y de baja rotacion; participa como agente de escalamiento y control | real-parcial | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py`, `pos_bridge/management/commands/setup_celery_schedules.py` |

Notas:

- El AI Gateway expone tools por **rol de usuario**, no por `AgentDefinition` catalogado. Por eso la columna de tools ligadas formalmente al agente es `no explicito`.
- Existe un agente tecnico `ai_gateway` creado on demand para aprobaciones seguras, pero no forma parte del catalogo base de `orquestacion/catalog.py`; se crea desde `api/ai_gateway_services.py`.

## 2. Reglas del catalogo

| Regla | Agente principal | Tipo | Declarada en catalogo | Runner implementado | Programada en scheduler | Requiere aprobacion o escalamiento | Estado real | Evidencia |
|---|---|---|---|---|---|---|---|---|
| `stock_arranque_branch_risk` | `agente_demanda_ventas` | threshold | si | no confirmado | no | si | solo catalogo | `orquestacion/catalog.py` |
| `seasonal_peak_detection` | `agente_demanda_ventas` | schedule | si | no confirmado | no | no directo, modo recommend | solo catalogo | `orquestacion/catalog.py` |
| `integration_job_failure_review` | `agente_conciliacion` | event | si | no confirmado | no | si | solo catalogo | `orquestacion/catalog.py` |
| `production_weekly_purchase_request_deadline` | `agente_compras` | schedule | si | no confirmado | no | si | solo catalogo | `orquestacion/catalog.py` |
| `purchase_exception_requires_dg_approval` | `agente_compras` | threshold | si | si | si | si | runtime real | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py`, `pos_bridge/management/commands/setup_celery_schedules.py` |
| `inventory_adjustment_authorization_guard` | `agente_conciliacion` | event | si | si | si | si | runtime real | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py`, `pos_bridge/management/commands/setup_celery_schedules.py` |
| `near_expiry_or_low_rotation_review` | `agente_conciliacion` | threshold | si | si | no | no, queda como revision operativa | runtime real sin scheduler propio | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py` |
| `branch_capacity_below_threshold` | `agente_demanda_ventas` | threshold | si | no confirmado | no | no directo, modo recommend | solo catalogo | `orquestacion/catalog.py` |
| `cold_chain_temperature_breach` | `agente_conciliacion` | threshold | si | no confirmado | no | si | solo catalogo | `orquestacion/catalog.py` |
| `daily_production_plan_missing` | `agente_produccion` | schedule | si | si | si | si | runtime real | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py`, `pos_bridge/management/commands/setup_celery_schedules.py` |
| `plan_demand_production_purchase_chain` | `agente_demanda_ventas` | manual | si | si | si | si | runtime real | `orquestacion/catalog.py`, `orquestacion/services/rule_runners.py`, `pos_bridge/management/commands/setup_celery_schedules.py` |
| `production_checklist_below_target` | `agente_produccion` | threshold | si | no confirmado | no | no directo, modo recommend | solo catalogo | `orquestacion/catalog.py` |

## 3. Runners realmente soportados

| Runner / regla soportada | Fuente | Estado |
|---|---|---|
| `daily_production_plan_missing` | `SUPPORTED_RULE_CODES` + funcion dedicada | implementado |
| `plan_demand_production_purchase_chain` | `SUPPORTED_RULE_CODES` + funcion dedicada | implementado |
| `purchase_exception_requires_dg_approval` | `SUPPORTED_RULE_CODES` + funcion dedicada | implementado |
| `inventory_adjustment_authorization_guard` | `SUPPORTED_RULE_CODES` + funcion dedicada | implementado |
| `near_expiry_or_low_rotation_review` | `SUPPORTED_RULE_CODES` + funcion dedicada | implementado |

## 4. Schedules reales de orquestacion

| Nombre de schedule | Regla asociada | Estado |
|---|---|---|
| `orquestacion: plan diario faltante` | `daily_production_plan_missing` | real |
| `orquestacion: cadena plan demanda-produccion-compras` | `plan_demand_production_purchase_chain` | real |
| `orquestacion: excepciones compra DG` | `purchase_exception_requires_dg_approval` | real |
| `orquestacion: guardia ajustes inventario` | `inventory_adjustment_authorization_guard` | real |

## 5. Tools reales del ERP AI Gateway

| Tool | Tipo | Dominio | Requiere aprobacion | Estado real | Evidencia |
|---|---|---|---|---|---|
| `erp.get_dashboard` | read | reporting | no | real | `api/ai_gateway_services.py` |
| `erp.get_audit_logs` | read | audit | no | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |
| `erp.get_sales_summary` | read | sales | no | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |
| `erp.get_sales_by_branch` | analyze | sales | no | real | `api/ai_gateway_services.py` |
| `erp.get_sales_trends` | analyze | sales | no | real | `api/ai_gateway_services.py` |
| `erp.get_inventory_low_stock` | read | inventory | no | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |
| `erp.get_discrepancies` | analyze | control | no | real | `api/ai_gateway_services.py` |
| `erp.get_sync_jobs` | read | integrations | no | real | `api/ai_gateway_services.py` |
| `erp.get_purchase_requests` | read | purchasing | no | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |
| `erp.get_purchase_orders` | read | purchasing | no | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |
| `erp.get_recipe_cost_history` | analyze | costing | no | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |
| `erp.trigger_sync_jobs` | execute_safe_action | integrations | si | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |
| `erp.create_purchase_request_draft` | execute_safe_action | purchasing | si | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |
| `erp.create_production_plan_draft` | execute_safe_action | production | si | real | `api/ai_gateway_services.py`, `api/tests_ai_gateway.py` |

## 6. Estado real de memoria

| Elemento | Existe hoy | Estado |
|---|---|---|
| `memory.md` | no | no implementado |
| memoria persistente reusable cargada por runtime | no confirmado | no implementado |
| memoria transaccional de corridas y tareas | si | real |
| trazabilidad de ejecucion y aprobacion | si | real |

## 7. Lectura rapida

- **Real hoy**: orquestador parcial, gateway real, approvals reales, 5 runners, 4 schedules, dashboard y admin.
- **Solo catalogo hoy**: varias reglas declaradas sin runner confirmado.
- **No implementado hoy**: memoria operativa persistente reusable tipo `memory.md` integrada al runtime.
