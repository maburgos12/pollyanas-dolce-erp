# Validacion de Suite de Pruebas ERP

Fecha de corte: 2026-04-09

## Estrategia de validacion

La validacion se hizo por capas y en corridas pequenas:

1. `orquestacion`
2. `api.tests_ai_gateway`
3. `pos_bridge.tests.test_celery_schedule_setup`
4. `reportes.tests_analytics_refresh`

Criterios usados:

- siempre con `./scripts/run_tests_local.sh`
- siempre sobre PostgreSQL
- sin usar SQLite como backend general
- sin lanzar suites en paralelo sobre el mismo test DB

## Matriz de estado

| Suite o grupo | Estado | Categoria principal | Evidencia resumida |
|---|---|---|---|
| `orquestacion.tests.SeedOrquestacionCatalogTest` | validada | n/a | pasa sobre PostgreSQL |
| `orquestacion.tests.OrquestacionDashboardViewTest` | validada | n/a | pasa sobre PostgreSQL |
| `orquestacion.tests` | validada | n/a | 47 pruebas en verde sobre PostgreSQL; incluye runtime minimo, runners y dashboard |
| `api.tests_ai_gateway.AIGatewayApiTests.test_manifest_exposes_safe_gateway_contract` | validada | n/a | pasa sobre PostgreSQL; valida el contrato seguro del manifest del gateway |
| `api.tests_ai_gateway` | validada | n/a | 16 pruebas en verde |
| `pos_bridge.tests.test_celery_schedule_setup` | validada | n/a | 2 pruebas en verde |
| `reportes.tests_analytics_refresh` | validada | n/a | 10 pruebas en verde despues de acotar mock de tiempo |
| suites restantes de `reportes` | no ejecutadas aun | pendiente | sin corrida en esta iteracion |
| suites restantes de `pos_bridge` | no ejecutadas aun | pendiente | sin corrida en esta iteracion |
| otras suites de `api` | no ejecutadas aun | pendiente | sin corrida en esta iteracion |

## Incidencias reales encontradas y resueltas

### 1. `orquestacion.tests`

Categoria principal original: `logica del codigo`

Estado actual:

- resuelto en esta iteracion
- causa raiz 1: el runner de cadena dependia demasiado del catalogo canonico y no protegia correctamente insumos no canonizados
- solucion aplicada: endurecer [rule_runners.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/rule_runners.py) para construir snapshots y resumenes sin asumir `canonical` siempre presente
- causa raiz 2: el runtime minimo tenia desacoples internos de contrato
- solucion aplicada:
  - alinear la firma de `execute_sales_event_publication()` en [sales_event_publication_guard.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/sales_event_publication_guard.py)
  - alinear `_run_single_goal()` con `run_agent_goal()` en [agent_runtime.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/agent_runtime.py)
  - corregir fixture desactualizado de `UnidadMedida` en [tests.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/tests.py)

### 2. `reportes.tests_analytics_refresh`

Categoria original: `prueba obsoleta` o `mock demasiado amplio`

Estado actual:

- resuelto en esta iteracion
- causa raiz: el test parchaba `reportes.views.timezone.localtime` durante toda la funcion, contaminando `django-celery-beat` al correr `setup_celery_schedules`
- solucion aplicada: limitar el mock solo al llamado de `_sales_refresh_status`

### 3. Colisiones de base de prueba

Categoria principal: `infraestructura`

Lectura:

- al correr suites en paralelo o reutilizar el mismo nombre de base de prueba, PostgreSQL puede chocar por `duplicate key` o bases intermedias
- mitigacion aplicada: `scripts/run_tests_local.sh` ahora genera `TEST_DB_NAME` unico por proceso cuando no se especifica manualmente

## Comando oficial

```bash
./scripts/run_tests_local.sh <test_labels>
```

Ejemplos ya validados:

```bash
./scripts/run_tests_local.sh api.tests_ai_gateway
./scripts/run_tests_local.sh pos_bridge.tests.test_celery_schedule_setup
./scripts/run_tests_local.sh reportes.tests_analytics_refresh
./scripts/run_tests_local.sh orquestacion.tests
```

## Que no debe hacerse

- no usar `python3 manage.py test` fuera de la venv del repo
- no usar SQLite como backend general de pruebas
- no asumir que una suite ya validada seguira verde si se tocan runners o runtime sin revalidarla
- no lanzar varias suites a la vez si se fija manualmente el mismo `TEST_DB_NAME`

## Siguiente cuello de botella real

El siguiente cuello de botella principal ya no es infraestructura ni la suite priorizada de agentes: ahora el gap mas importante esta en cierre de producto:

1. binding formal entre `AgentDefinition` y tools reales del gateway
2. escritura/gobierno de memoria persistente de largo plazo
3. extender el runtime minimo mas alla de `ventas.EventoVenta`
