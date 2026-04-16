# Loop de Calidad y Memoria del ERP

Fecha de corte: 2026-04-12

## Objetivo

Dejar un loop tecnico gobernado que pueda:

1. detectar una violacion nueva
2. bloquearla o marcarla
3. registrar el hallazgo
4. proponer memoria estable si aplica
5. proponer remediacion
6. validar el cierre por rerun

## Alcance real al corte actual

Implementado hoy para tres detectores seguros y gobernados:

- lector directo no autorizado de `PointDailySale`
- lector crudo de ventas en rutas protegidas del ERP visible, gateway y consultas operativas
- gap de publicación visible entre `PointDailySale`, `FactVentaDiaria` y corte del dashboard

Este loop ya cubre:

- detector standalone
- persistencia auditable
- puente controlado a `MemoryProposal`
- `RemediationProposal`
- validacion de cierre por rerun del detector
- visibilidad y operacion base desde el ERP en Orquestacion
- comando operativo persistente y wrapper local del loop
- politica por detector con allowlist o criterio operativo explicito
- clasificacion de detectores descartados por ruido o ambiguedad

No cubre y no promete:

- autofix de codigo
- autoaplicacion a `memory.md`
- detectores globales sobre fuentes historicas/financieras sin politica especifica por contexto

## Componentes

### 1. Guards arquitectonicos

- script: [check_pointdailysale_usage.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/check_pointdailysale_usage.py)
- politica central: [pointdailysale_guard.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/pointdailysale_guard.py)
- script: [check_protected_sales_readers.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/check_protected_sales_readers.py)
- politica central: [protected_sales_reader_guard.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/protected_sales_reader_guard.py)
- detector operativo: [sales_publication_guard.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/sales_publication_guard.py)

Que hace:

- detecta imports/usos directos de `PointDailySale`
- detecta lectores crudos de ventas dentro de archivos protegidos:
  - `api/ai_gateway_services.py`
  - `reportes/dashboard_sales_dataset.py`
  - `pos_bridge/services/agent_query_service.py`
- detecta rezago operativo entre extracción cerrada, facts publicados y corte visible del dashboard
- respeta una allowlist centralizada de rutas tecnicas permitidas
- falla con exit code no cero si encuentra violaciones

Que bloquea:

- nuevas lecturas visibles u operativas fuera de la capa canonica o de excepciones documentadas

Que no bloquea:

- `sales_publication_gap`, porque es hallazgo operativo no bloqueante; persiste, propone remediacion y exige correccion, pero no invalida de inmediato una corrida local si no hay violacion arquitectonica
- `sales_publication_gap` se difiere automaticamente si existe un sync de ventas activo para la fecha de referencia; eso reduce ruido mientras el cierre sigue en curso

### 2. Hallazgos persistentes

- modelo: [QualityFinding](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/models.py)
- servicio: [quality_findings.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/quality_findings.py)

Campos clave:

- `code`
- `category`
- `severity`
- `status`
- `source_type`
- `source_reference`
- `statement`
- `evidence_refs_json`
- `detected_count`
- `first_seen_at`
- `last_seen_at`
- `resolved_at`
- `details_json`

Politica:

- consolida por patron/archivo, no por cada linea suelta
- incrementa `detected_count` en reapariciones
- reabre un hallazgo si vuelve a aparecer despues de estar resuelto
- registra `AuditLog`

### 3. Memoria controlada

- modelo: [MemoryProposal](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/models.py)
- servicio: [memory_proposals.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/memory_proposals.py)

Regla vigente:

- un `QualityFinding` tecnico puede generar `MemoryProposal`
- hoy eso ocurre para:
  - `direct_pointdailysale_reader`
  - `protected_raw_sales_reader`
  cuando reaparecen y ya son estructurales
- la propuesta queda manual por categoria `architecture_guard_violation`
- `sales_publication_gap` no genera memoria automatica en esta linea porque sigue siendo señal operativa, no memoria estable por si sola
- no se aplica automaticamente a `memory.md`

### 4. Remediacion

- modelo: [RemediationProposal](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/models.py)

Cada remediacion deja:

- hallazgo asociado
- archivos objetivo
- sugerencia concreta
- pruebas recomendadas
- riesgo
- estado

## Flujo operativo

1. Ejecutar detector rapido:

```bash
./.venv/bin/python scripts/check_pointdailysale_usage.py
./.venv/bin/python scripts/check_protected_sales_readers.py
```

2. Ejecutar loop persistente:

```bash
./.venv/bin/python manage.py run_quality_guards --settings=config.settings_test
```

Wrapper operativo recomendado:

```bash
./scripts/run_quality_loop.sh full
```

Otros modos:

```bash
./scripts/run_quality_loop.sh quick
./scripts/run_quality_loop.sh persist
./scripts/run_quality_loop.sh validate
```

Perimetro exacto del bloque para consolidacion segura en repo sucio:

```bash
./scripts/show_sales_enforcement_scope.sh
git diff -- $(./scripts/show_sales_enforcement_scope.sh --pathspec)
```

Si solo se quiere validar sin persistir:

```bash
./.venv/bin/python manage.py run_quality_guards --settings=config.settings_test --no-persist
```

3. Ruta oficial de pruebas locales:

```bash
./scripts/run_tests_local.sh ...
```

Hoy esta ruta ya corre el guard antes de las pruebas, salvo que se fuerce:

```bash
SKIP_POINTDAILYSALE_GUARD=1 ./scripts/run_tests_local.sh ...
SKIP_PROTECTED_SALES_READER_GUARD=1 ./scripts/run_tests_local.sh ...
```

## Cierre de hallazgos

Un `QualityFinding` se marca como `resolved` solo cuando:

- se vuelve a correr el detector
- la violacion ya no aparece

Cuando eso pasa:

- el hallazgo queda `resolved`
- la `RemediationProposal` asociada queda `validated`
- se registra auditoria

## Operacion desde el ERP

La bandeja de Orquestacion ya expone:

- `Orquestacion > Loop de calidad y remediacion`
- detalle del `QualityFinding`
- detalle/estado de `RemediationProposal`
- vínculo a `MemoryProposal` cuando exista
- accion para relanzar el guard desde el ERP
- transiciones operativas de remediacion:
  - aceptar
  - marcar implementada
  - rechazar

Esto no reemplaza la correccion del codigo, pero si deja el ciclo visible y auditable sin depender solo de shell o admin.

## Excepciones intencionales

La allowlist no significa “todo vale”. Significa que esos archivos pertenecen a capas tecnicas donde el uso de `PointDailySale` sigue siendo valido al corte actual:

- staging e ingestion en `pos_bridge`
- monitoreo y diagnostico de integraciones
- servicios tecnicos de analytics y reconciliacion
- excepciones documentadas de pricing/historico

Si una pantalla, API o consulta operativa nueva necesita `PointDailySale` directo, primero debe justificarse como excepcion intencional y actualizar la politica central.

## Detectores descartados intencionalmente

No se implementaron detectores globales adicionales para estas familias porque hoy producirian ruido o falsos positivos:

- detector global de `VentaHistorica`
  - razon: mezcla contextos legitimos de historico ejecutivo, pricing, forecasting, postmortem y reconciliacion
- detector repo-wide que exija `sales_read_service` en cualquier lector de ventas
  - razon: monitoreo, diagnostico y publicacion analitica necesitan comparar varias capas por diseño
- detector global sobre `FactVentaDiaria`
  - razon: facts y publication layers legitimas siguen leyendo este modelo dentro de rutas tecnicas correctas

Estas familias quedan cerradas como excepcion intencional del alcance actual, no como fase pendiente olvidada.

## Riesgos

- falsos positivos si una ruta tecnica legitima no entra a la allowlist
- falsos negativos si la allowlist crece demasiado por comodidad
- ruido en memoria si se amplian categorias sin disciplina
- confusion si se interpreta este loop como autofix o autonomia total

## Cierre del alcance actual

Al corte actual este loop queda cerrado dentro del alcance seguro del repo para:

- `direct_pointdailysale_reader`
- `protected_raw_sales_reader`
- `sales_publication_gap`

No queda una expansion adicional tecnicamente obvia dentro de esta misma linea sin antes definir una politica nueva por fuente/contexto. Lo que no entro quedo descartado por criterio tecnico explicito, no por omision.
