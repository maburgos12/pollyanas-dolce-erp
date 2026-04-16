# Criterio de Memoria Operativa del Proyecto de Agentes ERP

Fecha de corte: 2026-04-12

## Objetivo

Definir que se considera memoria operativa real en este proyecto, como debe leerse y cuando debe actualizarse sin convertirla en ruido.

## Estado actual

Implementado en este bloque:

- `memory.md` como memoria operativa humana y reusable del proyecto.
- criterio formal para diferenciar memoria estable vs transcript, bitacora o estado transaccional.
- loader de lectura desde runtime minimo (`load_agent_memory`)
- escritura controlada mediante [memory_control.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/memory_control.py)
- comando operativo [record_agent_memory.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/management/commands/record_agent_memory.py)
- modelo [MemoryProposal](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/models.py)
- modelos [QualityFinding](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/models.py) y [RemediationProposal](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/models.py)
- servicio [memory_proposals.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/memory_proposals.py)
- loop tecnico inicial en [quality_findings.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/quality_findings.py)
- guards arquitectonicos [check_pointdailysale_usage.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/check_pointdailysale_usage.py) y [check_protected_sales_readers.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/check_protected_sales_readers.py)
- bandeja ERP en [views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/views.py) y templates de Orquestación para revisar, aprobar, rechazar y aplicar propuestas
- bandeja ERP de calidad/remediacion para `QualityFinding` y `RemediationProposal`
- wrapper operativo [run_quality_loop.sh](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/run_quality_loop.sh)
- detector operativo [sales_publication_guard.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/sales_publication_guard.py)
- perimetro exacto del bloque en [ALCANCE_ENFORCEMENT_VENTAS.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/ALCANCE_ENFORCEMENT_VENTAS.md)

No implementado aun:

- escritura automatica desde runtimes externos retirados o desde cualquier loop runtime sin control adicional
- memoria estructurada en BD sincronizada con `memory.md`
- autoaprobacion selectiva mas amplia por tipo de hallazgo
- autofix general de codigo o remediacion automatica
- detectores globales sobre fuentes historicas/financieras sin politica especifica por contexto

## Que SI es memoria operativa

- reglas de negocio confirmadas
- fuentes de verdad por dominio
- decisiones operativas confirmadas
- errores recurrentes y como evitarlos
- gaps estructurales confirmados
- preferencias operativas permanentes confirmadas por el negocio

## Que NO es memoria operativa

- transcript de chat
- logs de auditoria crudos
- `context_json` de una corrida puntual
- `details_json` de una sugerencia puntual
- tareas pendientes del dia
- ideas no confirmadas
- TODOs temporales

## Lectura al inicio

Secuencia recomendada para futuros agentes de implementacion:

1. `AGENTS.md`
2. skills core obligatorias del repo
3. `memory.md`
4. snapshot actual: [AGENTS_RUNTIME_SNAPSHOT.json](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/AGENTS_RUNTIME_SNAPSHOT.json)
5. documentos de readiness/runtime y reporte consolidado

## Actualizacion al cierre

Se debe actualizar `memory.md` solo si la iteracion deja un hecho estable nuevo.

Ruta controlada actual:

```bash
./.venv/bin/python manage.py record_agent_memory \
  --settings=config.settings_test \
  --section fact \
  --text "Hecho estable confirmado" \
  --source "runtime.manual_review" \
  --evidence docs/AGENTS_RUNTIME_SNAPSHOT.json
```

Guardrails de escritura:

- requiere `section`, `text`, `source` y al menos una `--evidence`
- deduplica entradas ya existentes
- registra `AuditLog`
- no escribe transcript ni logs crudos

Ruta manual recomendada desde ERP:

1. `Orquestación > Propuestas de memoria`
2. revisar evidencia, statement y origen
3. aprobar o rechazar
4. aplicar a `memory.md` solo cuando el hallazgo ya sea estable

Ruta de autoaprobacion minima vigente:

- feature flag: `ORQUESTACION_MEMORY_AUTO_APPROVAL_ENABLED`
- allowlist configurable: `ORQUESTACION_MEMORY_AUTO_APPROVAL_CATEGORIES`
- categoria autoaprobable inicial: `tool_binding_gap`
- umbrales:
  - solo `section in {"fact", "gap"}` pero hoy la allowlist solo cubre `gap`
  - solo `source_type in {"agent_runtime", "test_validation"}`
  - `confidence_score >= 0.95`
  - `detected_count >= 2`
  - al menos 2 evidencias
  - nunca si el statement parece decision de negocio u opinion
- resultado:
  - cambia `status` de `proposed` a `approved`
  - marca `approval_mode = auto`
  - registra `AuditLog`
  - no aplica `memory.md` automaticamente

Ruta de loop tecnico vigente:

1. `scripts/check_pointdailysale_usage.py` detecta lecturas directas no autorizadas de `PointDailySale`
2. `scripts/check_protected_sales_readers.py` detecta lectores crudos dentro de rutas protegidas del ERP visible, gateway y consultas operativas
3. `manage.py run_quality_guards` persiste `QualityFinding` para guards bloqueantes y para el detector no bloqueante `sales_publication_gap`
4. si el hallazgo se repite y es elegible, crea `MemoryProposal`
5. crea `RemediationProposal` con fix sugerido y pruebas recomendadas
6. un rerun limpio del detector marca el `QualityFinding` como `resolved` y la remediacion como `validated`

Wrapper operativo equivalente:

```bash
./scripts/run_quality_loop.sh full
```

Guardrails del loop:

- no escribe `memory.md` directamente
- no hace autofix de codigo
- no autoaprueba memoria de negocio
- si faltan migraciones, `run_quality_guards` falla con mensaje explicito y sugiere `migrate` o `--no-persist`
- `sales_publication_gap` es detector operativo no bloqueante: deja hallazgo y remediacion, pero no invalida por si solo una corrida local limpia
- `sales_publication_gap` se difiere cuando existe un sync de ventas activo para la fecha de referencia; no debe abrir hallazgos mientras el cierre aún está en curso

Eventos que SI deben actualizar memoria:

- se confirma una nueva restriccion operativa estable
- se implementa de verdad un componente que antes era gap
- se demuestra una incompatibilidad recurrente del entorno que condiciona el trabajo
- se fija un error repetido con una prevencion clara y durable
- se formaliza una fuente de verdad adicional o una decision de gobierno
- una propuesta tecnica repetida vuelve a aparecer y ya fue confirmada como estructural

Eventos que NO deben escribir memoria:

- una corrida puntual con exito o fallo aislado
- un experimento temporal
- una respuesta de exploracion sin implementacion
- una preferencia personal no confirmada por negocio
- un hallazgo que todavia depende de datos faltantes

Categorias de politica al corte actual:

- autoaprobables:
  - `tool_binding_gap`
- manuales por ahora:
  - `runtime_constraint`
  - `test_environment_fact`
  - `verified_command_path`
  - `architecture_guard_violation`
- prohibidas de facto para autoaprobacion:
  - cualquier propuesta sin evidencia suficiente
  - cualquier propuesta con statement de negocio, criterio comercial u opinion
  - cualquier propuesta `section=error` al corte actual

## Futuro recomendado para memoria persistente

Modelo objetivo sugerido:

- `memory.md` como memoria humana y auditable en repo
- `MemoryProposal` como bandeja Django para hallazgos propuestos y auditables
- proceso de sincronizacion controlado entre memoria estructurada y memoria documental
- validacion humana para cualquier insercion de memoria que afecte reglas de negocio

Campos minimos sugeridos si se implementa en BD:

- `key`
- `category`
- `statement`
- `evidence_path`
- `source_type`
- `status`
- `last_validated_at`
- `validated_by`

## Checklist

- [x] diferenciada memoria estable vs estado transaccional
- [x] creado `memory.md`
- [x] definida politica de lectura
- [x] definidos eventos que actualizan y que no actualizan memoria
- [x] existe ruta de escritura controlada con auditoria
- [x] documentado gap de escritura automatica general
- [x] existe bandeja manual dentro del ERP para aprobar o rechazar propuestas
- [x] existe politica minima de autoaprobacion con allowlist y feature flag

## Riesgos

- riesgo de usar `memory.md` como bitacora si no se mantiene disciplina editorial
- riesgo de duplicar hechos entre docs y memoria si no se consolida la fuente estable
- riesgo de afirmar aprendizaje persistente autonomo antes de implementar gobierno de escritura automatica
- riesgo de ampliar demasiado pronto la allowlist de autoaprobacion y empezar a consolidar memoria con ruido

## Rollback

- reducir `memory.md` a solo reglas de negocio y gaps confirmados
- archivar decisiones dudosas hasta nueva validacion
- mantener toda escritura de memoria via comando/control manual hasta tener governance tecnico superior
- apagar `ORQUESTACION_MEMORY_AUTO_APPROVAL_ENABLED`
