# Mapa Rápido: Runtime, DB y Fuentes ERP

Fecha de corte: 2026-04-15

## Objetivo

Concentrar en un solo lugar el mapa mínimo que necesita cualquier hilo nuevo para no arrancar a ciegas:

1. qué base de datos debería usar el ERP
2. dónde vive el runtime real de agentes
3. dónde está la canonicidad de ventas
4. dónde está la rutina operativa diaria
5. cómo detectar rápido si el hilo quedó conectado a una base local equivocada

## Resumen ejecutivo

- El ERP está diseñado para operar sobre PostgreSQL, no SQLite.
- La canonicidad de ventas ya está documentada y centralizada en `ventas/services/sales_canonical_source.py`.
- El runtime real de agentes vive en `orquestacion/` y se conecta con el gateway en `api/ai_gateway_services.py`.
- La rutina diaria integrada del ERP vive en `core/management/commands/ejecutar_rutina_diaria_erp.py`.
- En este repo, abrir un hilo nuevo sin validar la conexión activa a la base puede llevar a trabajar contra una base local distinta a la base viva del ERP.

## Ruta rápida para un hilo nuevo

### 1. Leer contexto base

- `AGENTS.md`
- `.agent/skills/README.md`
- `.agent/skills/00-core/skill-erp-context/SKILL.md`
- `.agent/skills/00-core/skill-director-general-mode/SKILL.md`
- `memory.md`

### 2. Correr diagnóstico de contexto

```bash
./scripts/diagnose_erp_runtime_context.sh
```

Ese script responde:

- qué base está usando Django en esta sesión
- si `DATABASE_URL` o las variables `DB_*` existen o no
- si las tablas canónicas mínimas del ERP están presentes
- cuántos registros hay en capas críticas como ventas, orquestación y movimientos de CEDIS

### 3. Si la base no es la viva del ERP

No seguir con extracción ni comparativas ejecutivas hasta corregir una de estas rutas:

1. cargar el `DATABASE_URL` real del VPS
2. cargar `DB_HOST` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` / `DB_PORT` en entornos locales aprobados
3. correr el comando objetivo dentro del VPS cuando la validación sea productiva

## Fuente de verdad: base de datos

### Configuración principal

- `config/settings.py`
- `docs/ENTORNO_LOCAL_PRUEBAS_ERP.md`

Regla vigente:

- prioridad 1: `DATABASE_URL`
- prioridad 2: `DB_HOST` + `DB_NAME` + `DB_USER` + `DB_PASSWORD` + `DB_PORT`
- SQLite no es ruta operativa válida

### Fuente operativa actual

El ERP opera contra PostgreSQL del VPS. No usar hosts ni variables heredadas de Railway como fallback operativo.

### Hallazgo local verificado en esta fecha

Con el `.env` actual del repo:

- Django conecta a `pollyana_db`
- host: `127.0.0.1`
- puerto: `5432`

Eso confirma PostgreSQL, pero no confirma que sea la base viva del ERP.

## Fuente de verdad: ventas canónicas

### Archivo central

- `ventas/services/sales_canonical_source.py`

### Lectura de negocio / política documentada

- `docs/CANONICIDAD_VENTAS_ERP.md`

### Capas relacionadas

- `ventas/services/sales_read_service.py`
- `reportes/dashboard_sales_dataset.py`
- `core/views.py`
- `reportes/views.py`
- `api/ai_gateway_services.py`
- `pos_bridge/services/agent_query_service.py`

### Regla operativa

La UI visible y el BI no deben decidir cada uno por su cuenta de dónde leer ventas. La resolución canónica debe salir del servicio compartido y degradar por contexto:

1. facts publicados
2. publicación canónica Point
3. histórico conciliado
4. fallback histórico referencial

## Fuente de verdad: runtime real de agentes

### Bounded context operativo

- `orquestacion/services/agent_runtime.py`
- `orquestacion/catalog.py`
- `orquestacion/services/rule_runners.py`
- `orquestacion/models.py`
- `orquestacion/tasks.py`
- `orquestacion/views.py`
- `orquestacion/management/commands/run_agent_goal.py`
- `orquestacion/management/commands/run_orchestration_rule.py`
- `orquestacion/management/commands/run_quality_guards.py`

### Gateway ERP

- `api/ai_gateway_services.py`

### Documentación de apoyo

- `docs/LINEA_BASE_REAL_AGENTES_ERP.md`
- `docs/AGENTS_RUNTIME_SNAPSHOT.json`
- `docs/AUTOMATIZACIONES_Y_ONYX_GATEWAY_AGENTES_ERP.md`

### Qué sí existe hoy

- catálogo de agentes
- runtime mínimo por goals
- checkpoints y auditoría
- dashboard de orquestación
- gateway con tools y aprobaciones

### Qué no debes asumir automáticamente

- que el runtime esté corriendo contra la base viva
- que todas las reglas catalogadas tengan runner real
- que el gateway esté leyendo la misma base que esta terminal

## Fuente de verdad: rutina diaria integrada

### Comando principal

- `core/management/commands/ejecutar_rutina_diaria_erp.py`

### Runbook

- `docs/OPERACION_RUTINA_DIARIA_ERP.md`

### Integraciones Point / POS Bridge relacionadas

- `pos_bridge/management/commands/run_daily_sales_sync.py`
- `pos_bridge/management/commands/run_official_sales_backfill.py`
- `pos_bridge/management/commands/run_production_entry_sync.py`
- `pos_bridge/management/commands/run_transfer_sync.py`
- `pos_bridge/management/commands/run_waste_sync.py`
- `pos_bridge/management/commands/setup_celery_schedules.py`

## Chequeo rápido: cómo detectar si el hilo quedó en la base equivocada

### Señales de alerta

- `DATABASE_URL` no está cargado en shell
- Django toma la base desde `.env` local y cae en `pollyana_db`
- faltan tablas críticas del ERP
- existen tablas pero con `0` registros en capas que deberían tener actividad viva

### Tablas mínimas para validar contexto

- `pos_bridge_daily_sales`
- `ventas_ventaautoritativapoint`
- `orquestacion_orchestrationrun`
- `recetas_movimientoproductocedis`

### Interpretación

- Si faltan las tablas: no estás en la base ERP correcta.
- Si las tablas existen pero todo está en cero: probablemente estás en una base de laboratorio, validación o snapshot vacío.
- Si el repo apunta a PostgreSQL local por `.env`, no asumir que eso equivale al PostgreSQL vivo del VPS.

## Evidencia local verificada en esta fecha

### Base activa en esta terminal

- `NAME=pollyana_db`
- `HOST=127.0.0.1`
- `PORT=5432`

### Bases PostgreSQL locales visibles

- `ad_agent`
- `agente_dg`
- `pastelerias_chat_native_validate`
- `pollyana_db`
- `postgres`
- `test_pollyana_db`

### Lectura operativa

- La terminal sí está en PostgreSQL.
- Eso no garantiza que esté en la base viva del ERP.
- Antes de extraer ventas, producción o mermas de marzo 2026 o posteriores, se debe validar la conexión real a la base viva o ejecutar dentro del VPS.

## Orden recomendado para cualquier análisis nuevo

1. correr `./scripts/diagnose_erp_runtime_context.sh`
2. validar si la base activa es la viva del ERP o una local
3. revisar `docs/CANONICIDAD_VENTAS_ERP.md` si el tema toca ventas
4. revisar `docs/LINEA_BASE_REAL_AGENTES_ERP.md` si el tema toca agentes/runtime
5. revisar `docs/OPERACION_RUTINA_DIARIA_ERP.md` si el tema toca sincronización o cierres
6. solo después extraer datos o construir reportes

## Datos faltantes para cerrar el circuito al 100%

Hoy este repo no trae visible en la shell actual:

- el `DATABASE_URL` vivo de la base operativa
- una marca única dentro del repo que diga “esta es la base productiva real de este hilo”

Hasta que eso no exista, un hilo nuevo todavía puede quedar en una base local correcta técnicamente, pero equivocada operativamente.

## Siguiente mejora recomendada

1. dejar disponible la conexión PostgreSQL del VPS en la ruta operativa aprobada
2. agregar este chequeo al arranque de comandos sensibles
3. no permitir comparativas ejecutivas si el diagnóstico detecta base local no viva

## Rollback

- borrar este documento si se reemplaza por una guía oficial mejor
- borrar `scripts/diagnose_erp_runtime_context.sh` si más adelante se centraliza el diagnóstico dentro de un management command oficial
