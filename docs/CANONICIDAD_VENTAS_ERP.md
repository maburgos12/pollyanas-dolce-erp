# Canonicidad de Ventas ERP

Fecha de corte: 2026-04-12

## Resumen ejecutivo

El problema estructural no estaba en la extracción de ventas desde Point sino en la cadena completa de publicación y consumo visible del ERP. Ya quedó alineada la lectura prioritaria para Dirección General y BI sobre una política compartida:

1. extracción desde Point
2. staging validado
3. publicación canónica
4. refresco de derivados
5. invalidación de cache
6. consumo desde la fuente correcta según contexto

La resolución de fuente visible ya no depende de lógica duplicada entre `core` y `reportes`. Ambas capas consumen ahora una librería compartida en [sales_canonical_source.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/sales_canonical_source.py).

## Diagnóstico estructural

Antes de esta iteración:

- `PointDailySale` podía estar al día mientras el ERP visible seguía mostrando un corte atrasado.
- `core/views.py` y `reportes/views.py` mantenían lógica duplicada para decidir si leer `PointDailySale`, `VentaHistorica` o facts publicados.
- el dataset de dashboard podía quedarse sin facts diarios y aun así devolver `today` como fecha visible, aunque ya existiera publicación canónica Point en `VentaAutoritativaPoint` o facts V2.

Después de esta iteración:

- `core` y `reportes` comparten la misma resolución de fuente.
- el dataset diario del dashboard degrada a publicación canónica Point cuando todavía no existe `FactVentaDiaria`.
- el dashboard ejecutivo y BI vuelven a reflejar la misma publicación canónica, con distinta agregación según contexto.

## Política canónica por contexto

### 1. Dashboard ejecutivo DG

- Fuente objetivo: publicación operativa canónica del día.
- Resolución:
  - preferir `reportes.dashboard_sales_dataset`
  - si no hay `FactVentaDiaria` ni `CorteOficialDiario`, usar fallback canónico Point (`VentaAutoritativaPoint` -> `PointSalesDaily*Fact` -> `PointDailySale`)
- Monto visible:
  - usar `raw_total_amount` cuando no existe corte oficial conciliado
  - no mezclar ticket/indicador como si fuera cierre oficial

### 2. BI ventas / BI ejecutivo

- Fuente objetivo: facts analíticos y corte oficial diario cuando exista.
- Resolución:
  - `reportes.dashboard_sales_dataset`
  - fallback canónico Point solo cuando todavía no existe fact diaria publicada
- Monto visible:
  - puede usar monto conciliado/indicadores del día
  - conserva `raw_total_amount` para trazabilidad

### 3. Histórico ejecutivo / planeación / comparables

- Fuente objetivo:
  - `FactVentaDiaria` si está limpia
  - si no, publicación canónica Point (`VentaAutoritativaPoint` / facts V2 / legacy)
  - si no existe publicación Point, `VentaHistorica` conciliada (`POINT_BRIDGE_SALES`)
  - `VentaHistorica` no canónica solo como último fallback referencial

### 4. Monitoreo de integraciones

- Puede leer múltiples capas a la vez.
- Justificación: su función es diagnóstica, no mostrar una única verdad de negocio.
- Debe seguir mostrando:
  - `PointDailySale`
  - `FactVentaDiaria`
  - snapshot visible del dashboard

### 5. API / Gateway / consultas de agentes

- Fuente objetivo futura: servicio compartido canónico.
- Estado actual: todavía hay lectores directos a `PointDailySale` y mezclas con `VentaHistorica`.
- No deben declararse cerrados todavía.

### 6. Postmortem / histórico de producto

- Puede usar históricos reconciliados por contexto.
- No debe usarse como fuente operativa visible del día.

## Matriz de fuentes de ventas

| Modulo / pantalla / servicio | Archivo | Funcion o vista | Fuente actual | Tipo de fuente actual | Fuente canonica objetivo | Motivo | Riesgo actual | Cambio requerido | Estado |
|---|---|---|---|---|---|---|---|---|---|
| Dashboard ejecutivo DG | [core/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/core/views.py) | `_sales_source_context` | `ventas.services.sales_canonical_source` | Resolucion compartida | `sales_canonical_source` | Evitar criterio duplicado entre DG y BI | Bajo | Ya unificado | alineado |
| Dashboard ejecutivo DG | [core/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/core/views.py) | `_build_dashboard_sales_history_summary` | `sales_canonical_source` + `get_sales_range` | Publicacion canónica / fallback histórico | Igual | El histórico visible ya no debe depender de reglas duplicadas | Bajo | Ya unificado | alineado |
| Dashboard ejecutivo DG | [core/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/core/views.py) | `_build_dashboard_daily_sales_snapshot` | `reportes.dashboard_sales_dataset` con fallback canónico | Dataset visible + fallback publicado | Igual | El tablero visible debe reflejar corte canónico aun cuando falte `FactVentaDiaria` | Bajo | Ya corregido | alineado |
| BI ventas / BI ejecutivo | [reportes/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/views.py) | `_sales_source_context` | `ventas.services.sales_canonical_source` | Resolucion compartida | `sales_canonical_source` | DG y BI deben hablar la misma verdad | Bajo | Ya unificado | alineado |
| BI ventas / BI ejecutivo | [reportes/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/views.py) | `_ventas_historicas_bi_summary` | `sales_canonical_source` + `get_sales_range` | Publicacion canónica / fallback histórico | Igual | Mantener comparables y cobertura visibles con misma política que DG | Bajo | Ya unificado | alineado |
| BI ventas / BI ejecutivo | [reportes/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/views.py) | `_bi_daily_sales_snapshot` | `reportes.dashboard_sales_dataset` con fallback canónico | Dataset visible + fallback publicado | Igual | BI diario debe degradar con seguridad cuando falta fact | Bajo | Ya corregido | alineado |
| Dataset diario compartido | [reportes/dashboard_sales_dataset.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/dashboard_sales_dataset.py) | `get_dashboard_sales_dataset` | `FactVentaDiaria` + `CorteOficialDiario` + fallback Point canónico | Publicación analítica | Igual | La fecha visible no debe caer en `today` vacío si hay datos canónicos publicados | Bajo | Ya corregido | alineado |
| Servicio canónico de lectura | [ventas/services/sales_read_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/sales_read_service.py) | `get_daily_sales`, `get_sales_range`, `get_daily_sales_bulk` | Facts limpios -> authoritative -> V2 -> legacy | Capa reusable canónica | Igual | Fundación para unificar lectores | Bajo | Ya existía; se consolida como base oficial | alineado |
| Monitor de integraciones | [integraciones/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/integraciones/views.py) | `_build_sales_monitor_cards` | `PointDailySale`, `FactVentaDiaria`, `get_dashboard_sales_dataset` | Diagnóstico multicapa | Diagnóstico multicapa | Aquí sí se necesitan varias capas para detectar rezagos | Bajo | Sin cambio; intencional | intencional |
| Gateway ERP | [api/ai_gateway_services.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/api/ai_gateway_services.py) | `_handle_sales_summary`, `_handle_sales_by_branch`, `_handle_sales_trends` | `sales_read_service` + agregados canónicos por rango | Publicación canónica compartida | Servicio compartido canónico | El gateway ya no debe responder desde staging accidental | Bajo | Ya migrado | alineado |
| Consultas operativas de agentes | [pos_bridge/services/agent_query_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/services/agent_query_service.py) | `_execute_sales_*` | `sales_read_service` para ventas canónicas + guardas de reconciliación histórica | Canónico con bloqueo cuando el histórico no es oficial | Servicio compartido canónico | Los agentes ya no deben responder distinto a DG/BI por leer staging directo | Bajo | Ya migrado | alineado |
| Forecast ejecutivo de eventos | [ventas/services/forecasting.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/forecasting.py) | `build_event_executive_projection_model`, `_align_forecast_to_executive_branch_model`, `_enforce_weekly_executive_ceiling`, `_enforce_main_day_peak_floor` | Benchmark comparable del evento + same-store + expansión + contracción + mix | Modelo ejecutivo auditable | Política explícita del evento | El total del evento no debe salir ni de caps planos ni de uplifts manuales; primero se fuerza techo semanal y luego piso del día principal | Bajo/medio | Mantener trazabilidad completa en explanation/snapshot/UI | alineado |
| Financiero / utilidades | [ventas/services/financials.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/financials.py) | helpers de costos y precios | `get_sales_range` para agregados + `VentaHistorica` / `PointDailySale` oficial para precio vigente | Mixto intencional por cálculo | Política explícita por cálculo | Algunas consultas buscan precio vigente y cobertura histórica, no dashboard diario | Bajo/medio | Mantener como excepción documentada; no cambiar a una sola capa por comodidad | excepción intencional |
| Postmortem de producto | [ventas/services/postmortem.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/postmortem.py) | `build_postmortem_*` | `VentaHistorica` primaria + fallback `get_daily_sales` canónico | Histórico/postmortem con fallback canónico | Histórico reconciliado | No es lector visible del día, pero su fallback ya no debe tomar staging crudo | Bajo | Ya alineado el fallback | alineado |

## Qué ocurre después de una extracción/publicación exitosa

El flujo correcto del ERP queda así:

1. `PointDailySale` recibe extracción de Point.
2. El sync diario intenta publicar analytics y refresh incremental.
3. `FactVentaDiaria` y derivados se reconstruyen si hay rezago.
4. Se invalidan scopes `ventas` y `dashboard`.
5. `core` y `reportes` resuelven la fuente desde `sales_canonical_source`.
6. Si aún no hay `FactVentaDiaria`, el dataset diario degrada a la publicación canónica Point.
7. La UI visible consume el dato actualizado sin pedir intervención manual.

## Evidencia de validación

Pruebas ejecutadas:

```bash
./scripts/run_tests_local.sh \
  core.tests.DashboardHomologacionContextTests.test_sales_source_context_detects_canonical_point_date_without_stage \
  core.tests.DashboardHomologacionContextTests.test_sales_source_context_keeps_stage_latest_date_when_stage_lags_canonical \
  core.tests.DashboardHomologacionContextTests.test_dashboard_sales_history_summary_uses_canonical_source_when_stage_is_missing \
  core.tests.DashboardHomologacionContextTests.test_dashboard_sales_history_summary_mentions_canonical_date_when_stage_lags \
  core.tests.DashboardHomologacionContextTests.test_dashboard_daily_sales_snapshot_uses_canonical_daily_totals_and_tops_without_stage \
  core.tests.DashboardHomologacionContextTests.test_dashboard_daily_sales_snapshot_uses_canonical_previous_day_comparison_when_stage_missing \
  reportes.tests.ReportesBITests.test_sales_source_context_detects_canonical_point_date_without_stage \
  reportes.tests.ReportesBITests.test_sales_source_context_keeps_stage_latest_date_when_stage_lags_canonical \
  reportes.tests.ReportesBITests.test_ventas_historicas_bi_summary_uses_canonical_source_when_stage_is_missing \
  reportes.tests.ReportesBITests.test_ventas_historicas_bi_summary_mentions_canonical_date_when_stage_lags \
  reportes.tests.ReportesBITests.test_bi_daily_sales_snapshot_uses_canonical_daily_totals_and_tops_without_stage \
  reportes.tests.ReportesBITests.test_bi_daily_sales_snapshot_uses_canonical_previous_day_comparison_when_stage_missing \
  reportes.tests_analytics_refresh \
  pos_bridge.tests.test_sales_sync_tasks
```

Resultado:

- 36 pruebas en verde sobre PostgreSQL.
- Cubren:
  - resolución canónica de fuente
  - histórico ejecutivo y BI
  - snapshot diario ejecutivo y BI
  - fallback canónico cuando falta `FactVentaDiaria`
  - publicación e invalidación de analytics

Validación adicional de cierre:

```bash
./scripts/run_tests_local.sh api.tests_ai_gateway pos_bridge.tests.test_agent_query_service ventas.tests.VentasEventosServiceTests.test_build_postmortem_compares_forecast_against_actual_sales ventas.tests.VentasEventosServiceTests.test_build_postmortem_uses_canonical_daily_sales_when_history_is_missing
```

Resultado:

- 35 pruebas en verde sobre PostgreSQL.
- Cubren:
  - tools de ventas del AI Gateway
  - consultas operativas de agentes
  - preferencia por facts canónicos sobre staging directo
  - fallback canónico de `postmortem`

## Qué quedó realmente resuelto

- DG y BI ya no dependen de dos implementaciones distintas para decidir “de dónde leer ventas”.
- El snapshot visible ya no cae en `today` vacío cuando existen hechos canónicos Point.
- La publicación visible del dashboard diario ya degrada de forma controlada a la capa canónica Point.
- El dashboard ejecutivo y BI ya consumen la misma publicación canónica, con distinta agregación de monto según el contexto.
- El forecast ejecutivo de eventos ya usa una política explícita de cohortes: benchmark comparable del evento + same-store real + expansión incremental + contracción estructural + pricing real al final.
- La publicación del forecast ejecutivo ya no puede cerrar solo con el día principal “bien”; la semana total también debe respetar el `target_total_qty` del modelo antes de aprobar/publicar, y después se revalida el pico principal.
- La distribución diaria del evento también es canónica: el día principal debe respetar el peso histórico real del homólogo y no puede quedar aplanado por una regla genérica de rebalance.
- Si Dirección fija benchmark explícito del día principal, ese pico debe heredarse como participación gobernada sobre el benchmark semanal y dejar trazabilidad visible en el snapshot ejecutivo.
- Una sucursal nueva ya no se trata como “cero histórico” ni como promedio ciego: toma una sucursal donadora y aplica una madurez incremental explícita.
- Una sucursal cerrada o fuera de alcance ya no debe sobrevivir implícitamente en el benchmark del evento; su aporte queda como contracción estructural trazable.

## Qué no quedó resuelto

- `financials.py` sigue siendo una excepción deliberada: mezcla fuentes porque su objetivo es costo/precio vigente y cobertura histórica, no el tablero visible del día.
- no existe un detector global repo-wide para cualquier uso de `VentaHistorica` o `FactVentaDiaria`, porque hoy mezclaría contextos legítimos de histórico, pricing, forecasting, publicación y diagnóstico.

## Riesgos

- Si se asume que el gateway ya quedó alineado, se reintroduce una segunda verdad de ventas para agentes.
- Si futuras pantallas vuelven a consultar `PointDailySale` directo, se romperá otra vez la coherencia visible.
- El fallback diario canónico corrige visibilidad, pero no sustituye la necesidad de seguir publicando `FactVentaDiaria` con disciplina.

## Rollback

- Revertir [sales_canonical_source.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/sales_canonical_source.py)
- Revertir wrappers en [core/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/core/views.py) y [reportes/views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/views.py)
- Revertir fallback canónico en [dashboard_sales_dataset.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/dashboard_sales_dataset.py)

## Cierre del alcance

La política canónica de ventas ya quedó respaldada por enforcement real en las rutas de mayor impacto:

- guard global para `PointDailySale`
- guard de rutas protegidas para gateway, dataset visible y consultas operativas
- detector operativo de `sales_publication_gap`

El detector `sales_publication_gap` no abre hallazgo mientras exista un sync de ventas activo para la fecha de referencia; así evita ruido durante cierres que todavía están en curso.

No queda un siguiente paso técnico obvio dentro de esta misma línea sin antes definir una política nueva por contexto para fuentes históricas o financieras.
