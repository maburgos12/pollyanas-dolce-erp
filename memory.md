# Memory

## Proposito

Memoria operativa estable del proyecto de agentes ERP de Pollyana's Dolce.

Este archivo no es bitacora diaria ni transcript. Su funcion es conservar solo hechos confirmados, decisiones estables y errores recurrentes que deben evitarse en futuras iteraciones del repo.

## Lectura recomendada

Leer despues de:

1. `AGENTS.md`
2. `.agent/skills/README.md`
3. `.agent/skills/00-core/skill-erp-context/SKILL.md`
4. `.agent/skills/00-core/skill-director-general-mode/SKILL.md`

## Hechos estables confirmados

- El ERP Django/PostgreSQL es la unica fuente de verdad del negocio.
- Prioridades permanentes: exactitud de inventario, vida util de 2 dias, conciliacion, trazabilidad, RBAC y consistencia de reportes.
- Pollyana's Dolce opera con 9 sucursales activas reales; `MATRIZDBG` no es una sucursal operativa real y debe excluirse de completitud, same-store y forecast comercial. `GUAMUCHIL` abrió en marzo de 2026 y debe tratarse como sucursal nueva, no madura.
- Para completitud diaria de ventas, ausencia de filas producto en el canónico no equivale automáticamente a error. Si existe `PointDailyBranchIndicator` para la sucursal/fecha, o hay evidencia de backfill oficial exitoso para esa sucursal/fecha aunque haya importado `0` renglones, o negocio valida cierre/no apertura/falla operativa, debe tratarse como cero válido; solo es hueco real cuando hay evidencia de venta no cargada.
- Domingo no tiene produccion ni distribucion, pero si hay ventas.
- El sistema actual de agentes es un orquestador nativo parcial con gateway controlado, no un sistema completo de agentes autonomos con memoria cargada por runtime.
- Al corte del snapshot actual hay 6 agentes declarados, 32 capacidades declaradas, 12 reglas declaradas, 5 runners soportados, 5 goal handlers del runtime minimo, 14 tools reales del gateway y 4 schedules reales de orquestacion.
- Existe un runtime local compartido en `orquestacion` que ya carga contexto Markdown, `memory.md`, tools y checkpoints para multiples agentes especializados sobre `ventas.EventoVenta`.
- El `costo estimado` de compras automáticas no sale de Point en vivo; se calcula con el último `CostoInsumo` vigente en ERP, por lo que un `$0` normalmente significa falta de costo persistido o match ERP, no necesariamente costo cero en Point.
- El loop oficial para poblar costo unitario desde Point ya queda pegado al sync diario completo de inventario: después del inventario principal, el ERP revisa `Point -> Existencias -> ALMACEN` y persiste costos reconocidos en `CostoInsumo`, con summary y warnings en el mismo `PointSyncJob`.
- La memoria transaccional actual vive en corridas, tareas, sugerencias y ejecuciones del bounded context `orquestacion`, pero eso no equivale a memoria reutilizable.
- `AgentDefinition.allowed_tools_json` y las tools del AI Gateway no tienen match exacto literal al corte actual; aun asi, ya existe una capa minima de alias para resolver equivalencias claras `api.* -> erp.*` dentro del runtime.
- La memoria ya no es solo lectura: existe una ruta de escritura controlada con evidencia, deduplicacion y `AuditLog` via `orquestacion.memory_control` y el comando `record_agent_memory`.
- El ERP ya tiene bandeja manual de propuestas de memoria en `Orquestacion > Propuestas de memoria`, respaldada por `MemoryProposal`, aprobacion humana y aplicacion controlada a `memory.md`.
- La autoaprobacion de memoria ya existe de forma minima y apagable, limitada por allowlist a ciertos gaps tecnicos de binding con evidencia, alta confianza y repeticion minima; no autoescribe `memory.md`.
- Existe un loop gobernado de enforcement de ventas con tres detectores seguros al corte actual: `direct_pointdailysale_reader`, `protected_raw_sales_reader` y `sales_publication_gap`; registra `QualityFinding`, puede proponer `MemoryProposal` solo en violaciones arquitectónicas repetidas, crea `RemediationProposal` y valida cierre por rerun.
- `sales_publication_gap` no debe abrir hallazgos mientras exista un sync de ventas activo para la fecha de referencia; primero se difiere y solo se vuelve hallazgo si el rezago persiste tras terminar el sync.
- El loop de calidad de ventas ya es visible y operable desde `Orquestacion`: existe bandeja ERP para hallazgos/remediaciones y wrapper oficial `./scripts/run_quality_loop.sh`; la migracion requerida del loop ya fue aplicada en la base local.
- El forecast ejecutivo profesional de eventos estacionales ya no debe resolverse con caps planos ni uplifts manuales: se construye con benchmark comparable del evento + same-store real por sucursal comparable + expansión incremental de sucursales nuevas + contracción por sucursales cerradas o fuera de alcance + mix por familia/SKU, manteniendo siempre `precio real vigente por SKU x piezas forecast por SKU` para el ingreso final.
- Sucursal nueva y sucursal cerrada son casos de primer nivel del motor de forecast; no deben tratarse como excepciones improvisadas ni contaminar el ratio same-store.
- La regla anterior ya escala a todo forecast comercial del ERP: eventos, campañas, semanas promocionales y planeación por sucursal deben seguir la misma descomposición base comparable + same-store + expansión + contracción + mix + temporalidad, con ingreso final siempre igual a `precio real vigente x piezas forecast`.
- La unidad de verdad del forecast comercial ya no puede ser el ingreso agregado ni el precio promedio: la proyección correcta nace en `SKU x sucursal x día`, primero en piezas y solo al final en dinero con el precio real actual de cada SKU.
- La historia diaria canónica suficiente para forecast ya existe en PostgreSQL (`reportes_factventadiaria` / `pos_bridge_daily_sales`) desde 2022; `pos_bridge_sales_daily_product_fact` es parcial y no debe usarse como único histórico largo por producto.
- Para eventos comerciales, el agente persistente correcto sigue este orden: objetivo -> autonomia/loop -> contexto Markdown -> `memory.md` -> herramientas (`skills` y MCP).
- El loop de publicacion de eventos no debe aceptar etiquetas de sucursal ni promover estados sin validar taxonomia de `base_method`, artifacts activos y workflow real.
- El runtime real implementado vive en `orquestacion.services.agent_runtime`, usa un loop comun y hoy soporta como goals operativos: `sales_event_publication_guard`, `production_readiness_guard`, `purchase_review_guard`, `reconciliation_guard` y `operational_chain_review`.
- `director_operativo` ya funciona como coordinador/orquestador del chain minimo `publicacion -> produccion -> compras`, con delegaciones persistidas en `AgentGoalDelegation`.
- El coordinador multi-agente ya quedó integrado también al circuito formal de reglas mediante `sales_event_operational_chain_review`, con soporte en `run_orchestration_rule` y `task_run_rule`.

## Errores recurrentes a evitar

- No afirmar que existe `memory.md` integrada al runtime solo porque existen campos JSON en modelos.
- No confundir skills documentales con runtime operativo de agentes.
- No llamar automatizacion a una sugerencia manual o a una respuesta repetible sin runner, trigger y trazabilidad.
- No afirmar que un runtime legado externo ya trabaja solo si todavia depende de pasos manuales o de aprobacion humana.
- No asumir que una regla declarada en `orquestacion/catalog.py` tiene runner implementado.
- No asumir que las tools del gateway estan ligadas formalmente a cada `AgentDefinition`.
- No inventar evidencia cuando una afirmacion no puede apuntar a archivo, endpoint, test o snapshot verificable.
- No afirmar que compras automáticas “lee costo desde Point” si el cálculo real depende de `CostoInsumo` persistido en ERP.
- No aceptar como valida una etiqueta UI de sucursal si contradice el `base_method` dominante real del forecast.
- No dejar que helpers de generacion de compras muevan el evento a Compras automaticamente.
- No dar por confiable un forecast de evento comercial solo porque el pipeline cerro sin error; primero contrastarlo contra el homologo real cuando exista un pico historico fuerte.
- El ingreso ejecutivo de un evento se calcula con la regla simple y obligatoria `precio real vigente por SKU x piezas forecast por SKU`; el homologo solo sirve como alerta ejecutiva de revision, no para reescribir automaticamente el ingreso.
- Si el ingreso semanal proyectado queda materialmente por encima de la referencia ejecutiva defendible del homologo, el ERP no debe permitir envio a aprobacion ni aprobacion; primero se corrigen las piezas forecast o se reconcilia el benchmark ejecutivo.
- Si Dirección ya definió un benchmark ejecutivo explicito del evento, ese benchmark debe quedar persistido en `objective_notes` y la plausibilidad financiera debe respetarlo como techo operativo antes de aprobar o publicar.
- No volver a usar indicadores históricos incompletos como señal same-store suficiente; si la cobertura del indicador no es comparable, el motor debe caer al ratio YTD comparable del evento antes de aceptar un crecimiento por sucursal.
- La expansión de una sucursal nueva no debe salir del histórico crudo del donor branch; primero se ajusta la sucursal donadora con same-store real y después se aplica la madurez incremental de la sucursal nueva.
- Nunca meter sucursal nueva dentro del same-store ni usar promedio ciego para proyectarla; siempre modelarla como expansión explícita con sucursal donadora/comparable y factor de madurez o capacidad.
- Nunca dejar sucursal cerrada, desactivada o fuera de alcance contaminando el forecast actual; su contribución base se elimina como contracción explícita.
- El día principal del evento debe seguir la curva histórica real del homólogo por sucursal; si la sucursal es nueva, usar la curva de su sucursal comparable, y si no existe historia suficiente, caer al perfil global del evento antes de aceptar una curva plana.
- Los días previos al pico no pueden quedar “por residuo” después de fijar el día principal; el pre-pico debe seguir una banda histórica defendible por cantidades, apoyada en homólogo comparable y tendencia reciente por weekday.
- No mezclar benchmark DG semanal con benchmark DG de día principal; son anclas distintas y deben quedar trazadas por separado en payload, explicación y guardrails.
- No defender un forecast solo porque el total semanal se ve bien si el día fuerte queda sobrecomprimido frente a su banda histórica, ni defender el día fuerte si la semana ignora la caída real del negocio.
- Si el día principal queda razonable pero `día -1`, `día -2` o `día -3` quedan artificialmente hundidos o inflados, el forecast sigue mal; hay que corregir la curva diaria, no solo el pico o la semana total.
- No dar por aplicado un forecast ejecutivo de evento solo porque el día principal quedó bien; la semana total también debe respetar el `target_total_qty` defendible del modelo ejecutivo por sucursal antes de publicar o aprobar.
- Si un evento ya estaba en `LISTO_PARA_REVISION` o `PENDIENTE_DIRECCION` y el recálculo vuelve a disparar el guard por semana total o día fuerte, debe regresar automáticamente a `EN_MODELADO`; persistido no significa aplicado.
- El forecast ejecutivo de eventos debe considerar solo los productos seleccionados del evento que pertenezcan al scope comercial principal; accesorios, bebidas de reventa y recetas con `modo_costeo=SERVICIO_ACCESORIO` se excluyen, pero postres vendidos como `Vasos Preparados` sí entran cuando son producto real del evento.
- Si el ERP se empieza a calentar durante recalculos, primero limpiar procesos pesados del repo con `scripts/cleanup_erp_forecast_processes.sh`; no volver a encadenar shells largos de `manage.py shell -c` sobre eventos vivos sin necesidad.
- No volver a reportar un forecast de evento como "aplicado", "cerrado" o "corregido" solo porque el código ya cambió; solo cuenta como aplicado cuando el recálculo realmente persiste nuevos valores, actualiza snapshot/estado y esos valores se verifican en el ERP.
- No volver a aceptar `MATRIZDBG` como sucursal real en monitoreo, completitud o benchmark; si aparece en Point/ERP debe tratarse como alias técnico de Matriz y excluirse del conteo de sucursales activas esperadas.
- No volver a tratar `GUAMUCHIL` como sucursal madura en same-store ni exigirle profundidad histórica larga; primero se modela como expansión reciente con donadora/comparable.
- Antes de dar por cuerdo un evento estacional, correr `./.venv/bin/python manage.py audit_seasonal_event_forecasts --enforce-status --write-report`; ese comando ejecuta 10 revisiones y compara 2026 vs 2025 por día y por top productos antes de declarar cierre.
- Para la vigilancia recurrente del loop comercial completo, correr `./.venv/bin/python manage.py audit_commercial_forecast_loop --days-back 30 --write-report`; ese comando revisa completitud diaria válida, alineación forecast/snapshot/artifacts y luego ejecuta la auditoría estacional 10/10.
- La SOP y tabla de control permanente del loop comercial quedaron en `docs/commercial_forecast_loop_sop.md`.
- Si el modelo ejecutivo ya calcula `branch_targets` / `target_total_qty` más bajos que el forecast persistido, el motor debe forzar un techo semanal final y luego revalidar el día principal; no dejar esa diferencia solo como alerta visual.
- No tratar el `DIADELPADRE-260621-001` como forecast validado hasta recalibrarlo contra el homologo real del domingo `2025-06-15`, donde la misma canasta/sucursales observó `2131` unidades y `$460,758.00`, muy por encima del forecast actual del dia principal 2026.
- Para eventos movibles, no usar solo la misma fecha calendario del año previo como homologo; primero se debe evaluar el homologo por misma ocurrencia de weekday dentro del mes y elegir el ancla con mayor señal historica real.
- La calibracion contra homologo no debe tener solo techo a la baja; tambien debe tener piso al alza contra el homologo ajustado por YTD, especialmente sobre el dia principal del evento.
- No asumir que extraer ventas a PointDailySale actualiza automaticamente el ERP visible; si no se publica FactVentaDiaria y no se invalidan los scopes ventas+dashboard, el sistema puede seguir mostrando cortes atrasados.
- No volver a duplicar en `core` y `reportes` la decision de fuente visible de ventas; la resolución canónica compartida ya vive en `ventas/services/sales_canonical_source.py`.
- No permitir que `api/ai_gateway_services.py` ni consultas operativas de agentes vuelvan a responder ventas desde lecturas directas de `PointDailySale`; deben pasar por `ventas/services/sales_read_service.py`.
- No reintroducir lectores visibles u operativos de `PointDailySale` fuera de la allowlist tecnica; `scripts/check_pointdailysale_usage.py` debe pasar antes de considerar valida una iteracion local.
- No reintroducir lectores crudos de ventas en archivos protegidos del ERP visible, gateway o consultas operativas; `scripts/check_protected_sales_readers.py` debe pasar antes de considerar valida una iteracion local.
- No llamar `paquete completo` al ZIP de proyeccion de eventos si no incluye los cuatro archivos ERP vigentes: semana, dia exacto, proyeccion por dia y dashboard ejecutivo.
- En preguntas ejecutivas de DG sobre gasto, forecast, compras, inventario, faltantes, margen o cierre de mes, la respuesta no debe quedarse solo en "no se puede confirmar" si existen proxies operativos en ERP.
- En esas preguntas ejecutivas, la estructura minima obligatoria es: `Resumen ejecutivo`, `Hecho auditado`, `Estimado operativo`, `Riesgo / interpretacion` y `Siguiente accion recomendada`.
- No confundir `comprometido en ERP` con `forecast operativo`; si compras registradas = 0, eso no autoriza responder que el gasto esperado real tambien es 0.
- Si falta un KPI perfecto o una tabla explicita de forecast, el agente debe intentar estimar con proxies operativos disponibles: ventas recientes, consumo reciente, inventario actual, stock bajo, recetas/BOM, dias restantes y compras pendientes.
- Si aun asi no hay base suficiente para una cifra seria, la respuesta debe decir explicitamente que si esta confirmado, que no esta confirmado y cual es el siguiente calculo operativo a ejecutar; no basta con responder "indeterminado" sin accion.


## Gaps estables confirmados

- Falta escritura automatica de memoria de largo plazo de vuelta a `memory.md`; hoy el runtime solo propone ciertos gaps tecnicos y la aplicacion sigue siendo aprobada.
- Falta extender las propuestas automaticas mas alla de gaps tecnicos de binding y decidir si alguna categoria tecnica muy segura amerita autoaplicacion futura.
- Falta contrato fuerte y cobertura total de capacidad efectiva entre catalogo, runtime, gateway y scheduler.
- Falta binding formal entre `system_prompt_version`, memoria, tools efectivas y loop operativo por agente.
- Falta extender el runtime multi-agente mas alla del bounded context `ventas.EventoVenta`.
- Falta scheduler persistente y binding MCP nativo dentro del runtime compartido.
- La cobertura de automatizacion sigue siendo parcial.
- El caso `Dia del Padre` quedó cerrado como regla permanente del motor: para eventos movibles con homologo fuerte no se puede usar solo la fecha calendario del año previo; se debe evaluar `calendar` vs `weekday_occurrence`, elegir el homologo con mayor señal histórica y calibrar con `cap` a la baja y `floor` al alza.
- El ciclo oficial para eventos estacionales queda: seleccion de canasta/sucursales -> forecast base -> tendencia/mix/sustitucion -> seleccion robusta de homologo -> calibracion con techo y piso -> rebalance del dia principal -> build_financials -> persistencia de artifacts -> approval guard.
- El rebalance del dia principal no puede ser un ajuste débil contra el día previo: debe redistribuir la semana usando el peso histórico de ventas del homólogo del evento para que el pico comercial quede defendible.
- El dia principal de un evento no se puede dejar solo con la curva semanal reacomodada si despues del alineamiento ejecutivo sigue subestimado; el motor debe aplicar un piso final por sucursal contra el homólogo fuerte y, si DG deja benchmark explícito del dia principal, también debe respetar esa participación sobre el benchmark semanal.
- El approval guard debe bloquear envios/aprobaciones cuando el dia principal quede materialmente por debajo del homologo ajustado sin justificacion suficiente; debe registrar notificacion critica, audit trail y trazabilidad de `homologue_mode`, `homologue_main_day`, `homologue_ytd_factor` y aplicacion de `cap/floor`.
- En eventos comerciales, `financials`, `views`, `event_detail_snapshot` y `tests` forman el paquete ERP minimo cuando se corrige plausibilidad de ingresos o publicacion ejecutiva; no deben moverse por separado.
- Si la correccion toca el modelo ejecutivo de forecast del evento, el paquete ERP minimo sube a cinco archivos: `forecasting`, `financials`, `views`, `event_detail_snapshot` y `tests`.
- La ruta oficial de pruebas locales usa PostgreSQL con `config.settings_test` y `./scripts/run_tests_local.sh`.
- SQLite queda solo como fallback explicito de laboratorio y no debe considerarse backend valido para la suite general porque `reportes` usa `MATERIALIZED VIEW`.
- La lectura de ventas sigue parcialmente fragmentada entre PointDailySale, FactVentaDiaria y VentaHistorica; falta estandarizar una fuente canónica por contexto para que todo el ERP refleje la misma publicación actualizada.
- El reproceso oficial de eventos no estaba refrescando `detail_snapshot`; si un reproceso recalcula forecast/financials/artifacts pero no actualiza snapshot, el resultado sigue incompleto y la auditoría puede ver ceros o versiones viejas.
- El sync/backfill oficial de ventas Point debe fallar rápido si falta `POINT_BASE_URL`; no se debe aceptar un error opaco tipo `Invalid URL '/'` como si fuera un problema de datos. Además, la selección canónica de branches de Point debe excluir alias técnicos como `MATRIZDBG`.
- `api/ai_gateway_services.py` y `pos_bridge/services/agent_query_service.py` ya quedaron alineados a la capa compartida para consultas operativas de ventas.
- `ventas/services/financials.py` sigue siendo una excepción intencional: combina ventas canónicas de rango con señales históricas/oficiales de precio porque su objetivo es pricing y cobertura, no el tablero visible del día.
- `ventas/services/postmortem.py` ya no usa `PointDailySale` directo como fallback; ahora toma fallback canónico vía `get_daily_sales`.
- El loop completo `detectar -> registrar hallazgo -> proponer memoria -> proponer remediacion -> validar cierre` ya queda cerrado para `direct_pointdailysale_reader`, `protected_raw_sales_reader` y `sales_publication_gap`.
- No existe hoy una política suficientemente segura para un detector global de `VentaHistorica` o `FactVentaDiaria`; esos contextos siguen mezclando histórico ejecutivo, pricing, forecasting, postmortem, publicación y diagnóstico legítimo.


## Politica de actualizacion

Actualizar este archivo solo cuando se cumpla al menos una condicion:

- se confirme un hecho estable nuevo con evidencia en repo
- se cierre un gap estructural relevante
- se detecte un error recurrente que ya tenga prevencion clara
- se cambie una regla de negocio no negociable confirmada por Direccion

No actualizar este archivo con:

- logs crudos
- transcript de conversaciones
- tareas temporales
- hipotesis no confirmadas
- promesas de arquitectura aun no implementadas
