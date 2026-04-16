# Alcance Exacto del Enforcement de Ventas

Fecha de corte: 2026-04-12

## Objetivo

Delimitar con precision los archivos que pertenecen al bloque de enforcement de ventas para reducir el riesgo de mezclar esta linea con cambios no relacionados dentro de un repo sucio.

## Perimetro principal del bloque

- [pointdailysale_guard.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/pointdailysale_guard.py)
- [protected_sales_reader_guard.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/protected_sales_reader_guard.py)
- [sales_publication_guard.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/sales_publication_guard.py)
- [quality_guard_runner.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/quality_guard_runner.py)
- [quality_findings.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/services/quality_findings.py)
- [run_quality_guards.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/management/commands/run_quality_guards.py)
- [views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/views.py)
- [tests_quality_loop.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/orquestacion/tests_quality_loop.py)
- [check_pointdailysale_usage.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/check_pointdailysale_usage.py)
- [check_protected_sales_readers.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/check_protected_sales_readers.py)
- [run_tests_local.sh](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/run_tests_local.sh)
- [run_quality_loop.sh](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/scripts/run_quality_loop.sh)
- [quality_findings.html](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/templates/orquestacion/quality_findings.html)
- [quality_finding_detail.html](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/templates/orquestacion/quality_finding_detail.html)
- [LOOP_CALIDAD_Y_MEMORIA_ERP.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/LOOP_CALIDAD_Y_MEMORIA_ERP.md)
- [CRITERIO_MEMORIA_OPERATIVA_ERP.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/CRITERIO_MEMORIA_OPERATIVA_ERP.md)
- [CIERRE_AUDITORIA_Y_REPORTE_CONSOLIDADO_AGENTES_ERP.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/CIERRE_AUDITORIA_Y_REPORTE_CONSOLIDADO_AGENTES_ERP.md)
- [CANONICIDAD_VENTAS_ERP.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/CANONICIDAD_VENTAS_ERP.md)
- [memory.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/memory.md)

## Archivos del dominio vigilado por los detectores

Estos no pertenecen al bloque de enforcement, pero son archivos sensibles donde el loop vigila regresiones:

- [ai_gateway_services.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/api/ai_gateway_services.py)
- [dashboard_sales_dataset.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/dashboard_sales_dataset.py)
- [agent_query_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/pos_bridge/services/agent_query_service.py)
- [sales_read_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/sales_read_service.py)
- [sales_canonical_source.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/sales_canonical_source.py)

## Paquete ERP obligatorio de esta corrección

Además del bloque de enforcement, esta corrección sí toca el paquete ERP de forecast ejecutivo que debe viajar junto cuando el problema es forecast/ingresos de evento:

- [forecasting.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/forecasting.py)
- [financials.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/financials.py)
- [views.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/views.py)
- [event_detail_snapshot.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/event_detail_snapshot.py)
- [tests.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/tests.py)

Regla:
- si el cambio corrige plausibilidad financiera, snapshot ejecutivo, benchmark del evento, same-store, expansión, contracción o detalle del evento, estos cinco archivos forman parte del paquete ERP completo y no deben omitirse del alcance.
- el ZIP `PAQUETE` del evento tambien debe incluir los cuatro entregables ERP vigentes: semana, dia exacto, proyeccion por dia y dashboard ejecutivo.
- si la plausibilidad financiera semanal queda fuera de banda defendible, el cambio tambien debe incluir el guard de aprobacion/publicacion; no basta con dejar una alerta visual.
- si DG fija un benchmark ejecutivo de ventas para un evento, debe quedar persistido en el evento y el paquete ERP debe regenerarse usando ese benchmark como referencia operativa, manteniendo `precio real x piezas` como formula de ingreso.
- el forecast ejecutivo profesional del evento debe dejar trazabilidad visible de `benchmark_source`, `same_store_factor`, `expansion_factor`, `contraction_factor`, `mix_adjustment_source` y `final_projection_reasoning`.
- el forecast ejecutivo profesional del evento tambien debe dejar trazabilidad visible del techo semanal final (`weekly_executive_ceiling_applied`, `weekly_executive_target_qty`, `weekly_executive_target_total_qty`) cuando el modelo detecte que la semana persistida quedó por encima del target defendible.
- cuando se corrija el dia principal del evento, tambien debe quedar trazabilidad visible del origen de la curva diaria (`daily_curve_source` o equivalente) para demostrar si el pico salió de historia propia, sucursal comparable o perfil global del evento.
- cuando DG fije benchmark explicito del dia principal, el forecast debe poder heredarlo como participacion gobernada sobre el benchmark semanal y dejar trazabilidad visible de `main_day_benchmark_sales`, `main_day_peak_floor_applied` y `main_day_peak_source`.
- la aprobacion/publicacion no puede darse por buena si solo el dia principal quedó corregido pero la semana total sigue fuera del `target_total_qty` del modelo; primero debe aplicarse el techo semanal y luego revalidarse el pico principal.
- si después de un recálculo el guard vuelve a detectar desviación en semana total o día fuerte, un evento en `LISTO_PARA_REVISION` o `PENDIENTE_DIRECCION` debe regresar a `EN_MODELADO`; el estado visible del ERP debe reflejar la realidad del guard.
- el forecast ejecutivo y su ingreso supuesto deben construirse solo con los productos activos del evento dentro del scope comercial principal; accesorios, bebidas de reventa y recetas de `SERVICIO_ACCESORIO` quedan fuera, pero postres vendidos como `Vasos Preparados` sí forman parte del forecast ejecutivo cuando están seleccionados en el evento.
- para evitar calentamiento del equipo local, no dejar shells largos de `manage.py shell -c` o `manage.py test` acumulados; usar `scripts/cleanup_erp_forecast_processes.sh` cuando haga falta limpiar procesos pesados del repo.

## Uso operativo para consolidar este bloque

Inspeccion rapida del alcance:

```bash
./scripts/show_sales_enforcement_scope.sh
```

Diff acotado al bloque:

```bash
git diff -- $(./scripts/show_sales_enforcement_scope.sh --pathspec)
```

Estado operativo del bloque:

```bash
./scripts/run_quality_loop.sh full
```

Perímetro completo del paquete ERP + enforcement:

```bash
./scripts/show_sales_enforcement_scope.sh
```

## Limite intencional

Este documento no intenta aislar todo el dominio de ventas del repo. Solo delimita el bloque de enforcement/calidad/memoria/remediacion construido para proteger la canonicidad visible y operativa de ventas.
