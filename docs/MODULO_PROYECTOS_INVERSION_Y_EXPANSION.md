# Modulo Proyectos de Inversion y Expansion

## Objetivo

Este modulo incorpora control financiero-operativo para aperturas, expansiones, remodelaciones y reubicaciones de sucursales, respetando la arquitectura real del ERP basada en Django y el dominio existente de `reportes`.

## Encaje arquitectonico

- Dominio backend: `reportes`
- Entidad operacional relacionada: `core.Sucursal`
- Fuentes operativas reales:
  - Ventas y costo de venta: `reportes.FactVentaDiaria`
  - Fallback de ventas cuando no existe fact table completa: `ventas.VentaAutoritativaPoint`, `pos_bridge.PointDailySale`
  - Gastos operativos mensuales: `reportes.GastoOperativoMensual`
  - Centro de costo por sucursal: `reportes.CentroCosto`
- Seguridad: `core.access.can_view_reportes`
- Trazabilidad: `core.audit.log_event`
- UI: templates Django + Chart.js + navegacion existente de reportes

## Modelo de datos agregado

- `ProyectoInversion`
  - Maestro del proyecto con estatus, inversion planeada/real, deuda, metas ROI/payback, estrategia de recuperacion y reglas de cierre.
- `ProyectoInversionGasto`
  - CAPEX detallado por categoria, montos, referencias ERP/contables y evidencia.
- `ProyectoInversionPagoDeuda`
  - Pagos reales de deuda para interes, capital amortizado y saldo insoluto.
- `ProyectoInversionEscenario`
  - Supuestos de simulacion conservador/base/optimista con persistencia de resultados.
- `ProyectoInversionSnapshotMensual`
  - KPIs historicos materializados por mes para auditoria, tablero y reportes.

## Logica financiera implementada

- Inversion real acumulada a partir de CAPEX registrado.
- Ventas, costo, utilidad bruta, utilidad operativa y flujo neto por mes.
- Recuperacion segun estrategia del proyecto:
  - `full_net_cashflow`
  - `percentage_of_profit`
  - `profit_after_debt_service`
- Saldo pendiente, porcentaje recuperado, ROI mensual/acumulado/anualizado.
- Servicio de deuda real si existe captura; estimado por anualidad si todavia no existe.
- Cierre automatico por recuperacion, deuda, ROI minimo o cierre manual.

## Reportes y UX

- Dashboard ejecutivo por proyecto con KPIs, graficas y timeline.
- Tabs operativas: resumen, inversion, operacion, deuda, escenarios, reportes y configuracion.
- Vista ejecutiva de expansion en `/reportes/proyectos_inversion/expansion/` con clasificacion automatica `EXPANDIR / VIGILAR / RIESGO`, politica global y simulador de nuevas aperturas.
- Simulador ejecutivo en `/reportes/expansion/simulador/` con:
  - guardado manual de simulaciones sin duplicar refresh accidental
  - estatus ejecutivo `en_revision / candidato / descartado / aprobado_preliminar`
  - historial reciente y reapertura de escenarios guardados
  - comparacion entre escenarios persistidos
  - exportacion PDF/XLSX del escenario actual o guardado
  - panel operativo con acceso al flujo oficial de carga mensual de OPEX
- Exportacion base a XLSX y PDF para:
  - resumen ejecutivo
  - detalle de inversion
  - desempeno mensual
  - recuperacion
  - comparativo real vs proyectado
  - estado de deuda
  - ROI / payback

## Capa estrategica de expansion

- Servicio de decision: `reportes/services_expansion_decision.py`
  - calcula clasificacion por proyecto a partir de `health_score`, ROI, payback real, flujo libre y tendencia de ventas.
  - consolida politica financiera global con `ExpansionPolicyConfig`.
  - emite alertas estrategicas como sobreapalancamiento, exceso de sucursales en riesgo y deterioro del payback promedio.
- Servicio de forecast: `reportes/services_expansion_forecast.py`
  - estima ventas, utilidad operativa, flujo libre, ROI y payback de una nueva sucursal usando proyectos comparables.
  - expone `recomendar_apertura()` para decidir abrir, esperar o detener expansion y calcular cuantas sucursales soporta la capacidad financiera actual.
- Servicio de calibracion: `reportes/services_expansion_calibration.py`
  - compara clasificacion del sistema vs clasificacion real capturada manualmente en `ProyectoInversion.metadata`.
  - parametriza pesos de `health_score`, umbrales de clasificacion y ventanas del forecast sin crear nuevas tablas.
  - recalibra usando proyectos con minimo 6 meses de snapshots y genera metricas de precision / error de payback.
- Estructura futura de zonas: `ExpansionZoneScore`
  - deja listo el modelo para puntaje por ciudad/zona sin inventar datos de mercado.

## Pendientes reales de datos

- Costo de venta completo depende de que la sucursal tenga datos en `FactVentaDiaria`. Si solo existe fallback de ventas, el modulo marca brecha de datos en lugar de inventar costo.
- La deuda puede operar con pagos estimados, pero para precision financiera se requiere captura recurrente de `ProyectoInversionPagoDeuda` o una integracion financiera futura.
- La politica global usa deuda del portafolio de expansion; el ERP todavia no expone una fuente corporativa canonica de pasivos consolidada para toda la empresa.
- La clasificacion fina de renta, servicios, marketing y otros depende de la calidad de codigos en `CategoriaGasto`.
- VAN y TIR quedaron preparados a nivel de snapshot pero sin activarse hasta definir la politica corporativa de tasa de descuento y horizonte oficial.
- La calibracion real requiere al menos 3 sucursales con clasificacion real capturada y 6 meses o mas de snapshots confiables; si no existen, el sistema muestra la brecha en UI y no fuerza ajustes.

## Flujo operativo recomendado

1. Correr simulacion desde `/reportes/expansion/simulador/`.
2. Revisar resultado final, comparacion contra base, sensibilidad y reglas visibles.
3. Guardar la simulacion si el caso amerita seguimiento.
4. Cambiar estatus ejecutivo del escenario segun la decision:
   - `en_revision`
   - `candidato`
   - `descartado`
   - `aprobado_preliminar`
5. Exportar PDF/XLSX para junta o comite.
6. Cargar OPEX mensual real desde `/reportes/gastos-operativos/importar/`.
7. Confirmar en historial que el archivo fue validado, cargado y que refresco proyectos afectados.

## Notas de operacion

- Las simulaciones guardadas reutilizan `ProyectoInversionEscenario`; no se creo una tabla paralela.
- La deduplicacion de simulaciones se basa en hash de la base seleccionada + inputs ejecutivos del simulador.
- El flujo de OPEX ya era operativo y se mantuvo como canal oficial; el simulador ahora lo referencia y muestra historial reciente para mantener vivo el modulo.
- La suite SQLite de `settings_test` sigue bloqueada por la migracion ajena `reportes.0016_mv_dashboard_daily_ops` con `MATERIALIZED VIEW`; por eso la validacion principal de este cierre se hizo contra PostgreSQL real.

## Despliegue incremental y rollback

Checklist de despliegue:

1. Aplicar migraciones del modulo `reportes` pendientes, incluyendo la base del modulo y la persistencia de simulaciones guardadas (`0013` a `0024` segun el entorno).
2. Validar permisos de acceso a `reportes`.
3. Crear al menos un proyecto piloto con sucursal real.
4. Refrescar snapshots y validar KPIs contra ventas/gastos reales.
5. Habilitar uso operativo para direccion y finanzas.

Rollback seguro:

1. Ocultar rutas/enlace de navegacion del modulo.
2. Mantener tablas nuevas sin borrar datos historicos.
3. Revertir migracion solo si el entorno no ha empezado a capturar informacion productiva.

## Mejoras recomendadas siguientes

- Benchmark entre aperturas historicas y sucursales comparables.
- Score de salud del proyecto con semaforos de recuperacion, margen y disciplina CAPEX.
- Alertas automaticas por desviacion de presupuesto, flujo negativo o atraso de payback.
- Forecast probabilistico de recuperacion con tendencia real.
- Analisis de sensibilidad sobre ventas, margen, OPEX y deuda.
