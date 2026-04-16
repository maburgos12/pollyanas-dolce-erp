# Mapa de Capacidades del ERP a Agentes

## 1. Objetivo

Mapear instrucciones, endpoints, jobs y automatizaciones ya existentes del ERP hacia los agentes del orquestador nativo, definiendo:

- agente responsable
- dominio
- modo de uso
- disparador
- nivel de riesgo
- necesidad de aprobacion

Este documento responde a la pregunta clave:

`si, las instrucciones actuales del ERP se pueden asignar al agente que corresponda, pero como capacidades controladas por dominio, reglas y permisos; no como acceso libre.`

## 2. Reglas de asignacion

Cada capacidad existente debe clasificarse en uno de estos modos:

- `read`
- `analyze`
- `recommend`
- `request_approval`
- `execute_safe_action`
- `execute_sensitive_action`

Reglas:

- `read` y `analyze` pueden habilitarse desde Fase 1
- `recommend` puede habilitarse desde Fase 1 con trazabilidad
- `request_approval` requiere flujo de aprobacion definido
- `execute_safe_action` solo aplica a acciones de bajo riesgo y reversibles
- `execute_sensitive_action` queda bloqueado hasta gobierno formal y aprobacion humana

## 3. Agentes objetivo

- `director_operativo`
- `agente_demanda_ventas`
- `agente_produccion`
- `agente_compras`
- `agente_conciliacion`

## 4. Mapa de capacidades por dominio

### 4.1 Director Operativo

Responsabilidad:

- ver estado consolidado
- priorizar
- delegar
- escalar

Capacidades iniciales:

| Capacidad ERP | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `GET /api/reportes/bi/dashboard/` | `read` | scheduler diario | bajo | no |
| `GET /api/audit/logs/` | `read` | manual, excepcion | bajo | no |
| `GET /api/integraciones/point/resumen/` | `read` | scheduler diario | bajo | no |
| `GET /api/pos-bridge/sync-jobs/` | `read` | scheduler, job fallido | bajo | no |
| `POST /api/pos-bridge/sync-jobs/trigger/` | `execute_safe_action` | job fallido, refresh manual | medio | si en Fase 1 |

Notas:

- El Director Operativo no debe usar capacidades de escritura de compras, inventario o produccion.
- Su rol principal es coordinar y escalar.

### 4.2 Agente de Demanda y Ventas

Responsabilidad:

- forecast
- estacionalidad
- stock minimo de arranque
- fechas fuertes
- productos especiales

Capacidades iniciales:

| Capacidad ERP | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `GET /api/pos-bridge/sales/summary/` | `read` | diario | bajo | no |
| `GET /api/pos-bridge/sales/by-branch/` | `analyze` | diario | bajo | no |
| `GET /api/pos-bridge/sales/by-product/` | `analyze` | diario | bajo | no |
| `GET /api/pos-bridge/sales/trends/` | `analyze` | semanal | bajo | no |
| `GET /api/ventas/historial/` | `analyze` | semanal, temporada | bajo | no |
| `GET /api/ventas/pronostico/` | `read` | diario | bajo | no |
| `GET /api/ventas/pronostico-insights/` | `analyze` | semanal | bajo | no |
| `POST /api/ventas/pronostico-backtest/` | `analyze` | revision de modelo | medio | no |
| `POST /api/ventas/pronostico-estadistico/` | `analyze` | temporada, ajuste forecast | medio | no |
| `POST /api/ventas/pronostico-estadistico/guardar/` | `execute_sensitive_action` | guardado de forecast | alto | si |
| `POST /api/ventas/solicitud/aplicar-forecast/` | `execute_sensitive_action` | formalizar señal comercial | alto | si |

Capacidades del `pos_bridge` y comandos reutilizables:

| Capacidad | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `python manage.py measure_sales_day_close` | `analyze` | cierre diario | medio | no |
| `python manage.py run_sales_history_sync` | `request_approval` | historico incompleto | medio | si |

Asignacion natural:

- este agente detecta demanda esperada
- recomienda stock de arranque
- alerta fechas fuertes con 2 a 6 semanas
- solicita validacion de producto especial

### 4.3 Agente de Produccion

Responsabilidad:

- traducir demanda validada a produccion
- revisar capacidad, merma y vida util

Capacidades iniciales:

| Capacidad ERP | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `POST /api/mrp/explode/` | `analyze` | nuevo escenario | medio | no |
| `POST /api/mrp/calcular-requerimientos/` | `analyze` | semanal, fecha fuerte | medio | no |
| `POST /api/mrp/generar-plan-pronostico/` | `recommend` | forecast validado | medio | si en Fase 1 |
| `GET /api/mrp/planes/` | `read` | diario | bajo | no |
| `GET /api/recetas/<id>/versiones/` | `read` | revision de receta | bajo | no |
| `GET /api/recetas/<id>/costo-historico/` | `analyze` | cambios de costo | bajo | no |

Capacidades del `pos_bridge` y comandos reutilizables:

| Capacidad | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `python manage.py run_production_entry_sync` | `request_approval` | conciliacion de entradas | medio | si |
| `python manage.py run_recipe_gap_audit` | `analyze` | recetas incompletas | medio | no |
| `python manage.py sync_point_derived_presentations` | `request_approval` | derivadas faltantes | medio | si |

Asignacion natural:

- este agente no produce solo
- propone cantidades, fechas y prioridades
- escala conflictos entre capacidad y demanda

### 4.4 Agente de Compras

Responsabilidad:

- convertir plan en insumos
- vigilar quiebres
- preparar solicitudes

Capacidades iniciales:

| Capacidad ERP | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `GET /api/inventario/sugerencias-compra/` | `read` | diario | bajo | no |
| `GET /api/pos-bridge/inventory/low-stock/` | `analyze` | diario, evento | bajo | no |
| `GET /api/pos-bridge/inventory/current/` | `read` | diario | bajo | no |
| `GET /api/compras/solicitudes/` | `read` | diario | bajo | no |
| `GET /api/compras/ordenes/` | `read` | diario | bajo | no |
| `GET /api/compras/recepciones/` | `read` | diario | bajo | no |
| `POST /api/compras/solicitud/` | `recommend` | quiebre o temporada | alto | si |
| `POST /api/compras/solicitud/<id>/crear-orden/` | `execute_sensitive_action` | convertir solicitud | alto | si |
| `POST /api/compras/orden/<id>/recepciones/` | `execute_sensitive_action` | recepcion de compra | alto | si |

Capacidades del `pos_bridge` y comandos reutilizables:

| Capacidad | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `python manage.py capture_point_inventory_cost` | `analyze` | desviacion de costo | medio | no |
| `python manage.py run_inventory_sync` | `execute_safe_action` | refresh operativo | medio | si en Fase 1 |
| `python manage.py run_realtime_inventory --force` | `execute_safe_action` | reposicion urgente | medio | si en Fase 1 |

Asignacion natural:

- este agente prepara necesidades y prioridades
- no debe aprobar ordenes ni recepciones por su cuenta

### 4.5 Agente de Conciliacion

Responsabilidad:

- revisar integridad Point vs ERP
- vigilar jobs
- detectar desviaciones y casos anormales

Capacidades iniciales:

| Capacidad ERP | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `GET /api/control/discrepancias/` | `read` | diario | bajo | no |
| `GET /api/integraciones/point/operaciones/historial/` | `read` | diario, excepcion | bajo | no |
| `GET /api/pos-bridge/sync-jobs/` | `read` | diario | bajo | no |
| `POST /api/integraciones/point/mantenimiento/ejecutar/` | `execute_safe_action` | mantenimiento controlado | medio | si |
| `POST /api/inventario/point-pendientes/resolver/` | `execute_sensitive_action` | correccion de pendientes | alto | si |

Capacidades del `pos_bridge` y comandos reutilizables:

| Capacidad | Tipo | Trigger sugerido | Riesgo | Aprobacion |
|---|---|---|---|---|
| `python manage.py retry_failed_jobs` | `execute_safe_action` | job fallido | medio | si en Fase 1 |
| `python manage.py reconcile_sales_report` | `analyze` | discrepancia de ventas | medio | no |
| `python manage.py reconcile_unresolved_sales_matches` | `request_approval` | ventas no resueltas | alto | si |
| `python manage.py repair_bridge_sales_materialization` | `execute_sensitive_action` | inconsistencia de materializacion | alto | si |
| `python manage.py run_recipe_gap_audit` | `analyze` | productos sin receta clara | medio | no |
| `python manage.py run_transfer_sync` | `request_approval` | diferencias de transferencia | medio | si |
| `python manage.py run_waste_sync` | `request_approval` | diferencias de merma | medio | si |

Asignacion natural:

- este agente es el primero en tomar errores, desfases y fallas operativas
- puede delegar a Produccion o Compras segun el hallazgo

## 5. Capacidades transversales no asignables libremente

Las siguientes capacidades no deben entregarse de forma amplia a ningun agente:

| Capacidad | Motivo |
|---|---|
| `POST /api/inventario/ajustes/` | altera stock y requiere control estricto |
| `POST /api/inventario/ajustes/<id>/decision/` | impacta inventario y conciliacion |
| `POST /api/compras/orden/<id>/estatus/` | cambia estado formal de compra |
| `POST /api/compras/recepcion/<id>/estatus/` | impacta recepcion y auditoria |
| `POST /api/pos-bridge/product-closures/<id>/lock/` | cierra datos y debe ser controlado |
| operaciones de usuarios, auth y token | alcance administrativo sensible |

Estas deben quedar reservadas para humanos `DG` o `ADMIN`, o para workflows muy acotados con aprobacion formal.

## 6. Reglas de delegacion sugeridas

### Regla 1: quiebre probable de sucursal

Condicion:

- `inventory.low_stock`
- demanda esperada > stock de arranque

Flujo:

1. `agente_demanda_ventas` analiza impacto comercial
2. `agente_compras` revisa insumo o reposicion
3. `director_operativo` decide escalamiento

### Regla 2: fecha fuerte detectada

Condicion:

- variacion positiva significativa vs historico
- proximidad de fecha comercial

Flujo:

1. `agente_demanda_ventas` genera alerta
2. solicita aprobacion de producto especial si aplica
3. `agente_produccion` propone plan
4. `agente_compras` propone abastecimiento

### Regla 3: job fallido de integracion

Condicion:

- `sync_job.status = failed`

Flujo:

1. `agente_conciliacion` analiza
2. si es refresh seguro, solicita `retry` o `sync trigger`
3. `director_operativo` valida severidad

### Regla 4: producto especial solicitado

Condicion:

- demanda extraordinaria
- fecha fuerte o iniciativa comercial

Flujo:

1. `agente_demanda_ventas` crea sugerencia
2. humano aprueba
3. `agente_produccion` convierte a plan
4. `agente_compras` convierte a insumos

## 7. Fase 1 recomendada

Capacidades a habilitar:

- todas las de `read`
- todas las de `analyze`
- algunas de `recommend`
- muy pocas de `execute_safe_action`

Capacidades seguras candidatas en Fase 1:

- `POST /api/pos-bridge/sync-jobs/trigger/`
- `python manage.py retry_failed_jobs`
- `python manage.py run_inventory_sync`

Siempre con:

- usuario tecnico con permisos minimos
- bitacora
- aprobacion humana si hay duda operacional

## 8. Datos faltantes para mejorar la asignacion

No conviene asumir aun:

- cuales comandos del repo ya corren en produccion vs solo local
- cuales endpoints estan autorizados para usuario tecnico
- que jobs son realmente reversibles
- que areas aproban cada accion sensible
- que sucursales tienen horarios de entrega estructurados en sistema

## 9. Criterio final

Las instrucciones actuales del ERP si se deben asignar al agente que corresponda, pero bajo este modelo:

- `instruccion existente` -> `capacidad catalogada`
- `capacidad catalogada` -> `agente responsable`
- `agente responsable` -> `regla y disparador`
- `regla y disparador` -> `accion permitida o aprobacion`

No se recomienda un modelo donde "todos los agentes pueden usar todo".
