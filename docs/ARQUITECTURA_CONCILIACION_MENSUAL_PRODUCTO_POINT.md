# Arquitectura De Conciliacion Mensual De Producto Point

## Objetivo

Automatizar dentro del ERP el comparativo mensual de producto terminado que hoy se hace en Excel:

- inventario inicial del mes
- produccion del mes
- venta del mes
- conversion de presentaciones derivadas a producto entero
- merma registrada en Point
- inventario teorico final del mes

Esta version no incluye inventario fisico ni auditoria de conteo. Esa capa se deja separada para una fase posterior.

## Decision de alcance v1

El cierre mensual automatico de esta seccion se calculara solo con movimientos generados por el sistema Point y ya materializados en el ERP.

Incluye:

- saldo inicial teorico
- produccion Point
- ventas Point
- presentaciones derivadas tipo rebanada convertidas a entero equivalente
- merma Point cuando exista
- cierre teorico mensual

Excluye en v1:

- inventario fisico de sucursales
- inventario fisico de CEDIS
- diferencias sistema vs conteo
- aprobaciones manuales de ajuste por conteo

## Fuente de verdad por dato

### Inventario inicial del mes

Fuente objetivo:

1. cierre mensual canonicamente guardado del mes anterior
2. si no existe cierre previo, snapshot Point del ultimo dia del mes anterior
3. si tampoco existe, ultimo snapshot disponible dentro de ventana de tolerancia con bandera de excepcion

Tolerancia oficial v1:

- `3` dias calendario

Notas:

- para cerrar `2025-09`, el saldo inicial debe venir de `2025-08`
- este saldo inicial es teorico del sistema, no fisico

### Produccion

Fuente:

- `pos_bridge.models.PointProductionLine`
- materializacion actual a `recetas.MovimientoProductoCedis`

Regla:

- usar solo producto final homologado a `receta`
- consolidar por mes y por receta padre

### Venta

Fuente:

- `pos_bridge.models.PointDailySale`
- historico materializado a `recetas.VentaHistorica` con fuente `POINT_BRIDGE_SALES`

Regla:

- usar venta oficial Point ya conciliada a receta
- consolidar por mes y por receta padre

### Conversion de rebanadas a entero

Fuente:

- `recetas.RecetaPresentacionDerivada`

Regla institucional:

- las ventas de rebanadas no deben leerse como inventario independiente final
- deben regresar a su producto entero padre como consumo equivalente
- formula:
  - `venta_entero_equivalente = venta_rebanadas / unidades_por_padre`

Ejemplos esperados:

- pastel mediano de 10 rebanadas
- pay grande de 8 rebanadas

### Merma

Fuente:

- `pos_bridge.models.PointWasteLine`
- materializacion actual a `control.MermaPOS`

Regla:

- si la merma corresponde a producto derivado, convertir a entero equivalente usando la misma relacion padre -> derivado
- si la merma corresponde a producto entero, consumir directo contra la receta padre

## Nivel canonico del cierre

El cierre mensual v1 se recomienda a nivel:

- `mes + receta padre`

No a nivel:

- `mes + sucursal + receta`
- `mes + snapshot inventario fisico`

Razon:

- el Excel actual busca explicar el destino total del producto
- las transferencias internas entre CEDIS y tiendas no cambian el total de la red
- la conversion de rebanadas debe consolidarse contra el padre

Esto permite una primera conciliacion automatica confiable y despues abrir una segunda vista de auditoria por sucursal.

## Formula canonica propuesta

Para cada `mes + receta_padre`:

```text
inventario_inicial_teorico
+ produccion_mes
- venta_directa_enteros
- venta_derivada_equivalente
- merma_directa_enteros
- merma_derivada_equivalente
= inventario_final_teorico
```

Formula extendida:

```text
venta_total_equivalente = venta_directa_enteros + venta_derivada_equivalente
merma_total_equivalente = merma_directa_enteros + merma_derivada_equivalente
inventario_final_teorico = inventario_inicial_teorico + produccion_mes - venta_total_equivalente - merma_total_equivalente
```

## Tratamiento de presentaciones derivadas

### Regla principal

Toda presentacion derivada activa debe participar en el cierre del padre.

### Casos

1. Si la receta vendida no es derivada:
   - suma a `venta_directa_enteros`
2. Si la receta vendida si es derivada:
   - convertir por `unidades_por_padre`
   - sumar a `venta_derivada_equivalente`
3. Si una receta derivada no tiene relacion activa:
   - no cerrar automatico
   - marcar incidencia de catalogo

### Regla de no doble conteo

El SKU derivado puede seguir existiendo para ventas y analitica comercial, pero no debe sostener inventario mensual final propio en el cierre canonico de producto entero.

## Flujo operativo mensual

### Paso 1. Determinar periodo

- `month_start`
- `month_end`
- `previous_month_end`
- zona horaria operativa local

### Paso 2. Resolver saldo inicial

- buscar cierre canonico del mes previo
- si no existe:
  - tomar snapshot Point de fin de mes previo
  - agrupar por receta padre
  - convertir derivadas a padre antes de guardar saldo inicial

### Paso 3. Consolidar produccion

- leer `PointProductionLine` del mes
- usar solo lineas homologadas a `receta`
- agrupar por receta padre

### Paso 4. Consolidar ventas

- leer `VentaHistorica` o `PointDailySale` ya conciliada
- separar venta directa de venta derivada
- convertir derivadas a padre

### Paso 5. Consolidar merma

- leer `PointWasteLine` o `MermaPOS`
- separar merma directa y derivada
- convertir derivadas a padre

### Paso 6. Calcular cierre

- aplicar formula canonica
- generar estatus por linea

### Paso 7. Persistir snapshot mensual

- guardar un cierre inmutable o versionado por `mes + receta_padre`
- ese cierre se vuelve la base del siguiente mes

### Paso 8. Gobierno de cierre

- el cierre se puede construir desde operacion para lectura y validacion
- el bloqueo del mes debe quedar restringido a `DG` o `ADMIN`
- el build queda restringido a `DG`, `ADMIN`, `PRODUCCION` y `ALMACEN`
- el rebuild queda restringido a `DG` y `ADMIN`
- no se debe permitir bloqueo si:
  - el cierre no esta en estado `BUILT`
  - no tiene lineas
  - existen incidencias de catalogo pendientes
  - existen productos del opening sin homologacion Point -> ERP
- el evento de bloqueo debe dejar rastro auditable en metadata

## Canales operativos

- UI:
  - ver
  - build
  - rebuild controlado
  - lock controlado
  - export CSV/XLSX
- API:
  - list/retrieve
  - build
  - lock
- Commands:
  - build por mes
  - backfill por rango

## Modelo de datos propuesto

Se recomienda agregar un cierre mensual propio en vez de recalcular siempre desde tablas crudas.

### Cabecera

`ProductoMonthClosure`

Campos sugeridos:

- `month_start`
- `month_end`
- `status`
- `build_source`
- `built_at`
- `built_by`
- `notes`
- `upstream_sync_cutoff_at`
- `is_locked`

### Detalle

`ProductoMonthClosureLine`

Campos sugeridos:

- `closure`
- `receta_padre`
- `inventario_inicial_teorico`
- `produccion_mes`
- `venta_directa_enteros`
- `venta_derivada_equivalente`
- `venta_total_equivalente`
- `merma_directa_enteros`
- `merma_derivada_equivalente`
- `merma_total_equivalente`
- `inventario_final_teorico`
- `source_snapshot_count`
- `source_sale_rows`
- `source_production_rows`
- `source_waste_rows`
- `has_catalog_issue`
- `catalog_issue_note`

### Tabla opcional de detalle explicativo

`ProductoMonthClosureEvidence`

Uso:

- guardar trazabilidad por linea derivada
- justificar conversiones
- permitir auditoria sin releer todo el staging

## Estados sugeridos del cierre

### Cabecera

- `DRAFT`
- `BUILT`
- `REVIEWED`
- `LOCKED`
- `REBUILD_REQUIRED`

### Linea

- `OK`
- `CATALOG_GAP`
- `MISSING_OPENING`
- `MISSING_DERIVED_RULE`
- `UNMATCHED_POINT_ROW`

## Servicios propuestos

### Servicio principal

`ProductoMonthClosureService`

Responsabilidades:

- resolver periodo
- calcular opening
- consolidar movimientos
- aplicar conversiones
- persistir cierre
- reconstruir un mes si cambia el catalogo

### Servicio auxiliar de opening

`ProductoMonthOpeningService`

Responsabilidades:

- obtener cierre previo
- fallback a snapshots Point
- aplicar bandera de excepcion si el opening no viene de un cierre canonico

### Servicio auxiliar de conversiones

`DerivedPresentationRollupService`

Responsabilidades:

- detectar si una receta es derivada
- mapearla a padre
- convertir unidades
- devolver evidencia del rollup

## API y operacion sugerida

### Command

`python manage.py build_product_month_closure --month 2025-09`

### API interna

`POST /api/pos/closures/product-month/build`

Payload sugerido:

```json
{
  "month": "2025-09",
  "rebuild": false,
  "lock_after_build": false
}
```

### API de consulta

`GET /api/pos/closures/product-month/?month=2025-09`

Debe devolver:

- resumen del mes
- lineas por receta padre
- trazabilidad de conversion derivada
- advertencias

## Integracion con lo que ya existe

### Reusar

- `PointProductionLine`
- `PointDailySale`
- `VentaHistorica`
- `PointWasteLine`
- `RecetaPresentacionDerivada`
- `MovimientoProductoCedis`

### No reusar como fuente final del cierre

- el Excel
- conteos fisicos manuales
- nombres libres no homologados

## Regla especifica septiembre 2025

Para reproducir el control que hoy se hace en Excel:

1. abrir `2025-09`
2. tomar inventario inicial desde `2025-08`
3. leer produccion Point de septiembre
4. leer ventas Point de septiembre
5. convertir rebanadas a entero equivalente
6. aplicar merma Point si existe y si se aprueba para v1
7. persistir cierre de septiembre
8. usar el cierre de septiembre como opening de octubre

## Supuestos actuales

Estos supuestos salen de la conversacion y deben validarse antes de programar:

- v1 no compara contra inventario fisico
- el cierre buscado es teorico y automatico
- las rebanadas deben regresar al entero padre
- septiembre y octubre son los meses de referencia para extraer la formulacion operativa
- el total de red es mas importante que el cierre por sucursal en esta fase

## Datos aun faltantes

Para no asumir de mas, quedan por confirmar:

- si v1 incluye merma obligatoriamente o se puede dejar como fase 1.1
- si el opening debe salir del snapshot final Point por sucursal sumado a red o del stock CEDIS de producto
- si el cierre canonico debe ser global red o doble vista:
  - global red
  - por sucursal
- politica de fallback cuando no exista snapshot de fin de agosto

## Implementacion incremental recomendada

### Fase 1

- construir modelos de cierre mensual
- resolver opening desde cierre previo o snapshot Point
- consolidar ventas y produccion
- incluir conversion derivada
- generar cierre mensual sin merma

### Fase 2

- agregar merma Point al calculo
- agregar evidencias por linea
- agregar rebuild seguro por mes

### Fase 3

- dashboard mensual
- API para agentes
- bloqueo de cierre mensual

### Fase 4

- vista separada de auditoria fisico vs teorico
- cierre por sucursal
- excepciones y aprobaciones

## Criterios de aceptacion

- el sistema puede generar `2025-09` usando opening de `2025-08`
- el sistema puede generar `2025-10` usando cierre de `2025-09`
- las rebanadas dejan de distorsionar el inventario mensual del padre
- el cierre es idempotente para el mismo mes y mismo corte de datos
- el cierre deja trazabilidad de fuente y hora de construccion

## Riesgos

- alias Point incompletos rompen homologacion de receta
- relaciones padre -> derivado faltantes distorsionan el cierre
- snapshots de fin de mes incompletos generan opening incorrecto
- cambios retroactivos en Point obligan rebuild del mes

## Rollback / mitigacion

- no reemplazar tablas operativas existentes
- persistir el cierre en tablas nuevas
- dejar el Excel como comparativo paralelo durante la validacion
- permitir `rebuild` por mes sin tocar ventas ni staging crudo
- bloquear `lock` del cierre hasta pasar validacion DG

## Bootstrap historico

Cuando Point no tenga snapshots historicos del ultimo dia del mes previo, el sistema puede sembrar un cierre semilla con `bootstrap_product_month_closure`.

- fuente registrada: archivo Excel, hoja y columna exacta usadas para el seed
- regla canonica: cualquier presentacion derivada se convierte al entero padre antes de persistir el cierre
- uso aprobado: tomar `Inventario inicial` de `SEPT 25` para sembrar `2025-08`
- control: el seed queda en `BUILT`; si arrastra productos sin homologacion, el siguiente mes hereda la guarda y no puede bloquearse
- objetivo: destrabar el opening del mes siguiente sin inventar snapshots faltantes ni tocar los jobs operativos de Point
