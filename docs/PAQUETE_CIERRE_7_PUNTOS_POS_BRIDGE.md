# Paquete De Cierre Operativo Point + ERP

## Objetivo

Cerrar en una sola fase de trabajo los 7 frentes operativos que hoy ya están parcialmente implementados en el ERP, evitando rehacer lo que ya funciona y enfocando el esfuerzo solo en lo pendiente para dejar una operación auditable, estable y lista para despliegue.

Este paquete no parte de cero. Parte del estado real actual del repo.

## Estado Actual Resumido

### Ya implementado

1. `pos_bridge` modular con login, inventario, ventas, recetas, mermas, producción y transferencias.
2. API pública de pickup y reservas.
3. API interna `pos_bridge`.
4. Historial de ventas desde `2022-01-01`.
5. `Q1 2026` corregido con fuente oficial Point para ventas comerciales en `PointDailySale`.
6. Agente local con fallback OpenAI.
7. Scheduler local con `launchd`.
8. Capa `Celery + Beat + Redis` lista en código.
9. Dashboard DG consolidado con:
   - plan de producción
   - reabasto CEDIS
   - cobertura histórica de ventas
   - cuadre operativo Point
10. Mermas con responsable ya materializado en `MermaPOS`.
11. `Guamuchil` ya existe como sucursal maestra y quedó en `activa=False` para no contaminar reportes antes de apertura.

### Pendiente de cierre real

1. Incluir en automatización programada:
   - mermas
   - producción
   - transferencias
2. Endurecer el dashboard DG para exponer mejor:
   - mermas por sucursal
   - mermas por responsable
   - producción central
   - transferencias hacia CEDIS
   - ticket promedio en más de un corte
3. Construir tablero operativo formal de conciliación:
   - producido
   - transferido
   - vendido
   - mermado
   - existencia inicial
   - existencia final
   - diferencia
   - semáforo
4. Definir cierre operativo por sucursal con estatus:
   - cuadra
   - desviación menor
   - desviación crítica
   - sin cierre
   - con excepción
5. Backfill histórico de:
   - mermas
   - producción
   - transferencias
   por bloques, no ciegamente desde 2022.
6. Endurecer gobierno del agente IA para que solo responda cifras “oficiales” cuando la fuente esté conciliada.
7. Preparar despliegue servidor:
   - Railway
   - Redis
   - worker
   - beat

## Los 7 Puntos Del Usuario, Cerrados Vs Pendientes

### 1. Dashboard

Estado: `parcial-avanzado`

Ya cubre:
- plan
- reabasto
- ventas
- cuadre Point
- tickets
- ticket promedio por sucursal/día dentro del cuadre Point

Falta para darlo por cerrado:
- vista ejecutiva específica de mermas por sucursal y responsable
- vista ejecutiva de producción/transferencias centralizadas
- vista de desviaciones con drilldown por sucursal

### 2. Automatizaciones

Estado: `parcial`

Ya automatizado localmente:
- ventas cerradas
- inventario
- snapshot DG

Falta:
- programar mermas
- programar producción
- programar transferencias
- definir si local seguirá con `launchd` y servidor con `Celery`, o si se migrará todo a `Celery`

### 3. Historial 2022 / corroborar errores

Estado: `parcial-controlado`

Ya cerrado:
- ventas desde 2022 existen
- `Q1 2026` comercial ya fue corregido contra Point oficial

Falta:
- backfill histórico de mermas, producción y transferencias
- conciliación por bloques, no reproceso total ciego

### 4. Mermas por sucursal / responsable

Estado: `casi cerrado`

Ya existe:
- extracción desde Point
- staging con responsable
- materialización en `MermaPOS` con `responsable_texto`

Falta:
- tablero/ranking formal
- exporte ejecutivo

### 5. Sucursal Guamuchil

Estado: `cerrado para preapertura`

Ya existe:
- alta maestra
- nombre Point: `Guamuchil`
- `activa=False`
- exclusión de dashboard operativo mientras siga inactiva

Falta:
- cuando se defina apertura real, activarla
- opcional: registrar fecha de apertura formal

### 6. Tablero producido vs venta vs merma

Estado: `parcial-avanzado`

Ya existe:
- primera versión de cuadre operativo Point

Falta:
- convertirlo en tablero de cierre operativo formal
- agregar excepción/aprobación/cierre
- mostrar producción directa y transferida con más detalle

### 7. Upgrade IA

Estado: `técnicamente integrado, funcionalmente pendiente de endurecimiento`

Ya existe:
- API interna
- realtime inventory
- agente
- Celery/Beat en código

Falta:
- reglas de gobierno de fuentes
- respuestas solo con fuente oficial conciliada
- más métricas operativas útiles para DG

## Alcance Del Paquete De Cierre

### Fase 1. Cierre Operativo De Datos

Entregables:
- scheduler completo para:
  - ventas
  - inventario
  - mermas
  - producción
  - transferencias
- idempotencia y logging homogéneo
- alertas de fallo

Aceptación:
- todos los jobs crean `PointSyncJob`
- todos los jobs dejan `PointExtractionLog`
- no duplican movimientos ni staging

### Fase 2. Dashboard DG Final

Entregables:
- bloque de mermas por sucursal
- bloque de mermas por responsable
- bloque de producción central
- bloque de transferencias a CEDIS
- ticket promedio visible donde corresponda
- exporte consolidado DG

Aceptación:
- lectura ejecutiva diaria completa sin ir a módulos técnicos

### Fase 3. Cuadre Operativo Formal

Entregables:
- tablero por fecha/sucursal con:
  - inventario inicial
  - producción
  - transferencias
  - ventas
  - tickets
  - ticket promedio
  - mermas
  - esperado
  - cierre
  - variación
  - estatus
- estatus formal:
  - `CUADRA`
  - `DESVIACION_MENOR`
  - `DESVIACION_CRITICA`
  - `SIN_CIERRE`
  - `CON_EXCEPCION`

Aceptación:
- una sucursal puede verse y compararse en un solo renglón por día

### Fase 4. Historial Correctivo

Entregables:
- comandos de backfill por rango para:
  - mermas
  - producción
  - transferencias
- validación mensual/trimestral
- reportes de conciliación

Regla:
- no reprocesar `2022-2025` completo salvo evidencia
- trabajar por ventanas

Aceptación:
- fechas con cobertura explícita
- gaps documentados

### Fase 5. Gobierno Del Agente IA

Entregables:
- respuestas solo sobre fuentes permitidas
- bloqueo automático cuando el rango no esté conciliado
- etiquetas de fuente:
  - `PointDailySaleOfficial`
  - `VentaHistorica`
  - `PointBridgeStaging`
  - `NotReconciled`

Aceptación:
- el agente no debe volver a dar una cifra “oficial” incorrecta

### Fase 6. Operación De Sucursal Nueva

Entregables:
- `Guamuchil` preparada como preapertura
- guía de activación
- exclusión correcta antes de abrir

Aceptación:
- no contamina dashboards ni cierres antes de la apertura

### Fase 7. Despliegue Servidor

Entregables:
- configuración Railway + Redis
- proceso `web`
- proceso `worker`
- proceso `beat`
- desactivar automatizaciones locales cuando el servidor tome control

Aceptación:
- un solo scheduler por entorno

## Datos Faltantes Que Deben Documentarse Si Aplica

No asumir sin confirmación:

1. Fecha exacta de apertura de `Guamuchil`.
2. Ventana operativa exacta para automatizar:
   - mermas
   - producción
   - transferencias
3. Si el cierre operativo formal requerirá aprobación humana o solo semáforo.
4. Si Railway será el entorno definitivo para producción.

## Riesgos Reales

1. Duplicar movimientos si se programa producción y transferencias sin control de idempotencia.
2. Doblar inventario CEDIS si `Produccion Crucero` entra directo y además por transferencia.
3. Mezclar fuente receta-ligada con fuente comercial oficial en el agente.
4. Activar `Guamuchil` antes de tiempo y contaminar dashboards.
5. Correr `launchd` y `Celery` simultáneamente en el mismo entorno.

## Secuencia Recomendada

1. Automatizaciones faltantes.
2. Dashboard DG final.
3. Cuadre formal.
4. Backfill por bloques.
5. Gobierno del agente.
6. Activación controlada de Guamuchil.
7. Despliegue servidor.

## Rollback Seguro

Si alguna fase falla:

1. No borrar staging.
2. Desactivar solo el job nuevo afectado.
3. Mantener dashboards previos.
4. Revertir materialización canónica, no extracción bruta.
5. Documentar rango afectado y volver a correr solo esa ventana.

## Prompt Maestro Para Ejecutar Todo En Una Sola Solicitud

```text
Quiero que cierres de forma integral y real los 7 puntos pendientes del paquete operativo Point + ERP ya implementado parcialmente en este repo.

CONTEXTO
- No partas de cero.
- Este ERP ya tiene pos_bridge, pickup, ventas históricas, dashboard DG, mermas, producción, transferencias, agente IA y capa Celery parcial.
- No rehagas lo que ya funciona.
- Quiero cierre real de operación, no propuesta teórica.

OBJETIVO
Cerrar correctamente estos 7 puntos:
1. dashboard ejecutivo completo
2. automatizaciones completas
3. historial y conciliación por bloques
4. mermas por sucursal y responsable
5. operación preapertura/apertura de Guamuchil
6. tablero formal de cuadre producido vs transferido vs vendido vs merma vs inventario
7. endurecimiento final del upgrade de IA y scheduler servidor

REGLAS
1. Inspecciona primero el estado actual del repo.
2. No rompas pickup ni ventas ya corregidas.
3. No reproceses 2022-2025 completo sin evidencia.
4. Usa staging + materialización idempotente + auditoría.
5. Mantén separación por capas.
6. Mantén compatibilidad local macOS y prepara servidor Linux/Railway.
7. Antes de tocar una pieza sensible, entiende su flujo actual.
8. Si falta un dato crítico, documenta el supuesto o deja el punto claramente marcado.

ALCANCE OBLIGATORIO

FASE 1
- completar automatización de:
  - ventas
  - inventario
  - mermas
  - producción
  - transferencias
- evitar duplicidad entre launchd y Celery

FASE 2
- completar dashboard DG para mostrar:
  - mermas por sucursal
  - mermas por responsable
  - producción central
  - transferencias hacia CEDIS
  - tickets y ticket promedio donde corresponda

FASE 3
- consolidar tablero formal de cuadre diario por sucursal con:
  - inventario inicial
  - producción
  - transferencias
  - ventas
  - tickets
  - ticket promedio
  - mermas
  - inventario esperado
  - inventario de cierre
  - variación
  - semáforo

FASE 4
- preparar backfill por bloques de:
  - mermas
  - producción
  - transferencias
- dejar herramientas de conciliación y reportes por rango

FASE 5
- endurecer agente IA para que solo responda cifras oficiales cuando la fuente esté conciliada
- etiquetar claramente la fuente de cada respuesta

FASE 6
- dejar Guamuchil correctamente controlada como sucursal preapertura
- sin contaminar dashboards ni cierres
- documentar cómo activarla cuando abra

FASE 7
- cerrar la capa de despliegue servidor con Celery + Beat + Redis para Railway
- sin activar producción todavía si no se pide

ENTREGABLES
- cambios reales en código
- migraciones si aplican
- tests o validaciones mínimas por cada frente
- documentación corta de operación
- resumen final con:
  - qué quedó cerrado
  - qué quedó parcial
  - qué dato falta si algo no puede cerrarse

NO QUIERO
- explicación sin implementación
- prompts genéricos
- duplicidad de lógica
- dashboards bonitos pero no auditables
- cifras inventadas por el agente
```

## Criterio Final De Cierre

Este paquete se considera cerrado solo cuando:

1. los 7 frentes tengan estatus explícito `cerrado` o `cerrado con dato pendiente identificado`,
2. el dashboard DG sea suficiente para operación diaria,
3. el agente no pueda responder cifras incorrectas como oficiales,
4. las automatizaciones críticas estén definidas,
5. la ruta de despliegue servidor esté documentada y lista.
