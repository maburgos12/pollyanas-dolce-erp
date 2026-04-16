# Propuesta de Unificación a Una Sola PostgreSQL

Fecha de corte: 2026-04-15

## 1. Objetivo

Eliminar el enredo operativo de fuentes locales mixtas y dejar al ERP con una sola base de datos canónica en PostgreSQL para:

- operación diaria
- forecast y eventos comerciales
- dashboards y BI
- runtime de agentes
- auditoría y trazabilidad
- pruebas locales alineadas con producción

Esta propuesta **no** autoriza borrar SQLite de inmediato. Primero exige validar que la PostgreSQL elegida sea:

1. la base correcta
2. con esquema completo
3. con datos defendibles
4. con lectura y escritura verificadas

Solo al final se retiran los residuos SQLite.

---

## 2. Diagnóstico actual

### 2.1 Hechos confirmados

- El ERP ya declara PostgreSQL como única ruta soportada en [config/settings.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/config/settings.py:175).
- La suite oficial de pruebas también exige PostgreSQL en [config/settings_test.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/config/settings_test.py:10).
- La canonicidad visible de ventas ya está concentrada en [ventas/services/sales_canonical_source.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/sales_canonical_source.py:1) y [ventas/services/sales_read_service.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/ventas/services/sales_read_service.py:1).
- El runtime de agentes y la rutina diaria ya están pensados para operar sobre PostgreSQL.
- El repo todavía contiene residuos SQLite:
  - archivos físicos legacy
  - script legado de migración desde SQLite ya retirado
  - script piloto no rastreado que consume snapshot SQLite
- El `.env` actual apunta a `pollyana_db`, que hoy no es una base defendible para trabajo operativo del ERP.

### 2.2 Problema raíz

El problema principal no es “que exista SQLite”, sino esto:

1. el hilo arranca con un PostgreSQL local no validado
2. la base activa puede ser técnicamente accesible pero operativamente incorrecta
3. existen snapshots SQLite históricos que confunden a personas y scripts
4. no había una compuerta obligatoria que frenara trabajo sensible sobre una base equivocada

### 2.3 Riesgo real de negocio

Si se sigue trabajando sin cerrar esta gobernanza:

- dashboards pueden leer datos de una base equivocada
- forecast puede recalcular sobre datos incompletos
- agentes pueden generar conclusiones inconsistentes
- una limpieza prematura de SQLite puede destruir el último rastro de un snapshot útil sin haber conciliado PostgreSQL

---

## 3. Estado objetivo

### 3.1 Política final

Debe existir **una sola base PostgreSQL canónica** para el ERP operativo.

Todo lo demás queda clasificado así:

- SQLite: solo legado histórico aislado, no operativo
- bases locales vacías o de laboratorio: no defendibles para operación
- bases de test: válidas solo para `settings_test`
- base canónica viva: única autorizada para operación, forecast, BI y runtime

### 3.2 Regla de oro

Ningún hilo, script, comando, vista, agente o dashboard puede:

- leer desde SQLite
- usar una PostgreSQL local no validada como si fuera la viva
- responder a Dirección General con datos fuera de la base canónica

---

## 4. Fuente de verdad por capa

### 4.1 Base de datos

- Fuente única: PostgreSQL canónica viva
- Resolución de conexión:
  1. `DATABASE_URL`
  2. `DATABASE_PUBLIC_URL` cuando `railway.internal` no resuelva localmente
- `DB_HOST` + `DB_NAME` + `DB_USER` + `DB_PASSWORD` + `DB_PORT` quedan solo como fallback local controlado, nunca como garantía de “base viva”

### 4.2 Ventas

Orden correcto ya documentado en [docs/CANONICIDAD_VENTAS_ERP.md](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/docs/CANONICIDAD_VENTAS_ERP.md:1):

1. extracción Point
2. staging validado
3. publicación canónica
4. refresco de derivados
5. invalidación de cache
6. consumo desde servicio compartido

### 4.3 Runtime de agentes

Debe observar siempre:

1. contexto
2. memoria
3. diagnóstico estricto de DB
4. lectura sobre PostgreSQL canónica
5. ejecución auditable

### 4.4 Forecast comercial

La unidad de verdad sigue siendo:

- `SKU x sucursal x día`

Y el ingreso:

- `precio real vigente x piezas forecast`

Eso se calcula únicamente sobre PostgreSQL canónica.

---

## 5. Orden correcto para programar y guardar todo en el sistema

Esta es la parte más importante del rediseño.

### 5.1 Orden de programación

El orden recomendado de implementación es:

1. **Gobernanza de conexión**
   - definir una única base canónica
   - bloquear bases no defendibles
   - documentar y automatizar el preflight

2. **Protección de lectura**
   - prohibir SQLite en runtime operativo
   - centralizar lecturas visibles vía servicios canónicos
   - eliminar lectores sueltos de fuentes crudas

3. **Protección de escritura**
   - garantizar que toda escritura operativa persista en PostgreSQL canónica
   - fallar si la persistencia no se confirma
   - no esconder errores como “fue tema de SQLite”

4. **Conciliación de datos**
   - comparar PostgreSQL candidata vs snapshots históricos
   - decidir qué datos faltan realmente
   - migrar solo con evidencia

5. **Retiro de residuos**
   - desactivar scripts legacy
   - mover SQLite a archivo histórico fuera de flujo
   - borrar solo cuando exista reconciliación firmada

6. **Blindaje permanente**
   - pruebas
   - comandos de diagnóstico
   - guards en AGENTS
   - runbooks actualizados

### 5.2 Orden correcto de guardado en el sistema

Para cualquier flujo operativo, el orden de persistencia debe ser:

1. **Captura cruda**
   - guardar payload o staging crudo
   - conservar origen, fecha, usuario/job, endpoint y checksum

2. **Normalización**
   - homologar sucursal, producto, receta y claves
   - registrar gaps/matches pendientes

3. **Publicación canónica**
   - escribir en tablas canónicas ERP
   - marcar versión, ventana y cobertura

4. **Derivados**
   - facts, agregados, dashboards, snapshots, ventanas de refresh

5. **Invalidación de cache**
   - scopes `ventas`, `dashboard` y equivalentes

6. **Lectura visible**
   - UI, BI, gateway y agentes leen solo desde la capa canónica o su servicio compartido

7. **Auditoría**
   - registrar resultado, fechas, actor, evidencia y estado final

### 5.3 Orden correcto para cambios de negocio sensibles

Para forecast, producción, compras, dashboard y runtime:

1. leer contexto y memory
2. correr diagnóstico de DB
3. validar esquema y datos mínimos
4. ejecutar cálculo
5. persistir snapshot/versionado
6. verificar lectura posterior
7. solo entonces informar “aplicado”

---

## 6. Propuesta de corrección por fases

## Fase 0. Congelamiento de ambigüedad

Objetivo:
- impedir que nuevos hilos arranquen ciegos

Acciones:
- mantener obligatorio `./scripts/diagnose_erp_runtime_context.sh --strict`
- mantener retirado el script legacy de migración desde SQLite
- no borrar SQLite todavía

Estado:
- ya iniciado en este repo

Rollback:
- revertir guards y comando de diagnóstico
- no recomendado

## Fase 1. Selección oficial de la PostgreSQL canónica

Objetivo:
- escoger una sola DB operativa

Acciones:
1. identificar la base viva real
2. cargar `DATABASE_URL` o `DATABASE_PUBLIC_URL` real
3. validar:
   - tablas críticas
   - datos maestros
   - ventas
   - orquestación
   - inventario
   - reportes críticos
4. fijar esa conexión como ruta oficial del repo

Validación mínima:
- `core_sucursal`
- `pos_bridge_*` críticos
- `ventas_*` críticos
- `orquestacion_*`
- `reportes_productbusinessrule`

Bloqueo:
- si no existe URL viva, no seguir a Fase 2

Rollback:
- volver al estado actual sin borrar ningún snapshot

## Fase 2. Reconciliación contra legados

Objetivo:
- demostrar que PostgreSQL ya contiene lo necesario

Acciones:
1. catalogar cada `.sqlite3`
2. clasificarlo:
   - histórico
   - laboratorio
   - snapshot parcial
3. comparar contra PostgreSQL canónica:
   - tablas
   - conteos
   - fechas máximas
   - cobertura por módulo
4. documentar diferencias reales

Salida esperada:
- acta de reconciliación por fuente

Bloqueo:
- si falta data material, no se borra SQLite

Rollback:
- conservar todos los archivos legacy sin tocar

## Fase 3. Migración controlada de faltantes

Objetivo:
- traer a PostgreSQL solo lo que realmente falte

Acciones:
1. no cargar a ciegas dumps completos
2. migrar por bounded context
3. validar idempotencia
4. registrar evidencia antes/después

Orden sugerido:
1. maestros/catálogos
2. sucursales y homologaciones
3. POS bridge histórico útil
4. ventas publicadas
5. reportes críticos
6. orquestación si aplica

Regla:
- cada migración debe tener script reproducible y reporte

Rollback:
- snapshot/backup previo de PostgreSQL
- reversa por lote importado

## Fase 4. Corte operacional definitivo

Objetivo:
- dejar una sola base de operación

Acciones:
1. actualizar `.env` local oficial
2. eliminar referencias operativas a `pollyana_db`
3. dejar que todo comando sensible falle si no usa la DB canónica
4. endurecer scripts y comandos con `CommandError`

Resultado esperado:
- un solo contexto de DB defendible

Rollback:
- restaurar `.env` previo
- mantener snapshots PostgreSQL

## Fase 5. Retiro de SQLite

Objetivo:
- sacar definitivamente SQLite del flujo

Acciones:
1. mover snapshots históricos a carpeta de archivo fuera del runtime
2. quitar cualquier script operativo que los use
3. borrar solo residuos confirmados como innecesarios

Regla:
- no borrar nada sin reconciliación firmada

Rollback:
- restaurar archivo desde backup

---

## 7. Propuesta concreta de orden técnico de implementación

### Sprint A. Asegurar la compuerta

Entregables:
- diagnóstico estricto
- guard en AGENTS
- fallas honestas en comandos críticos

Estado:
- ya avanzado

### Sprint B. Nombrar la base canónica

Entregables:
- variable oficial `DATABASE_URL` o `DATABASE_PUBLIC_URL`
- `.env` saneado
- runbook de conexión

### Sprint C. Auditoría de cobertura

Entregables:
- tabla módulo -> fuente -> estado -> brecha
- reconciliación SQLite vs PostgreSQL

### Sprint D. Migración de brechas

Entregables:
- scripts por bounded context
- reporte de antes/después

### Sprint E. Retiro de residuos

Entregables:
- SQLite fuera del flujo
- documentación final

---

## 8. Qué no debe hacerse

- No apuntar el repo a una base distinta solo porque “tiene más tablas”.
- No usar ejemplos de tests como credenciales reales.
- No borrar `.sqlite3` antes de reconciliar.
- No usar `dumpdata/loaddata` masivo como solución ciega.
- No mezclar producción con bases de validación o test.
- No permitir que scripts piloto no versionados vuelvan a definir la verdad operativa.

---

## 9. Checklist de aceptación final

- [ ] Existe una sola `DATABASE_URL` o `DATABASE_PUBLIC_URL` oficial para operación.
- [ ] `./scripts/diagnose_erp_runtime_context.sh --strict` pasa en verde.
- [ ] Las tablas críticas existen en la base activa.
- [ ] Las capas críticas tienen datos defendibles.
- [ ] Ventas visibles, BI, gateway y agentes leen desde la misma política canónica.
- [ ] No queda ningún comando operativo leyendo SQLite.
- [ ] SQLite queda archivado o eliminado fuera del runtime.
- [ ] Existe runbook de rollback.

---

## 10. Datos faltantes para cerrar al 100%

Hoy siguen faltando estos insumos para ejecutar el corte final:

1. la `DATABASE_URL` o `DATABASE_PUBLIC_URL` viva real
2. confirmación de cuál base PostgreSQL es la operativa
3. reconciliación firmada de snapshots SQLite vs PostgreSQL

Sin esos tres puntos, sí podemos ordenar el repo y blindarlo, pero no debemos declarar “corte completo” ni borrar SQLite definitivamente.

---

## 11. Recomendación ejecutiva

La decisión correcta no es “borrar SQLite ya”.

La decisión correcta es:

1. fijar una sola PostgreSQL canónica
2. bloquear cualquier base no defendible
3. reconciliar legados
4. migrar brechas
5. retirar SQLite al final

Eso sí reduce el caos sin poner en riesgo el ERP.

---

## 12. Rollback general

Si una fase falla:

1. detener cambios de estado
2. volver a la última `DATABASE_URL` defendible
3. restaurar snapshot PostgreSQL previo
4. conservar snapshots SQLite sin borrar
5. dejar evidencia del bloqueo

Nunca presentar el sistema como “ordenado” o “ya corregido” si:

- la persistencia no se verificó
- el diagnóstico estricto sigue rojo
- la reconciliación no está cerrada
