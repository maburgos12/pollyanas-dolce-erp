# Inventario con fuente única Point

**Fecha:** 2026-08-22
**Estado:** diseño aprobado para revisión escrita
**Alcance inicial:** consultas de existencias de insumos, separación ALMACÉN/CEDIS y bloqueo de decisiones con información vencida.

## Problema

El ERP mantiene existencias y movimientos provenientes de varias rutas: Point, compras, consumos BOM, ajustes, conteos, importaciones y procesos internos de producción. Distintos reportes vuelven a sumar esas rutas y ubicaciones, por lo que presentan saldos que no corresponden ni a ALMACÉN ni a CEDIS.

La corrección no consiste en cambiar una cifra negativa ni en sumar ambos libros. Se necesita un contrato único que determine de dónde sale cada existencia, su ubicación, unidad, momento de captura y aptitud para tomar decisiones.

## Decisiones aprobadas

1. **Point es la única fuente madre del inventario operativo.**
2. **El ERP es una réplica de consulta y auditoría.** No reconstruye el stock operativo sumando movimientos propios.
3. **ALMACÉN y CEDIS son libros independientes.** Nunca se compensan ni se presentan como un saldo disponible combinado.
4. **Los reportes pueden mostrar el último dato Point con su fecha y estado de frescura.**
5. **Compras automáticas, MRP y cualquier decisión automática fallan cerradas** cuando falta una lectura vigente de Point.
6. **No se consultará Point cada diez minutos desde cada módulo.** La captura será centralizada y los consumidores leerán una sola réplica canónica.
7. La generación automática de solicitudes de compra permanecerá desactivada hasta completar y validar este contrato.

## Dominios y ubicaciones

| Dominio | Ubicación de negocio | Sucursal Point | Uso permitido |
| --- | --- | --- | --- |
| Inventario de compra | ALMACÉN | Almacen | Recepciones, disponibilidad para surtir y decisiones de compra |
| Inventario entregado a producción | CEDIS | CEDIS | Producción, MRP, recetas, consumo y reabasto |
| Proceso interno | CFP, ARMADO y otras estaciones | No aplica | Trazabilidad de lotes y trabajo en proceso; nunca se suma al stock Point |
| Producto terminado | CEDIS producto terminado | La fuente Point que corresponda al producto | Reporte separado del inventario de insumos |

Los códigos históricos `ALMACEN_1` y `CUARTO_FRIO` pueden permanecer internamente durante la transición, pero las APIs y pantallas nuevas expondrán únicamente `ALMACÉN` y `CEDIS`. `CUARTO_FRIO` no se presentará como si fuera una sucursal Point distinta de CEDIS.

## Arquitectura

### 1. Captura central de Point

Los trabajos de sincronización de Point son los únicos que alimentan la réplica canónica. Cada lectura conserva:

- producto o insumo Point;
- insumo canónico ERP;
- sucursal Point y ubicación de negocio;
- cantidad y unidad originales de Point;
- cantidad normalizada para cálculo;
- fecha efectiva de captura;
- trabajo de sincronización y resultado;
- estado de frescura.

La tabla histórica `PointInventorySnapshot` conserva evidencia. Un servicio de lectura canónico selecciona la última posición válida por insumo y ubicación; ningún consumidor consulta ni agrega snapshots por su cuenta.

### 2. Servicio único de lectura

Se introducirá el límite de dominio `CanonicalPointInventoryService`, con operaciones explícitas:

- obtener existencia de un insumo en ALMACÉN;
- obtener existencia de un insumo en CEDIS;
- obtener un lote de existencias para un conjunto de insumos y una sola ubicación;
- devolver cantidad, unidad, fecha, origen y estado de frescura;
- exigir una lectura vigente para una decisión operativa.

El servicio no tendrá una operación de “stock total empresarial”. Cualquier intento de consultar sin ubicación será inválido.

### 3. Frescura y disponibilidad

La frescura se evaluará contra el ciclo de sincronización esperado de la ubicación, no mediante accesos independientes cada diez minutos.

- Una lectura es **vigente** si pertenece al último ciclo exitoso exigible para esa ubicación.
- Es **vencida** cuando existe una lectura anterior, pero no cubre el ciclo esperado.
- Es **faltante** cuando no existe una lectura oficial del insumo en esa ubicación.
- Es **fallida** cuando el ciclo de Point terminó con error o parcialidad que afecta esa ubicación.

Las pantallas informativas muestran la última cantidad disponible junto con la fecha y la advertencia correspondiente. Las decisiones automáticas y acciones que comprometan inventario reciben un error de dominio y no continúan.

### 4. Unidades

La cantidad original de Point se conserva sin alteraciones. Para cálculo se normaliza únicamente dentro de la misma dimensión:

- gramos a kilogramos para presentación;
- mililitros a litros para presentación;
- piezas permanecen como piezas.

No se suman kilogramos, litros y piezas. Los agregados se separan por unidad y ubicación. La interfaz muestra kg o L para evitar cifras artificialmente grandes, pero mantiene precisión suficiente para reconciliar contra Point.

### 5. Escrituras y movimientos ERP

Los movimientos internos dejan de ser una fuente alternativa de existencia Point:

- Compras puede registrar solicitudes, órdenes y recepciones, pero una recepción ERP no incrementa por sí sola el saldo operativo mostrado.
- Un ajuste, merma o transferencia originado en ERP debe enviarse al flujo autorizado de Point.
- El ERP solo refleja el nuevo saldo cuando Point lo confirma y una captura oficial lo incorpora.
- Los movimientos ERP permanecen como evidencia del proceso, con actor, referencia e idempotencia.
- Los movimientos de CFP, ARMADO y otras estaciones continúan como trazabilidad productiva separada.

La reconstrucción de saldos históricos no forma parte de la primera entrega. Se hará después mediante una conciliación Point-versus-ERP por insumo y ubicación, con vista previa, respaldo, autorización y lectura posterior.

## Consumidores que deben migrarse

La migración se realizará por grupos para poder probar cada contrato:

1. **Críticos de decisión:** compras automáticas, solicitudes sugeridas, MRP y reabasto.
2. **Críticos informativos:** Dashboard de Dirección, BI, alertas, faltantes y cierre operativo.
3. **APIs y reutilizadores:** APIs públicas/internas, sugerencias, agentes y orquestación.
4. **Auditoría:** consumo real y discrepancias, manteniendo el libro y la ubicación explícitos.

Cada consumidor declarará su ubicación:

- Compras y abastecimiento consultan ALMACÉN.
- Producción, MRP, recetas y consumo consultan CEDIS.
- Dirección puede comparar ALMACÉN contra CEDIS, pero nunca compensarlos.

Las pantallas actualmente acotadas correctamente por ubicación se conservarán y se migrarán al servicio común sin cambiar su resultado funcional.

## Comportamiento ante errores

- Point no disponible: mostrar última captura y su antigüedad; bloquear decisiones.
- Insumo sin código o categoría Point: marcar `faltante`, nunca asumir cero.
- Sucursal Point sin correspondencia: marcar error de configuración y excluir de decisiones.
- Trabajo parcial: afectar solamente las ubicaciones o insumos no confirmados; no declarar el ciclo completo como vigente.
- Unidad incompatible: rechazar la lectura para cálculo y conservar evidencia diagnóstica.
- Duplicado de captura: resolver por identidad idempotente del trabajo, sucursal, insumo y fecha efectiva.

## Estrategia de implementación

### Entrega 1: contrato y contención

- Crear el servicio canónico y los tipos de resultado/frescura.
- Probar que toda consulta exige ubicación.
- Probar que ALMACÉN y CEDIS nunca se suman.
- Probar presentación en kg/L/pza.
- Probar bloqueo de decisiones con datos vencidos, faltantes o fallidos.
- Mantener desactivadas las solicitudes automáticas.

### Entrega 2: consumidores críticos

- Migrar Compras, MRP, reabasto, Dashboard, BI, alertas y faltantes.
- Eliminar agregaciones directas de `ExistenciaInsumo.stock_actual` en esos consumidores.
- Mostrar fuente y fecha en las superficies visibles.

### Entrega 3: APIs, agentes y auditoría

- Migrar APIs, sugerencias, orquestación y cálculos de auditoría.
- Añadir una verificación automatizada que impida introducir nuevas sumas globales de existencias.

### Entrega 4: escrituras y conciliación histórica

- Alinear recepciones, ajustes, mermas y transferencias con confirmación Point.
- Añadir cantidad realmente recibida donde el flujo de Compras lo requiera.
- Ejecutar conciliación histórica separada por ALMACÉN y CEDIS, nunca mediante compensación.

## Pruebas y aceptación

La implementación seguirá pruebas rojas-verdes por comportamiento. Los criterios mínimos son:

1. Un insumo con ALMACÉN negativo y CEDIS positivo conserva ambos resultados independientes.
2. Ninguna consulta canónica funciona sin ubicación.
3. Una captura vencida se muestra como vencida y bloquea decisiones automáticas.
4. Un ciclo parcial no vuelve vigente una ubicación no confirmada.
5. Un insumo sin correspondencia Point no se interpreta como existencia cero.
6. Gramos y mililitros se presentan en kg y L sin perder la cantidad original.
7. Los consumidores críticos dejan de ejecutar `Sum("stock_actual")` entre ubicaciones.
8. Las solicitudes automáticas continúan desactivadas hasta una autorización posterior.
9. `manage.py check`, `migrate --check` y las pruebas de los módulos afectados terminan sin errores.
10. La validación final se realiza en las pantallas reales, revisando respuesta, fecha, ubicación, consola y solicitudes de red.

## Despliegue y seguridad operativa

La entrega de código seguirá PR, merge, `deploy_web_safe.sh` y validación en producción. El primer despliegue no corregirá saldos ni ejecutará conciliaciones masivas. Las escrituras de datos se autorizarán por separado y usarán vista previa, transacción, actor de auditoría y lectura posterior desde Point y desde la pantalla consumidora.

## Fuera de alcance de la primera entrega

- Borrar movimientos históricos.
- Compensar ALMACÉN contra CEDIS.
- Hacer ajustes masivos de inventario.
- Cambiar credenciales de Point.
- Consultar Point desde cada pantalla o cada diez minutos.
- Reactivar compras automáticas.
- Mezclar inventario de insumos con producto terminado o trabajo en proceso.
