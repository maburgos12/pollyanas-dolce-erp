# Dashboard consolidado de mermas

## Estado

Diseño funcional y técnico aprobado por Mauricio Burgos el 25 de julio de 2026.
Este documento no autoriza por sí solo cambios en producción ni alteraciones de
datos operativos.

## Problema

El ERP ya contiene dos flujos de merma distintos:

- `MermaRegistro` y `MermaProducto` administran producto terminado desde la
  salida de sucursal hasta la recepción en CEDIS.
- `MermaInsumo`, `MermaInsumoEvento` y `OrdenAjustePoint` administran la captura,
  autorización y ajuste de insumos en la sucursal.

El panel `/mermas/` muestra principalmente producto terminado. La merma de
insumos solo aparece en la App Operativa para captura, autorizaciones pendientes
y los últimos ocho registros del usuario. No existe una consulta gerencial
consolidada por periodo, sucursal, insumo, motivo, costo y estado.

## Objetivo

Convertir el módulo existente de Mermas en el punto único de consulta, control y
auditoría para producto terminado e insumos, sin mezclar sus ciclos operativos ni
duplicar la lógica de autorización.

El módulo vivirá en `Comercial → Mermas` y conservará `/mermas/` como ruta
principal.

## Principios

1. Producto e insumos comparten navegación, pero conservan modelos, métricas,
   estados y procesos independientes.
2. Point continúa como fuente operativa de existencia y unidad para insumos.
3. El costo histórico se congela y nunca se recalcula con precios posteriores.
4. Las tarjetas, gráficas, tabla y exportación deben cuadrar con los mismos
   filtros y reglas.
5. El dashboard consulta y audita; no reejecuta órdenes Point ni reescribe
   eventos históricos.
6. La ausencia de costo no bloquea la captura o autorización, pero nunca se
   sustituye con un valor inventado.

## Navegación

El menú `Mermas` se trasladará del grupo Logística al grupo Comercial. Dentro
del módulo habrá cuatro pestañas:

### Resumen general

Presenta indicadores y tendencias comunes por número de registros, sucursal,
motivo y estado. Sus cantidades físicas no se sumarán cuando tengan unidades
incompatibles.

Esta fase valoriza económicamente la merma de insumos. `MermaProducto` no conserva
un snapshot de costo histórico, por lo que el Resumen no mostrará un costo
monetario combinado de producto e insumos. El costo histórico de producto
terminado requerirá un diseño posterior específico.

### Producto

Conserva el panel actual de producto terminado:

- salida de sucursal;
- cola y recepción CEDIS;
- repartidor;
- cantidades enviadas y recibidas;
- diferencias;
- evidencias;
- seguimiento detallado.

### Insumos

Incorpora análisis por sucursal, insumo, motivo, costo histórico, responsable,
jefe autorizador, estado y resultado de la orden Point.

### Autorizaciones

Reutiliza los endpoints y reglas vigentes para:

- aprobar;
- solicitar aclaración;
- rechazar;
- reenviar;
- reasignar casos sin responsable.

No se duplicará lógica de negocio en el dashboard.

## Periodo y filtros

La vista abre en el mes actual y compara contra el mes calendario anterior.

Los filtros serán:

- rango de fechas;
- sucursal;
- tipo de merma: producto o insumo;
- producto o insumo;
- motivo;
- estado;
- usuario que registró;
- jefe que autorizó.

Los filtros se conservarán al cambiar de pestaña y serán la fuente única para
indicadores, gráficas, tabla y exportación.

## Indicadores

### Indicadores principales

- costo confirmado de insumos;
- variación contra el periodo anterior;
- costo pendiente de autorización de insumos;
- número de registros confirmados;
- número de registros pendientes;
- registros sin costo;
- sucursal con mayor impacto económico de insumos;
- insumo con mayor impacto;
- motivo predominante.

### Reglas económicas

- `Costo confirmado de insumos` incluye únicamente mermas de insumo aprobadas o
  aplicadas.
- Para insumos usa `cantidad_aprobada × costo_unitario_historico`.
- Las mermas enviadas, sin responsable o en aclaración se muestran como costo
  pendiente usando la cantidad reportada.
- Las rechazadas permanecen en auditoría, pero aportan `$0` al impacto real.
- Los estados técnicos se muestran de forma separada y no se ocultan:
  `EJECUTANDO`, `REQUIERE_REVISION` e `INTERVENCION_TECNICA`.
- Los registros sin costo se excluyen del total monetario y se cuentan
  explícitamente; la interfaz no debe presentarlos como costo cero conocido.

## Costo histórico de insumos

### Momento del snapshot

El costo se congela cuando la sucursal envía la merma por primera vez. Un reenvío
por aclaración conserva el snapshot original.

### Fuente

La fuente primaria será `CostoInsumo`: el costo canónico con la fecha más
reciente que no sea posterior a `MermaInsumo.creado_en`.

Si la merma no tiene un `Insumo` canónico vinculado o no existe un costo histórico
válido, el registro queda marcado como `SIN_COSTO`. No se usará el costo actual
como sustituto.

### Datos que deben persistirse

`MermaInsumo` deberá conservar un snapshot inmutable equivalente a:

- costo unitario histórico;
- moneda;
- fecha efectiva del costo;
- identificador y tipo de fuente;
- estado de valorización;
- fecha en que se resolvió el snapshot.

El costo total confirmado se deriva de la cantidad aprobada y el costo unitario
histórico. No debe almacenarse como una segunda verdad editable si puede
calcularse de manera determinista.

### Registros existentes

Una migración de datos controlada intentará valorizar los registros históricos
con el mismo criterio temporal:

1. resolver el `Insumo` canónico ya vinculado;
2. buscar el último `CostoInsumo.fecha <= MermaInsumo.creado_en`;
3. guardar el snapshot y su fuente;
4. marcar como `SIN_COSTO` cualquier caso sin evidencia suficiente;
5. producir conteos antes y después para revisión.

La migración no cambiará cantidades, estados, responsables, eventos ni órdenes
Point.

## Detalle auditable

Cada fila de insumo abrirá un expediente de solo lectura con:

- sucursal, fecha y usuario reportante;
- insumo, código Point y unidad Point;
- cantidad reportada y aprobada;
- motivo, comentario, fotografía o justificación sin fotografía;
- costo unitario histórico, moneda, fecha, fuente y costo confirmado;
- jefe asignado y jefe autorizador;
- línea de tiempo de `MermaInsumoEvento`;
- estado actual;
- orden Point;
- existencia anterior y posterior;
- referencia Point;
- intentos, evidencia técnica y errores.

El detalle no permitirá borrar eventos, editar cantidades aprobadas ni reaplicar
órdenes.

## Tabla de insumos

La tabla mostrará, como mínimo:

- fecha;
- sucursal;
- insumo;
- motivo;
- cantidad reportada;
- cantidad aprobada;
- unidad;
- costo unitario histórico;
- costo total;
- estado;
- reportante;
- autorizador;
- resultado Point;
- acceso al detalle.

Debe admitir ordenamiento, paginación y estado vacío explícito. Los totales no
se calcularán exclusivamente sobre la página visible.

## Visualizaciones

- tendencia diaria o semanal del costo de insumos confirmado y pendiente;
- costo de insumos por sucursal;
- costo por insumo;
- distribución por motivo;
- distribución por estado.

Las leyendas y ejes deben mostrar unidades monetarias o conteos. No se graficará
una suma de kilogramos, piezas y litros como si fueran una misma cantidad.

## Permisos

### Alcance general

Usuarios de Dirección, administración y responsables de Ventas con acceso
`mermas.dashboard` pueden consultar todas las sucursales.

### Alcance de sucursal

Las jefas autorizadas pueden consultar únicamente su sucursal y atender las
solicitudes que ya les asigna el flujo actual.

### Captura

Los empleados con `mermas.captura` conservan la App Operativa y sus registros
recientes. No obtienen acceso automático al dashboard general ni a costos de
otras sucursales.

Las consultas, exportaciones y detalles aplicarán el alcance en servidor; ocultar
controles en HTML no cuenta como autorización.

## Exportación Excel

La exportación respetará los filtros activos y tendrá dos hojas:

### Resumen

- totales del periodo;
- comparación contra el periodo anterior;
- desglose por sucursal;
- desglose por insumo o producto;
- desglose por motivo;
- desglose por estado;
- conteo de registros sin costo.

### Detalle

Una fila por merma con cantidades, costo histórico, responsables, fechas, estado
y referencia Point.

Las fotografías se incluirán como enlaces protegidos. No se incrustarán archivos
binarios dentro del libro.

## Errores y consistencia

- Una falla de lectura de Point no puede crear, duplicar ni reaplicar una orden.
- Un reintento de aprobación debe conservar la idempotencia existente.
- Un registro sin costo seguirá visible y accionable.
- Los casos sin responsable aparecerán en Autorizaciones para Dirección.
- Si una consulta analítica falla, la pantalla mostrará un error recuperable sin
  alterar el filtro ni ejecutar acciones operativas.
- La exportación no se generará con datos parciales silenciosos.
- Los eventos y órdenes técnicas permanecerán como evidencia inmutable.

## Arquitectura recomendada

Se ampliará el módulo `mermas` existente en lugar de crear un segundo sistema de
reportes.

Las responsabilidades sugeridas son:

- una capa de consulta para aplicar alcance, filtros y periodo;
- una capa de métricas compartida por HTML y Excel;
- vistas de dashboard y detalle sin lógica de escritura;
- reutilización directa de los servicios existentes de autorización;
- una migración de esquema y datos para el snapshot de costo;
- templates separados por pestaña para evitar concentrar toda la interfaz en un
  único archivo.

No se propone una tabla analítica duplicada ni una vista materializada en esta
fase. Podrá evaluarse más adelante únicamente si el volumen real lo exige.

## Rendimiento

- Las consultas deben usar agregaciones de base de datos y evitar recorridos
  N+1 sobre eventos, costos y órdenes.
- La tabla se paginará en servidor.
- Los filtros de sucursal, estado y fecha deben aprovechar índices existentes o
  índices específicos justificados por el plan de consultas.
- La comparación con el mes anterior no debe duplicar todas las consultas del
  detalle.
- La exportación debe reutilizar la consulta filtrada sin cargar fotografías.

## Validación

### Dominio y costos

- snapshot con costo anterior o igual a la fecha de la merma;
- rechazo de costos posteriores;
- conservación del costo después de una aclaración;
- cálculo con cantidad aprobada;
- exclusión monetaria y conteo de casos sin costo;
- backfill idempotente y sin modificación de movimientos operativos.

### Permisos

- Dirección y administradores ven todas las sucursales autorizadas;
- jefa ve solo su sucursal;
- capturista no accede al dashboard ni a exportaciones;
- detalle y Excel no permiten evadir el alcance mediante parámetros.

### Dashboard

- periodo por defecto y comparación anterior;
- filtros combinados;
- totales consistentes entre tarjetas, gráficas y tabla;
- producto e insumos no mezclan cantidades incompatibles;
- estados pendientes, rechazados y técnicos se clasifican correctamente.

### Integraciones

- aprobación reutiliza el servicio existente;
- reintentos no duplican `OrdenAjustePoint`;
- fallas Point no corrompen el dashboard;
- referencias y existencias antes/después aparecen en el detalle.

### Exportación y navegador

- Excel con hojas Resumen y Detalle;
- totales iguales a la vista filtrada;
- enlaces de evidencia protegidos;
- revisión en escritorio, iPhone y Android;
- validación productiva sin crear mermas ni ajustes ficticios.

## Despliegue previsto

La futura implementación requerirá aprobación específica porque incluye modelo,
migración de datos, permisos, navegación y producción.

El despliegue deberá:

1. respaldar la base;
2. registrar conteos de mermas de insumos por estado;
3. aplicar migraciones;
4. revisar el resultado del backfill y los casos `SIN_COSTO`;
5. ejecutar checks y pruebas;
6. desplegar mediante `scripts/deploy_web_safe.sh`;
7. validar permisos, dashboard y exportación en producción;
8. verificar que captura y autorizaciones existentes sigan operativas.

## Fuera de alcance

- recepción física de insumos en CEDIS;
- cambios a la captura de producto terminado;
- modificación de existencias Point desde el dashboard;
- edición o eliminación de eventos históricos;
- recosteo retroactivo con precios actuales;
- creación de una nueva plataforma BI;
- cambios a nómina, ventas o datos maestros no relacionados.

## Criterios de aceptación

1. `Comercial → Mermas` abre un módulo con Resumen, Producto, Insumos y
   Autorizaciones.
2. El mes actual y la comparación contra el anterior se muestran por defecto.
3. El usuario puede filtrar por fecha, sucursal, tipo, artículo, motivo, estado,
   reportante y autorizador.
4. El costo confirmado de insumos usa cantidad aprobada y costo histórico
   congelado.
5. Pendientes y rechazadas no inflan el impacto confirmado.
6. Los casos sin costo son visibles y cuantificados.
7. Cada merma de insumo tiene un detalle auditable con eventos y orden Point.
8. Los permisos se aplican en servidor por rol y sucursal.
9. Excel y pantalla entregan los mismos resultados filtrados.
10. La captura móvil, autorizaciones y órdenes Point existentes no presentan
    regresiones.
