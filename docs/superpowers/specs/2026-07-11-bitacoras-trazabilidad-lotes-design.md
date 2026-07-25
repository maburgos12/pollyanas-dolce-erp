# Bitacoras de produccion con lotes y movimientos trazables

Fecha: 2026-07-11

Estado: diseno aprobado para preparar plan de implementacion

## Objetivo

Convertir las bitacoras de produccion en el origen operativo de movimientos trazables, sin crear un catalogo ni un inventario paralelo. La primera implementacion validara una cadena completa para Pastel Crunch Chico antes de extender el patron al resto de productos y procesos.

La cadena piloto es:

```text
Point -> recetas e insumos canonicos ERP -> Hornos -> CFP 1.1 -> Armado -> producto terminado -> Inventario CFP1
```

## Fuentes de verdad

Point es la fuente maestra para identidad comercial y operativa disponible:

- codigo;
- nombre oficial;
- presentacion;
- unidad cuando Point la entregue;
- productos finales, insumos comprados e insumos producidos internamente;
- compras, stocks y movimientos que actualmente se administran en Point.

El ERP replica esa identidad y agrega informacion que Point no modela con el detalle requerido:

- receta y componentes;
- etapa y area de produccion;
- bitacora y linea de origen;
- lote;
- ubicacion interna;
- usuario responsable;
- conciliacion fisica;
- trazabilidad entre insumo producido y producto terminado.

La jerarquia operativa sera:

| Pregunta | Fuente de verdad |
| --- | --- |
| Que capturo el empleado | Bitacora cerrada y su linea |
| Quien, cuando y en que proceso | Bitacora, linea y usuario |
| Que lote se produjo o consumio | Lote vinculado a la linea |
| Por que cambio el inventario | Movimiento generado por la linea |
| Cuanto deberia existir | Suma de movimientos confirmados |
| Cuanto habia fisicamente | Corte ciego |
| Cual es la diferencia | Fisico menos esperado |
| Que saldo muestra rapidamente el ERP | Proyeccion reconstruible por insumo y ubicacion |

## Identidad de productos

Las bitacoras deben resolver cada renglon en este orden:

1. `codigo_point`;
2. insumo canonico;
3. receta vinculada;
4. alias para registros historicos;
5. pendiente de vinculacion cuando no exista una relacion segura.

Los nombres escritos en bitacoras nunca crean ni renombran productos maestros. Un producto sin codigo Point canonico puede guardarse como borrador, pero no genera lote ni movimiento.

Las preparaciones internas deben usar el `Insumo` canonico vinculado a su `Receta` de tipo `PREPARACION`. Los productos terminados deben usar la `Receta` de tipo `PRODUCTO_FINAL` y su codigo Point.

## Alcance inicial

La primera implementacion cubre exclusivamente:

- producto piloto Pastel Crunch Chico;
- preparaciones internas reales que su receta requiera;
- Hornos o el proceso productor correspondiente;
- custodia y entrega desde CFP 1.1;
- consumo real y producto terminado en Armado;
- entrada del producto terminado a Inventario CFP1;
- corte ciego matutino y conciliacion al dia siguiente.

No incluye todavia:

- descuento automatico completo de materias primas entregadas por Point;
- integracion exhaustiva de compras y almacen;
- todas las familias de producto;
- escaneo obligatorio de etiquetas;
- sensores o basculas;
- despliegue a produccion.

La estructura debe permitir agregar esas etapas sin reemplazar lotes ni movimientos.

## Modelo de datos

### Lote de produccion

Un lote representa una cantidad identificable de un insumo interno o producto terminado. Debe contener:

- codigo unico generado por el ERP;
- insumo canonico Point;
- receta de origen;
- cantidad inicial;
- unidad;
- fecha y hora de produccion;
- linea de bitacora de origen;
- usuario responsable;
- estado operativo.

El lote no tendra un saldo editable. Su disponibilidad se obtiene de sus movimientos.

El codigo sera determinista para la linea de origen:

```text
LOT-{codigo Point normalizado}-{fecha YYYYMMDD}-{id de linea de bitacora}
```

Un ejemplo es `LOT-0060-20260711-1842`. Los lotes de apertura usaran el prefijo `INI` y el id de su registro de apertura. Los estados iniciales seran `DISPONIBLE`, `AGOTADO`, `RETENIDO` y `CANCELADO`; solo un ajuste autorizado puede cancelar un lote que ya genero movimientos.

### Movimiento de inventario

Se reutilizara `MovimientoInventario`; no se creara un segundo libro mayor. Se ampliara para admitir, cuando aplique:

- lote;
- bitacora y linea de origen;
- usuario real;
- ubicacion;
- clave idempotente.

Los movimientos historicos sin esos campos seguiran siendo validos. Los movimientos nuevos generados por bitacoras deben tener un origen verificable y una clave que impida duplicados.

### Existencia por ubicacion

La relacion correcta es:

```text
Insumo + ubicacion = existencia
```

El mismo insumo debe poder existir simultaneamente en CFP 1.1, Armado y otras ubicaciones. La estructura actual de una sola `ExistenciaInsumo` por insumo requiere una migracion compatible y una revision de todos sus consumidores.

La primera fase agregara como ubicaciones operativas `CFP_1_1`, `ARMADO` y `CFP_1`, conservando las ubicaciones de almacen existentes.

El saldo persistido sera una proyeccion reconstruible. Los movimientos confirmados son la base de calculo y auditoria.

### Transferencias

Una entrega de CFP 1.1 a Armado genera dos efectos vinculados por lote, linea de origen y referencia:

```text
- cantidad en CFP 1.1
+ cantidad en Armado
```

La primera implementacion no agregara un modelo separado de transferencia. La operacion atomica y la referencia comun agrupan ambos movimientos.

## Arranque inicial

No se inventara trazabilidad anterior a la puesta en marcha. El jefe de Produccion registrara una apertura controlada por producto:

- producto Point;
- cantidad fisica;
- fecha conocida, cuando exista;
- responsable;
- observacion.

Cuando no se conozca la fecha, el lote se identificara explicitamente como inicial sin origen historico. A partir de la activacion, todos los lotes nuevos tendran origen completo.

## Flujo operativo

### Hornos o proceso productor

1. La pantalla muestra productos canonicos Point.
2. El empleado captura preparacion producida.
3. Guardar conserva un borrador y no afecta inventario.
4. Cerrar produccion valida identidad, receta, unidad y cantidad.
5. Cada linea valida con cantidad positiva crea un lote.
6. El lote genera una entrada automatica a CFP 1.1.
7. La pantalla muestra codigo de lote, cantidad, hora y destino.

### Corte ciego CFP 1.1

Se realizara un unico corte fisico al iniciar el turno.

Antes de guardarlo, el empleado solo ve:

- fecha actual fija;
- productos Point;
- campo de existencia fisica;
- accion Guardar existencia.

No se muestran esperado, diferencias, lotes, stock fijo ni proyeccion.

Despues de guardar se muestran:

- existencia esperada;
- diferencia;
- stock fijo;
- proyeccion;
- lotes disponibles;
- hora y usuario del corte.

Durante el dia, entradas y salidas actualizan el saldo esperado. El corte ciego de la manana siguiente valida el ciclo anterior:

```text
conteo anterior + entradas - salidas +/- ajustes = existencia esperada
diferencia = conteo fisico nuevo - existencia esperada
```

No se exige un segundo conteo al cierre. El cierre diario confirma que las operaciones del turno quedaron registradas.

### Entrega a Armado

1. CFP selecciona producto y cantidad.
2. El sistema propone los lotes mas antiguos disponibles, siguiendo FIFO.
3. Una excepcion FIFO requiere motivo y usuario.
4. Una sola confirmacion genera salida de CFP y entrada en Armado.
5. Armado recibe automaticamente producto, cantidad y lotes; no vuelve a capturar la recepcion.

### Armado

1. La pantalla muestra lo recibido por producto y lote.
2. El empleado captura el consumo real de cada preparacion.
3. Registra las piezas terminadas de Pastel Crunch Chico.
4. El ERP compara consumo real contra consumo teorico de la receta, sin sustituir el dato real.
5. Al cerrar se generan consumos de lotes internos.
6. Se crea el lote de producto terminado.
7. Se genera la entrada automatica a Inventario CFP1.

Si la receta real de Pastel Crunch Chico no esta completa o no tiene componentes canonicos, se permite guardar borrador pero se bloquea el cierre del lote terminado.

## Correcciones y autorizacion

- Un empleado puede capturar y guardar borradores del dia.
- Un empleado no puede modificar directamente una bitacora cerrada.
- El jefe directo revisa diferencias y autoriza correcciones.
- Una correccion genera un movimiento compensatorio; no altera ni elimina el original.
- Toda autorizacion registra usuario, fecha, motivo y referencia al evento corregido.
- Las fechas anteriores son de consulta, no de captura ordinaria.

## Reglas de integridad

- Producto sin codigo Point: no genera movimiento.
- Codigo Point duplicado: cierre bloqueado.
- Unidad faltante o incompatible: cierre bloqueado.
- Cantidad negativa: rechazada.
- Salida o consumo superior a disponibilidad: rechazado.
- Borrador: no afecta inventario.
- Cierre repetido: devuelve el resultado existente sin duplicar.
- Dos usuarios operando el mismo lote: bloqueo transaccional y recalculo de disponibilidad.
- Error durante cierre: no se guarda ningun lote ni movimiento y la bitacora permanece en borrador.
- Excepcion FIFO: requiere motivo y queda auditada.

El cierre debe ejecutarse dentro de una transaccion de base de datos. Lote, movimientos y existencias se aplican juntos o no se aplica ninguno.

## Experiencia de uso

Se conservaran las rutas actuales de bitacoras. No se creara una aplicacion paralela.

Roles:

- empleado: captura del dia y consulta del resultado;
- jefe directo: referencias, diferencias, autorizaciones y ajustes;
- administrador: configuracion, auditoria y vinculaciones.

Comportamiento responsive:

- celular: una linea de producto a la vez;
- tablet: captura compacta y tactil;
- desktop: tabla completa con conciliacion y trazabilidad;
- acciones principales fijas al pie durante captura;
- sin bloques largos de instrucciones en la interfaz.

## Notificaciones

Una diferencia en el corte ciego genera notificacion interna al jefe de Produccion con:

- producto;
- cantidad esperada;
- cantidad fisica;
- diferencia;
- fecha y hora;
- responsable;
- enlace a la revision.

No se enviaran notificaciones por cada movimiento correcto.

## Pruebas de aceptacion del piloto

1. Hornos cierra ocho unidades y genera exactamente un lote y una entrada en CFP 1.1.
2. Repetir el cierre no genera objetos adicionales.
3. CFP recibe las ocho unidades automaticamente.
4. El corte ciego no muestra cantidades esperadas antes de guardar.
5. Una entrega de seis unidades deja dos disponibles y aparece automaticamente en Armado.
6. Armado consume componentes y genera el lote de Pastel Crunch Chico.
7. El ERP muestra consumo teorico y real sin reemplazar el real.
8. El corte de la manana siguiente clasifica correctamente faltante, sobrante u OK.
9. Una correccion autorizada conserva el movimiento original y crea compensacion.
10. Una operacion concurrente no permite consumir el mismo saldo dos veces.
11. La captura funciona en celular, tablet y desktop.
12. Los flujos existentes de inventario, compras y Point mantienen su comportamiento.

## Estrategia de entrega

La implementacion se dividira en fases verificables:

1. contratos de identidad, ubicacion, lote e idempotencia;
2. cierre de Hornos y entrada automatica a CFP 1.1;
3. corte ciego y conciliacion del dia siguiente;
4. transferencia FIFO de CFP 1.1 a Armado;
5. consumo real y lote terminado de Pastel Crunch Chico;
6. permisos, ajustes y auditoria;
7. validacion responsive y regresion de inventario;
8. extension gradual a otros productos y procesos.

No se desplegara ni se extendera a todas las bitacoras hasta que el piloto cuadre de extremo a extremo en local.
