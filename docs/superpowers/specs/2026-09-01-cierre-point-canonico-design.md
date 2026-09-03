# Diseño: cierre mensual canónico de productos Point

Fecha: 2026-09-01
Estado: implementado; pendiente de despliegue
Alcance: `pos_bridge`, `reportes` y pruebas relacionadas

## Problema

El reporte **Producido vs vendido** presenta el inventario final de Point como si fuera un conteo físico. Además, calcula las conversiones de productos enteros a porciones a partir de las ventas y mermas de las porciones, aunque Point ya entrega movimientos de conversión (`FK_TipoMovimiento=21`).

Esto provoca dos errores de interpretación:

1. Un saldo del sistema Point aparece bajo las etiquetas “Físico”, “Sobrante físico” y “Faltante no explicado”, aun cuando ninguna sucursal capturó un conteo físico.
2. La columna de conversión puede mostrar una cantidad estimada por consumo, no la conversión realmente registrada en Point.

El cierre mensual y el reporte también reconstruyen el mismo balance por caminos diferentes. Por ello pueden mostrar totales distintos para el mismo mes.

## Objetivo

Mostrar, para cada producto homologado con Point, un balance mensual trazable que use exclusivamente fuentes operativas registradas:

El contrato será único y parametrizado por mes. Aplicará a cualquier periodo disponible en Point, sin una excepción ni un parche exclusivo para agosto de 2026.

```text
Saldo calculado = Saldo inicial Point
                 + Producción Point
                 + Conversión de entrada Point
                 - Venta Point
                 - Merma Point
                 - Conversión de salida Point
```

La comparación del mes será:

```text
Diferencia = Saldo final Point - Saldo calculado
```

El signo quedará documentado de forma visible: positivo significa que Point cerró con más unidades que las explicadas por los movimientos incluidos; negativo significa que Point cerró con menos.

## Fuera de alcance

- Captura manual del conteo físico por sucursal.
- Aprobación, bloqueo o ajuste de inventario a partir de un conteo físico.
- Corrección automática de saldos en Point o en producción.
- Inventar movimientos faltantes para forzar un cuadre.
- Reescritura automática y masiva de cierres históricos durante el deploy.

El conteo físico será una fuente separada en una fase posterior. No se reutilizará el saldo Point como sustituto de ese dato.

## Alternativas consideradas

### A. Corregir únicamente las etiquetas

Cambiar “Físico” por “Point” elimina una interpretación incorrecta, pero conserva la conversión estimada y las dos fórmulas divergentes. No resuelve la causa del resultado.

### B. Sustituir solo la conversión del reporte

Usar `PointConversionLine` en la vista actual mejora esa columna, pero el cierre mensual seguiría calculándose por otro camino. El mismo mes podría continuar mostrando resultados diferentes entre pantallas y exportaciones.

### C. Proyección canónica de movimientos Point — recomendada

Crear una proyección mensual reutilizable que conserve cada producto Point como renglón operativo. El reporte y el constructor del cierre consumirán esa misma proyección; el cierre podrá seguir guardando su proyección consolidada cuando sea necesaria, pero no volverá a reinterpretar movimientos por su cuenta.

Esta alternativa elimina la estimación de conversiones, conserva la trazabilidad por fuente y prepara el contrato para agregar después el conteo físico como una comparación independiente.

## Diseño recomendado

### 1. Servicio de balance mensual

Se agregará un servicio de dominio en `pos_bridge/services/` responsable de construir el balance por receta homologada y mes. Entregará por renglón:

- receta y producto Point homologado;
- saldo inicial Point y fecha efectiva del snapshot;
- producción;
- venta;
- merma;
- conversión de entrada;
- conversión de salida;
- saldo calculado;
- saldo final Point y fecha efectiva del snapshot;
- diferencia Point contra calculado;
- conteo de filas y procedencia de cada fuente;
- incidencias de homologación o cobertura.

Los consumidores no recalcularán la fórmula. Recibirán valores ya calculados junto con su procedencia.

### 2. Inventario inicial y final

Los snapshots Point se mantendrán por producto/receta, sin convertir porciones a “pasteles enteros equivalentes” para la tabla operativa. Esto permite comparar el producto que Point vende y almacena con sus propios movimientos.

El inicial provendrá del cierre acreditado del último día del mes anterior, con cantidades Point por receta, o de la captura de ese mismo día. El final provendrá de la última captura válida del último día del mes. No se permiten fechas cercanas: la tolerancia de fechas es cero para todos los meses. La fecha operativa del cierre no cambia porque el correo llegue de madrugada al día siguiente; tampoco se resta un día a las capturas en vivo. Se muestran los saldos disponibles independientemente de las advertencias de cobertura, sin inventar cantidades ausentes ni compararlas con un físico no capturado.

Una receta no homologada quedará como incidencia visible; no se omitirá silenciosamente ni se sumará a otro producto.

### 3. Conversiones reales

La entrada será la cantidad de destino registrada en `PointConversionLine`. Nunca se derivará de ventas o mermas.

La salida se resolverá así:

1. usar el producto origen que Point entregue, cuando exista y esté homologado;
2. si Point no entrega origen, usar la `RecetaEquivalencia` activa para identificar el padre y convertir la cantidad de destino con su factor;
3. si tampoco existe una equivalencia válida, conservar la entrada registrada, marcar el origen como no resuelto y bloquear el estado de “coincide”; no estimar una salida.

La procedencia del origen quedará indicada como `POINT` o `EQUIVALENCIA_CONFIGURADA`. El factor configurado resuelve la relación entre unidades; no sustituye la cantidad real convertida.

### 4. Ventas, producción y mermas

Cada movimiento permanecerá en la receta que Point reportó. No se convertirán automáticamente ventas o mermas de rebanadas en consumo del pastel padre dentro de la proyección operativa; ese consumo ya debe estar representado por la conversión de salida real.

Se conservarán las prioridades actuales de fuentes oficiales y sus advertencias. Una fuente de respaldo deberá aparecer como tal en la pantalla y en los metadatos.

### 5. Cierre mensual

`ProductMonthClosureService` dejará de cargar y reinterpretar cada familia de movimientos por separado. Consumirá la proyección canónica para persistir el cierre y sus metadatos.

Para evitar una migración de esquema en esta corrección, los nuevos campos de conversión y procedencia se guardarán en el `metadata` JSON ya existente de cada línea. Los campos numéricos actuales seguirán representando los valores compatibles del cierre. Si durante la implementación se comprueba que esta compatibilidad altera el significado de un campo compartido, se detendrá el cambio y se propondrá una migración separada antes de modificar el contrato.

No se reconstruirán cierres productivos automáticamente durante el deploy. El
setup de Celery registra la tarea mensual pausada y con `lock_after_build=false`;
no reactiva una tarea existente salvo el opt-in explícito
`--enable-monthly-product-closure`. Aun con ese opt-in la automatización solo
construye borradores y nunca bloquea. Agosto se reconstruirá únicamente después
de validar la nueva proyección y mediante una acción operacional explícita y
auditable.

### 6. Reporte y lenguaje visible

La tabla usará estos términos:

- `Ini. Point`: saldo inicial recibido de Point;
- `Saldo calc.`: resultado de los movimientos incluidos;
- `Fin. Point`: saldo final recibido de Point;
- `Dif. Point`: saldo final Point menos saldo calculado;
- `Coincide`: diferencia dentro de la tolerancia;
- `Point mayor`: Point cerró con más unidades;
- `Point menor`: Point cerró con menos unidades;
- `Revisar fuente`: existe una incidencia de cobertura u homologación.

La pantalla incluirá una explicación corta de la fórmula y del signo de la diferencia. Los estados no dependerán solo del color. Se eliminarán “Físico”, “Sobrante físico” y “Faltante no explicado” de esta superficie, PDF, CSV y XLSX relacionados.

Cuando en el futuro exista captura física, aparecerá como una columna y comparación independiente, por ejemplo `Conteo físico` y `Físico vs Point`.

## Compatibilidad y consumidores

Se revisarán:

- pantalla Producido vs vendido;
- JSON usado por sus exportaciones;
- PDF del reporte;
- CSV/XLSX del cierre mensual;
- constructor y vista del cierre mensual;
- pruebas de sincronización de conversiones;
- cualquier consumidor de `ProductoMonthClosureLine` detectado por el grafo de código.

No se cambiará el contrato de Point ni se escribirán datos operativos de ventas, producción, mermas o inventario.

## Validación

La implementación seguirá pruebas primero. Como mínimo cubrirá:

1. una conversión registrada en Point genera entrada en el destino y salida equivalente en el origen;
2. una venta de porciones sin conversión Point no inventa una conversión;
3. un origen ausente usa una equivalencia activa y deja registrada esa procedencia;
4. una conversión sin origen ni equivalencia queda como incidencia, sin salida estimada;
5. el saldo calculado respeta la fórmula y el signo de `Fin. Point - Saldo calc.`;
6. el reporte y el cierre consumen los mismos valores canónicos;
7. las etiquetas visibles y exportadas no afirman que existe inventario físico;
8. los cierres históricos siguen siendo legibles;
9. agosto se compara contra la evidencia productiva conocida antes de autorizar su reconstrucción.
10. al menos otro mes distinto de agosto produce el mismo contrato y fórmula, demostrando que no existe lógica especial por periodo.

## Despliegue

1. Validar en PostgreSQL local aislado.
2. Revisar consumidores y exportaciones.
3. Abrir PR de una sola tarea y validar CI.
4. Desplegar con `scripts/deploy_web_safe.sh`.
5. Verificar la pantalla productiva y las fuentes de agosto sin modificar el cierre.
6. Presentar el comparativo anterior/nuevo.
7. Solo con autorización operacional, reconstruir agosto y hacer una lectura fresca de pantalla y base.
