# Diseño: gastos y servicios correctos en el resumen de flota

## Objetivo

El resumen de flota debe mostrar el costo operativo real de mantenimiento de cada
unidad sin confundir servicios realizados, reparaciones, combustible ni trabajos
programados. El comportamiento debe ser uniforme para todas las unidades.

## Problema confirmado

- La tarjeta `Servicios` obtiene el último registro de
  `ServicioRealizadoUnidad` ordenado por `fecha_servicio`.
- La tarjeta `Gastos año` solo cuenta y suma `ReparacionUnidad`.
- Por ello, servicios con costo —como la suspensión de la Cheyenne por $6,898—
  aparecen en el historial pero no en el gasto anual.
- Un servicio con fecha futura puede convertirse incorrectamente en el “último
  servicio realizado”.
- La suspensión de la Cheyenne conserva una factura y costo válidos, pero su tipo
  de servicio no describe el trabajo efectuado.

## Comportamiento aprobado

### Resumen por unidad

Cada unidad activa mostrará:

1. `Servicios año`: cantidad y suma de servicios vigentes realizados durante el
   año actual.
2. `Reparaciones año`: cantidad y suma de reparaciones del año actual.
3. `Gasto total año`: suma de servicios y reparaciones.

No se incluyen combustible, lavados, documentos, registros iniciales de
kilometraje, servicios anulados ni servicios con fecha futura.

El total se obtiene de dos fuentes distintas y no se recaptura información. Un
registro solo pertenece a una de ellas, lo que evita duplicarlo en la suma.

### Último y próximo servicio

- `Último servicio` solo considera servicios vigentes cuya fecha sea hoy o
  anterior.
- `Próximo servicio` conserva la lógica de programación existente.
- Una fecha futura nunca se presenta como servicio ya realizado.

### Validación de captura

El formulario de “servicio realizado” rechazará fechas posteriores al día local
actual con un mensaje claro. La validación debe existir en el servidor, no solo en
el navegador.

La programación futura seguirá usando el mecanismo de próxima fecha o próximo
kilometraje; no se guardará como servicio realizado.

## Correcciones auditables de la Cheyenne GS-CH1

1. Anular `ServicioRealizadoUnidad #18`, capturado con fecha 2026-10-28, con el
   motivo `Fecha futura y servicio no confirmado`.
   - No se elimina el registro.
   - Se conservan autor, fecha de captura y contenido original.
   - Queda fuera del último servicio y de todos los totales.
2. Mantener `ServicioRealizadoUnidad #20`, costo $6,898 y factura existente.
3. Cambiar únicamente su tipo a un tipo canónico de suspensión que describa el
   trabajo realizado; no crear un segundo gasto ni mover la factura.
4. Registrar las correcciones mediante el flujo auditable existente y verificar
   antes y después los identificadores, costos y estado de anulación.

Si no existe un tipo canónico de suspensión apropiado, se reutilizará uno
existente equivalente. No se creará catálogo maestro en producción sin revisión
previa.

## Superficies afectadas

- Cálculo del resumen de flota.
- Tarjeta visible de cada unidad.
- Validación del formulario para registrar servicios realizados.
- Pruebas del módulo de logística/flota.
- Dos registros operativos concretos de la Cheyenne, después del despliegue y con
  verificación previa.

No se modifican combustible, turnos, kilometraje, documentos, permisos ni otras
unidades.

## Estrategia técnica

El cálculo se concentrará en una función reutilizable de resumen anual por
unidad. La vista no volverá a construir totales independientes con reglas
distintas. La función recibirá unidad, año y fecha local, y devolverá conteos y
sumas de servicios, reparaciones y total.

Los servicios considerados serán los vigentes, realizados en el año y con fecha
menor o igual a hoy. Los costos nulos se tratarán como cero. Las reparaciones se
filtrarán por `fecha_ingreso` del año.

## Manejo de errores

- Fecha futura: el guardado se rechaza y conserva los datos del formulario para
  corrección.
- Costo nulo: el registro puede existir, pero aporta cero al total.
- Registro anulado: no aparece en conteos, totales ni como último servicio.
- Tipo de suspensión inexistente: la corrección de datos se detiene y se reporta;
  no se improvisa un catálogo productivo.

## Pruebas de aceptación

1. Un servicio vigente con costo aumenta `Servicios año` y `Gasto total año`.
2. Una reparación aumenta `Reparaciones año` y `Gasto total año`.
3. Un servicio y una reparación se suman una sola vez cada uno.
4. Un servicio anulado no cuenta ni suma.
5. Un registro inicial de kilometraje no cuenta ni suma.
6. Un servicio futuro no cuenta, no suma y no aparece como último servicio.
7. El servidor rechaza guardar un servicio realizado con fecha futura.
8. La Cheyenne muestra la suspensión por $6,898 dentro del gasto anual después de
   corregir su tipo.
9. El registro #18 permanece consultable como anulado y no altera cifras.
10. La pantalla funciona para todas las unidades activas y conserva permisos y
    navegación actuales.

## Despliegue y verificación productiva

Después de pruebas locales y revisión del diff:

1. Claude realiza commit, PR, merge y despliegue conforme al protocolo del repo.
2. Antes de corregir datos se toma una lectura de los registros #18 y #20.
3. Se aplica la anulación/corrección de forma idempotente y auditable.
4. Se verifica en la pantalla real de la Cheyenne:
   - el registro futuro ya no es el último servicio;
   - la suspensión conserva factura y costo;
   - los conteos separados y el total anual coinciden con la base.
5. Se verifican al menos otras dos unidades para confirmar que la regla es
   general y no específica de GS-CH1.
