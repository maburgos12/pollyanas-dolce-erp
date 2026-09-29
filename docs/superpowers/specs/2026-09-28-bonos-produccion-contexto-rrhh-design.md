# Diseño — bonos de producción basados en contexto RRHH

Fecha: 2026-09-28

Estado: aprobado conceptualmente por Mauricio; pendiente revisión de esta especificación escrita.

## Problema

Bonos de producción equipara “no hubo marca” con “hubo falta”. La sincronización sólo carga asistencias e incidencias parciales, y el cálculo deriva faltas restando asistencias a un total global del periodo. Esto convierte incapacidades, festivos, descansos y otras ausencias justificadas en faltas.

El caso reproducible es Argelia:

- el corte va del 28-08-2026 al 26-09-2026;
- Capital Humano registra incapacidades contiguas hasta el 01-09-2026;
- volvió a laborar el 02-09-2026 y tiene 21 asistencias hasta el cierre;
- el 16-09-2026 es descanso obligatorio;
- bonos cuenta el 01 y el 16 como faltas porque desconoce ambos contextos.

Separadamente, los montos de Preparación y Cuartos fríos existen en modelo y backend, pero no aparecen como campos editables en la pantalla.

## Alternativas consideradas

### A. Clasificador diario compartido de RRHH y proyección trazable en bonos — recomendada

Extraer una API interna de solo lectura que clasifique empleado + fecha usando las fuentes existentes. Bonos guarda el resultado mínimo en su registro diario y calcula faltas explícitas.

Ventajas: una sola interpretación laboral, explica cada día, funciona aunque la incidencia diaria todavía no haya sido materializada, permite sincronización idempotente y evita otra captura.

Costo: migración aditiva y actualización de serializers/UI.

### B. Consumir únicamente `IncidenciaAsistencia`

Bonos penaliza sólo faltas pendientes y omite faltas conciliadas.

Ventajas: cambio pequeño.

Desventajas: una incapacidad o festivo normalmente evita que se cree una incidencia; si el barrido de RRHH no corrió, el silencio es ambiguo. No explica los días ni resuelve jornadas o vigencias.

### C. Duplicar consultas de RRHH dentro de bonos

Consultar directamente cada modelo desde `services_checador.py`.

Ventajas: rápida de escribir.

Desventajas: duplica reglas, provoca deriva con prenómina y genera N+1. Se descarta.

## Arquitectura propuesta

### 1. Contexto laboral diario de RRHH

Crear un servicio público y de solo lectura en RRHH con una salida estable:

- `codigo`: asistencia, falta, incapacidad, festivo, descanso, preingreso, postbaja, permiso, vacaciones, suspensión o exento;
- `es_exigible`;
- `falta_penalizable`;
- `motivo`;
- identificador de fuente cuando corresponda.

El servicio reutiliza los modelos existentes y permite carga por lote para un conjunto de empleados y un rango. No crea ni modifica incidencias.

Precedencia:

1. fecha fuera de la relación laboral;
2. descanso obligatorio o descanso de jornada;
3. incapacidad activa/cerrada no cancelada;
4. suspensión activa;
5. vacaciones con reserva viva;
6. permiso aprobado según la conciliación vigente de RRHH;
7. asistencia e incidencias pendientes/conciliadas;
8. ausencia sin justificación = falta penalizable.

Si existe asistencia en un día no exigible, se conserva la asistencia real y el día sigue sin convertirse en falta.

### 2. Proyección diaria del bono

Extender `RegistroDiarioProduccion` de forma aditiva:

- `estado_rrhh` nullable;
- `motivo_rrhh` vacío por defecto;
- `falta_penalizable` nullable;
- fecha completa del registro para eliminar la ambigüedad de cortes entre meses, conservando temporalmente `dia` por compatibilidad.

Los registros históricos permanecen en modo legado mientras esos campos sean null. No habrá migración de datos que recalcule o interprete periodos anteriores.

La sincronización por lote:

- precarga los registros existentes;
- crea/actualiza por operaciones masivas;
- no sobrescribe booleanos capturados manualmente cuando `capturado_por` está presente;
- sí actualiza el contexto RRHH para que la pantalla explique la discrepancia;
- produce conteos de creados, actualizados, preservados manualmente y sin cambio;
- una segunda ejecución produce cero cambios funcionales.

### 3. Cálculo

Agregar a `BonoProduccionEmpleado` un contador nullable de faltas RRHH.

- Null conserva el cálculo legado y evita que un simple deploy cambie totales.
- Tras una sincronización completa, el contador se obtiene de registros con `falta_penalizable=True`.
- Cancelación y pase de asistencia usan ese contador.
- Retardos continúan derivándose de incidencias pendientes y sólo de días con asistencia.
- `bono_extra`, `ajuste_positivo`, `ajuste_negativo` y sus descripciones nunca se reinicializan.

Para Argelia, el resultado esperado del preview es:

- 28-08 al 01-09 dentro de incapacidad: no penalizable;
- 16-09 festivo: no penalizable;
- 21 asistencias reales;
- cero faltas penalizables antes de considerar retardos;
- cualquier captura manual de Carolina permanece intacta.

### 4. Montos de Preparación y Cuartos fríos

Agregar los dos inputs faltantes al formulario existente y sus valores por defecto de renderizado. El backend ya utiliza `AREA_AMOUNT_FIELDS`, por lo que no se crea otra configuración.

Guardar un monto recalcula importes derivados, pero conserva todos los campos manuales. El despliegue no cambia $850 a $300 automáticamente. Ese cambio de datos se presentará después como preview separado.

### 5. Experiencia de sincronización

- La acción se envía una vez y deshabilita el botón mientras está en curso.
- Se muestra progreso textual y resultado estable, sin pedir actualizar repetidamente.
- Una solicitud repetida reutiliza el resultado idempotente y no duplica filas.
- Los días muestran etiquetas comprensibles: “Incapacidad”, “Festivo”, “Permiso”, “Vacaciones”, “Suspensión”, “Descanso” o “Falta”.
- Cualquier modificación de template/JS incluye aumento de `CACHE_NAME` del service worker.

## Protección de datos operativos

Antes de cualquier corrección productiva se obtendrá snapshot de:

- valores y descripciones de ajustes manuales;
- registros diarios con `capturado_por`;
- montos del periodo;
- totales y motivos actuales.

El deploy sólo aplica esquema y código. No ejecuta sincronización ni recalcula el periodo. Después se generará un preview por empleado y, con autorización separada, se aplicará la corrección. La comparación posterior debe demostrar que los campos manuales son idénticos.

## Pruebas

Desarrollo guiado por pruebas:

1. incapacidad hasta el día 1 y asistencia desde el 2 no genera falta;
2. 16 de septiembre no genera falta;
3. vacaciones, permiso aprobado, suspensión, descanso y preingreso no se convierten en falta;
4. ausencia sin justificación sí genera falta;
5. retardo pendiente conserva su efecto;
6. captura manual no se sobrescribe;
7. segunda sincronización no cambia filas ni totales;
8. corte cruzado guarda fecha completa correctamente;
9. los campos de Preparación y Cuartos fríos se muestran y guardan;
10. guardar configuración conserva bonos y ajustes manuales;
11. migración, `check`, pruebas de bonos/RRHH y verificación en navegador.

## Alcance

Archivos probables:

- `rrhh/services_asistencia_contexto.py` y pruebas RRHH;
- `bonos_produccion/models.py` y migración aditiva;
- `bonos_produccion/services_checador.py`;
- `bonos_produccion/services_recalculo.py`;
- serializers, templates/React y pruebas de bonos;
- `bonos_produccion/static/bonos_produccion/sw.js`.

Contratos compartidos afectados: modelos y migración de bonos, proyección diaria, API de captura y service worker. No se cambia autenticación, ventas, nómina ni datos productivos durante la implementación.

## Criterios de aceptación

- Argelia deja de recibir faltas por su incapacidad hasta el 1 y por el festivo del 16.
- Ninguna ausencia justificada o día no exigible se convierte en falta.
- Una falta real pendiente sigue cancelando según la regla configurada.
- Carolina puede editar Preparación y Cuartos fríos.
- La sincronización se ejecuta una vez, termina con respuesta visible y es idempotente.
- Las capturas y ajustes manuales previos conservan exactamente sus valores.
- No hay cambios productivos hasta aprobar el preview posterior al deploy.
