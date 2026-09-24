# Diseño: jornadas semanales por empleado en RRHH

## Objetivo

Permitir que Capital Humano seleccione y cambie la jornada semanal de una
persona desde el alta y la edición del empleado, con vigencia, historial y
auditoría. La jornada debe resolver el turno correcto para cada día de la
semana y alimentar asistencia, incidencias y horas extra sin inferir horarios
por las checadas.

La primera aplicación productiva corresponde a seis personas administrativas.
La operación declara que estos horarios se han utilizado durante 2026, pero el
ERP los utilizará para cálculos únicamente desde el 1 de septiembre de 2026.
No se recalculará enero-agosto.

## Estado actual

- `Turno` representa una sola hora de entrada y salida.
- `AsignacionTurnoEmpleado` asigna un `Turno` a una persona dentro de un rango
  de fechas, pero aplica el mismo turno todos los días del rango.
- `turno_asignado_para_fecha()` es el contrato utilizado por las fuentes de
  asistencia para resolver el horario confirmado.
- El alta y la edición normal de empleados no capturan turnos ni jornadas.
- Las asignaciones solo pueden administrarse desde Django Admin.
- En producción, las seis personas objetivo no tienen asignaciones de turno.
- Un único turno diario no puede representar correctamente lunes-viernes y
  sábado; asignarlo directamente produciría cálculos incorrectos el sábado.

## Decisiones aprobadas

### Jornada administrativa 2026

| Día | Entrada | Salida |
| --- | --- | --- |
| Lunes a viernes | 08:00 | 16:30 |
| Sábado | 08:00 | 13:30 |
| Domingo | Descanso | Descanso |

Total programado: 48 horas semanales.

### Jornada Johana 2026

| Día | Entrada | Salida |
| --- | --- | --- |
| Lunes a viernes | 09:00 | 17:30 |
| Sábado | 08:00 | 13:30 |
| Domingo | Descanso | Descanso |

Total programado: 48 horas semanales.

### Personas objetivo

| ID RRHH | Persona | Jornada |
| ---: | --- | --- |
| 53 | Eguino Reyes Luis Octavio | Administrativa 2026 |
| 4 | Norzagaray Contreras Julieta Guadalupe | Administrativa 2026 |
| 3 | Soto Inzunza Yesenia | Administrativa 2026 |
| 99 | Figueroa Soto Johan | Administrativa 2026 |
| 33 | Lugo Espinoza Paula Elizabeth | Administrativa 2026 |
| 8 | López Palos Johana Adelin | Johana 2026 |

Quedan explícitamente excluidos limpieza, almacén y responsables de
producción, aunque alguno esté clasificado dentro de Administración.

### Vigencia inicial

- Inicio calculable: 2026-09-01.
- Fin: 2026-12-31.
- Enero-agosto de 2026 queda sin recalcular.
- La jornada 2026 no se extenderá automáticamente a 2027. Capital Humano
  deberá seleccionar una jornada 2027 compatible con el límite semanal que
  corresponda.

## Arquitectura propuesta

### Perfil semanal

Crear un catálogo `JornadaSemanal` con:

- nombre;
- descripción operativa;
- estado activo/inactivo;
- vigencia orientativa opcional;
- total semanal calculado a partir de sus días, no capturado manualmente.

Crear `JornadaSemanalDia` con:

- jornada;
- día de la semana;
- turno aplicable, o día de descanso;
- unicidad por jornada y día.

Los horarios diarios seguirán usando `Turno`. Así, los consumidores existentes
recibirán el mismo tipo de objeto y no se duplicarán reglas de entrada, salida o
tolerancia.

### Asignación a la persona

Crear `AsignacionJornadaEmpleado` con:

- empleado;
- jornada semanal;
- fecha de inicio;
- fecha de fin opcional;
- motivo;
- autor y fecha de creación;
- protección de reingesta histórica cuando aplique.

Las vigencias de una persona no podrán solaparse. Un cambio de jornada cerrará
la asignación anterior el día previo y creará una asignación nueva; nunca
sobrescribirá el historial.

### Resolución de turno

Crear `horario_programado_para_fecha(empleado, fecha)` como contrato canónico
que distinga `laborable`, `descanso` y `sin_asignación`:

1. Buscará una asignación semanal vigente.
2. Resolverá el detalle correspondiente al día de la semana.
3. Devolverá el estado y su `Turno` cuando sea laborable.
4. Si no existe asignación semanal, conservará como compatibilidad la búsqueda
   actual en `AsignacionTurnoEmpleado`.

`turno_asignado_para_fecha()` se conservará como adaptador compatible que
devuelve `Turno | None`. Los consumidores que necesiten diferenciar descanso de
una omisión deberán migrar al contrato canónico.

La jornada semanal confirmada tendrá prioridad sobre la inferencia por checada.
No se cambiará la modalidad de marcaje de los empleados en este alcance.

## Experiencia en Capital Humano

### Alta de empleado

Dentro del flujo existente se agregará la sección `Jornada` con:

- selector de jornada semanal activa;
- fecha de inicio, precargada con la fecha de ingreso;
- vista previa compacta de lunes a domingo;
- total semanal;
- opción explícita `Sin jornada asignada`, acompañada de una advertencia de que
  la asistencia y las horas extra pueden quedar no calculables.

La jornada no se inferirá automáticamente por área o puesto. Capital Humano
debe confirmarla.

### Edición de empleado

La ficha editable mostrará:

- jornada vigente;
- horario semanal;
- vigencia actual;
- historial de asignaciones;
- acción `Cambiar jornada` con nueva fecha de inicio y motivo obligatorio.

Una modificación retroactiva mostrará una advertencia con el rango que será
reevaluado. La respuesta utilizará el contrato compartido `data-async-action`,
toast accesible, bloqueo únicamente del botón presionado y conservación del
contexto. La acción se registrará en
`docs/ux/action-context-coverage.md` sin declarar cobertura total.

Solo usuarios con `can_manage_rrhh` podrán crear o cambiar asignaciones. Los
usuarios con acceso de consulta podrán ver la jornada y su historial.

## Aplicación inicial y reevaluación

La carga inicial se realizará mediante un comando idempotente con modo preview
predeterminado y `--apply` explícito. Antes de escribir mostrará:

- perfiles y turnos que creará o reutilizará;
- seis personas resueltas por ID y código;
- asignaciones vigentes o solapadas;
- asistencias desde 2026-09-01 afectadas;
- propuestas automáticas pendientes que cambiarían, se crearían o cancelarían;
- autorizadas o pagadas que presentarían una diferencia.

Al aplicar:

1. Creará o reutilizará los tres turnos diarios necesarios:
   08:00-16:30, 09:00-17:30 y 08:00-13:30; los perfiles podrán compartir el
   turno del sábado.
2. Creará los dos perfiles semanales.
3. Asignará la jornada correspondiente a las seis personas del 2026-09-01 al
   2026-12-31.
4. Reevaluará asistencias desde el 1 de septiembre, asignando el turno resuelto
   para ese día.
5. Reconciliará solamente propuestas automáticas pendientes.
6. No modificará horas extra autorizadas, pagadas, rechazadas o canceladas; las
   diferencias quedarán reportadas para revisión humana.
7. Creará `AuditLog` para catálogos, asignaciones y reevaluación.

La aplicación abortará completa y transaccionalmente si falta una persona, hay
una asignación solapada, una asistencia no corresponde al empleado/fecha o el
preview deja de coincidir con el estado bloqueado al aplicar.

## Horas extra

La salida programada se obtendrá del turno del día:

- Administrativos lunes-viernes: 16:30.
- Johana lunes-viernes: 17:30.
- Todos los sábados: 13:30.

Se conserva el umbral vigente: una propuesta automática solo nace desde 50
minutos posteriores a la salida programada. La autorización seguirá exigiendo
bloques de 30 minutos y permitirá el ajuste justificado ya aprobado.

## Migración y compatibilidad

- Se agregarán modelos nuevos; no se eliminarán ni modificarán migraciones
  existentes.
- Las asignaciones diarias históricas permanecerán válidas como fallback.
- Los consumidores de `turno_asignado_para_fecha()` no necesitarán conocer el
  nuevo modelo.
- Se revisarán Hikvision, Point, asistencia manual, reporte de asistencia,
  incidencias, prenómina y horas extra como consumidores del contrato.
- No se alterarán credenciales, modalidad de marcaje, salario, departamento,
  puesto, sucursal ni estado activo de las seis personas.

## Pruebas y aceptación

### Modelos y servicios

- No permite dos detalles para el mismo día.
- No permite vigencias solapadas por persona.
- Resuelve correctamente lunes-viernes, sábado y domingo.
- Prioriza jornada semanal y conserva fallback diario.
- El cambio de vigencia mantiene el historial.

### Alta y edición

- Capital Humano puede seleccionar una jornada al crear un empleado.
- Puede dejarla vacía con advertencia explícita.
- Puede cambiarla con fecha y motivo sin sobrescribir la anterior.
- Un usuario de solo lectura no puede cambiarla.
- Errores conservan los datos capturados y permiten reintento.

### Reconciliación

- Preview no escribe.
- `--apply` es idempotente.
- Solo afecta a las seis personas y desde 2026-09-01.
- No cambia enero-agosto.
- No modifica extras autorizadas o pagadas.
- Yesenia deja de aparecer como `Sin turno asignado` en una jornada reevaluada
  cuando corresponde.

### Validación final

- PostgreSQL aislado, migraciones completas y `migrate --check` limpio.
- `python manage.py check` sin errores.
- Pruebas enfocadas de RRHH, Hikvision, Point, asistencia y horas extra.
- Validación de alta y edición en navegador, incluidos errores de consola y
  solicitudes de red.
- PR, CI, merge y despliegue mediante `scripts/deploy_web_safe.sh`.
- Preview productivo antes de aplicar datos.
- Aplicación autorizada y verificación posterior en producción de las seis
  asignaciones, una jornada entre semana, una del sábado y el caso de Yesenia.

## Fuera de alcance

- Recalcular enero-agosto de 2026.
- Asignar jornadas a limpieza, almacén, producción o cualquier otra persona.
- Cambiar la modalidad de marcaje o las reglas de comida.
- Diseñar automáticamente la jornada 2027.
- Modificar horas extra ya resueltas.
