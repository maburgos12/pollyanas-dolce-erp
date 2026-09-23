# Identidad Hik segura después de una baja

Fecha: 2026-09-23

Estado: aprobado conceptualmente por Dirección; pendiente de revisión del documento

Responsable: Codex

Rama: `codex/rrhh-hik-baja-identidad`

## Problema confirmado

El código externo `355` perteneció a Johan Figueroa y produjo 22 eventos Hik
entre el 2 y el 15 de septiembre. El 15 de septiembre el ERP cambió a Johan al
código `356` y asignó `355` a Angélica Johana Gastélum, quien ya estaba inactiva
y tenía una baja efectiva del 10 de septiembre. El dispositivo continuó
enviando `355`; la ingesta v2 resolvió cada evento contra el propietario actual
del código y proyectó las marcas sobre Angélica.

El resultado es asistencia improcedente de Angélica desde el 17 de septiembre,
dos horas extra automáticas pendientes y ausencia de asistencia para Johan. La
continuidad horaria de `355`, el historial del ledger y la ausencia de eventos
para el nuevo código de Johan confirman que la identidad cambió en el ERP, no en
el dispositivo.

La ruta antigua de recepción ya descarta marcas posteriores a la baja. La ruta
v2 durable no ejecuta esa protección antes de crear la proyección diaria. El
generador de horas extra confía en la asistencia proyectada y por ello propaga
el error.

## Resultado esperado

1. Una marca ocurrida después de la baja no crea ni modifica asistencia, horas
   extra, incidencias ni bonos.
2. El ledger conserva el evento y explica que fue rechazado por baja; el agente
   recibe un acuse terminal y no lo reintenta indefinidamente.
3. Un código Hik con historia de otra persona no puede reasignarse desde los
   flujos ordinarios de empleado.
4. El caso `355` se reconcilia hacia Johan sin perder evidencia cruda y sin
   borrar decisiones autorizadas o pagadas.
5. Toda corrección deja respaldo, conteos antes/después y `AuditLog`.

## Alternativas consideradas

### 1. Bloquear únicamente empleados inactivos

Es el cambio más pequeño y detiene nuevas filas, pero no evita que un código de
una persona activa se reasigne a otra persona activa. Tampoco corrige el caso
existente. Es necesario, pero insuficiente por sí solo.

### 2. Crear una tabla temporal de identidades Hik

Una identidad externa con vigencias explícitas sería el modelo más completo
para transferencias históricas. Requiere migración, interfaz, adopción de todos
los importadores y decisiones sobre traslados legítimos que hoy no existen.
Queda como evolución futura, no como reparación inmediata.

### 3. Protección sin migración y reconciliación auditada

La opción seleccionada reutiliza el ledger durable existente como reserva
histórica del código. La ingesta v2 comparte la misma regla de baja que la ruta
antigua; `Empleado.save()` bloquea un cambio hacia un código ya proyectado para
otra persona; y un comando de corrección, cerrado por precondiciones y `dry-run`,
repara el incidente `355`. Resuelve el defecto actual sin ampliar el esquema ni
inventar una política general de transferencias.

## Diseño funcional

### Protección de baja en Hik v2

Antes de `AsistenciaEmpleado.get_or_create`, la proyección obtiene la fecha local
del evento y consulta una regla compartida:

- empleado activo: la marca puede continuar;
- empleado inactivo con baja: se permite únicamente el día efectivo de la baja
  y se bloquean días posteriores;
- empleado inactivo sin baja: se bloquea cualquier fecha.

El recibo bloqueado queda durable con:

- estado `rejected`;
- `reason_code=employee_inactive_after_termination`;
- `retryable=false`;
- proyección `none`;
- efectos `skipped`;
- empleado resuelto y fecha de procesamiento.

La respuesta individual al agente será `rejected`, no `accepted`. Un reenvío
idéntico devuelve el mismo resultado terminal y no intenta proyectar nuevamente.

### Reserva histórica de códigos

Al cambiar `Empleado.codigo`, el modelo compara el valor anterior con el nuevo.
Si el nuevo código aparece en `EventoHikCloud` vinculado a otra persona, se
rechaza con un `ValidationError` accionable que identifica el conflicto sin
exponer datos sensibles adicionales.

La validación aplica a formularios, administración y llamadas ordinarias a
`save()`. Un código sin ledger previo continúa permitido. Guardar otros campos
sin cambiar el código no queda bloqueado, incluso para expedientes históricos
que ya contienen una colisión. `QuerySet.update()` y SQL directo permanecen
fuera del contrato operativo; solo el comando correctivo puede usarlos bajo
precondiciones y auditoría.

### Reconciliación del código 355

Se agregará un comando de gestión específico y reusable con `dry-run` por
omisión. Recibirá código afectado, código histórico que se restaurará al
origen, empleado origen, empleado destino, fecha inicial, fecha final y
`--apply`. Antes de escribir validará:

1. que el origen esté inactivo y la fecha sea posterior a su baja;
2. que el destino esté activo;
3. que los recibos pertenezcan al código y al origen;
4. que las asistencias afectadas provengan de Hik;
5. que no exista asistencia de destino en las mismas fechas;
6. que no exista hora extra autorizada, pagada o ajustada manualmente;
7. que la historia anterior del código apunte al destino;
8. que los códigos que se restaurarán correspondan a su historia previa.

Con `--apply`, el comando primero escribe una instantánea JSON recuperable fuera
del repositorio y después ejecuta una sola transacción que:

1. restaura `355` a Johan y `356` a Angélica mediante un valor temporal único;
2. mueve los recibos afectados al destino;
3. mueve cada asistencia Hik completa al destino;
4. mueve las horas extra automáticas vinculadas, actualiza su jefatura y las
   reevalúa con el turno vigente del destino;
5. reevalúa incidencias y sincronización derivada únicamente para esas jornadas;
6. crea `AuditLog` con identificadores, rango, conteos, valores anteriores y
   posteriores, motivo y huella del respaldo.

No se borran recibos. Una hora extra pendiente puede recalcularse o cancelarse
por saldo cero conforme al servicio normal. Una hora extra autorizada, pagada,
rechazada o con ajuste humano hace fallar todo el comando antes de escribir.

## Concurrencia y consistencia

La proyección toma el bloqueo de empleado ya existente y debe decidir el bloqueo
por baja antes de crear asistencia. La reconciliación adquiere las jornadas del
origen y destino en orden determinista mediante el mecanismo de bloqueo de horas
extra, y después bloquea recibos, asistencias y extras dentro de la misma
transacción.

Entre el despliegue del bloqueo y la corrección de códigos se realizará una
lectura fresca. Los eventos nuevos del `355` quedarán rechazados contra Angélica;
después de restaurar el código a Johan, los eventos posteriores se proyectarán
normalmente. Los recibos rechazados dentro del rango también podrán incluirse en
la reconciliación desde el payload durable.

## Pruebas

Las pruebas se escribirán antes del código de producción y cubrirán:

1. v2 rechaza marca posterior a la baja y no crea asistencia ni efectos;
2. v2 conserva el último día trabajado;
3. inactivo sin registro de baja queda bloqueado;
4. reenvío de un GUID bloqueado es terminal e idempotente;
5. cambio de código hacia uno con ledger de otra persona falla;
6. guardar otros campos con un código histórico sin cambiarlo continúa;
7. código nuevo o reservado por la misma persona continúa permitido;
8. `dry-run` del reconciliador no modifica ninguna tabla;
9. reconciliación mueve recibos, asistencia y extra pendiente al destino;
10. conflicto de asistencia o estado autorizado/pagado aborta todo;
11. la reevaluación usa el turno/jefatura del destino;
12. el `AuditLog` describe exactamente los cambios.

## Despliegue y corrección productiva

1. PostgreSQL 16 aislado, migraciones actuales aplicadas y baseline limpio.
2. Ciclos TDD dirigidos, suite RRHH/Hik afectada, `manage.py check` y
   `migrate --check`.
3. Commit, revisión completa, PR, CI y merge.
4. Despliegue mediante `scripts/deploy_web_safe.sh`, sin `git pull` manual.
5. Prueba controlada de rechazo de una marca post-baja sin alterar datos
   operativos, usando el servicio y una transacción revertida.
6. Lectura fresca del rango y ejecución productiva inicial en `dry-run`.
7. Respaldo JSON, ejecución `--apply` y lectura posterior de empleados,
   recibos, asistencias, extras, incidencias y auditoría.
8. Validación en la pantalla de horas extra y en reporte de asistencia.

## Fuera de alcance

- Cambiar horarios, salarios, nómina, bonos manuales o permisos.
- Corregir personas distintas a las detectadas por el comando y su rango.
- Borrar huellas del dispositivo Hik; esa acción física/MDM se reportará como
  seguimiento operativo si continúa enviando códigos retirados.
- Introducir una tabla de identidad temporal o migrar otros sistemas externos.
- Recalcular masivamente el historial de asistencia.
