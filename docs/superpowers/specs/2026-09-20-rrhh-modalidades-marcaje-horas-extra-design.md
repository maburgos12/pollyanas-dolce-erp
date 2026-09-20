# Modalidades de marcaje y horas extra verificables

## Problema

La detección automática vigente aplica una regla uniforme a realidades distintas. Point entrega principalmente entrada y salida; los repartidores registran por Hik-Connect, pero tampoco completan cuatro marcas; CEDIS y parte de Matriz sí registran salida y regreso de comida. Cuando una asistencia no tiene turno, el sistema usa una jornada genérica de ocho horas y puede crear horas extra aunque desconozca el horario programado y la duración real del descanso.

En la revisión productiva del 1 de agosto al 19 de septiembre de 2026 se observaron 184 días de repartidores: ninguno tuvo cuatro marcas completas, 170 tuvieron únicamente entrada y salida, y 14 carecieron de salida final. Los siete repartidores activos tampoco tenían turno vinculado a sus asistencias. Entre el 17 y el 19 de septiembre se generaron cuatro horas extra automáticas para repartidores, todas sin turno y sin marcas completas de comida.

## Resultado esperado

El ERP debe distinguir una marca esperada de una marca faltante. Una jornada de dos marcas no debe presentarse como una jornada de cuatro marcas incompleta; una jornada sin turno no debe producir una cantidad pagable de horas extra; y RRHH debe ver por qué un cálculo es válido, requiere revisión o no puede realizarse.

La implementación no asignará horarios inventados, no cancelará horas extra históricas y no alterará asistencias existentes. Los turnos reales de repartidores y demás personal deberán confirmarse operativamente antes de cargarlos.

## Alternativas consideradas

### 1. Descontar siempre 35 minutos cuando no haya marcas de comida

Es una corrección pequeña y eliminaría los cuatro casos recientes de repartidores, pero inventaría un descanso que en rutas puede no haber ocurrido. También confundiría comida incluida en la jornada con comida no pagada. Se descarta.

### 2. Inferir todo únicamente desde la fuente y el puesto

Point implicaría dos marcas, `puesto_operativo=REPARTIDOR` implicaría ruta y el resto implicaría cuatro marcas. Evita una migración de configuración, pero no admite excepciones reales: una persona puede cambiar de dispositivo, sede o modalidad. Se conserva como valor automático, no como única fuente.

### 3. Modalidad automática con excepción explícita

Se agrega una modalidad configurable en el empleado con valor predeterminado `AUTO`. Un único resolver decide la modalidad efectiva usando, en orden: excepción explícita, puesto de repartidor, fuente Point y modalidad de cuatro marcas. Esta opción conserva el comportamiento operativo actual sin captura masiva, permite corregir excepciones y concentra la regla en un solo lugar. Es la alternativa seleccionada.

## Modelo de datos

### Empleado

Agregar `modalidad_marcaje` con estas opciones:

- `AUTO`: el sistema resuelve la modalidad a partir del puesto y la fuente.
- `CUATRO_MARCAS`: entrada, salida de comida, regreso de comida y salida final.
- `DOS_MARCAS`: entrada y salida final; la comida no es observable en el checador.
- `RUTA`: entrada y salida final con actividad fuera del centro de trabajo.

El valor predeterminado será `AUTO`. No habrá migración de datos: los repartidores se reconocerán por `puesto_operativo='REPARTIDOR'` y Point por `AsistenciaEmpleado.fuente='point'`. El campo se expondrá en la administración de empleados para registrar excepciones sin cambiar puesto, sucursal o fuente.

### Incidencias de asistencia

Agregar dos tipos informativos, sin equivalencia automática a falta ni descuento:

- `hora_extra_no_calculable`: existe un intervalo que supera la referencia de ocho horas y diez minutos, pero falta un turno para determinar el excedente real.
- `marcaje_incompleto`: falta entrada o salida final, o una modalidad de cuatro marcas contiene únicamente una de las marcas de comida.

Estas incidencias deben quedar pendientes de revisión y no crear una `HoraExtra`.

## Resolución de modalidad

Crear una función pura y centralizada:

1. Si `Empleado.modalidad_marcaje` no es `AUTO`, usar la elección explícita.
2. Si `puesto_operativo` normalizado es `REPARTIDOR`, usar `RUTA`.
3. Si la asistencia proviene de Point, usar `DOS_MARCAS`.
4. En cualquier otro caso, usar `CUATRO_MARCAS`.

Los servicios de incidencias, horas extra y presentación deben consumir esta función. Ningún template debe duplicar la inferencia.

## Cálculo de horas extra

### Requisitos comunes

Una hora extra automática solo será calculable cuando existan:

- entrada;
- salida final;
- turno asignado;
- intervalo cronológicamente válido y menor o igual a 24 horas.

La entrada anticipada seguirá sin ampliar la jornada: el cálculo comienza en el máximo entre la entrada real y el inicio del turno. La tolerancia será la configurada en el turno.

### Cuatro marcas

Con salida y regreso de comida válidos, los primeros 35 minutos continúan incluidos en la jornada y solo se descuenta el exceso. Si aparece exactamente una marca de comida, el día queda como `marcaje_incompleto` y no genera hora extra automática. Si no aparece ninguna marca de comida, el ERP no inventa el descanso: puede detectar permanencia posterior al turno, pero la hora extra queda identificada visualmente como basada en comida no observable y requiere revisión humana antes de autorización.

### Dos marcas y ruta

La ausencia de marcas de comida es esperada y no genera incidencia. El posible excedente se mide contra la salida programada del turno, no contra ocho horas genéricas. La solicitud automática queda pendiente y su explicación debe indicar que la comida no fue observable; la autorización humana confirma el caso. Si se registraron ambas marcas de comida, se usan para descontar únicamente el exceso sobre 35 minutos.

### Sin turno

No se crea ni actualiza una `HoraExtra` automática. Si el intervalo supera ocho horas y diez minutos, se crea o actualiza `hora_extra_no_calculable` con el motivo “Falta asignar el turno de esta jornada”. Las solicitudes manuales permanecen permitidas y no se bloquean por esta regla.

### Registros históricos

- No cancelar, rechazar, recalcular ni borrar horas extra autorizadas, rechazadas o pagadas.
- Una hora extra automática pendiente sin turno no podrá autorizarse desde la lista, API de RRHH ni APIs de bonos hasta que la asistencia tenga turno y sea reevaluada.
- Una hora extra automática ya autorizada sin turno mostrará “Revisión recomendada”, pero conservará estado, monto y auditoría.
- Para autorización y presentación, toda solicitud con `asistencia_id` es automática: el vínculo estructural creado por el generador determina su origen, aunque las notas se editen o queden vacías. Las capturas manuales no asignan asistencia y no se someten al bloqueo retroactivo. El prefijo de las notas nunca debe decidir si se valida el diagnóstico/saldo.

## Interfaz de horas extra

Cada registro automático mostrará una explicación corta y accionable, derivada del backend:

- modalidad efectiva: “4 marcas”, “2 marcas” o “Ruta”;
- cobertura observada: “Comida registrada” o “Comida no observable”;
- turno utilizado o “Sin turno asignado”;
- resultado: “Calculado”, “Requiere revisión” o “No calculable”.

El texto genérico “Jornada con comida incluida. Saldo no cubierto por otros registros” dejará de ser la única explicación. Las notas históricas se conservarán, pero la pantalla antepondrá el contexto calculado actual.

Para una hora automática pendiente sin turno, el botón `Autorizar` estará deshabilitado y acompañado por “Asigna el turno y reevalúa la asistencia antes de autorizar”. `Rechazar` seguirá disponible para resolver propuestas históricas improcedentes. La interfaz utilizará texto, no solo color, y mantendrá el diseño compacto y adaptable existente.

## Flujo y errores

1. La ingesta guarda las marcas sin reinterpretar datos históricos.
2. La evaluación diaria resuelve la modalidad efectiva.
3. Un marcaje incompleto produce incidencia y nunca hora extra.
4. Una jornada sin turno puede producir `hora_extra_no_calculable`, pero nunca una cantidad pagable.
5. Una jornada calculable genera o actualiza únicamente la hora automática pendiente asociada.
6. Un servicio compartido vuelve a validar estado pendiente, permiso y cálculo vigente en todas las autorizaciones. Las automáticas necesitan saldo positivo y coincidencia exacta entre las horas almacenadas y el saldo no cubierto; ante diferencias se exige reevaluación sin modificar estado, monto ni metadatos.

La resolución transaccional bloquea primero todas las asistencias del empleado/fecha en orden de pk, aunque la solicitud sea manual y no tenga asistencia vinculada. Después relee y bloquea las horas extra del día en orden de pk, verifica el enlace si existe y aplica autorización o rechazo. Este orden coincide con generador/señales y evita el ciclo asistencia → extra → asistencia en resoluciones manuales concurrentes.

La lógica de cálculo seguirá siendo única para UI, incidencias y autorización; no se duplicará en templates ni endpoints.

## Compatibilidad y consumidores

- `rrhh.services_extra_conciliacion`: resolverá modalidad y calculabilidad.
- `rrhh.services`: conservará idempotencia y protección de estados autorizados, rechazados, pagados y cancelados.
- `rrhh.services_asistencia_reglas`: emitirá incidencias de dato incompleto/no calculable.
- `rrhh.services_horas_extra_autorizacion`: centralizará resolución, validación y orden de bloqueos.
- `rrhh.views.horas_extra_list`, `rrhh.api_views.HoraExtraViewSet` y `rrhh.bonos_horas_extra.BaseHorasExtraEquipoViewSet`: usarán el servicio compartido conservando alcance, permisos y formatos de respuesta; producción y ventas heredan el adaptador de bonos.
- `rrhh/templates/rrhh/horas_extra_list.html`: mostrará contexto y bloqueo accionable.
- Administración de `Empleado`: permitirá elegir una excepción explícita.
- Bonos, nómina y prenómina continuarán consumiendo únicamente horas autorizadas; sus contratos no cambian.

No se modifica Point, Hik-Connect, el esquema de rutas ni el cálculo de nómina.

## Pruebas

Las pruebas se escribirán antes de cada cambio de producción y cubrirán:

1. Resolución automática y excepción explícita de modalidad.
2. Repartidor sin turno: no crea hora extra y genera incidencia no calculable cuando corresponde.
3. Point sin turno: mismo bloqueo sin tratar la ausencia de comida como error.
4. Cuatro marcas con comida válida: conserva el cálculo actual.
5. Una sola marca de comida en modalidad de cuatro marcas: incidencia de marcaje incompleto y cero horas automáticas.
6. Dos marcas o ruta con turno: calcula contra el turno y no genera incidencia de comida.
7. Entrada sin salida y salida sin entrada: marcaje incompleto, nunca hora extra.
8. Hora automática pendiente sin turno: autorización bloqueada con mensaje; rechazo permitido.
9. Hora manual: autorización conserva el flujo vigente.
10. Estados autorizados, rechazados y pagados no se reescriben durante reevaluación.
11. Migración, serialización/admin, vista de escritorio y vista móvil.

## Validación y despliegue

- PostgreSQL aislado en el worktree, todas las migraciones aplicadas y `migrate --check` limpio.
- Pruebas dirigidas de RRHH, pruebas consumidoras de bonos/prenómina y `manage.py check`.
- Revisión del diff completo y CI del PR.
- Despliegue con `scripts/deploy_web_safe.sh`, sin `git pull` manual.
- Lectura posterior en producción para verificar migración, código servido y que no se modificaron estados históricos.
- Validación en navegador real de la lista de horas extra en escritorio y móvil, incluyendo un registro calculable y uno bloqueado.

## Fuera de alcance

- Inventar o cargar los horarios reales de repartidores sin confirmación operativa.
- Cancelar en lote las horas extra automáticas existentes.
- Cambiar salarios, tasas, nómina, permisos o políticas laborales.
- Rediseñar el módulo completo de RRHH.
