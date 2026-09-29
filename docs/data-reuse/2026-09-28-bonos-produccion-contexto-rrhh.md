# Ficha de fuentes — bonos de producción con contexto RRHH

Fecha y ambiente consultado: 2026-09-28; código en `origin/main` y consultas acotadas de solo lectura en producción.

## Necesidad y unidad de análisis

Bonos de producción necesita decidir, para cada combinación **empleado + fecha del corte**, si el día:

- tuvo asistencia y puntualidad;
- constituye una falta penalizable;
- está justificado por una fuente vigente de Capital Humano; o
- no era exigible por ingreso, baja, descanso programado o descanso obligatorio.

El bono es un consumidor calculado. No debe volver a capturar incapacidades, permisos, vacaciones, suspensiones, jornadas ni festivos.

## Fuentes candidatas

| Concepto | Modelo / tabla | Fuente que crea y actualiza | Identificador y ámbito | Evidencia de registros | Consumidores |
| --- | --- | --- | --- | --- | --- |
| Identidad y vigencia laboral | `rrhh.Empleado`, `rrhh.EmpleadoBaja` | Ficha y bajas de Capital Humano | `empleado_id`; `fecha_ingreso`; última baja aplicable | Argelia es empleado 86, activa, ingreso histórico 2007-07-30 y sin baja | RRHH, nómina, asistencia, bonos |
| Jornada y descanso semanal | `rrhh.AsignacionJornadaEmpleado`, `JornadaSemanal`, `JornadaSemanalDia` | Configuración de jornadas de RRHH | empleado + vigencia + día de semana | El inventario confirmó las tres tablas; el resolvedor actual es `horario_programado_para_fecha` | Checador, incidencias, horas extra |
| Descanso obligatorio | `rrhh.services_vacaciones.es_descanso_oficial` | Regla laboral centralizada en código | fecha | El 16-09-2026 ya está reconocido como descanso oficial; no existe otra tabla de festivos | Vacaciones y reglas de asistencia |
| Marcas reales | `rrhh.AsistenciaEmpleado` | HikConnect/checador y ajustes autorizados | único empleado + fecha | Argelia tiene 21 fechas con asistencia del 02 al 26 de septiembre dentro del corte | Asistencia, prenómina, bonos |
| Incidencias evaluadas | `rrhh.IncidenciaAsistencia` | `evaluar_dia_empleado` y conciliaciones de RRHH | único empleado + fecha + tipo | Contiene estados pendiente, conciliado y resuelto, y relaciones a permiso/vacaciones | Prenómina, reportes, bonos |
| Incapacidad | `rrhh.IncapacidadEmpleado` | Captura auditada de Capital Humano | empleado + rango; folio único no vacío por empleado | Argelia tiene seis incapacidades contiguas del 03-06-2026 al 01-09-2026; estados activas | Asistencia y prenómina |
| Permiso | `rrhh.PermisoSalida` | Solicitud y autorización de jefe/Dirección | folio; empleado + rango + estado | Argelia tiene permisos aprobados por horas dentro del corte | Asistencia y prenómina |
| Vacaciones | `rrhh.SolicitudVacaciones` | Flujo de jefe y aprobación RRHH | folio; empleado + rango + estado | Fuente confirmada por inventario y por el conciliador de faltas | Saldos, asistencia, prenómina |
| Suspensión | `rrhh.SuspensionEmpleado` | Capital Humano | empleado + rango + estado | Fuente confirmada por inventario; RRHH genera incidencia conciliada de suspensión | Asistencia y prenómina |
| Configuración y montos | `bonos_produccion.ConfigBonoPeriodo` | Pantalla de configuración de bonos | único mes + año | Producción ya tiene `monto_preparacion` y `monto_cuartos_frios`; ambos valen 850 en el periodo revisado | Cálculo de bonos |
| Captura diaria del bono | `bonos_produccion.RegistroDiarioProduccion` | Sincronización RRHH y correcciones manuales autorizadas | único bono + día | Actualmente sólo guarda booleanos y no distingue falta de incapacidad/festivo | PWA de bonos y cálculo |

## Alias y equivalencias

| Términos o identificadores | Estado | Evidencia y caso contrario | Revisión requerida |
| --- | --- | --- | --- |
| “Reingresó el 2” = terminó incapacidad el 1 | Confirmada para el caso de Argelia | La última incapacidad registrada termina el 01-09-2026 y la primera asistencia posterior es el 02-09-2026. No equivale a cambiar su antigüedad o `fecha_ingreso`. | Ninguna equivalencia global: se resuelve por rangos reales de incapacidad |
| Incapacidad activa/cerrada = día no penalizable | Confirmada | RRHH resuelve incidencias stale y no crea falta durante el rango; coincide con LFT arts. 42 y 43 y con la justificación de ausencia del certificado IMSS | Mantener estados cancelados fuera |
| 16 de septiembre = descanso obligatorio | Confirmada | LFT art. 74, fracción V, y `es_descanso_oficial` existente | Ninguna |
| Permiso aprobado = falta conciliada | Confirmada como política actual de RRHH | `_evaluar_falta_sin_registro` crea falta conciliada y conserva goce/sin goce | Bonos debe reutilizar el resultado, no reinterpretar el permiso |
| Vacación solicitada/preautorizada/aprobada = rango reservado no penalizable | Confirmada como política actual de RRHH | El servicio vigente concilia el día mientras la reserva esté viva y reevalúa si se rechaza | Ninguna |
| Suspensión activa = falta | Distinta | RRHH registra una incidencia de suspensión y evita duplicarla como falta. Que una suspensión cancele un bono requeriría una regla de negocio separada que hoy no existe. | No inventar cancelación de bono |
| Ausencia de marca = falta | Candidata, no suficiente por sí sola | Puede ser incapacidad, descanso, festivo, permiso, vacaciones, suspensión, preingreso o postbaja | Resolver siempre contra contexto RRHH |

## Decisión de diseño

Se reutilizarán las fuentes y reglas existentes de RRHH mediante un clasificador diario de solo lectura, compartido y público. Bonos almacenará únicamente la **proyección del resultado** necesaria para trazabilidad y cálculo: estado RRHH del día, motivo visible y si existe falta penalizable. No se crearán capturas paralelas de incidencias ni una fecha de “reingreso” en bonos.

La sincronización:

1. cargará por lote empleados, asistencias, incidencias y rangos RRHH;
2. clasificará cada fecha respetando vigencias y estados;
3. conservará valores diarios capturados manualmente y los ajustes monetarios;
4. calculará faltas desde la clasificación explícita, no desde `dias_laborables - asistencias`; y
5. será idempotente y usará operaciones masivas para evitar esperas y recargas repetidas.

`ConfigBonoPeriodo` seguirá siendo la única fuente de montos. Se expondrán en la pantalla los campos existentes de Preparación y Cuartos fríos; no se sembrará ni aplicará automáticamente $300 a producción.

Consultas o procedimiento reproducible:

- `python manage.py inventario_fuentes_datos --term incapacidad|permiso|suspension|vacaciones|asistencia|jornada|festivo` con PostgreSQL.
- Consultas ORM acotadas a empleado 86 y al corte 2026-08-28 a 2026-09-26.
- Inspección del grafo de código de modelos, servicios y consumidores.

Riesgos y pendientes:

- Los registros diarios históricos no tienen fecha completa ni procedencia RRHH; la migración debe ser aditiva y nullable para no reinterpretarlos hasta una sincronización explícita.
- Los días capturados manualmente deben conservar prioridad y mostrar que son una excepción manual.
- Bonos de ventas comparte un patrón parecido, pero queda fuera de este cambio para no mezclar módulos; se documentará como consumidor candidato posterior.
- La corrección de datos del periodo actual se hará sólo después de un preview de diferencias y autorización independiente.

## Base normativa consultada

- Ley Federal del Trabajo vigente, artículos 42 y 43: la incapacidad temporal suspende la obligación de prestar el servicio durante el periodo fijado por el IMSS.
- Ley Federal del Trabajo vigente, artículo 74, fracción V: el 16 de septiembre es descanso obligatorio.
- IMSS, “Pago de incapacidades”: el certificado justifica la ausencia durante los días de recuperación.

Fuentes oficiales:

- https://www.diputados.gob.mx/LeyesBiblio/pdf/LFT.pdf
- https://www.imss.gob.mx/derechoH/pago-incapacidades
